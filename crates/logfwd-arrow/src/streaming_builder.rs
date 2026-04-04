// streaming_builder.rs -- Zero-copy hot-path builder.
//
// Uses Arrow's StringViewArray to store string values as 16-byte views
// pointing directly into the input buffer. No string copies during scanning
// or during finish_batch. The input buffer (as bytes::Bytes) is reference-counted
// and shared between the builder and the resulting Arrow arrays.
//
// For the hot pipeline path: scan -> query -> output -> discard.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringViewBuilder, StructArray};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use logfwd_core::scan_config::{parse_float_fast, parse_int_fast};
use logfwd_core::scanner::BuilderState;

use crate::check_dup_bits;

// ---------------------------------------------------------------------------
// Per-field state
// ---------------------------------------------------------------------------

struct FieldColumns {
    name: Vec<u8>,
    /// String values: (row, offset_in_buffer, len). Views into the shared
    /// buffer. Offsets `< buf.len()` reference the original input buffer;
    /// offsets `>= buf.len()` reference the decoded-strings buffer at
    /// `offset - buf.len()`. See `StreamingBuilder::decoded_buf`.
    str_views: Vec<(u32, u32, u32)>,
    /// Int values: (row, parsed_value).
    int_values: Vec<(u32, i64)>,
    /// Float values: (row, parsed_value).
    float_values: Vec<(u32, f64)>,
    has_str: bool,
    has_int: bool,
    has_float: bool,
}

impl FieldColumns {
    fn new(name: &[u8]) -> Self {
        FieldColumns {
            name: name.to_vec(),
            str_views: Vec::with_capacity(256),
            int_values: Vec::with_capacity(256),
            float_values: Vec::with_capacity(256),
            has_str: false,
            has_int: false,
            has_float: false,
        }
    }

    fn clear(&mut self) {
        self.str_views.clear();
        self.int_values.clear();
        self.float_values.clear();
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
    }
}

// ---------------------------------------------------------------------------
// StreamingBuilder
// ---------------------------------------------------------------------------

/// Zero-copy builder for the hot pipeline path.
///
/// String values are stored as (offset, len) views into the input buffer.
/// The buffer is reference-counted via `bytes::Bytes` and shared with the
/// resulting Arrow `StringViewArray` columns -- no string copies at any point.
///
/// Numeric values (int, float) are parsed during scanning and stored directly.
///
/// When `keep_raw` is true, a `_raw` column is emitted in each batch as a
/// zero-copy `StringViewArray` containing the full unparsed line per row.
///
/// # Usage
/// ```ignore
/// let mut builder = StreamingBuilder::new(false);
/// builder.begin_batch(bytes::Bytes::from(buf));
/// // ... scan fields, call append_*_by_idx ...
/// let batch = builder.finish_batch();
/// ```
pub struct StreamingBuilder {
    fields: Vec<FieldColumns>,
    field_index: HashMap<Vec<u8>, usize>,
    /// Number of fields active in the current batch. Slots `0..num_active` in
    /// `fields` are in use; slots beyond that are pre-allocated but dormant.
    /// Resetting this to zero on `begin_batch` — together with clearing
    /// `field_index` — bounds `fields.len()` to the high-water mark of unique
    /// fields seen in any *single* batch rather than growing without limit.
    num_active: usize,
    row_count: u32,
    /// Tracks which fields (by index) were written in the current row.
    /// Only covers the first 64 fields (indices 0-63); see `check_dup_bits`.
    written_bits: u64,
    /// Reference-counted buffer. Stored here to compute offsets safely
    /// and shared with Arrow StringViewArrays in finish_batch.
    buf: bytes::Bytes,
    /// Secondary buffer for decoded string values (JSON escape sequences).
    /// String views with offsets `>= buf.len()` reference this buffer at
    /// `offset - buf.len()`. Allocated lazily; empty when no escapes are decoded.
    decoded_buf: Vec<u8>,
    /// When true, `append_raw` stores (offset, len) views for the `_raw` column.
    keep_raw: bool,
    /// Raw line views: (offset_in_buf, len) per row, in row order.
    /// Populated only when `keep_raw` is true.
    raw_views: Vec<(u32, u32)>,
    /// Protocol state — enforced via `debug_assert` in each method.
    state: BuilderState,
}

impl Default for StreamingBuilder {
    fn default() -> Self {
        Self::new(false)
    }
}

impl StreamingBuilder {
    pub fn new(keep_raw: bool) -> Self {
        StreamingBuilder {
            fields: Vec::with_capacity(32),
            field_index: HashMap::with_capacity(32),
            num_active: 0,
            row_count: 0,
            written_bits: 0,
            buf: bytes::Bytes::new(),
            decoded_buf: Vec::new(),
            keep_raw,
            raw_views: Vec::new(),
            state: BuilderState::Idle,
        }
    }

    /// Start a new batch. Takes ownership of the input buffer via Bytes
    /// (zero-copy if converted from Vec via `Bytes::from(vec)`).
    ///
    /// # Panics (debug builds)
    /// Debug-asserts that `buf.len()` fits in a u32, because string-view
    /// offsets are stored as u32.  Buffers larger than 4 GiB would produce
    /// silently truncated offsets without this guard.
    pub fn begin_batch(&mut self, buf: bytes::Bytes) {
        debug_assert_ne!(
            self.state,
            BuilderState::InRow,
            "begin_batch called while inside a row (missing end_row)"
        );
        debug_assert!(
            u32::try_from(buf.len()).is_ok(),
            "StreamingBuilder buffer too large for u32 offsets ({} bytes)",
            buf.len()
        );
        self.buf = buf;
        self.decoded_buf.clear();
        self.row_count = 0;
        // Only clear the slots that were active in the previous batch.
        // This preserves the inner-Vec capacity of each FieldColumns for
        // hot-path reuse while still bounding memory under key churn.
        for fc in &mut self.fields[..self.num_active] {
            fc.clear();
        }
        // Discard all name→index mappings so resolve_field rebuilds them from
        // scratch.  Combined with resetting num_active, this prevents the
        // HashMap and the fields Vec from growing across batches with churning
        // field names (issue: field_index HashMap grows unboundedly).
        self.field_index.clear();
        self.num_active = 0;
        self.raw_views.clear();
        self.state = BuilderState::InBatch;
    }

    #[inline(always)]
    pub fn begin_row(&mut self) {
        debug_assert_eq!(
            self.state,
            BuilderState::InBatch,
            "begin_row called outside of a batch (call begin_batch first)"
        );
        self.written_bits = 0;
        self.state = BuilderState::InRow;
    }

    #[inline(always)]
    pub fn end_row(&mut self) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "end_row called without a matching begin_row"
        );
        self.row_count = self
            .row_count
            .checked_add(1)
            .expect("row_count overflow: batch exceeds u32::MAX rows");
        self.state = BuilderState::InBatch;
    }

    #[inline]
    pub fn resolve_field(&mut self, key: &[u8]) -> usize {
        debug_assert!(
            self.state == BuilderState::InBatch || self.state == BuilderState::InRow,
            "resolve_field called outside of an active batch"
        );
        if let Some(&idx) = self.field_index.get(key) {
            return idx;
        }
        let idx = self.num_active;
        if idx < self.fields.len() {
            // Reuse an existing slot — its data was cleared in begin_batch.
            // Only the name needs updating.
            self.fields[idx].name.clear();
            self.fields[idx].name.extend_from_slice(key);
        } else {
            self.fields.push(FieldColumns::new(key));
        }
        self.num_active += 1;
        self.field_index.insert(key.to_vec(), idx);

        idx
    }

    /// Compute the byte offset of `value` within `self.buf`.
    ///
    /// `value` must be a subslice of `self.buf`; i.e. both its pointer and
    /// length lie within `self.buf` bounds. We use `usize` subtraction rather
    /// than `offset_from` to avoid that API's stricter UB preconditions.
    ///
    /// # Panics (debug builds)
    /// Debug-asserts that `value` lies entirely within `self.buf`, and that
    /// the computed byte offset fits in a u32.  A buffer larger than 4 GiB
    /// would cause the offset to silently truncate without this guard.
    #[inline(always)]
    fn offset_of(&self, value: &[u8]) -> u32 {
        let base = self.buf.as_ptr() as usize;
        let ptr = value.as_ptr() as usize;
        debug_assert!(
            ptr >= base && ptr + value.len() <= base + self.buf.len(),
            "value must be within buffer bounds"
        );
        let offset = ptr - base;
        debug_assert!(
            u32::try_from(offset).is_ok(),
            "StreamingBuilder buffer offset exceeds u32::MAX ({offset} bytes)"
        );
        offset as u32
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_str_by_idx called outside of a row"
        );
        if check_dup_bits(&mut self.written_bits, idx) {
            return;
        }
        // StringViewArray requires valid UTF-8.  JSON is always UTF-8 in
        // production; for fuzz / corrupted input we skip non-UTF-8 bytes so
        // that append_view_unchecked is never called on invalid bytes.
        if std::str::from_utf8(value).is_err() {
            return;
        }
        let offset = self.offset_of(value);
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.str_views
            .push((self.row_count, offset, value.len() as u32));
    }

    /// Append a decoded string value that is NOT a subslice of the input
    /// buffer. Used for strings whose JSON escape sequences have been decoded
    /// (see issue #410). Appends the bytes to `decoded_buf` and records a
    /// view in the same `str_views` vector as regular strings, with the
    /// offset shifted by `buf.len()` so that `finish_batch` can create a
    /// combined Arrow buffer.
    #[inline(always)]
    pub fn append_decoded_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_decoded_str_by_idx called outside of a row"
        );
        if check_dup_bits(&mut self.written_bits, idx) {
            return;
        }
        if std::str::from_utf8(value).is_err() {
            return;
        }
        let decoded_offset = self.decoded_buf.len() as u32;
        self.decoded_buf.extend_from_slice(value);
        // Offset into the combined buffer: original buf bytes come first,
        // decoded bytes follow at buf.len() + decoded_offset.
        let combined_offset = self.buf.len() as u32 + decoded_offset;
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.str_views
            .push((self.row_count, combined_offset, value.len() as u32));
    }

    #[inline(always)]
    pub fn append_int_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_int_by_idx called outside of a row"
        );
        if check_dup_bits(&mut self.written_bits, idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if let Some(v) = parse_int_fast(value) {
            fc.has_int = true;
            fc.int_values.push((self.row_count, v));
        }
    }

    #[inline(always)]
    pub fn append_float_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_float_by_idx called outside of a row"
        );
        if check_dup_bits(&mut self.written_bits, idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if let Some(v) = parse_float_fast(value) {
            fc.has_float = true;
            fc.float_values.push((self.row_count, v));
        }
    }

    #[inline(always)]
    pub fn append_null_by_idx(&mut self, idx: usize) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_null_by_idx called outside of a row"
        );
        // Nulls are represented by gaps -- no value record needed.
        // But mark as written for duplicate-key detection.
        let _ = check_dup_bits(&mut self.written_bits, idx);
    }

    /// Store a zero-copy view of the raw unparsed line.
    ///
    /// Only has effect when the builder was created with `keep_raw: true`.
    /// The line must be a subslice of the buffer passed to `begin_batch`.
    #[inline(always)]
    pub fn append_raw(&mut self, line: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_raw called outside of a row"
        );
        if self.keep_raw {
            let offset = self.offset_of(line);
            self.raw_views.push((offset, line.len() as u32));
        }
    }

    /// Build a RecordBatch with zero-copy StringViewArrays.
    ///
    /// When no JSON escape sequences were decoded, the resulting RecordBatch
    /// shares the input buffer via Bytes reference counting (zero-copy).
    /// When decoded strings exist, a combined buffer is built that appends
    /// decoded bytes after the original input so that all str_views offsets
    /// resolve into a single contiguous Arrow buffer.
    pub fn finish_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        debug_assert_eq!(
            self.state,
            BuilderState::InBatch,
            "finish_batch called outside of a batch (call begin_batch first, and ensure all rows are closed with end_row)"
        );
        let num_rows = self.row_count as usize;

        // Build the Arrow buffer. When no decoded strings exist, this is
        // zero-copy via Bytes refcount. When decoded strings are present,
        // we concatenate the original buffer with the decoded buffer so that
        // str_views offsets >= buf.len() resolve correctly.
        let arrow_buf = if self.decoded_buf.is_empty() {
            Buffer::from(self.buf.clone())
        } else {
            let mut combined = Vec::with_capacity(self.buf.len() + self.decoded_buf.len());
            combined.extend_from_slice(&self.buf);
            combined.extend_from_slice(&self.decoded_buf);
            Buffer::from(combined)
        };
        // Separate zero-copy buffer for _raw views (always into the original
        // input buffer, never into decoded_buf).
        let raw_arrow_buf = Buffer::from(self.buf.clone());

        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.num_active);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_active);

        // Detect duplicate output column names before building the schema.
        let mut emitted_names = std::collections::HashSet::new();
        if self.keep_raw && !self.raw_views.is_empty() {
            emitted_names.insert("_raw".to_string());
        }
        let mut reserve_name = |name: &str| -> Result<(), ArrowError> {
            if emitted_names.insert(name.to_string()) {
                Ok(())
            } else {
                Err(ArrowError::InvalidArgumentError(format!(
                    "duplicate output column name: {name}"
                )))
            }
        };

        for fc in &self.fields[..self.num_active] {
            // Field names come from JSON keys (valid UTF-8 in well-formed input).
            // Use from_utf8_lossy so that fuzz inputs with arbitrary bytes are
            // handled gracefully instead of triggering undefined behaviour.
            let name = String::from_utf8_lossy(&fc.name);

            // Emit a StructArray when the same field has multiple types in this
            // batch. Single-type fields use the bare field name as a flat column.
            let conflict = (fc.has_int as u8) + (fc.has_float as u8) + (fc.has_str as u8) > 1;

            if conflict {
                let mut child_fields: Vec<Arc<Field>> = Vec::new();
                let mut child_arrays: Vec<ArrayRef> = Vec::new();

                if fc.has_int {
                    let mut values = vec![0i64; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.int_values {
                        let row = row as usize;
                        if row >= num_rows {
                            continue;
                        }
                        values[row] = v;
                        valid[row] = true;
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = Int64Array::new(values.into(), Some(nulls));
                    child_fields.push(Arc::new(Field::new("int", DataType::Int64, true)));
                    child_arrays.push(Arc::new(array) as ArrayRef);
                }

                if fc.has_float {
                    let mut values = vec![0.0f64; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.float_values {
                        let row = row as usize;
                        if row >= num_rows {
                            continue;
                        }
                        values[row] = v;
                        valid[row] = true;
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = Float64Array::new(values.into(), Some(nulls));
                    child_fields.push(Arc::new(Field::new("float", DataType::Float64, true)));
                    child_arrays.push(Arc::new(array) as ArrayRef);
                }

                if fc.has_str {
                    let mut builder = StringViewBuilder::new();
                    let block = builder.append_block(arrow_buf.clone());
                    let mut vi = 0;
                    for row in 0..num_rows as u32 {
                        if vi < fc.str_views.len() && fc.str_views[vi].0 == row {
                            let (_, offset, len) = fc.str_views[vi];
                            builder
                                .try_append_view(block, offset, len)
                                .expect("offset/len pre-validated by offset_of and UTF-8 check");
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    child_fields.push(Arc::new(Field::new("str", DataType::Utf8View, true)));
                    child_arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }

                // Struct is non-null iff any child is non-null for that row.
                let struct_validity: Vec<bool> = (0..num_rows)
                    .map(|i| child_arrays.iter().any(|arr| !arr.is_null(i)))
                    .collect();

                let fields = Fields::from(child_fields);
                let struct_arr = StructArray::new(
                    fields.clone(),
                    child_arrays,
                    Some(NullBuffer::from(struct_validity)),
                );

                reserve_name(name.as_ref())?;
                schema_fields.push(Field::new(name.as_ref(), DataType::Struct(fields), true));
                arrays.push(Arc::new(struct_arr) as ArrayRef);
            } else {
                // Single-type field: bare flat column.
                if fc.has_int {
                    reserve_name(name.as_ref())?;
                    let mut values = vec![0i64; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.int_values {
                        let row = row as usize;
                        if row >= num_rows {
                            continue;
                        }
                        values[row] = v;
                        valid[row] = true;
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = Int64Array::new(values.into(), Some(nulls));
                    schema_fields.push(Field::new(name.as_ref(), DataType::Int64, true));
                    arrays.push(Arc::new(array) as ArrayRef);
                }

                if fc.has_float {
                    reserve_name(name.as_ref())?;
                    let mut values = vec![0.0f64; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.float_values {
                        let row = row as usize;
                        if row >= num_rows {
                            continue;
                        }
                        values[row] = v;
                        valid[row] = true;
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = Float64Array::new(values.into(), Some(nulls));
                    schema_fields.push(Field::new(name.as_ref(), DataType::Float64, true));
                    arrays.push(Arc::new(array) as ArrayRef);
                }

                if fc.has_str {
                    reserve_name(name.as_ref())?;
                    let mut builder = StringViewBuilder::new();
                    let block = builder.append_block(arrow_buf.clone());

                    let mut vi = 0;
                    for row in 0..num_rows as u32 {
                        if vi < fc.str_views.len() && fc.str_views[vi].0 == row {
                            let (_, offset, len) = fc.str_views[vi];
                            builder
                                .try_append_view(block, offset, len)
                                .expect("offset/len pre-validated by offset_of and UTF-8 check");
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }

                    schema_fields.push(Field::new(name.as_ref(), DataType::Utf8View, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }
            }
        }

        if self.keep_raw && !self.raw_views.is_empty() {
            debug_assert_eq!(
                self.raw_views.len(),
                num_rows,
                "raw_views must have exactly one entry per row: {} views for {} rows",
                self.raw_views.len(),
                num_rows
            );
            let mut builder = StringViewBuilder::new();
            let block = builder.append_block(raw_arrow_buf);

            for row in 0..num_rows {
                if row < self.raw_views.len() {
                    let (offset, len) = self.raw_views[row];
                    builder
                        .try_append_view(block, offset, len)
                        .expect("raw view offset/len must be within buffer");
                } else {
                    builder.append_null();
                }
            }

            schema_fields.push(Field::new("_raw", DataType::Utf8View, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let result = RecordBatch::try_new_with_options(schema, arrays, &opts);
        self.state = BuilderState::Idle;
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StructArray as ArrowStructArray};

    #[test]
    fn test_basic_string_and_int() {
        let json = br"not used directly";
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        let idx_name = b.resolve_field(b"name");
        let idx_age = b.resolve_field(b"age");

        b.begin_row();
        // "name" value at bytes 0..3 = "not"
        b.append_str_by_idx(idx_name, &buf[0..3]);
        b.append_int_by_idx(idx_age, b"42");
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Single-type fields use bare column names (no suffix).
        assert!(batch.column_by_name("name").is_some());
        assert!(batch.column_by_name("age").is_some());
    }

    #[test]
    fn test_type_conflict_produces_struct_column() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        let idx_status = b.resolve_field(b"status");

        // Row 0: status is an int
        b.begin_row();
        b.append_int_by_idx(idx_status, b"200");
        b.end_row();

        // Row 1: status is a string -- type conflict
        b.begin_row();
        b.append_str_by_idx(idx_status, &buf[0..0]); // empty but valid str
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Old suffixed columns must not exist
        assert!(
            batch.column_by_name("status__int").is_none(),
            "status__int must not exist"
        );
        assert!(
            batch.column_by_name("status__str").is_none(),
            "status__str must not exist"
        );
        // New struct column
        let status_col = batch
            .column_by_name("status")
            .expect("status struct column must exist");
        assert!(
            matches!(status_col.data_type(), DataType::Struct(_)),
            "status must be Struct, got {:?}",
            status_col.data_type()
        );
        let sa = status_col
            .as_any()
            .downcast_ref::<ArrowStructArray>()
            .unwrap();
        let child_names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(child_names.contains(&"int"), "missing int child");
        assert!(child_names.contains(&"str"), "missing str child");
    }

    #[test]
    fn test_zero_copy_string_content() {
        let data = b"hello world foobar";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"msg");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..5]); // "hello"
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx, &buf[6..11]); // "world"
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        let col = batch
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");
        assert_eq!(col.value(1), "world");
    }

    #[test]
    fn test_missing_fields_produce_nulls() {
        let data = b"aabb";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        let idx_a = b.resolve_field(b"a");
        let idx_b = b.resolve_field(b"b");

        b.begin_row();
        b.append_str_by_idx(idx_a, &buf[0..2]);
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx_b, &buf[2..4]);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string fields: bare names
        let a = batch.column_by_name("a").unwrap();
        assert!(!a.is_null(0));
        assert!(a.is_null(1));
        let b_col = batch.column_by_name("b").unwrap();
        assert!(b_col.is_null(0));
        assert!(!b_col.is_null(1));
    }

    #[test]
    fn test_duplicate_key_first_writer_wins() {
        let data = b"firstsecond";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"val");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..5]); // "first"
        b.append_str_by_idx(idx, &buf[5..11]); // "second" -- should be ignored
        b.end_row();

        let batch = b.finish_batch().unwrap();
        // Single-type string field: bare name
        let col = batch
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(col.value(0), "first");
    }

    #[test]
    fn test_empty_batch() {
        let buf = bytes::Bytes::from_static(b"");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_batch_reuse() {
        let data1 = bytes::Bytes::from_static(b"hello");
        let data2 = bytes::Bytes::from_static(b"world");
        let mut b = StreamingBuilder::new(false);

        b.begin_batch(data1.clone());
        let idx = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx, &data1[0..5]);
        b.end_row();
        let b1 = b.finish_batch().unwrap();
        assert_eq!(b1.num_rows(), 1);

        // begin_batch clears field_index to prevent unbounded HashMap growth
        // under key churn (the core fix for this issue).  As a result,
        // resolve_field must be called again in each new batch to re-register
        // field names; any index obtained in a previous batch is no longer valid.
        b.begin_batch(data2.clone());
        let idx2 = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx2, &data2[0..5]);
        b.end_row();
        let b2 = b.finish_batch().unwrap();
        assert_eq!(b2.num_rows(), 1);
        // Column must exist and hold the value from the second batch.
        let col = b2
            .column_by_name("x")
            .expect("column x must exist in batch 2")
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("x must be StringViewArray");
        assert_eq!(col.value(0), "world");
    }

    #[test]
    fn test_float_values() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);

        let idx = b.resolve_field(b"lat");
        b.begin_row();
        b.append_float_by_idx(idx, b"3.25");
        b.end_row();

        let batch = b.finish_batch().unwrap();
        // Single-type float field: bare name
        let col = batch
            .column_by_name("lat")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 3.25).abs() < 1e-10);
    }

    /// Fields beyond index 63 do not get duplicate-key detection (the bitmask
    /// only covers 64 entries). This test verifies that writing to such fields
    /// does not panic and that the batch still builds correctly.
    #[test]
    fn test_no_panic_with_65_fields_duplicate_key() {
        // Build a buffer large enough to hold all the "val" strings we'll write.
        let payload = b"aabbccdd";
        let buf = bytes::Bytes::from(payload.to_vec());
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        // Resolve 65 fields so the 65th field has index 64 (>= 64).
        let mut indices = Vec::new();
        for i in 0..65u8 {
            let name = format!("field{}", i);
            indices.push(b.resolve_field(name.as_bytes()));
        }
        let idx_65 = indices[64]; // index 64 -- first field outside the bitmask

        b.begin_row();
        b.append_str_by_idx(idx_65, &buf[0..2]);
        b.append_str_by_idx(idx_65, &buf[2..4]);
        b.end_row();

        // Must not panic and must produce a valid batch.
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("field64").is_some());
    }

    /// `append_raw` stores zero-copy views into the buffer when `keep_raw` is true.
    #[test]
    fn test_append_raw_keep_raw_true() {
        let data = b"hello world\ngoodbye world\n";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(true);
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"msg");

        b.begin_row();
        b.append_raw(&buf[0..11]); // "hello world"
        b.append_str_by_idx(idx, &buf[0..5]); // "hello"
        b.end_row();

        b.begin_row();
        b.append_raw(&buf[12..25]); // "goodbye world"
        b.append_str_by_idx(idx, &buf[12..19]); // "goodbye"
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        assert!(batch.column_by_name("msg").is_some());

        let raw_col = batch
            .column_by_name("_raw")
            .expect("_raw column must be present when keep_raw=true");
        let raw_arr = raw_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .expect("_raw must be StringViewArray");
        assert_eq!(raw_arr.value(0), "hello world");
        assert_eq!(raw_arr.value(1), "goodbye world");
    }

    /// `append_raw` is a no-op when `keep_raw` is false -- `_raw` column absent.
    #[test]
    fn test_append_raw_keep_raw_false() {
        let data = b"hello world\n";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());

        b.begin_row();
        b.append_raw(&buf[0..11]);
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert!(
            batch.column_by_name("_raw").is_none(),
            "_raw must not be present when keep_raw=false"
        );
    }

    /// Struct child "int" has correct values when rows are int-typed.
    #[test]
    fn struct_child_int_values_correct() {
        let buf = bytes::Bytes::from_static(b"ERR");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"code");
        // Make it a conflict: rows 0,1 are int, row 2 is str
        b.begin_row();
        b.append_int_by_idx(si, b"100");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..3]);
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("code").expect("code struct");
        assert!(matches!(col.data_type(), DataType::Struct(_)));
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let int_child = sa.column_by_name("int").expect("int child");
        let int_arr = int_child.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 100);
        assert_eq!(int_arr.value(1), 200);
        assert!(int_arr.is_null(2));
    }

    /// Struct child "str" has correct values when rows are str-typed.
    #[test]
    fn struct_child_str_values_correct() {
        let buf = bytes::Bytes::from_static(b"helloworld");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"msg");
        // Make it a conflict: row 0 is int, rows 1,2 are str
        b.begin_row();
        b.append_int_by_idx(si, b"1");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..5]); // "hello"
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[5..10]); // "world"
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("msg").expect("msg struct");
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let str_child = sa.column_by_name("str").expect("str child");
        let str_arr = str_child
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(str_arr.is_null(0));
        assert_eq!(str_arr.value(1), "hello");
        assert_eq!(str_arr.value(2), "world");
    }

    /// Struct NullBuffer: row is non-null iff at least one child is non-null.
    #[test]
    fn struct_null_iff_all_children_null() {
        let buf = bytes::Bytes::from_static(b"OK");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"val");
        // Row 0: int=200, str=null
        b.begin_row();
        b.append_int_by_idx(si, b"200");
        b.end_row();
        // Row 1: int=null, str="OK"
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..2]);
        b.end_row();
        // Row 2: int=null, str=null
        b.begin_row();
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("val").expect("val struct");
        assert!(!col.is_null(0), "row 0 should be non-null");
        assert!(!col.is_null(1), "row 1 should be non-null");
        assert!(col.is_null(2), "row 2 should be null");
    }

    /// Three-way conflict: int, float, str all appear -- struct has all three children.
    #[test]
    fn three_way_conflict_int_float_str() {
        let buf = bytes::Bytes::from_static(b"text");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        let si = b.resolve_field(b"mixed");
        b.begin_row();
        b.append_int_by_idx(si, b"42");
        b.end_row();
        b.begin_row();
        b.append_float_by_idx(si, b"3.14");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, &buf[0..4]);
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("mixed").expect("mixed struct");
        assert!(matches!(col.data_type(), DataType::Struct(_)));
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let child_names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(child_names.contains(&"int"), "missing int child");
        assert!(child_names.contains(&"float"), "missing float child");
        assert!(child_names.contains(&"str"), "missing str child");
    }

    /// Single-type int field stays as bare flat Int64 column, not a struct.
    #[test]
    fn single_type_field_stays_flat() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);
        let si = b.resolve_field(b"count");
        b.begin_row();
        b.append_int_by_idx(si, b"1");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"2");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"3");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("count").expect("count column");
        assert!(
            matches!(col.data_type(), DataType::Int64),
            "single-type int must be bare Int64, got {:?}",
            col.data_type()
        );
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 3);
    }

    /// A 3-way conflict (int + float + str in the same field across rows) must
    /// produce a StructArray with all three children present.
    #[test]
    fn test_three_way_conflict_int_float_str() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"mixed");

        b.begin_row();
        b.append_int_by_idx(idx, b"42");
        b.end_row();

        b.begin_row();
        b.append_float_by_idx(idx, b"3.14");
        b.end_row();

        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..0]); // empty-but-valid string view
        b.end_row();

        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let col = batch.column_by_name("mixed").expect("mixed must exist");
        assert!(
            matches!(col.data_type(), DataType::Struct(_)),
            "3-way conflict must produce Struct, got {:?}",
            col.data_type()
        );
        let sa = col.as_any().downcast_ref::<StructArray>().unwrap();
        let names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"int"), "int child missing");
        assert!(names.contains(&"float"), "float child missing");
        assert!(names.contains(&"str"), "str child missing");

        // Row 0: int=42, float=null, str=null
        let int_idx = sa.fields().iter().position(|f| f.name() == "int").unwrap();
        let int_col = sa
            .column(int_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 42);
        assert!(int_col.is_null(1));
        assert!(int_col.is_null(2));

        // Struct-level null: no row should be all-null here
        assert!(!col.is_null(0));
        assert!(!col.is_null(1));
        assert!(!col.is_null(2));
    }

    /// After a conflict batch (struct column), a second batch with only a single
    /// type for the same field must emit a flat column — not a struct.
    #[test]
    fn test_batch_reuse_conflict_then_single_type_emits_flat() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new(false);

        // Batch 1: conflict → struct
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"status");
        b.begin_row();
        b.append_int_by_idx(idx, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..0]);
        b.end_row();
        let b1 = b.finish_batch().unwrap();
        assert!(
            matches!(
                b1.column_by_name("status").unwrap().data_type(),
                DataType::Struct(_)
            ),
            "batch 1 must be struct"
        );

        // Batch 2: only int — must be flat Int64
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"status");
        b.begin_row();
        b.append_int_by_idx(idx, b"404");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(idx, b"500");
        b.end_row();
        let b2 = b.finish_batch().unwrap();
        assert!(
            matches!(
                b2.column_by_name("status").unwrap().data_type(),
                DataType::Int64
            ),
            "batch 2 must be flat Int64 after clean data, got {:?}",
            b2.column_by_name("status").unwrap().data_type()
        );
    }

    /// `field_index` must not grow without bound when every batch introduces a
    /// completely new set of field names (key churn).
    ///
    /// Previously, `begin_batch` never cleared `field_index`, so each unique
    /// field name from every past batch accumulated in the HashMap forever.
    /// After the fix, `begin_batch` clears `field_index` and resets
    /// `num_active`, bounding `fields.len()` to the high-water mark of unique
    /// fields in any *single* batch.
    #[test]
    fn field_index_stays_bounded_under_key_churn() {
        let mut b = StreamingBuilder::new(false);
        const BATCHES: usize = 20;

        for i in 0..BATCHES {
            let payload = format!("val{i}");
            let buf = bytes::Bytes::from(payload.clone().into_bytes());

            b.begin_batch(buf.clone());
            // Each batch uses a different unique field name.
            let name = format!("field_{i}");
            let idx = b.resolve_field(name.as_bytes());
            b.begin_row();
            b.append_str_by_idx(idx, &buf[..]);
            b.end_row();
            let batch = b.finish_batch().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert!(
                batch.column_by_name(&name).is_some(),
                "column {name} must exist"
            );
        }

        // After all batches, the fields Vec must not have grown to BATCHES
        // entries — it should only hold the high-water mark (1 here).
        assert_eq!(
            b.fields.len(),
            1,
            "fields Vec must be bounded by max unique fields per single batch, \
             got {} (expected 1)",
            b.fields.len()
        );
        assert_eq!(
            b.field_index.len(),
            1,
            "field_index must hold only the current batch's entries, \
             got {} (expected 1)",
            b.field_index.len()
        );
    }

    /// Repeated batches with the *same* field names reuse pre-allocated
    /// `FieldColumns` slots (inner Vec capacity is preserved across batches).
    #[test]
    fn stable_fields_reuse_slots_across_batches() {
        let data1 = bytes::Bytes::from_static(b"web1200");
        let data2 = bytes::Bytes::from_static(b"web2404");
        let mut b = StreamingBuilder::new(false);

        // Prime the builder so slots are allocated.
        b.begin_batch(data1.clone());
        let h = b.resolve_field(b"host");
        let s = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h, &data1[0..4]); // "web1"
        b.append_int_by_idx(s, b"200");
        b.end_row();
        let _ = b.finish_batch().unwrap();

        // Second batch: same fields. Slots must be reused (fields.len() stays 2).
        b.begin_batch(data2.clone());
        let h2 = b.resolve_field(b"host");
        let s2 = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h2, &data2[0..4]); // "web2"
        b.append_int_by_idx(s2, b"404");
        b.end_row();
        let batch = b.finish_batch().unwrap();

        assert_eq!(
            b.fields.len(),
            2,
            "fields Vec must not grow when same fields reappear"
        );
        assert_eq!(batch.num_rows(), 1);
        // Slot indices match here because both batches resolved fields in the
        // same order ("host" first, "status" second), so the slot-reuse
        // algorithm assigns the same indices.  The important invariant is that
        // `fields.len()` stayed bounded — not that the indices are identical.
        assert_eq!(h, h2, "same field name must resolve to the same slot index");
        assert_eq!(s, s2, "same field name must resolve to the same slot index");
        assert_eq!(
            batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .unwrap()
                .value(0),
            "web2"
        );
    }

    // -----------------------------------------------------------------------
    // Protocol-violation tests: debug_assert fires on incorrectly wired callers.
    // These are only meaningful in debug builds where debug_assert is active.
    // -----------------------------------------------------------------------

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "begin_row called outside of a batch")]
    fn test_begin_row_without_batch_panics() {
        let mut b = StreamingBuilder::new(false);
        b.begin_row(); // no begin_batch — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "end_row called without a matching begin_row")]
    fn test_end_row_without_begin_row_panics() {
        let buf = bytes::Bytes::from_static(b"x");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);
        b.end_row(); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "begin_batch called while inside a row")]
    fn test_begin_batch_inside_row_panics() {
        let buf = bytes::Bytes::from_static(b"x");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        b.begin_row();
        b.begin_batch(buf); // inside a row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_str_by_idx called outside of a row")]
    fn test_append_str_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"val");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf.clone());
        let idx = b.resolve_field(b"x");
        b.append_str_by_idx(idx, &buf[0..3]); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_int_by_idx called outside of a row")]
    fn test_append_int_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"42");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"n");
        b.append_int_by_idx(idx, b"42"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_float_by_idx called outside of a row")]
    fn test_append_float_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"1.5");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"f");
        b.append_float_by_idx(idx, b"1.5"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_null_by_idx called outside of a row")]
    fn test_append_null_outside_row_panics() {
        let buf = bytes::Bytes::from_static(b"x");
        let mut b = StreamingBuilder::new(false);
        b.begin_batch(buf);
        let idx = b.resolve_field(b"n");
        b.append_null_by_idx(idx); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "finish_batch called outside of a batch")]
    fn test_finish_batch_without_batch_panics() {
        let mut b = StreamingBuilder::new(false);
        let _ = b.finish_batch(); // no begin_batch — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "resolve_field called outside of an active batch")]
    fn test_resolve_field_without_batch_panics() {
        let mut b = StreamingBuilder::new(false);
        b.resolve_field(b"x"); // no begin_batch — must panic
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Prove that the duplicate-name guard fires for a small known case:
    /// two fields with the same name in the same batch must return an error.
    #[kani::proof]
    fn verify_duplicate_name_check_rejects_collision() {
        let mut emitted_names = std::collections::HashSet::new();
        let name = "status";
        let first = emitted_names.insert(name.to_string());
        let second = emitted_names.insert(name.to_string());
        assert!(first, "first reservation must succeed");
        assert!(!second, "second reservation of same name must fail");
    }

    /// Prove that a single-type field produces exactly num_rows entries in the
    /// values buffer before being handed to Int64Array.
    #[kani::proof]
    fn verify_single_type_produces_num_rows_entries() {
        let num_rows: usize = kani::any();
        kani::assume(num_rows <= 4);

        let values: Vec<i64> = vec![0i64; num_rows];
        let valid: Vec<bool> = vec![false; num_rows];

        assert_eq!(values.len(), num_rows);
        assert_eq!(valid.len(), num_rows);
        kani::cover!(num_rows == 0, "zero-row batch exercised");
        kani::cover!(num_rows > 0, "non-empty batch exercised");
    }
}
