// streaming_builder.rs — Zero-copy hot-path builder.
//
// Uses Arrow's StringViewArray to store string values as 16-byte views
// pointing directly into the input buffer. No string copies during scanning
// or during finish_batch. The input buffer (as bytes::Bytes) is reference-counted
// and shared between the builder and the resulting Arrow arrays.
//
// For the hot pipeline path: scan → query → output → discard.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringViewBuilder};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use logfwd_core::scan_config::{parse_float_fast, parse_int_fast};

use crate::check_dup_bits;

// ---------------------------------------------------------------------------
// Per-field state
// ---------------------------------------------------------------------------

struct FieldColumns {
    name: Vec<u8>,
    /// String values: (row, offset_in_buffer, len). Views into the shared buffer.
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
/// resulting Arrow `StringViewArray` columns — no string copies at any point.
///
/// Numeric values (int, float) are parsed during scanning and stored directly.
///
/// # Usage
/// ```ignore
/// let mut builder = StreamingBuilder::new();
/// builder.begin_batch(bytes::Bytes::from(buf));
/// // ... scan fields, call append_*_by_idx ...
/// let batch = builder.finish_batch();
/// ```
pub struct StreamingBuilder {
    fields: Vec<FieldColumns>,
    field_index: HashMap<Vec<u8>, usize>,
    row_count: u32,
    /// Tracks which fields (by index) were written in the current row.
    /// Only covers the first 64 fields (indices 0–63); see `check_dup_bits`.
    written_bits: u64,
    /// Reference-counted buffer. Stored here to compute offsets safely
    /// and shared with Arrow StringViewArrays in finish_batch.
    buf: bytes::Bytes,
}

impl Default for StreamingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingBuilder {
    pub fn new() -> Self {
        StreamingBuilder {
            fields: Vec::with_capacity(32),
            field_index: HashMap::with_capacity(32),
            row_count: 0,
            written_bits: 0,
            buf: bytes::Bytes::new(),
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
        debug_assert!(
            buf.len() <= u32::MAX as usize,
            "StreamingBuilder buffer too large for u32 offsets ({} bytes)",
            buf.len()
        );
        self.buf = buf;
        self.row_count = 0;
        for fc in &mut self.fields {
            fc.clear();
        }
    }

    #[inline(always)]
    pub fn begin_row(&mut self) {
        self.written_bits = 0;
    }

    #[inline(always)]
    pub fn end_row(&mut self) {
        self.row_count = self
            .row_count
            .checked_add(1)
            .expect("row_count overflow: batch exceeds u32::MAX rows");
    }

    #[inline]
    pub fn resolve_field(&mut self, key: &[u8]) -> usize {
        if let Some(&idx) = self.field_index.get(key) {
            return idx;
        }
        let idx = self.fields.len();
        self.fields.push(FieldColumns::new(key));
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
            offset <= u32::MAX as usize,
            "StreamingBuilder buffer offset exceeds u32::MAX ({offset} bytes)"
        );
        offset as u32
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
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

    #[inline(always)]
    pub fn append_int_by_idx(&mut self, idx: usize, value: &[u8]) {
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
        // Nulls are represented by gaps — no value record needed.
        // But mark as written for duplicate-key detection.
        let _ = check_dup_bits(&mut self.written_bits, idx);
    }

    /// No-op: StreamingBuilder does not support _raw column.
    /// Use StorageBuilder (SimdScanner) if keep_raw is needed.
    pub fn append_raw(&mut self, _line: &[u8]) {}

    /// Build a RecordBatch with zero-copy StringViewArrays.
    ///
    /// The resulting RecordBatch shares the input buffer via Bytes reference
    /// counting — no string data is copied.
    pub fn finish_batch(&self) -> Result<RecordBatch, ArrowError> {
        let num_rows = self.row_count as usize;
        let arrow_buf = Buffer::from(self.buf.clone());

        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.fields.len());

        for fc in &self.fields {
            // Field names come from JSON keys (valid UTF-8 in well-formed input).
            // Use from_utf8_lossy so that fuzz inputs with arbitrary bytes are
            // handled gracefully instead of triggering undefined behaviour.
            let name = String::from_utf8_lossy(&fc.name);

            if fc.has_int {
                let col_name = format!("{}_int", name);
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
                schema_fields.push(Field::new(col_name, DataType::Int64, true));
                arrays.push(Arc::new(array) as ArrayRef);
            }

            if fc.has_float {
                let col_name = format!("{}_float", name);
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
                schema_fields.push(Field::new(col_name, DataType::Float64, true));
                arrays.push(Arc::new(array) as ArrayRef);
            }

            if fc.has_str {
                let col_name = format!("{}_str", name);
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

                schema_fields.push(Field::new(col_name, DataType::Utf8View, true));
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(schema, arrays, &opts)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn test_basic_string_and_int() {
        let json = br#"not used directly"#;
        let buf = bytes::Bytes::from(json.to_vec());
        let mut b = StreamingBuilder::new();
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
        assert!(batch.column_by_name("name_str").is_some());
        assert!(batch.column_by_name("age_int").is_some());
    }

    #[test]
    fn test_zero_copy_string_content() {
        let data = b"hello world foobar";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new();
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
        // The values should be correct even though they're views into the buffer
        let col = batch
            .column_by_name("msg_str")
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
        let mut b = StreamingBuilder::new();
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
        let a = batch.column_by_name("a_str").unwrap();
        assert!(!a.is_null(0));
        assert!(a.is_null(1));
        let b_col = batch.column_by_name("b_str").unwrap();
        assert!(b_col.is_null(0));
        assert!(!b_col.is_null(1));
    }

    #[test]
    fn test_duplicate_key_first_writer_wins() {
        let data = b"firstsecond";
        let buf = bytes::Bytes::from(data.to_vec());
        let mut b = StreamingBuilder::new();
        b.begin_batch(buf.clone());

        let idx = b.resolve_field(b"val");
        b.begin_row();
        b.append_str_by_idx(idx, &buf[0..5]); // "first"
        b.append_str_by_idx(idx, &buf[5..11]); // "second" — should be ignored
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch
            .column_by_name("val_str")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(col.value(0), "first");
    }

    #[test]
    fn test_empty_batch() {
        let buf = bytes::Bytes::from_static(b"");
        let mut b = StreamingBuilder::new();
        b.begin_batch(buf);
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_batch_reuse() {
        let data1 = bytes::Bytes::from_static(b"hello");
        let data2 = bytes::Bytes::from_static(b"world");
        let mut b = StreamingBuilder::new();

        b.begin_batch(data1.clone());
        let idx = b.resolve_field(b"x");
        b.begin_row();
        b.append_str_by_idx(idx, &data1[0..5]);
        b.end_row();
        let b1 = b.finish_batch().unwrap();
        assert_eq!(b1.num_rows(), 1);

        b.begin_batch(data2.clone());
        b.begin_row();
        b.append_str_by_idx(idx, &data2[0..5]);
        b.end_row();
        let b2 = b.finish_batch().unwrap();
        assert_eq!(b2.num_rows(), 1);
    }

    #[test]
    fn test_float_values() {
        let buf = bytes::Bytes::from_static(b"unused");
        let mut b = StreamingBuilder::new();
        b.begin_batch(buf);

        let idx = b.resolve_field(b"lat");
        b.begin_row();
        b.append_float_by_idx(idx, b"3.25");
        b.end_row();

        let batch = b.finish_batch().unwrap();
        let col = batch
            .column_by_name("lat_float")
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
        let mut b = StreamingBuilder::new();
        b.begin_batch(buf.clone());

        // Resolve 65 fields so the 65th field has index 64 (>= 64).
        let mut indices = Vec::new();
        for i in 0..65u8 {
            let name = format!("field{}", i);
            indices.push(b.resolve_field(name.as_bytes()));
        }
        let idx_65 = indices[64]; // index 64 — first field outside the bitmask

        b.begin_row();
        // Write the 65th field twice. Duplicate-key detection is not active for
        // idx >= 64, so both writes go through without panic. The second write
        // overwrites the first in the (row, offset, len) list, but that is the
        // documented behaviour for fields beyond the bitmask range.
        b.append_str_by_idx(idx_65, &buf[0..2]);
        b.append_str_by_idx(idx_65, &buf[2..4]);
        b.end_row();

        // Must not panic and must produce a valid batch.
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("field64_str").is_some());
    }
}
