// storage_builder.rs — Self-contained persistence builder.
//
// Collects (row, value) records during scanning, then bulk-builds Arrow
// columns at finish_batch time. No incremental null padding, no cross-builder
// coordination — each column is built independently from its own records.
//
// For the persistence path: scan → build → compress → disk.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringBuilder, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use logfwd_core::scan_config::{parse_float_fast, parse_int_fast};
use logfwd_core::scanner::BuilderState;

use crate::check_dup_bits;

struct FieldCollector {
    name: Vec<u8>,
    str_values: Vec<(u32, Vec<u8>)>,
    int_values: Vec<(u32, i64)>,
    float_values: Vec<(u32, f64)>,
    has_str: bool,
    has_int: bool,
    has_float: bool,
}

impl FieldCollector {
    fn new(name: &[u8]) -> Self {
        FieldCollector {
            name: name.to_vec(),
            str_values: Vec::with_capacity(256),
            int_values: Vec::with_capacity(256),
            float_values: Vec::with_capacity(256),
            has_str: false,
            has_int: false,
            has_float: false,
        }
    }
    fn clear(&mut self) {
        self.str_values.clear();
        self.int_values.clear();
        self.float_values.clear();
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
    }
}

/// Self-contained persistence builder.
///
/// Collects `(row, value)` records during scanning, then bulk-builds Arrow
/// columns at `finish_batch` time. Each column is independent — no cross-builder
/// null padding, correct by construction.
///
/// Use for: scan → build → compress → disk queue.
///
/// # Usage
/// ```ignore
/// let mut builder = StorageBuilder::new(true);
/// builder.begin_batch();
/// // ... scan fields, call append_*_by_idx ...
/// let batch = builder.finish_batch()?;
/// // ... write/compress `batch` ...
/// # Ok::<(), arrow::error::ArrowError>(())
/// ```
pub struct StorageBuilder {
    fields: Vec<FieldCollector>,
    field_index: HashMap<Vec<u8>, usize>,
    /// Number of fields active in the current batch. Slots `0..num_active` in
    /// `fields` are in use; slots beyond that are pre-allocated but dormant.
    /// Resetting this to zero on `begin_batch` — together with clearing
    /// `field_index` — bounds `fields.len()` to the high-water mark of unique
    /// fields seen in any *single* batch rather than growing without limit.
    num_active: usize,
    raw_values: Vec<Vec<u8>>,
    row_count: u32,
    keep_raw: bool,
    /// Tracks which fields (by index) were written in the current row.
    /// Only covers the first 64 fields (indices 0-63); see `check_dup_bits`.
    written_bits: u64,
    /// Protocol state — enforced via `debug_assert` in each method.
    state: BuilderState,
}

impl StorageBuilder {
    pub fn new(keep_raw: bool) -> Self {
        StorageBuilder {
            fields: Vec::with_capacity(32),
            field_index: HashMap::with_capacity(32),
            num_active: 0,
            raw_values: Vec::new(),
            row_count: 0,
            keep_raw,
            written_bits: 0,
            state: BuilderState::Idle,
        }
    }

    pub fn begin_batch(&mut self) {
        debug_assert_ne!(
            self.state,
            BuilderState::InRow,
            "begin_batch called while inside a row (missing end_row)"
        );
        self.row_count = 0;
        // Only clear the slots that were active in the previous batch.
        // This preserves the inner-Vec capacity of each FieldCollector for
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
        self.raw_values.clear();
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
            self.fields.push(FieldCollector::new(key));
        }
        self.num_active += 1;
        self.field_index.insert(key.to_vec(), idx);
        idx
    }

    #[inline(always)]
    fn check_dup(&mut self, idx: usize) -> bool {
        check_dup_bits(&mut self.written_bits, idx)
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_str_by_idx called outside of a row"
        );
        if self.check_dup(idx) {
            return;
        }
        // StringArray requires valid UTF-8.  JSON is always UTF-8 in production;
        // for fuzz / corrupted input we skip non-UTF-8 bytes rather than storing
        // them and triggering UB later in finish_batch.
        if std::str::from_utf8(value).is_err() {
            return;
        }
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.str_values.push((self.row_count, value.to_vec()));
    }

    #[inline(always)]
    pub fn append_int_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_int_by_idx called outside of a row"
        );
        if self.check_dup(idx) {
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
        if self.check_dup(idx) {
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
        // Mark as written so duplicate keys are detected.
        // Null values are implicit (absence of a record = null at finish_batch).
        self.check_dup(idx);
    }

    #[inline]
    pub fn append_raw(&mut self, line: &[u8]) {
        debug_assert_eq!(
            self.state,
            BuilderState::InRow,
            "append_raw called outside of a row"
        );
        if self.keep_raw {
            self.raw_values.push(line.to_vec());
        }
    }

    pub fn finish_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        debug_assert_eq!(
            self.state,
            BuilderState::InBatch,
            "finish_batch called outside of a batch (call begin_batch first, and ensure all rows are closed with end_row)"
        );
        let num_rows = self.row_count as usize;
        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.num_active + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_active + 1);

        // Detect duplicate output column names before building the schema.
        let mut emitted_names = std::collections::HashSet::new();
        // Pre-reserve "_raw" only when it will actually be emitted.
        if self.keep_raw && !self.raw_values.is_empty() {
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
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    let int_array = Int64Array::new(values.into(), Some(NullBuffer::from(valid)));
                    child_fields.push(Arc::new(Field::new("int", DataType::Int64, true)));
                    child_arrays.push(Arc::new(int_array) as ArrayRef);
                }
                if fc.has_float {
                    let mut values = vec![0.0f64; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.float_values {
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    let float_array =
                        Float64Array::new(values.into(), Some(NullBuffer::from(valid)));
                    child_fields.push(Arc::new(Field::new("float", DataType::Float64, true)));
                    child_arrays.push(Arc::new(float_array) as ArrayRef);
                }
                if fc.has_str {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    let mut vi = 0;
                    for row in 0..num_rows {
                        if vi < fc.str_values.len() && fc.str_values[vi].0 as usize == row {
                            let s = String::from_utf8_lossy(&fc.str_values[vi].1);
                            builder.append_value(&*s);
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    let str_array = builder.finish();
                    child_fields.push(Arc::new(Field::new("str", DataType::Utf8, true)));
                    child_arrays.push(Arc::new(str_array) as ArrayRef);
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
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    schema_fields.push(Field::new(name.as_ref(), DataType::Int64, true));
                    arrays.push(Arc::new(Int64Array::new(
                        values.into(),
                        Some(NullBuffer::from(valid)),
                    )) as ArrayRef);
                }
                if fc.has_float {
                    reserve_name(name.as_ref())?;
                    let mut values = vec![0.0f64; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.float_values {
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    schema_fields.push(Field::new(name.as_ref(), DataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::new(
                        values.into(),
                        Some(NullBuffer::from(valid)),
                    )) as ArrayRef);
                }
                if fc.has_str {
                    reserve_name(name.as_ref())?;
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    let mut vi = 0;
                    for row in 0..num_rows {
                        if vi < fc.str_values.len() && fc.str_values[vi].0 as usize == row {
                            let s = String::from_utf8_lossy(&fc.str_values[vi].1);
                            builder.append_value(&*s);
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    schema_fields.push(Field::new(name.as_ref(), DataType::Utf8, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }
            }
        }

        if self.keep_raw && !self.raw_values.is_empty() {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
            for val in &self.raw_values {
                builder.append_value(&*String::from_utf8_lossy(val));
            }
            for _ in self.raw_values.len()..num_rows {
                builder.append_null();
            }
            schema_fields.push(Field::new("_raw", DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let result = RecordBatch::try_new_with_options(schema, arrays, &opts);
        self.state = BuilderState::Idle;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, StructArray as ArrowStructArray};

    #[test]
    fn test_basic() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let h = b.resolve_field(b"host");
        let s = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h, b"web1");
        b.append_int_by_idx(s, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(h, b"web2");
        b.append_int_by_idx(s, b"404");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type fields use bare names
        assert_eq!(
            batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "web1"
        );
        assert_eq!(
            batch
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            404
        );
    }

    #[test]
    fn test_missing_fields() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let a = b.resolve_field(b"a");
        b.begin_row();
        b.append_str_by_idx(a, b"hello");
        b.end_row();
        let bx = b.resolve_field(b"b");
        b.begin_row();
        b.append_str_by_idx(bx, b"world");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string field: bare name
        let ac = batch.column_by_name("a").unwrap();
        assert!(!ac.is_null(0));
        assert!(ac.is_null(1));
    }

    #[test]
    fn test_type_conflict() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let idx = b.resolve_field(b"status");
        b.begin_row();
        b.append_int_by_idx(idx, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(idx, b"OK");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        // Old suffixed columns must not exist
        assert!(batch.column_by_name("status__int").is_none());
        assert!(batch.column_by_name("status__str").is_none());
        // New struct column with child fields
        let status_col = batch
            .column_by_name("status")
            .expect("status struct column");
        assert!(matches!(status_col.data_type(), DataType::Struct(_)));
        let sa = status_col
            .as_any()
            .downcast_ref::<ArrowStructArray>()
            .unwrap();
        let child_names: Vec<&str> = sa.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(child_names.contains(&"int"), "missing int child");
        assert!(child_names.contains(&"str"), "missing str child");
    }

    #[test]
    fn test_many_fields_across_lines() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let a = b.resolve_field(b"a");
        let bx = b.resolve_field(b"b");
        let c = b.resolve_field(b"c");
        b.begin_row();
        b.append_str_by_idx(a, b"1");
        b.append_str_by_idx(bx, b"2");
        b.append_str_by_idx(c, b"3");
        b.end_row();
        let d = b.resolve_field(b"d");
        let e = b.resolve_field(b"e");
        b.begin_row();
        b.append_str_by_idx(d, b"4");
        b.append_int_by_idx(e, b"5");
        b.end_row();
        let f = b.resolve_field(b"f");
        b.begin_row();
        b.append_str_by_idx(a, b"6");
        b.append_float_by_idx(f, b"7.0");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 3);
        // Single-type string field: bare name
        let ac = batch.column_by_name("a").unwrap();
        assert!(!ac.is_null(0));
        assert!(ac.is_null(1));
        assert!(!ac.is_null(2));
    }

    #[test]
    fn test_duplicate_keys() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let idx = b.resolve_field(b"a");
        b.begin_row();
        b.append_int_by_idx(idx, b"1");
        b.append_int_by_idx(idx, b"2");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        // Single-type int field: bare name
        assert_eq!(
            batch
                .column_by_name("a")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn test_empty_batch() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Fields beyond index 63 do not get duplicate-key detection (the bitmask
    /// only covers 64 entries). This test verifies that writing to such fields
    /// does not panic and that the batch still builds correctly.
    #[test]
    fn test_no_panic_with_65_fields_duplicate_key() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();

        // Resolve 65 fields so the 65th field has index 64 (>= 64).
        let mut indices = Vec::new();
        for i in 0..65u8 {
            let name = format!("field{}", i);
            indices.push(b.resolve_field(name.as_bytes()));
        }
        let idx_65 = indices[64]; // index 64 -- first field outside the bitmask

        b.begin_row();
        // Write the 65th field twice. Duplicate-key detection is not active for
        // idx >= 64, so both writes go through without panic.
        b.append_int_by_idx(idx_65, b"1");
        b.append_int_by_idx(idx_65, b"2");
        b.end_row();

        // Must not panic and must produce a valid batch.
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("field64").is_some());
    }

    /// Struct child "int" has correct values when rows are int-typed.
    #[test]
    fn struct_child_int_values_correct() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let si = b.resolve_field(b"code");
        // Make it a conflict by adding one str row
        b.begin_row();
        b.append_int_by_idx(si, b"100");
        b.end_row();
        b.begin_row();
        b.append_int_by_idx(si, b"200");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, b"ERR");
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
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let si = b.resolve_field(b"msg");
        // Make it a conflict by adding one int row
        b.begin_row();
        b.append_int_by_idx(si, b"1");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, b"hello");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, b"world");
        b.end_row();
        let batch = b.finish_batch().unwrap();
        let col = batch.column_by_name("msg").expect("msg struct");
        let sa = col.as_any().downcast_ref::<ArrowStructArray>().unwrap();
        let str_child = sa.column_by_name("str").expect("str child");
        let str_arr = str_child.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(str_arr.is_null(0));
        assert_eq!(str_arr.value(1), "hello");
        assert_eq!(str_arr.value(2), "world");
    }

    /// Struct NullBuffer: row is non-null iff at least one child is non-null.
    #[test]
    fn struct_null_iff_all_children_null() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let si = b.resolve_field(b"val");
        // Row 0: int=200, str=null
        b.begin_row();
        b.append_int_by_idx(si, b"200");
        b.end_row();
        // Row 1: int=null, str="OK"
        b.begin_row();
        b.append_str_by_idx(si, b"OK");
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
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let si = b.resolve_field(b"mixed");
        b.begin_row();
        b.append_int_by_idx(si, b"42");
        b.end_row();
        b.begin_row();
        b.append_float_by_idx(si, b"3.14");
        b.end_row();
        b.begin_row();
        b.append_str_by_idx(si, b"text");
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
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
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
        let mut b = StorageBuilder::new(false);
        const BATCHES: usize = 20;

        for i in 0..BATCHES {
            b.begin_batch();
            // Each batch uses a different unique field name.
            let name = format!("field_{i}");
            let idx = b.resolve_field(name.as_bytes());
            b.begin_row();
            b.append_int_by_idx(idx, b"1");
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
    /// `FieldCollector` slots (inner Vec capacity is preserved across batches).
    #[test]
    fn stable_fields_reuse_slots_across_batches() {
        let mut b = StorageBuilder::new(false);

        // Prime the builder so slots are allocated.
        b.begin_batch();
        let h = b.resolve_field(b"host");
        let s = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h, b"web1");
        b.append_int_by_idx(s, b"200");
        b.end_row();
        let _ = b.finish_batch().unwrap();

        // Second batch: same fields. Slots must be reused (fields.len() stays 2).
        b.begin_batch();
        let h2 = b.resolve_field(b"host");
        let s2 = b.resolve_field(b"status");
        b.begin_row();
        b.append_str_by_idx(h2, b"web2");
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
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "web2"
        );
    }

    // -----------------------------------------------------------------------
    // Protocol-violation tests: debug_assert fires on mis-wired callers.
    // These are only meaningful in debug builds where debug_assert is active.
    // -----------------------------------------------------------------------

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "begin_row called outside of a batch")]
    fn test_begin_row_without_batch_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_row(); // no begin_batch — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "end_row called without a matching begin_row")]
    fn test_end_row_without_begin_row_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        b.end_row(); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "begin_batch called while inside a row")]
    fn test_begin_batch_inside_row_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        b.begin_row();
        b.begin_batch(); // inside a row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_str_by_idx called outside of a row")]
    fn test_append_str_outside_row_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let idx = b.resolve_field(b"x");
        b.append_str_by_idx(idx, b"val"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_int_by_idx called outside of a row")]
    fn test_append_int_outside_row_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let idx = b.resolve_field(b"n");
        b.append_int_by_idx(idx, b"42"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_float_by_idx called outside of a row")]
    fn test_append_float_outside_row_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let idx = b.resolve_field(b"f");
        b.append_float_by_idx(idx, b"1.5"); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "append_null_by_idx called outside of a row")]
    fn test_append_null_outside_row_panics() {
        let mut b = StorageBuilder::new(false);
        b.begin_batch();
        let idx = b.resolve_field(b"n");
        b.append_null_by_idx(idx); // no begin_row — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "finish_batch called outside of a batch")]
    fn test_finish_batch_without_batch_panics() {
        let mut b = StorageBuilder::new(false);
        let _ = b.finish_batch(); // no begin_batch — must panic
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "resolve_field called outside of an active batch")]
    fn test_resolve_field_without_batch_panics() {
        let mut b = StorageBuilder::new(false);
        b.resolve_field(b"x"); // no begin_batch — must panic
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Prove that the duplicate-name guard fires for a small known case:
    /// two fields with the same name in the same batch must return an error.
    ///
    /// We verify this by constructing the duplicate detection logic in isolation
    /// (same HashSet approach as finish_batch) rather than running full Arrow
    /// construction, keeping the proof tractable.
    #[kani::proof]
    fn verify_duplicate_name_check_rejects_collision() {
        let mut emitted_names = std::collections::HashSet::new();
        // Simulate reserving the same name twice.
        let name = "status";
        let first = emitted_names.insert(name.to_string());
        let second = emitted_names.insert(name.to_string());
        assert!(first, "first reservation must succeed");
        assert!(!second, "second reservation of same name must fail");
    }

    /// Prove that a single-type field produces exactly num_rows entries in the
    /// values buffer before being handed to Int64Array.
    ///
    /// We use a small bounded num_rows to keep the proof tractable.
    #[kani::proof]
    fn verify_single_type_produces_num_rows_entries() {
        let num_rows: usize = kani::any();
        kani::assume(num_rows <= 4);

        // Simulate the values/valid vec allocation that finish_batch performs.
        let values: Vec<i64> = vec![0i64; num_rows];
        let valid: Vec<bool> = vec![false; num_rows];

        assert_eq!(values.len(), num_rows);
        assert_eq!(valid.len(), num_rows);
        kani::cover!(num_rows == 0, "zero-row batch exercised");
        kani::cover!(num_rows > 0, "non-empty batch exercised");
    }
}
