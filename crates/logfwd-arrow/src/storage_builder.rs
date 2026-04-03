// storage_builder.rs — Self-contained persistence builder.
//
// Collects (row, value) records during scanning, then bulk-builds Arrow
// columns at finish_batch time. No incremental null padding, no cross-builder
// coordination — each column is built independently from its own records.
//
// For the persistence path: scan → build → compress → disk.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringBuilder};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

/// Arrow schema metadata key used to record conflict groups.
/// Format: semicolon-separated `base:type1,type2` entries, e.g. `"status:int,str"`.
pub const CONFLICT_GROUPS_METADATA_KEY: &str = "logfwd.conflict_groups";

use logfwd_core::scan_config::{parse_float_fast, parse_int_fast};

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
    raw_values: Vec<Vec<u8>>,
    row_count: u32,
    keep_raw: bool,
    /// Tracks which fields (by index) were written in the current row.
    /// Only covers the first 64 fields (indices 0–63); see `check_dup_bits`.
    written_bits: u64,
}

impl StorageBuilder {
    pub fn new(keep_raw: bool) -> Self {
        StorageBuilder {
            fields: Vec::with_capacity(32),
            field_index: HashMap::with_capacity(32),
            raw_values: Vec::new(),
            row_count: 0,
            keep_raw,
            written_bits: 0,
        }
    }

    pub fn begin_batch(&mut self) {
        self.row_count = 0;
        for fc in &mut self.fields {
            fc.clear();
        }
        self.raw_values.clear();
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
        self.fields.push(FieldCollector::new(key));
        self.field_index.insert(key.to_vec(), idx);
        idx
    }

    #[inline(always)]
    fn check_dup(&mut self, idx: usize) -> bool {
        check_dup_bits(&mut self.written_bits, idx)
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
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
        // Mark as written so duplicate keys are detected.
        // Null values are implicit (absence of a record = null at finish_batch).
        self.check_dup(idx);
    }

    #[inline]
    pub fn append_raw(&mut self, line: &[u8]) {
        if self.keep_raw {
            self.raw_values.push(line.to_vec());
        }
    }

    pub fn finish_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        let num_rows = self.row_count as usize;
        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.fields.len() + 1);
        // Accumulate conflict group descriptions for schema metadata.
        let mut conflict_meta: Vec<String> = Vec::new();

        // Detect duplicate output column names before building the schema.
        let mut emitted_names = std::collections::HashSet::new();
        // Pre-reserve "_raw" only when it will actually be emitted — same condition
        // as the raw column construction below (!self.raw_values.is_empty()). This
        // prevents a user field named "_raw" from colliding with the reserved column.
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

        for fc in &self.fields {
            // Field names come from JSON keys (valid UTF-8 in well-formed input).
            // Use from_utf8_lossy so that fuzz inputs with arbitrary bytes are
            // handled gracefully instead of triggering undefined behaviour.
            let name = String::from_utf8_lossy(&fc.name);

            // Suffix columns only when the same field has multiple types in this
            // batch. Single-type fields use the bare field name.
            let conflict = (fc.has_int as u8) + (fc.has_float as u8) + (fc.has_str as u8) > 1;

            if conflict {
                // Record this group for schema metadata.
                let mut types = Vec::with_capacity(3);
                if fc.has_int {
                    types.push("int");
                }
                if fc.has_float {
                    types.push("float");
                }
                if fc.has_str {
                    types.push("str");
                }
                conflict_meta.push(format!("{}:{}", name, types.join(",")));
            }

            if fc.has_int {
                let col_name = if conflict {
                    format!("{}__int", name)
                } else {
                    name.to_string()
                };
                reserve_name(&col_name)?;
                let mut values = vec![0i64; num_rows];
                let mut valid = vec![false; num_rows];
                for &(row, v) in &fc.int_values {
                    let r = row as usize;
                    if r < num_rows {
                        values[r] = v;
                        valid[r] = true;
                    }
                }
                schema_fields.push(Field::new(col_name, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::new(
                    values.into(),
                    Some(NullBuffer::from(valid)),
                )) as ArrayRef);
            }
            if fc.has_float {
                let col_name = if conflict {
                    format!("{}__float", name)
                } else {
                    name.to_string()
                };
                reserve_name(&col_name)?;
                let mut values = vec![0.0f64; num_rows];
                let mut valid = vec![false; num_rows];
                for &(row, v) in &fc.float_values {
                    let r = row as usize;
                    if r < num_rows {
                        values[r] = v;
                        valid[r] = true;
                    }
                }
                schema_fields.push(Field::new(col_name, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::new(
                    values.into(),
                    Some(NullBuffer::from(valid)),
                )) as ArrayRef);
            }
            if fc.has_str {
                let col_name = if conflict {
                    format!("{}__str", name)
                } else {
                    name.to_string()
                };
                reserve_name(&col_name)?;
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                let mut vi = 0;
                for row in 0..num_rows {
                    if vi < fc.str_values.len() && fc.str_values[vi].0 as usize == row {
                        // String values come from the JSON input.  Use
                        // from_utf8_lossy so that non-UTF-8 fuzz input is
                        // handled safely (replacement characters) rather than
                        // invoking undefined behaviour.
                        let s = String::from_utf8_lossy(&fc.str_values[vi].1);
                        builder.append_value(&*s);
                        vi += 1;
                    } else {
                        builder.append_null();
                    }
                }
                schema_fields.push(Field::new(col_name, DataType::Utf8, true));
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
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

        let schema = if conflict_meta.is_empty() {
            Arc::new(Schema::new(schema_fields))
        } else {
            let mut meta = HashMap::new();
            meta.insert(
                CONFLICT_GROUPS_METADATA_KEY.to_string(),
                conflict_meta.join(";"),
            );
            Arc::new(Schema::new_with_metadata(schema_fields, meta))
        };
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(schema, arrays, &opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};

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
        assert!(batch.column_by_name("status__int").is_some());
        assert!(batch.column_by_name("status__str").is_some());
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
        let idx_65 = indices[64]; // index 64 — first field outside the bitmask

        b.begin_row();
        // Write the 65th field twice. Duplicate-key detection is not active for
        // idx >= 64, so both writes go through without panic. The documented
        // first-writer-wins guarantee only holds for fields 0–63.
        b.append_int_by_idx(idx_65, b"1");
        b.append_int_by_idx(idx_65, b"2");
        b.end_row();

        // Must not panic and must produce a valid batch.
        let batch = b.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("field64").is_some());
    }
}
