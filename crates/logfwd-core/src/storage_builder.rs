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
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::scan_config::{parse_float_fast, parse_int_fast};

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
pub struct StorageBuilder {
    fields: Vec<FieldCollector>,
    field_index: HashMap<Vec<u8>, usize>,
    raw_values: Vec<Vec<u8>>,
    row_count: u32,
    keep_raw: bool,
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
        self.row_count += 1;
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
        if idx < 64 {
            let bit = 1u64 << idx;
            if self.written_bits & bit != 0 {
                return true;
            }
            self.written_bits |= bit;
            false
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        if self.check_dup(idx) {
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

    pub fn finish_batch(&mut self) -> RecordBatch {
        let num_rows = self.row_count as usize;
        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.fields.len() + 1);

        for fc in &self.fields {
            // SAFETY: field names are JSON keys, guaranteed valid UTF-8.
            let name = unsafe { std::str::from_utf8_unchecked(&fc.name) };

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
                schema_fields.push(Field::new(format!("{}_int", name), DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::new(
                    values.into(),
                    Some(NullBuffer::from(valid)),
                )) as ArrayRef);
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
                schema_fields.push(Field::new(
                    format!("{}_float", name),
                    DataType::Float64,
                    true,
                ));
                arrays.push(Arc::new(Float64Array::new(
                    values.into(),
                    Some(NullBuffer::from(valid)),
                )) as ArrayRef);
            }
            if fc.has_str {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                let mut vi = 0;
                for row in 0..num_rows {
                    if vi < fc.str_values.len() && fc.str_values[vi].0 as usize == row {
                        // SAFETY: JSON keys and values from the scanner are valid UTF-8 (ASCII subset).
                        let s = unsafe { std::str::from_utf8_unchecked(&fc.str_values[vi].1) };
                        builder.append_value(s);
                        vi += 1;
                    } else {
                        builder.append_null();
                    }
                }
                schema_fields.push(Field::new(format!("{}_str", name), DataType::Utf8, true));
                arrays.push(Arc::new(builder.finish()) as ArrayRef);
            }
        }

        if self.keep_raw && !self.raw_values.is_empty() {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
            for val in &self.raw_values {
                builder.append_value(unsafe { std::str::from_utf8_unchecked(val) });
            }
            for _ in self.raw_values.len()..num_rows {
                builder.append_null();
            }
            schema_fields.push(Field::new("_raw", DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(schema, arrays, &opts)
            .expect("storage_builder: schema/array mismatch")
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
        let batch = b.finish_batch();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column_by_name("host_str")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "web1"
        );
        assert_eq!(
            batch
                .column_by_name("status_int")
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
        let batch = b.finish_batch();
        assert_eq!(batch.num_rows(), 2);
        let ac = batch.column_by_name("a_str").unwrap();
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
        let batch = b.finish_batch();
        assert!(batch.column_by_name("status_int").is_some());
        assert!(batch.column_by_name("status_str").is_some());
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
        let batch = b.finish_batch();
        assert_eq!(batch.num_rows(), 3);
        let ac = batch.column_by_name("a_str").unwrap();
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
        let batch = b.finish_batch();
        assert_eq!(
            batch
                .column_by_name("a_int")
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
        let batch = b.finish_batch();
        assert_eq!(batch.num_rows(), 0);
    }
}
