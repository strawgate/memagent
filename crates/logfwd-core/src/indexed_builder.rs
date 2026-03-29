// indexed_builder.rs — Index-based BatchBuilder variant.
//
// Same row-at-a-time architecture as BatchBuilder, but:
// - resolve_field(key) caches field index (HashMap hit once per field per batch)
// - append_*_by_idx(idx, value) uses direct array access (no hash)
// - u64 bitset tracks which fields are written per row (no iteration)
// - end_row pads only unwritten fields via bit scan

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::batch_builder::{parse_float_fast, parse_int_fast};

// ---------------------------------------------------------------------------
// Per-field column state (same as BatchBuilder's FieldColumns)
// ---------------------------------------------------------------------------

struct FieldColumns {
    name: Vec<u8>,
    str_builder: Option<StringBuilder>,
    int_builder: Option<Int64Builder>,
    float_builder: Option<Float64Builder>,
    has_str: bool,
    has_int: bool,
    has_float: bool,
}

impl FieldColumns {
    fn new(name: &[u8], capacity: usize) -> Self {
        FieldColumns {
            name: name.to_vec(),
            str_builder: Some(StringBuilder::with_capacity(capacity, capacity * 16)),
            int_builder: None,
            float_builder: None,
            has_str: false,
            has_int: false,
            has_float: false,
        }
    }

    #[inline]
    fn ensure_int(&mut self, row_count: usize) -> &mut Int64Builder {
        if self.int_builder.is_none() {
            let mut b = Int64Builder::with_capacity(row_count + 64);
            for _ in 0..row_count {
                b.append_null();
            }
            self.int_builder = Some(b);
        }
        self.int_builder.as_mut().unwrap()
    }

    #[inline]
    fn ensure_float(&mut self, row_count: usize) -> &mut Float64Builder {
        if self.float_builder.is_none() {
            let mut b = Float64Builder::with_capacity(row_count + 64);
            for _ in 0..row_count {
                b.append_null();
            }
            self.float_builder = Some(b);
        }
        self.float_builder.as_mut().unwrap()
    }

    #[inline]
    fn ensure_str(&mut self, row_count: usize) -> &mut StringBuilder {
        if self.str_builder.is_none() {
            let mut b = StringBuilder::with_capacity(row_count + 64, (row_count + 64) * 16);
            for _ in 0..row_count {
                b.append_null();
            }
            self.str_builder = Some(b);
        }
        self.str_builder.as_mut().unwrap()
    }

    #[inline]
    fn pad_null(&mut self) {
        if let Some(ref mut b) = self.str_builder {
            b.append_null();
        }
        if let Some(ref mut b) = self.int_builder {
            b.append_null();
        }
        if let Some(ref mut b) = self.float_builder {
            b.append_null();
        }
    }

    fn reset(&mut self) {
        self.str_builder = None;
        self.int_builder = None;
        self.float_builder = None;
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
    }
}

// ---------------------------------------------------------------------------
// IndexedBatchBuilder
// ---------------------------------------------------------------------------

pub struct IndexedBatchBuilder {
    fields: Vec<FieldColumns>,
    field_index: HashMap<Vec<u8>, usize>,
    raw_builder: Option<StringBuilder>,
    row_count: usize,
    expected_rows: usize,
    keep_raw: bool,
    /// Bitset: bit i set if field i was written this row. Supports up to 64 fields.
    written_bits: u64,
    /// Mask of all known fields (bit i set if field i exists).
    field_mask: u64,
}

impl IndexedBatchBuilder {
    pub fn new(expected_rows: usize, keep_raw: bool) -> Self {
        IndexedBatchBuilder {
            fields: Vec::with_capacity(32),
            field_index: HashMap::with_capacity(32),
            raw_builder: if keep_raw {
                Some(StringBuilder::with_capacity(
                    expected_rows,
                    expected_rows * 256,
                ))
            } else {
                None
            },
            row_count: 0,
            expected_rows,
            keep_raw,
            written_bits: 0,
            field_mask: 0,
        }
    }

    pub fn begin_batch(&mut self) {
        self.row_count = 0;
        for fc in &mut self.fields {
            fc.reset();
        }
        if self.keep_raw {
            self.raw_builder = Some(StringBuilder::with_capacity(
                self.expected_rows,
                self.expected_rows * 256,
            ));
        }
    }

    #[inline(always)]
    pub fn begin_row(&mut self) {
        self.written_bits = 0;
    }

    #[inline]
    pub fn end_row(&mut self) {
        // Pad NULLs only for fields that were NOT written this row.
        let mut missing = !self.written_bits & self.field_mask;
        while missing != 0 {
            let idx = missing.trailing_zeros() as usize;
            self.fields[idx].pad_null();
            missing &= missing - 1; // clear lowest set bit
        }
        self.row_count += 1;
    }

    /// Resolve a field name to an index. Call once per field per batch,
    /// then use the index for all subsequent rows.
    #[inline]
    pub fn resolve_field(&mut self, key: &[u8]) -> usize {
        if let Some(&idx) = self.field_index.get(key) {
            return idx;
        }
        let idx = self.fields.len();

        self.fields.push(FieldColumns::new(key, self.expected_rows));
        self.field_index.insert(key.to_vec(), idx);
        if idx < 64 {
            self.field_mask |= 1u64 << idx;
        }
        // Back-fill for prior rows.
        if self.row_count > 0 {
            let fc = &mut self.fields[idx];
            let sb = fc.ensure_str(0);
            for _ in 0..self.row_count {
                sb.append_null();
            }
        }
        idx
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return; // duplicate key
        }
        self.written_bits |= bit;
        let row = self.row_count;
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        let s = unsafe { std::str::from_utf8_unchecked(value) };
        fc.ensure_str(row).append_value(s);
        if let Some(ref mut b) = fc.int_builder {
            b.append_null();
        }
        if let Some(ref mut b) = fc.float_builder {
            b.append_null();
        }
    }

    #[inline(always)]
    pub fn append_int_by_idx(&mut self, idx: usize, value: &[u8]) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        let row = self.row_count;
        let fc = &mut self.fields[idx];
        fc.has_int = true;
        if let Some(v) = parse_int_fast(value) {
            fc.ensure_int(row).append_value(v);
        } else {
            fc.ensure_int(row).append_null();
        }
        if let Some(ref mut b) = fc.str_builder {
            b.append_null();
        }
        if let Some(ref mut b) = fc.float_builder {
            b.append_null();
        }
    }

    #[inline(always)]
    pub fn append_float_by_idx(&mut self, idx: usize, value: &[u8]) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        let row = self.row_count;
        let fc = &mut self.fields[idx];
        fc.has_float = true;
        if let Some(v) = parse_float_fast(value) {
            fc.ensure_float(row).append_value(v);
        } else {
            fc.ensure_float(row).append_null();
        }
        if let Some(ref mut b) = fc.str_builder {
            b.append_null();
        }
        if let Some(ref mut b) = fc.int_builder {
            b.append_null();
        }
    }

    #[inline(always)]
    pub fn append_null_by_idx(&mut self, idx: usize) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        self.fields[idx].pad_null();
    }

    #[inline]
    pub fn append_raw(&mut self, line: &[u8]) {
        if let Some(ref mut b) = self.raw_builder {
            let s = unsafe { std::str::from_utf8_unchecked(line) };
            b.append_value(s);
        }
    }

    pub fn finish_batch(&mut self) -> RecordBatch {
        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.fields.len() + 1);

        for fc in &mut self.fields {
            let name = unsafe { std::str::from_utf8_unchecked(&fc.name) };
            let make_col_name = |suffix: &str| -> String { format!("{}_{}", name, suffix) };

            if fc.has_int
                && let Some(ref mut b) = fc.int_builder
            {
                schema_fields.push(Field::new(make_col_name("int"), DataType::Int64, true));
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
            if fc.has_float
                && let Some(ref mut b) = fc.float_builder
            {
                schema_fields.push(Field::new(make_col_name("float"), DataType::Float64, true));
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
            if fc.has_str
                && let Some(ref mut b) = fc.str_builder
            {
                schema_fields.push(Field::new(make_col_name("str"), DataType::Utf8, true));
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
        }

        if let Some(ref mut b) = self.raw_builder {
            schema_fields.push(Field::new("_raw", DataType::Utf8, true));
            arrays.push(Arc::new(b.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        if arrays.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            RecordBatch::try_new(schema, arrays).expect("indexed_builder: schema/array mismatch")
        }
    }
}
