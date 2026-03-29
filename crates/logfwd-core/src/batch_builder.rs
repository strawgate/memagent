// batch_builder.rs — Manages typed Arrow column builders, produces RecordBatches.
//
// Hot path: every append method is called once per field per line.
// Design: no per-call allocations; builders are reused across batches.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Which typed sub-columns a field ended up with in a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTag {
    Str,
    Int,
    Float,
}

/// Manages typed Arrow column builders for one batch of JSON lines.
pub struct BatchBuilder {
    fields: Vec<FieldColumns>,
    field_index: HashMap<Vec<u8>, usize>,
    raw_builder: Option<StringBuilder>,
    row_count: usize,
    expected_rows: usize,
    keep_raw: bool,
}

/// Per-field column state.  At most three typed sub-columns.
struct FieldColumns {
    name: Vec<u8>,
    str_builder: Option<StringBuilder>,
    int_builder: Option<Int64Builder>,
    float_builder: Option<Float64Builder>,
    /// Has this field been written in the current row?
    current_row_set: bool,
    /// Track which type variants have been seen across the batch.
    has_str: bool,
    has_int: bool,
    has_float: bool,
}

// ---------------------------------------------------------------------------
// Fast integer parser — no allocation, no UTF-8 validation.
// ---------------------------------------------------------------------------

/// Parse a byte slice as a signed 64-bit integer.
/// Returns None on overflow or non-digit bytes.
#[inline(always)]
pub fn parse_int_fast(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };
    if start >= bytes.len() {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?;
        acc = acc.checked_add((b - b'0') as i64)?;
    }
    if neg { Some(-acc) } else { Some(acc) }
}

/// Parse a byte slice as f64 using the standard library.
#[inline(always)]
pub fn parse_float_fast(bytes: &[u8]) -> Option<f64> {
    // SAFETY: We only call this on bytes that look like a JSON number,
    // which is always valid ASCII, so from_utf8_unchecked is fine.
    let s = unsafe { std::str::from_utf8_unchecked(bytes) };
    s.parse::<f64>().ok()
}

// ---------------------------------------------------------------------------
// FieldColumns helpers
// ---------------------------------------------------------------------------

impl FieldColumns {
    fn new(name: &[u8], capacity: usize) -> Self {
        // Start with only a string builder; add int/float lazily.
        FieldColumns {
            name: name.to_vec(),
            str_builder: Some(StringBuilder::with_capacity(capacity, capacity * 16)),
            int_builder: None,
            float_builder: None,
            current_row_set: false,
            has_str: false,
            has_int: false,
            has_float: false,
        }
    }

    /// Ensure the int builder exists with back-filled NULLs up to `row_count`.
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

    /// Ensure the float builder exists with back-filled NULLs up to `row_count`.
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

    /// Ensure the str builder exists with back-filled NULLs up to `row_count`.
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

    /// Pad all active builders with a NULL for the current row.
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

    /// Reset for a new batch without dropping allocated memory.
    fn reset(&mut self) {
        // We recreate builders but HashMap + name vecs survive.
        // Arrow builders don't expose a "reset" that keeps capacity,
        // so we create fresh ones with the same capacity hint.
        // The Vec<u8> name is kept.
        self.str_builder = None;
        self.int_builder = None;
        self.float_builder = None;
        self.current_row_set = false;
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
    }
}

// ---------------------------------------------------------------------------
// BatchBuilder implementation
// ---------------------------------------------------------------------------

impl BatchBuilder {
    pub fn new(expected_rows: usize, keep_raw: bool) -> Self {
        BatchBuilder {
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
        }
    }

    /// Reset all builders for a new batch.  Keeps field_index and names so
    /// subsequent batches from the same source don't re-discover fields.
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

    /// Mark the start of a new row.
    #[inline(always)]
    pub fn begin_row(&mut self) {
        // Mark all fields as "not set" for this row.
        for fc in &mut self.fields {
            fc.current_row_set = false;
        }
    }

    /// Finish the current row — pad NULLs for any field not written.
    #[inline]
    pub fn end_row(&mut self) {
        for fc in &mut self.fields {
            if !fc.current_row_set {
                fc.pad_null();
            }
        }
        self.row_count += 1;
    }

    // -----------------------------------------------------------------------
    // Field accessors
    // -----------------------------------------------------------------------

    /// Get or create the field index for `key`.
    #[inline]
    fn get_or_create_field(&mut self, key: &[u8]) -> usize {
        if let Some(&idx) = self.field_index.get(key) {
            return idx;
        }
        let idx = self.fields.len();
        self.fields.push(FieldColumns::new(key, self.expected_rows));
        self.field_index.insert(key.to_vec(), idx);
        // Back-fill the new field's str builder with NULLs for prior rows.
        let fc = &mut self.fields[idx];
        if self.row_count > 0 {
            let sb = fc.ensure_str(0);
            for _ in 0..self.row_count {
                sb.append_null();
            }
        }
        idx
    }

    // -----------------------------------------------------------------------
    // Append methods — hot path
    // -----------------------------------------------------------------------

    /// Append a string value for `key`.
    /// `value` is the raw bytes *without* surrounding quotes and with escapes
    /// still present (caller strips quotes).
    #[inline]
    pub fn append_str(&mut self, key: &[u8], value: &[u8]) {
        let row = self.row_count;
        let idx = self.get_or_create_field(key);
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.current_row_set = true;
        // SAFETY: JSON string content (after escape processing) is valid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(value) };
        fc.ensure_str(row).append_value(s);
        // Pad other active builders with NULL.
        if let Some(ref mut b) = fc.int_builder {
            b.append_null();
        }
        if let Some(ref mut b) = fc.float_builder {
            b.append_null();
        }
    }

    /// Append an integer value.  Writes to int builder; pads str/float with NULL.
    #[inline]
    pub fn append_int(&mut self, key: &[u8], value: &[u8]) {
        let row = self.row_count;
        let idx = self.get_or_create_field(key);
        let fc = &mut self.fields[idx];
        fc.has_int = true;
        fc.current_row_set = true;
        if let Some(v) = parse_int_fast(value) {
            fc.ensure_int(row).append_value(v);
        } else {
            fc.ensure_int(row).append_null();
        }
        // Pad other active builders.
        if let Some(ref mut b) = fc.str_builder {
            b.append_null();
        }
        if let Some(ref mut b) = fc.float_builder {
            b.append_null();
        }
    }

    /// Append a float value.  Writes to float builder; pads str/int with NULL.
    #[inline]
    pub fn append_float(&mut self, key: &[u8], value: &[u8]) {
        let row = self.row_count;
        let idx = self.get_or_create_field(key);
        let fc = &mut self.fields[idx];
        fc.has_float = true;
        fc.current_row_set = true;
        if let Some(v) = parse_float_fast(value) {
            fc.ensure_float(row).append_value(v);
        } else {
            fc.ensure_float(row).append_null();
        }
        // Pad other active builders.
        if let Some(ref mut b) = fc.str_builder {
            b.append_null();
        }
        if let Some(ref mut b) = fc.int_builder {
            b.append_null();
        }
    }

    /// Append an explicit null for `key`.
    #[inline]
    pub fn append_null(&mut self, key: &[u8]) {
        let row = self.row_count;
        let idx = self.get_or_create_field(key);
        let fc = &mut self.fields[idx];
        fc.current_row_set = true;
        // Pad all active builders.
        let _ = row; // used by ensure_* if needed
        fc.pad_null();
    }

    /// Append to the _raw column.
    #[inline]
    pub fn append_raw(&mut self, line: &[u8]) {
        if let Some(ref mut b) = self.raw_builder {
            let s = unsafe { std::str::from_utf8_unchecked(line) };
            b.append_value(s);
        }
    }

    // -----------------------------------------------------------------------
    // Finish
    // -----------------------------------------------------------------------

    /// Finalize the batch into an Arrow RecordBatch.
    ///
    /// Column naming:
    /// - If a field has exactly one type: `{name}_{suffix}`
    /// - If a field has multiple types: emit one column per type, each
    ///   `{name}_{suffix}`.
    /// - `_raw` column if enabled.
    pub fn finish_batch(&mut self) -> RecordBatch {
        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.fields.len() + 1);

        for fc in &mut self.fields {
            let name = unsafe { std::str::from_utf8_unchecked(&fc.name) };
            let type_count = fc.has_str as u8 + fc.has_int as u8 + fc.has_float as u8;
            let _multi = type_count > 1;

            // Determine suffix: if single type, use plain name; if multi, use name_suffix.
            let make_col_name = |suffix: &str| -> String { format!("{}_{}", name, suffix) };

            if fc.has_int
                && let Some(ref mut b) = fc.int_builder
            {
                let col_name = make_col_name("int");
                schema_fields.push(Field::new(&col_name, DataType::Int64, true));
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
            if fc.has_float
                && let Some(ref mut b) = fc.float_builder
            {
                let col_name = make_col_name("float");
                schema_fields.push(Field::new(&col_name, DataType::Float64, true));
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
            if fc.has_str
                && let Some(ref mut b) = fc.str_builder
            {
                let col_name = make_col_name("str");
                schema_fields.push(Field::new(&col_name, DataType::Utf8, true));
                arrays.push(Arc::new(b.finish()) as ArrayRef);
            }
        }

        // _raw column
        if let Some(ref mut b) = self.raw_builder {
            schema_fields.push(Field::new("_raw", DataType::Utf8, true));
            arrays.push(Arc::new(b.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        if arrays.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            RecordBatch::try_new(schema, arrays).expect("batch_builder: schema/array mismatch")
        }
    }

    /// Return the discovered field type map.
    pub fn field_type_map(&self) -> HashMap<String, Vec<TypeTag>> {
        let mut m = HashMap::with_capacity(self.fields.len());
        for fc in &self.fields {
            let name = unsafe { std::str::from_utf8_unchecked(&fc.name) }.to_string();
            let mut tags = Vec::new();
            if fc.has_int {
                tags.push(TypeTag::Int);
            }
            if fc.has_float {
                tags.push(TypeTag::Float);
            }
            if fc.has_str {
                tags.push(TypeTag::Str);
            }
            if !tags.is_empty() {
                m.insert(name, tags);
            }
        }
        m
    }

    /// Number of rows written so far (before `end_row` on current row).
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};

    #[test]
    fn test_parse_int_fast() {
        assert_eq!(parse_int_fast(b"0"), Some(0));
        assert_eq!(parse_int_fast(b"42"), Some(42));
        assert_eq!(parse_int_fast(b"-7"), Some(-7));
        assert_eq!(parse_int_fast(b""), None);
        assert_eq!(parse_int_fast(b"-"), None);
        assert_eq!(parse_int_fast(b"3.14"), None); // dot
        assert_eq!(parse_int_fast(b"abc"), None);
    }

    #[test]
    fn test_parse_float_fast() {
        assert!((parse_float_fast(b"3.14").unwrap() - 3.14).abs() < 1e-10);
        assert_eq!(parse_float_fast(b"abc"), None);
    }

    #[test]
    fn test_builder_basic() {
        let mut bb = BatchBuilder::new(4, true);
        bb.begin_batch();

        // Row 0
        bb.begin_row();
        bb.append_str(b"host", b"web1");
        bb.append_int(b"status", b"200");
        bb.append_raw(b"{\"host\":\"web1\",\"status\":200}");
        bb.end_row();

        // Row 1
        bb.begin_row();
        bb.append_str(b"host", b"web2");
        bb.append_int(b"status", b"404");
        bb.append_raw(b"{\"host\":\"web2\",\"status\":404}");
        bb.end_row();

        let batch = bb.finish_batch();
        assert_eq!(batch.num_rows(), 2);

        // host should be str-only → column "host_str"
        let host_col = batch.column_by_name("host_str").expect("host_str column");
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(host_arr.value(0), "web1");
        assert_eq!(host_arr.value(1), "web2");

        // status should be int-only → column "status_int"
        let status_col = batch
            .column_by_name("status_int")
            .expect("status_int column");
        let status_arr = status_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(status_arr.value(0), 200);
        assert_eq!(status_arr.value(1), 404);

        // _raw
        let raw_col = batch.column_by_name("_raw").expect("_raw column");
        let raw_arr = raw_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(raw_arr.value(0).contains("web1"));
    }

    #[test]
    fn test_builder_type_conflict() {
        let mut bb = BatchBuilder::new(4, false);
        bb.begin_batch();

        // Row 0: status is int
        bb.begin_row();
        bb.append_int(b"status", b"200");
        bb.end_row();

        // Row 1: status is str
        bb.begin_row();
        bb.append_str(b"status", b"OK");
        bb.end_row();

        let batch = bb.finish_batch();
        assert_eq!(batch.num_rows(), 2);

        // Both columns should exist
        assert!(batch.column_by_name("status_int").is_some());
        assert!(batch.column_by_name("status_str").is_some());

        let int_arr = batch
            .column_by_name("status_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_arr.value(0), 200);
        assert!(int_arr.is_null(1));

        let str_arr = batch
            .column_by_name("status_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(str_arr.is_null(0));
        assert_eq!(str_arr.value(1), "OK");
    }

    #[test]
    fn test_builder_missing_fields() {
        let mut bb = BatchBuilder::new(4, false);
        bb.begin_batch();

        bb.begin_row();
        bb.append_str(b"a", b"hello");
        bb.end_row();

        bb.begin_row();
        bb.append_str(b"b", b"world");
        bb.end_row();

        let batch = bb.finish_batch();
        assert_eq!(batch.num_rows(), 2);

        let a = batch
            .column_by_name("a_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "hello");
        assert!(a.is_null(1));

        let b = batch
            .column_by_name("b_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(b.is_null(0));
        assert_eq!(b.value(1), "world");
    }
}
