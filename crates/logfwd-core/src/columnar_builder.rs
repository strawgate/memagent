// columnar_builder.rs — Zero-copy columnar BatchBuilder with compressed IPC output.
//
// During scanning: stores (offset, len) pointers into the input buffer per field.
// No value copies during the hot scan loop.
//
// On finish: bulk-builds Arrow columns from collected offsets, optionally
// producing zstd-compressed Arrow IPC bytes for in-memory storage.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::builder::StringDictionaryBuilder;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringBuilder};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Int8Type, Int16Type, Schema};
use arrow::record_batch::RecordBatch;

use crate::batch_builder::{parse_float_fast, parse_int_fast};

// ---------------------------------------------------------------------------
// Value records — zero-copy offset pointers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
#[repr(u8)]
enum ValueType {
    Str = 0,
    Int = 1,
    Float = 2,
    Null = 3,
}

/// A value recorded during scanning. Points into the input buffer.
#[derive(Clone, Copy)]
struct ValueRecord {
    /// Row number within the batch.
    row: u32,
    /// Byte offset into the input buffer.
    offset: u32,
    /// Length of the value bytes.
    len: u32,
    /// Value type.
    vtype: ValueType,
}

/// Per-field collection of value records.
struct FieldCollector {
    name: Vec<u8>,
    values: Vec<ValueRecord>,
    has_str: bool,
    has_int: bool,
    has_float: bool,
}

impl FieldCollector {
    fn new(name: &[u8]) -> Self {
        FieldCollector {
            name: name.to_vec(),
            values: Vec::with_capacity(256),
            has_str: false,
            has_int: false,
            has_float: false,
        }
    }

    fn clear(&mut self) {
        self.values.clear();
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
    }
}

// ---------------------------------------------------------------------------
// ColumnarBatchBuilder
// ---------------------------------------------------------------------------

pub struct ColumnarBatchBuilder {
    fields: Vec<FieldCollector>,
    field_index: HashMap<Vec<u8>, usize>,
    /// Raw line offsets: (offset, len) into the input buffer.
    raw_offsets: Vec<(u32, u32)>,
    row_count: u32,
    keep_raw: bool,
    /// Bitset for duplicate key detection within a row.
    written_bits: u64,
    field_mask: u64,
    /// Start offset of the input buffer (set when scanning starts).
    buf_base: *const u8,
}

// SAFETY: buf_base is only used during finish_batch while the buffer is alive.
unsafe impl Send for ColumnarBatchBuilder {}

impl ColumnarBatchBuilder {
    pub fn new(_expected_rows: usize, keep_raw: bool) -> Self {
        ColumnarBatchBuilder {
            fields: Vec::with_capacity(32),
            field_index: HashMap::with_capacity(32),
            raw_offsets: Vec::new(),
            row_count: 0,
            keep_raw,
            written_bits: 0,
            field_mask: 0,
            buf_base: std::ptr::null(),
        }
    }

    pub fn begin_batch(&mut self) {
        self.row_count = 0;
        for fc in &mut self.fields {
            fc.clear();
        }
        self.raw_offsets.clear();
    }

    /// Set the input buffer pointer. Must be called before scanning.
    /// The buffer must remain valid until `finish_batch` returns.
    #[inline(always)]
    pub fn set_buffer(&mut self, buf: &[u8]) {
        self.buf_base = buf.as_ptr();
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
        if idx < 64 {
            self.field_mask |= 1u64 << idx;
        }
        idx
    }

    /// Compute the offset of a value slice relative to the buffer base.
    #[inline(always)]
    fn offset_of(&self, value: &[u8]) -> u32 {
        let ptr = value.as_ptr();
        // SAFETY: value is a subslice of the buffer set by set_buffer.
        unsafe { ptr.offset_from(self.buf_base) as u32 }
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        let offset = self.offset_of(value);
        let row = self.row_count;
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.values.push(ValueRecord {
            row,
            offset,
            len: value.len() as u32,
            vtype: ValueType::Str,
        });
    }

    #[inline(always)]
    pub fn append_int_by_idx(&mut self, idx: usize, value: &[u8]) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        let offset = self.offset_of(value);
        let row = self.row_count;
        let fc = &mut self.fields[idx];
        fc.has_int = true;
        fc.values.push(ValueRecord {
            row,
            offset,
            len: value.len() as u32,
            vtype: ValueType::Int,
        });
    }

    #[inline(always)]
    pub fn append_float_by_idx(&mut self, idx: usize, value: &[u8]) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        let offset = self.offset_of(value);
        let row = self.row_count;
        let fc = &mut self.fields[idx];
        fc.has_float = true;
        fc.values.push(ValueRecord {
            row,
            offset,
            len: value.len() as u32,
            vtype: ValueType::Float,
        });
    }

    #[inline(always)]
    pub fn append_null_by_idx(&mut self, idx: usize) {
        let bit = if idx < 64 { 1u64 << idx } else { 0 };
        if self.written_bits & bit != 0 {
            return;
        }
        self.written_bits |= bit;
        self.fields[idx].values.push(ValueRecord {
            row: self.row_count,
            offset: 0,
            len: 0,
            vtype: ValueType::Null,
        });
    }

    /// Record raw line as offset into the buffer.
    #[inline]
    pub fn append_raw(&mut self, line: &[u8]) {
        if self.keep_raw {
            let offset = self.offset_of(line);
            self.raw_offsets.push((offset, line.len() as u32));
        }
    }

    /// Bulk-build Arrow RecordBatch from collected offsets.
    ///
    /// `buf` must be the same buffer passed to `set_buffer`.
    pub fn finish_batch(&self, buf: &[u8]) -> RecordBatch {
        let num_rows = self.row_count as usize;
        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 1);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.fields.len() + 1);

        for fc in &self.fields {
            let name = unsafe { std::str::from_utf8_unchecked(&fc.name) };

            if fc.has_int {
                let col_name = format!("{}_int", name);
                let mut values = vec![0i64; num_rows];
                let mut valid = vec![false; num_rows];
                for vr in &fc.values {
                    if let ValueType::Int = vr.vtype {
                        let row = vr.row as usize;
                        let bytes =
                            &buf[vr.offset as usize..(vr.offset as usize + vr.len as usize)];
                        if let Some(v) = parse_int_fast(bytes) {
                            values[row] = v;
                            valid[row] = true;
                        }
                    }
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
                for vr in &fc.values {
                    if let ValueType::Float = vr.vtype {
                        let row = vr.row as usize;
                        let bytes =
                            &buf[vr.offset as usize..(vr.offset as usize + vr.len as usize)];
                        if let Some(v) = parse_float_fast(bytes) {
                            values[row] = v;
                            valid[row] = true;
                        }
                    }
                }
                let nulls = NullBuffer::from(valid);
                let array = Float64Array::new(values.into(), Some(nulls));
                schema_fields.push(Field::new(col_name, DataType::Float64, true));
                arrays.push(Arc::new(array) as ArrayRef);
            }

            if fc.has_str {
                let col_name = format!("{}_str", name);
                let str_values: Vec<_> = fc
                    .values
                    .iter()
                    .filter(|vr| matches!(vr.vtype, ValueType::Str))
                    .collect();

                // Count unique values to decide encoding strategy.
                let mut uniques = HashSet::new();
                for vr in &str_values {
                    let bytes = &buf[vr.offset as usize..(vr.offset as usize + vr.len as usize)];
                    uniques.insert(bytes);
                }
                let cardinality = uniques.len();

                // Adaptive encoding:
                //   < 256 uniques → DictionaryArray<Int8>  (1 byte/row)
                //   < 32768 uniques and < 50% of rows → DictionaryArray<Int16> (2 bytes/row)
                //   else → plain StringArray
                if cardinality < 256 {
                    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
                    let mut vi = 0;
                    for row in 0..num_rows {
                        if vi < str_values.len() && str_values[vi].row as usize == row {
                            let vr = str_values[vi];
                            let bytes =
                                &buf[vr.offset as usize..(vr.offset as usize + vr.len as usize)];
                            let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                            builder.append_value(s);
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    let dict_type =
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
                    schema_fields.push(Field::new(col_name, dict_type, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                } else if cardinality < 32768 && cardinality < num_rows / 2 {
                    let mut builder = StringDictionaryBuilder::<Int16Type>::new();
                    let mut vi = 0;
                    for row in 0..num_rows {
                        if vi < str_values.len() && str_values[vi].row as usize == row {
                            let vr = str_values[vi];
                            let bytes =
                                &buf[vr.offset as usize..(vr.offset as usize + vr.len as usize)];
                            let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                            builder.append_value(s);
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    let dict_type =
                        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
                    schema_fields.push(Field::new(col_name, dict_type, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                } else {
                    // High cardinality — plain StringArray
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
                    let mut vi = 0;
                    for row in 0..num_rows {
                        if vi < str_values.len() && str_values[vi].row as usize == row {
                            let vr = str_values[vi];
                            let bytes =
                                &buf[vr.offset as usize..(vr.offset as usize + vr.len as usize)];
                            let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                            builder.append_value(s);
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    schema_fields.push(Field::new(col_name, DataType::Utf8, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }
            }
        }

        // _raw column
        if self.keep_raw && !self.raw_offsets.is_empty() {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
            for &(offset, len) in &self.raw_offsets {
                let bytes = &buf[offset as usize..(offset as usize + len as usize)];
                let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                builder.append_value(s);
            }
            for _ in self.raw_offsets.len()..num_rows {
                builder.append_null();
            }
            schema_fields.push(Field::new("_raw", DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        if arrays.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            RecordBatch::try_new(schema, arrays).expect("columnar_builder: schema/array mismatch")
        }
    }

    /// Bulk-build and compress to Arrow IPC format with zstd compression.
    ///
    /// Returns the compressed IPC bytes. This is the format for in-memory
    /// storage — dramatically smaller than an uncompressed RecordBatch.
    pub fn finish_batch_compressed(&self, buf: &[u8]) -> Vec<u8> {
        let batch = self.finish_batch(buf);
        let mut output = Vec::new();

        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .expect("zstd compression supported");

        let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
            &mut output,
            &batch.schema(),
            options,
        )
        .expect("create IPC writer");

        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish IPC stream");

        output
    }
}
