#![allow(clippy::indexing_slicing)]
//! RecordBatch finalization: `finish_batch` (zero-copy) and
//! `finish_batch_detached` (owned/detached strings).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewBuilder, StructArray,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use ffwd_core::scanner::BuilderState;

use super::{StreamingBuilder, append_string_view, new_emitted_name_set};

impl StreamingBuilder {
    /// Build a RecordBatch with zero-copy StringViewArrays.
    ///
    /// When no JSON escape sequences were decoded, the resulting RecordBatch
    /// shares the input buffer via Bytes reference counting (zero-copy).
    /// When decoded strings exist, the original and decoded buffers are exposed
    /// as separate Arrow StringView blocks to avoid copying the whole input.
    #[allow(clippy::indexing_slicing)]
    pub fn finish_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InBatch,
            "finish_batch called outside of a batch (call begin_batch first, and ensure all rows are closed with end_row)"
        );
        let num_rows = self.lifecycle.row_count() as usize;

        // StringView offsets use original-buffer offsets for unescaped strings
        // and offsets >= original_buf_len for decoded strings. Keep those as two
        // Arrow blocks so decoding one field never copies the full input buffer.
        let original_buf_len = u32::try_from(self.buf.len()).map_err(|_e| {
            ArrowError::InvalidArgumentError("input buffer exceeds StringView offset range".into())
        })?;
        let arrow_buf = Buffer::from(self.buf.clone());
        let decoded_arrow_buf = if self.decoded_buf.is_empty() {
            None
        } else {
            Some(Buffer::from(self.decoded_buf.clone()))
        };

        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.num_active);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_active);

        // Detect duplicate output column names before building the schema.
        let mut emitted_names = new_emitted_name_set();
        if let Some(line_field_name) = self.line_field_name.as_ref() {
            emitted_names.insert(line_field_name.clone());
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
            if self.line_field_name.as_deref() == Some(name.as_ref()) {
                // Line capture owns this output column name for this batch.
                // Keep scanner semantics as "line wins" when names collide.
                continue;
            }

            // Emit a StructArray when the same field has multiple types in this
            // batch. Single-type fields use the bare field name as a flat column.
            let conflict = (fc.has_int as u8)
                + (fc.has_float as u8)
                + (fc.has_str as u8)
                + (fc.has_bool as u8)
                > 1;

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
                    let original_block = builder.append_block(arrow_buf.clone());
                    let decoded_block = decoded_arrow_buf
                        .as_ref()
                        .map(|buf| builder.append_block(buf.clone()));
                    let mut vi = 0;
                    for row in 0..num_rows as u32 {
                        if vi < fc.str_views.len() && fc.str_views[vi].0 == row {
                            let (_, offset, len) = fc.str_views[vi];
                            append_string_view(
                                &mut builder,
                                original_block,
                                decoded_block,
                                original_buf_len,
                                offset,
                                len,
                            )
                            .expect("offset/len pre-validated by offset_of and UTF-8 check");
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    child_fields.push(Arc::new(Field::new("str", DataType::Utf8View, true)));
                    child_arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }

                if fc.has_bool {
                    let mut values = vec![false; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.bool_values {
                        let row = row as usize;
                        if row >= num_rows {
                            continue;
                        }
                        values[row] = v;
                        valid[row] = true;
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = BooleanArray::new(values.into(), Some(nulls));
                    child_fields.push(Arc::new(Field::new("bool", DataType::Boolean, true)));
                    child_arrays.push(Arc::new(array) as ArrayRef);
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
                    let original_block = builder.append_block(arrow_buf.clone());
                    let decoded_block = decoded_arrow_buf
                        .as_ref()
                        .map(|buf| builder.append_block(buf.clone()));

                    let mut vi = 0;
                    for row in 0..num_rows as u32 {
                        if vi < fc.str_views.len() && fc.str_views[vi].0 == row {
                            let (_, offset, len) = fc.str_views[vi];
                            append_string_view(
                                &mut builder,
                                original_block,
                                decoded_block,
                                original_buf_len,
                                offset,
                                len,
                            )
                            .expect("offset/len pre-validated by offset_of and UTF-8 check");
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }

                    schema_fields.push(Field::new(name.as_ref(), DataType::Utf8View, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }

                if fc.has_bool {
                    reserve_name(name.as_ref())?;
                    let mut values = vec![false; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.bool_values {
                        let row = row as usize;
                        if row >= num_rows {
                            continue;
                        }
                        values[row] = v;
                        valid[row] = true;
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = BooleanArray::new(values.into(), Some(nulls));
                    schema_fields.push(Field::new(name.as_ref(), DataType::Boolean, true));
                    arrays.push(Arc::new(array) as ArrayRef);
                }
            }
        }

        if self.line_field_name.is_some() {
            // When line capture is enabled every row must have exactly one line entry.
            // Check cardinality even when line_views is empty so that callers who
            // never invoke append_line() get an explicit error rather than a batch
            // silently missing the line column.
            if self.line_views.len() != num_rows {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "line_views cardinality mismatch: {} views for {} rows",
                    self.line_views.len(),
                    num_rows
                )));
            }
            let mut builder = StringViewBuilder::new();
            if num_rows > 0 {
                let original_block = builder.append_block(arrow_buf);
                let decoded_block = decoded_arrow_buf
                    .as_ref()
                    .map(|buf| builder.append_block(buf.clone()));
                for row in 0..num_rows {
                    let (offset, len) = self.line_views[row];
                    append_string_view(
                        &mut builder,
                        original_block,
                        decoded_block,
                        original_buf_len,
                        offset,
                        len,
                    )
                    .expect("line view offset/len must be within buffer");
                }
            }
            let line_field_name = self
                .line_field_name
                .as_deref()
                .expect("line_field_name must be set when capture is enabled");
            schema_fields.push(Field::new(line_field_name, DataType::Utf8View, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        // Emit resource.attributes.* columns unconditionally (even for empty batches) so
        // that the schema is identical regardless of row count. Arrow pipelines
        // that concatenate or compare batches require a consistent schema; omitting
        // these columns for num_rows == 0 would cause schema mismatch errors.
        for (key, value) in &self.resource_attrs {
            let col_name = Self::resource_col_name(key);
            reserve_name(&col_name)?;
            let mut builder = StringViewBuilder::new();
            if num_rows > 0 {
                let Ok(value_len) = u32::try_from(value.len()) else {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "resource attribute value too large for Utf8View: {key}"
                    )));
                };
                let block = builder.append_block(Buffer::from(value.as_bytes().to_vec()));
                for _ in 0..num_rows {
                    builder
                        .try_append_view(block, 0, value_len)
                        .expect("resource attr constant view must be valid");
                }
            }
            schema_fields.push(Field::new(col_name, DataType::Utf8View, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let result = RecordBatch::try_new_with_options(schema, arrays, &opts);
        self.lifecycle.finish_batch();
        result
    }

    /// Build a self-contained `RecordBatch` with detached `StringArray` columns.
    ///
    /// Uses the same scan data as `finish_batch` but produces `StringArray`
    /// (contiguous offsets + data) instead of `StringViewArray` (views into
    /// the input buffer). The input `Bytes` can be freed immediately after
    /// this call.
    ///
    /// This is the optimal persistence path: zero-copy scan speed during
    /// parsing, single bulk copy during finalization, and the resulting
    /// `StringArray` compresses efficiently via IPC zstd.
    #[allow(clippy::indexing_slicing)]
    pub fn finish_batch_detached(&mut self) -> Result<RecordBatch, ArrowError> {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InBatch,
            "finish_batch_detached called outside of a batch"
        );
        let num_rows = self.lifecycle.row_count() as usize;
        let has_decoded = !self.decoded_buf.is_empty();

        let mut schema_fields: Vec<Field> = Vec::with_capacity(self.num_active);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_active);

        let mut emitted_names = new_emitted_name_set();
        if let Some(line_field_name) = self.line_field_name.as_ref() {
            emitted_names.insert(line_field_name.clone());
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
            let name = String::from_utf8_lossy(&fc.name);
            if self.line_field_name.as_deref() == Some(name.as_ref()) {
                // Line capture owns this output column name for this batch.
                // Keep scanner semantics as "line wins" when names collide.
                continue;
            }
            let conflict = (fc.has_int as u8)
                + (fc.has_float as u8)
                + (fc.has_str as u8)
                + (fc.has_bool as u8)
                > 1;

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
                    let nulls = NullBuffer::from(valid);
                    let array = Int64Array::new(values.into(), Some(nulls));
                    child_fields.push(Arc::new(Field::new("int", DataType::Int64, true)));
                    child_arrays.push(Arc::new(array) as ArrayRef);
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
                    let nulls = NullBuffer::from(valid);
                    let array = Float64Array::new(values.into(), Some(nulls));
                    child_fields.push(Arc::new(Field::new("float", DataType::Float64, true)));
                    child_arrays.push(Arc::new(array) as ArrayRef);
                }

                if fc.has_str {
                    let total_bytes: usize = fc.str_views.iter().map(|&(_, _, l)| l as usize).sum();
                    let mut builder =
                        arrow::array::StringBuilder::with_capacity(num_rows, total_bytes);
                    let mut vi = 0;
                    for row in 0..num_rows as u32 {
                        if vi < fc.str_views.len() && fc.str_views[vi].0 == row {
                            let (_, offset, len) = fc.str_views[vi];
                            if let Some(s) = self.read_str(offset, len, has_decoded) {
                                builder.append_value(s);
                            } else {
                                builder.append_null();
                            }
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    child_fields.push(Arc::new(Field::new("str", DataType::Utf8, true)));
                    child_arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }

                if fc.has_bool {
                    let mut values = vec![false; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.bool_values {
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = BooleanArray::new(values.into(), Some(nulls));
                    child_fields.push(Arc::new(Field::new("bool", DataType::Boolean, true)));
                    child_arrays.push(Arc::new(array) as ArrayRef);
                }

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
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = Float64Array::new(values.into(), Some(nulls));
                    schema_fields.push(Field::new(name.as_ref(), DataType::Float64, true));
                    arrays.push(Arc::new(array) as ArrayRef);
                }

                if fc.has_str {
                    reserve_name(name.as_ref())?;
                    let total_bytes: usize = fc.str_views.iter().map(|&(_, _, l)| l as usize).sum();
                    let mut builder =
                        arrow::array::StringBuilder::with_capacity(num_rows, total_bytes);
                    let mut vi = 0;
                    for row in 0..num_rows as u32 {
                        if vi < fc.str_views.len() && fc.str_views[vi].0 == row {
                            let (_, offset, len) = fc.str_views[vi];
                            if let Some(s) = self.read_str(offset, len, has_decoded) {
                                builder.append_value(s);
                            } else {
                                builder.append_null();
                            }
                            vi += 1;
                        } else {
                            builder.append_null();
                        }
                    }
                    schema_fields.push(Field::new(name.as_ref(), DataType::Utf8, true));
                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                }

                if fc.has_bool {
                    reserve_name(name.as_ref())?;
                    let mut values = vec![false; num_rows];
                    let mut valid = vec![false; num_rows];
                    for &(row, v) in &fc.bool_values {
                        let r = row as usize;
                        if r < num_rows {
                            values[r] = v;
                            valid[r] = true;
                        }
                    }
                    let nulls = NullBuffer::from(valid);
                    let array = BooleanArray::new(values.into(), Some(nulls));
                    schema_fields.push(Field::new(name.as_ref(), DataType::Boolean, true));
                    arrays.push(Arc::new(array) as ArrayRef);
                }
            }
        }

        if self.line_field_name.is_some() {
            // Same cardinality guard as the non-detached path: every row must
            // have a line entry, including the zero-row case (0 == 0 passes).
            if self.line_views.len() != num_rows {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "line_views cardinality mismatch: {} views for {} rows",
                    self.line_views.len(),
                    num_rows
                )));
            }
            let total_bytes: usize = self.line_views.iter().map(|&(_, l)| l as usize).sum();
            let mut builder = arrow::array::StringBuilder::with_capacity(num_rows, total_bytes);
            let has_decoded = !self.decoded_buf.is_empty();
            for row in 0..num_rows {
                let (offset, len) = self.line_views[row];
                if let Some(s) = self.read_str(offset, len, has_decoded) {
                    builder.append_value(s);
                } else {
                    builder.append_null();
                }
            }
            let line_field_name = self
                .line_field_name
                .as_deref()
                .expect("line_field_name must be set when capture is enabled");
            schema_fields.push(Field::new(line_field_name, DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        // Emit resource.attributes.* columns unconditionally (even for empty batches) so
        // that the schema is identical regardless of row count. Arrow pipelines
        // that concatenate or compare batches require a consistent schema; omitting
        // these columns for num_rows == 0 would cause schema mismatch errors.
        for (key, value) in &self.resource_attrs {
            let col_name = Self::resource_col_name(key);
            reserve_name(&col_name)?;
            let mut builder =
                arrow::array::StringBuilder::with_capacity(num_rows, num_rows * value.len());
            for _ in 0..num_rows {
                builder.append_value(value);
            }
            schema_fields.push(Field::new(col_name, DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(schema_fields));
        let opts = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let result = RecordBatch::try_new_with_options(schema, arrays, &opts);
        self.lifecycle.finish_batch();
        result
    }

    /// Read a string value from the buffer(s) by offset and length.
    ///
    /// Offsets `< buf.len()` read from the original input buffer.
    /// Offsets `>= buf.len()` read from `decoded_buf` at `offset - buf.len()`.
    pub(crate) fn read_str(&self, offset: u32, len: u32, has_decoded: bool) -> Option<&str> {
        let start = offset as usize;
        let end = start.checked_add(len as usize)?;
        let buf_len = self.buf.len();
        let bytes = if !has_decoded || start < buf_len {
            self.buf.get(start..end)?
        } else {
            let dec_start = start.checked_sub(buf_len)?;
            let dec_end = end.checked_sub(buf_len)?;
            self.decoded_buf.get(dec_start..dec_end)?
        };
        std::str::from_utf8(bytes).ok()
    }
}
