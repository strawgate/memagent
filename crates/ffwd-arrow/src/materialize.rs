#![allow(clippy::indexing_slicing)]
// materialize.rs — Detach a RecordBatch from its input buffer.
//
// StreamingBuilder produces StringViewArray columns that reference the
// original input `Bytes` buffer. Before the input can be freed (or the
// batch compressed for persistence), string data must be copied into
// independent buffers.
//
// DataFusion's WHERE filter already does this implicitly (the filter
// kernel copies selected rows into fresh arrays). For passthrough
// queries (SELECT * without WHERE), an explicit detach step is needed.
//
// `detach_if_attached` checks whether any column still references the
// input buffer and only copies when necessary — zero overhead when a
// filter already detached the data.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

/// Unconditionally detach all `Utf8View` columns to `Utf8`.
///
/// Produces a self-contained `RecordBatch` whose string columns are
/// contiguous `StringArray` instead of `StringViewArray`. The input
/// buffer can be freed after this call.
///
/// Recurses into `StructArray` conflict columns to detach their
/// `Utf8View` children. If no `Utf8View` columns exist at any level,
/// returns a cheap clone (Arc bumps only).
///
/// When `Utf8View` columns exist, performs a bulk `arrow::compute::cast`
/// from `Utf8View` to `Utf8`. This copies all string data once.
pub fn detach(batch: &RecordBatch) -> RecordBatch {
    if !has_utf8view_anywhere(batch) {
        return batch.clone();
    }
    cast_views(batch)
}

/// Detach only if at least one column still references `input_buf`.
///
/// After a DataFusion WHERE filter, string columns are typically already
/// independent of the input buffer. In that case this function returns
/// the batch unchanged (zero copy). Only when columns still hold views
/// into the input does it perform the bulk cast.
///
/// This is the optimal persistence boundary: zero cost when the SQL
/// transform already detached the data, bulk copy otherwise.
pub fn detach_if_attached(batch: &RecordBatch, input_buf: &bytes::Bytes) -> RecordBatch {
    if is_attached(batch, input_buf) {
        cast_views(batch)
    } else {
        batch.clone()
    }
}

/// Check whether any column buffer in the batch overlaps the input buffer.
///
/// Recurses into `StructArray` children to catch conflict columns whose
/// `Utf8View` `"str"` child references the input buffer.
pub fn is_attached(batch: &RecordBatch, input_buf: &bytes::Bytes) -> bool {
    let buf_start = input_buf.as_ptr() as usize;
    let buf_end = buf_start + input_buf.len();

    for col in batch.columns() {
        if buffers_overlap(&col.to_data(), buf_start, buf_end) {
            return true;
        }
    }
    false
}

/// Recursively check whether any buffer in an ArrayData (including children)
/// overlaps the given memory range.
fn buffers_overlap(data: &arrow::array::ArrayData, buf_start: usize, buf_end: usize) -> bool {
    for buffer in data.buffers() {
        let ptr = buffer.as_ptr() as usize;
        let end = ptr + buffer.len();
        if ptr < buf_end && end > buf_start {
            return true;
        }
    }
    for child in data.child_data() {
        if buffers_overlap(child, buf_start, buf_end) {
            return true;
        }
    }
    false
}

/// Check whether any column (including struct children) contains Utf8View.
fn has_utf8view_anywhere(batch: &RecordBatch) -> bool {
    fn check_type(dt: &DataType) -> bool {
        match dt {
            DataType::Utf8View => true,
            DataType::Struct(fields) => fields.iter().any(|f| check_type(f.data_type())),
            _ => false,
        }
    }
    batch
        .schema()
        .fields()
        .iter()
        .any(|f| check_type(f.data_type()))
}

/// Cast all Utf8View columns to Utf8 (StringArray), recursing into StructArray.
fn cast_views(batch: &RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let (new_field, new_col) = cast_column(field, batch.column(i));
        fields.push(new_field);
        columns.push(new_col);
    }
    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, columns).expect("schema/column mismatch after cast")
}

/// Cast a single column, recursing into StructArray children.
fn cast_column(field: &Arc<Field>, col: &ArrayRef) -> (Arc<Field>, ArrayRef) {
    match field.data_type() {
        DataType::Utf8View => {
            let utf8_col = arrow::compute::cast(col, &DataType::Utf8).expect("Utf8View→Utf8 cast");
            let new_field = Arc::new(
                Field::new(field.name(), DataType::Utf8, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            );
            (new_field, utf8_col)
        }
        DataType::Struct(struct_fields) => {
            // Check if any child is Utf8View
            let needs_cast = struct_fields
                .iter()
                .any(|f| *f.data_type() == DataType::Utf8View);
            if !needs_cast {
                return (Arc::clone(field), Arc::clone(col));
            }
            // Rebuild the struct with cast children
            let struct_arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("DataType::Struct must be StructArray");
            let mut new_children: Vec<ArrayRef> = Vec::with_capacity(struct_fields.len());
            let mut new_child_fields: Vec<Arc<Field>> = Vec::with_capacity(struct_fields.len());
            for (j, child_field) in struct_fields.iter().enumerate() {
                let child_col = struct_arr.column(j);
                let (new_cf, new_cc) = cast_column(child_field, child_col);
                new_child_fields.push(new_cf);
                new_children.push(new_cc);
            }
            let new_struct_fields = Fields::from(new_child_fields);
            let new_struct = StructArray::new(
                new_struct_fields.clone(),
                new_children,
                struct_arr.nulls().cloned(),
            );
            let new_field = Arc::new(
                Field::new(
                    field.name(),
                    DataType::Struct(new_struct_fields),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            );
            (new_field, Arc::new(new_struct) as ArrayRef)
        }
        _ => (Arc::clone(field), Arc::clone(col)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Scanner;
    use bytes::Bytes;
    use ffwd_core::scan_config::ScanConfig;

    fn scan(input: &[u8]) -> (RecordBatch, Bytes) {
        let buf = Bytes::from(input.to_vec());
        let mut scanner = Scanner::new(ScanConfig::default());
        let batch = scanner.scan(buf.clone()).unwrap();
        (batch, buf)
    }

    #[test]
    fn is_attached_true_after_scan() {
        let (batch, buf) = scan(b"{\"msg\":\"hello\"}\n");
        assert!(is_attached(&batch, &buf));
    }

    #[test]
    fn is_attached_false_after_detach() {
        let (batch, buf) = scan(b"{\"msg\":\"hello\"}\n");
        let owned = detach(&batch);
        assert!(!is_attached(&owned, &buf));
    }

    #[test]
    fn detach_if_attached_copies_when_attached() {
        let (batch, buf) = scan(b"{\"msg\":\"hello\"}\n");
        let result = detach_if_attached(&batch, &buf);
        assert!(!is_attached(&result, &buf));
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn detach_if_attached_skips_when_not_attached() {
        let (batch, buf) = scan(b"{\"msg\":\"hello\"}\n");
        let owned = detach(&batch);
        let result = detach_if_attached(&owned, &buf);
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn int_only_batch_not_attached() {
        let (batch, buf) = scan(b"{\"n\":42}\n");
        let owned = detach(&batch);
        assert!(!is_attached(&owned, &buf));
    }

    #[test]
    fn preserves_data_after_detach() {
        let (batch, _buf) = scan(b"{\"msg\":\"hello\",\"n\":42}\n");
        let owned = detach(&batch);
        assert_eq!(owned.num_rows(), 1);
        assert_eq!(owned.num_columns(), batch.num_columns());

        let msg_col = owned.column_by_name("msg").expect("msg column");
        let arr = msg_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should be StringArray after detach");
        assert_eq!(arr.value(0), "hello");

        let n_col = owned.column_by_name("n").expect("n column");
        let int_arr = n_col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("should be Int64Array");
        assert_eq!(int_arr.value(0), 42);
    }

    // --- Conflict column (StructArray) tests ---

    #[test]
    fn detach_handles_conflict_struct_column() {
        // status is int in row 0, string in row 1 → StructArray with str child as Utf8View
        let (batch, buf) = scan(b"{\"status\":200}\n{\"status\":\"OK\"}\n");
        assert!(
            is_attached(&batch, &buf),
            "conflict struct should be attached"
        );

        let owned = detach(&batch);
        assert!(
            !is_attached(&owned, &buf),
            "should be detached after detach()"
        );
        assert_eq!(owned.num_rows(), 2);

        // Verify the struct's str child is now Utf8 (not Utf8View)
        let status_col = owned.column_by_name("status").unwrap();
        let struct_arr = status_col
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("should still be StructArray");
        let str_child = struct_arr.column_by_name("str").unwrap();
        assert_eq!(
            *str_child.data_type(),
            DataType::Utf8,
            "str child should be Utf8 after detach"
        );
    }

    #[test]
    fn is_attached_detects_conflict_struct_children() {
        let (batch, buf) = scan(b"{\"v\":1}\n{\"v\":\"x\"}\n");
        assert!(
            is_attached(&batch, &buf),
            "conflict struct with Utf8View child should be detected as attached"
        );
    }

    // --- Edge case tests ---

    #[test]
    fn detach_empty_batch() {
        let (batch, _buf) = scan(b"\n");
        let owned = detach(&batch);
        assert_eq!(owned.num_rows(), 0);
    }

    #[test]
    fn detach_no_string_columns() {
        let (batch, buf) = scan(b"{\"a\":1,\"b\":2.5}\n");
        // No Utf8View columns — detach should be a cheap clone
        let owned = detach(&batch);
        assert!(!is_attached(&owned, &buf));
        assert_eq!(owned.num_rows(), 1);
    }

    #[test]
    fn detach_preserves_schema_and_field_metadata() {
        use std::collections::HashMap;
        let (batch, _buf) = scan(b"{\"msg\":\"hello\"}\n");

        let mut schema_meta = HashMap::new();
        schema_meta.insert("schema_key".to_string(), "schema_val".to_string());

        let mut field_meta = HashMap::new();
        field_meta.insert("field_key".to_string(), "field_val".to_string());

        let old_schema = batch.schema();
        let mut fields = Vec::with_capacity(old_schema.fields().len());
        for field in old_schema.fields() {
            fields.push(Arc::new(
                field.as_ref().clone().with_metadata(field_meta.clone()),
            ));
        }
        let new_schema = Arc::new(Schema::new_with_metadata(fields, schema_meta));

        // Rebuild batch with metadata
        let batch_with_meta = RecordBatch::try_new(new_schema, batch.columns().to_vec()).unwrap();

        let owned = detach(&batch_with_meta);

        // Validate schema metadata
        assert_eq!(
            owned.schema().metadata().get("schema_key"),
            Some(&"schema_val".to_string()),
            "schema metadata must be preserved"
        );

        // Validate field metadata
        let owned_schema = owned.schema();
        let msg_field = owned_schema.field_with_name("msg").unwrap();
        assert_eq!(
            msg_field.metadata().get("field_key"),
            Some(&"field_val".to_string()),
            "field metadata must be preserved"
        );
    }
}
