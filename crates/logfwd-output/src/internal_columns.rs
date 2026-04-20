use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use logfwd_types::field_names;

/// Build a projected batch without FastForward internal columns.
///
/// Returns `None` when the input has no internal columns so callers can keep
/// the zero-copy path for ordinary batches.
pub(crate) fn project_external_batch(
    batch: &RecordBatch,
) -> Result<Option<RecordBatch>, ArrowError> {
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|field| field_names::is_internal_column(field.name()))
    {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if field_names::is_internal_column(field.name()) {
            continue;
        }
        fields.push(Arc::clone(field));
        columns.push(Arc::clone(batch.column(idx)));
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map(Some)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use logfwd_types::field_names;

    use super::*;

    #[test]
    fn project_external_batch_returns_none_without_internal_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))])
            .expect("batch");

        assert!(
            project_external_batch(&batch)
                .expect("projection")
                .is_none()
        );
    }

    #[test]
    fn project_external_batch_drops_only_internal_columns_and_keeps_metadata() {
        let metadata = HashMap::from([("origin".to_string(), "unit-test".to_string())]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new(field_names::SOURCE_ID, DataType::UInt64, true),
                Field::new("__typename", DataType::Utf8, true),
                Field::new("msg", DataType::Utf8, true),
            ],
            metadata.clone(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![Some(7)])),
                Arc::new(StringArray::from(vec!["GraphQLType"])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .expect("batch");

        let projected = project_external_batch(&batch)
            .expect("projection")
            .expect("internal column should project");

        assert!(projected.column_by_name(field_names::SOURCE_ID).is_none());
        assert!(projected.column_by_name("__typename").is_some());
        assert!(projected.column_by_name("msg").is_some());
        assert_eq!(projected.schema().metadata(), &metadata);
    }
}
