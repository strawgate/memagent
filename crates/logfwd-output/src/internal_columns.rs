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
pub(crate) fn external_batch_view(batch: &RecordBatch) -> Result<Option<RecordBatch>, ArrowError> {
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

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map(Some)
}
