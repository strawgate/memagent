use datafusion::prelude::*;
use logfwd_transform::SqlTransform;
use arrow::record_batch::RecordBatch;
use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, true),
    ]));
    let trace_id: arrow::array::ArrayRef = Arc::new(StringArray::from(vec![
        Some("trace123"),
        None,
        Some("trace123"),
        Some("other_trace"),
    ]));
    let batch = RecordBatch::try_new(schema, vec![trace_id]).unwrap();

    let mut transform = SqlTransform::new("SELECT trace_id, hash(trace_id) as h FROM logs").unwrap();
    let res = transform.execute(batch).await.unwrap();

    let h_col = res.column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
    assert!(h_col.is_null(1));
    assert_eq!(h_col.value(0), h_col.value(2));
    assert_ne!(h_col.value(0), h_col.value(3));
    println!("test passed!");
}
