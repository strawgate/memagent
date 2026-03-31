#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use crate::otlp_sink::resolve_batch_columns;

    #[test]
    #[should_panic(expected = "downcast to PrimitiveArray of Int64Type failed")]
    fn test_otlp_sink_panic_on_type_mismatch() {
        // Define a schema with a column named "val_int" but with type Utf8 (String)
        let schema = Arc::new(Schema::new(vec![
            Field::new("val_int", DataType::Utf8, true),
        ]));

        // Create a batch with a StringArray for the "val_int" column
        let array = Arc::new(StringArray::from(vec!["not an int"]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        // This should panic because resolve_batch_columns expects "val_int" to be Int64
        let _ = resolve_batch_columns(&batch);
    }
}
