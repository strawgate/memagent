fn sensor_schema() -> Arc<Schema> {
    macro_rules! schema_from_sensor_columns {
        ($(($field:ident, $ty:ty, $array:ident, $data_type:expr, $nullable:expr))+) => {
            Arc::new(Schema::new(vec![
                $(Field::new(stringify!($field), $data_type, $nullable)),+
            ]))
        };
    }

    sensor_row_columns!(schema_from_sensor_columns)
}
