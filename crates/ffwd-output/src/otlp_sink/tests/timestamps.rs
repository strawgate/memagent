#[test]
fn uint64_timestamp_column_is_recognised() {
    use arrow::array::UInt64Array;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ts_arr = UInt64Array::from(vec![EXPECTED_NS]);
    let body_arr = StringArray::from(vec!["hello"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(body_arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode UInt64-timestamp batch");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.time_unix_nano, EXPECTED_NS,
        "UInt64 time_unix_nano column must be encoded as LogRecord.time_unix_nano"
    );
}

#[test]
fn uint64_attribute_encodes_or_drops_by_int64_range() {
    use arrow::array::UInt64Array;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
        Field::new("count_u64", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("ok"), Some("too-big")])),
            Arc::new(UInt64Array::from(vec![Some(42u64), Some(u64::MAX)])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode UInt64 attribute batch");
    let records = &request.resource_logs[0].scope_logs[0].log_records;
    let first = records[0]
        .attributes
        .iter()
        .find(|kv| kv.key == "count_u64")
        .expect("count_u64 attribute must be present");

    assert_eq!(
        first.value.as_ref().and_then(|v| match v.value.as_ref() {
            Some(Value::IntValue(v)) => Some(*v),
            _ => None,
        }),
        Some(42),
        "representable UInt64 must encode as AnyValue.int_value"
    );
    assert!(
        records[1].attributes.iter().all(|kv| kv.key != "count_u64"),
        "out-of-range UInt64 must not be stringified or wrapped into int_value"
    );
}

/// A Timestamp(Nanosecond) Arrow column must also be recognised as the timestamp field.
#[test]
fn timestamp_nanosecond_column_is_recognised() {
    use arrow::array::TimestampNanosecondArray;
    use arrow::datatypes::TimeUnit;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let ts_arr = TimestampNanosecondArray::from(vec![EXPECTED_NS as i64]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode Timestamp(Nanosecond) batch");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.time_unix_nano, EXPECTED_NS,
        "Timestamp(Nanosecond) column must be encoded as LogRecord.time_unix_nano"
    );
}

#[test]
fn timestamp_second_column_is_recognised() {
    use arrow::array::TimestampSecondArray;
    use arrow::datatypes::TimeUnit;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;
    const INPUT_S: i64 = 1_705_314_600; // EXPECTED_NS / 1_000_000_000

    let schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    )]));
    let ts_arr = TimestampSecondArray::from(vec![INPUT_S]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode Timestamp(Second) batch");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.time_unix_nano, EXPECTED_NS,
        "Timestamp(Second) column must be scaled to nanoseconds in LogRecord.time_unix_nano"
    );
}

#[test]
fn timestamp_millisecond_column_is_recognised() {
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::TimeUnit;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;
    const INPUT_MS: i64 = 1_705_314_600_000; // EXPECTED_NS / 1_000_000

    let schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let ts_arr = TimestampMillisecondArray::from(vec![INPUT_MS]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode Timestamp(Millisecond) batch");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.time_unix_nano, EXPECTED_NS,
        "Timestamp(Millisecond) column must be scaled to nanoseconds in LogRecord.time_unix_nano"
    );
}

#[test]
fn timestamp_microsecond_column_is_recognised() {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::TimeUnit;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;
    const INPUT_US: i64 = 1_705_314_600_000_000; // EXPECTED_NS / 1_000

    let schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let ts_arr = TimestampMicrosecondArray::from(vec![INPUT_US]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode Timestamp(Microsecond) batch");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.time_unix_nano, EXPECTED_NS,
        "Timestamp(Microsecond) column must be scaled to nanoseconds in LogRecord.time_unix_nano"
    );
}

#[test]
fn generated_fast_timestamp_numeric_columns_match_handwritten() {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
    };
    use arrow::datatypes::TimeUnit;

    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

    // Int64 nanos
    assert_timestamp_encoding_parity(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, true),
            Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![EXPECTED_NS as i64])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
        EXPECTED_NS,
    );

    // UInt64 nanos
    assert_timestamp_encoding_parity(
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, true),
            Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![EXPECTED_NS])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
        EXPECTED_NS,
    );

    // Timestamp(Nanosecond)
    assert_timestamp_encoding_parity(
        Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )])),
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            EXPECTED_NS as i64,
        ]))],
        EXPECTED_NS,
    );

    // Timestamp(Microsecond)
    assert_timestamp_encoding_parity(
        Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )])),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![
            1_705_314_600_000_000i64,
        ]))],
        EXPECTED_NS,
    );

    // Timestamp(Millisecond)
    assert_timestamp_encoding_parity(
        Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )])),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1_705_314_600_000i64,
        ]))],
        EXPECTED_NS,
    );

    // Timestamp(Second)
    assert_timestamp_encoding_parity(
        Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )])),
        vec![Arc::new(TimestampSecondArray::from(vec![1_705_314_600i64]))],
        EXPECTED_NS,
    );
}
