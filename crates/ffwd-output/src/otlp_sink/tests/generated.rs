#[test]
fn resource_grouping_treats_null_resource_values_as_distinct_combinations() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("resource.attributes.service.name", DataType::Utf8, true),
        Field::new("resource.attributes.service.shard", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("a1"),
                Some("n1"),
                Some("a2"),
                Some("n2"),
                Some("b1"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("checkout"),
                None,
                Some("checkout"),
                None,
                Some("payments"),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                None,
                Some(2),
            ])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());
    let request =
        ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
    assert_eq!(
        request.resource_logs.len(),
        4,
        "distinct null/non-null resource tuples should form separate groups"
    );

    let grouped_bodies: Vec<Vec<String>> = request
        .resource_logs
        .iter()
        .map(|rl| {
            rl.scope_logs[0]
                .log_records
                .iter()
                .map(|lr| {
                    lr.body
                        .as_ref()
                        .and_then(|v| match &v.value {
                            Some(Value::StringValue(s)) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        grouped_bodies,
        vec![
            vec!["a1".to_string(), "a2".to_string()], // checkout + shard=1
            vec!["n1".to_string()],                   // null service + shard=1
            vec!["n2".to_string()],                   // null service + null shard
            vec!["b1".to_string()],                   // payments + shard=2
        ],
        "row order must remain stable inside each resource group"
    );
}

#[test]
fn generated_fast_otlp_matches_handwritten_encoder() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("flags", DataType::Int64, true),
        Field::new("host", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("latency", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00Z"),
                Some("2024-01-15T10:30:01Z"),
            ])),
            Arc::new(StringArray::from(vec![Some("INFO"), Some("ERROR")])),
            Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
            Arc::new(StringArray::from(vec![
                Some("0102030405060708090a0b0c0d0e0f10"),
                Some("1112131415161718191a1b1c1d1e1f20"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("0102030405060708"),
                Some("1112131415161718"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(255)])),
            Arc::new(StringArray::from(vec![Some("web-01"), Some("web-02")])),
            Arc::new(Int64Array::from(vec![Some(42), Some(7)])),
            Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), Some(2.5)])),
            Arc::new(arrow::array::BooleanArray::from(vec![
                Some(true),
                Some(false),
            ])),
        ],
    )
    .expect("valid batch");

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([("service.name".to_string(), "otlp-test".to_string())]),
        observed_time_ns: 1_700_000_000_000_000_000,
    };

    let mut handwritten = make_sink();
    handwritten.encode_batch(&batch, &metadata);

    let mut generated = make_sink();
    generated.encode_batch_generated_fast(&batch, &metadata);

    assert_eq!(
        generated.encoded_payload(),
        handwritten.encoded_payload(),
        "generated-fast OTLP payload drifted from handwritten encoder",
    );
}

#[test]
fn generated_fast_otlp_matches_handwritten_encoder_with_string_views() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8View, true),
        Field::new("level", DataType::Utf8View, true),
        Field::new("message", DataType::Utf8View, true),
        Field::new("trace_id", DataType::Utf8View, true),
        Field::new("span_id", DataType::Utf8View, true),
        Field::new("flags", DataType::Int64, true),
        Field::new("host", DataType::Utf8View, true),
        Field::new("count", DataType::Int64, true),
        Field::new("latency", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(vec![
                Some("2024-01-15T10:30:00Z"),
                Some("2024-01-15T10:30:01Z"),
            ])),
            Arc::new(StringViewArray::from(vec![Some("INFO"), Some("ERROR")])),
            Arc::new(StringViewArray::from(vec![Some("first"), Some("second")])),
            Arc::new(StringViewArray::from(vec![
                Some("0102030405060708090a0b0c0d0e0f10"),
                Some("1112131415161718191a1b1c1d1e1f20"),
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("0102030405060708"),
                Some("1112131415161718"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(255)])),
            Arc::new(StringViewArray::from(vec![Some("web-01"), Some("web-02")])),
            Arc::new(Int64Array::from(vec![Some(42), Some(7)])),
            Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), Some(2.5)])),
            Arc::new(arrow::array::BooleanArray::from(vec![
                Some(true),
                Some(false),
            ])),
        ],
    )
    .expect("valid batch");

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([("service.name".to_string(), "otlp-test".to_string())]),
        observed_time_ns: 1_700_000_000_000_000_000,
    };

    let mut handwritten = make_sink();
    handwritten.encode_batch(&batch, &metadata);

    let mut generated = make_sink();
    generated.encode_batch_generated_fast(&batch, &metadata);

    assert_eq!(
        generated.encoded_payload(),
        handwritten.encoded_payload(),
        "generated-fast OTLP payload drifted from handwritten encoder on Utf8View inputs",
    );
}

#[test]
fn generated_fast_otlp_matches_handwritten_encoder_with_large_strings() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::LargeUtf8, true),
        Field::new("level", DataType::LargeUtf8, true),
        Field::new("message", DataType::LargeUtf8, true),
        Field::new("trace_id", DataType::LargeUtf8, true),
        Field::new("span_id", DataType::LargeUtf8, true),
        Field::new("flags", DataType::Int64, true),
        Field::new("host", DataType::LargeUtf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("latency", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(LargeStringArray::from(vec![
                Some("2024-01-15T10:30:00Z"),
                Some("2024-01-15T10:30:01Z"),
            ])),
            Arc::new(LargeStringArray::from(vec![Some("INFO"), Some("ERROR")])),
            Arc::new(LargeStringArray::from(vec![Some("first"), Some("second")])),
            Arc::new(LargeStringArray::from(vec![
                Some("0102030405060708090a0b0c0d0e0f10"),
                Some("1112131415161718191a1b1c1d1e1f20"),
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("0102030405060708"),
                Some("1112131415161718"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(255)])),
            Arc::new(LargeStringArray::from(vec![Some("web-01"), Some("web-02")])),
            Arc::new(Int64Array::from(vec![Some(42), Some(7)])),
            Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), Some(2.5)])),
            Arc::new(arrow::array::BooleanArray::from(vec![
                Some(true),
                Some(false),
            ])),
        ],
    )
    .expect("valid batch");

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([("service.name".to_string(), "otlp-test".to_string())]),
        observed_time_ns: 1_700_000_000_000_000_000,
    };

    let mut handwritten = make_sink();
    handwritten.encode_batch(&batch, &metadata);

    let mut generated = make_sink();
    generated.encode_batch_generated_fast(&batch, &metadata);

    assert_eq!(
        generated.encoded_payload(),
        handwritten.encoded_payload(),
        "generated-fast OTLP payload drifted from handwritten encoder on LargeUtf8 inputs",
    );
}

/// An Int64 timestamp column (as produced by the OTLP receiver for `time_unix_nano`)
/// must be recognised as the timestamp field and encoded into LogRecord.time_unix_nano.
/// Before the fix, `resolve_batch_columns` rejected Int64 columns, so time_unix_nano
/// was always 0 in OTLP→pipeline→OTLP pipelines.
#[test]
fn int64_timestamp_column_is_recognised() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    // 2024-01-15T10:30:00Z expressed as raw nanoseconds (as OTLP receiver produces).
    const EXPECTED_NS: u64 = 1_705_314_600_000_000_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ts_arr = Int64Array::from(vec![EXPECTED_NS as i64]);
    let body_arr = StringArray::from(vec!["hello"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(body_arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode Int64-timestamp batch");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.time_unix_nano, EXPECTED_NS,
        "Int64 time_unix_nano column must be encoded as LogRecord.time_unix_nano"
    );
}

#[test]
fn epoch_zero_string_timestamp_is_encoded_as_valid_time() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("1970-01-01T00:00:00Z")])),
            Arc::new(StringArray::from(vec![Some("epoch")])),
        ],
    )
    .expect("valid batch");

    let mut handwritten = make_sink();
    handwritten.encode_batch(&batch, &make_metadata());

    let mut generated = make_sink();
    generated.encode_batch_generated_fast(&batch, &make_metadata());
    assert_eq!(generated.encoded_payload(), handwritten.encoded_payload());

    let request = ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
        .expect("prost must decode epoch timestamp batch");
    let record = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(record.time_unix_nano, 0);
    assert!(
        contains_bytes(&handwritten.encoder_buf, &[0x09, 0, 0, 0, 0, 0, 0, 0, 0]),
        "valid epoch-zero timestamp should be emitted, not treated as parse failure"
    );
}
