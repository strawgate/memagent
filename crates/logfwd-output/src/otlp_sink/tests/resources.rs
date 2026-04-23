#[test]
fn otlp_encoder_keeps_unselected_body_alias_as_attribute() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("alias-body")])),
            Arc::new(StringArray::from(vec![Some("canonical-body")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink().with_message_field("message".to_string());
    sink.encode_batch(&batch, &make_metadata());
    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode output");
    let record = &request.resource_logs[0].scope_logs[0].log_records[0];
    let body = record.body.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    let message_attr = record.attributes.iter().find(|kv| kv.key == "message");
    let message_attr = message_attr
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });

    assert_eq!(body, Some("canonical-body"));
    assert_eq!(message_attr, Some("alias-body"));
}

#[test]
fn body_column_falls_back_when_message_is_null_for_row() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("message-body"), None])),
            Arc::new(StringArray::from(vec![
                Some("raw-first"),
                Some("raw-fallback"),
            ])),
        ],
    )
    .expect("valid batch");

    let metadata = make_metadata();

    let mut handwritten = OtlpSink::new(
        "test".to_string(),
        "http://localhost:4318".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .expect("handwritten sink")
    .with_message_field("message".to_string());
    handwritten.encode_batch(&batch, &metadata);
    let handwritten_request = ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
        .expect("prost must decode handwritten output");

    let mut generated = OtlpSink::new(
        "test".to_string(),
        "http://localhost:4318".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .expect("generated sink")
    .with_message_field("message".to_string());
    generated.encode_batch_generated_fast(&batch, &metadata);
    let generated_request = ExportLogsServiceRequest::decode(generated.encoder_buf.as_slice())
        .expect("prost must decode generated-fast output");

    let handwritten_bodies: Vec<&str> = handwritten_request.resource_logs[0].scope_logs[0]
        .log_records
        .iter()
        .map(|lr| {
            lr.body
                .as_ref()
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        })
        .collect();
    let generated_bodies: Vec<&str> = generated_request.resource_logs[0].scope_logs[0]
        .log_records
        .iter()
        .map(|lr| {
            lr.body
                .as_ref()
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        })
        .collect();

    assert_eq!(handwritten_bodies, vec!["raw-first", "raw-fallback"]);
    assert_eq!(generated_bodies, vec!["raw-first", "raw-fallback"]);
}

/// Roundtrip with multiple rows to verify repeated LogRecord encoding.
#[test]
fn roundtrip_multiple_rows() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
    ]));
    let msg_arr = StringArray::from(vec!["first", "second", "third"]);
    let level_arr = StringArray::from(vec!["INFO", "WARN", "ERROR"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(msg_arr), Arc::new(level_arr)])
        .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode multi-row batch");

    let records = &request.resource_logs[0].scope_logs[0].log_records;
    assert_eq!(records.len(), 3, "must have 3 LogRecords");

    let bodies: Vec<&str> = records
        .iter()
        .filter_map(|lr| {
            lr.body.as_ref().and_then(|v| match &v.value {
                Some(Value::StringValue(s)) => Some(s.as_str()),
                _ => None,
            })
        })
        .collect();
    assert_eq!(bodies, vec!["first", "second", "third"]);

    let severities: Vec<i32> = records.iter().map(|lr| lr.severity_number).collect();
    assert_eq!(severities, vec![9, 13, 17], "INFO=9, WARN=13, ERROR=17");
}

#[test]
fn resource_columns_group_rows_into_distinct_resource_logs() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("resource.attributes.service.name", DataType::Utf8, true),
        Field::new("resource.attributes.k8s.namespace", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("a1"),
                Some("b1"),
                Some("a2"),
                Some("b2"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("checkout"),
                Some("payments"),
                Some("checkout"),
                Some("payments"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("prod"),
                Some("prod"),
                Some("prod"),
                Some("prod"),
            ])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(
        &batch,
        &BatchMetadata {
            resource_attrs: Arc::from([("deployment.environment".to_string(), "test".to_string())]),
            observed_time_ns: 1,
        },
    );

    let request =
        ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
    assert_eq!(request.resource_logs.len(), 2, "two resource groups");

    let first_records = &request.resource_logs[0].scope_logs[0].log_records;
    let second_records = &request.resource_logs[1].scope_logs[0].log_records;
    let first_bodies: Vec<&str> = first_records
        .iter()
        .map(|lr| {
            lr.body
                .as_ref()
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        })
        .collect();
    let second_bodies: Vec<&str> = second_records
        .iter()
        .map(|lr| {
            lr.body
                .as_ref()
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        })
        .collect();
    assert_eq!(first_bodies, vec!["a1", "a2"], "group keeps row order");
    assert_eq!(second_bodies, vec!["b1", "b2"], "group keeps row order");

    let first_resource = request.resource_logs[0]
        .resource
        .as_ref()
        .expect("resource");
    let second_resource = request.resource_logs[1]
        .resource
        .as_ref()
        .expect("resource");
    assert!(
        first_resource
            .attributes
            .iter()
            .any(|kv| kv.key == "service.name"),
        "original dotted resource key preserved (not mangled to service_name)"
    );
    assert!(
        first_resource
            .attributes
            .iter()
            .any(|kv| kv.key == "deployment.environment"),
        "metadata resource attrs retained"
    );
    assert!(
        second_resource
            .attributes
            .iter()
            .any(|kv| kv.key == "service.name"),
        "second group carries service.name"
    );
}

#[test]
fn resource_columns_not_emitted_as_log_record_attributes() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("resource.attributes.service.name", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("hello")])),
            Arc::new(StringArray::from(vec![Some("checkout")])),
            Arc::new(StringArray::from(vec![Some("host-a")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request =
        ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert!(
        lr.attributes.iter().any(|kv| kv.key == "host"),
        "normal attrs remain log record attrs"
    );
    assert!(
        lr.attributes
            .iter()
            .all(|kv| kv.key != "resource.attributes.service.name"),
        "resource columns must not be log record attrs"
    );
}

#[test]
fn empty_resource_attribute_key_is_dropped() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("resource.attributes.", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("hello")])),
            Arc::new(StringArray::from(vec![Some("bad-empty-key")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request =
        ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
    if let Some(resource) = request.resource_logs[0].resource.as_ref() {
        assert!(
            resource.attributes.iter().all(|kv| !kv.key.is_empty()),
            "empty resource keys must not be emitted"
        );
    }
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert!(
        lr.attributes
            .iter()
            .all(|kv| kv.key != "resource.attributes."),
        "malformed resource prefix column should not fall through to log attributes"
    );
}

#[test]
fn typed_resource_columns_group_rows_and_encode_typed_attrs() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("resource.attributes.service.shard", DataType::Int64, true),
        Field::new(
            "resource.attributes.service.enabled",
            DataType::Boolean,
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("a1"),
                Some("b1"),
                Some("a2"),
                Some("b2"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(1), Some(2)])),
            Arc::new(arrow::array::BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request =
        ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice()).expect("decode request");
    assert_eq!(request.resource_logs.len(), 2, "two typed resource groups");

    let first = &request.resource_logs[0];
    let second = &request.resource_logs[1];

    let first_bodies: Vec<&str> = first.scope_logs[0]
        .log_records
        .iter()
        .map(|lr| {
            lr.body
                .as_ref()
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        })
        .collect();
    let second_bodies: Vec<&str> = second.scope_logs[0]
        .log_records
        .iter()
        .map(|lr| {
            lr.body
                .as_ref()
                .and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        })
        .collect();
    assert_eq!(first_bodies, vec!["a1", "a2"]);
    assert_eq!(second_bodies, vec!["b1", "b2"]);

    let first_attrs = &first.resource.as_ref().expect("resource").attributes;
    assert!(
        first_attrs.iter().any(|kv| {
            kv.key == "service.shard"
                && matches!(
                    kv.value.as_ref().and_then(|v| v.value.as_ref()),
                    Some(Value::IntValue(1))
                )
        }),
        "int resource attrs must be preserved and grouped"
    );
    assert!(
        first_attrs.iter().any(|kv| {
            kv.key == "service.enabled"
                && matches!(
                    kv.value.as_ref().and_then(|v| v.value.as_ref()),
                    Some(Value::BoolValue(true))
                )
        }),
        "bool resource attrs must be preserved and grouped"
    );
}
