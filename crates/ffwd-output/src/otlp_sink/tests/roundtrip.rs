/// Roundtrip oracle test: encode a RecordBatch with our hand-rolled encoder,
/// decode with prost (the canonical protobuf library), and compare fields.
///
/// This is the definitive test that our OTLP encoding is spec-compliant.
/// If we encode a field incorrectly, prost::Message::decode will either
/// fail or produce different values.
#[test]
fn roundtrip_encode_decode_via_prost() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    // Build a RecordBatch with all supported LogRecord field types.
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("flags", DataType::Int64, true),
        Field::new("host", DataType::Utf8, true), // string attribute
        Field::new("count", DataType::Int64, true), // int attribute
        Field::new("latency", DataType::Float64, true), // double attribute
        Field::new("active", DataType::Boolean, true), // bool attribute
    ]));

    let ts_arr = StringArray::from(vec!["2024-01-15T10:30:00Z"]);
    let level_arr = StringArray::from(vec!["ERROR"]);
    let msg_arr = StringArray::from(vec!["disk full"]);
    let trace_arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
    let span_arr = StringArray::from(vec!["0102030405060708"]);
    let flags_arr = Int64Array::from(vec![1i64]);
    let host_arr = StringArray::from(vec!["web-01"]);
    let count_arr = Int64Array::from(vec![42i64]);
    let latency_arr = arrow::array::Float64Array::from(vec![1.5f64]);
    let active_arr = arrow::array::BooleanArray::from(vec![Some(true)]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts_arr),
            Arc::new(level_arr),
            Arc::new(msg_arr),
            Arc::new(trace_arr),
            Arc::new(span_arr),
            Arc::new(flags_arr),
            Arc::new(host_arr),
            Arc::new(count_arr),
            Arc::new(latency_arr),
            Arc::new(active_arr),
        ],
    )
    .expect("valid batch");

    let observed_ns: u64 = 1_700_000_000_000_000_000;
    let resource_attrs = Arc::from([("k8s.pod.name".to_string(), "my-pod".to_string())]);
    let metadata = BatchMetadata {
        resource_attrs,
        observed_time_ns: observed_ns,
    };

    // Encode with our hand-rolled encoder.
    let mut sink = make_sink();
    sink.encode_batch(&batch, &metadata);
    assert!(
        !sink.encoder_buf.is_empty(),
        "encoder must produce non-empty output"
    );

    // Decode with prost — the canonical protobuf decoder.
    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode our encoding without error");

    // --- Verify structure ---
    assert_eq!(request.resource_logs.len(), 1, "exactly one ResourceLogs");
    let rl = &request.resource_logs[0];

    // Resource attributes
    let resource = rl.resource.as_ref().expect("Resource must be present");
    let pod_attr = resource
        .attributes
        .iter()
        .find(|kv| kv.key == "k8s.pod.name");
    assert!(pod_attr.is_some(), "resource attr k8s.pod.name must exist");
    let pod_val = pod_attr
        .unwrap()
        .value
        .as_ref()
        .and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
    assert_eq!(pod_val, Some("my-pod"), "resource attr value mismatch");

    // ScopeLogs
    assert_eq!(rl.scope_logs.len(), 1, "exactly one ScopeLogs");
    let sl = &rl.scope_logs[0];
    let scope = sl
        .scope
        .as_ref()
        .expect("InstrumentationScope must be present");
    assert_eq!(scope.name, "ffwd", "scope name must be 'ffwd'");
    assert_eq!(
        scope.version,
        env!("CARGO_PKG_VERSION"),
        "scope version must match CARGO_PKG_VERSION"
    );

    // LogRecord
    assert_eq!(sl.log_records.len(), 1, "exactly one LogRecord");
    let lr = &sl.log_records[0];

    // time_unix_nano: 2024-01-15T10:30:00Z = 1705314600 seconds
    assert_eq!(
        lr.time_unix_nano, 1_705_314_600_000_000_000,
        "time_unix_nano mismatch"
    );

    // observed_time_unix_nano
    assert_eq!(
        lr.observed_time_unix_nano, observed_ns,
        "observed_time_unix_nano mismatch"
    );

    // severity
    assert_eq!(lr.severity_number, 17, "ERROR severity_number must be 17");
    assert_eq!(lr.severity_text, "ERROR", "severity_text mismatch");

    // body
    let body_str = lr.body.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(body_str, Some("disk full"), "body mismatch");

    // trace_id (16 bytes, decoded from hex)
    assert_eq!(
        lr.trace_id,
        vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10
        ],
        "trace_id mismatch"
    );

    // span_id (8 bytes, decoded from hex)
    assert_eq!(
        lr.span_id,
        vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        "span_id mismatch"
    );

    // flags
    assert_eq!(lr.flags, 1, "flags mismatch");

    // --- Verify attributes ---
    let find_attr = |name: &str| lr.attributes.iter().find(|kv| kv.key == name);

    // String attribute: host
    let host_kv = find_attr("host").expect("host attribute must exist");
    let host_val = host_kv.value.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(host_val, Some("web-01"), "host attribute value mismatch");

    // Int attribute: count
    let count_kv = find_attr("count").expect("count attribute must exist");
    let count_val = count_kv.value.as_ref().and_then(|v| match &v.value {
        Some(Value::IntValue(i)) => Some(*i),
        _ => None,
    });
    assert_eq!(count_val, Some(42), "count attribute value mismatch");

    // Double attribute: latency
    let latency_kv = find_attr("latency").expect("latency attribute must exist");
    let latency_val = latency_kv.value.as_ref().and_then(|v| match &v.value {
        Some(Value::DoubleValue(d)) => Some(*d),
        _ => None,
    });
    assert!(
        (latency_val.unwrap() - 1.5).abs() < f64::EPSILON,
        "latency attribute value mismatch"
    );

    // Bool attribute: active
    let active_kv = find_attr("active").expect("active attribute must exist");
    let active_val = active_kv.value.as_ref().and_then(|v| match &v.value {
        Some(Value::BoolValue(b)) => Some(*b),
        _ => None,
    });
    assert_eq!(active_val, Some(true), "active attribute value mismatch");
}

#[test]
fn binary_attributes_encode_as_otlp_bytes_values() {
    use arrow::array::BinaryArray;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
        Field::new("log_payload", DataType::Binary, true),
        Field::new(
            "resource.attributes.resource_payload",
            DataType::Binary,
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("binary attrs")])),
            Arc::new(BinaryArray::from(vec![Some(&[0x00_u8, 0x0f, 0xff][..])])),
            Arc::new(BinaryArray::from(vec![Some(&[0xde_u8, 0xad][..])])),
        ],
    )
    .expect("valid batch");
    let metadata = make_metadata();

    let mut handwritten = make_sink();
    handwritten.encode_batch(&batch, &metadata);

    let mut generated = make_sink();
    generated.encode_batch_generated_fast(&batch, &metadata);
    assert_eq!(
        generated.encoded_payload(),
        handwritten.encoded_payload(),
        "generated-fast payload drifted from handwritten binary attribute encoding"
    );

    let request = ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
        .expect("prost must decode binary attributes");
    let resource = request.resource_logs[0]
        .resource
        .as_ref()
        .expect("resource");
    let resource_payload = resource
        .attributes
        .iter()
        .find(|kv| kv.key == "resource_payload")
        .and_then(|kv| kv.value.as_ref())
        .and_then(|value| match &value.value {
            Some(Value::BytesValue(bytes)) => Some(bytes.as_slice()),
            _ => None,
        });
    assert_eq!(resource_payload, Some(&[0xde, 0xad][..]));

    let record = &request.resource_logs[0].scope_logs[0].log_records[0];
    let log_payload = record
        .attributes
        .iter()
        .find(|kv| kv.key == "log_payload")
        .and_then(|kv| kv.value.as_ref())
        .and_then(|value| match &value.value {
            Some(Value::BytesValue(bytes)) => Some(bytes.as_slice()),
            _ => None,
        });
    assert_eq!(log_payload, Some(&[0x00, 0x0f, 0xff][..]));
}

/// Roundtrip with minimal fields: only body, no timestamp, no severity,
/// no trace context. Ensures sparse records encode correctly.
#[test]
fn roundtrip_minimal_record() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        true,
    )]));
    let msg_arr = StringArray::from(vec!["hello world"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(msg_arr)]).expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode minimal record");

    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(lr.time_unix_nano, 0, "no timestamp column means 0");
    assert_eq!(lr.severity_number, 0, "no severity means unspecified");
    let body_str = lr.body.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(body_str, Some("hello world"), "body mismatch");
    assert!(lr.trace_id.is_empty(), "no trace_id column means empty");
    assert!(lr.span_id.is_empty(), "no span_id column means empty");
}

#[test]
fn severity_number_null_falls_back_to_level() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new(field_names::SEVERITY_NUMBER, DataType::Int64, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("ERROR")])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("broken")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode output");
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.severity_number, 17,
        "null severity_number column must fall back to parsed level"
    );
}

#[test]
fn negative_severity_number_encodes_unspecified() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new(field_names::SEVERITY_NUMBER, DataType::Int64, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("ERROR")])),
            Arc::new(Int64Array::from(vec![Some(-1)])),
            Arc::new(StringArray::from(vec![Some("broken")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode output");
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert_eq!(
        lr.severity_number, 0,
        "negative severity_number must not wrap or inherit level severity"
    );
    assert_eq!(lr.severity_text, "ERROR");
}

#[test]
fn unsupported_well_known_types_fall_back_to_attributes() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let metadata = BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: 123_456_789,
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new(field_names::SEVERITY_NUMBER, DataType::Utf8, true),
        Field::new(field_names::OBSERVED_TIMESTAMP, DataType::Utf8, true),
        Field::new(field_names::SCOPE_NAME, DataType::Int64, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("ERROR")])),
            Arc::new(StringArray::from(vec![Some("17")])),
            Arc::new(StringArray::from(vec![Some("not-a-nanos-value")])),
            Arc::new(Int64Array::from(vec![Some(42)])),
            Arc::new(StringArray::from(vec![Some("hello")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &metadata);

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode output");
    let rl = &request.resource_logs[0];
    let sl = &rl.scope_logs[0];
    let lr = &sl.log_records[0];

    assert_eq!(lr.severity_number, 17);
    assert_eq!(lr.observed_time_unix_nano, metadata.observed_time_ns);
    let scope = sl.scope.as_ref().expect("scope must exist");
    assert_eq!(scope.name, "ffwd");

    let find_attr = |name: &str| lr.attributes.iter().find(|kv| kv.key == name);

    let sev_attr = find_attr(field_names::SEVERITY_NUMBER)
        .expect("severity_number should fall back to regular attribute");
    let sev_text = sev_attr.value.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(sev_text, Some("17"));

    let observed_attr = find_attr(field_names::OBSERVED_TIMESTAMP)
        .expect("observed_timestamp should fall back to regular attribute");
    let observed_text = observed_attr.value.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(observed_text, Some("not-a-nanos-value"));

    let scope_name_attr = find_attr(field_names::SCOPE_NAME)
        .expect("scope.name should fall back to regular attribute");
    let scope_name_int = scope_name_attr.value.as_ref().and_then(|v| match &v.value {
        Some(Value::IntValue(i)) => Some(*i),
        _ => None,
    });
    assert_eq!(scope_name_int, Some(42));
}
