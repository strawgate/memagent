#[test]
fn span_id_not_encoded_as_attribute() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "span_id",
        DataType::Utf8,
        true,
    )]));
    let arr = StringArray::from(vec!["0102030405060708"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    assert!(
        !contains_bytes(&sink.encoder_buf, b"span_id"),
        "span_id key must not appear as an attribute"
    );
}

#[test]
fn configured_message_field_does_not_shadow_trace_id() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("canonical-body")])),
            Arc::new(StringArray::from(vec![Some(
                "0102030405060708090a0b0c0d0e0f10",
            )])),
        ],
    )
    .unwrap();

    let mut sink = make_sink().with_message_field("trace_id".to_string());
    sink.encode_batch(&batch, &make_metadata());
    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode output");
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];

    let body = lr.body.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(body, Some("canonical-body"));
    assert_eq!(
        lr.trace_id,
        vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10
        ],
        "trace_id should remain encoded in dedicated OTLP field"
    );
}

#[test]
fn configured_message_field_does_not_shadow_span_id() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    };
    use prost::Message;

    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("canonical-body")])),
            Arc::new(StringArray::from(vec![Some("0102030405060708")])),
        ],
    )
    .unwrap();

    let mut sink = make_sink().with_message_field("span_id".to_string());
    sink.encode_batch(&batch, &make_metadata());
    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost must decode output");
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];

    let body = lr.body.as_ref().and_then(|v| match &v.value {
        Some(Value::StringValue(s)) => Some(s.as_str()),
        _ => None,
    });
    assert_eq!(body, Some("canonical-body"));
    assert_eq!(
        lr.span_id,
        vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        "span_id should remain encoded in dedicated OTLP field"
    );
}

#[test]
fn invalid_trace_id_hex_is_silently_ignored() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        true,
    )]));
    // Not a valid 32-char hex string.
    let arr = StringArray::from(vec!["not-a-valid-hex-string-here!!!!"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata()); // must not panic

    // Field 9 tag 0x4A must not appear.
    let mut probe = vec![0x4Au8, 0x10u8];
    probe.extend_from_slice(&[0u8; 16]);
    assert!(
        !contains_bytes(&sink.encoder_buf, &probe),
        "invalid trace_id should not produce field 9"
    );
}

#[test]
fn grpc_frame_prepends_five_byte_header() {
    let proto_payload = [0x0a, 0x02, 0x08, 0x01];
    let mut framed = Vec::new();
    write_grpc_frame(&mut framed, &proto_payload, false).unwrap();
    assert_eq!(framed.len(), 5 + proto_payload.len());
    assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
    let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
    assert_eq!(
        msg_len as usize,
        proto_payload.len(),
        "length field must match payload length"
    );
    assert_eq!(
        &framed[5..],
        &proto_payload,
        "payload bytes must follow header"
    );
}

#[test]
fn grpc_frame_empty_payload() {
    let mut framed = Vec::new();
    write_grpc_frame(&mut framed, &[], false).unwrap();
    assert_eq!(framed.len(), 5);
    assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
    let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
    assert_eq!(msg_len, 0, "length field must be zero for empty payload");
}

#[test]
fn grpc_frame_compressed_flag() {
    let proto_payload = [0x0a, 0x02];
    let mut framed = Vec::new();
    write_grpc_frame(&mut framed, &proto_payload, true).unwrap();
    assert_eq!(framed[0], 0x01, "compressed flag must be 0x01");
}

/// Verify that `encode_batch` + `write_grpc_frame` produce a valid 5-byte gRPC frame header
/// followed by the exact protobuf payload. Tests the encode-and-frame path directly without
/// making a real network call.
#[test]
fn encode_and_frame_payload() {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let arr = StringArray::from(vec!["hello"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    // Encode the batch the same way send_batch would, then frame it manually.
    let mut sink = OtlpSink::new(
        "test".into(),
        "http://localhost:4318".into(),
        OtlpProtocol::Grpc,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();
    sink.encode_batch(&batch, &make_metadata());
    let proto_payload = sink.encoder_buf.clone();

    // Frame as send_batch would.
    let mut framed = Vec::new();
    write_grpc_frame(&mut framed, &proto_payload, false).unwrap();

    // The frame header must be 5 bytes followed by the exact protobuf payload.
    assert_eq!(
        framed[0], 0x00,
        "compressed flag must be 0x00 for uncompressed gRPC"
    );
    let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
    assert_eq!(
        msg_len as usize,
        proto_payload.len(),
        "gRPC length field must match protobuf payload length"
    );
    assert_eq!(
        &framed[5..],
        proto_payload.as_slice(),
        "protobuf payload must follow the frame header"
    );
}

#[test]
fn scope_logs_has_instrumentation_scope() {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let arr = StringArray::from(vec!["hello"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // The InstrumentationScope name "ffwd" must be present in the encoded bytes.
    assert!(
        contains_bytes(&sink.encoder_buf, b"ffwd"),
        "InstrumentationScope name 'ffwd' not found in encoded output"
    );

    // The InstrumentationScope version (from CARGO_PKG_VERSION) must also be present.
    let version = env!("CARGO_PKG_VERSION").as_bytes();
    assert!(
        contains_bytes(&sink.encoder_buf, version),
        "InstrumentationScope version not found in encoded output"
    );
}

#[test]
fn encode_boolean_as_attribute() {
    use arrow::array::BooleanArray;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "active",
        DataType::Boolean,
        true,
    )]));
    let arr = BooleanArray::from(vec![Some(true)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // LogRecord field 6 tag: (6 << 3) | 2 = 0x32
    // KeyValue field 1 key tag: (1 << 3) | 2 = 0x0A, then "active"
    // KeyValue field 2 value AnyValue tag: (2 << 3) | 2 = 0x12
    // AnyValue field 2 bool_value tag: (2 << 3) | 0 = 0x10, then 0x01
    let expected = [0x10u8, 0x01];
    assert!(
        contains_bytes(&sink.encoder_buf, &expected),
        "boolean attribute not found in encoded output"
    );
    assert!(
        contains_bytes(&sink.encoder_buf, b"active"),
        "attribute key 'active' not found"
    );
}
