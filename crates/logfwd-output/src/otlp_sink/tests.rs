//! Unit and property-based tests for the OTLP sink encoder and transport.
use std::io;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, Int64Array, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_config::OtlpProtocol;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;
use proptest::prelude::*;
use proptest::string::string_regex;

use super::encode::write_grpc_frame;
use super::types::{DEFAULT_GRPC_MAX_MESSAGE_BYTES, OtlpSink};
use crate::{BatchMetadata, Compression};

#[tokio::test]
async fn send_payload_returns_rejected_on_4xx() {
    let mut server = mockito::Server::new_async().await;
    let _mock = server
        .mock("POST", "/v1/logs")
        .with_status(400)
        .create_async()
        .await;

    let mut sink = OtlpSink::new(
        "test".into(),
        server.url() + "/v1/logs",
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();

    sink.encoder_buf.push(1); // Non-empty so it sends
    let result = sink.send_payload(1).await.unwrap();
    match result {
        crate::sink::SendResult::Rejected(_) => {} // Expected
        _ => panic!("Expected Rejected on 400 response, got: {:?}", result),
    }
}

#[tokio::test]
async fn send_payload_returns_io_error_on_5xx_without_retry_after() {
    let mut server = mockito::Server::new_async().await;
    // Server responds with 500 and no Retry-After header. The shared
    // classifier returns IoError so the worker pool applies exponential
    // backoff (better than the old fixed-5s retry).
    let _mock = server
        .mock("POST", "/v1/logs")
        .with_status(500)
        .create_async()
        .await;

    let mut sink = OtlpSink::new(
        "test".into(),
        server.url() + "/v1/logs",
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();

    sink.encoder_buf.push(1);
    let result = sink.send_payload(1).await.unwrap();
    assert!(
        matches!(result, crate::sink::SendResult::IoError(_)),
        "Expected IoError on 500 without Retry-After, got: {result:?}",
    );
}

#[tokio::test]
async fn send_payload_5xx_honours_retry_after_header() {
    let mut server = mockito::Server::new_async().await;
    // Server responds 503 with a Retry-After: 42 header.
    // send_payload should surface that duration rather than the default.
    let _mock = server
        .mock("POST", "/v1/logs")
        .with_status(503)
        .with_header("Retry-After", "42")
        .create_async()
        .await;

    let mut sink = OtlpSink::new(
        "test".into(),
        server.url() + "/v1/logs",
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();

    sink.encoder_buf.push(1);
    let result = sink.send_payload(1).await.unwrap();
    match result {
        crate::sink::SendResult::RetryAfter(d) => {
            assert_eq!(d.as_secs(), 42, "should honour Retry-After header value");
        }
        _ => panic!("Expected RetryAfter on 503 response, got: {:?}", result),
    }
}

#[tokio::test]
async fn grpc_oversized_payload_is_rejected_before_send() {
    let mut sink = OtlpSink::new(
        "test".into(),
        "http://localhost:4317".into(),
        OtlpProtocol::Grpc,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .expect("sink must construct");

    sink.encoder_buf = vec![0u8; DEFAULT_GRPC_MAX_MESSAGE_BYTES + 1];
    let err = sink
        .send_payload(1)
        .await
        .expect_err("oversized gRPC payload must return InvalidInput");

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn invalid_struct_array_downcast_does_not_panic() {
    use crate::{ColVariant, get_array, is_null};

    // Create a non-struct array (e.g. StringArray)
    let str_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello"]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "fake_struct",
        DataType::Utf8, // It's actually utf8
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![str_arr]).unwrap();

    // Simulate a variant that thinks the column is a StructArray
    let variant = ColVariant::StructField {
        struct_col_idx: 0,
        field_idx: 0,
        dt: DataType::Utf8,
    };

    // These should gracefully return true/None, not panic.
    assert!(is_null(&batch, &variant, 0));
    assert!(get_array(&batch, &variant).is_none());
}

/// Struct conflict columns (status: Struct { int, str }) must be normalized
/// to flat Utf8 before OTLP encoding so values are not silently dropped.
#[test]
fn struct_conflict_column_is_normalized_not_dropped() {
    use arrow::array::{Int64Array as I64A, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field as F, Fields};

    let int_arr: Arc<dyn Array> = Arc::new(I64A::from(vec![Some(200i64), None]));
    let str_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![None::<&str>, Some("OK")]));
    let child_fields = Fields::from(vec![
        Arc::new(F::new("int", DataType::Int64, true)),
        Arc::new(F::new("str", DataType::Utf8, true)),
    ]);
    let validity = NullBuffer::from(vec![true, true]);
    let struct_arr: Arc<dyn Array> = Arc::new(StructArray::new(
        child_fields.clone(),
        vec![Arc::clone(&int_arr), Arc::clone(&str_arr)],
        Some(validity),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Struct(child_fields),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![struct_arr]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // After normalization the "status" key must appear in the encoded output
    // with its coalesced value ("200" from the int child, "OK" from str child).
    assert!(
        contains_bytes(&sink.encoder_buf, b"status"),
        "conflict struct column 'status' must be encoded as an OTLP attribute after normalization"
    );
    assert!(
        contains_bytes(&sink.encoder_buf, b"200"),
        "int value 200 must be encoded as the coalesced string '200'"
    );
    assert!(
        contains_bytes(&sink.encoder_buf, b"OK"),
        "str value 'OK' must be encoded as an OTLP attribute"
    );
}

fn make_sink() -> OtlpSink {
    OtlpSink::new(
        "test".into(),
        "http://localhost:4318".into(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        shared_test_client(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap()
}

fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: 1_000_000_000,
    }
}

fn assert_timestamp_encoding_parity(
    schema: Arc<Schema>,
    columns: Vec<Arc<dyn Array>>,
    expected_ns: u64,
) {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message;

    let batch = RecordBatch::try_new(schema, columns).expect("valid timestamp test batch");
    let metadata = make_metadata();

    let mut handwritten = make_sink();
    handwritten.encode_batch(&batch, &metadata);
    let handwritten_request = ExportLogsServiceRequest::decode(handwritten.encoder_buf.as_slice())
        .expect("prost must decode handwritten output");

    let mut generated = make_sink();
    generated.encode_batch_generated_fast(&batch, &metadata);
    let generated_request = ExportLogsServiceRequest::decode(generated.encoder_buf.as_slice())
        .expect("prost must decode generated-fast output");

    let handwritten_ns =
        handwritten_request.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano;
    let generated_ns =
        generated_request.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano;

    assert_eq!(
        handwritten_ns, expected_ns,
        "handwritten path timestamp mismatch"
    );
    assert_eq!(
        generated_ns, expected_ns,
        "generated-fast path timestamp mismatch"
    );
    assert_eq!(
        generated.encoded_payload(),
        handwritten.encoded_payload(),
        "generated-fast payload drifted from handwritten payload"
    );
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn shared_test_client() -> reqwest::Client {
    static TEST_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    TEST_CLIENT.get_or_init(reqwest::Client::new).clone()
}

fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..=max_len)
        .prop_map(|chars| chars.into_iter().collect())
}

proptest! {
    #[test]
    fn proptest_generated_fast_matches_handwritten_random_utf8_rows(
        rows in proptest::collection::vec(
            (
                prop::option::of(unicode_string(30)),
                prop::option::of(unicode_string(8)),
                prop::option::of(unicode_string(40)),
                prop::option::of(prop_oneof![
                    string_regex("[0-9a-f]{32}").expect("valid regex"),
                    unicode_string(40),
                ]),
                prop::option::of(prop_oneof![
                    string_regex("[0-9a-f]{16}").expect("valid regex"),
                    unicode_string(24),
                ]),
                prop::option::of(any::<i64>()),
                prop::option::of(unicode_string(20)),
                prop::option::of(any::<i64>()),
                prop::option::of((-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 10.0)),
                prop::option::of(any::<bool>()),
            ),
            0..24
        )
    ) {
        let timestamps: Vec<Option<String>> = rows.iter().map(|r| r.0.clone()).collect();
        let levels: Vec<Option<String>> = rows.iter().map(|r| r.1.clone()).collect();
        let messages: Vec<Option<String>> = rows.iter().map(|r| r.2.clone()).collect();
        let trace_ids: Vec<Option<String>> = rows.iter().map(|r| r.3.clone()).collect();
        let span_ids: Vec<Option<String>> = rows.iter().map(|r| r.4.clone()).collect();
        let flags: Vec<Option<i64>> = rows.iter().map(|r| r.5).collect();
        let hosts: Vec<Option<String>> = rows.iter().map(|r| r.6.clone()).collect();
        let counts: Vec<Option<i64>> = rows.iter().map(|r| r.7).collect();
        let latencies: Vec<Option<f64>> = rows.iter().map(|r| r.8).collect();
        let actives: Vec<Option<bool>> = rows.iter().map(|r| r.9).collect();

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
                Arc::new(StringArray::from(timestamps.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(levels.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(messages.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(trace_ids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(span_ids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(flags)),
                Arc::new(StringArray::from(hosts.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(counts)),
                Arc::new(arrow::array::Float64Array::from(latencies)),
                Arc::new(arrow::array::BooleanArray::from(actives)),
            ],
        ).expect("valid random utf8 batch");

        let metadata = make_metadata();

        let mut handwritten = make_sink();
        handwritten.encode_batch(&batch, &metadata);

        let mut generated = make_sink();
        generated.encode_batch_generated_fast(&batch, &metadata);

        prop_assert_eq!(
            generated.encoded_payload(),
            handwritten.encoded_payload(),
            "generated-fast OTLP payload drifted from handwritten encoder on random Utf8 rows"
        );
    }
}

#[test]
fn encode_trace_id_as_field_9() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        true,
    )]));
    let arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // field 9, wire type 2: tag = (9 << 3) | 2 = 0x4A; length = 16 = 0x10
    let mut expected = vec![0x4Au8, 0x10u8];
    expected.extend_from_slice(&[
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]);
    assert!(
        contains_bytes(&sink.encoder_buf, &expected),
        "trace_id field 9 not found in encoded output"
    );
}

#[test]
fn encode_span_id_as_field_10() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "span_id",
        DataType::Utf8,
        true,
    )]));
    let arr = StringArray::from(vec!["0102030405060708"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // field 10, wire type 2: tag = (10 << 3) | 2 = 0x52; length = 8 = 0x08
    let mut expected = vec![0x52u8, 0x08u8];
    expected.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    assert!(
        contains_bytes(&sink.encoder_buf, &expected),
        "span_id field 10 not found in encoded output"
    );
}

#[test]
fn encode_flags_as_field_8() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "flags",
        DataType::Int64,
        true,
    )]));
    let arr = Int64Array::from(vec![1i64]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // field 8, wire type 5: tag = (8 << 3) | 5 = 0x45; then 4 bytes LE
    let expected = [0x45u8, 0x01, 0x00, 0x00, 0x00];
    assert!(
        contains_bytes(&sink.encoder_buf, &expected),
        "flags field 8 not found in encoded output"
    );
}

#[test]
fn trace_id_not_encoded_as_attribute() {
    // A trace_id column must NOT appear as a KeyValue attribute (field 6).
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        true,
    )]));
    let arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // If trace_id were encoded as an attribute, its key bytes would appear.
    assert!(
        !contains_bytes(&sink.encoder_buf, b"trace_id"),
        "trace_id key must not appear as an attribute"
    );
}

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

    // The InstrumentationScope name "logfwd" must be present in the encoded bytes.
    assert!(
        contains_bytes(&sink.encoder_buf, b"logfwd"),
        "InstrumentationScope name 'logfwd' not found in encoded output"
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
    let resource_attrs = Arc::new(vec![("k8s.pod.name".to_string(), "my-pod".to_string())]);
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
    assert_eq!(scope.name, "logfwd", "scope name must be 'logfwd'");
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
    assert_eq!(scope.name, "logfwd");

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
            resource_attrs: Arc::from([(
                "deployment.environment".to_string(),
                "test".to_string(),
            )]),
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
