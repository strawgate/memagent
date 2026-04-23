// Unit and property-based tests for the OTLP sink encoder and transport.
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
