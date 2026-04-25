//! Contract tests for OTLP receiver semantics across wire variants.
//!
//! These tests verify that semantically equivalent OTLP requests preserve the
//! same decoded meaning across JSON, protobuf, and compressed protobuf paths.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::otlp_receiver::OtlpReceiverInput;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;

fn poll_single_batch(input: &mut dyn InputSource, timeout: Duration) -> RecordBatch {
    let deadline = Instant::now() + timeout;
    let mut batches = Vec::new();

    while Instant::now() < deadline {
        for event in input.poll().expect("poll receiver") {
            if let InputEvent::Batch { batch, .. } = event {
                batches.push(batch);
            }
        }

        if !batches.is_empty() {
            assert_eq!(
                batches.len(),
                1,
                "expected exactly one batch, got {}",
                batches.len()
            );
            return batches.pop().expect("one batch");
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for OTLP receiver batch");
}

fn send_status(url: &str, body: &[u8], content_type: &str, content_encoding: Option<&str>) -> u16 {
    let mut request = ureq::post(url).header("content-type", content_type);
    if let Some(encoding) = content_encoding {
        request = request.header("content-encoding", encoding);
    }

    match request.send(body) {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(err) => panic!("unexpected OTLP request error: {err}"),
    }
}

fn make_otlp_input(stats: Arc<ComponentStats>) -> (OtlpReceiverInput, String) {
    let receiver = OtlpReceiverInput::new_with_stats("contract", "127.0.0.1:0", Arc::clone(&stats))
        .expect("receiver should start");
    let url = format!("http://{}/v1/logs", receiver.local_addr());
    (receiver, url)
}

fn poll_until_events(input: &mut dyn InputSource, timeout: Duration) -> Vec<InputEvent> {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let events = input.poll().expect("poll receiver");
        if !events.is_empty() {
            return events;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for OTLP receiver events");
}

fn assert_accounted_bytes_for_payload(
    body: &[u8],
    content_type: &str,
    content_encoding: Option<&str>,
) {
    let stats = Arc::new(ComponentStats::new());
    let mut receiver =
        OtlpReceiverInput::new_with_stats("contract", "127.0.0.1:0", Arc::clone(&stats))
            .expect("receiver should start");
    let url = format!("http://{}/v1/logs", receiver.local_addr());

    let status = send_status(&url, body, content_type, content_encoding);
    assert_eq!(status, 200, "request should succeed");

    let events = poll_until_events(&mut receiver, Duration::from_secs(2));
    assert!(
        !events.is_empty(),
        "receiver should emit at least one event"
    );

    let mut total_accounted: u64 = 0;
    let mut total_rows: u64 = 0;
    for event in &events {
        if let InputEvent::Batch {
            batch,
            accounted_bytes,
            ..
        } = event
        {
            total_accounted += accounted_bytes;
            total_rows += batch.num_rows() as u64;
        }
    }
    assert_eq!(
        total_accounted,
        body.len() as u64,
        "receiver should charge the accepted request-body size at the input boundary"
    );
    assert_eq!(total_rows, 1, "one OTLP request should yield one row");
}

fn semantic_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("checkout-api".into())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_705_314_600_000_000_000,
                    severity_text: "ERROR".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("disk full".into())),
                    }),
                    attributes: vec![
                        KeyValue {
                            key: "request_count".into(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(7)),
                            }),
                        },
                        KeyValue {
                            key: "latency_ratio".into(),
                            value: Some(AnyValue {
                                value: Some(Value::DoubleValue(3.5)),
                            }),
                        },
                        KeyValue {
                            key: "sampled".into(),
                            value: Some(AnyValue {
                                value: Some(Value::BoolValue(true)),
                            }),
                        },
                        KeyValue {
                            key: "payload_hex".into(),
                            value: Some(AnyValue {
                                value: Some(Value::BytesValue(vec![0x00, 0x01, 0x02, 0xff])),
                            }),
                        },
                    ],
                    trace_id: vec![
                        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                        0x0d, 0x0e, 0x0f, 0x10,
                    ],
                    span_id: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Validate that a single-row batch contains the expected semantic values
/// from the canonical semantic_request().
fn assert_semantic_batch(batch: &RecordBatch) {
    assert_eq!(batch.num_rows(), 1, "expected exactly one row");

    let ts = batch
        .column_by_name(field_names::TIMESTAMP)
        .expect("_timestamp must exist");
    let ts = ts
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("_timestamp must be Int64");
    assert_eq!(ts.value(0), 1_705_314_600_000_000_000_i64);

    let severity = batch
        .column_by_name(field_names::SEVERITY)
        .expect("severity must exist");
    let severity = severity
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("severity must be String");
    assert_eq!(severity.value(0), "ERROR");

    let body = batch
        .column_by_name(field_names::BODY)
        .expect("body must exist");
    let body = body
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("body must be String");
    assert_eq!(body.value(0), "disk full");

    let request_count = batch
        .column_by_name("request_count")
        .expect("request_count must exist");
    let request_count = request_count
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("request_count must be Int64");
    assert_eq!(request_count.value(0), 7);

    let latency = batch
        .column_by_name("latency_ratio")
        .expect("latency_ratio must exist");
    let latency = latency
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("latency_ratio must be Float64");
    assert!((latency.value(0) - 3.5).abs() < f64::EPSILON);

    let sampled = batch.column_by_name("sampled").expect("sampled must exist");
    let sampled = sampled
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("sampled must be Boolean");
    assert!(sampled.value(0));

    let trace_id = batch
        .column_by_name(field_names::TRACE_ID)
        .expect("trace_id must exist");
    let trace_id = trace_id
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("trace_id must be String");
    assert_eq!(trace_id.value(0), "0102030405060708090a0b0c0d0e0f10");

    let span_id = batch
        .column_by_name(field_names::SPAN_ID)
        .expect("span_id must exist");
    let span_id = span_id
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("span_id must be String");
    assert_eq!(span_id.value(0), "0102030405060708");
}

#[test]
fn otlp_receiver_preserves_semantics_across_json_protobuf_zstd_and_gzip() {
    let mut receiver = OtlpReceiverInput::new("contract", "127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/logs", receiver.local_addr());

    // --- JSON path ---
    let json_body = serde_json::json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {"stringValue": "checkout-api"}
                }]
            },
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": "1705314600000000000",
                    "severityText": "ERROR",
                    "body": {"stringValue": "disk full"},
                    "attributes": [
                        {"key": "request_count", "value": {"intValue": "7"}},
                        {"key": "latency_ratio", "value": {"doubleValue": 3.5}},
                        {"key": "sampled", "value": {"boolValue": true}},
                        {"key": "payload_hex", "value": {"bytesValue": "AAEC/w=="}}
                    ],
                    "traceId": "0102030405060708090a0b0c0d0e0f10",
                    "spanId": "0102030405060708"
                }]
            }]
        }]
    })
    .to_string();

    let status = send_status(&url, json_body.as_bytes(), "application/json", None);
    assert_eq!(status, 200, "json OTLP request should succeed");
    let _json_batch = poll_single_batch(&mut receiver, Duration::from_secs(2));

    // --- Protobuf path ---
    let request = semantic_request();
    let protobuf_body = request.encode_to_vec();
    let status = send_status(&url, &protobuf_body, "application/x-protobuf", None);
    assert_eq!(status, 200, "protobuf OTLP request should succeed");
    let protobuf_batch = poll_single_batch(&mut receiver, Duration::from_secs(2));
    assert_semantic_batch(&protobuf_batch);

    // --- Zstd path ---
    let compressed_body = zstd::bulk::compress(&protobuf_body, 1).expect("zstd compress");
    let status = send_status(
        &url,
        &compressed_body,
        "application/x-protobuf",
        Some("zstd"),
    );
    assert_eq!(status, 200, "zstd OTLP request should succeed");
    let zstd_batch = poll_single_batch(&mut receiver, Duration::from_secs(2));
    assert_semantic_batch(&zstd_batch);

    // --- Gzip path ---
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    use std::io::Write as _;
    encoder
        .write_all(&protobuf_body)
        .expect("gzip write should succeed");
    let gzip_body = encoder.finish().expect("gzip finish should succeed");
    let status = send_status(&url, &gzip_body, "application/x-protobuf", Some("gzip"));
    assert_eq!(status, 200, "gzip OTLP request should succeed");
    let gzip_batch = poll_single_batch(&mut receiver, Duration::from_secs(2));
    assert_semantic_batch(&gzip_batch);
}

#[test]
fn otlp_receiver_invalid_json_bytes_value_returns_400() {
    let mut receiver = OtlpReceiverInput::new("contract", "127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/logs", receiver.local_addr());

    let invalid_body = br#"{
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{
                    "attributes": [{
                        "key": "payload_hex",
                        "value": {"bytesValue": "***not-base64***"}
                    }]
                }]
            }]
        }]
    }"#;

    let status = send_status(&url, invalid_body, "application/json", None);
    assert_eq!(status, 400, "invalid bytesValue must be rejected");

    let events = receiver
        .poll()
        .expect("poll receiver after rejected request");
    assert!(
        events.is_empty(),
        "rejected request must not enqueue decoded data"
    );
}

#[test]
fn otlp_receiver_accounts_input_bytes() {
    let request = semantic_request();
    let protobuf_body = request.encode_to_vec();
    let expected_bytes = protobuf_body.len() as u64;

    let stats = Arc::new(ComponentStats::new());
    let (mut input, url) = make_otlp_input(Arc::clone(&stats));

    let status = send_status(&url, &protobuf_body, "application/x-protobuf", None);
    assert_eq!(status, 200, "request should succeed");

    let events = poll_until_events(&mut input, Duration::from_secs(2));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, InputEvent::Batch { batch, .. } if batch.num_rows() == 1)),
        "should emit one-row batch"
    );

    assert_eq!(stats.lines(), 1, "should account one input row");
    assert_eq!(
        stats.bytes(),
        expected_bytes,
        "should account the accepted protobuf payload bytes"
    );
    assert_eq!(
        stats.errors(),
        0,
        "successful request should not count errors"
    );
    assert_eq!(
        stats.parse_errors(),
        0,
        "successful request should not count parse errors"
    );
}

#[test]
fn otlp_receiver_rejections_increment_parse_errors() {
    let stats = Arc::new(ComponentStats::new());
    let (mut input, url) = make_otlp_input(Arc::clone(&stats));

    let status = send_status(&url, b"not valid protobuf", "application/x-protobuf", None);
    assert_eq!(status, 400, "invalid protobuf must be rejected");

    std::thread::sleep(Duration::from_millis(20));
    let events = input.poll().expect("poll receiver after rejected request");
    assert!(
        events.is_empty(),
        "rejected request must not enqueue decoded data"
    );
    assert_eq!(stats.lines(), 0, "rejected request must not count lines");
    assert_eq!(stats.bytes(), 0, "rejected request must not count bytes");
    assert_eq!(
        stats.errors(),
        0,
        "parse rejection should not count transport errors"
    );
    assert_eq!(
        stats.parse_errors(),
        1,
        "invalid protobuf should increment parse errors"
    );
}

#[test]
fn otlp_receiver_transport_rejections_increment_errors() {
    let stats = Arc::new(ComponentStats::new());
    let (mut input, url) = make_otlp_input(Arc::clone(&stats));

    let status = send_status(
        &url,
        b"{}",
        "application/json",
        Some("brotli-not-supported"),
    );
    assert_eq!(status, 415, "unsupported encoding must be rejected");

    std::thread::sleep(Duration::from_millis(20));
    let events = input.poll().expect("poll receiver after rejected request");
    assert!(
        events.is_empty(),
        "transport rejection must not enqueue decoded data"
    );
    assert_eq!(stats.lines(), 0, "rejected request must not count lines");
    assert_eq!(stats.bytes(), 0, "rejected request must not count bytes");
    assert_eq!(
        stats.errors(),
        1,
        "unsupported content-encoding should increment transport errors"
    );
    assert_eq!(
        stats.parse_errors(),
        0,
        "transport rejection should not increment parse errors"
    );
}

#[test]
fn otlp_receiver_accounts_gzip_body_bytes() {
    let request = semantic_request();
    let protobuf_body = request.encode_to_vec();
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    use std::io::Write as _;
    encoder
        .write_all(&protobuf_body)
        .expect("gzip write should succeed");
    let gzip_body = encoder.finish().expect("gzip finish should succeed");

    assert_accounted_bytes_for_payload(&gzip_body, "application/x-protobuf", Some("gzip"));
}

#[test]
fn otlp_receiver_accounts_zstd_body_bytes() {
    let request = semantic_request();
    let protobuf_body = request.encode_to_vec();
    let zstd_body = zstd::bulk::compress(&protobuf_body, 1).expect("zstd compress");

    assert_accounted_bytes_for_payload(&zstd_body, "application/x-protobuf", Some("zstd"));
}
