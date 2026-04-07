//! Contract tests for OTLP receiver semantics across wire variants.
//!
//! These tests verify that semantically equivalent OTLP requests preserve the
//! same decoded meaning across JSON, protobuf, and compressed protobuf paths.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::otlp_contract_support::expected_single_row_from_request;
use logfwd_io::diagnostics::ComponentStats;
use logfwd_io::format::FormatDecoder;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::otlp_receiver::OtlpReceiverInput;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;

fn poll_single_row(input: &mut dyn InputSource, timeout: Duration) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    let mut buf = Vec::new();

    while Instant::now() < deadline {
        for event in input.poll().expect("poll receiver") {
            if let InputEvent::Data { bytes, .. } = event {
                buf.extend_from_slice(&bytes);
            }
        }

        if let Some(last_newline) = buf.iter().rposition(|b| *b == b'\n') {
            let complete = buf.drain(..=last_newline).collect::<Vec<_>>();
            let mut rows = Vec::new();
            for line in complete
                .split(|b| *b == b'\n')
                .filter(|line| !line.is_empty())
            {
                let line = std::str::from_utf8(line).expect("receiver emits utf8 json");
                rows.push(
                    serde_json::from_str(line)
                        .unwrap_or_else(|e| panic!("valid json row: {e}; line={line}")),
                );
            }
            assert_eq!(
                rows.len(),
                1,
                "expected exactly one complete JSON row, got {}",
                rows.len()
            );
            return rows.pop().expect("one parsed row");
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for OTLP receiver data");
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

fn make_framed_otlp_input(stats: Arc<ComponentStats>, structured: bool) -> (FramedInput, String) {
    let receiver = if structured {
        OtlpReceiverInput::new_structured_with_stats("contract", "127.0.0.1:0", Arc::clone(&stats))
            .expect("structured receiver should start")
    } else {
        OtlpReceiverInput::new_with_stats("contract", "127.0.0.1:0", Arc::clone(&stats))
            .expect("legacy receiver should start")
    };
    let url = format!("http://{}/v1/logs", receiver.local_addr());
    let framed = FramedInput::new(
        Box::new(receiver),
        FormatDecoder::passthrough_json(Arc::clone(&stats)),
        stats,
    );
    (framed, url)
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
    for structured in [false, true] {
        let stats = Arc::new(ComponentStats::new());
        let (mut input, url) = make_framed_otlp_input(Arc::clone(&stats), structured);

        let status = send_status(&url, body, content_type, content_encoding);
        assert_eq!(status, 200, "request should succeed");

        let events = poll_until_events(&mut input, Duration::from_secs(2));
        assert!(
            !events.is_empty(),
            "receiver should emit at least one event"
        );
        assert_eq!(
            stats.bytes(),
            body.len() as u64,
            "receiver should charge the accepted request-body size at the input boundary"
        );
        assert_eq!(stats.lines(), 1, "one OTLP request should yield one row");
    }
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

#[test]
fn otlp_receiver_preserves_semantics_across_json_protobuf_zstd_and_gzip() {
    let mut receiver = OtlpReceiverInput::new("contract", "127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/logs", receiver.local_addr());

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
    let json_row = poll_single_row(&mut receiver, Duration::from_secs(2));

    let request = semantic_request();
    let expected_row = expected_single_row_from_request(&request);
    let protobuf_body = request.encode_to_vec();
    let status = send_status(&url, &protobuf_body, "application/x-protobuf", None);
    assert_eq!(status, 200, "protobuf OTLP request should succeed");
    let protobuf_row = poll_single_row(&mut receiver, Duration::from_secs(2));

    let compressed_body = zstd::bulk::compress(&protobuf_body, 1).expect("zstd compress");
    let status = send_status(
        &url,
        &compressed_body,
        "application/x-protobuf",
        Some("zstd"),
    );
    assert_eq!(status, 200, "zstd OTLP request should succeed");
    let zstd_row = poll_single_row(&mut receiver, Duration::from_secs(2));

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    use std::io::Write as _;
    encoder
        .write_all(&protobuf_body)
        .expect("gzip write should succeed");
    let gzip_body = encoder.finish().expect("gzip finish should succeed");
    let status = send_status(&url, &gzip_body, "application/x-protobuf", Some("gzip"));
    assert_eq!(status, 200, "gzip OTLP request should succeed");
    let gzip_row = poll_single_row(&mut receiver, Duration::from_secs(2));

    for row in [&json_row, &protobuf_row, &zstd_row, &gzip_row] {
        assert_eq!(
            row, &expected_row,
            "receiver output must match the official OTLP protobuf oracle"
        );
    }

    assert_eq!(json_row, protobuf_row, "json and protobuf must match");
    assert_eq!(
        protobuf_row, zstd_row,
        "compressed protobuf must match uncompressed protobuf"
    );
    assert_eq!(
        protobuf_row, gzip_row,
        "gzip protobuf must match uncompressed protobuf"
    );
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
fn otlp_receiver_legacy_and_structured_account_the_same_input_bytes() {
    let request = semantic_request();
    let protobuf_body = request.encode_to_vec();
    let expected_bytes = protobuf_body.len() as u64;

    for structured in [false, true] {
        let stats = Arc::new(ComponentStats::new());
        let (mut input, url) = make_framed_otlp_input(Arc::clone(&stats), structured);

        let status = send_status(&url, &protobuf_body, "application/x-protobuf", None);
        assert_eq!(status, 200, "request should succeed");

        let events = poll_until_events(&mut input, Duration::from_secs(2));
        if structured {
            assert!(
                events
                    .iter()
                    .any(|event| matches!(event, InputEvent::Batch { batch, .. } if batch.num_rows() == 1)),
                "structured ingress should emit one-row batch"
            );
        } else {
            assert!(
                events.iter().any(
                    |event| matches!(event, InputEvent::Data { bytes, .. } if !bytes.is_empty())
                ),
                "legacy ingress should emit framed data"
            );
        }

        assert_eq!(
            stats.lines(),
            1,
            "both OTLP modes should account one input row"
        );
        assert_eq!(
            stats.bytes(),
            expected_bytes,
            "both OTLP modes should account the accepted protobuf payload bytes"
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
}

#[test]
fn otlp_receiver_legacy_and_structured_rejections_increment_parse_errors() {
    for structured in [false, true] {
        let stats = Arc::new(ComponentStats::new());
        let (mut input, url) = make_framed_otlp_input(Arc::clone(&stats), structured);

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
}

#[test]
fn otlp_receiver_legacy_and_structured_account_gzip_body_bytes() {
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
fn otlp_receiver_legacy_and_structured_account_zstd_body_bytes() {
    let request = semantic_request();
    let protobuf_body = request.encode_to_vec();
    let zstd_body = zstd::bulk::compress(&protobuf_body, 1).expect("zstd compress");

    assert_accounted_bytes_for_payload(&zstd_body, "application/x-protobuf", Some("zstd"));
}

#[test]
#[should_panic(expected = "expected exactly one complete JSON row")]
fn poll_single_row_panics_when_one_post_emits_multiple_rows() {
    let mut receiver = OtlpReceiverInput::new("contract", "127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/logs", receiver.local_addr());

    let json_body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [
                    {"body": {"stringValue": "first"}},
                    {"body": {"stringValue": "second"}}
                ]
            }]
        }]
    })
    .to_string();

    let status = send_status(&url, json_body.as_bytes(), "application/json", None);
    assert_eq!(status, 200, "multi-record OTLP request should still decode");

    let _ = poll_single_row(&mut receiver, Duration::from_secs(2));
}
