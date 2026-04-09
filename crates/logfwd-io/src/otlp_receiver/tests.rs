use super::*;
use arrow::array::{Array, BooleanArray, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use bytes::Bytes;
use logfwd_arrow::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use proptest::prelude::*;
use prost::Message;
use std::time::{Duration, Instant};

fn make_test_request() -> Vec<u8> {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![
                    LogRecord {
                        time_unix_nano: 1_705_314_600_000_000_000,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("hello world".into())),
                        }),
                        attributes: vec![
                            KeyValue {
                                key: "service".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("myapp".into())),
                                }),
                            },
                            KeyValue {
                                key: "payload".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::BytesValue(vec![0x01, 0x02, 0x03, 0x04])),
                                }),
                            },
                        ],
                        ..Default::default()
                    },
                    LogRecord {
                        severity_text: "ERROR".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("something broke".into())),
                        }),
                        attributes: vec![KeyValue {
                            key: "status".into(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(500)),
                            }),
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    request.encode_to_vec()
}

fn wait_until<F>(timeout: Duration, mut predicate: F, failure_message: &str)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(predicate(), "{failure_message}");
}

fn poll_receiver_until<F>(
    receiver: &mut OtlpReceiverInput,
    timeout: Duration,
    mut predicate: F,
    failure_message: &str,
) -> Vec<InputEvent>
where
    F: FnMut(&[InputEvent]) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let events = receiver.poll().unwrap();
        if predicate(&events) {
            return events;
        }
        assert!(Instant::now() < deadline, "{failure_message}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..=max_len)
        .prop_map(|chars| chars.into_iter().collect())
}

fn non_empty_unicode_string(max_len: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 1..=max_len)
        .prop_map(|chars| chars.into_iter().collect())
}

fn any_value_to_string_simple(v: &AnyValue) -> Option<String> {
    match &v.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::BytesValue(b)) => Some(hex::encode(b)),
        _ => None,
    }
}

fn any_value_to_json_simple(value: &AnyValue) -> Option<serde_json::Value> {
    match &value.value {
        Some(Value::IntValue(v)) => Some(serde_json::Value::from(*v)),
        Some(Value::DoubleValue(v)) => {
            let mut buf = Vec::new();
            write_f64_to_buf_simple(&mut buf, *v);
            serde_json::from_slice(&buf).ok()
        }
        Some(Value::BoolValue(v)) => Some(serde_json::Value::from(*v)),
        Some(Value::StringValue(v)) => Some(serde_json::Value::String(v.clone())),
        Some(Value::BytesValue(v)) => Some(serde_json::Value::String(hex::encode(v))),
        _ => None,
    }
}

fn convert_request_to_json_lines_simple(request: &ExportLogsServiceRequest) -> Vec<u8> {
    let mut out = Vec::new();

    for resource_logs in &request.resource_logs {
        let mut resource_attrs: Vec<(String, String)> = Vec::new();
        if let Some(resource) = &resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(value) = &attr.value
                    && let Some(stringified) = any_value_to_string_simple(value)
                {
                    resource_attrs.push((attr.key.clone(), stringified));
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                let mut obj = serde_json::Map::new();

                if record.time_unix_nano > 0 {
                    obj.insert(
                        field_names::TIMESTAMP.to_string(),
                        serde_json::Value::from(record.time_unix_nano),
                    );
                }

                if !record.severity_text.is_empty() {
                    obj.insert(
                        field_names::SEVERITY.to_string(),
                        serde_json::Value::String(record.severity_text.clone()),
                    );
                }

                if let Some(body) = &record.body
                    && let Some(body_str) = any_value_to_string_simple(body)
                {
                    obj.insert(
                        field_names::BODY.to_string(),
                        serde_json::Value::String(body_str),
                    );
                }

                for (key, value) in &resource_attrs {
                    obj.insert(key.clone(), serde_json::Value::String(value.clone()));
                }

                for attr in &record.attributes {
                    if let Some(value) = &attr.value
                        && let Some(json_value) = any_value_to_json_simple(value)
                    {
                        obj.insert(attr.key.clone(), json_value);
                    }
                }

                if !record.trace_id.is_empty() {
                    obj.insert(
                        field_names::TRACE_ID.to_string(),
                        serde_json::Value::String(hex::encode(&record.trace_id)),
                    );
                }
                if !record.span_id.is_empty() {
                    obj.insert(
                        field_names::SPAN_ID.to_string(),
                        serde_json::Value::String(hex::encode(&record.span_id)),
                    );
                }

                serde_json::to_writer(&mut out, &serde_json::Value::Object(obj))
                    .expect("json serialization should succeed");
                out.push(b'\n');
            }
        }
    }

    out
}

fn parse_json_lines_values(bytes: &[u8]) -> Vec<serde_json::Value> {
    if bytes.is_empty() {
        return Vec::new();
    }
    std::str::from_utf8(bytes)
        .expect("json lines must be valid UTF-8")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid json line"))
        .collect()
}

fn any_value_strategy() -> impl Strategy<Value = AnyValue> {
    prop_oneof![
        unicode_string(20).prop_map(|s| AnyValue {
            value: Some(Value::StringValue(s)),
        }),
        any::<i64>().prop_map(|v| AnyValue {
            value: Some(Value::IntValue(v)),
        }),
        prop_oneof![
            (-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 100.0),
            Just(f64::NAN),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
        ]
        .prop_map(|v| AnyValue {
            value: Some(Value::DoubleValue(v)),
        }),
        any::<bool>().prop_map(|b| AnyValue {
            value: Some(Value::BoolValue(b)),
        }),
        proptest::collection::vec(any::<u8>(), 0..16).prop_map(|b| AnyValue {
            value: Some(Value::BytesValue(b)),
        }),
        Just(AnyValue { value: None }),
    ]
}

fn key_value_strategy() -> impl Strategy<Value = KeyValue> {
    (
        non_empty_unicode_string(16),
        prop::option::of(any_value_strategy()),
    )
        .prop_map(|(key, value)| KeyValue { key, value })
}

fn log_record_strategy() -> impl Strategy<Value = LogRecord> {
    (
        any::<u64>(),
        unicode_string(8),
        prop::option::of(any_value_strategy()),
        proptest::collection::vec(key_value_strategy(), 0..6),
        proptest::collection::vec(any::<u8>(), 0..20),
        proptest::collection::vec(any::<u8>(), 0..12),
    )
        .prop_map(
            |(time_unix_nano, severity_text, body, attributes, trace_id, span_id)| LogRecord {
                time_unix_nano,
                severity_text,
                body,
                attributes,
                trace_id,
                span_id,
                ..Default::default()
            },
        )
}

fn scope_logs_strategy() -> impl Strategy<Value = ScopeLogs> {
    proptest::collection::vec(log_record_strategy(), 0..6).prop_map(|log_records| ScopeLogs {
        log_records,
        ..Default::default()
    })
}

fn resource_logs_strategy() -> impl Strategy<Value = ResourceLogs> {
    (
        proptest::collection::vec(key_value_strategy(), 0..6),
        proptest::collection::vec(scope_logs_strategy(), 0..3),
    )
        .prop_map(|(resource_attributes, scope_logs)| ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes,
                ..Default::default()
            }),
            scope_logs,
            ..Default::default()
        })
}

fn request_strategy() -> impl Strategy<Value = ExportLogsServiceRequest> {
    proptest::collection::vec(resource_logs_strategy(), 0..4)
        .prop_map(|resource_logs| ExportLogsServiceRequest { resource_logs })
}

#[test]
fn structured_batch_matches_legacy_scanned_batch() {
    let body = make_test_request();
    let json_lines = decode_otlp_logs(&body).expect("legacy decode succeeds");
    let structured = decode_otlp_logs_to_batch(&body).expect("structured decode succeeds");

    let mut scanner = Scanner::new(ScanConfig::default());
    let legacy = scanner
        .scan(Bytes::from(json_lines))
        .expect("legacy JSON lines scan");

    assert_eq!(legacy.num_columns(), structured.num_columns());
    for idx in 0..legacy.num_columns() {
        assert_eq!(
            legacy.schema().field(idx).name(),
            structured.schema().field(idx).name()
        );
        assert_eq!(
            column_values(legacy.column(idx).as_ref()),
            column_values(structured.column(idx).as_ref())
        );
    }
}

proptest! {
    #[test]
    fn proptest_convert_request_to_json_lines_fast_matches_simple(
        request in request_strategy()
    ) {
        let fast = convert_request_to_json_lines(&request);
        let simple = convert_request_to_json_lines_simple(&request);

        prop_assert_eq!(
            parse_json_lines_values(&fast),
            parse_json_lines_values(&simple),
            "convert_request_to_json_lines fast path drifted from simple reference"
        );
    }
}

fn column_values(array: &dyn Array) -> Vec<String> {
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    "NULL".to_string()
                } else {
                    array.value(idx).to_string()
                }
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    "NULL".to_string()
                } else {
                    array.value(idx).to_string()
                }
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    "NULL".to_string()
                } else {
                    array.value(idx).to_string()
                }
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    "NULL".to_string()
                } else {
                    array.value(idx).to_string()
                }
            })
            .collect();
    }
    panic!("unsupported test array type: {:?}", array.data_type());
}

#[test]
fn structured_batch_preserves_boolean_type_and_dotted_attributes() {
    let request = ExportLogsServiceRequest {
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
                    attributes: vec![KeyValue {
                        key: "sampled".into(),
                        value: Some(AnyValue {
                            value: Some(Value::BoolValue(true)),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let batch = convert_request_to_batch(&request).expect("structured decode succeeds");

    let sampled = batch
        .column_by_name("sampled")
        .expect("sampled column must exist");
    assert_eq!(sampled.data_type(), &DataType::Boolean);
    let sampled = sampled
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("sampled must be BooleanArray");
    assert!(sampled.value(0), "sampled=true must be preserved as bool");

    let service_name = batch
        .column_by_name("service.name")
        .expect("dotted attribute must keep original column name");
    let service_name = service_name
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("service.name should remain a flat string column");
    assert_eq!(service_name.value(0), "checkout-api");
    assert!(
        batch.column_by_name("service_name").is_none(),
        "dotted attributes must not require sanitized internal-name coupling"
    );
}

#[test]
fn structured_batch_preserves_resource_boolean_type() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "resource.sampled".into(),
                    value: Some(AnyValue {
                        value: Some(Value::BoolValue(true)),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord::default()],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let batch = convert_request_to_batch(&request).expect("structured decode succeeds");
    let sampled = batch
        .column_by_name("resource.sampled")
        .expect("resource.sampled must exist");
    assert_eq!(sampled.data_type(), &DataType::Boolean);
    let sampled = sampled
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("resource.sampled must be BooleanArray");
    assert!(sampled.value(0), "resource bool attribute must stay typed");
}

#[test]
fn decodes_otlp_to_json_lines() {
    let body = make_test_request();
    let json = decode_otlp_logs(&body).unwrap();
    let text = String::from_utf8(json).unwrap();
    let lines: Vec<&str> = text.lines().collect();

    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"level\":\"INFO\""), "got: {}", lines[0]);
    assert!(
        lines[0].contains("\"message\":\"hello world\""),
        "got: {}",
        lines[0]
    );
    assert!(
        lines[0].contains("\"service\":\"myapp\""),
        "got: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("\"level\":\"ERROR\""),
        "got: {}",
        lines[1]
    );
    assert!(lines[1].contains("\"status\":500"), "got: {}", lines[1]);
}

/// Contract test: protobuf and JSON OTLP inputs that represent the same
/// records must produce the same semantic JSON-line output.
#[test]
fn protobuf_and_json_inputs_match_semantics() {
    let protobuf_lines = decode_otlp_logs(&make_test_request()).unwrap();

    let json_body = r#"{
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [
                    {
                        "timeUnixNano": "1705314600000000000",
                        "severityText": "INFO",
                        "body": {"stringValue": "hello world"},
                        "attributes": [
                            {
                                "key": "service",
                                "value": {"stringValue": "myapp"}
                            },
                            {
                                "key": "payload",
                                "value": {"bytesValue": "AQIDBA=="}
                            }
                        ]
                    },
                    {
                        "severityText": "ERROR",
                        "body": {"stringValue": "something broke"},
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": "500"}
                        }]
                    }
                ]
            }]
        }]
    }"#;
    let json_lines = decode_otlp_logs_json(json_body.as_bytes()).unwrap();

    let parse = |lines: &[u8]| -> Vec<serde_json::Value> {
        String::from_utf8(lines.to_vec())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect()
    };

    let left = parse(&protobuf_lines);
    let right = parse(&json_lines);
    assert_eq!(left.len(), 2, "expected 2 protobuf-decoded rows");
    assert_eq!(right.len(), 2, "expected 2 json-decoded rows");

    for (lhs, rhs) in left.iter().zip(right.iter()) {
        assert_eq!(lhs.get("level"), rhs.get("level"));
        assert_eq!(lhs.get("message"), rhs.get("message"));
        assert_eq!(lhs.get("service"), rhs.get("service"));
        assert_eq!(lhs.get("status"), rhs.get("status"));
        assert_eq!(lhs.get("payload"), rhs.get("payload"));
    }
}

#[test]
fn record_attributes_override_resource_attributes_in_protobuf_paths() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("resource".into())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("record".into())),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let json_lines = decode_otlp_logs(&request.encode_to_vec()).expect("protobuf decode");
    let rows = parse_json_lines_values(&json_lines);
    assert_eq!(rows.len(), 1, "expected one decoded row");
    assert_eq!(
        rows[0]
            .get("service.name")
            .and_then(serde_json::Value::as_str),
        Some("record"),
        "record attribute must override same-key resource attribute in JSON lines"
    );

    let batch = convert_request_to_batch(&request).expect("structured batch decode");
    let service_name = batch
        .column_by_name("service.name")
        .expect("service.name column should exist");
    let service_name = service_name
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("service.name must be a string column");
    assert_eq!(
        service_name.value(0),
        "record",
        "record attribute must override same-key resource attribute in structured batch"
    );
}

#[test]
fn record_attributes_override_resource_attributes_in_json_input_path() {
    let json_body = serde_json::json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {"stringValue": "resource"}
                }]
            },
            "scopeLogs": [{
                "logRecords": [{
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "record"}
                    }]
                }]
            }]
        }]
    });

    let json_lines = decode_otlp_logs_json(json_body.to_string().as_bytes()).expect("json decode");
    let rows = parse_json_lines_values(&json_lines);
    assert_eq!(rows.len(), 1, "expected one decoded row");
    assert_eq!(
        rows[0]
            .get("service.name")
            .and_then(serde_json::Value::as_str),
        Some("record"),
        "record attribute must override same-key resource attribute for OTLP JSON input"
    );
}

#[test]
fn json_bytes_value_matches_protobuf_semantics() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::BytesValue(vec![0x01, 0x02, 0x03, 0x04])),
                    }),
                    attributes: vec![KeyValue {
                        key: "payload".into(),
                        value: Some(AnyValue {
                            value: Some(Value::BytesValue(vec![0x0a, 0x0b, 0x0c])),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let protobuf_lines = decode_otlp_logs(&request.encode_to_vec()).unwrap();
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "body": {"bytesValue": "AQIDBA=="},
                        "attributes": [{
                            "key": "payload",
                            "value": {"bytesValue": "CgsM"}
                        }]
                    }]
                }]
            }]
        }"#,
    )
    .unwrap();

    let parse_first = |lines: &[u8]| -> serde_json::Value {
        let line = String::from_utf8(lines.to_vec())
            .unwrap()
            .lines()
            .next()
            .expect("one decoded row")
            .to_string();
        serde_json::from_str(&line).unwrap()
    };

    let left = parse_first(&protobuf_lines);
    let right = parse_first(&json_lines);

    assert_eq!(
        left.get("message").and_then(serde_json::Value::as_str),
        Some("01020304")
    );
    assert_eq!(left.get("message"), right.get("message"));
    assert_eq!(left.get("payload"), right.get("payload"));
}

#[test]
fn invalid_json_bytes_value_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "payload",
                            "value": {"bytesValue": "***not-base64***"}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(result.is_err(), "invalid base64 bytesValue must fail");
}

#[test]
fn json_bytes_value_accepts_urlsafe_and_unpadded_base64() {
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "payload",
                            "value": {"bytesValue": "-_8"}
                        }]
                    }]
                }]
            }]
        }"#,
    )
    .expect("urlsafe unpadded base64 should decode");

    let line = String::from_utf8(json_lines).expect("utf8");
    let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(
        row.get("payload").and_then(serde_json::Value::as_str),
        Some("fbff")
    );
}

#[test]
fn invalid_json_int_value_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": true}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(result.is_err(), "invalid intValue must fail");
}

#[test]
fn invalid_json_double_value_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "latency_ratio",
                            "value": {"doubleValue": "not-a-double"}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(result.is_err(), "invalid doubleValue must fail");
}

#[test]
fn non_numeric_json_int_string_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": "notanumber"}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(
        result.is_err(),
        "non-numeric intValue string must fail instead of emitting malformed JSON"
    );
}

#[test]
fn exponent_form_json_int_value_is_accepted() {
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": "5e2"}
                        }]
                    }]
                }]
            }]
        }"#,
    )
    .expect("ProtoJSON exponent intValue should decode");

    let line = String::from_utf8(json_lines).expect("utf8");
    let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(
        row.get("status").and_then(serde_json::Value::as_i64),
        Some(500)
    );
}

#[test]
fn bare_exponent_json_int_value_is_accepted() {
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": 1e2}
                        }]
                    }]
                }]
            }]
        }"#,
    )
    .expect("bare exponent intValue should decode");

    let line = String::from_utf8(json_lines).expect("utf8");
    let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(
        row.get("status").and_then(serde_json::Value::as_i64),
        Some(100)
    );
}

#[test]
fn huge_positive_exponent_json_int_value_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": "1e2147483647"}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(result.is_err(), "huge exponent intValue must fail");
}

#[test]
fn huge_negative_exponent_json_int_value_returns_error_without_panicking() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": "1e-2147483648"}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(
        result.is_err(),
        "extreme negative exponent intValue must fail without panicking"
    );
}

#[test]
fn protojson_integral_normalization_accepts_integral_decimal_forms() {
    assert_eq!(
        normalize_protojson_integral_digits(" +001.2300e+2 "),
        Some((false, "123".to_string()))
    );
    assert_eq!(
        normalize_protojson_integral_digits("-9223372036854775808"),
        Some((true, "9223372036854775808".to_string()))
    );
    assert_eq!(
        normalize_protojson_integral_digits("0.000e+999999"),
        Some((false, "0".to_string()))
    );
}

#[test]
fn protojson_integral_normalization_rejects_non_integral_or_oversized_forms() {
    assert_eq!(normalize_protojson_integral_digits("1.5"), None);
    assert_eq!(normalize_protojson_integral_digits("1e-1"), None);
    assert_eq!(normalize_protojson_integral_digits("1e20"), None);
    assert_eq!(normalize_protojson_integral_digits("1e2147483647"), None);
}

#[test]
fn out_of_range_json_int_value_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "status",
                            "value": {"intValue": "9223372036854775808"}
                        }]
                    }]
                }]
            }]
        }"#,
    );

    assert!(result.is_err(), "out-of-range intValue must fail");
}

#[test]
fn invalid_json_time_unix_nano_returns_error() {
    let result = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "not-a-number"
                    }]
                }]
            }]
        }"#,
    );

    assert!(result.is_err(), "invalid timeUnixNano must fail");
}

#[test]
fn zero_json_time_unix_nano_is_accepted_and_omitted() {
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "0",
                        "body": {"stringValue": "hello"}
                    }]
                }]
            }]
        }"#,
    )
    .expect("zero timeUnixNano should be accepted");

    let line = String::from_utf8(json_lines).expect("utf8");
    let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(
        row.get(field_names::BODY)
            .and_then(serde_json::Value::as_str),
        Some("hello")
    );
    assert!(
        row.get(field_names::TIMESTAMP).is_none(),
        "unknown timestamp should be omitted, not emitted as 0"
    );
}

/// JSON OTLP path: when timeUnixNano is 0 but observedTimeUnixNano is set,
/// the timestamp must use the observed time (issue #1690).
#[test]
fn json_path_uses_observed_time_when_event_time_is_zero() {
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "0",
                        "observedTimeUnixNano": "1705314700000000000",
                        "body": {"stringValue": "hello"}
                    }]
                }]
            }]
        }"#,
    )
    .expect("valid OTLP JSON");

    let line = String::from_utf8(json_lines).expect("utf8");
    let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(
        row.get(field_names::TIMESTAMP)
            .and_then(serde_json::Value::as_u64),
        Some(1_705_314_700_000_000_000),
        "JSON path must fall back to observedTimeUnixNano when timeUnixNano==0"
    );
}

#[test]
fn special_float_strings_match_protobuf_semantics() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::DoubleValue(f64::NAN)),
                    }),
                    attributes: vec![
                        KeyValue {
                            key: "nan_attr".into(),
                            value: Some(AnyValue {
                                value: Some(Value::DoubleValue(f64::NAN)),
                            }),
                        },
                        KeyValue {
                            key: "pos_inf".into(),
                            value: Some(AnyValue {
                                value: Some(Value::DoubleValue(f64::INFINITY)),
                            }),
                        },
                        KeyValue {
                            key: "neg_inf".into(),
                            value: Some(AnyValue {
                                value: Some(Value::DoubleValue(f64::NEG_INFINITY)),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let protobuf_lines = decode_otlp_logs(&request.encode_to_vec()).unwrap();
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "body": {"doubleValue": "NaN"},
                        "attributes": [
                            {"key": "nan_attr", "value": {"doubleValue": "NaN"}},
                            {"key": "pos_inf", "value": {"doubleValue": "Infinity"}},
                            {"key": "neg_inf", "value": {"doubleValue": "-Infinity"}}
                        ]
                    }]
                }]
            }]
        }"#,
    )
    .expect("special float string tokens should decode");

    let parse_first = |lines: &[u8]| -> serde_json::Value {
        let line = String::from_utf8(lines.to_vec())
            .unwrap()
            .lines()
            .next()
            .expect("one decoded row")
            .to_string();
        serde_json::from_str(&line).unwrap()
    };

    let protobuf_row = parse_first(&protobuf_lines);
    let json_row = parse_first(&json_lines);
    assert_eq!(protobuf_row, json_row);
    assert_eq!(
        json_row.get("message").and_then(serde_json::Value::as_str),
        Some("NaN")
    );
    assert!(
        json_row
            .get("nan_attr")
            .is_some_and(serde_json::Value::is_null)
    );
    assert!(
        json_row
            .get("pos_inf")
            .is_some_and(serde_json::Value::is_null)
    );
    assert!(
        json_row
            .get("neg_inf")
            .is_some_and(serde_json::Value::is_null)
    );
}

#[test]
fn empty_string_body_and_unsupported_values_preserve_wire_equivalence() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    KeyValue {
                        key: "empty_resource".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(String::new())),
                        }),
                    },
                    KeyValue {
                        key: "unsupported_resource".into(),
                        value: Some(AnyValue {
                            value: Some(Value::ArrayValue(Default::default())),
                        }),
                    },
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::StringValue(String::new())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let protobuf_lines = decode_otlp_logs(&request.encode_to_vec()).unwrap();
    let json_lines = decode_otlp_logs_json(
        br#"{
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        {"key": "empty_resource", "value": {"stringValue": ""}},
                        {"key": "unsupported_resource", "value": {"arrayValue": {"values": []}}}
                    ]
                },
                "scopeLogs": [{
                    "logRecords": [{
                        "body": {"stringValue": ""}
                    }]
                }]
            }]
        }"#,
    )
    .unwrap();

    let parse_first = |lines: &[u8]| -> serde_json::Value {
        let line = String::from_utf8(lines.to_vec())
            .unwrap()
            .lines()
            .next()
            .expect("one decoded row")
            .to_string();
        serde_json::from_str(&line).unwrap()
    };

    let protobuf_row = parse_first(&protobuf_lines);
    let json_row = parse_first(&json_lines);
    assert_eq!(protobuf_row, json_row);
    assert_eq!(
        protobuf_row
            .get("empty_resource")
            .and_then(serde_json::Value::as_str),
        Some("")
    );
    assert!(protobuf_row.get("unsupported_resource").is_none());
    assert_eq!(
        protobuf_row
            .get("message")
            .and_then(serde_json::Value::as_str),
        Some("")
    );
}

#[test]
fn handles_invalid_protobuf() {
    let result = decode_otlp_logs(b"not valid protobuf");
    assert!(result.is_err());
}

#[test]
fn invalid_protobuf_increments_parse_errors_when_stats_hooked() {
    let stats = Arc::new(ComponentStats::new());
    let receiver = OtlpReceiverInput::new_with_capacity_and_stats(
        "test",
        "127.0.0.1:0",
        16,
        Arc::clone(&stats),
    )
    .unwrap();
    let url = format!("http://{}/v1/logs", receiver.local_addr());

    let status = match ureq::post(&url)
        .header("content-type", "application/x-protobuf")
        .send(b"not valid protobuf".as_slice())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected transport error: {e}"),
    };

    assert_eq!(status, 400);
    assert_eq!(stats.parse_errors(), 1);
    assert_eq!(stats.errors(), 0);
}

#[test]
fn handles_empty_body() {
    let json = decode_otlp_logs(b"").unwrap();
    assert!(json.is_empty());
}

#[test]
fn handles_request_with_no_log_records() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body = request.encode_to_vec();
    let json = decode_otlp_logs(&body).unwrap();
    assert!(json.is_empty());
}

#[test]
fn handles_record_with_no_body() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    severity_text: "WARN".into(),
                    body: None,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body = request.encode_to_vec();
    let json = decode_otlp_logs(&body).unwrap();
    let text = String::from_utf8(json).unwrap();
    assert!(text.contains("\"level\":\"WARN\""));
    assert!(!text.contains("\"message\""));
}

#[test]
fn hex_encode_empty() {
    assert_eq!(hex::encode(&[]), "");
}

#[test]
fn write_hex_to_buf_uses_lowercase_pairs() {
    let mut out = Vec::new();
    write_hex_to_buf(&mut out, &[0x00, 0xab, 0xff]);
    assert_eq!(String::from_utf8(out).unwrap(), "00abff");
}

#[test]
fn integer_writers_emit_canonical_decimal_strings() {
    let mut out = Vec::new();

    write_u64_to_buf(&mut out, 0);
    assert_eq!(String::from_utf8(out.clone()).unwrap(), "0");

    out.clear();
    write_u64_to_buf(&mut out, 42);
    assert_eq!(String::from_utf8(out.clone()).unwrap(), "42");

    out.clear();
    write_u64_to_buf(&mut out, u64::MAX);
    assert_eq!(
        String::from_utf8(out.clone()).unwrap(),
        u64::MAX.to_string()
    );

    out.clear();
    write_i64_to_buf(&mut out, -17);
    assert_eq!(String::from_utf8(out.clone()).unwrap(), "-17");

    out.clear();
    write_i64_to_buf(&mut out, i64::MIN);
    assert_eq!(String::from_utf8(out).unwrap(), i64::MIN.to_string());
}

/// Local microbenchmark for OTLP request -> NDJSON conversion.
///
/// Run with:
/// `cargo test -p logfwd-io otlp_receiver::tests::bench_convert_request_to_json_lines_fast_vs_simple --release -- --ignored --nocapture`
#[test]
#[ignore = "microbenchmark"]
fn bench_convert_request_to_json_lines_fast_vs_simple() {
    use std::hint::black_box;
    use std::time::Instant;

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("bench".into())),
                        }),
                    },
                    KeyValue {
                        key: "service.instance".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("instance-1".into())),
                        }),
                    },
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: (0..2_000)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_710_000_000_000_000_000 + i as u64,
                        severity_text: match i % 4 {
                            0 => "INFO".into(),
                            1 => "WARN".into(),
                            2 => "ERROR".into(),
                            _ => "DEBUG".into(),
                        },
                        body: Some(AnyValue {
                            value: Some(Value::StringValue(format!("message-{i}"))),
                        }),
                        attributes: vec![
                            KeyValue {
                                key: "status".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::IntValue(200 + (i % 5) as i64)),
                                }),
                            },
                            KeyValue {
                                key: "latency".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::DoubleValue((i % 1000) as f64 / 10.0)),
                                }),
                            },
                            KeyValue {
                                key: "active".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::BoolValue(i % 2 == 0)),
                                }),
                            },
                        ],
                        trace_id: if i % 3 == 0 {
                            Vec::new()
                        } else {
                            vec![
                                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
                                0x0c, 0x0d, 0x0e, 0x0f, 0x10,
                            ]
                        },
                        span_id: if i % 5 == 0 {
                            Vec::new()
                        } else {
                            vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
                        },
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let fast_once = convert_request_to_json_lines(&request);
    let simple_once = convert_request_to_json_lines_simple(&request);
    assert_eq!(
        parse_json_lines_values(&fast_once),
        parse_json_lines_values(&simple_once),
        "fast and simple converters must remain semantically equivalent"
    );

    const ITERS: usize = 80;
    let t0 = Instant::now();
    for _ in 0..ITERS {
        let out = convert_request_to_json_lines(&request);
        black_box(out.len());
    }
    let fast = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..ITERS {
        let out = convert_request_to_json_lines_simple(&request);
        black_box(out.len());
    }
    let simple = t0.elapsed();

    eprintln!(
        "convert_request_to_json_lines bench records={} iters={ITERS}",
        request.resource_logs[0].scope_logs[0].log_records.len()
    );
    eprintln!("  fast={:?}", fast);
    eprintln!("  simple={:?}", simple);
}

proptest! {
    #[test]
    fn proptest_write_i64_fast_matches_simple(n in any::<i64>(), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
        let mut fast = prefix.clone();
        let mut simple = prefix;
        write_i64_to_buf(&mut fast, n);
        write_i64_to_buf_simple(&mut simple, n);
        prop_assert_eq!(fast, simple);
    }

    #[test]
    fn proptest_write_u64_fast_matches_simple(n in any::<u64>(), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
        let mut fast = prefix.clone();
        let mut simple = prefix;
        write_u64_to_buf(&mut fast, n);
        write_u64_to_buf_simple(&mut simple, n);
        prop_assert_eq!(fast, simple);
    }

    #[test]
    fn proptest_write_f64_fast_matches_simple(bits in any::<u64>(), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
        let d = f64::from_bits(bits);
        let mut fast = prefix.clone();
        let mut simple = prefix;
        write_f64_to_buf(&mut fast, d);
        write_f64_to_buf_simple(&mut simple, d);
        prop_assert_eq!(fast, simple);
    }

    #[test]
    fn proptest_write_hex_fast_matches_simple(bytes in proptest::collection::vec(any::<u8>(), 0..256), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
        let mut fast = prefix.clone();
        let mut simple = prefix;
        write_hex_to_buf(&mut fast, &bytes);
        write_hex_to_buf_simple(&mut simple, &bytes);
        prop_assert_eq!(fast, simple);
    }
}

/// Local microbenchmark for writer helpers.
///
/// Run with:
/// `cargo test -p logfwd-io otlp_receiver::tests::bench_writer_helpers_fast_vs_simple --release -- --ignored --nocapture`
#[test]
#[ignore = "microbenchmark"]
fn bench_writer_helpers_fast_vs_simple() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 200_000;
    let i64_inputs: Vec<i64> = (0..N)
        .map(|i| ((i as i64).wrapping_mul(1_048_573)).wrapping_sub(73_421))
        .collect();
    let u64_inputs: Vec<u64> = (0..N)
        .map(|i| (i as u64).wrapping_mul(2_654_435_761))
        .collect();
    let f64_inputs: Vec<f64> = (0..N)
        .map(|i| {
            let r = (i as f64) * 0.125 - 17_333.75;
            if i % 97 == 0 {
                f64::NAN
            } else if i % 89 == 0 {
                f64::INFINITY
            } else if i % 83 == 0 {
                f64::NEG_INFINITY
            } else {
                r
            }
        })
        .collect();
    let hex_inputs: Vec<Vec<u8>> = (0..N)
        .map(|i| {
            let x = (i as u64).wrapping_mul(11_400_714_819_323_198_485);
            x.to_le_bytes().to_vec()
        })
        .collect();

    let mut buf = Vec::with_capacity(256 * 1024);

    let t0 = Instant::now();
    for &n in &i64_inputs {
        buf.clear();
        write_i64_to_buf(&mut buf, n);
        black_box(buf.len());
    }
    let i64_fast = t0.elapsed();

    let t0 = Instant::now();
    for &n in &i64_inputs {
        buf.clear();
        write_i64_to_buf_simple(&mut buf, n);
        black_box(buf.len());
    }
    let i64_simple = t0.elapsed();

    let t0 = Instant::now();
    for &n in &u64_inputs {
        buf.clear();
        write_u64_to_buf(&mut buf, n);
        black_box(buf.len());
    }
    let u64_fast = t0.elapsed();

    let t0 = Instant::now();
    for &n in &u64_inputs {
        buf.clear();
        write_u64_to_buf_simple(&mut buf, n);
        black_box(buf.len());
    }
    let u64_simple = t0.elapsed();

    let t0 = Instant::now();
    for &d in &f64_inputs {
        buf.clear();
        write_f64_to_buf(&mut buf, d);
        black_box(buf.len());
    }
    let f64_fast = t0.elapsed();

    let t0 = Instant::now();
    for &d in &f64_inputs {
        buf.clear();
        write_f64_to_buf_simple(&mut buf, d);
        black_box(buf.len());
    }
    let f64_simple = t0.elapsed();

    let t0 = Instant::now();
    for v in &hex_inputs {
        buf.clear();
        write_hex_to_buf(&mut buf, v);
        black_box(buf.len());
    }
    let hex_fast = t0.elapsed();

    let t0 = Instant::now();
    for v in &hex_inputs {
        buf.clear();
        write_hex_to_buf_simple(&mut buf, v);
        black_box(buf.len());
    }
    let hex_simple = t0.elapsed();

    eprintln!("writer bench N={N}");
    eprintln!("  i64  fast={:?} simple={:?}", i64_fast, i64_simple);
    eprintln!("  u64  fast={:?} simple={:?}", u64_fast, u64_simple);
    eprintln!("  f64  fast={:?} simple={:?}", f64_fast, f64_simple);
    eprintln!("  hex  fast={:?} simple={:?}", hex_fast, hex_simple);
}

#[test]
fn json_escaping_control_chars() {
    // Build a string with all control chars that are valid single-byte UTF-8 (0x00-0x1f all are).
    let ctrl: String = (0u8..=0x1f).map(|b| b as char).collect();
    let mut out = Vec::new();
    write_json_string_field(&mut out, "k", &ctrl);
    let text = String::from_utf8(out).unwrap();

    // No raw control bytes should appear in the output.
    for b in text.as_bytes() {
        // The only bytes < 0x20 allowed are the literal `"` delimiters… but `"` is 0x22.
        // So nothing < 0x20 should appear at all.
        assert!(
            *b >= 0x20,
            "raw control byte 0x{:02x} found in output: {text}",
            b
        );
    }

    // Spot-check specific escapes.
    assert!(text.contains(r"\u0000"), "NUL not escaped: {text}");
    assert!(text.contains(r"\u0001"), "SOH not escaped: {text}");
    assert!(text.contains(r"\u0008"), "BS not escaped: {text}");
    assert!(text.contains(r"\t"), "TAB not escaped: {text}");
    assert!(text.contains(r"\n"), "LF not escaped: {text}");
    assert!(text.contains(r"\r"), "CR not escaped: {text}");
    assert!(text.contains(r"\u000c"), "FF not escaped: {text}");
}

#[test]
fn json_escaping_unicode() {
    // Multi-byte UTF-8 should pass through unchanged.
    let input = "hello \u{00e9}\u{1f600} world \u{4e16}\u{754c}";
    let mut out = Vec::new();
    write_json_string_field(&mut out, "k", input);
    let text = String::from_utf8(out).unwrap();

    // The multi-byte chars should appear literally (not \u-escaped).
    assert!(text.contains('\u{00e9}'), "e-acute missing: {text}");
    assert!(text.contains('\u{1f600}'), "emoji missing: {text}");
    assert!(text.contains('\u{4e16}'), "CJK char missing: {text}");

    // Verify the whole thing is valid JSON.
    let json_str = format!("{{{text}}}");
    serde_json::from_str::<serde_json::Value>(&json_str)
        .unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
}

#[test]
fn json_escaping_key_chars() {
    let mut out = Vec::new();
    write_json_string_field(&mut out, "my\"key\\path", "value");
    let text = String::from_utf8(out).unwrap();
    let json_str = format!("{{{text}}}");
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
    assert_eq!(parsed["my\"key\\path"], "value");
}

/// Regression test: when the pipeline channel is full the receiver must
/// return 429 rather than silently dropping the payload and returning 200.
#[test]
fn returns_429_when_channel_full_not_200() {
    // Use a tiny channel so it fills up after 2 sends.
    let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 2).unwrap();
    let addr = receiver.local_addr();
    let url = format!("http://{addr}/v1/logs");

    let body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{"body": {"stringValue": "x"}}]
            }]
        }]
    })
    .to_string();

    // Fill the channel (capacity = 2 so two sends succeed).
    for i in 0..2 {
        let resp = ureq::post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes())
            .unwrap_or_else(|e| panic!("request {i} failed: {e}"));
        assert_eq!(
            resp.status(),
            200,
            "expected 200 while channel has capacity (request {i})"
        );
    }

    // The channel is now full; the next request must not return 200.
    let result = ureq::post(&url)
        .header("content-type", "application/json")
        .send(body.as_bytes());

    let status: u16 = match result {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_ne!(
        status, 200,
        "channel-full request must not return 200 (got {status})"
    );
    assert!(
        status == 429 || status == 503,
        "expected 429 or 503 for backpressure, got {status}"
    );
    assert_eq!(receiver.health(), ComponentHealth::Degraded);

    // Drain the two buffered entries so the receiver is valid.
    let _ = receiver.poll().unwrap();

    let resp = ureq::post(&url)
        .header("content-type", "application/json")
        .send(body.as_bytes())
        .expect("request after drain failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(receiver.health(), ComponentHealth::Healthy);
}

// Bug #686: /v1/logsFOO and /v1/logs/extra should return 404, not 200.
#[test]
fn path_prefix_variants_return_404() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();

    for bad_path in &["/v1/logsFOO", "/v1/logs/extra", "/v1/logs2", "/v1/log"] {
        let url = format!("http://127.0.0.1:{port}{bad_path}");
        let status = match ureq::get(&url).call() {
            Ok(r) => r.status().as_u16(),
            Err(ureq::Error::StatusCode(c)) => c,
            Err(e) => panic!("unexpected error for {bad_path}: {e}"),
        };
        assert_eq!(status, 404, "{bad_path} should return 404, got {status}");
    }
}

// Bug #687: Content-Type: Application/JSON (capital A) should be treated as JSON.
#[test]
fn content_type_matching_is_case_insensitive() {
    let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{"body": {"stringValue": "hello"}}]
            }]
        }]
    })
    .to_string();

    let resp = ureq::post(&url)
        .header("content-type", "Application/JSON")
        .send(body.as_bytes())
        .expect("request failed");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Application/JSON should be decoded as JSON and return 200"
    );

    let data = poll_receiver_until(
        &mut receiver,
        Duration::from_secs(1),
        |events| !events.is_empty(),
        "timed out waiting for mixed-case Content-Type JSON payload",
    );
    assert!(
        !data.is_empty(),
        "expected data from JSON body with mixed-case Content-Type"
    );
}

#[test]
fn content_type_substring_match_does_not_route_json() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let body = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{
                "logRecords": [{"body": {"stringValue": "hello"}}]
            }]
        }]
    })
    .to_string();

    let status = match ureq::post(&url)
        .header("content-type", "application/jsonl")
        .send(body.as_bytes())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_eq!(
        status, 400,
        "application/jsonl must not route to JSON decoder"
    );
}

// Bug #723: wrong HTTP method should return 405, not 404.
#[test]
fn wrong_http_method_returns_405() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    for (method, result) in [
        ("GET", ureq::get(&url).call()),
        ("DELETE", ureq::delete(&url).call()),
    ] {
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error for {method}: {e}"),
        };
        assert_eq!(
            status, 405,
            "{method} /v1/logs should return 405 Method Not Allowed, got {status}"
        );
    }
}

// Bug #722: JSON body missing resourceLogs should return 400, not 200.
#[test]
fn missing_resource_logs_returns_400() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let bad_bodies = [r"{}", r#"{"foo":"bar"}"#, r#"{"resourceLogs":null}"#];
    for body in &bad_bodies {
        let result = ureq::post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes());
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error for body {body}: {e}"),
        };
        assert_eq!(status, 400, "body {body:?} should return 400, got {status}");
    }
}

#[test]
fn receiver_shuts_down_cleanly_on_drop() {
    // Create an input receiver binding to port 0 (OS assigns port).
    let receiver =
        OtlpReceiverInput::new("test-drop", "127.0.0.1:0").expect("should bind successfully");
    let local_addr = receiver.local_addr();

    // Drop the receiver. This should trigger `Drop`, set `is_running` to false,
    // and join the thread, closing the HTTP server and releasing the port.
    drop(receiver);

    // Retry bind on the same port until it succeeds or times out.
    wait_until(
        Duration::from_secs(1),
        || OtlpReceiverInput::new("test-drop-2", &local_addr.to_string()).is_ok(),
        "should bind successfully to the exact same port",
    );
}

#[test]
fn receiver_health_is_healthy_while_running() {
    let receiver =
        OtlpReceiverInput::new("test-health", "127.0.0.1:0").expect("should bind successfully");

    assert_eq!(receiver.health(), ComponentHealth::Healthy);
}

#[test]
fn receiver_health_reports_stopping_when_shutdown_requested() {
    let receiver = OtlpReceiverInput::new("test-health-stop", "127.0.0.1:0")
        .expect("should bind successfully");
    receiver.is_running.store(false, Ordering::Relaxed);
    receiver
        .health
        .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);

    assert_eq!(receiver.health(), ComponentHealth::Stopping);
}

#[test]
fn receiver_health_reports_failed_when_server_thread_exits() {
    let mut receiver = OtlpReceiverInput::new("test-health-failed", "127.0.0.1:0")
        .expect("should bind successfully");
    // Ensure the real worker exits before replacing the ownership task in-test.
    receiver.is_running.store(false, Ordering::Relaxed);
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();
    receiver.background_task = BackgroundHttpTask::new_axum(shutdown_tx, std::thread::spawn(|| {}));
    receiver.is_running.store(true, Ordering::Relaxed);
    wait_until(
        Duration::from_secs(1),
        || receiver.health() == ComponentHealth::Failed,
        "receiver health did not transition to failed",
    );

    assert_eq!(receiver.health(), ComponentHealth::Failed);
}

#[test]
fn receiver_health_reports_failed_when_pipeline_disconnects() {
    let mut receiver = OtlpReceiverInput::new_with_capacity("test-disconnect", "127.0.0.1:0", 16)
        .expect("should bind successfully");
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");
    receiver.rx.take();

    let status = match ureq::post(&url)
        .header("content-type", "application/json")
        .send(
        br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}"#,
    ) {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };

    assert_eq!(status, 503);
    assert_eq!(receiver.health(), ComponentHealth::Failed);
}

// Valid OTLP JSON should still return 200 after the 400 fix.
#[test]
fn valid_otlp_json_returns_200() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let valid_body = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"severityText":"INFO","body":{"stringValue":"hello"}}]}]}]}"#;
    let result = ureq::post(&url)
        .header("content-type", "application/json")
        .send(valid_body.as_bytes());
    let status: u16 = match result {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_eq!(
        status, 200,
        "valid OTLP JSON should return 200, got {status}"
    );
}

/// Regression test for issue #1690: when time_unix_nano is 0 but
/// observed_time_unix_nano is set, the timestamp must use the observed time.
#[test]
fn uses_observed_time_when_event_time_is_zero() {
    const OBSERVED_NS: u64 = 1_705_314_700_000_000_000;

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 0,                    // event time unknown
                    observed_time_unix_nano: OBSERVED_NS, // observation time known
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("test".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let json = convert_request_to_json_lines(&request);
    let text = String::from_utf8(json).unwrap();

    // Must contain the observed timestamp.
    assert!(
        text.contains(&OBSERVED_NS.to_string()),
        "observed_time_unix_nano not written when time_unix_nano==0: {text}"
    );
}

/// When time_unix_nano is set, it takes priority over observed_time_unix_nano.
#[test]
fn prefers_event_time_over_observed_time() {
    const EVENT_NS: u64 = 1_705_314_600_000_000_000;
    const OBSERVED_NS: u64 = 1_705_314_700_000_000_000;

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: EVENT_NS,
                    observed_time_unix_nano: OBSERVED_NS,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("test".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let json = convert_request_to_json_lines(&request);
    let text = String::from_utf8(json).unwrap();

    assert!(
        text.contains(&EVENT_NS.to_string()),
        "event time_unix_nano should be used when set: {text}"
    );
    assert!(
        !text.contains(&OBSERVED_NS.to_string()),
        "observed time should not appear when event time is set: {text}"
    );
}

// Regression tests for issue #1167: non-finite floats must emit null, not "NaN"/"inf".
#[test]
fn write_f64_nan_emits_null() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, f64::NAN);
    assert_eq!(&out, b"null", "NaN must serialize as JSON null");
}

#[test]
fn write_f64_infinity_emits_null() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, f64::INFINITY);
    assert_eq!(&out, b"null", "Infinity must serialize as JSON null");
}

#[test]
fn write_f64_neg_infinity_emits_null() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, f64::NEG_INFINITY);
    assert_eq!(&out, b"null", "-Infinity must serialize as JSON null");
}

#[test]
fn write_f64_finite_unchanged() {
    let mut out = Vec::new();
    write_f64_to_buf(&mut out, 3.14);
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.starts_with("3.14"),
        "finite float should be formatted normally: {text}"
    );
}

#[test]
fn write_json_string_field_escapes_key() {
    let mut out = Vec::new();
    write_json_string_field(&mut out, r#"k"ey"#, "value");
    let text = String::from_utf8(out).unwrap();
    // Must be valid JSON
    let json_str = format!("{{{text}}}");
    serde_json::from_str::<serde_json::Value>(&json_str)
        .unwrap_or_else(|e| panic!("invalid JSON after key escaping: {e}\n{json_str}"));
    assert!(
        text.contains(r#"k\"ey"#),
        "quote in key not escaped: {text}"
    );
}

// Regression tests for #1665: BytesValue/ArrayValue/KvListValue attributes
// must not produce spurious commas (invalid JSON) in the binary OTLP path.

#[test]
fn bytes_value_attr_produces_valid_json() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    attributes: vec![
                        // int attribute before bytes
                        KeyValue {
                            key: "count".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(42)),
                            }),
                        },
                        // bytes attribute — must not produce a spurious comma
                        KeyValue {
                            key: "trace_bytes".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::BytesValue(vec![0xde, 0xad, 0xbe, 0xef])),
                            }),
                        },
                        // int attribute after bytes — must appear correctly
                        KeyValue {
                            key: "status".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(200)),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let output = convert_request_to_json_lines(&req);
    let text = String::from_utf8(output).expect("valid UTF-8");
    let line = text.trim();
    assert!(!line.is_empty(), "expected at least one output line");
    let v: serde_json::Value = serde_json::from_str(line)
        .unwrap_or_else(|e| panic!("bytes_value attribute produced invalid JSON: {e}\n{line}"));
    // count and status must be present
    assert_eq!(
        v["count"], 42,
        "int attribute before bytes must be preserved"
    );
    assert_eq!(
        v["status"], 200,
        "int attribute after bytes must be preserved"
    );
    // bytes value must be present as hex string
    assert_eq!(
        v["trace_bytes"], "deadbeef",
        "bytes value must be hex-encoded"
    );
}

#[test]
fn array_value_attr_skipped_no_spurious_comma() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, KeyValue, any_value::Value,
    };
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    attributes: vec![
                        KeyValue {
                            key: "before".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("a".to_string())),
                            }),
                        },
                        // ArrayValue — must be silently skipped, not produce spurious comma
                        KeyValue {
                            key: "tags".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::ArrayValue(ArrayValue {
                                    values: vec![AnyValue {
                                        value: Some(Value::StringValue("x".to_string())),
                                    }],
                                })),
                            }),
                        },
                        KeyValue {
                            key: "after".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("b".to_string())),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let output = convert_request_to_json_lines(&req);
    let text = String::from_utf8(output).expect("valid UTF-8");
    let line = text.trim();
    assert!(!line.is_empty(), "expected at least one output line");
    let v: serde_json::Value = serde_json::from_str(line)
        .unwrap_or_else(|e| panic!("array_value attribute produced invalid JSON: {e}\n{line}"));
    assert_eq!(v["before"], "a", "attribute before array must be present");
    assert_eq!(v["after"], "b", "attribute after array must be present");
    assert!(
        v.get("tags").is_none(),
        "array attribute must be skipped entirely"
    );
}

/// Regression test for issue #1691: convert_request_to_batch must also
/// fall back to observed_time_unix_nano when time_unix_nano is 0.
#[test]
fn batch_path_uses_observed_time_when_event_time_is_zero() {
    const OBSERVED_NS: u64 = 1_705_314_600_000_000_000;

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 0,
                    observed_time_unix_nano: OBSERVED_NS,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("test".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let batch = convert_request_to_batch(&request).expect("batch build must succeed");
    let ts_col = batch
        .column_by_name(field_names::TIMESTAMP)
        .expect("_timestamp column must exist");
    let ts_arr = ts_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("_timestamp column must be Int64");
    assert_eq!(ts_arr.len(), 1, "expected exactly one row");
    assert_eq!(
        ts_arr.value(0),
        OBSERVED_NS as i64,
        "batch path must use observed_time_unix_nano when time_unix_nano is 0"
    );
}
