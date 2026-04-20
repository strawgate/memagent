use super::*;
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use proptest::prelude::*;
use prost::Message;
use std::time::{Duration, Instant};

fn string_value_at(col: &dyn Array, row: usize) -> String {
    match col.data_type() {
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 string array")
            .value(row)
            .to_string(),
        DataType::Utf8View => col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Utf8View string array")
            .value(row)
            .to_string(),
        other => panic!("expected string column, got {other}"),
    }
}

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

fn loopback_http_client() -> ureq::Agent {
    ureq::Agent::config_builder()
        .proxy(None)
        .timeout_global(Some(Duration::from_secs(5)))
        .build()
        .into()
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

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("structured decode succeeds");

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
        .column_by_name("resource.attributes.service.name")
        .expect("resource attribute must be prefixed with resource.attributes.");
    let service_name = service_name
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("resource.attributes.service.name should remain a flat string column");
    assert_eq!(service_name.value(0), "checkout-api");
    assert!(
        batch.column_by_name("service.name").is_none(),
        "resource attributes must not appear as bare keys"
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

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("structured decode succeeds");
    let sampled = batch
        .column_by_name("resource.attributes.resource.sampled")
        .expect("resource.attributes.resource.sampled must exist");
    assert_eq!(sampled.data_type(), &DataType::Boolean);
    let sampled = sampled
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("resource.sampled must be BooleanArray");
    assert!(sampled.value(0), "resource bool attribute must stay typed");
}

#[test]
fn structured_values_are_serialized_deterministically() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "resource.labels".into(),
                    value: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::BoolValue(true)),
                                },
                                AnyValue {
                                    value: Some(Value::StringValue("x".into())),
                                },
                            ],
                        })),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::StringValue("hello".into())),
                                },
                                AnyValue {
                                    value: Some(Value::IntValue(2)),
                                },
                            ],
                        })),
                    }),
                    attributes: vec![KeyValue {
                        key: "ctx".into(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![
                                    KeyValue {
                                        key: "b".into(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("two".into())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "a".into(),
                                        value: Some(AnyValue {
                                            value: Some(Value::IntValue(1)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "a".into(),
                                        value: Some(AnyValue {
                                            value: Some(Value::IntValue(3)),
                                        }),
                                    },
                                ],
                            })),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("structured decode succeeds");

    let body = batch
        .column_by_name(field_names::BODY)
        .expect("body column must exist");
    assert_eq!(string_value_at(body.as_ref(), 0), "[\"hello\",2]");

    let ctx = batch.column_by_name("ctx").expect("ctx column must exist");
    assert_eq!(
        string_value_at(ctx.as_ref(), 0),
        "[{\"k\":\"b\",\"v\":\"two\"},{\"k\":\"a\",\"v\":1},{\"k\":\"a\",\"v\":3}]"
    );

    let labels = batch
        .column_by_name("resource.attributes.resource.labels")
        .expect("resource labels column must exist");
    assert_eq!(string_value_at(labels.as_ref(), 0), "[true,\"x\"]");
}

#[test]
fn canonical_fields_are_not_shadowed_by_attribute_collisions() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                scope: Some(
                    opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        name: "otel-scope".into(),
                        version: "1.2.3".into(),
                        ..Default::default()
                    },
                ),
                log_records: vec![LogRecord {
                    flags: 123,
                    attributes: vec![
                        KeyValue {
                            key: field_names::FLAGS.into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("shadow-flags".into())),
                            }),
                        },
                        KeyValue {
                            key: field_names::SCOPE_NAME.into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("shadow-scope".into())),
                            }),
                        },
                        KeyValue {
                            key: field_names::SCOPE_VERSION.into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("shadow-version".into())),
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

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("structured decode succeeds");

    let flags = batch
        .column_by_name(field_names::FLAGS)
        .expect("flags column must exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("flags must be Int64");
    assert_eq!(flags.value(0), 123);

    let scope_name = batch
        .column_by_name(field_names::SCOPE_NAME)
        .expect("scope.name column must exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("scope.name must be Utf8");
    assert_eq!(scope_name.value(0), "otel-scope");

    let scope_version = batch
        .column_by_name(field_names::SCOPE_VERSION)
        .expect("scope.version column must exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("scope.version must be Utf8");
    assert_eq!(scope_version.value(0), "1.2.3");
}

#[test]
fn decodes_protobuf_to_batch() {
    let body = make_test_request();
    let batch =
        decode_otlp_protobuf(&body, field_names::DEFAULT_RESOURCE_PREFIX).expect("decode succeeds");
    assert_eq!(batch.num_rows(), 2);

    let severity = batch
        .column_by_name(field_names::SEVERITY)
        .expect("severity column must exist");
    let severity = severity
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("severity must be StringArray");
    assert_eq!(severity.value(0), "INFO");
    assert_eq!(severity.value(1), "ERROR");

    let body_col = batch
        .column_by_name(field_names::BODY)
        .expect("body column must exist");
    let body_col = body_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("body must be StringArray");
    assert_eq!(body_col.value(0), "hello world");
    assert_eq!(body_col.value(1), "something broke");

    let service = batch
        .column_by_name("service")
        .expect("service column must exist");
    let service = service
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("service must be StringArray");
    assert_eq!(service.value(0), "myapp");
}

/// Contract test: protobuf and JSON OTLP inputs that represent the same
/// records must produce batches with the same row count.
#[test]
fn protobuf_and_json_inputs_produce_batches() {
    let proto_batch =
        decode_otlp_protobuf(&make_test_request(), field_names::DEFAULT_RESOURCE_PREFIX).unwrap();

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
    let json_batch =
        decode_otlp_json(json_body.as_bytes(), field_names::DEFAULT_RESOURCE_PREFIX).unwrap();

    assert_eq!(
        proto_batch.num_rows(),
        2,
        "expected 2 protobuf-decoded rows"
    );
    assert_eq!(json_batch.num_rows(), 2, "expected 2 json-decoded rows");
}

#[test]
fn record_attributes_override_resource_attributes_in_structured_batch() {
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

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("structured batch decode");
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
fn invalid_json_bytes_value_returns_error() {
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "invalid base64 bytesValue must fail");
}

#[test]
fn json_bytes_value_accepts_urlsafe_and_unpadded_base64() {
    let batch = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("urlsafe unpadded base64 should decode");

    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn invalid_json_int_value_returns_error() {
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "invalid intValue must fail");
}

#[test]
fn invalid_json_double_value_returns_error() {
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "invalid doubleValue must fail");
}

#[test]
fn non_numeric_json_int_string_returns_error() {
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(
        result.is_err(),
        "non-numeric intValue string must fail instead of emitting malformed JSON"
    );
}

#[test]
fn exponent_form_json_int_value_is_accepted() {
    let batch = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("ProtoJSON exponent intValue should decode");

    assert_eq!(batch.num_rows(), 1);
    let status = batch
        .column_by_name("status")
        .expect("status column must exist");
    let status = status
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("status must be Int64Array");
    assert_eq!(status.value(0), 500);
}

#[test]
fn bare_exponent_json_int_value_is_accepted() {
    let batch = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("bare exponent intValue should decode");

    assert_eq!(batch.num_rows(), 1);
    let status = batch
        .column_by_name("status")
        .expect("status column must exist");
    let status = status
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("status must be Int64Array");
    assert_eq!(status.value(0), 100);
}

#[test]
fn huge_positive_exponent_json_int_value_returns_error() {
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "huge exponent intValue must fail");
}

#[test]
fn huge_negative_exponent_json_int_value_returns_error_without_panicking() {
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
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
    let result = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "out-of-range intValue must fail");
}

#[test]
fn invalid_json_time_unix_nano_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "not-a-number"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "invalid timeUnixNano must fail");
}

#[test]
fn invalid_json_observed_time_unix_nano_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "0",
                        "observedTimeUnixNano": "not-a-number"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(
        result.is_err(),
        "invalid observedTimeUnixNano must fail instead of being treated as missing"
    );
}

#[test]
fn invalid_json_severity_number_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "severityNumber": "bad-severity"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(
        result.is_err(),
        "invalid severityNumber must fail instead of being treated as missing"
    );
}

#[test]
fn invalid_json_flags_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "flags": "invalid-flags"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(
        result.is_err(),
        "invalid flags must fail instead of being treated as missing"
    );
}

#[test]
fn negative_json_flags_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "flags": -1
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(
        result.is_err(),
        "negative flags must fail because OTLP flags are uint32"
    );
}

#[test]
fn out_of_range_json_flags_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "flags": 4294967296
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "flags above uint32::MAX must fail");
}

#[test]
fn invalid_json_trace_id_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "traceId": "abc123"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "invalid traceId must fail");
}

#[test]
fn invalid_json_span_id_returns_error() {
    let result = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "spanId": "zzzzzzzzzzzzzzzz"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    );

    assert!(result.is_err(), "invalid spanId must fail");
}

#[test]
fn json_trace_and_span_ids_are_normalized_to_lower_hex() {
    let batch = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "traceId": "0123456789ABCDEF0123456789ABCDEF",
                        "spanId": "89ABCDEF01234567"
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("valid trace/span ids should decode");

    let trace_col = batch
        .column_by_name(field_names::TRACE_ID)
        .expect("trace_id column must exist");
    let span_col = batch
        .column_by_name(field_names::SPAN_ID)
        .expect("span_id column must exist");
    assert_eq!(
        string_value_at(trace_col.as_ref(), 0),
        "0123456789abcdef0123456789abcdef"
    );
    assert_eq!(string_value_at(span_col.as_ref(), 0), "89abcdef01234567");
}

#[test]
fn zero_json_time_unix_nano_is_accepted_and_omitted() {
    let batch = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("zero timeUnixNano should be accepted");

    assert_eq!(batch.num_rows(), 1);
    // When timestamp is 0, it should be omitted from the batch.
    if let Some(ts_col) = batch.column_by_name(field_names::TIMESTAMP) {
        let ts_arr = ts_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_timestamp must be Int64");
        assert!(
            ts_arr.is_null(0),
            "unknown timestamp should be null, not emitted as 0"
        );
    }
}

/// JSON OTLP path: when timeUnixNano is 0 but observedTimeUnixNano is set,
/// the timestamp must use the observed time (issue #1690).
#[test]
fn json_path_uses_observed_time_when_event_time_is_zero() {
    let batch = decode_otlp_json(
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
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("valid OTLP JSON");

    assert_eq!(batch.num_rows(), 1);
    let ts_col = batch
        .column_by_name(field_names::TIMESTAMP)
        .expect("_timestamp column must exist");
    let ts_arr = ts_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("_timestamp must be Int64");
    assert_eq!(
        ts_arr.value(0),
        1_705_314_700_000_000_000_i64,
        "JSON path must fall back to observedTimeUnixNano when timeUnixNano==0"
    );
}

#[test]
fn json_path_structured_values_match_protobuf_shape() {
    let batch = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "resource": {
                    "attributes": [{
                        "key": "resource.labels",
                        "value": {
                            "arrayValue": {
                                "values": [
                                    { "boolValue": true },
                                    { "stringValue": "x" }
                                ]
                            }
                        }
                    }]
                },
                "scopeLogs": [{
                    "logRecords": [{
                        "body": {
                            "arrayValue": {
                                "values": [
                                    { "stringValue": "hello" },
                                    { "intValue": "2" }
                                ]
                            }
                        },
                        "attributes": [{
                            "key": "ctx",
                            "value": {
                                "kvlistValue": {
                                    "values": [
                                        { "key": "b", "value": { "stringValue": "two" } },
                                        { "key": "a", "value": { "intValue": "1" } },
                                        { "key": "a", "value": { "intValue": "3" } }
                                    ]
                                }
                            }
                        }]
                    }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("structured OTLP JSON decode succeeds");

    let body = batch
        .column_by_name(field_names::BODY)
        .expect("body column must exist");
    assert_eq!(string_value_at(body.as_ref(), 0), "[\"hello\",2]");

    let ctx = batch.column_by_name("ctx").expect("ctx column must exist");
    assert_eq!(
        string_value_at(ctx.as_ref(), 0),
        "[{\"k\":\"b\",\"v\":\"two\"},{\"k\":\"a\",\"v\":1},{\"k\":\"a\",\"v\":3}]"
    );

    let labels = batch
        .column_by_name("resource.attributes.resource.labels")
        .expect("resource labels column must exist");
    assert_eq!(string_value_at(labels.as_ref(), 0), "[true,\"x\"]");
}

#[test]
fn json_resource_scalar_attributes_preserve_supported_types() {
    let batch = decode_otlp_json(
        br#"{
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        { "key": "sampled", "value": { "boolValue": true } },
                        { "key": "retries", "value": { "intValue": "7" } },
                        { "key": "ratio", "value": { "doubleValue": 1.25 } },
                        { "key": "service", "value": { "stringValue": "checkout" } },
                        { "key": "payload", "value": { "bytesValue": "AQIDBA==" } }
                    ]
                },
                "scopeLogs": [{
                    "logRecords": [{ "body": { "stringValue": "ok" } }]
                }]
            }]
        }"#,
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect("resource scalar attributes should decode");

    let sampled = batch
        .column_by_name("resource.attributes.sampled")
        .expect("resource bool column must exist");
    assert_eq!(sampled.data_type(), &DataType::Boolean);
    let sampled = sampled
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("resource bool should be BooleanArray");
    assert!(sampled.value(0));

    let retries = batch
        .column_by_name("resource.attributes.retries")
        .expect("resource int column must exist");
    assert_eq!(retries.data_type(), &DataType::Int64);
    let retries = retries
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("resource int should be Int64Array");
    assert_eq!(retries.value(0), 7);

    let ratio = batch
        .column_by_name("resource.attributes.ratio")
        .expect("resource float column must exist");
    assert_eq!(ratio.data_type(), &DataType::Float64);
    let ratio = ratio
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("resource float should be Float64Array");
    assert_eq!(ratio.value(0), 1.25);

    let service = batch
        .column_by_name("resource.attributes.service")
        .expect("resource string column must exist");
    assert_eq!(string_value_at(service.as_ref(), 0), "checkout");

    let payload = batch
        .column_by_name("resource.attributes.payload")
        .expect("resource bytes column must exist");
    assert_eq!(string_value_at(payload.as_ref(), 0), "01020304");
}

#[test]
fn handles_invalid_protobuf() {
    let result = decode_otlp_protobuf(b"not valid protobuf", field_names::DEFAULT_RESOURCE_PREFIX);
    assert!(result.is_err());
}

#[test]
fn invalid_protobuf_increments_parse_errors_when_stats_hooked() {
    let stats = Arc::new(ComponentStats::new());
    let receiver = OtlpReceiverInput::new_with_capacity_and_stats(
        "test",
        "127.0.0.1:0",
        16,
        Some(Arc::clone(&stats)),
    )
    .unwrap();
    let url = format!("http://{}/v1/logs", receiver.local_addr());

    let status = match loopback_http_client()
        .post(&url)
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
fn experimental_projection_decode_mode_controls_http_protobuf_path() {
    let primitive_request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("primitive".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let primitive_body = primitive_request.encode_to_vec();
    let unsupported_request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![AnyValue {
                                value: Some(Value::StringValue("nested".into())),
                            }],
                        })),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let unsupported_body = unsupported_request.encode_to_vec();

    let projected_only = OtlpReceiverInput::new_with_protobuf_decode_mode_experimental(
        "projected-only",
        "127.0.0.1:0",
        None,
        OtlpProtobufDecodeMode::ProjectedOnly,
        None,
    )
    .expect("projected-only receiver should bind");
    let projected_only_url = format!("http://{}/v1/logs", projected_only.local_addr());
    let projected_only_status = match loopback_http_client()
        .post(&projected_only_url)
        .header("content-type", "application/x-protobuf")
        .send(unsupported_body.as_slice())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected transport error: {e}"),
    };
    assert_eq!(
        projected_only_status, 200,
        "ProjectedOnly must accept complex AnyValue shapes after projection support was added"
    );

    let stats = Arc::new(ComponentStats::new());
    let mut fallback = OtlpReceiverInput::new_with_protobuf_decode_mode_experimental(
        "projected-fallback",
        "127.0.0.1:0",
        Some(Arc::clone(&stats)),
        OtlpProtobufDecodeMode::ProjectedFallback,
        None,
    )
    .expect("fallback receiver should bind");
    let fallback_url = format!("http://{}/v1/logs", fallback.local_addr());
    let projected_status = match loopback_http_client()
        .post(&fallback_url)
        .header("content-type", "application/x-protobuf")
        .send(primitive_body.as_slice())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected transport error: {e}"),
    };
    assert_eq!(
        projected_status, 200,
        "ProjectedFallback must use the projected path for primitive OTLP shapes"
    );
    assert_eq!(stats.otlp_projected_success(), 1);
    assert_eq!(stats.otlp_projected_fallback(), 0);
    assert_eq!(stats.otlp_projection_invalid(), 0);

    let fallback_status = match loopback_http_client()
        .post(&fallback_url)
        .header("content-type", "application/x-protobuf")
        .send(unsupported_body.as_slice())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected transport error: {e}"),
    };
    assert_eq!(
        fallback_status, 200,
        "ProjectedFallback must handle complex AnyValue shapes via projection"
    );
    assert_eq!(stats.otlp_projected_success(), 2);
    assert_eq!(stats.otlp_projected_fallback(), 0);
    assert_eq!(stats.otlp_projection_invalid(), 0);

    let malformed_status = match loopback_http_client()
        .post(&fallback_url)
        .header("content-type", "application/x-protobuf")
        .send(b"not valid protobuf".as_slice())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected transport error: {e}"),
    };
    assert_eq!(
        malformed_status, 400,
        "ProjectedFallback must reject malformed protobuf instead of falling back"
    );
    assert_eq!(stats.otlp_projected_success(), 2);
    assert_eq!(stats.otlp_projected_fallback(), 0);
    assert_eq!(stats.otlp_projection_invalid(), 1);
    assert_eq!(stats.parse_errors(), 1);

    let events = poll_receiver_until(
        &mut fallback,
        Duration::from_secs(2),
        |events| {
            events
                .iter()
                .filter(|event| {
                    matches!(event, InputEvent::Batch { batch, .. } if batch.num_rows() == 1)
                })
                .count()
                == 2
        },
        "fallback receiver should emit decoded batch",
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(event, InputEvent::Batch { batch, .. } if batch.num_rows() == 1)
            })
            .count(),
        2
    );
}

#[test]
fn handles_empty_body() {
    let batch =
        decode_otlp_protobuf(b"", field_names::DEFAULT_RESOURCE_PREFIX).expect("empty body ok");
    assert_eq!(batch.num_rows(), 0);
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
    let batch =
        decode_otlp_protobuf(&body, field_names::DEFAULT_RESOURCE_PREFIX).expect("decode ok");
    assert_eq!(batch.num_rows(), 0);
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
    let batch =
        decode_otlp_protobuf(&body, field_names::DEFAULT_RESOURCE_PREFIX).expect("decode ok");
    assert_eq!(batch.num_rows(), 1);
    let sev = batch
        .column_by_name(field_names::SEVERITY)
        .expect("severity column");
    let sev = sev
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("severity string");
    assert_eq!(sev.value(0), "WARN");
    assert!(
        batch.column_by_name(field_names::BODY).is_none()
            || batch.column_by_name(field_names::BODY).unwrap().is_null(0),
        "body column should be absent or null"
    );
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
    let ctrl: String = (0u8..=0x1f).map(|b| b as char).collect();
    let mut out = Vec::new();
    write_json_string_field(&mut out, "k", &ctrl);
    let text = String::from_utf8(out).unwrap();

    for b in text.as_bytes() {
        assert!(
            *b >= 0x20,
            "raw control byte 0x{:02x} found in output: {text}",
            b
        );
    }

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
    let input = "hello \u{00e9}\u{1f600} world \u{4e16}\u{754c}";
    let mut out = Vec::new();
    write_json_string_field(&mut out, "k", input);
    let text = String::from_utf8(out).unwrap();

    assert!(text.contains('\u{00e9}'), "e-acute missing: {text}");
    assert!(text.contains('\u{1f600}'), "emoji missing: {text}");
    assert!(text.contains('\u{4e16}'), "CJK char missing: {text}");

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
        let resp = loopback_http_client()
            .post(&url)
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
    let result = loopback_http_client()
        .post(&url)
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
    assert_eq!(status, 429, "expected 429 for backpressure, got {status}");
    assert_eq!(receiver.health(), ComponentHealth::Degraded);

    // Drain the two buffered entries so the receiver is valid.
    let _ = receiver.poll().unwrap();

    let resp = loopback_http_client()
        .post(&url)
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
        let status = match loopback_http_client().get(&url).call() {
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

    let resp = loopback_http_client()
        .post(&url)
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

    let status = match loopback_http_client()
        .post(&url)
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

#[test]
fn blank_content_encoding_returns_bad_request() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    let status = match loopback_http_client()
        .post(&url)
        .header("content-encoding", "   ")
        .send(&make_test_request())
    {
        Ok(resp) => resp.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("unexpected error: {e}"),
    };
    assert_eq!(status, 400, "blank Content-Encoding must be malformed");
}

// Bug #723: wrong HTTP method should return 405, not 404.
#[test]
fn wrong_http_method_returns_405() {
    let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
    let port = receiver.local_addr().port();
    let url = format!("http://127.0.0.1:{port}/v1/logs");

    for (method, result) in [
        ("GET", loopback_http_client().get(&url).call()),
        ("DELETE", loopback_http_client().delete(&url).call()),
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
        let result = loopback_http_client()
            .post(&url)
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
    let receiver =
        OtlpReceiverInput::new("test-drop", "127.0.0.1:0").expect("should bind successfully");
    let local_addr = receiver.local_addr();

    drop(receiver);

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

    let status = match loopback_http_client()
        .post(&url)
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
    let result = loopback_http_client()
        .post(&url)
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
    write_f64_to_buf(&mut out, 1.25);
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.starts_with("1.25"),
        "finite float should be formatted normally: {text}"
    );
}

#[test]
fn write_json_string_field_escapes_key() {
    let mut out = Vec::new();
    write_json_string_field(&mut out, r#"k"ey"#, "value");
    let text = String::from_utf8(out).unwrap();
    let json_str = format!("{{{text}}}");
    serde_json::from_str::<serde_json::Value>(&json_str)
        .unwrap_or_else(|e| panic!("invalid JSON after key escaping: {e}\n{json_str}"));
    assert!(
        text.contains(r#"k\"ey"#),
        "quote in key not escaped: {text}"
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

    let batch = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("batch build must succeed");
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

#[test]
fn batch_path_errors_on_overflowed_observed_timestamp() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 0,
                    observed_time_unix_nano: u64::MAX,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("overflow".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let err = convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("timestamp overflow must fail deterministically");
    let msg = err.to_string();
    assert!(
        msg.contains("observed_time_unix_nano"),
        "overflow diagnostic should name offending field: {msg}"
    );
    assert!(
        msg.contains("exceeds signed 64-bit nanosecond range"),
        "overflow diagnostic should explain range failure: {msg}"
    );
}
