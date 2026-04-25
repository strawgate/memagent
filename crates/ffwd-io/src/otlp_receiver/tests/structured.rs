use super::*;
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use ffwd_types::field_names;
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
) -> Vec<SourceEvent>
where
    F: FnMut(&[SourceEvent]) -> bool,
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
