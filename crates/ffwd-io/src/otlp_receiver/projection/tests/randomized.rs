#[test]
fn projected_empty_payload_matches_prost() {
    assert_projected_payload_matches_prost(&[]);
}

#[test]
fn projected_ignored_metadata_fields_match_prost_conversion() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv_string("service.name", "metadata-test")],
                dropped_attributes_count: 3,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "scope-with-ignored-metadata".into(),
                    version: "2.0.0".into(),
                    attributes: vec![kv_string("scope.attr", "ignored")],
                    dropped_attributes_count: 5,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_001,
                    observed_time_unix_nano: 1_700_000_000_000_000_002,
                    severity_number: 9,
                    severity_text: "INFO".into(),
                    body: Some(any_string("metadata fields are ignored")),
                    attributes: vec![kv_string("kept", "log-attr")],
                    dropped_attributes_count: 7,
                    event_name: "ignored.event".into(),
                    ..Default::default()
                }],
                schema_url: "https://example.test/scope-schema/1.0.0".into(),
                ..Default::default()
            }],
            schema_url: "https://example.test/resource-schema/1.0.0".into(),
            ..Default::default()
        }],
    };

    assert_projected_matches_prost(&request);
}

#[test]
fn projected_high_cardinality_dynamic_attrs_match_prost_conversion() {
    let attributes = (0..160)
        .map(|idx| {
            let key = format!("attr.high_cardinality.{idx}");
            match idx % 5 {
                0 => kv_string(&key, &format!("value-{idx}")),
                1 => kv_i64(&key, idx as i64),
                2 => kv_f64(&key, idx as f64 + 0.25),
                3 => kv_bool(&key, idx % 2 == 0),
                _ => kv_bytes(&key, &[idx as u8, (idx >> 8) as u8]),
            }
        })
        .collect::<Vec<_>>();

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv_string("service.name", "high-cardinality")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(any_string("wide dynamic attrs")),
                    attributes,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    assert_projected_matches_prost(&request);
}

// -- Malformed wire data tests --

#[test]
fn projected_truncated_varint_is_invalid() {
    // A varint that starts with a continuation bit but has no following byte.
    let payload = vec![0x80];
    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncated varint should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_truncated_fixed64_is_invalid() {
    // A tag claiming fixed64 (wire type 1) followed by only 4 bytes instead of 8.
    let mut payload = Vec::new();
    // field=1, wire_type=1 (fixed64) => tag = (1 << 3) | 1 = 9
    // Wrap in a resource_logs container so field 1 is expected.
    let mut resource_logs_inner = Vec::new();
    encode_varint(&mut resource_logs_inner, (1_u64 << 3) | 1); // field 1, fixed64
    resource_logs_inner.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]); // only 4 bytes

    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs_inner,
    );

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncated fixed64 should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_truncated_fixed32_is_invalid() {
    // A tag claiming fixed32 (wire type 5) followed by only 2 bytes instead of 4.
    let mut inner = Vec::new();
    encode_varint(&mut inner, (8_u64 << 3) | 5); // log_record field 8 (flags), fixed32
    inner.extend_from_slice(&[0x01, 0x02]); // only 2 bytes

    let mut scope_logs = Vec::new();
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &inner);
    let mut resource_logs = Vec::new();
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
        &scope_logs,
    );
    let mut payload = Vec::new();
    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs,
    );

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncated fixed32 should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_length_exceeding_buffer_is_invalid() {
    // A length-delimited field that claims more bytes than are available.
    let mut payload = Vec::new();
    encode_varint(&mut payload, (1_u64 << 3) | 2); // field 1, len
    encode_varint(&mut payload, 9999); // claims 9999 bytes
    payload.extend_from_slice(b"short"); // only 5 bytes

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("length exceeding buffer should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_group_mismatch_is_invalid() {
    // Start group field 99, end group field 98 (mismatch).
    let mut payload = Vec::new();
    encode_start_group(&mut payload, 99);
    encode_end_group(&mut payload, 98);

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("mismatched group boundaries should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_nested_group_mismatch_is_invalid() {
    let mut payload = Vec::new();
    encode_start_group(&mut payload, 99);
    encode_start_group(&mut payload, 100);
    encode_end_group(&mut payload, 99);

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("mismatched nested group boundaries should be invalid");
    assert!(
        matches!(
            err,
            ProjectionError::Invalid("mismatched protobuf end group")
        ),
        "expected nested group mismatch, got {err:?}"
    );
}

#[test]
fn projected_group_depth_limit_is_invalid() {
    let mut payload = Vec::new();
    for _ in 0..=wire::PROTOBUF_MAX_GROUP_DEPTH {
        encode_start_group(&mut payload, 99);
    }

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("overly deep protobuf groups should be invalid");
    assert!(
        matches!(
            err,
            ProjectionError::Invalid("protobuf group nesting too deep")
        ),
        "expected group depth failure, got {err:?}"
    );
}

#[test]
fn projected_truncated_field_inside_group_is_invalid() {
    let mut payload = Vec::new();
    encode_start_group(&mut payload, 99);
    encode_fixed32_field(&mut payload, 100, 0x0102_0304);
    payload.truncate(payload.len() - 2);

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncated field inside group should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid("truncated fixed32 field")),
        "expected truncated field failure, got {err:?}"
    );
}

#[test]
fn projected_nested_truncation_inside_log_record_is_invalid() {
    // Build a valid outer wrapper but truncate inside the log record body.
    let mut log_record = Vec::new();
    // severity_text field (3, len) with truncated length
    encode_varint(
        &mut log_record,
        (otlp_field::LOG_RECORD_SEVERITY_TEXT as u64) << 3 | 2,
    );
    encode_varint(&mut log_record, 100); // claims 100 bytes
    log_record.extend_from_slice(b"short"); // only 5 bytes

    let mut scope_logs = Vec::new();
    encode_len_field(
        &mut scope_logs,
        otlp_field::SCOPE_LOGS_LOG_RECORDS,
        &log_record,
    );
    let mut resource_logs = Vec::new();
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
        &scope_logs,
    );
    let mut payload = Vec::new();
    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs,
    );

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncation inside log record should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_invalid_utf8_in_attribute_key_is_invalid() {
    let mut kv_bytes = Vec::new();
    // key = field 1, len with invalid UTF-8
    encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_KEY, &[0xff, 0xfe]);
    // value = field 2, len with a string AnyValue
    let mut any_val = Vec::new();
    encode_len_field(&mut any_val, otlp_field::ANY_VALUE_STRING_VALUE, b"valid");
    encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_VALUE, &any_val);

    let mut log_record = Vec::new();
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_ATTRIBUTES,
        &kv_bytes,
    );

    let mut scope_logs = Vec::new();
    encode_len_field(
        &mut scope_logs,
        otlp_field::SCOPE_LOGS_LOG_RECORDS,
        &log_record,
    );
    let mut resource_logs = Vec::new();
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
        &scope_logs,
    );
    let mut payload = Vec::new();
    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs,
    );

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("invalid UTF-8 in attribute key should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_invalid_utf8_in_attribute_string_value_is_invalid() {
    let mut any_val = Vec::new();
    encode_len_field(
        &mut any_val,
        otlp_field::ANY_VALUE_STRING_VALUE,
        &[0x80, 0x81],
    );
    let mut kv_bytes = Vec::new();
    encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_KEY, b"bad-val");
    encode_len_field(&mut kv_bytes, otlp_field::KEY_VALUE_VALUE, &any_val);

    let mut log_record = Vec::new();
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_ATTRIBUTES,
        &kv_bytes,
    );

    let mut scope_logs = Vec::new();
    encode_len_field(
        &mut scope_logs,
        otlp_field::SCOPE_LOGS_LOG_RECORDS,
        &log_record,
    );
    let mut resource_logs = Vec::new();
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
        &scope_logs,
    );
    let mut payload = Vec::new();
    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs,
    );

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("invalid UTF-8 in attribute string value should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

#[test]
fn projected_invalid_utf8_in_body_string_is_invalid() {
    let mut any_val = Vec::new();
    encode_len_field(
        &mut any_val,
        otlp_field::ANY_VALUE_STRING_VALUE,
        &[0xc0, 0xaf],
    );
    let mut log_record = Vec::new();
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_val);

    let mut scope_logs = Vec::new();
    encode_len_field(
        &mut scope_logs,
        otlp_field::SCOPE_LOGS_LOG_RECORDS,
        &log_record,
    );
    let mut resource_logs = Vec::new();
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
        &scope_logs,
    );
    let mut payload = Vec::new();
    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs,
    );

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("invalid UTF-8 in body string should be invalid");
    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid, got {err:?}"
    );
}

// -- Complex AnyValue projection tests --

#[test]
fn projected_kvlist_attribute_is_projected_as_json() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(any_string("normal body")),
                    attributes: vec![KeyValue {
                        key: "nested".into(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![kv_string("inner", "value")],
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
    let payload = request.encode_to_vec();
    let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
        .expect("prost should decode kvlist attribute fixture");
    let actual = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("kvlist attribute should decode in projection");
    assert_batches_match(&expected, &actual);
}

#[test]
fn projected_array_attribute_is_projected_as_json() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(any_string("normal body")),
                    attributes: vec![KeyValue {
                        key: "tags".into(),
                        value: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![any_string("a"), any_string("b")],
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
    let payload = request.encode_to_vec();
    let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
        .expect("prost should decode array attribute fixture");
    let actual = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("array attribute should decode in projection");
    assert_batches_match(&expected, &actual);
}

#[test]
fn projected_kvlist_body_is_projected_as_json() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: vec![kv_string("key", "value")],
                        })),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let payload = request.encode_to_vec();
    let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
        .expect("prost should decode kvlist body fixture");
    let actual = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("kvlist body should decode in projection");
    assert_batches_match(&expected, &actual);
}

#[test]
fn projected_body_anyvalue_shapes_match_prost_conversion() {
    let bodies = [
        any_string("body"),
        AnyValue {
            value: Some(Value::BoolValue(true)),
        },
        AnyValue {
            value: Some(Value::IntValue(-42)),
        },
        AnyValue {
            value: Some(Value::DoubleValue(0.0)),
        },
        AnyValue {
            value: Some(Value::DoubleValue(12.5)),
        },
        AnyValue {
            value: Some(Value::BytesValue(vec![0xde, 0xad, 0xbe, 0xef])),
        },
        AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![any_string("a"), any_string("b")],
            })),
        },
        AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![kv_string("k", "v")],
            })),
        },
    ];

    for body in bodies {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(body),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_projected_matches_prost(&request);
    }
}
