#[test]
fn projected_fallback_for_array_attr_matches_prost() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(any_string("fallback body")),
                    attributes: vec![KeyValue {
                        key: "tags".into(),
                        value: Some(AnyValue {
                            value: Some(Value::ArrayValue(ArrayValue {
                                values: vec![any_string("x"), any_string("y")],
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
        .expect("prost reference should decode array attribute fixture");
    let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
        Bytes::from(payload),
    )
    .expect("projected fallback path should produce a batch for array attribute");
    assert_batches_match(&expected, &actual);
}

#[test]
fn projected_mixed_primitive_and_complex_attrs_match_prost() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(any_string("mixed")),
                    attributes: vec![
                        kv_string("simple", "ok"),
                        kv_i64("count", 42),
                        KeyValue {
                            key: "nested_list".into(),
                            value: Some(AnyValue {
                                value: Some(Value::KvlistValue(KeyValueList {
                                    values: vec![kv_string("deep", "val")],
                                })),
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
    let payload = request.encode_to_vec();

    let direct = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect("mixed attrs with kvlist should decode in projection");

    let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
        .expect("prost should decode mixed attrs");
    assert_batches_match(&expected, &direct);

    let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
        Bytes::from(payload),
    )
    .expect("fallback should handle mixed attrs");
    assert_batches_match(&expected, &actual);
}

#[test]
fn projected_malformed_wire_does_not_fall_back_as_valid_data() {
    // Malformed protobuf must return an error even in fallback mode,
    // because prost also rejects it.
    let mut payload = Vec::new();
    encode_varint(&mut payload, 0); // field number 0 -- always invalid

    let err = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
        Bytes::from(payload),
    );
    assert!(
        err.is_err(),
        "malformed wire data must not silently produce a valid batch"
    );
}

#[test]
fn projected_complex_anyvalue_plus_malformed_wire_remains_error() {
    // Complex AnyValue payloads are projected, but malformed trailing wire
    // must still fail instead of being treated as valid data.
    let mut any_val = Vec::new();
    encode_len_field(
        &mut any_val,
        otlp_field::ANY_VALUE_ARRAY_VALUE,
        &ArrayValue::default().encode_to_vec(),
    );

    let mut log_record = Vec::new();
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_val);
    encode_varint(&mut log_record, 0); // malformed field number zero

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

    let projection_err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("projection should reject malformed trailing wire");
    assert!(matches!(projection_err, ProjectionError::Invalid(_)));

    let fallback_err = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
        Bytes::from(payload),
    );
    assert!(
        fallback_err.is_err(),
        "unsupported projection fallback must preserve prost malformed-wire rejection"
    );
}

#[test]
fn projected_complex_anyvalue_escapes_control_chars_quotes_and_backslashes_like_prost() {
    let weird = "quote=\" backslash=\\ newline=\n tab=\t nul=\u{0000}";
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                any_string(weird),
                                AnyValue {
                                    value: Some(Value::KvlistValue(KeyValueList {
                                        values: vec![KeyValue {
                                            key: weird.into(),
                                            value: Some(any_string(weird)),
                                        }],
                                    })),
                                },
                            ],
                        })),
                    }),
                    attributes: vec![KeyValue {
                        key: weird.into(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: weird.into(),
                                    value: Some(any_string(weird)),
                                }],
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

    assert_projected_matches_prost(&request);
}

#[test]
fn projected_complex_anyvalue_non_finite_and_negative_zero_match_prost() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::DoubleValue(f64::NAN)),
                                },
                                AnyValue {
                                    value: Some(Value::DoubleValue(f64::INFINITY)),
                                },
                                AnyValue {
                                    value: Some(Value::DoubleValue(f64::NEG_INFINITY)),
                                },
                                AnyValue {
                                    value: Some(Value::DoubleValue(-0.0)),
                                },
                            ],
                        })),
                    }),
                    attributes: vec![KeyValue {
                        key: "float.edge.cases".into(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: "value".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::DoubleValue(-0.0)),
                                    }),
                                }],
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

    assert_projected_matches_prost(&request);
}

#[test]
fn projected_complex_anyvalue_duplicate_kv_fields_are_last_value_wins_like_prost() {
    let mut kv = Vec::new();
    encode_len_field(&mut kv, otlp_field::KEY_VALUE_KEY, b"first-key");
    encode_len_field(
        &mut kv,
        otlp_field::KEY_VALUE_VALUE,
        &any_string("first-value").encode_to_vec(),
    );
    encode_len_field(&mut kv, otlp_field::KEY_VALUE_KEY, b"last-key");
    encode_len_field(
        &mut kv,
        otlp_field::KEY_VALUE_VALUE,
        &any_string("last-value").encode_to_vec(),
    );

    let mut kvlist = Vec::new();
    encode_len_field(&mut kvlist, 1, &kv);

    let mut body = Vec::new();
    encode_len_field(&mut body, otlp_field::ANY_VALUE_KVLIST_VALUE, &kvlist);

    let mut log_record = Vec::new();
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &body);
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

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_complex_anyvalue_malformed_nested_length_is_invalid() {
    // AnyValue.array_value containing ArrayValue.values with truncated length.
    // array bytes: field=1(len), length=2, payload has only one byte.
    let malformed_array = [0x0a, 0x02, 0x01];
    let mut any = Vec::new();
    encode_len_field(
        &mut any,
        otlp_field::ANY_VALUE_ARRAY_VALUE,
        &malformed_array,
    );

    let mut log_record = Vec::new();
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any);
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

    let projection_err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncated nested complex AnyValue should fail projection");
    assert!(matches!(projection_err, ProjectionError::Invalid(_)));
    assert!(
        crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
        "production decoder should reject malformed nested complex AnyValue wire"
    );
}
