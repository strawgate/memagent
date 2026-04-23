#[test]
fn projected_severity_number_uses_int32_truncation_like_prost() {
    let mut log_record = Vec::new();
    encode_varint_field(
        &mut log_record,
        otlp_field::LOG_RECORD_SEVERITY_NUMBER,
        u64::from(u32::MAX),
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

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_field_zero_is_invalid() {
    let err = decode_projected_otlp_logs(&[0x00, 0x00], field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("protobuf field number zero should fail projection");

    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {err:?}"
    );
}

#[test]
fn invalid_utf8_string_field_is_rejected_by_projection() {
    let mut log_record = Vec::new();
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_SEVERITY_TEXT,
        &[0xff, 0xfe],
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

    let projection_err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("invalid UTF-8 should fail projection");
    assert!(
        matches!(projection_err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {projection_err:?}"
    );
    assert!(
        crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
        "production decoder should reject invalid UTF-8"
    );
}

#[test]
fn projected_truncated_length_field_is_invalid() {
    let err = decode_projected_otlp_logs(&[0x0a, 0x05, 0x01], field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("truncated top-level resource_logs field should fail projection");

    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {err:?}"
    );
}

#[test]
fn projected_overlong_ten_byte_varint_is_invalid() {
    let err = decode_projected_otlp_logs(
        &[
            0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ],
        field_names::DEFAULT_RESOURCE_PREFIX,
    )
    .expect_err("overlong varint should fail projection");

    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {err:?}"
    );
}

#[test]
fn projected_known_log_record_field_with_wrong_wire_type_is_invalid() {
    let mut log_record = Vec::new();
    encode_varint_field(&mut log_record, otlp_field::LOG_RECORD_SEVERITY_TEXT, 1);

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
        .expect_err("wrong wire type should fail projection");
    assert!(
        matches!(projection_err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {projection_err:?}"
    );
    assert!(
        crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
        "production decoder should reject invalid known-field wire types"
    );
}

#[test]
fn projected_top_level_known_field_with_wrong_wire_type_is_invalid() {
    let mut payload = Vec::new();
    encode_varint_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        0,
    );

    let projection_err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("wrong top-level wire type should fail projection");
    assert!(
        matches!(projection_err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {projection_err:?}"
    );
    assert!(
        crate::otlp_receiver::decode_protobuf_to_batch(&payload).is_err(),
        "production decoder should reject invalid top-level known-field wire types"
    );
}

#[test]
fn projected_nested_kvlist_anyvalue_matches_prost_conversion() {
    let request = nested_kvlist_request();
    assert_projected_matches_prost(&request);
}

#[test]
fn experimental_projection_fallback_matches_prost_for_unsupported_anyvalue() {
    let payload = nested_kvlist_request().encode_to_vec();
    let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
        .expect("prost reference should decode unsupported projection fixture");
    let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
        Bytes::from(payload),
    )
    .expect("experimental projection decoder should fall back to prost");

    assert_batches_match(&expected, &actual);
}

#[test]
fn experimental_projection_fallback_matches_prost_for_array_anyvalue() {
    let payload = array_body_request().encode_to_vec();
    let expected = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
        .expect("prost reference should decode array AnyValue fixture");
    let actual = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
        Bytes::from(payload),
    )
    .expect("experimental projection decoder should fall back to prost");

    assert_batches_match(&expected, &actual);
}

fn array_body_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![any_string("nested")],
                        })),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn nested_kvlist_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    attributes: vec![KeyValue {
                        key: "complex".into(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![kv_string("nested", "value")],
                            })),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn assert_projected_payload_matches_prost(payload: &[u8]) {
    let request = ExportLogsServiceRequest::decode(payload)
        .expect("prost reference should decode handcrafted payload");
    let expected =
        convert_request_to_batch(&request, field_names::DEFAULT_RESOURCE_PREFIX).unwrap();
    let actual = decode_projected_otlp_logs(payload, field_names::DEFAULT_RESOURCE_PREFIX).unwrap();

    assert_batches_match(&expected, &actual);
}

fn assert_projected_matches_prost(request: &ExportLogsServiceRequest) {
    let payload = request.encode_to_vec();
    assert_projected_payload_matches_prost(&payload);
}

/// Compare prost (StreamingBuilder) and projected (ColumnarBatchBuilder) batches.
///
/// ColumnarBatchBuilder produces `Utf8View` (including inside Struct
/// conflict columns) while StreamingBuilder produces `Utf8`. This function
/// normalizes Utf8View -> Utf8 recursively, then compares by column name
/// so column ordering differences don't cause false failures.
fn assert_batches_match(expected: &RecordBatch, actual: &RecordBatch) {
    use arrow::array::StructArray;
    use arrow::compute::cast;
    assert_eq!(expected.num_rows(), actual.num_rows(), "row count mismatch");

    /// Recursively cast Utf8View -> Utf8 inside an array (handles Struct children).
    fn normalize_utf8view(arr: &dyn arrow::array::Array) -> Arc<dyn arrow::array::Array> {
        use arrow::array::Array;
        use arrow::datatypes::DataType;
        match arr.data_type() {
            DataType::Utf8View => cast(arr, &DataType::Utf8).expect("cast Utf8View->Utf8"),
            DataType::Struct(fields) => {
                let struct_arr = arr
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("struct downcast");
                let new_fields: Vec<_> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let child = normalize_utf8view(struct_arr.column(i).as_ref());
                        let new_field = if f.data_type() == &DataType::Utf8View {
                            Arc::new(arrow::datatypes::Field::new(
                                f.name(),
                                DataType::Utf8,
                                f.is_nullable(),
                            ))
                        } else {
                            Arc::clone(f)
                        };
                        (new_field, child)
                    })
                    .collect();
                let (new_field_refs, new_arrays): (Vec<_>, Vec<_>) = new_fields.into_iter().unzip();
                Arc::new(
                    StructArray::try_new(
                        new_field_refs.into(),
                        new_arrays,
                        struct_arr.nulls().cloned(),
                    )
                    .expect("struct rebuild"),
                )
            }
            _ => arrow::array::make_array(arr.to_data()),
        }
    }

    // Match columns by name so ordering differences don't cause failures.
    let actual_schema = actual.schema();
    for (i, expected_field) in expected.schema().fields().iter().enumerate() {
        let name = expected_field.name();
        let actual_idx = actual_schema
            .index_of(name)
            .unwrap_or_else(|_| panic!("projected batch missing column '{name}'"));
        let expected_col = expected.column(i);
        let actual_col = actual.column(actual_idx);

        let actual_normalized = normalize_utf8view(actual_col.as_ref());

        assert_eq!(
            expected_col.to_data(),
            actual_normalized.to_data(),
            "column '{name}' data mismatch"
        );
    }

    // Both batches should now have the same column count since unwritten
    // planned fields are omitted (matching StreamingBuilder).
    assert_eq!(
        expected.num_columns(),
        actual.num_columns(),
        "column count mismatch: expected columns {:?}, actual columns {:?}",
        expected
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        actual
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
    );
}

fn encode_len_field(out: &mut Vec<u8>, field: u32, value: &[u8]) {
    encode_varint(out, u64::from(field << 3 | 2));
    encode_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn encode_varint_field(out: &mut Vec<u8>, field: u32, value: u64) {
    encode_varint(out, u64::from(field << 3));
    encode_varint(out, value);
}

fn encode_fixed64_field(out: &mut Vec<u8>, field: u32, value: u64) {
    encode_varint(out, u64::from(field << 3 | 1));
    out.extend_from_slice(&value.to_le_bytes());
}

fn encode_fixed32_field(out: &mut Vec<u8>, field: u32, value: u32) {
    encode_varint(out, u64::from(field << 3 | 5));
    out.extend_from_slice(&value.to_le_bytes());
}

fn encode_start_group(out: &mut Vec<u8>, field: u32) {
    encode_varint(out, u64::from(field << 3 | 3));
}

fn encode_end_group(out: &mut Vec<u8>, field: u32) {
    encode_varint(out, u64::from(field << 3 | 4));
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[test]
fn projected_complex_anyvalue_matches_prost_conversion() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![any_string("nested")],
                        })),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    assert_projected_matches_prost(&request);
}

// -- Multi-resource/scope container tests --

#[test]
fn projected_multiple_resources_match_prost() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv_string("service.name", "frontend")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-fe".into(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        body: Some(any_string("frontend-log")),
                        severity_text: "INFO".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        kv_string("service.name", "backend"),
                        kv_i64("resource.pid", 42),
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-be".into(),
                        version: "2.0.0".into(),
                        ..Default::default()
                    }),
                    log_records: vec![
                        LogRecord {
                            body: Some(any_string("backend-log-1")),
                            severity_text: "WARN".into(),
                            ..Default::default()
                        },
                        LogRecord {
                            body: Some(any_string("backend-log-2")),
                            attributes: vec![kv_bool("retried", true)],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
    };
    assert_projected_matches_prost(&request);
}

#[test]
fn projected_multiple_scopes_per_resource_match_prost() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv_string("service.name", "multi-scope")],
                ..Default::default()
            }),
            scope_logs: vec![
                ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "http-handler".into(),
                        version: "1.0.0".into(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        body: Some(any_string("request-received")),
                        severity_text: "INFO".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "db-client".into(),
                        version: "3.2.0".into(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        body: Some(any_string("query-executed")),
                        severity_text: "DEBUG".into(),
                        attributes: vec![kv_string("db.system", "postgres")],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    };
    assert_projected_matches_prost(&request);
}

#[test]
fn projected_resource_attrs_do_not_collide_with_log_attrs() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    kv_string("service.name", "collision-test"),
                    kv_string("host", "resource-host"),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    attributes: vec![kv_string("host", "log-host")],
                    body: Some(any_string("collision")),
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
fn projected_empty_resource_and_scope_match_prost() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    body: Some(any_string("no-context")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    assert_projected_matches_prost(&request);
}
