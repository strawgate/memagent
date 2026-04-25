#[test]
fn projected_deeply_nested_complex_anyvalue_matches_prost() {
    let mut nested = any_string("leaf");
    for _ in 0..48 {
        nested = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![nested],
            })),
        };
    }

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(nested),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    assert_projected_matches_prost(&request);
}

// -- Expanded proptest --

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn projected_fallback_arbitrary_bytes_matches_prost_classification(
        data in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let prost = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&data);
        let fallback = crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(
            Bytes::from(data),
        );

        match (prost, fallback) {
            (Ok(expected), Ok(actual)) => assert_batches_match(&expected, &actual),
            (Err(_), Err(_)) => {}
            (Ok(_), Err(err)) => {
                prop_assert!(
                    false,
                    "projected fallback rejected protobuf bytes accepted by prost: {err}"
                );
            }
            (Err(err), Ok(actual)) => {
                prop_assert!(
                    false,
                    "projected fallback accepted malformed bytes rejected by prost ({err}); \
                     actual columns={:?}, rows={}",
                    actual.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                    actual.num_rows(),
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn projected_randomized_primitive_payload_matches_prost(
        rows in 1usize..24,
        attrs_per_row in 0usize..8,
        resource_attrs in 0usize..6,
        severity in proptest::option::of("[A-Z]{0,12}"),
        body in proptest::option::of("[ -~]{0,80}"),
        scope_name in proptest::option::of("[a-zA-Z0-9_.-]{0,24}"),
        scope_version in proptest::option::of("[0-9.]{0,12}"),
    ) {
        let request = randomized_primitive_request(
            rows,
            attrs_per_row,
            resource_attrs,
            severity,
            body,
            scope_name,
            scope_version,
        );
        let payload = request.encode_to_vec();
        let projected = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("primitive randomized payload should decode in projection");
        let prost = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost reference should decode randomized primitive payload");

        assert_batches_match(&prost, &projected);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn projected_multi_container_payload_matches_prost(
        num_resources in 1usize..5,
        scopes_per_resource in 1usize..4,
        rows_per_scope in 1usize..30,
        attrs_per_row in 0usize..12,
        resource_attrs in 0usize..8,
    ) {
        let request = randomized_multi_container_request(
            num_resources,
            scopes_per_resource,
            rows_per_scope,
            attrs_per_row,
            resource_attrs,
        );
        let payload = request.encode_to_vec();
        let projected = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
            .expect("multi-container randomized payload should decode in projection");
        let prost = crate::otlp_receiver::decode_protobuf_to_batch_prost_reference(&payload)
            .expect("prost reference should decode multi-container payload");

        assert_batches_match(&prost, &projected);
    }
}

fn randomized_multi_container_request(
    num_resources: usize,
    scopes_per_resource: usize,
    rows_per_scope: usize,
    attrs_per_row: usize,
    resource_attrs: usize,
) -> ExportLogsServiceRequest {
    let mut global_row = 0usize;
    let resource_logs = (0..num_resources)
        .map(|res_idx| {
            let resource_attributes = (0..resource_attrs)
                .map(|idx| {
                    let key = format!("resource.{res_idx}.attr.{idx}");
                    match idx % 4 {
                        0 => kv_string(&key, &format!("rv-{res_idx}-{idx}")),
                        1 => kv_i64(&key, (res_idx * 100 + idx) as i64),
                        2 => kv_f64(&key, res_idx as f64 + idx as f64 * 0.1),
                        _ => kv_bool(&key, idx % 2 == 0),
                    }
                })
                .collect::<Vec<_>>();

            let scope_logs = (0..scopes_per_resource)
                .map(|scope_idx| {
                    let log_records = (0..rows_per_scope)
                        .map(|_| {
                            let row = global_row;
                            global_row += 1;
                            let attributes = (0..attrs_per_row)
                                .map(|attr_idx| {
                                    let key = format!("attr.{attr_idx}");
                                    match (row + attr_idx) % 5 {
                                        0 => kv_string(&key, &format!("v-{row}-{attr_idx}")),
                                        1 => kv_i64(&key, row as i64 + attr_idx as i64),
                                        2 => kv_f64(&key, row as f64 + attr_idx as f64 / 10.0),
                                        3 => kv_bool(&key, (row + attr_idx).is_multiple_of(2)),
                                        _ => kv_bytes(&key, &[row as u8, attr_idx as u8]),
                                    }
                                })
                                .collect::<Vec<_>>();

                            LogRecord {
                                time_unix_nano: 1_700_000_000_000_000_000u64
                                    .saturating_add(row as u64),
                                observed_time_unix_nano: 1_700_000_000_000_001_000u64
                                    .saturating_add(row as u64),
                                severity_number: (row % 24) as i32,
                                severity_text: ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"][row % 5]
                                    .to_string(),
                                body: Some(any_string(&format!("row-{row}"))),
                                attributes,
                                trace_id: vec![row as u8; 16],
                                span_id: vec![row as u8; 8],
                                flags: row as u32,
                                ..Default::default()
                            }
                        })
                        .collect::<Vec<_>>();

                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: format!("scope-{res_idx}-{scope_idx}"),
                            version: format!("{res_idx}.{scope_idx}.0"),
                            ..Default::default()
                        }),
                        log_records,
                        ..Default::default()
                    }
                })
                .collect::<Vec<_>>();

            ResourceLogs {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    ..Default::default()
                }),
                scope_logs,
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();

    ExportLogsServiceRequest { resource_logs }
}

fn randomized_primitive_request(
    rows: usize,
    attrs_per_row: usize,
    resource_attrs: usize,
    severity: Option<String>,
    body: Option<String>,
    scope_name: Option<String>,
    scope_version: Option<String>,
) -> ExportLogsServiceRequest {
    let resource_attributes = (0..resource_attrs)
        .map(|index| {
            let key = if index % 2 == 0 {
                format!("resource.key.{index}")
            } else {
                "resource.key.dup".to_string()
            };
            match index % 4 {
                0 => kv_string(&key, &format!("resource-value-{index}")),
                1 => kv_i64(&key, -(index as i64) - 1),
                2 => kv_f64(&key, index as f64 + 0.5),
                _ => kv_bool(&key, index % 3 == 0),
            }
        })
        .collect::<Vec<_>>();

    let log_records = (0..rows)
        .map(|row_index| {
            let attributes = (0..attrs_per_row)
                .map(|attr_index| {
                    let key = if attr_index % 3 == 0 {
                        format!("attr.{row_index}.{}", attr_index % 2)
                    } else {
                        format!("attr.{attr_index}")
                    };
                    match (row_index + attr_index) % 5 {
                        0 => kv_string(&key, &format!("value-{row_index}-{attr_index}")),
                        1 => {
                            let magnitude = (row_index * 10 + attr_index) as i64;
                            let value = if (row_index + attr_index) % 2 == 0 {
                                magnitude
                            } else {
                                -magnitude
                            };
                            kv_i64(&key, value)
                        }
                        2 => kv_f64(&key, row_index as f64 + attr_index as f64 / 10.0),
                        3 => kv_bool(&key, (row_index + attr_index) % 2 == 0),
                        _ => kv_bytes(&key, &[row_index as u8, attr_index as u8]),
                    }
                })
                .collect::<Vec<_>>();

            LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000u64.saturating_add(row_index as u64),
                observed_time_unix_nano: 1_700_000_000_000_001_000u64
                    .saturating_add(row_index as u64),
                severity_number: (row_index % 24) as i32,
                severity_text: severity.clone().unwrap_or_else(|| "INFO".to_string()),
                body: body.clone().as_deref().map(any_string),
                attributes,
                trace_id: vec![row_index as u8; 16],
                span_id: vec![row_index as u8; 8],
                flags: row_index as u32,
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: scope_name.unwrap_or_else(|| "scope-a".to_string()),
                    version: scope_version.unwrap_or_else(|| "1.0.0".to_string()),
                    ..Default::default()
                }),
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn any_string(value: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(value.into())),
    }
}

fn kv_string(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(any_string(value)),
    }
}

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
    }
}

fn kv_i64(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
    }
}

fn kv_f64(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
    }
}

fn kv_bytes(key: &str, value: &[u8]) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::BytesValue(value.to_vec())),
        }),
    }
}
