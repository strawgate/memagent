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
                    matches!(event, SourceEvent::Batch { batch, .. } if batch.num_rows() == 1)
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
                matches!(event, SourceEvent::Batch { batch, .. } if batch.num_rows() == 1)
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
