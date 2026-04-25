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
