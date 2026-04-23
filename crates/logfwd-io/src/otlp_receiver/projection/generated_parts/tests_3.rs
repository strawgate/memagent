    #[test]
    fn generated_projection_field_numbers_match_core_otlp_constants() {
        assert_eq!(
            EXPORTLOGSSERVICEREQUEST_FIELDS
                .iter()
                .find(|f| f.name == "resource_logs")
                .expect("projection field")
                .number,
            otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS
        );
        assert_eq!(
            RESOURCELOGS_FIELDS
                .iter()
                .find(|f| f.name == "resource")
                .expect("projection field")
                .number,
            otlp::RESOURCE_LOGS_RESOURCE
        );
        assert_eq!(
            RESOURCELOGS_FIELDS
                .iter()
                .find(|f| f.name == "scope_logs")
                .expect("projection field")
                .number,
            otlp::RESOURCE_LOGS_SCOPE_LOGS
        );
        assert_eq!(
            RESOURCE_FIELDS
                .iter()
                .find(|f| f.name == "attributes")
                .expect("projection field")
                .number,
            otlp::RESOURCE_ATTRIBUTES
        );
        assert_eq!(
            SCOPELOGS_FIELDS
                .iter()
                .find(|f| f.name == "scope")
                .expect("projection field")
                .number,
            otlp::SCOPE_LOGS_SCOPE
        );
        assert_eq!(
            SCOPELOGS_FIELDS
                .iter()
                .find(|f| f.name == "log_records")
                .expect("projection field")
                .number,
            otlp::SCOPE_LOGS_LOG_RECORDS
        );
        assert_eq!(
            INSTRUMENTATIONSCOPE_FIELDS
                .iter()
                .find(|f| f.name == "name")
                .expect("projection field")
                .number,
            otlp::INSTRUMENTATION_SCOPE_NAME
        );
        assert_eq!(
            INSTRUMENTATIONSCOPE_FIELDS
                .iter()
                .find(|f| f.name == "version")
                .expect("projection field")
                .number,
            otlp::INSTRUMENTATION_SCOPE_VERSION
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "time_unix_nano")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_TIME_UNIX_NANO
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "severity_number")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_SEVERITY_NUMBER
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "severity_text")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_SEVERITY_TEXT
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "body")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_BODY
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "attributes")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_ATTRIBUTES
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "flags")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_FLAGS
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "trace_id")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_TRACE_ID
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "span_id")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_SPAN_ID
        );
        assert_eq!(
            LOGRECORD_FIELDS
                .iter()
                .find(|f| f.name == "observed_time_unix_nano")
                .expect("projection field")
                .number,
            otlp::LOG_RECORD_OBSERVED_TIME_UNIX_NANO
        );
        assert_eq!(
            KEYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "key")
                .expect("projection field")
                .number,
            otlp::KEY_VALUE_KEY
        );
        assert_eq!(
            KEYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "value")
                .expect("projection field")
                .number,
            otlp::KEY_VALUE_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "string_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_STRING_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "bool_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_BOOL_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "int_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_INT_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "double_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_DOUBLE_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "array_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_ARRAY_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "kvlist_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_KVLIST_VALUE
        );
        assert_eq!(
            ANYVALUE_FIELDS
                .iter()
                .find(|f| f.name == "bytes_value")
                .expect("projection field")
                .number,
            otlp::ANY_VALUE_BYTES_VALUE
        );
    }

    #[test]
    fn generated_decode_anyvalue_keeps_last_oneof_value() {
        let bytes = [42, 0, 10, 2, b'o', b'k'];
        let value = decode_any_value_wire(&bytes).expect("generated AnyValue should decode");
        match value {
            Some(WireAny::String(value)) => assert_eq!(value, b"ok"),
            _ => panic!("unexpected generated AnyValue variant"),
        }
    }

    #[test]
    fn generated_decode_anyvalue_terminal_complex_keeps_last_oneof_value() {
        let bytes = [10, 2, b'o', b'k', 42, 0];
        let value = decode_any_value_wire(&bytes).expect("terminal oneof should decode");
        assert!(matches!(value, Some(WireAny::ArrayRaw(_))));
    }

    #[test]
    fn generated_decode_anyvalue_wrong_wire_is_invalid() {
        let bytes = [8, 1];
        let err = match decode_any_value_wire(&bytes) {
            Ok(_) => panic!("wrong wire type should be invalid"),
            Err(err) => err,
        };
        assert!(matches!(err, ProjectionError::Invalid(_)));
    }

    #[test]
    fn generated_decode_keyvalue_keeps_last_key_and_value() {
        let bytes = [
            10, 3, b'o', b'l', b'd', 10, 3, b'k', b'e', b'y', 18, 4, 10, 2, b'o', b'k',
        ];
        let value = decode_key_value_wire(&bytes).expect("generated KeyValue should decode");
        match value {
            Some((key, WireAny::String(value))) => {
                assert_eq!(key, b"key");
                assert_eq!(value, b"ok");
            }
            _ => panic!("unexpected generated KeyValue variant"),
        }
    }

    #[test]
    fn generated_decode_keyvalue_without_value_is_omitted() {
        let bytes = [10, 3, b'k', b'e', b'y'];
        let value = decode_key_value_wire(&bytes).expect("generated KeyValue should decode");
        assert!(value.is_none());
    }

    #[test]
    fn generated_known_fields_reject_wrong_wire_types() {
        for &message in all_messages() {
            for rule in fields_for(message) {
                for wrong_wire in wrong_wires(rule.expected_wire) {
                    let payload = sample_field_for_wire(rule.number, wrong_wire);
                    let err = scan_message(&payload, message)
                        .expect_err("known field wrong wire should be invalid");
                    assert!(
                        matches!(err, ProjectionError::Invalid(_)),
                        "message={message:?} field={} wrong_wire={wrong_wire:?} err={err:?}",
                        rule.name
                    );
                }
            }
        }
    }

    #[test]
    fn generated_ignored_fields_accept_expected_wire() {
        for &message in all_messages() {
            for rule in fields_for(message) {
                if rule.action != ProjectionAction::Ignore {
                    continue;
                }
                let payload = sample_field_for_wire(rule.number, rule.expected_wire);
                scan_message(&payload, message)
                    .expect("ignored field with expected wire should be accepted");
            }
        }
    }

    #[test]
    fn generated_ignored_fields_reject_wrong_wire() {
        for &message in all_messages() {
            for rule in fields_for(message) {
                if rule.action != ProjectionAction::Ignore {
                    continue;
                }
                for wrong_wire in wrong_wires(rule.expected_wire) {
                    let payload = sample_field_for_wire(rule.number, wrong_wire);
                    let err = scan_message(&payload, message)
                        .expect_err("ignored field wrong wire should be invalid");
                    assert!(
                        matches!(err, ProjectionError::Invalid(_)),
                        "message={message:?} field={} wrong_wire={wrong_wire:?} err={err:?}",
                        rule.name
                    );
                }
            }
        }
    }

    #[test]
    fn generated_anyvalue_complex_shapes_are_projected() {
        for &field_number in complex_anyvalue_field_numbers() {
            let payload = sample_field_for_wire(field_number, WireKind::Len);
            scan_message(&payload, MessageKind::AnyValue)
                .expect("complex AnyValue shape should classify for projection");
            let value = decode_any_value_wire(&payload).expect("complex AnyValue should decode");
            assert!(matches!(
                value,
                Some(WireAny::ArrayRaw(_)) | Some(WireAny::KvListRaw(_))
            ));
        }
    }

    #[test]
    fn generated_anyvalue_oneof_last_value_wins_for_complex_and_primitive() {
        let projected = sample_field_for_wire(otlp::ANY_VALUE_STRING_VALUE, WireKind::Len);
        for &field_number in complex_anyvalue_field_numbers() {
            let complex = sample_field_for_wire(field_number, WireKind::Len);

            let mut complex_then_projected = complex.clone();
            complex_then_projected.extend_from_slice(&projected);
            let value = decode_any_value_wire(&complex_then_projected)
                .expect("terminal projected oneof should decode");
            assert!(matches!(value, Some(WireAny::String(_))));

            let mut projected_then_complex = projected.clone();
            projected_then_complex.extend_from_slice(&complex);
            let value = decode_any_value_wire(&projected_then_complex)
                .expect("terminal complex oneof should decode");
            assert!(matches!(
                value,
                Some(WireAny::ArrayRaw(_)) | Some(WireAny::KvListRaw(_))
            ));
        }
    }
