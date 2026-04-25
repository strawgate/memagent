    #[test]
    fn generated_planned_appenders_write_expected_columns() {
        use arrow::array::{Int64Array, StringViewArray};

        let (plan, handles) = build_otlp_plan();
        let mut builder = ColumnarBatchBuilder::new(plan);
        let mut scratch = WireScratch::default();

        builder.begin_batch();
        builder.begin_row();
        handles.write_timestamp(&mut builder, 11);
        handles.write_observed_timestamp(&mut builder, 12);
        handles
            .write_severity(&mut builder, b"INFO", StringStorage::Decoded)
            .expect("severity appender should write");
        handles.write_severity_number(&mut builder, 9);
        handles
            .write_body(
                &mut builder,
                WireAny::String(b"body"),
                &mut scratch,
                StringStorage::Decoded,
            )
            .expect("body appender should write");
        handles
            .write_trace_id(&mut builder, &[0xab, 0xcd])
            .expect("trace id appender should write");
        handles
            .write_span_id(&mut builder, &[0x12, 0x34])
            .expect("span id appender should write");
        handles.write_flags(&mut builder, 1);
        handles
            .write_scope_name(&mut builder, b"scope", StringStorage::Decoded)
            .expect("scope name appender should write");
        handles
            .write_scope_version(&mut builder, b"1.2.3", StringStorage::Decoded)
            .expect("scope version appender should write");
        builder.end_row();

        let batch = builder
            .finish_batch()
            .expect("generated appenders should produce a batch");

        let int_value = |name: &str| -> i64 {
            let column = batch.column(batch.schema().index_of(name).expect("planned int field"));
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("planned field should be Int64")
                .value(0)
        };
        let str_value = |name: &str| -> String {
            let column = batch.column(batch.schema().index_of(name).expect("planned string field"));
            column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("planned field should be StringViewArray")
                .value(0)
                .to_owned()
        };

        assert_eq!(int_value(field_names::TIMESTAMP), 11);
        assert_eq!(int_value(field_names::OBSERVED_TIMESTAMP), 12);
        assert_eq!(str_value(field_names::SEVERITY), "INFO");
        assert_eq!(int_value(field_names::SEVERITY_NUMBER), 9);
        assert_eq!(str_value(field_names::BODY), "body");
        assert_eq!(str_value(field_names::TRACE_ID), "abcd");
        assert_eq!(str_value(field_names::SPAN_ID), "1234");
        assert_eq!(int_value(field_names::FLAGS), 1);
        assert_eq!(str_value(field_names::SCOPE_NAME), "scope");
        assert_eq!(str_value(field_names::SCOPE_VERSION), "1.2.3");
    }

    #[test]
    fn generated_scope_decoder_merges_partial_scope_messages() {
        let mut scope_fields = ScopeFields::default();
        let mut name = Vec::new();
        let mut version = Vec::new();
        push_len_field(
            &mut name,
            field_numbers::INSTRUMENTATION_SCOPE_NAME,
            b"scope-a",
        );
        push_len_field(
            &mut version,
            field_numbers::INSTRUMENTATION_SCOPE_VERSION,
            b"1.2.3",
        );

        merge_scope_wire(&name, &mut scope_fields).expect("scope name should decode");
        merge_scope_wire(&version, &mut scope_fields).expect("scope version should decode");

        assert_eq!(scope_fields.name, Some(b"scope-a".as_slice()));
        assert_eq!(scope_fields.version, Some(b"1.2.3".as_slice()));
    }

    #[test]
    fn generated_log_record_decoder_extracts_projected_fields() {
        let mut body = Vec::new();
        push_len_field(&mut body, field_numbers::ANY_VALUE_STRING_VALUE, b"body");

        let mut attr_value = Vec::new();
        push_len_field(
            &mut attr_value,
            field_numbers::ANY_VALUE_STRING_VALUE,
            b"value",
        );

        let mut attr = Vec::new();
        push_len_field(&mut attr, field_numbers::KEY_VALUE_KEY, b"key");
        push_len_field(&mut attr, field_numbers::KEY_VALUE_VALUE, &attr_value);

        let trace_id = [0x11; 16];
        let span_id = [0x22; 8];
        let mut log_record = Vec::new();
        push_fixed64_field(
            &mut log_record,
            field_numbers::LOG_RECORD_TIME_UNIX_NANO,
            11,
        );
        push_fixed64_field(
            &mut log_record,
            field_numbers::LOG_RECORD_OBSERVED_TIME_UNIX_NANO,
            12,
        );
        push_varint_field(
            &mut log_record,
            field_numbers::LOG_RECORD_SEVERITY_NUMBER,
            9,
        );
        push_len_field(
            &mut log_record,
            field_numbers::LOG_RECORD_SEVERITY_TEXT,
            b"INFO",
        );
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_BODY, &body);
        push_len_field(
            &mut log_record,
            field_numbers::LOG_RECORD_TRACE_ID,
            &trace_id,
        );
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_SPAN_ID, &span_id);
        push_fixed32_field(&mut log_record, field_numbers::LOG_RECORD_FLAGS, 1);
        push_len_field(&mut log_record, field_numbers::LOG_RECORD_ATTRIBUTES, &attr);

        let mut attr_ranges = Vec::new();
        let decoded = decode_log_record_fields(&log_record, &mut attr_ranges)
            .expect("log record fields should decode");

        assert_eq!(decoded.time_unix_nano, 11);
        assert_eq!(decoded.observed_time_unix_nano, 12);
        assert_eq!(decoded.severity_number, 9);
        assert_eq!(decoded.severity_text, Some(b"INFO".as_slice()));
        assert_eq!(decoded.trace_id, Some(trace_id.as_slice()));
        assert_eq!(decoded.span_id, Some(span_id.as_slice()));
        assert_eq!(decoded.flags, 1);
        match decoded.body {
            Some(WireAny::String(value)) => assert_eq!(value, b"body"),
            _ => panic!("body should decode as string AnyValue"),
        }
        assert_eq!(attr_ranges.len(), 1);
        let (start, len) = attr_ranges[0];
        assert_eq!(&log_record[start..start + len], attr.as_slice());
    }

    #[test]
    fn generated_len_visitors_yield_expected_payloads() {
        let mut export = Vec::new();
        let mut resource_logs = Vec::new();
        let mut resource = Vec::new();
        let mut scope_logs = Vec::new();

        push_len_field(&mut resource, field_numbers::RESOURCE_ATTRIBUTES, b"attr");
        push_len_field(&mut scope_logs, field_numbers::SCOPE_LOGS_SCOPE, b"scope");
        push_len_field(
            &mut scope_logs,
            field_numbers::SCOPE_LOGS_LOG_RECORDS,
            b"log",
        );
        push_len_field(
            &mut resource_logs,
            field_numbers::RESOURCE_LOGS_RESOURCE,
            &resource,
        );
        push_len_field(
            &mut resource_logs,
            field_numbers::RESOURCE_LOGS_SCOPE_LOGS,
            &scope_logs,
        );
        push_len_field(
            &mut export,
            field_numbers::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            &resource_logs,
        );

        let mut seen_resource_logs = Vec::new();
        for_each_export_resource_logs(&export, |payload| {
            seen_resource_logs.push(payload.to_vec());
            Ok(())
        })
        .expect("export visitor should yield resource logs");
        assert_eq!(seen_resource_logs, vec![resource_logs.clone()]);

        let mut seen_resources = Vec::new();
        for_each_resource_logs_resource(&resource_logs, |payload| {
            seen_resources.push(payload.to_vec());
            Ok(())
        })
        .expect("resource visitor should yield resource payload");
        assert_eq!(seen_resources, vec![resource.clone()]);

        let mut seen_scope_logs = Vec::new();
        for_each_resource_logs_scope_logs(&resource_logs, |payload| {
            seen_scope_logs.push(payload.to_vec());
            Ok(())
        })
        .expect("scope logs visitor should yield scope logs payload");
        assert_eq!(seen_scope_logs, vec![scope_logs.clone()]);

        let mut seen_attrs = Vec::new();
        for_each_resource_attribute(&resource, |payload| {
            seen_attrs.push(payload.to_vec());
            Ok(())
        })
        .expect("resource attribute visitor should yield attribute payload");
        assert_eq!(seen_attrs, vec![b"attr".to_vec()]);

        let mut seen_scopes = Vec::new();
        for_each_scope_logs_scope(&scope_logs, |payload| {
            seen_scopes.push(payload.to_vec());
            Ok(())
        })
        .expect("scope visitor should yield scope payload");
        assert_eq!(seen_scopes, vec![b"scope".to_vec()]);

        let mut seen_logs = Vec::new();
        for_each_scope_logs_log_record(&scope_logs, |payload| {
            seen_logs.push(payload.to_vec());
            Ok(())
        })
        .expect("log visitor should yield log payload");
        assert_eq!(seen_logs, vec![b"log".to_vec()]);
    }

    #[test]
    fn generated_len_visitors_reject_wrong_wire() {
        let payload =
            sample_field_for_wire(field_numbers::RESOURCE_LOGS_SCOPE_LOGS, WireKind::Varint);
        let err = for_each_resource_logs_scope_logs(&payload, |_| Ok(()))
            .expect_err("wrong wire should be invalid");
        assert!(matches!(err, ProjectionError::Invalid(_)));
    }

    #[test]
    fn generated_logrecord_table_tracks_projected_protocol_fields() {
        let fields = fields_for(MessageKind::LogRecord);
        assert!(
            fields
                .iter()
                .any(|f| f.name == "body" && f.child == Some(MessageKind::AnyValue))
        );
        assert!(
            fields
                .iter()
                .any(|f| f.name == "attributes" && f.child == Some(MessageKind::KeyValue))
        );
        assert!(
            fields
                .iter()
                .any(|f| f.name == "flags" && f.expected_wire == WireKind::Fixed32)
        );
    }
