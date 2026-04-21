//! Tests for star schema flat ↔ OTAP conversion.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampNanosecondArray, UInt8Array, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::super::helpers::{
        build_fixed_binary_array, parse_rfc3339_nanos, parse_timestamp_to_nanos,
    };
    use super::super::star_to_flat::collect_template_values_by_id;
    use super::super::{ATTR_TYPE_BYTES, ATTR_TYPE_STR};
    use super::super::{StarSchema, attrs_schema, flat_to_star, logs_schema, star_to_flat};

    /// Helper: create a flat RecordBatch with mixed columns.
    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("resource.attributes.service_name", DataType::Utf8, true),
            Field::new("resource.attributes.k8s_pod", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00.123456789Z"),
                Some("2024-01-15T10:30:01.000000000Z"),
                Some("2024-01-15T10:30:02.500Z"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("INFO"),
                Some("ERROR"),
                Some("WARN"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("request started"),
                Some("connection failed"),
                Some("retry attempt"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("00112233445566778899aabbccddeeff"),
                Some("fedcba98765432100123456789abcdef"),
                Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("0011223344556677"),
                Some("8899aabbccddeeff"),
                Some("1234567890abcdef"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(3), Some(255)])),
            Arc::new(StringArray::from(vec![
                Some("api-server"),
                Some("api-server"),
                Some("worker"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("pod-abc"),
                Some("pod-abc"),
                Some("pod-xyz"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("host-1"),
                Some("host-2"),
                Some("host-1"),
            ])),
            Arc::new(Int64Array::from(vec![Some(200), Some(500), Some(429)])),
        ];

        RecordBatch::try_new(schema, columns).expect("valid batch")
    }

    #[test]
    fn roundtrip_preserves_data() {
        let flat = make_test_batch();
        let star = flat_to_star(&flat).expect("flat_to_star");

        // Verify star schema structure.
        assert_eq!(star.logs.num_rows(), 3);
        assert!(star.log_attrs.num_rows() > 0, "should have log attrs");
        assert!(
            star.resource_attrs.num_rows() > 0,
            "should have resource attrs"
        );
        assert_eq!(star.scope_attrs.num_rows(), 1);

        // Convert back.
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 3);

        // Verify well-known columns survived.
        let rt_schema = roundtrip.schema();

        // _timestamp
        let ts_idx = rt_schema.index_of("_timestamp").expect("_timestamp col");
        let ts_arr = roundtrip
            .column(ts_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ts string");
        // Timestamps may lose trailing zeros but must parse to the same value.
        assert!(
            ts_arr
                .value(0)
                .starts_with("2024-01-15T10:30:00.123456789Z")
        );
        assert!(ts_arr.value(1).starts_with("2024-01-15T10:30:01Z"));

        // level
        let lvl_idx = rt_schema.index_of("level").expect("level col");
        let lvl_arr = roundtrip
            .column(lvl_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("level string");
        assert_eq!(lvl_arr.value(0), "INFO");
        assert_eq!(lvl_arr.value(1), "ERROR");
        assert_eq!(lvl_arr.value(2), "WARN");

        // message
        let msg_idx = rt_schema.index_of("message").expect("message col");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("msg string");
        assert_eq!(msg_arr.value(0), "request started");
        assert_eq!(msg_arr.value(1), "connection failed");
        assert_eq!(msg_arr.value(2), "retry attempt");

        let trace_idx = rt_schema.index_of("trace_id").expect("trace_id col");
        let trace_arr = roundtrip
            .column(trace_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trace string");
        assert_eq!(trace_arr.value(0), "00112233445566778899aabbccddeeff");
        assert_eq!(trace_arr.value(1), "fedcba98765432100123456789abcdef");
        assert_eq!(trace_arr.value(2), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let span_idx = rt_schema.index_of("span_id").expect("span_id col");
        let span_arr = roundtrip
            .column(span_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("span string");
        assert_eq!(span_arr.value(0), "0011223344556677");
        assert_eq!(span_arr.value(1), "8899aabbccddeeff");
        assert_eq!(span_arr.value(2), "1234567890abcdef");

        let flags_idx = rt_schema.index_of("flags").expect("flags col");
        let flags_arr = roundtrip
            .column(flags_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("flags i64");
        assert_eq!(flags_arr.value(0), 1);
        assert_eq!(flags_arr.value(1), 3);
        assert_eq!(flags_arr.value(2), 255);

        let sev_num_idx = rt_schema
            .index_of("severity_number")
            .expect("severity_number col");
        let sev_num_arr = roundtrip
            .column(sev_num_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("severity_number i64");
        assert_eq!(sev_num_arr.value(0), 9); // INFO
        assert_eq!(sev_num_arr.value(1), 17); // ERROR
        assert_eq!(sev_num_arr.value(2), 13); // WARN

        let scope_name_idx = rt_schema.index_of("scope.name").expect("scope.name col");
        let scope_name_arr = roundtrip
            .column(scope_name_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("scope.name string");
        assert_eq!(scope_name_arr.value(0), "logfwd");
        assert_eq!(scope_name_arr.value(1), "logfwd");
        assert_eq!(scope_name_arr.value(2), "logfwd");

        let rs_idx = rt_schema
            .index_of("resource.attributes.service_name")
            .expect("resource_service_name col");
        let rs_arr = roundtrip
            .column(rs_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resource str");
        assert_eq!(rs_arr.value(0), "api-server");
        assert_eq!(rs_arr.value(1), "api-server");
        assert_eq!(rs_arr.value(2), "worker");

        let rp_idx = rt_schema
            .index_of("resource.attributes.k8s_pod")
            .expect("resource_k8s_pod col");
        let rp_arr = roundtrip
            .column(rp_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resource str");
        assert_eq!(rp_arr.value(0), "pod-abc");
        assert_eq!(rp_arr.value(1), "pod-abc");
        assert_eq!(rp_arr.value(2), "pod-xyz");

        // host (attribute column)
        let host_idx = rt_schema.index_of("host").expect("host col");
        let host_arr = roundtrip
            .column(host_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("host str");
        assert_eq!(host_arr.value(0), "host-1");
        assert_eq!(host_arr.value(1), "host-2");
        assert_eq!(host_arr.value(2), "host-1");

        // status (int attribute — now roundtrips as Int64)
        let st_idx = rt_schema.index_of("status").expect("status col");
        let st_arr = roundtrip
            .column(st_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("status i64");
        assert_eq!(st_arr.value(0), 200);
        assert_eq!(st_arr.value(1), 500);
        assert_eq!(st_arr.value(2), 429);
    }

    #[test]
    fn roundtrip_preserves_existing_scope_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("scope.name", DataType::Utf8, true),
            Field::new("scope.version", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(StringArray::from(vec![Some("otel"), Some("custom")])),
                Arc::new(StringArray::from(vec![Some("1.0.0"), Some("2.0.0")])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let scope_name = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.name")
                    .expect("scope.name idx"),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("scope.name utf8");
        assert_eq!(scope_name.value(0), "otel");
        assert_eq!(scope_name.value(1), "custom");

        let scope_version = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.version")
                    .expect("scope.version idx"),
            )
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("scope.version utf8");
        assert_eq!(scope_version.value(0), "1.0.0");
        assert_eq!(scope_version.value(1), "2.0.0");
    }

    #[test]
    fn roundtrip_preserves_typed_scope_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("scope.name", DataType::Utf8, true),
            Field::new("scope.enabled", DataType::Boolean, true),
            Field::new("scope.rank", DataType::Int64, true),
            Field::new("scope.ratio", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(StringArray::from(vec![Some("otel"), Some("otel")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
                Arc::new(Int64Array::from(vec![Some(7), Some(7)])),
                Arc::new(Float64Array::from(vec![Some(0.25), Some(0.25)])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let scope_enabled = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.enabled")
                    .expect("scope.enabled idx"),
            )
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("scope.enabled bool");
        assert!(scope_enabled.value(0));
        assert!(scope_enabled.value(1));

        let scope_rank = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.rank")
                    .expect("scope.rank idx"),
            )
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("scope.rank i64");
        assert_eq!(scope_rank.value(0), 7);
        assert_eq!(scope_rank.value(1), 7);

        let scope_ratio = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.ratio")
                    .expect("scope.ratio idx"),
            )
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("scope.ratio f64");
        assert_eq!(scope_ratio.value(0), 0.25);
        assert_eq!(scope_ratio.value(1), 0.25);
    }

    #[test]
    fn noncanonical_scope_columns_do_not_inject_default_scope_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("scope.enabled", DataType::Boolean, true),
            Field::new("scope.rank", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(Int64Array::from(vec![Some(7), Some(9)])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        assert_eq!(
            star.scope_attrs.num_rows(),
            0,
            "any real scope.* columns should suppress the default scope row"
        );

        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert!(
            roundtrip.column_by_name("scope.name").is_none(),
            "default scope.name must not be synthesized when only noncanonical scope columns exist"
        );

        let scope_enabled = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.enabled")
                    .expect("scope.enabled idx"),
            )
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("scope.enabled bool");
        assert!(scope_enabled.value(0));
        assert!(!scope_enabled.value(1));

        let scope_rank = roundtrip
            .column(
                roundtrip
                    .schema()
                    .index_of("scope.rank")
                    .expect("scope.rank idx"),
            )
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("scope.rank i64");
        assert_eq!(scope_rank.value(0), 7);
        assert_eq!(scope_rank.value(1), 9);
    }

    #[test]
    fn empty_batch_produces_empty_star() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let star = flat_to_star(&batch).expect("flat_to_star empty");

        assert_eq!(star.logs.num_rows(), 0);
        assert_eq!(star.log_attrs.num_rows(), 0);
        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert_eq!(star.scope_attrs.num_rows(), 0);

        let roundtrip = star_to_flat(&star).expect("star_to_flat empty");
        assert_eq!(roundtrip.num_rows(), 0);
    }

    #[test]
    fn collect_template_values_prefers_template_row_index() {
        let values = vec![None, None, Some("otel".to_string())];
        let ids = UInt32Array::from(vec![2u32, 2, 2]);

        let collected = collect_template_values_by_id(&values, &ids, 3);

        assert_eq!(collected.get(&2), Some(&Some("otel".to_string())));
    }

    #[test]
    fn collect_template_values_skips_template_ids_outside_column_len() {
        let values = vec![Some("otel".to_string())];
        let ids = UInt32Array::from(vec![0u32, 1]);

        let collected = collect_template_values_by_id(&values, &ids, 2);

        assert_eq!(collected.get(&0), Some(&Some("otel".to_string())));
        assert!(!collected.contains_key(&1));
    }

    #[test]
    fn batch_with_no_resource_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["hello", "world"])),
            Arc::new(StringArray::from(vec!["INFO", "DEBUG"])),
            Arc::new(StringArray::from(vec!["host-a", "host-b"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.logs.num_rows(), 2);
        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert!(star.log_attrs.num_rows() > 0);

        // All rows share the same (empty) resource set → single resource_id.
        let rid_arr = star
            .logs
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("rid");
        assert_eq!(rid_arr.value(0), rid_arr.value(1));

        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 2);

        let msg_idx = roundtrip.schema().index_of("message").expect("msg");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(msg_arr.value(0), "hello");
        assert_eq!(msg_arr.value(1), "world");
    }

    #[test]
    fn flat_to_star_prefers_canonical_body_over_message_alias() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["alias-value"])),
            Arc::new(StringArray::from(vec!["canonical-value"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let body_idx = star.logs.schema().index_of("body_str").expect("body_str");
        let body_arr = star
            .logs
            .column(body_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body_str should be Utf8");
        assert_eq!(
            body_arr.value(0),
            "canonical-value",
            "canonical body should win over alias columns"
        );
    }

    #[test]
    fn flat_to_star_keeps_unselected_body_alias_as_attribute() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["alias-value"])),
            Arc::new(StringArray::from(vec!["canonical-value"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let key_idx = star.log_attrs.schema().index_of("key").expect("key");
        let str_idx = star.log_attrs.schema().index_of("str").expect("str");
        let keys = star
            .log_attrs
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key should be Utf8");
        let values = star
            .log_attrs
            .column(str_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str should be Utf8");
        let message_value = keys
            .iter()
            .zip(values.iter())
            .find_map(|(key, value)| if key == Some("message") { value } else { None });

        assert_eq!(message_value, Some("alias-value"));
    }

    #[test]
    fn batch_with_only_resource_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.service", DataType::Utf8, true),
            Field::new("resource.attributes.env", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["api", "worker"])),
            Arc::new(StringArray::from(vec!["prod", "prod"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.logs.num_rows(), 2);
        assert!(star.resource_attrs.num_rows() > 0);
        assert_eq!(star.log_attrs.num_rows(), 0);

        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 2);

        let svc_idx = roundtrip
            .schema()
            .index_of("resource.attributes.service")
            .expect("svc");
        let svc_arr = roundtrip
            .column(svc_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(svc_arr.value(0), "api");
        assert_eq!(svc_arr.value(1), "worker");
    }

    #[test]
    fn legacy_resource_prefix_is_regular_log_attribute() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("_resource_service", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(StringArray::from(vec!["api"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert_eq!(star.log_attrs.num_rows(), 1);

        let key_idx = star.log_attrs.schema().index_of("key").expect("key");
        let keys = star
            .log_attrs
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key strings");
        assert_eq!(keys.value(0), "_resource_service");
    }

    #[test]
    fn null_values_handled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
            Arc::new(StringArray::from(vec![None, Some("ERROR"), None])),
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00Z"),
                None,
                Some("2024-01-15T10:30:02Z"),
            ])),
            Arc::new(StringArray::from(vec![None, None, Some("extra-val")])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.logs.num_rows(), 3);

        // Verify nulls in body.
        let body_idx = star.logs.schema().index_of("body_str").expect("body_str");
        let body_arr = star
            .logs
            .column(body_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert!(!body_arr.is_null(0));
        assert!(body_arr.is_null(1)); // was None in input
        assert!(!body_arr.is_null(2));

        // Roundtrip.
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        assert_eq!(roundtrip.num_rows(), 3);

        let msg_idx = roundtrip.schema().index_of("message").expect("msg");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(msg_arr.value(0), "hello");
        assert!(msg_arr.is_null(1));
        assert_eq!(msg_arr.value(2), "world");
    }

    #[test]
    fn resource_deduplication() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.svc", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["api", "api", "worker"])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        assert_eq!(star.resource_attrs.num_rows(), 2);

        let rid_arr = star
            .logs
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("rid");
        assert_eq!(rid_arr.value(0), rid_arr.value(1));
        assert_ne!(rid_arr.value(0), rid_arr.value(2));
    }

    #[test]
    fn resource_dedup_distinguishes_null_and_empty_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.svc", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![None, Some(""), Some("")])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let rid_arr = star
            .logs
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("resource_id");
        assert_ne!(
            rid_arr.value(0),
            rid_arr.value(1),
            "NULL and empty-string resource attrs must dedupe separately"
        );
        assert_eq!(rid_arr.value(1), rid_arr.value(2));
    }

    #[test]
    fn severity_mapping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL",
            ])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        let sev_idx = star.logs.schema().index_of("severity_number").expect("sev");
        let sev_arr = star
            .logs
            .column(sev_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");

        assert_eq!(sev_arr.value(0), 1); // TRACE
        assert_eq!(sev_arr.value(1), 5); // DEBUG
        assert_eq!(sev_arr.value(2), 9); // INFO
        assert_eq!(sev_arr.value(3), 13); // WARN
        assert_eq!(sev_arr.value(4), 17); // ERROR
        assert_eq!(sev_arr.value(5), 21); // FATAL
    }

    #[test]
    fn star_schema_table_schemas() {
        let batch = make_test_batch();
        let star = flat_to_star(&batch).expect("flat_to_star");

        let logs_schema = star.logs.schema();
        let logs_fields: Vec<&str> = logs_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            logs_fields,
            vec![
                "id",
                "resource_id",
                "scope_id",
                "time_unix_nano",
                "severity_number",
                "severity_text",
                "body_str",
                "trace_id",
                "span_id",
                "flags",
                "dropped_attributes_count"
            ]
        );

        let attrs_schema = star.log_attrs.schema();
        let attrs_fields: Vec<&str> = attrs_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            attrs_fields,
            vec![
                "parent_id",
                "key",
                "type",
                "str",
                "int",
                "double",
                "bool",
                "bytes"
            ]
        );
    }

    #[test]
    fn int_attrs_use_typed_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(Int64Array::from(vec![Some(200)])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");

        let type_arr = star
            .log_attrs
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("u8");

        let key_arr = star
            .log_attrs
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");

        let mut found = false;
        for row in 0..star.log_attrs.num_rows() {
            if key_arr.value(row) == "status" {
                assert_eq!(type_arr.value(row), super::super::ATTR_TYPE_INT);
                let int_arr = star
                    .log_attrs
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("i64");
                assert_eq!(int_arr.value(row), 200);
                found = true;
                break;
            }
        }
        assert!(found, "status attr not found in log_attrs");
    }

    #[test]
    fn binary_attrs_roundtrip_as_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("row-0"), Some("row-1")])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0x00_u8, 0x0f_u8, 0xff_u8][..]),
                Some(&[0xab_u8, 0xcd_u8][..]),
            ])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid batch");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let payload_idx = roundtrip.schema().index_of("payload").expect("payload col");
        let payload_arr = roundtrip
            .column(payload_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("payload binary");

        assert_eq!(payload_arr.value(0), &[0x00, 0x0f, 0xff]);
        assert_eq!(payload_arr.value(1), &[0xab, 0xcd]);
    }

    #[test]
    fn mixed_attrs_promote_bytes_first_column_to_string() {
        let logs = RecordBatch::try_new(
            Arc::new(logs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32, 1])),
                Arc::new(UInt32Array::from(vec![0_u32, 0])),
                Arc::new(UInt32Array::from(vec![0_u32, 0])),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>, None])),
                Arc::new(Int32Array::from(vec![None::<i32>, None])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![Some("row-0"), Some("row-1")])),
                build_fixed_binary_array::<16>(&[None, None]).expect("trace ids"),
                build_fixed_binary_array::<8>(&[None, None]).expect("span ids"),
                Arc::new(UInt32Array::from(vec![None::<u32>, None])),
                Arc::new(UInt32Array::from(vec![Some(0_u32), Some(0)])),
            ],
        )
        .expect("valid logs");

        let log_attrs = RecordBatch::try_new(
            Arc::new(attrs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32, 1])),
                Arc::new(StringArray::from(vec!["payload", "payload"])),
                Arc::new(UInt8Array::from(vec![ATTR_TYPE_BYTES, ATTR_TYPE_STR])),
                Arc::new(StringArray::from(vec![None, Some("text")])),
                Arc::new(Int64Array::from(vec![None::<i64>, None])),
                Arc::new(Float64Array::from(vec![None::<f64>, None])),
                Arc::new(BooleanArray::from(vec![None::<bool>, None])),
                Arc::new(BinaryArray::from(vec![Some(&[0xde_u8, 0xad_u8][..]), None])),
            ],
        )
        .expect("valid attrs");

        let star = StarSchema {
            logs,
            log_attrs,
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        let payload_idx = roundtrip.schema().index_of("payload").expect("payload");
        let payload_arr = roundtrip
            .column(payload_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("mixed column promotes to Utf8");

        assert_eq!(payload_arr.value(0), "dead");
        assert_eq!(payload_arr.value(1), "text");
    }

    #[test]
    fn log_attr_collision_does_not_overwrite_fact_column() {
        let logs = RecordBatch::try_new(
            Arc::new(logs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>])),
                Arc::new(Int32Array::from(vec![Some(9_i32)])),
                Arc::new(StringArray::from(vec![Some("ERROR")])),
                Arc::new(StringArray::from(vec![Some("row-0")])),
                build_fixed_binary_array::<16>(&[None]).expect("trace ids"),
                build_fixed_binary_array::<8>(&[None]).expect("span ids"),
                Arc::new(UInt32Array::from(vec![Some(3_u32)])),
                Arc::new(UInt32Array::from(vec![Some(0_u32)])),
            ],
        )
        .expect("valid logs");

        let log_attrs = RecordBatch::try_new(
            Arc::new(attrs_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(StringArray::from(vec!["flags"])),
                Arc::new(UInt8Array::from(vec![ATTR_TYPE_STR])),
                Arc::new(StringArray::from(vec![Some("not-flags")])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Float64Array::from(vec![None::<f64>])),
                Arc::new(BooleanArray::from(vec![None::<bool>])),
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
            ],
        )
        .expect("valid attrs");

        let star = StarSchema {
            logs,
            log_attrs,
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };
        let roundtrip = star_to_flat(&star).expect("star_to_flat");
        let flags_idx = roundtrip.schema().index_of("flags").expect("flags");
        let flags_arr = roundtrip
            .column(flags_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("fact flags stays int");

        assert_eq!(flags_arr.value(0), 3);
    }

    #[test]
    fn binary_resource_attrs_roundtrip_as_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("resource.attributes.payload", DataType::Binary, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("row-0"),
                Some("row-1"),
                Some("row-2"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0xde_u8, 0xad_u8, 0xbe_u8, 0xef_u8][..]),
                Some(&[0xde_u8, 0xad_u8, 0xbe_u8, 0xef_u8][..]),
                Some(&[0x01_u8, 0x02_u8][..]),
            ])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid batch");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let payload_idx = roundtrip
            .schema()
            .index_of("resource.attributes.payload")
            .expect("resource.attributes.payload col");
        let payload_arr = roundtrip
            .column(payload_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("resource.attributes.payload binary");

        assert_eq!(payload_arr.value(0), &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(payload_arr.value(1), &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(payload_arr.value(2), &[0x01, 0x02]);
    }

    #[test]
    fn typed_resource_attrs_scatter_for_duplicate_resource_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource.attributes.retry_count", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(9)])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let idx = roundtrip
            .schema()
            .index_of("resource.attributes.retry_count")
            .expect("resource.attributes.retry_count");
        let arr = roundtrip
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(arr.value(0), 5);
        assert_eq!(arr.value(1), 5);
        assert_eq!(arr.value(2), 9);
    }

    #[test]
    fn test_parse_timestamp_to_nanos_integer() {
        assert_eq!(
            parse_timestamp_to_nanos("1705314600"),
            Some(1_705_314_600_000_000_000)
        );
        assert_eq!(
            parse_timestamp_to_nanos("1705314600123"),
            Some(1_705_314_600_123_000_000)
        );
        assert_eq!(
            parse_timestamp_to_nanos("1705314600123456"),
            Some(1_705_314_600_123_456_000)
        );
        assert_eq!(
            parse_timestamp_to_nanos("1705314600123456789"),
            Some(1_705_314_600_123_456_789)
        );
    }

    #[test]
    fn test_parse_timestamp_to_nanos_overflow() {
        assert_eq!(parse_timestamp_to_nanos("10413792000"), None);
    }

    #[test]
    fn test_parse_rfc3339_nanos_invalid_time() {
        assert_eq!(parse_rfc3339_nanos("2024-01-01T99:99:99Z"), None);
        assert_eq!(parse_rfc3339_nanos("2024-01-01T24:00:00Z"), None);
        assert_eq!(parse_rfc3339_nanos("2024-01-01T23:60:00Z"), None);
        assert_eq!(parse_rfc3339_nanos("2024-01-01T23:59:61Z"), None);
    }

    #[test]
    fn timestamp_roundtrip_precision() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));

        let ts = "2024-06-15T08:30:45.123456789Z";
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some(ts)])),
            Arc::new(StringArray::from(vec![Some("test")])),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("valid");
        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let ts_idx = roundtrip.schema().index_of("_timestamp").expect("ts");
        let ts_arr = roundtrip
            .column(ts_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");

        assert_eq!(ts_arr.value(0), ts);
    }

    #[test]
    fn star_to_flat_returns_error_for_invalid_logs_column_types() {
        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            Field::new("time_unix_nano", DataType::Utf8, true),
            Field::new("severity_text", DataType::Int64, true),
            Field::new("body_str", DataType::Boolean, true),
        ]));

        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(StringArray::from(vec![Some("2024-01-01T00:00:00Z")])),
                Arc::new(Int64Array::from(vec![Some(9i64)])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .expect("valid malformed logs batch");

        let star = StarSchema {
            logs,
            log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };

        let err = star_to_flat(&star).expect_err("invalid schema must return error");
        assert!(
            err.to_string()
                .contains("time_unix_nano must be Timestamp(Nanosecond)"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_optional_logs_column_types() {
        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            Field::new("severity_number", DataType::Utf8, true),
        ]));
        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(StringArray::from(vec![Some("9")])),
            ],
        )
        .expect("valid malformed logs batch");
        let star = StarSchema {
            logs,
            log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };

        let err = star_to_flat(&star).expect_err("invalid severity_number type must fail");
        assert!(
            err.to_string().contains("severity_number must be Int32"),
            "unexpected error: {err}"
        );
    }

    fn assert_optional_logs_column_type_error(field: Field, column: ArrayRef, expected: &str) {
        let field_name = field.name().clone();
        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            field,
        ]));
        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(UInt32Array::from(vec![0u32])),
                column,
            ],
        )
        .expect("valid malformed logs batch");
        let star = StarSchema {
            logs,
            log_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            resource_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
            scope_attrs: RecordBatch::new_empty(Arc::new(attrs_schema())),
        };

        let err = star_to_flat(&star).expect_err("invalid optional LOGS column type must fail");
        let message = err.to_string();
        assert!(
            message.contains(&field_name),
            "expected error to mention {field_name}, got: {message}"
        );
        assert!(
            message.contains(expected),
            "expected error to mention {expected}, got: {message}"
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_trace_id_type() {
        assert_optional_logs_column_type_error(
            Field::new("trace_id", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("not-binary")])) as ArrayRef,
            "FixedSizeBinary(16)",
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_span_id_type() {
        assert_optional_logs_column_type_error(
            Field::new("span_id", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("not-binary")])) as ArrayRef,
            "FixedSizeBinary(8)",
        );
    }

    #[test]
    fn star_to_flat_returns_error_for_unsupported_flags_type() {
        assert_optional_logs_column_type_error(
            Field::new("flags", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(1i64)])) as ArrayRef,
            "UInt32",
        );
    }

    #[test]
    fn empty_string_log_attr_survives_star_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("user.id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
                Arc::new(StringArray::from(vec![Some(""), Some("alice")])),
            ],
        )
        .expect("valid batch");

        let star = flat_to_star(&batch).expect("flat_to_star");
        let roundtrip = star_to_flat(&star).expect("star_to_flat");

        let idx = roundtrip.schema().index_of("user.id").expect("user.id");
        let values = roundtrip
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert!(!values.is_null(0), "empty string must not become NULL");
        assert_eq!(values.value(0), "");
        assert_eq!(values.value(1), "alice");
    }

    #[test]
    fn conflict_struct_column_preserved_in_log_attrs() {
        use arrow::array::{Int64Array, StructArray};

        let int_arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(200), None]));
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, Some("NOT_FOUND")]));

        let struct_field = Field::new(
            "status",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("int", DataType::Int64, true),
                Field::new("str", DataType::Utf8, true),
            ])),
            true,
        );
        let struct_col: ArrayRef = Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Field::new("int", DataType::Int64, true),
                Field::new("str", DataType::Utf8, true),
            ]),
            vec![int_arr, str_arr],
            None,
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            struct_field,
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("request ok"),
                    Some("not found"),
                ])),
                struct_col,
            ],
        )
        .expect("valid batch");

        assert!(
            batch.schema().fields().iter().any(|f| {
                if let DataType::Struct(cf) = f.data_type() {
                    !cf.is_empty()
                        && cf
                            .iter()
                            .all(|c| matches!(c.name().as_str(), "int" | "float" | "str" | "bool"))
                } else {
                    false
                }
            }),
            "test batch must contain a conflict struct column"
        );

        let star = flat_to_star(&batch).expect("flat_to_star");

        let log_attrs = &star.log_attrs;
        let key_arr = log_attrs
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let str_arr = log_attrs
            .column_by_name("str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let status_values: Vec<_> = (0..log_attrs.num_rows())
            .filter(|&i| key_arr.value(i) == "status")
            .map(|i| {
                if str_arr.is_null(i) {
                    None
                } else {
                    Some(str_arr.value(i))
                }
            })
            .collect();
        for value in &status_values {
            match value {
                Some("200" | "NOT_FOUND") => {}
                other => panic!("unexpected status value in log_attrs: {:?}", other),
            }
        }
        assert!(
            status_values.contains(&Some("200")),
            "status=200 (from int child) must appear in log_attrs, got: {:?}",
            status_values
                .iter()
                .map(|value| value.map_or_else(|| "NULL".to_string(), str::to_string))
                .collect::<Vec<_>>()
        );
        assert!(
            status_values.contains(&Some("NOT_FOUND")),
            "status=NOT_FOUND (from str child) must appear in log_attrs"
        );
    }

    #[test]
    fn at_timestamp_col_maps_to_time_unix_nano() {
        let ts_ns: i64 = 1_705_314_600_000_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("@timestamp", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![ts_ns])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();

        let star = flat_to_star(&batch).unwrap();

        let time_col = star
            .logs
            .column_by_name("time_unix_nano")
            .expect("time_unix_nano column must exist");
        assert!(!time_col.is_null(0), "time_unix_nano must not be NULL");

        let key_col = star
            .log_attrs
            .column_by_name("key")
            .expect("key column must exist")
            .as_string::<i32>();
        let has_at_timestamp_attr = (0..key_col.len()).any(|i| key_col.value(i) == "@timestamp");
        assert!(
            !has_at_timestamp_attr,
            "@timestamp must map to time_unix_nano, not appear as a LOG_ATTR"
        );
    }
}

#[cfg(test)]
mod str_value_at_tests {
    use arrow::array::{
        Float32Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };

    use super::super::helpers::{parse_timestamp_to_nanos, str_value_at};

    #[test]
    fn int_types() {
        let i8_arr = Int8Array::from(vec![Some(-1i8)]);
        assert_eq!(str_value_at(&i8_arr, 0), "-1");
        let i16_arr = Int16Array::from(vec![Some(256i16)]);
        assert_eq!(str_value_at(&i16_arr, 0), "256");
        let i32_arr = Int32Array::from(vec![Some(100_000i32)]);
        assert_eq!(str_value_at(&i32_arr, 0), "100000");
        let i64_arr = Int64Array::from(vec![Some(42i64)]);
        assert_eq!(str_value_at(&i64_arr, 0), "42");
    }

    #[test]
    fn uint_types() {
        use arrow::array::{UInt8Array, UInt16Array, UInt32Array, UInt64Array};
        let u8_arr = UInt8Array::from(vec![Some(255u8)]);
        assert_eq!(str_value_at(&u8_arr, 0), "255");
        let u16_arr = UInt16Array::from(vec![Some(65535u16)]);
        assert_eq!(str_value_at(&u16_arr, 0), "65535");
        let u32_arr = UInt32Array::from(vec![Some(123456u32)]);
        assert_eq!(str_value_at(&u32_arr, 0), "123456");
        let u64_arr = UInt64Array::from(vec![Some(u64::MAX)]);
        assert_eq!(str_value_at(&u64_arr, 0), u64::MAX.to_string());
    }

    #[test]
    fn float_types() {
        let f32_arr = Float32Array::from(vec![Some(3.14f32)]);
        let val = str_value_at(&f32_arr, 0);
        assert!(val.starts_with("3.14"), "got: {val}");
        use arrow::array::Float64Array;
        let f64_arr = Float64Array::from(vec![Some(2.718f64)]);
        let val = str_value_at(&f64_arr, 0);
        assert!(val.starts_with("2.718"), "got: {val}");
    }

    #[test]
    fn large_utf8() {
        let arr = LargeStringArray::from(vec![Some("hello large")]);
        assert_eq!(str_value_at(&arr, 0), "hello large");
    }

    #[test]
    fn null_returns_empty() {
        let arr = Int32Array::from(vec![None::<i32>]);
        assert_eq!(str_value_at(&arr, 0), "");
    }

    #[test]
    fn timestamp_nanos_normalized() {
        let arr = TimestampNanosecondArray::from(vec![Some(1_000_000_000i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
        let nanos = parse_timestamp_to_nanos(&str_value_at(&arr, 0));
        assert_eq!(nanos, Some(1_000_000_000_000_000_000i64));
    }

    #[test]
    fn timestamp_seconds_normalized_to_nanos() {
        let arr = TimestampSecondArray::from(vec![Some(1i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
    }

    #[test]
    fn timestamp_millis_normalized_to_nanos() {
        let arr = TimestampMillisecondArray::from(vec![Some(1000i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
    }

    #[test]
    fn timestamp_micros_normalized_to_nanos() {
        let arr = TimestampMicrosecondArray::from(vec![Some(1_000_000i64)]);
        assert_eq!(str_value_at(&arr, 0), "1000000000");
    }

    #[test]
    fn fallback_exotic_type() {
        let arr = arrow::array::Date32Array::from(vec![Some(0i32)]);
        let val = str_value_at(&arr, 0);
        assert!(
            val.contains("1970-01-01"),
            "expected Date32 fallback to produce a date string, got: {val}"
        );
    }
}
