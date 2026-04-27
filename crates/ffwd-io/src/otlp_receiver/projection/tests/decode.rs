use std::sync::Arc;

use ffwd_types::field_names;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use proptest::prelude::*;
use prost::Message as _;

use super::*;
use crate::otlp_receiver::convert::convert_request_to_batch;

fn primitive_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    kv_string("service.name", "checkout"),
                    kv_bool("resource.sampled", true),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "scope-a".into(),
                    version: "1.2.3".into(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_712_509_200_123_456_789,
                    observed_time_unix_nano: 1_712_509_200_123_456_999,
                    severity_number: 9,
                    severity_text: "INFO".into(),
                    body: Some(any_string("hello")),
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    flags: 1,
                    attributes: vec![
                        kv_i64("status", 200),
                        kv_f64("duration_ms", 12.5),
                        kv_bool("success", true),
                        kv_bytes("payload", &[0xde, 0xad, 0xbe, 0xef]),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn generated_otlp_plan_declares_expected_planned_fields() {
    let (plan, handles) = generated::build_otlp_plan();

    assert_eq!(
        plan.lookup(field_names::TIMESTAMP)
            .expect("timestamp handle should exist"),
        handles.timestamp
    );
    assert_eq!(
        plan.lookup(field_names::OBSERVED_TIMESTAMP)
            .expect("observed timestamp handle should exist"),
        handles.observed_timestamp
    );
    assert_eq!(
        plan.lookup(field_names::SEVERITY)
            .expect("severity handle should exist"),
        handles.severity
    );
    assert_eq!(
        plan.lookup(field_names::SEVERITY_NUMBER)
            .expect("severity number handle should exist"),
        handles.severity_number
    );
    assert_eq!(
        plan.lookup(field_names::BODY)
            .expect("body handle should exist"),
        handles.body
    );
    assert_eq!(
        plan.lookup(field_names::TRACE_ID)
            .expect("trace id handle should exist"),
        handles.trace_id
    );
    assert_eq!(
        plan.lookup(field_names::SPAN_ID)
            .expect("span id handle should exist"),
        handles.span_id
    );
    assert_eq!(
        plan.lookup(field_names::FLAGS)
            .expect("flags handle should exist"),
        handles.flags
    );
    assert_eq!(
        plan.lookup(field_names::SCOPE_NAME)
            .expect("scope name handle should exist"),
        handles.scope_name
    );
    assert_eq!(
        plan.lookup(field_names::SCOPE_VERSION)
            .expect("scope version handle should exist"),
        handles.scope_version
    );
    assert_eq!(plan.num_planned(), 10);
}

#[test]
fn projected_primitive_request_matches_prost_conversion() {
    let request = primitive_request();
    assert_projected_matches_prost(&request);
}

#[test]
fn projected_bytes_api_keeps_supported_strings_attached_to_request_body() {
    let body = Bytes::from(primitive_request().encode_to_vec());
    let batch =
        crate::otlp_receiver::decode_protobuf_bytes_to_batch_projected_experimental(body.clone())
            .expect("experimental projection should decode primitive request");

    assert!(
        ffwd_arrow::materialize::is_attached(&batch, &body),
        "projected string values should be Arrow views backed by the OTLP body"
    );
    assert_eq!(
        batch
            .schema()
            .field_with_name(field_names::BODY)
            .expect("body field should exist")
            .data_type(),
        &arrow::datatypes::DataType::Utf8View
    );
}

#[test]
fn projected_duplicate_names_match_prost_conversion() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv_string("service.name", "checkout")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    body: Some(any_string("canonical-body")),
                    trace_id: vec![0xaa; 16],
                    flags: 1,
                    attributes: vec![
                        kv_string("body", "attr-body-shadow"),
                        kv_string("trace_id", "attr-trace-shadow"),
                        kv_string("flags", "attr-flags-shadow"),
                        kv_string("resource_shadow.service.name", "attr-resource-shadow"),
                    ],
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
fn projected_empty_key_attribute_matches_prost_conversion() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    attributes: vec![kv_string("", "empty-key")],
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
fn projected_repeated_scope_messages_merge_like_prost() {
    let first_scope = InstrumentationScope {
        name: "scope-a".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let second_scope = InstrumentationScope {
        version: "1.2.3".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let record = LogRecord {
        body: Some(any_string("hello")),
        ..Default::default()
    }
    .encode_to_vec();

    let mut scope_logs = Vec::new();
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &first_scope);
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &second_scope);
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &record);

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
fn projected_repeated_empty_scope_name_clears_prior_scope_like_prost() {
    let first_scope = InstrumentationScope {
        name: "scope-a".into(),
        version: "1.2.3".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let mut second_scope = Vec::new();
    encode_len_field(
        &mut second_scope,
        otlp_field::INSTRUMENTATION_SCOPE_NAME,
        b"",
    );
    encode_len_field(
        &mut second_scope,
        otlp_field::INSTRUMENTATION_SCOPE_VERSION,
        b"",
    );
    let record = LogRecord {
        body: Some(any_string("hello")),
        ..Default::default()
    }
    .encode_to_vec();

    let mut scope_logs = Vec::new();
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &first_scope);
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &second_scope);
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_LOG_RECORDS, &record);

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
fn projected_unknown_fields_match_prost_conversion() {
    let request = primitive_request();
    let mut payload = request.encode_to_vec();
    encode_len_field(&mut payload, 99, b"ignored-top-level-field");

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_unknown_fields_for_all_supported_wire_types_match_prost_conversion() {
    let request = primitive_request();
    let mut payload = request.encode_to_vec();
    encode_varint_field(&mut payload, 98, 123);
    encode_fixed64_field(&mut payload, 99, 0x0102_0304_0506_0708);
    encode_len_field(&mut payload, 100, b"ignored-top-level-field");
    encode_fixed32_field(&mut payload, 101, 0x0a0b_0c0d);

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_nested_unknown_field_interleavings_match_prost_conversion() {
    let mut any_value = Vec::new();
    encode_varint_field(&mut any_value, 98, 1);
    encode_len_field(&mut any_value, otlp_field::ANY_VALUE_STRING_VALUE, b"body");
    encode_fixed64_field(&mut any_value, 99, 0x0102_0304_0506_0708);

    let mut attr = Vec::new();
    encode_len_field(&mut attr, 77, b"ignored-kv-prefix");
    encode_len_field(&mut attr, otlp_field::KEY_VALUE_KEY, b"attr");
    encode_start_group(&mut attr, 78);
    encode_varint_field(&mut attr, 79, 123);
    encode_end_group(&mut attr, 78);
    encode_len_field(&mut attr, otlp_field::KEY_VALUE_VALUE, &any_value);

    let mut scope = Vec::new();
    encode_varint_field(&mut scope, 77, 7);
    encode_len_field(&mut scope, otlp_field::INSTRUMENTATION_SCOPE_NAME, b"scope");
    encode_start_group(&mut scope, 78);
    encode_len_field(&mut scope, 79, b"inside-scope-group");
    encode_end_group(&mut scope, 78);
    encode_len_field(
        &mut scope,
        otlp_field::INSTRUMENTATION_SCOPE_VERSION,
        b"1.0.0",
    );

    let mut log_record = Vec::new();
    encode_fixed32_field(&mut log_record, 77, 0x0a0b_0c0d);
    encode_fixed64_field(&mut log_record, otlp_field::LOG_RECORD_TIME_UNIX_NANO, 123);
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_value);
    encode_start_group(&mut log_record, 78);
    encode_varint_field(&mut log_record, 79, 456);
    encode_end_group(&mut log_record, 78);
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_ATTRIBUTES, &attr);

    let mut scope_logs = Vec::new();
    encode_len_field(&mut scope_logs, 77, b"ignored-scope-logs-prefix");
    encode_len_field(&mut scope_logs, otlp_field::SCOPE_LOGS_SCOPE, &scope);
    encode_len_field(
        &mut scope_logs,
        otlp_field::SCOPE_LOGS_LOG_RECORDS,
        &log_record,
    );
    encode_fixed32_field(&mut scope_logs, 78, 99);

    let mut resource = Vec::new();
    encode_varint_field(&mut resource, 77, 1);
    encode_len_field(&mut resource, otlp_field::RESOURCE_ATTRIBUTES, &attr);
    encode_start_group(&mut resource, 78);
    encode_end_group(&mut resource, 78);

    let mut resource_logs = Vec::new();
    encode_len_field(&mut resource_logs, 77, b"ignored-resource-logs-prefix");
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_RESOURCE,
        &resource,
    );
    encode_len_field(
        &mut resource_logs,
        otlp_field::RESOURCE_LOGS_SCOPE_LOGS,
        &scope_logs,
    );
    encode_fixed64_field(&mut resource_logs, 78, 0x1111_2222_3333_4444);

    let mut payload = Vec::new();
    encode_start_group(&mut payload, 99);
    encode_varint_field(&mut payload, 100, 1);
    encode_end_group(&mut payload, 99);
    encode_len_field(
        &mut payload,
        otlp_field::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
        &resource_logs,
    );
    encode_fixed32_field(&mut payload, 101, 0x0102_0304);

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_oversized_field_number_is_invalid() {
    let mut payload = Vec::new();
    encode_varint(&mut payload, (u64::from(u32::MAX) + 1) << 3);

    let err = decode_projected_otlp_logs(&payload, field_names::DEFAULT_RESOURCE_PREFIX)
        .expect_err("oversized protobuf field number should fail projection");

    assert!(
        matches!(err, ProjectionError::Invalid(_)),
        "expected invalid projection reason, got {err:?}"
    );
}

#[test]
fn projected_unknown_group_wire_type_matches_prost_conversion() {
    let mut payload = Vec::new();
    encode_start_group(&mut payload, 99);
    encode_varint_field(&mut payload, 99, 123);
    encode_end_group(&mut payload, 99);

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_nested_unknown_group_wire_type_matches_prost_conversion() {
    let mut payload = Vec::new();
    encode_start_group(&mut payload, 99);
    encode_start_group(&mut payload, 100);
    encode_varint_field(&mut payload, 101, 123);
    encode_end_group(&mut payload, 100);
    encode_end_group(&mut payload, 99);

    assert_projected_payload_matches_prost(&payload);
}

#[test]
fn projected_repeated_empty_log_record_fields_match_prost_conversion() {
    let mut log_record = Vec::new();
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_SEVERITY_TEXT,
        b"INFO",
    );
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SEVERITY_TEXT, b"");
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_BODY,
        &any_string("first").encode_to_vec(),
    );
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, b"");
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_TRACE_ID,
        &[0xaa; 16],
    );
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_TRACE_ID, b"");
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SPAN_ID, &[0xbb; 8]);
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_SPAN_ID, b"");

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
fn projected_repeated_empty_key_value_value_matches_prost_conversion() {
    let mut attr = Vec::new();
    encode_len_field(&mut attr, otlp_field::KEY_VALUE_KEY, b"attr");
    encode_len_field(
        &mut attr,
        otlp_field::KEY_VALUE_VALUE,
        &any_string("kept").encode_to_vec(),
    );
    encode_len_field(&mut attr, otlp_field::KEY_VALUE_VALUE, b"");

    let mut log_record = Vec::new();
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_ATTRIBUTES, &attr);

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
fn projected_anyvalue_unsupported_oneof_followed_by_primitive_matches_prost() {
    let mut any_value = Vec::new();
    encode_len_field(
        &mut any_value,
        otlp_field::ANY_VALUE_ARRAY_VALUE,
        &ArrayValue::default().encode_to_vec(),
    );
    encode_len_field(&mut any_value, otlp_field::ANY_VALUE_STRING_VALUE, b"kept");

    let mut log_record = Vec::new();
    encode_len_field(&mut log_record, otlp_field::LOG_RECORD_BODY, &any_value);

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
fn projected_invalid_log_record_dropped_attributes_wire_type_is_invalid() {
    let mut log_record = Vec::new();
    encode_len_field(
        &mut log_record,
        otlp_field::LOG_RECORD_DROPPED_ATTRIBUTES_COUNT,
        b"wrong-wire-type",
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
        .expect_err("invalid dropped_attributes_count wire type should fail projection");
    assert!(
        matches!(err, ProjectionError::Invalid(message) if message.contains("dropped_attributes_count")),
        "expected dropped_attributes_count validation error, got {err:?}"
    );
}

#[test]
fn projected_invalid_log_record_event_name_wire_type_is_invalid() {
    let mut log_record = Vec::new();
    encode_varint_field(&mut log_record, otlp_field::LOG_RECORD_EVENT_NAME, 1);

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
        .expect_err("invalid event_name wire type should fail projection");
    assert!(
        matches!(err, ProjectionError::Invalid(message) if message.contains("event_name")),
        "expected event_name validation error, got {err:?}"
    );
}
