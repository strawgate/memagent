use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_types::field_names;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use reqwest::header::{HeaderMap, HeaderValue};

use super::*;

fn make_sink() -> OtlpSink {
    OtlpSink::new(
        "test".into(),
        "http://localhost:4318".into(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .expect("sink")
}

fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: 1_000_000_000,
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn grpc_status_headers_are_classified() {
    let mut retryable = HeaderMap::new();
    retryable.insert("grpc-status", HeaderValue::from_static("14"));
    retryable.insert("grpc-message", HeaderValue::from_static("unavailable"));
    assert!(
        matches!(
            classify_grpc_status_headers(&retryable),
            Some(crate::sink::SendResult::IoError(_))
        ),
        "grpc-status 14 should map to retryable IoError",
    );

    let mut success = HeaderMap::new();
    success.insert("grpc-status", HeaderValue::from_static("0"));
    assert!(classify_grpc_status_headers(&success).is_none());
    assert!(classify_grpc_status_headers(&HeaderMap::new()).is_none());
}

fn assert_flags_column_encodes_as_field_8(
    data_type: DataType,
    arr: Arc<dyn Array>,
    type_name: &str,
) {
    let schema = Arc::new(Schema::new(vec![Field::new("flags", data_type, true)]));
    let batch = RecordBatch::try_new(schema, vec![arr]).expect("flags batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let expected = [0x45u8, 0x01, 0x00, 0x00, 0x00];
    assert!(
        contains_bytes(&sink.encoder_buf, &expected),
        "flags field 8 not found in encoded output for {type_name}"
    );
}

#[test]
fn encode_unsigned_flags_as_field_8() {
    assert_flags_column_encodes_as_field_8(
        DataType::UInt32,
        Arc::new(UInt32Array::from(vec![1u32])),
        "UInt32",
    );
    assert_flags_column_encodes_as_field_8(
        DataType::UInt64,
        Arc::new(UInt64Array::from(vec![1u64])),
        "UInt64",
    );
}

#[test]
fn encode_flags_uint64_out_of_range_silently_dropped() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "flags",
        DataType::UInt64,
        true,
    )]));
    let arr = UInt64Array::from(vec![u64::from(u32::MAX) + 1]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("flags batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    // field 8, wire type 5 tag = 0x45 — must NOT appear when value > u32::MAX
    assert!(
        !contains_bytes(&sink.encoder_buf, &[0x45u8]),
        "flags field 8 must not be encoded for out-of-range UInt64 value"
    );
}

#[test]
fn internal_columns_are_not_encoded_as_log_attributes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("message", DataType::Utf8, true),
        Field::new(field_names::SOURCE_ID, DataType::UInt64, true),
        Field::new("__typename", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("hello")])),
            Arc::new(UInt64Array::from(vec![Some(42)])),
            Arc::new(StringArray::from(vec![Some("LogEvent")])),
        ],
    )
    .expect("valid batch");

    let mut sink = make_sink();
    sink.encode_batch(&batch, &make_metadata());

    let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
        .expect("prost decodes output");
    let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
    assert!(
        lr.attributes
            .iter()
            .all(|kv| kv.key != field_names::SOURCE_ID),
        "internal source id must stay out of OTLP attributes"
    );
    assert!(
        lr.attributes.iter().any(|kv| kv.key == "__typename"),
        "user double-underscore fields must remain OTLP attributes"
    );
}
