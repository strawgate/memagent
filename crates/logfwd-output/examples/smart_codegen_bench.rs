use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_core::otlp::{
    Severity, decode_tag, decode_varint, encode_bytes_field, encode_tag, encode_varint,
    encode_varint_field, hex_decode, parse_severity, parse_timestamp_nanos, skip_field,
};
use logfwd_output::{
    ArrowPayloadType, BatchMetadata, BatchStatus, Compression, DecodedPayload, OtlpProtocol,
    OtlpSink, StatusCode, decode_batch_arrow_records, decode_batch_arrow_records_generated_fast,
    decode_batch_status, decode_batch_status_generated_fast, encode_batch_arrow_records,
    encode_batch_arrow_records_generated_fast,
};
use logfwd_types::diagnostics::ComponentStats;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

const SCOPE_NAME: &str = "logfwd";
const SCOPE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
struct GeneratedRowRefs<'a> {
    timestamp_col: Option<&'a dyn Array>,
    level_col: Option<&'a dyn Array>,
    message_col: Option<&'a dyn Array>,
    trace_id_col: Option<&'a dyn Array>,
    span_id_col: Option<&'a dyn Array>,
    attrs: Vec<(String, &'a dyn Array)>,
}

fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::from([
            (
                "service.name".to_string(),
                "smart-codegen-bench".to_string(),
            ),
            ("service.namespace".to_string(), "bench".to_string()),
        ]),
        observed_time_ns: 1_710_000_000_000_000_000,
    }
}

fn make_batch(rows: usize, wide: bool) -> RecordBatch {
    let mut fields = vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Float64, true),
        Field::new("success", DataType::Boolean, true),
    ];

    let timestamps: Vec<Option<String>> = (0..rows)
        .map(|i| Some(format!("2026-04-06T12:34:{:02}.123456789Z", i % 60)))
        .collect();
    let levels: Vec<Option<&str>> = (0..rows)
        .map(|i| match i % 4 {
            0 => Some("INFO"),
            1 => Some("WARN"),
            2 => Some("ERROR"),
            _ => Some("DEBUG"),
        })
        .collect();
    let messages: Vec<Option<String>> = (0..rows)
        .map(|i| Some(format!("request {i} completed with synthetic payload")))
        .collect();
    let trace_ids: Vec<Option<&str>> = (0..rows)
        .map(|i| {
            if i % 5 == 0 {
                None
            } else {
                Some("00112233445566778899aabbccddeeff")
            }
        })
        .collect();
    let span_ids: Vec<Option<&str>> = (0..rows)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some("0011223344556677")
            }
        })
        .collect();
    let hosts: Vec<Option<String>> = (0..rows).map(|i| Some(format!("host-{}", i % 8))).collect();
    let statuses: Vec<Option<i64>> = (0..rows).map(|i| Some(200 + (i % 5) as i64)).collect();
    let durations: Vec<Option<f64>> = (0..rows).map(|i| Some((i % 100) as f64 * 1.25)).collect();
    let successes: Vec<Option<bool>> = (0..rows).map(|i| Some(i % 11 != 0)).collect();

    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(timestamps)),
        Arc::new(StringArray::from(levels)),
        Arc::new(StringArray::from(messages)),
        Arc::new(StringArray::from(trace_ids)),
        Arc::new(StringArray::from(span_ids)),
        Arc::new(StringArray::from(hosts)),
        Arc::new(Int64Array::from(statuses)),
        Arc::new(Float64Array::from(durations)),
        Arc::new(BooleanArray::from(successes)),
    ];

    if wide {
        for idx in 0..12 {
            fields.push(Field::new(format!("attr_{idx}"), DataType::Utf8, true));
            let values: Vec<Option<String>> = (0..rows)
                .map(|i| Some(format!("v{idx}-{}", i % 17)))
                .collect();
            cols.push(Arc::new(StringArray::from(values)));
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("valid batch")
}

fn resolve_generated_columns(batch: &RecordBatch) -> GeneratedRowRefs<'_> {
    let schema = batch.schema();
    let mut timestamp_col = None;
    let mut level_col = None;
    let mut message_col = None;
    let mut trace_id_col = None;
    let mut span_id_col = None;
    let mut attrs = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name();
        let arr = batch.column(idx).as_ref();
        if timestamp_col.is_none() && (name == "timestamp" || name == "time") {
            timestamp_col = Some(arr);
            continue;
        }
        if level_col.is_none() && name == "level" {
            level_col = Some(arr);
            continue;
        }
        if message_col.is_none() && name == "message" {
            message_col = Some(arr);
            continue;
        }
        if trace_id_col.is_none() && name == "trace_id" {
            trace_id_col = Some(arr);
            continue;
        }
        if span_id_col.is_none() && name == "span_id" {
            span_id_col = Some(arr);
            continue;
        }
        attrs.push((name.clone(), arr));
    }

    GeneratedRowRefs {
        timestamp_col,
        level_col,
        message_col,
        trace_id_col,
        span_id_col,
        attrs,
    }
}

fn arr_string_at(arr: &dyn Array, row: usize) -> Option<&str> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    None
}

fn arr_i64_at(arr: &dyn Array, row: usize) -> Option<i64> {
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .and_then(|a| (!a.is_null(row)).then(|| a.value(row)))
}

fn arr_f64_at(arr: &dyn Array, row: usize) -> Option<f64> {
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .and_then(|a| (!a.is_null(row)).then(|| a.value(row)))
}

fn arr_bool_at(arr: &dyn Array, row: usize) -> Option<bool> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .and_then(|a| (!a.is_null(row)).then(|| a.value(row)))
}

fn arr_u64_at(arr: &dyn Array, row: usize) -> Option<u64> {
    if let Some(a) = arr_string_at(arr, row) {
        return parse_timestamp_nanos(a.as_bytes());
    }
    arr_i64_at(arr, row).map(|v| v as u64)
}

fn otlp_any_value_from_array(arr: &dyn Array, row: usize) -> Option<AnyValue> {
    if let Some(s) = arr_string_at(arr, row) {
        return Some(AnyValue {
            value: Some(Value::StringValue(s.to_string())),
        });
    }
    if let Some(v) = arr_i64_at(arr, row) {
        return Some(AnyValue {
            value: Some(Value::IntValue(v)),
        });
    }
    if let Some(v) = arr_f64_at(arr, row) {
        return Some(AnyValue {
            value: Some(Value::DoubleValue(v)),
        });
    }
    if let Some(v) = arr_bool_at(arr, row) {
        return Some(AnyValue {
            value: Some(Value::BoolValue(v)),
        });
    }
    None
}

fn populate_generated_record(
    record: &mut LogRecord,
    columns: &GeneratedRowRefs<'_>,
    metadata: &BatchMetadata,
    row: usize,
) {
    record.time_unix_nano = 0;
    record.observed_time_unix_nano = metadata.observed_time_ns;
    record.severity_number = Severity::Unspecified as i32;
    record.severity_text.clear();
    record.body = None;
    record.trace_id.clear();
    record.span_id.clear();
    record.attributes.clear();

    if let Some(arr) = columns.timestamp_col
        && let Some(ts) = arr_u64_at(arr, row)
    {
        record.time_unix_nano = ts;
    }
    if let Some(arr) = columns.level_col
        && let Some(level) = arr_string_at(arr, row)
    {
        let (sev, sev_text) = parse_severity(level.as_bytes());
        record.severity_number = sev as i32;
        if (sev as i32) != (Severity::Unspecified as i32) {
            record
                .severity_text
                .push_str(&String::from_utf8_lossy(sev_text));
        }
    }
    if let Some(arr) = columns.message_col
        && let Some(message) = arr_string_at(arr, row)
    {
        record.body = Some(AnyValue {
            value: Some(Value::StringValue(message.to_string())),
        });
    }
    if let Some(arr) = columns.trace_id_col
        && let Some(trace_hex) = arr_string_at(arr, row)
    {
        let mut trace = [0u8; 16];
        if trace_hex.len() == 32 && hex_decode(trace_hex.as_bytes(), &mut trace) {
            record.trace_id.extend_from_slice(&trace);
        }
    }
    if let Some(arr) = columns.span_id_col
        && let Some(span_hex) = arr_string_at(arr, row)
    {
        let mut span = [0u8; 8];
        if span_hex.len() == 16 && hex_decode(span_hex.as_bytes(), &mut span) {
            record.span_id.extend_from_slice(&span);
        }
    }
    for (name, arr) in &columns.attrs {
        if let Some(any) = otlp_any_value_from_array(*arr, row) {
            record.attributes.push(KeyValue {
                key: name.clone(),
                value: Some(any),
            });
        }
    }
}

fn populate_resource_attributes(resource: &mut Resource, metadata: &BatchMetadata) {
    resource.attributes.clear();
    resource.attributes.reserve(metadata.resource_attrs.len());
    for (k, v) in metadata.resource_attrs.iter() {
        resource.attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.clone())),
            }),
        });
    }
}

fn build_generated_request_naive(
    batch: &RecordBatch,
    metadata: &BatchMetadata,
) -> ExportLogsServiceRequest {
    let columns = resolve_generated_columns(batch);
    let mut log_records = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut record = LogRecord::default();
        populate_generated_record(&mut record, &columns, metadata, row);
        log_records.push(record);
    }

    let mut resource = Resource::default();
    populate_resource_attributes(&mut resource, metadata);

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: SCOPE_NAME.to_string(),
                    version: SCOPE_VERSION.to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn encode_generated_request_reuse(
    batch: &RecordBatch,
    metadata: &BatchMetadata,
    request: &mut ExportLogsServiceRequest,
    encoded: &mut Vec<u8>,
) {
    let columns = resolve_generated_columns(batch);

    if request.resource_logs.is_empty() {
        request.resource_logs.push(ResourceLogs::default());
    }
    let resource_logs = &mut request.resource_logs[0];

    if resource_logs.scope_logs.is_empty() {
        resource_logs.scope_logs.push(ScopeLogs::default());
    }
    if resource_logs.resource.is_none() {
        resource_logs.resource = Some(Resource::default());
    }

    if let Some(resource) = resource_logs.resource.as_mut() {
        populate_resource_attributes(resource, metadata);
    }

    let scope_logs = &mut resource_logs.scope_logs[0];
    if scope_logs.scope.is_none() {
        scope_logs.scope = Some(InstrumentationScope::default());
    }
    if let Some(scope) = scope_logs.scope.as_mut() {
        scope.name.clear();
        scope.name.push_str(SCOPE_NAME);
        scope.version.clear();
        scope.version.push_str(SCOPE_VERSION);
    }

    scope_logs.log_records.clear();
    scope_logs.log_records.reserve(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut record = LogRecord::default();
        populate_generated_record(&mut record, &columns, metadata, row);
        scope_logs.log_records.push(record);
    }

    encoded.clear();
    encoded.reserve(request.encoded_len());
    request.encode(encoded).expect("encode generated request");
}

fn prepare_generated_request_recycle(
    batch: &RecordBatch,
    metadata: &BatchMetadata,
    request: &mut ExportLogsServiceRequest,
) {
    let columns = resolve_generated_columns(batch);

    if request.resource_logs.is_empty() {
        request.resource_logs.push(ResourceLogs::default());
    }
    let resource_logs = &mut request.resource_logs[0];

    if resource_logs.scope_logs.is_empty() {
        resource_logs.scope_logs.push(ScopeLogs::default());
    }
    if resource_logs.resource.is_none() {
        resource_logs.resource = Some(Resource::default());
    }
    if let Some(resource) = resource_logs.resource.as_mut() {
        populate_resource_attributes(resource, metadata);
    }

    let scope_logs = &mut resource_logs.scope_logs[0];
    if scope_logs.scope.is_none() {
        scope_logs.scope = Some(InstrumentationScope::default());
    }
    if let Some(scope) = scope_logs.scope.as_mut() {
        scope.name.clear();
        scope.name.push_str(SCOPE_NAME);
        scope.version.clear();
        scope.version.push_str(SCOPE_VERSION);
    }

    if scope_logs.log_records.len() < batch.num_rows() {
        scope_logs
            .log_records
            .resize_with(batch.num_rows(), LogRecord::default);
    } else {
        scope_logs.log_records.truncate(batch.num_rows());
    }

    for (row, record) in scope_logs.log_records.iter_mut().enumerate() {
        populate_generated_record(record, &columns, metadata, row);
    }
}

fn manual_encode_arrow_payload(
    buf: &mut Vec<u8>,
    schema_id: &str,
    payload_type: ArrowPayloadType,
    record: &[u8],
) {
    if !schema_id.is_empty() {
        encode_bytes_field(buf, 1, schema_id.as_bytes());
    }
    let type_val = payload_type as u64;
    if type_val != 0 {
        encode_varint_field(buf, 2, type_val);
    }
    if !record.is_empty() {
        encode_bytes_field(buf, 3, record);
    }
}

fn manual_encode_batch_arrow_records(
    buf: &mut Vec<u8>,
    batch_id: i64,
    payloads: &[(String, ArrowPayloadType, Vec<u8>)],
    headers: &[u8],
) {
    if batch_id != 0 {
        encode_varint_field(buf, 1, batch_id as u64);
    }
    for (schema_id, payload_type, record) in payloads {
        let mut sub = Vec::with_capacity(record.len() + 64);
        manual_encode_arrow_payload(&mut sub, schema_id, *payload_type, record);
        encode_tag(buf, 2, 2);
        encode_varint(buf, sub.len() as u64);
        buf.extend_from_slice(&sub);
    }
    if !headers.is_empty() {
        encode_bytes_field(buf, 3, headers);
    }
}

fn manual_decode_arrow_payload(data: &[u8]) -> std::io::Result<DecodedPayload> {
    let mut schema_id = String::new();
    let mut payload_type: u32 = 0;
    let mut record = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (field_number, wire_type, new_pos) =
            decode_tag(data, pos).map_err(std::io::Error::other)?;
        pos = new_pos;
        match (field_number, wire_type) {
            (1, 2) => {
                let (len, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                let end = new_pos + len as usize;
                schema_id = String::from_utf8_lossy(&data[new_pos..end]).into_owned();
                pos = end;
            }
            (2, 0) => {
                let (val, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                payload_type = val as u32;
                pos = new_pos;
            }
            (3, 2) => {
                let (len, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                let end = new_pos + len as usize;
                record = data[new_pos..end].to_vec();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos).map_err(std::io::Error::other)?;
            }
        }
    }

    Ok((schema_id, payload_type, record))
}

fn manual_decode_batch_arrow_records(
    data: &[u8],
) -> std::io::Result<(i64, Vec<DecodedPayload>, Vec<u8>)> {
    let mut batch_id = 0i64;
    let mut payloads = Vec::new();
    let mut headers = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (field_number, wire_type, new_pos) =
            decode_tag(data, pos).map_err(std::io::Error::other)?;
        pos = new_pos;
        match (field_number, wire_type) {
            (1, 0) => {
                let (val, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                batch_id = val as i64;
                pos = new_pos;
            }
            (2, 2) => {
                let (len, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                let end = new_pos + len as usize;
                payloads.push(manual_decode_arrow_payload(&data[new_pos..end])?);
                pos = end;
            }
            (3, 2) => {
                let (len, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                let end = new_pos + len as usize;
                headers = data[new_pos..end].to_vec();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos).map_err(std::io::Error::other)?;
            }
        }
    }

    Ok((batch_id, payloads, headers))
}

fn manual_decode_batch_status(data: &[u8]) -> std::io::Result<BatchStatus> {
    if data.is_empty() {
        return Ok(BatchStatus {
            batch_id: 0,
            status_code: StatusCode::Ok,
            status_message: String::new(),
        });
    }
    let mut batch_id = 0i64;
    let mut status_code = StatusCode::Ok;
    let mut status_message = String::new();
    let mut pos = 0;

    while pos < data.len() {
        let (field_number, wire_type, new_pos) =
            decode_tag(data, pos).map_err(std::io::Error::other)?;
        pos = new_pos;
        match (field_number, wire_type) {
            (1, 0) => {
                let (val, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                batch_id = val as i64;
                pos = new_pos;
            }
            (2, 0) => {
                let (val, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                status_code = match val {
                    0 => StatusCode::Ok,
                    1 => StatusCode::Unavailable,
                    2 => StatusCode::InvalidArgument,
                    _ => StatusCode::InvalidArgument,
                };
                pos = new_pos;
            }
            (3, 2) => {
                let (len, new_pos) = decode_varint(data, pos).map_err(std::io::Error::other)?;
                let end = new_pos + len as usize;
                status_message = String::from_utf8_lossy(&data[new_pos..end]).into_owned();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos).map_err(std::io::Error::other)?;
            }
        }
    }

    Ok(BatchStatus {
        batch_id,
        status_code,
        status_message,
    })
}

fn run<F: FnMut()>(label: &str, iterations: usize, bytes_per_iter: usize, mut f: F) {
    for _ in 0..3 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(f());
    }
    let elapsed = start.elapsed();
    let ns_per_iter = elapsed.as_secs_f64() * 1e9 / iterations as f64;
    let mib_per_sec =
        (bytes_per_iter as f64 * iterations as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "{label:36}  {:10.1} ns/iter  {:10.2} MiB/s",
        ns_per_iter, mib_per_sec
    );
}

fn main() {
    let metadata = make_metadata();
    let narrow = make_batch(1_000, false);
    let wide = make_batch(10_000, true);

    println!("OTLP encode");
    for (name, batch, iterations) in [("narrow-1k", &narrow, 200usize), ("wide-10k", &wide, 20)] {
        let mut sink = OtlpSink::new(
            "bench".into(),
            "http://localhost:1".into(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        )
        .expect("valid sink");
        sink.encode_batch(batch, &metadata);
        let manual_bytes = sink.encoded_payload().len();
        run(
            &format!("otlp manual {name}"),
            iterations,
            manual_bytes,
            || {
                sink.encode_batch(batch, &metadata);
                black_box(sink.encoded_payload());
            },
        );

        sink.encode_batch_generated_fast(batch, &metadata);
        let generated_fast_bytes = sink.encoded_payload().len();
        run(
            &format!("otlp generated fast {name}"),
            iterations,
            generated_fast_bytes,
            || {
                sink.encode_batch_generated_fast(batch, &metadata);
                black_box(sink.encoded_payload());
            },
        );

        sink.encode_rows_only_for_bench(batch, &metadata);
        let manual_rows_bytes = sink.encoded_payload().len();
        run(
            &format!("otlp rows manual {name}"),
            iterations,
            manual_rows_bytes,
            || {
                sink.encode_rows_only_for_bench(batch, &metadata);
                black_box(sink.encoded_payload());
            },
        );

        sink.encode_rows_only_generated_fast_for_bench(batch, &metadata);
        let generated_fast_rows_bytes = sink.encoded_payload().len();
        run(
            &format!("otlp rows generated fast {name}"),
            iterations,
            generated_fast_rows_bytes,
            || {
                sink.encode_rows_only_generated_fast_for_bench(batch, &metadata);
                black_box(sink.encoded_payload());
            },
        );

        let naive = build_generated_request_naive(batch, &metadata);
        let naive_bytes = naive.encoded_len();
        run(
            &format!("otlp generated build naive {name}"),
            iterations,
            naive_bytes,
            || {
                let request = build_generated_request_naive(batch, &metadata);
                black_box(request);
            },
        );
        let mut naive_encoded = Vec::with_capacity(naive_bytes);
        run(
            &format!("otlp generated encode only naive {name}"),
            iterations,
            naive_bytes,
            || {
                naive_encoded.clear();
                naive_encoded.reserve(naive.encoded_len());
                naive.encode(&mut naive_encoded).expect("encode generated");
                black_box(&naive_encoded);
            },
        );
        run(
            &format!("otlp generated naive {name}"),
            iterations,
            naive_bytes,
            || {
                let request = build_generated_request_naive(batch, &metadata);
                let mut encoded = Vec::with_capacity(request.encoded_len());
                request.encode(&mut encoded).expect("encode generated");
                black_box(encoded);
            },
        );

        let mut request = ExportLogsServiceRequest::default();
        let mut encoded = Vec::new();
        encode_generated_request_reuse(batch, &metadata, &mut request, &mut encoded);
        let reuse_bytes = encoded.len();
        run(
            &format!("otlp generated build reuse {name}"),
            iterations,
            reuse_bytes,
            || {
                prepare_generated_request_recycle(batch, &metadata, &mut request);
                black_box(&request);
            },
        );
        run(
            &format!("otlp generated encode only reuse {name}"),
            iterations,
            reuse_bytes,
            || {
                encoded.clear();
                encoded.reserve(request.encoded_len());
                request
                    .encode(&mut encoded)
                    .expect("encode generated request");
                black_box(&encoded);
            },
        );
        run(
            &format!("otlp generated reuse {name}"),
            iterations,
            reuse_bytes,
            || {
                encode_generated_request_reuse(batch, &metadata, &mut request, &mut encoded);
                black_box(&encoded);
            },
        );

        let mut recycled_request = ExportLogsServiceRequest::default();
        let mut recycled_encoded = Vec::new();
        prepare_generated_request_recycle(batch, &metadata, &mut recycled_request);
        recycled_encoded.reserve(recycled_request.encoded_len());
        recycled_request
            .encode(&mut recycled_encoded)
            .expect("encode recycled request");
        let recycled_bytes = recycled_encoded.len();
        run(
            &format!("otlp generated build recycled {name}"),
            iterations,
            recycled_bytes,
            || {
                prepare_generated_request_recycle(batch, &metadata, &mut recycled_request);
                black_box(&recycled_request);
            },
        );
        run(
            &format!("otlp generated encode only recycled {name}"),
            iterations,
            recycled_bytes,
            || {
                recycled_encoded.clear();
                recycled_encoded.reserve(recycled_request.encoded_len());
                recycled_request
                    .encode(&mut recycled_encoded)
                    .expect("encode recycled request");
                black_box(&recycled_encoded);
            },
        );
        run(
            &format!("otlp generated recycled {name}"),
            iterations,
            recycled_bytes,
            || {
                prepare_generated_request_recycle(batch, &metadata, &mut recycled_request);
                recycled_encoded.clear();
                recycled_encoded.reserve(recycled_request.encoded_len());
                recycled_request
                    .encode(&mut recycled_encoded)
                    .expect("encode recycled request");
                black_box(&recycled_encoded);
            },
        );
    }

    println!("\nOTAP boundary");
    for (name, payload_size, iterations) in [
        ("small", 512usize, 20_000usize),
        ("large", 64 * 1024usize, 1_000usize),
    ] {
        let payloads = vec![
            (
                "logs".to_string(),
                ArrowPayloadType::Logs,
                vec![0x11; payload_size],
            ),
            (
                "log_attrs".to_string(),
                ArrowPayloadType::LogAttrs,
                vec![0x22; payload_size],
            ),
            (
                "resource_attrs".to_string(),
                ArrowPayloadType::ResourceAttrs,
                vec![0x33; payload_size],
            ),
            (
                "scope_attrs".to_string(),
                ArrowPayloadType::ScopeAttrs,
                vec![0x44; payload_size],
            ),
        ];
        let headers = b"authorization: bearer bench-token";
        let mut encoded = Vec::new();
        encode_batch_arrow_records(&mut encoded, 42, &payloads, headers);
        let bytes = encoded.len();

        run(
            &format!("otap encode manual {name}"),
            iterations,
            bytes,
            || {
                let mut buf = Vec::with_capacity(bytes);
                manual_encode_batch_arrow_records(&mut buf, 42, &payloads, headers);
                black_box(buf);
            },
        );
        run(
            &format!("otap encode generated {name}"),
            iterations,
            bytes,
            || {
                let mut buf = Vec::with_capacity(bytes);
                encode_batch_arrow_records(&mut buf, 42, &payloads, headers);
                black_box(buf);
            },
        );
        run(
            &format!("otap encode generated fast {name}"),
            iterations,
            bytes,
            || {
                let mut buf = Vec::with_capacity(bytes);
                encode_batch_arrow_records_generated_fast(&mut buf, 42, &payloads, headers);
                black_box(buf);
            },
        );
        run(
            &format!("otap decode manual {name}"),
            iterations,
            bytes,
            || {
                let decoded = manual_decode_batch_arrow_records(&encoded).expect("manual decode");
                black_box(decoded);
            },
        );
        run(
            &format!("otap decode generated {name}"),
            iterations,
            bytes,
            || {
                let decoded = decode_batch_arrow_records(&encoded).expect("generated decode");
                black_box(decoded);
            },
        );
        run(
            &format!("otap decode generated fast {name}"),
            iterations,
            bytes,
            || {
                let decoded = decode_batch_arrow_records_generated_fast(&encoded)
                    .expect("generated fast decode");
                black_box(decoded);
            },
        );
    }

    let mut status_buf = Vec::new();
    encode_varint_field(&mut status_buf, 1, 42);
    encode_varint_field(&mut status_buf, 2, StatusCode::Unavailable as u64);
    encode_bytes_field(&mut status_buf, 3, b"bench status");
    let status_bytes = status_buf.len();
    run("otap status decode manual", 100_000, status_bytes, || {
        let status = manual_decode_batch_status(&status_buf).expect("manual status");
        black_box(status);
    });
    run(
        "otap status decode generated",
        100_000,
        status_bytes,
        || {
            let status = decode_batch_status(&status_buf).expect("generated status");
            black_box(status);
        },
    );
    run(
        "otap status decode generated fast",
        100_000,
        status_bytes,
        || {
            let status =
                decode_batch_status_generated_fast(&status_buf).expect("generated fast status");
            black_box(status);
        },
    );
}
