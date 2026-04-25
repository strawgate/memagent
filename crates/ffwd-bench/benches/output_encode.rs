//! Output format encoding benchmarks.
//!
//! Compares the encoding cost of different output formats given the same
//! `RecordBatch`:
//!
//! - **OTLP protobuf (manual hot path)** — `OtlpSink::encode_batch`
//! - **OTLP protobuf (generated/prost naive)** — per-row generated message build + `encode`
//! - **OTLP protobuf (generated/prost reusable)** — reusable generated structures + `encode`
//! - **OTLP protobuf + zstd** — encode then compress
//! - **JSON lines** — `build_col_infos` + `write_row_json`
//! - **NullSink** — baseline (measures iteration overhead only)
//!
//! Varies: narrow (5 fields) vs wide (20+ fields), 1K/10K rows.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffwd_bench::{NullSink, generators, make_otlp_sink};
use ffwd_core::otlp::{Severity, hex_decode, parse_severity, parse_timestamp_nanos};
use ffwd_io::compress::ChunkCompressor;
use ffwd_output::{BatchMetadata, Compression};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

const SCOPE_NAME: &str = "ffwd";
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
        attrs.push((name.to_string(), arr));
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
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    if let Some(a) = arr
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    None
}

fn arr_i64_at(arr: &dyn Array, row: usize) -> Option<i64> {
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    None
}

fn arr_f64_at(arr: &dyn Array, row: usize) -> Option<f64> {
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    None
}

fn arr_bool_at(arr: &dyn Array, row: usize) -> Option<bool> {
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    None
}

fn arr_u64_at(arr: &dyn Array, row: usize) -> Option<u64> {
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return (!a.is_null(row)).then(|| a.value(row) as u64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
        return (!a.is_null(row)).then(|| parse_timestamp_nanos(a.value(row).as_bytes()))?;
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        return (!a.is_null(row)).then(|| parse_timestamp_nanos(a.value(row).as_bytes()))?;
    }
    if let Some(a) = arr
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return (!a.is_null(row)).then(|| parse_timestamp_nanos(a.value(row).as_bytes()))?;
    }
    None
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

fn build_generated_request_naive(
    batch: &RecordBatch,
    metadata: &BatchMetadata,
) -> ExportLogsServiceRequest {
    let columns = resolve_generated_columns(batch);
    let mut log_records = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut record = LogRecord {
            observed_time_unix_nano: metadata.observed_time_ns,
            ..Default::default()
        };

        if let Some(arr) = columns.timestamp_col {
            if let Some(ts) = arr_u64_at(arr, row) {
                record.time_unix_nano = ts;
            }
        }
        if let Some(arr) = columns.level_col {
            if let Some(level) = arr_string_at(arr, row) {
                let (sev, sev_text) = parse_severity(level.as_bytes());
                record.severity_number = sev as i32;
                if (sev as i32) != (Severity::Unspecified as i32) {
                    record.severity_text = String::from_utf8_lossy(sev_text).into_owned();
                }
            }
        }
        if let Some(arr) = columns.message_col {
            if let Some(message) = arr_string_at(arr, row) {
                record.body = Some(AnyValue {
                    value: Some(Value::StringValue(message.to_string())),
                });
            }
        }
        if let Some(arr) = columns.trace_id_col {
            if let Some(trace_hex) = arr_string_at(arr, row) {
                let mut trace = [0u8; 16];
                if trace_hex.len() == 32 && hex_decode(trace_hex.as_bytes(), &mut trace) {
                    record.trace_id = trace.to_vec();
                }
            }
        }
        if let Some(arr) = columns.span_id_col {
            if let Some(span_hex) = arr_string_at(arr, row) {
                let mut span = [0u8; 8];
                if span_hex.len() == 16 && hex_decode(span_hex.as_bytes(), &mut span) {
                    record.span_id = span.to_vec();
                }
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
        log_records.push(record);
    }

    let mut resource_attributes = Vec::with_capacity(metadata.resource_attrs.len());
    for (k, v) in metadata.resource_attrs.iter() {
        resource_attributes.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.clone())),
            }),
        });
    }

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes,
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
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
        let mut record = LogRecord {
            observed_time_unix_nano: metadata.observed_time_ns,
            ..Default::default()
        };

        if let Some(arr) = columns.timestamp_col {
            if let Some(ts) = arr_u64_at(arr, row) {
                record.time_unix_nano = ts;
            }
        }
        if let Some(arr) = columns.level_col {
            if let Some(level) = arr_string_at(arr, row) {
                let (sev, sev_text) = parse_severity(level.as_bytes());
                record.severity_number = sev as i32;
                if (sev as i32) != (Severity::Unspecified as i32) {
                    record.severity_text = String::from_utf8_lossy(sev_text).into_owned();
                }
            }
        }
        if let Some(arr) = columns.message_col {
            if let Some(message) = arr_string_at(arr, row) {
                record.body = Some(AnyValue {
                    value: Some(Value::StringValue(message.to_string())),
                });
            }
        }
        if let Some(arr) = columns.trace_id_col {
            if let Some(trace_hex) = arr_string_at(arr, row) {
                let mut trace = [0u8; 16];
                if trace_hex.len() == 32 && hex_decode(trace_hex.as_bytes(), &mut trace) {
                    record.trace_id = trace.to_vec();
                }
            }
        }
        if let Some(arr) = columns.span_id_col {
            if let Some(span_hex) = arr_string_at(arr, row) {
                let mut span = [0u8; 8];
                if span_hex.len() == 16 && hex_decode(span_hex.as_bytes(), &mut span) {
                    record.span_id = span.to_vec();
                }
            }
        }

        record.attributes.reserve(columns.attrs.len());
        for (name, arr) in &columns.attrs {
            if let Some(any) = otlp_any_value_from_array(*arr, row) {
                record.attributes.push(KeyValue {
                    key: name.clone(),
                    value: Some(any),
                });
            }
        }

        scope_logs.log_records.push(record);
    }

    encoded.clear();
    encoded.reserve(request.encoded_len());
    request
        .encode(encoded)
        .expect("generated/prost encoding should not fail");
}

fn bench_output_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("output_encode");
    group.sample_size(20);

    let meta = generators::make_metadata();

    #[allow(clippy::type_complexity)]
    let variants: Vec<(&str, Vec<(usize, RecordBatch)>)> = vec![
        (
            "narrow",
            vec![
                (1_000, generators::gen_narrow_batch(1_000, 42)),
                (10_000, generators::gen_narrow_batch(10_000, 42)),
            ],
        ),
        (
            "wide",
            vec![
                (1_000, generators::gen_wide_batch(1_000, 42)),
                (10_000, generators::gen_wide_batch(10_000, 42)),
            ],
        ),
        (
            // Same schema as `wide` but ~25% of optional columns are null.
            // Exercises the `has_nulls = true` path in `encode_col_attr` and
            // provides a stable baseline for nullable-data encoder performance.
            "wide_sparse",
            vec![
                (1_000, generators::gen_wide_sparse_batch(1_000, 42)),
                (10_000, generators::gen_wide_sparse_batch(10_000, 42)),
            ],
        ),
    ];

    for (schema_name, sizes) in &variants {
        for (n, batch) in sizes {
            let label = format!("{schema_name}/{n}");

            group.throughput(Throughput::Elements(*n as u64));

            group.bench_with_input(BenchmarkId::new("null_sink", &label), batch, |b, batch| {
                let mut sink = NullSink;
                b.iter(|| sink.send_batch(batch, &meta).unwrap());
            });

            group.bench_with_input(
                BenchmarkId::new("otlp_encode_manual", &label),
                batch,
                |b, batch| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        std::hint::black_box(sink.encode_batch(batch, &meta));
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("otlp_encode_generated_naive", &label),
                batch,
                |b, batch| {
                    b.iter(|| {
                        let request = build_generated_request_naive(batch, &meta);
                        let mut encoded = Vec::with_capacity(request.encoded_len());
                        request
                            .encode(&mut encoded)
                            .expect("generated/prost encoding should not fail");
                        std::hint::black_box(encoded);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("otlp_encode_generated_reuse", &label),
                batch,
                |b, batch| {
                    let mut request = ExportLogsServiceRequest::default();
                    let mut encoded = Vec::with_capacity(batch.num_rows() * 128);
                    b.iter(|| {
                        encode_generated_request_reuse(batch, &meta, &mut request, &mut encoded);
                        std::hint::black_box(&encoded);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("otlp_encode_generated_fast", &label),
                batch,
                |b, batch| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        std::hint::black_box(sink.encode_batch_generated_fast(batch, &meta));
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("json_lines_zstd", &label),
                batch,
                |b, batch| {
                    let cols = ffwd_output::build_col_infos(batch);
                    let mut json_buf = Vec::with_capacity(*n * 300);
                    let mut compressor =
                        ChunkCompressor::new(1).expect("zstd level 1 is always valid");
                    b.iter(|| {
                        json_buf.clear();
                        for row in 0..batch.num_rows() {
                            ffwd_output::write_row_json(batch, row, &cols, &mut json_buf, false)
                                .expect("JSON serialization should not fail");
                            json_buf.push(b'\n');
                        }
                        let compressed = compressor
                            .compress(&json_buf)
                            .expect("compression should not fail");
                        std::hint::black_box(&compressed);
                    });
                },
            );

            group.bench_with_input(BenchmarkId::new("json_lines", &label), batch, |b, batch| {
                let cols = ffwd_output::build_col_infos(batch);
                let mut buf = Vec::with_capacity(*n * 300);
                b.iter(|| {
                    buf.clear();
                    for row in 0..batch.num_rows() {
                        ffwd_output::write_row_json(batch, row, &cols, &mut buf, false)
                            .expect("JSON serialization should not fail");
                        buf.push(b'\n');
                    }
                    std::hint::black_box(&buf);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_output_encode);
criterion_main!(benches);
