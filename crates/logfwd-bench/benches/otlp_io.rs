//! Decision-grade OTLP I/O benchmarks.
//!
//! Fixture synthesis, protobuf encoding, and parity checks happen once in setup
//! and are excluded from timing. Each benchmark only measures the labeled
//! stage:
//! - parser-only protobuf decode (`prost`)
//! - receiver decode + Arrow materialization
//! - sink encode-only (handwritten vs generated-fast)
//! - encoded-payload compression
//! - in-process decode -> encode.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_io::compress::ChunkCompressor;
use logfwd_io::otlp_receiver::{
    decode_protobuf_bytes_to_batch_projected_experimental, decode_protobuf_to_batch,
    decode_protobuf_to_batch_projected_detached_experimental,
    decode_protobuf_to_batch_prost_reference,
};
use logfwd_output::Compression;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

const NANOS_BASE: u64 = 1_712_509_200_123_456_789;

#[derive(Clone, Copy)]
struct FixtureProfile {
    name: &'static str,
    resources: usize,
    scopes_per_resource: usize,
    rows_per_scope: usize,
    attrs_per_record: usize,
    resource_attrs: usize,
    body_len: usize,
    has_complex_any: bool,
    has_duplicate_keys: bool,
    has_trace_heavy: bool,
}

impl FixtureProfile {
    const fn total_rows(self) -> usize {
        self.resources * self.scopes_per_resource * self.rows_per_scope
    }
}

const FIXTURES: [FixtureProfile; 10] = [
    FixtureProfile {
        name: "tiny",
        resources: 1,
        scopes_per_resource: 1,
        rows_per_scope: 32,
        attrs_per_record: 4,
        resource_attrs: 3,
        body_len: 48,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "narrow-1k",
        resources: 1,
        scopes_per_resource: 1,
        rows_per_scope: 1_000,
        attrs_per_record: 4,
        resource_attrs: 4,
        body_len: 72,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "wide-10k",
        resources: 1,
        scopes_per_resource: 1,
        rows_per_scope: 10_000,
        attrs_per_record: 20,
        resource_attrs: 6,
        body_len: 240,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "attrs-heavy",
        resources: 1,
        scopes_per_resource: 1,
        rows_per_scope: 2_000,
        attrs_per_record: 40,
        resource_attrs: 8,
        body_len: 96,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "multi-resource-scope",
        resources: 12,
        scopes_per_resource: 4,
        rows_per_scope: 120,
        attrs_per_record: 6,
        resource_attrs: 8,
        body_len: 96,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "complex-anyvalue",
        resources: 1,
        scopes_per_resource: 2,
        rows_per_scope: 1_000,
        attrs_per_record: 12,
        resource_attrs: 6,
        body_len: 128,
        has_complex_any: true,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "duplicate-collision",
        resources: 1,
        scopes_per_resource: 1,
        rows_per_scope: 1_500,
        attrs_per_record: 14,
        resource_attrs: 6,
        body_len: 80,
        has_complex_any: false,
        has_duplicate_keys: true,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "trace-heavy",
        resources: 1,
        scopes_per_resource: 2,
        rows_per_scope: 4_000,
        attrs_per_record: 8,
        resource_attrs: 6,
        body_len: 120,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: true,
    },
    FixtureProfile {
        name: "resource-heavy",
        resources: 80,
        scopes_per_resource: 1,
        rows_per_scope: 64,
        attrs_per_record: 5,
        resource_attrs: 24,
        body_len: 64,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: false,
    },
    FixtureProfile {
        name: "compression-relevant",
        resources: 1,
        scopes_per_resource: 1,
        rows_per_scope: 5_000,
        attrs_per_record: 20,
        resource_attrs: 16,
        body_len: 512,
        has_complex_any: false,
        has_duplicate_keys: false,
        has_trace_heavy: true,
    },
];

struct FixtureData {
    profile: FixtureProfile,
    payload: Vec<u8>,
    payload_bytes: bytes::Bytes,
    production_batch: arrow::record_batch::RecordBatch,
    projected_batch: Option<arrow::record_batch::RecordBatch>,
    projected_view_batch: Option<arrow::record_batch::RecordBatch>,
}

fn fixtures() -> Vec<FixtureData> {
    FIXTURES.into_iter().map(build_fixture).collect()
}

fn build_fixture(profile: FixtureProfile) -> FixtureData {
    // Setup-only work: synthesize a request, encode it, and validate the
    // decoder outputs before any Criterion timing begins.
    let payload = build_request(profile).encode_to_vec();
    let payload_bytes = bytes::Bytes::from(payload.clone());
    let prost_reference_batch = decode_protobuf_to_batch_prost_reference(&payload)
        .expect("benchmark fixture should decode");
    assert_eq!(prost_reference_batch.num_rows(), profile.total_rows());
    let production_batch =
        decode_protobuf_to_batch(&payload).expect("production fixture should decode");
    assert_batch_matches(&prost_reference_batch, &production_batch, profile.name);
    assert_encode_paths_match(&production_batch, profile.name);
    let mut projected_batch = None;
    let mut projected_view_batch = None;
    if !profile.has_complex_any {
        let wire_batch = decode_protobuf_to_batch_projected_detached_experimental(&payload)
            .expect("experimental wire decoder should decode primitive fixture");
        assert_batch_matches(&prost_reference_batch, &wire_batch, profile.name);
        assert_encode_paths_match(&wire_batch, profile.name);
        let view_batch =
            decode_protobuf_bytes_to_batch_projected_experimental(payload_bytes.clone())
                .expect("experimental wire view decoder should decode primitive fixture");
        let detached_view_batch = logfwd_arrow::materialize::detach(&view_batch);
        assert_batch_matches(&prost_reference_batch, &detached_view_batch, profile.name);
        assert_encode_paths_match(&view_batch, profile.name);
        projected_batch = Some(wire_batch);
        projected_view_batch = Some(view_batch);
    }
    FixtureData {
        profile,
        payload,
        payload_bytes,
        production_batch,
        projected_batch,
        projected_view_batch,
    }
}

fn assert_encode_paths_match(batch: &arrow::record_batch::RecordBatch, fixture_name: &str) {
    let metadata = generators::make_metadata();
    let mut handwritten_sink = make_otlp_sink(Compression::None);
    handwritten_sink.encode_batch(batch, &metadata);
    let handwritten_request = ExportLogsServiceRequest::decode(handwritten_sink.encoded_payload())
        .expect("handwritten OTLP encode path should produce protobuf");

    let mut generated_sink = make_otlp_sink(Compression::None);
    generated_sink.encode_batch_generated_fast(batch, &metadata);
    let generated_request = ExportLogsServiceRequest::decode(generated_sink.encoded_payload())
        .expect("generated-fast OTLP encode path should produce protobuf");

    assert_eq!(
        handwritten_request, generated_request,
        "generated-fast OTLP encode must match handwritten encode for {fixture_name}"
    );
}

fn assert_batch_matches(
    expected: &arrow::record_batch::RecordBatch,
    actual: &arrow::record_batch::RecordBatch,
    fixture_name: &str,
) {
    assert_eq!(
        expected.schema(),
        actual.schema(),
        "experimental wire schema must match prost batch for {fixture_name}"
    );
    assert_eq!(
        expected.num_rows(),
        actual.num_rows(),
        "experimental wire row count must match prost batch for {fixture_name}"
    );
    assert_eq!(
        expected.num_columns(),
        actual.num_columns(),
        "experimental wire column count must match prost batch for {fixture_name}"
    );
    for (idx, (expected_column, actual_column)) in
        expected.columns().iter().zip(actual.columns()).enumerate()
    {
        assert_eq!(
            expected_column.to_data(),
            actual_column.to_data(),
            "experimental wire column {idx} must match prost batch for {fixture_name}"
        );
    }
}

fn bench_parser_only(c: &mut Criterion) {
    let fixtures = fixtures();
    let mut group = c.benchmark_group("otlp_input_parser_only");
    group.sample_size(10);

    for fixture in &fixtures {
        // Timed work: protobuf decode only. Fixture generation and encoding are
        // already done in `fixtures()`.
        group.throughput(Throughput::Elements(fixture.profile.total_rows() as u64));
        group.bench_with_input(
            BenchmarkId::new("prost_decode", fixture.profile.name),
            &fixture.payload,
            |b, payload| {
                b.iter(|| {
                    let request = ExportLogsServiceRequest::decode(payload.as_slice())
                        .expect("protobuf parser should decode fixture");
                    std::hint::black_box(request.resource_logs.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_decode_materialize(c: &mut Criterion) {
    let fixtures = fixtures();
    let mut group = c.benchmark_group("otlp_input_decode_materialize");
    group.sample_size(10);

    for fixture in &fixtures {
        // Timed work: decode into Arrow batches. Fixture generation and parity
        // validation are not part of the measurement.
        group.throughput(Throughput::Elements(fixture.profile.total_rows() as u64));
        group.bench_with_input(
            BenchmarkId::new("prost_reference_to_batch", fixture.profile.name),
            &fixture.payload,
            |b, payload| {
                b.iter(|| {
                    let batch = decode_protobuf_to_batch_prost_reference(payload)
                        .expect("prost reference decode + materialize should decode fixture");
                    std::hint::black_box(batch.num_rows());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("production_current_to_batch", fixture.profile.name),
            &fixture.payload,
            |b, payload| {
                b.iter(|| {
                    let batch = decode_protobuf_to_batch(payload)
                        .expect("production decode + materialize should decode fixture");
                    std::hint::black_box(batch.num_rows());
                });
            },
        );

        if !fixture.profile.has_complex_any {
            group.bench_with_input(
                BenchmarkId::new("projected_detached_to_batch", fixture.profile.name),
                &fixture.payload,
                |b, payload| {
                    b.iter(|| {
                        let batch =
                            decode_protobuf_to_batch_projected_detached_experimental(payload)
                                .expect("experimental wire decoder should decode fixture");
                        std::hint::black_box(batch.num_rows());
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("projected_view_to_batch", fixture.profile.name),
                &fixture.payload_bytes,
                |b, payload| {
                    b.iter(|| {
                        let batch =
                            decode_protobuf_bytes_to_batch_projected_experimental(payload.clone())
                                .expect("experimental wire view decoder should decode fixture");
                        std::hint::black_box(batch.num_rows());
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_output_encode_only(c: &mut Criterion) {
    let fixtures = fixtures();
    let metadata = generators::make_metadata();
    let mut group = c.benchmark_group("otlp_output_encode_only");
    group.sample_size(10);

    for fixture in &fixtures {
        // Timed work: serialize a prebuilt batch into OTLP bytes. The batch
        // setup happens once in `fixtures()`.
        group.throughput(Throughput::Elements(fixture.profile.total_rows() as u64));

        group.bench_with_input(
            BenchmarkId::new("handwritten", fixture.profile.name),
            fixture,
            |b, fixture| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    sink.encode_batch(&fixture.production_batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("generated_fast", fixture.profile.name),
            fixture,
            |b, fixture| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    sink.encode_batch_generated_fast(&fixture.production_batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        if let Some(batch) = fixture.projected_batch.as_ref() {
            group.bench_with_input(
                BenchmarkId::new("projected_detached_handwritten", fixture.profile.name),
                batch,
                |b, batch| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        sink.encode_batch(batch, &metadata);
                        std::hint::black_box(sink.encoded_payload().len());
                    });
                },
            );
        }

        if let Some(batch) = fixture.projected_view_batch.as_ref() {
            group.bench_with_input(
                BenchmarkId::new("projected_view_handwritten", fixture.profile.name),
                batch,
                |b, batch| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        sink.encode_batch(batch, &metadata);
                        std::hint::black_box(sink.encoded_payload().len());
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_output_compression(c: &mut Criterion) {
    let fixtures = fixtures();
    let metadata = generators::make_metadata();
    let mut group = c.benchmark_group("otlp_output_compression");
    group.sample_size(10);

    for fixture in &fixtures {
        // Timed work: zstd compression of an already encoded payload. Encoding
        // is done once up front so the compression numbers stay isolated.
        let mut sink = make_otlp_sink(Compression::None);
        sink.encode_batch(&fixture.production_batch, &metadata);
        let encoded = sink.encoded_payload().to_vec();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("zstd_level1", fixture.profile.name),
            &encoded,
            |b, encoded| {
                let mut compressor =
                    ChunkCompressor::new(1).expect("zstd level 1 compressor should build");
                b.iter(|| {
                    let compressed = compressor
                        .compress(encoded)
                        .expect("encoded OTLP payload should compress");
                    std::hint::black_box(compressed.compressed_size);
                });
            },
        );
    }

    group.finish();
}

fn bench_end_to_end(c: &mut Criterion) {
    let fixtures = fixtures();
    let metadata = generators::make_metadata();
    let mut group = c.benchmark_group("otlp_e2e_in_process");
    group.sample_size(10);

    for fixture in &fixtures {
        // Timed work: decode + encode in one process. This still excludes
        // request synthesis, fixture validation, and any transport/network I/O.
        group.throughput(Throughput::Elements(fixture.profile.total_rows() as u64));

        group.bench_with_input(
            BenchmarkId::new(
                "prost_reference_to_handwritten_encode",
                fixture.profile.name,
            ),
            &fixture.payload,
            |b, payload| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch = decode_protobuf_to_batch_prost_reference(payload)
                        .expect("prost reference decode + materialize should decode fixture");
                    sink.encode_batch(&batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(
                "prost_reference_to_generated_fast_encode",
                fixture.profile.name,
            ),
            &fixture.payload,
            |b, payload| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch = decode_protobuf_to_batch_prost_reference(payload)
                        .expect("prost reference decode + materialize should decode fixture");
                    sink.encode_batch_generated_fast(&batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(
                "production_current_to_handwritten_encode",
                fixture.profile.name,
            ),
            &fixture.payload,
            |b, payload| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch = decode_protobuf_to_batch(payload)
                        .expect("production decode + materialize should decode fixture");
                    sink.encode_batch(&batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(
                "production_current_to_generated_fast_encode",
                fixture.profile.name,
            ),
            &fixture.payload,
            |b, payload| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch = decode_protobuf_to_batch(payload)
                        .expect("production decode + materialize should decode fixture");
                    sink.encode_batch_generated_fast(&batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        if !fixture.profile.has_complex_any {
            group.bench_with_input(
                BenchmarkId::new(
                    "projected_detached_to_handwritten_encode",
                    fixture.profile.name,
                ),
                &fixture.payload,
                |b, payload| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        let batch =
                            decode_protobuf_to_batch_projected_detached_experimental(payload)
                                .expect("experimental wire decoder should decode fixture");
                        sink.encode_batch(&batch, &metadata);
                        std::hint::black_box(sink.encoded_payload().len());
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("projected_view_to_handwritten_encode", fixture.profile.name),
                &fixture.payload_bytes,
                |b, payload| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        let batch =
                            decode_protobuf_bytes_to_batch_projected_experimental(payload.clone())
                                .expect("experimental wire view decoder should decode fixture");
                        sink.encode_batch(&batch, &metadata);
                        std::hint::black_box(sink.encoded_payload().len());
                    });
                },
            );
        }
    }

    group.finish();
}

fn build_request(profile: FixtureProfile) -> ExportLogsServiceRequest {
    let mut resource_logs = Vec::with_capacity(profile.resources);

    for resource_idx in 0..profile.resources {
        let mut scopes = Vec::with_capacity(profile.scopes_per_resource);
        for scope_idx in 0..profile.scopes_per_resource {
            let mut records = Vec::with_capacity(profile.rows_per_scope);
            for row in 0..profile.rows_per_scope {
                let global_row = ((resource_idx * profile.scopes_per_resource + scope_idx)
                    * profile.rows_per_scope)
                    + row;
                let mut attributes = make_record_attrs(profile, global_row);
                if profile.has_duplicate_keys {
                    attributes.push(kv_string("body", "attr-body-shadow"));
                    attributes.push(kv_string("trace_id", "attr-trace-shadow"));
                    attributes.push(kv_string("flags", "attr-flags-shadow"));
                    attributes.push(kv_string("_resource_service.name", "attr-resource-shadow"));
                }

                let body = if profile.has_complex_any && row % 2 == 0 {
                    complex_body_any_value(global_row)
                } else {
                    AnyValue {
                        value: Some(Value::StringValue(make_message(
                            global_row,
                            profile.body_len,
                        ))),
                    }
                };

                let trace_id = if profile.has_trace_heavy || row % 3 != 0 {
                    trace_id_for(global_row)
                } else {
                    Vec::new()
                };
                let span_id = if profile.has_trace_heavy || row % 5 != 0 {
                    span_id_for(global_row)
                } else {
                    Vec::new()
                };

                records.push(LogRecord {
                    time_unix_nano: NANOS_BASE + global_row as u64,
                    observed_time_unix_nano: NANOS_BASE + global_row as u64 + 123,
                    severity_text: severity_text(global_row).to_string(),
                    severity_number: severity_number(global_row),
                    body: Some(body),
                    trace_id,
                    span_id,
                    flags: 1,
                    attributes,
                    ..Default::default()
                });
            }

            scopes.push(ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: format!("bench-scope-{resource_idx}-{scope_idx}"),
                    version: "2026.04".to_string(),
                    attributes: vec![kv_string("scope.env", "bench")],
                    dropped_attributes_count: 0,
                }),
                log_records: records,
                schema_url: String::new(),
            });
        }

        resource_logs.push(ResourceLogs {
            resource: Some(Resource {
                attributes: make_resource_attrs(profile, resource_idx),
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: scopes,
            schema_url: String::new(),
        });
    }

    ExportLogsServiceRequest { resource_logs }
}

fn make_resource_attrs(profile: FixtureProfile, resource_idx: usize) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(profile.resource_attrs.max(4));
    attrs.push(kv_string(
        "service.name",
        &format!("bench-service-{resource_idx}"),
    ));
    attrs.push(kv_string("service.namespace", "bench"));
    attrs.push(kv_string(
        "host.name",
        &format!("bench-node-{}", resource_idx % 32),
    ));
    attrs.push(kv_string("k8s.cluster.name", "bench-cluster"));

    for idx in 0..profile.resource_attrs.saturating_sub(4) {
        attrs.push(kv_string(
            &format!("resource.attr.{idx}"),
            &format!("rv{}_{}", resource_idx % 19, idx % 11),
        ));
    }

    attrs
}

fn make_record_attrs(profile: FixtureProfile, row: usize) -> Vec<KeyValue> {
    let mut attrs = Vec::with_capacity(profile.attrs_per_record + 4);
    attrs.push(kv_string("host", &format!("host-{}", row % 64)));
    attrs.push(kv_i64("status", 200 + (row % 9) as i64));
    attrs.push(kv_f64("duration_ms", (row % 1000) as f64 / 10.0));
    attrs.push(kv_bool("success", row % 17 != 0));

    for idx in 0..profile.attrs_per_record.saturating_sub(4) {
        let value = if profile.has_complex_any && idx % 5 == 0 {
            complex_body_any_value(row + idx)
        } else if idx % 3 == 0 {
            AnyValue {
                value: Some(Value::IntValue((row as i64 + idx as i64) % 4096)),
            }
        } else {
            AnyValue {
                value: Some(Value::StringValue(format!("v{}_{}", idx % 37, row % 23))),
            }
        };
        attrs.push(KeyValue {
            key: format!("attr_{idx}"),
            value: Some(value),
        });
    }

    attrs
}

fn complex_body_any_value(row: usize) -> AnyValue {
    AnyValue {
        value: Some(Value::KvlistValue(KeyValueList {
            values: vec![
                kv_string("event.kind", "benchmark"),
                KeyValue {
                    key: "event.row".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::IntValue(row as i64)),
                    }),
                },
                KeyValue {
                    key: "nested".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::StringValue("alpha".to_string())),
                                },
                                AnyValue {
                                    value: Some(Value::BoolValue(row % 2 == 0)),
                                },
                            ],
                        })),
                    }),
                },
            ],
        })),
    }
}

fn make_message(row: usize, target_len: usize) -> String {
    let mut message = format!("benchmark row={row} subsystem=otlp_io ");
    while message.len() < target_len {
        message.push_str("payload_token ");
    }
    message.truncate(target_len);
    message
}

fn severity_text(row: usize) -> &'static str {
    match row % 4 {
        0 => "INFO",
        1 => "WARN",
        2 => "ERROR",
        _ => "DEBUG",
    }
}

fn severity_number(row: usize) -> i32 {
    match row % 4 {
        0 => 9,
        1 => 13,
        2 => 17,
        _ => 5,
    }
}

fn trace_id_for(row: usize) -> Vec<u8> {
    let mut out = vec![0u8; 16];
    for (idx, byte) in out.iter_mut().enumerate() {
        *byte = ((row + idx) % 251) as u8;
    }
    out
}

fn span_id_for(row: usize) -> Vec<u8> {
    let mut out = vec![0u8; 8];
    for (idx, byte) in out.iter_mut().enumerate() {
        *byte = ((row + idx + 31) % 251) as u8;
    }
    out
}

fn kv_string(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

fn kv_i64(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
    }
}

fn kv_f64(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
    }
}

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::BoolValue(value)),
        }),
    }
}

criterion_group!(
    benches,
    bench_parser_only,
    bench_decode_materialize,
    bench_output_encode_only,
    bench_output_compression,
    bench_end_to_end
);
criterion_main!(benches);
