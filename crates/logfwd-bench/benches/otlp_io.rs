//! Decision-grade OTLP I/O benchmarks.
//!
//! Fixture synthesis, protobuf encoding, and parity checks happen once in setup
//! and are excluded from timing. Each benchmark only measures the labeled
//! stage:
//! - parser-only protobuf decode (`prost`)
//! - receiver decode + Arrow materialization
//! - request decompression + receiver decode + Arrow materialization
//! - sink encode-only (handwritten vs generated-fast)
//! - encoded-payload compression
//! - in-process decode -> encode.

use std::sync::Arc;

use arrow::array::{Array, StructArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use logfwd_bench::generators::otlp::{self, OtlpFixtureProfile};
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_io::compress::ChunkCompressor;
use logfwd_io::otlp_receiver::{
    ProjectedOtlpDecoder, decode_protobuf_bytes_to_batch_projected_experimental,
    decode_protobuf_bytes_to_batch_projected_only_experimental, decode_protobuf_to_batch,
    decode_protobuf_to_batch_projected_detached_experimental,
    decode_protobuf_to_batch_prost_reference,
};
use logfwd_output::Compression;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

struct FixtureData {
    profile: OtlpFixtureProfile,
    payload: Vec<u8>,
    payload_bytes: bytes::Bytes,
    zstd_payload: Vec<u8>,
    production_batch: arrow::record_batch::RecordBatch,
    projected_batch: Option<arrow::record_batch::RecordBatch>,
    projected_view_batch: Option<arrow::record_batch::RecordBatch>,
    projected_fallback_batch: arrow::record_batch::RecordBatch,
}

fn fixtures() -> Vec<FixtureData> {
    otlp::ALL_PROFILES
        .iter()
        .copied()
        .map(build_fixture)
        .collect()
}

fn build_fixture(profile: OtlpFixtureProfile) -> FixtureData {
    // Setup-only work: synthesize a request, encode it, and validate the
    // decoder outputs before any Criterion timing begins.
    let otlp_data = otlp::build_fixture(profile);
    let prost_reference_batch = decode_protobuf_to_batch_prost_reference(&otlp_data.payload)
        .expect("benchmark fixture should decode");
    assert_eq!(prost_reference_batch.num_rows(), profile.total_rows());
    let production_batch =
        decode_protobuf_to_batch(&otlp_data.payload).expect("production fixture should decode");
    assert_batch_matches(&prost_reference_batch, &production_batch, profile.name);
    assert_encode_paths_match(&production_batch, profile.name);
    let projected_fallback_batch =
        decode_protobuf_bytes_to_batch_projected_experimental(otlp_data.payload_bytes.clone())
            .expect("experimental projected fallback decoder should decode fixture");
    let detached_projected_fallback = logfwd_arrow::materialize::detach(&projected_fallback_batch);
    assert_batch_matches(
        &prost_reference_batch,
        &detached_projected_fallback,
        profile.name,
    );
    assert_encode_paths_match(&projected_fallback_batch, profile.name);
    let zstd_payload =
        zstd::encode_all(otlp_data.payload.as_slice(), 1).expect("fixture should zstd compress");
    let wire_batch = decode_protobuf_to_batch_projected_detached_experimental(&otlp_data.payload)
        .expect("experimental wire decoder should decode fixture");
    assert_batch_matches(&prost_reference_batch, &wire_batch, profile.name);
    assert_encode_paths_match(&wire_batch, profile.name);
    let view_batch =
        decode_protobuf_bytes_to_batch_projected_only_experimental(otlp_data.payload_bytes.clone())
            .expect("experimental wire view decoder should decode fixture");
    let detached_view_batch = logfwd_arrow::materialize::detach(&view_batch);
    assert_batch_matches(&prost_reference_batch, &detached_view_batch, profile.name);
    assert_encode_paths_match(&view_batch, profile.name);
    FixtureData {
        profile,
        payload: otlp_data.payload,
        payload_bytes: otlp_data.payload_bytes,
        zstd_payload,
        production_batch,
        projected_batch: Some(wire_batch),
        projected_view_batch: Some(view_batch),
        projected_fallback_batch,
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

/// Recursively cast Utf8View → Utf8 in an array, including Struct children.
fn normalize_utf8view_array(arr: &dyn Array) -> Arc<dyn Array> {
    match arr.data_type() {
        DataType::Utf8View => cast(arr, &DataType::Utf8).expect("cast Utf8View→Utf8"),
        DataType::Struct(fields) => {
            let struct_arr = arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct downcast");
            let new_fields: Vec<_> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let child = normalize_utf8view_array(struct_arr.column(i).as_ref());
                    let new_dt = child.data_type().clone();
                    let new_field = if &new_dt == f.data_type() {
                        Arc::clone(f)
                    } else {
                        Arc::new(f.as_ref().clone().with_data_type(new_dt))
                    };
                    (new_field, child)
                })
                .collect();
            let (new_field_refs, new_arrays): (Vec<_>, Vec<_>) = new_fields.into_iter().unzip();
            Arc::new(
                StructArray::try_new(
                    new_field_refs.into(),
                    new_arrays,
                    struct_arr.nulls().cloned(),
                )
                .expect("struct rebuild"),
            )
        }
        _ => make_array(arr.to_data()),
    }
}

/// Normalize a batch by recursively casting Utf8View columns to Utf8.
/// ColumnarBatchBuilder produces Utf8View; prost/StreamingBuilder produces Utf8.
fn normalize_utf8view(
    batch: &arrow::record_batch::RecordBatch,
) -> arrow::record_batch::RecordBatch {
    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let normalized = normalize_utf8view_array(col.as_ref());
        let new_dt = normalized.data_type().clone();
        // Preserve field metadata — only change data_type when it differs.
        let new_field = if &new_dt == field.data_type() {
            Arc::clone(field)
        } else {
            Arc::new(field.as_ref().clone().with_data_type(new_dt))
        };
        new_fields.push(new_field);
        new_columns.push(normalized);
    }
    let new_schema = Arc::new(Schema::new(new_fields));
    arrow::record_batch::RecordBatch::try_new(new_schema, new_columns).expect("normalized batch")
}

fn assert_batch_matches(
    expected: &arrow::record_batch::RecordBatch,
    actual: &arrow::record_batch::RecordBatch,
    fixture_name: &str,
) {
    let actual = normalize_utf8view(actual);
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

        group.bench_with_input(
            BenchmarkId::new("projected_fallback_to_batch", fixture.profile.name),
            &fixture.payload_bytes,
            |b, payload| {
                b.iter(|| {
                    let batch =
                        decode_protobuf_bytes_to_batch_projected_experimental(payload.clone())
                            .expect(
                                "projected fallback decode + materialize should decode fixture",
                            );
                    std::hint::black_box(batch.num_rows());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("projected_detached_to_batch", fixture.profile.name),
            &fixture.payload,
            |b, payload| {
                b.iter(|| {
                    let batch = decode_protobuf_to_batch_projected_detached_experimental(payload)
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
                        decode_protobuf_bytes_to_batch_projected_only_experimental(payload.clone())
                            .expect("experimental wire view decoder should decode fixture");
                    std::hint::black_box(batch.num_rows());
                });
            },
        );
    }

    group.finish();
}

fn bench_decompress_decode(c: &mut Criterion) {
    let fixtures = fixtures();
    let mut group = c.benchmark_group("otlp_input_decompress_decode");
    group.sample_size(10);

    for fixture in fixtures
        .iter()
        .filter(|f| is_decompress_gate_profile(f.profile))
    {
        // Timed work: zstd request decompression plus decode into Arrow. HTTP
        // header parsing, body collection, and receiver channel handoff are
        // covered by `src/bin/http_receiver_apples.rs`; this group isolates the
        // decompression+decode cost with the same payload shapes.
        group.throughput(Throughput::Elements(fixture.profile.total_rows() as u64));
        group.bench_with_input(
            BenchmarkId::new("zstd_production_current_to_batch", fixture.profile.name),
            &fixture.zstd_payload,
            |b, compressed| {
                b.iter(|| {
                    let decompressed =
                        zstd::decode_all(compressed.as_slice()).expect("zstd fixture decodes");
                    let batch = decode_protobuf_to_batch(&decompressed)
                        .expect("production decode + materialize should decode fixture");
                    std::hint::black_box(batch.num_rows());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("zstd_projected_fallback_to_batch", fixture.profile.name),
            &fixture.zstd_payload,
            |b, compressed| {
                b.iter(|| {
                    let decompressed =
                        zstd::decode_all(compressed.as_slice()).expect("zstd fixture decodes");
                    let batch = decode_protobuf_bytes_to_batch_projected_experimental(
                        bytes::Bytes::from(decompressed),
                    )
                    .expect("projected fallback decode + materialize should decode fixture");
                    std::hint::black_box(batch.num_rows());
                });
            },
        );
    }

    group.finish();
}

fn is_decompress_gate_profile(profile: OtlpFixtureProfile) -> bool {
    matches!(
        profile.name,
        "narrow-1k" | "compression-relevant" | "complex-anyvalue"
    )
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

        group.bench_with_input(
            BenchmarkId::new("projected_fallback_handwritten", fixture.profile.name),
            &fixture.projected_fallback_batch,
            |b, batch| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    sink.encode_batch(batch, &metadata);
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

        group.bench_with_input(
            BenchmarkId::new(
                "projected_fallback_to_handwritten_encode",
                fixture.profile.name,
            ),
            &fixture.payload_bytes,
            |b, payload| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch =
                        decode_protobuf_bytes_to_batch_projected_experimental(payload.clone())
                            .expect(
                                "projected fallback decode + materialize should decode fixture",
                            );
                    sink.encode_batch(&batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(
                "projected_detached_to_handwritten_encode",
                fixture.profile.name,
            ),
            &fixture.payload,
            |b, payload| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch = decode_protobuf_to_batch_projected_detached_experimental(payload)
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
                        decode_protobuf_bytes_to_batch_projected_only_experimental(payload.clone())
                            .expect("experimental wire view decoder should decode fixture");
                    sink.encode_batch(&batch, &metadata);
                    std::hint::black_box(sink.encoded_payload().len());
                });
            },
        );
    }

    group.finish();
}

fn bench_projected_fallback_mix(c: &mut Criterion) {
    let projected = build_fixture(otlp::NARROW_1K);
    let fallback = build_fixture(otlp::COMPLEX_ANYVALUE);
    let payloads = [
        projected.payload_bytes.clone(),
        projected.payload_bytes.clone(),
        projected.payload_bytes.clone(),
        projected.payload_bytes.clone(),
        fallback.payload_bytes.clone(),
    ];
    let rows_per_iter = projected.profile.total_rows() * 4 + fallback.profile.total_rows();

    let mut group = c.benchmark_group("otlp_projected_fallback_mix");
    group.sample_size(10);
    group.throughput(Throughput::Elements(rows_per_iter as u64));
    group.bench_function("projected_success_4x_plus_fallback_1x", |b| {
        b.iter(|| {
            let mut rows = 0usize;
            for payload in &payloads {
                let batch = decode_protobuf_bytes_to_batch_projected_experimental(payload.clone())
                    .expect("projected fallback mix should decode each fixture");
                rows += batch.num_rows();
            }
            std::hint::black_box(rows);
        });
    });
    group.finish();
}

/// Multi-batch decoder reuse: measures steady-state decode performance when the
/// same `ProjectedOtlpDecoder` processes multiple batches in sequence.
///
/// Scenarios:
/// - **repeated**: same fixture decoded N times (steady-state, column hints warm)
/// - **alternating**: two different fixtures decoded alternately (schema churn)
/// - **spike-recovery**: one wide batch followed by many narrow batches
///   (tests that a single anomalous batch doesn't permanently inflate overhead)
fn bench_multi_batch_reuse(c: &mut Criterion) {
    let mut group = c.benchmark_group("otlp_multi_batch_reuse");
    group.sample_size(10);

    // --- Repeated: same payload decoded 10 times ---
    let profiles_for_repeat = [
        otlp::NARROW_1K,
        otlp::WIDE_10K,
        otlp::ATTRS_HEAVY,
        otlp::MINIMAL,
    ];
    for profile in &profiles_for_repeat {
        if profile.has_complex_any {
            continue;
        }
        let fixture = otlp::build_fixture(*profile);
        let batches_per_iter = 10;
        let total_rows = profile.total_rows() * batches_per_iter;
        group.throughput(Throughput::Elements(total_rows as u64));
        group.bench_with_input(
            BenchmarkId::new("repeated_10x", profile.name),
            &fixture.payload_bytes,
            |b, payload| {
                let mut decoder = ProjectedOtlpDecoder::new("resource.");
                // Warm with one batch outside timing.
                let _ = decoder.decode_view_bytes(payload.clone()).unwrap();
                b.iter(|| {
                    for _ in 0..batches_per_iter {
                        let batch = decoder
                            .decode_view_bytes(payload.clone())
                            .expect("repeated decode should succeed");
                        std::hint::black_box(batch.num_rows());
                    }
                });
            },
        );
    }

    // --- Alternating: two different schemas decoded alternately ---
    let pairs = [
        (otlp::NARROW_1K, otlp::ATTRS_HEAVY, "narrow_vs_heavy"),
        (otlp::MINIMAL, otlp::WIDE_10K, "minimal_vs_wide"),
    ];
    for (a, b_profile, label) in &pairs {
        if a.has_complex_any || b_profile.has_complex_any {
            continue;
        }
        let fix_a = otlp::build_fixture(*a);
        let fix_b = otlp::build_fixture(*b_profile);
        let rounds = 5;
        let total_rows = (a.total_rows() + b_profile.total_rows()) * rounds;
        group.throughput(Throughput::Elements(total_rows as u64));
        group.bench_with_input(
            BenchmarkId::new("alternating_5x", *label),
            &(fix_a.payload_bytes.clone(), fix_b.payload_bytes.clone()),
            |bench, (pa, pb)| {
                let mut decoder = ProjectedOtlpDecoder::new("resource.");
                // Warm
                let _ = decoder.decode_view_bytes(pa.clone()).unwrap();
                bench.iter(|| {
                    for _ in 0..rounds {
                        let batch_a = decoder
                            .decode_view_bytes(pa.clone())
                            .expect("decode A should succeed");
                        std::hint::black_box(batch_a.num_rows());
                        let batch_b = decoder
                            .decode_view_bytes(pb.clone())
                            .expect("decode B should succeed");
                        std::hint::black_box(batch_b.num_rows());
                    }
                });
            },
        );
    }

    // --- Spike recovery: 1 wide batch then 10 narrow batches ---
    // Tests that processing one anomalously wide batch doesn't permanently
    // inflate allocations for subsequent normal batches.
    {
        let spike = otlp::build_fixture(otlp::WIDE_ATTRS);
        let normal = otlp::build_fixture(otlp::NARROW_1K);
        let narrow_count = 10;
        let total_rows =
            otlp::WIDE_ATTRS.total_rows() + otlp::NARROW_1K.total_rows() * narrow_count;
        group.throughput(Throughput::Elements(total_rows as u64));
        group.bench_function(
            BenchmarkId::new("spike_then_narrow_10x", "wide_attrs_then_narrow"),
            |bench| {
                let mut decoder = ProjectedOtlpDecoder::new("resource.");
                // Warm
                let _ = decoder
                    .decode_view_bytes(normal.payload_bytes.clone())
                    .unwrap();
                bench.iter(|| {
                    // Spike batch
                    let batch = decoder
                        .decode_view_bytes(spike.payload_bytes.clone())
                        .expect("spike decode should succeed");
                    std::hint::black_box(batch.num_rows());
                    // Normal batches
                    for _ in 0..narrow_count {
                        let batch = decoder
                            .decode_view_bytes(normal.payload_bytes.clone())
                            .expect("narrow decode should succeed");
                        std::hint::black_box(batch.num_rows());
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parser_only,
    bench_decode_materialize,
    bench_decompress_decode,
    bench_output_encode_only,
    bench_output_compression,
    bench_end_to_end,
    bench_projected_fallback_mix,
    bench_multi_batch_reuse
);
criterion_main!(benches);
