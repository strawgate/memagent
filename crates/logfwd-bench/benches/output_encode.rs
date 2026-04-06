//! Output format encoding benchmarks.
//!
//! Compares the encoding cost of different output formats given the same
//! `RecordBatch`:
//!
//! - **OTLP protobuf** — `OtlpSink::encode_batch`
//! - **OTLP protobuf + zstd** — encode then compress
//! - **JSON lines** — `build_col_infos` + `write_row_json`
//! - **NullSink** — baseline (measures iteration overhead only)
//!
//! Varies: narrow (5 fields) vs wide (20+ fields), 1K/10K rows.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::{NullSink, generators, make_otlp_sink};
use logfwd_core::scan_config::ScanConfig;
use logfwd_io::compress::ChunkCompressor;
use logfwd_output::Compression;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn scan_to_batch(data: &[u8]) -> arrow::record_batch::RecordBatch {
    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan_detached(bytes::Bytes::copy_from_slice(data))
        .expect("bench: scan should not fail")
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

fn bench_output_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("output_encode");
    group.sample_size(20);

    let meta = generators::make_metadata();

    // Schema variants: narrow (5 fields) and wide (20+ fields)
    #[allow(clippy::type_complexity)]
    let variants: Vec<(&str, Vec<(usize, Vec<u8>)>)> = vec![
        (
            "narrow",
            vec![
                (1_000, generators::gen_narrow(1_000, 42)),
                (10_000, generators::gen_narrow(10_000, 42)),
            ],
        ),
        (
            "wide",
            vec![
                (1_000, generators::gen_wide(1_000, 42)),
                (10_000, generators::gen_wide(10_000, 42)),
            ],
        ),
    ];

    for (schema_name, sizes) in &variants {
        for (n, data) in sizes {
            let batch = scan_to_batch(data);
            let label = format!("{schema_name}/{n}");

            group.throughput(Throughput::Elements(*n as u64));

            // NullSink — baseline
            group.bench_with_input(BenchmarkId::new("null_sink", &label), &batch, |b, batch| {
                let mut sink = NullSink;
                b.iter(|| sink.send_batch(batch, &meta).unwrap());
            });

            // OTLP protobuf encoding (no compression)
            group.bench_with_input(
                BenchmarkId::new("otlp_encode", &label),
                &batch,
                |b, batch| {
                    let mut sink = make_otlp_sink(Compression::None);
                    b.iter(|| {
                        std::hint::black_box(sink.encode_batch(batch, &meta));
                    });
                },
            );

            // JSON lines + zstd: serialize to NDJSON then compress.
            // Measures the combined cost of JSON serialization and compression.
            group.bench_with_input(
                BenchmarkId::new("json_lines_zstd", &label),
                &batch,
                |b, batch| {
                    let cols = logfwd_output::build_col_infos(batch);
                    let mut json_buf = Vec::with_capacity(*n * 300);
                    let mut compressor =
                        ChunkCompressor::new(1).expect("zstd level 1 is always valid");
                    b.iter(|| {
                        json_buf.clear();
                        for row in 0..batch.num_rows() {
                            logfwd_output::write_row_json(batch, row, &cols, &mut json_buf)
                                .expect("JSON serialization should not fail");
                            json_buf.push(b'\n');
                        }
                        let compressed = compressor
                            .compress(&json_buf)
                            .expect("compression should not fail");
                        std::hint::black_box(compressed.compressed_size);
                    });
                },
            );

            // JSON lines serialization (build_col_infos + write_row_json)
            group.bench_with_input(
                BenchmarkId::new("json_lines", &label),
                &batch,
                |b, batch| {
                    let cols = logfwd_output::build_col_infos(batch);
                    let mut buf = Vec::with_capacity(*n * 300);
                    b.iter(|| {
                        buf.clear();
                        for row in 0..batch.num_rows() {
                            logfwd_output::write_row_json(batch, row, &cols, &mut buf)
                                .expect("JSON serialization should not fail");
                            buf.push(b'\n');
                        }
                        std::hint::black_box(buf.len());
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_output_encode);
criterion_main!(benches);
