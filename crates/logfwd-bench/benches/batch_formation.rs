//! Batch formation timing benchmarks.
//!
//! Measures how batch size affects scan + transform throughput and captures
//! the amortization curve: at what batch size does per-row overhead stabilize?
//!
//! - Varies batch sizes: 100, 500, 1K, 5K, 10K, 50K, 100K rows
//! - Measures: scan time, transform time, and combined throughput per batch
//! - Tracks: per-row cost at each batch size to find the amortization knee

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::{NullSink, generators, make_otlp_sink};
use logfwd_core::scan_config::ScanConfig;
use logfwd_output::Compression;
use logfwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// Batch size scaling: scan
// ---------------------------------------------------------------------------

fn bench_batch_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_scan");
    group.sample_size(50);

    let batch_sizes: &[usize] = &[100, 500, 1_000, 5_000, 10_000, 50_000, 100_000];

    for &n in batch_sizes {
        let data = generators::gen_production_mixed(n, 42);

        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("scan", n), &data, |b, data| {
            let data_bytes = bytes::Bytes::from(data.clone());
            let mut scanner = Scanner::new(ScanConfig::default());
            b.iter(|| {
                std::hint::black_box(
                    scanner
                        .scan_detached(data_bytes.clone())
                        .expect("scan should not fail"),
                )
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Batch size scaling: scan + transform
// ---------------------------------------------------------------------------

fn bench_batch_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_transform");
    group.sample_size(50);

    let meta = generators::make_metadata();
    let batch_sizes: &[usize] = &[100, 500, 1_000, 5_000, 10_000, 50_000];

    for &n in batch_sizes {
        let data = generators::gen_production_mixed(n, 42);

        group.throughput(Throughput::Bytes(data.len() as u64));

        // Scan + SELECT * (passthrough)
        group.bench_with_input(BenchmarkId::new("scan_passthrough", n), &data, |b, data| {
            let data_bytes = bytes::Bytes::from(data.clone());
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
            let mut sink = NullSink;
            b.iter(|| {
                let batch = scanner
                    .scan_detached(data_bytes.clone())
                    .expect("scan should not fail");
                let result = transform.execute_blocking(batch).unwrap();
                std::hint::black_box(sink.send_batch(&result, &meta).unwrap());
            });
        });

        // Scan + WHERE filter
        group.bench_with_input(BenchmarkId::new("scan_filter", n), &data, |b, data| {
            let data_bytes = bytes::Bytes::from(data.clone());
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform =
                SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
            let mut sink = NullSink;
            b.iter(|| {
                let batch = scanner
                    .scan_detached(data_bytes.clone())
                    .expect("scan should not fail");
                let result = transform.execute_blocking(batch).unwrap();
                std::hint::black_box(sink.send_batch(&result, &meta).unwrap());
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Batch size scaling: full pipeline (scan + transform + encode)
// ---------------------------------------------------------------------------

fn bench_batch_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_pipeline");
    group.sample_size(50);

    let meta = generators::make_metadata();
    let batch_sizes: &[usize] = &[100, 500, 1_000, 5_000, 10_000, 50_000];

    for &n in batch_sizes {
        let data = generators::gen_production_mixed(n, 42);

        group.throughput(Throughput::Bytes(data.len() as u64));

        // Full pipeline: scan → SELECT * → OTLP encode
        group.bench_with_input(
            BenchmarkId::new("scan_transform_otlp", n),
            &data,
            |b, data| {
                let data_bytes = bytes::Bytes::from(data.clone());
                let mut scanner = Scanner::new(ScanConfig::default());
                let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| {
                    let batch = scanner
                        .scan_detached(data_bytes.clone())
                        .expect("scan should not fail");
                    let result = transform.execute_blocking(batch).unwrap();
                    std::hint::black_box(sink.encode_batch(&result, &meta));
                });
            },
        );

        // Full pipeline: scan → SELECT * → JSON serialize
        group.bench_with_input(
            BenchmarkId::new("scan_transform_json", n),
            &data,
            |b, data| {
                let data_bytes = bytes::Bytes::from(data.clone());
                let mut scanner = Scanner::new(ScanConfig::default());
                let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
                let mut buf = Vec::with_capacity(n * 300);
                b.iter(|| {
                    let batch = scanner
                        .scan_detached(data_bytes.clone())
                        .expect("scan should not fail");
                    let result = transform.execute_blocking(batch).unwrap();
                    let cols = logfwd_output::build_col_infos(&result);
                    buf.clear();
                    for row in 0..result.num_rows() {
                        logfwd_output::write_row_json(&result, row, &cols, &mut buf, false)
                            .expect("JSON serialization should not fail");
                        buf.push(b'\n');
                    }
                    std::hint::black_box(&buf);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_batch_scan,
    bench_batch_transform,
    bench_batch_pipeline
);
criterion_main!(benches);
