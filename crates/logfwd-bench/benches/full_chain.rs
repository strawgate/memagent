//! End-to-end function composition benchmarks.
//!
//! Composes the complete synchronous data path to measure hidden handoff and
//! allocation overhead between stages:
//!
//! - **JSON chain:** JSON bytes → Scanner → SqlTransform(SELECT *) → OtlpSink::encode_batch
//! - **CRI chain:** CRI bytes → parse+reassemble → Scanner → SqlTransform(WHERE) → OTLP encode
//! - **Grok chain:** JSON bytes → Scanner → SqlTransform(grok+filter) → JSON serialize
//!
//! Varies: 1K / 10K / 100K rows, narrow / production-mixed data.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::{NullSink, generators, make_otlp_sink};
use logfwd_core::cri::{CriReassembler, parse_cri_line};
use logfwd_core::reassembler::AggregateResult;
use logfwd_core::scan_config::ScanConfig;
use logfwd_output::Compression;
use logfwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse CRI data and reassemble into JSON lines buffer.
fn cri_to_json(data: &[u8], reassembler: &mut CriReassembler, json_buf: &mut Vec<u8>) {
    reassembler.reset();
    json_buf.clear();
    let mut start = 0;
    while start < data.len() {
        let end = memchr::memchr(b'\n', &data[start..]).map_or(data.len(), |p| start + p);
        if let Some(cri) = parse_cri_line(&data[start..end]) {
            match reassembler.feed(cri.message, cri.is_full) {
                AggregateResult::Complete(msg) | AggregateResult::Truncated(msg) => {
                    json_buf.extend_from_slice(msg);
                    json_buf.push(b'\n');
                    reassembler.reset();
                }
                AggregateResult::Pending => {}
            }
        }
        start = end + 1;
    }
}

// ---------------------------------------------------------------------------
// JSON chain: JSON → scan → transform → OTLP encode
// ---------------------------------------------------------------------------

fn bench_json_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_chain_json");
    group.sample_size(20);

    let meta = generators::make_metadata();

    for &n in &[1_000usize, 10_000, 100_000] {
        let data = generators::gen_production_mixed(n, 42);

        group.throughput(Throughput::Bytes(data.len() as u64));

        // Composed: scan → SELECT * → OTLP encode
        group.bench_with_input(BenchmarkId::new("composed", n), &data, |b, data| {
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
        });

        // Individual stages for comparison:

        // Stage 1: scan only
        group.bench_with_input(BenchmarkId::new("scan_only", n), &data, |b, data| {
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

        // Stage 2: transform only (pre-scanned batch)
        let mut scanner = Scanner::new(ScanConfig::default());
        let batch = scanner
            .scan_detached(bytes::Bytes::from(data.clone()))
            .expect("scan should not fail");
        group.bench_with_input(BenchmarkId::new("transform_only", n), &batch, |b, batch| {
            let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
            b.iter(|| transform.execute_blocking(batch.clone()).unwrap());
        });

        // Stage 3: OTLP encode only (pre-transformed batch)
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        let transformed = transform.execute_blocking(batch).unwrap();
        group.bench_with_input(
            BenchmarkId::new("encode_only", n),
            &transformed,
            |b, batch| {
                let mut sink = make_otlp_sink(Compression::None);
                b.iter(|| std::hint::black_box(sink.encode_batch(batch, &meta)));
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// CRI chain: CRI → parse+reassemble → scan → WHERE filter → OTLP encode
// ---------------------------------------------------------------------------

fn bench_cri_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_chain_cri");
    group.sample_size(20);

    let meta = generators::make_metadata();

    for &n in &[1_000usize, 10_000] {
        let data = generators::gen_cri_k8s(n, 42);

        group.throughput(Throughput::Bytes(data.len() as u64));

        // Composed: CRI parse → scan → WHERE filter → OTLP encode
        group.bench_with_input(BenchmarkId::new("composed", n), &data, |b, data| {
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform =
                SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
            let mut sink = make_otlp_sink(Compression::None);
            let mut reassembler = CriReassembler::new(1024 * 1024);
            let mut json_buf = Vec::with_capacity(data.len());
            b.iter(|| {
                cri_to_json(data, &mut reassembler, &mut json_buf);
                let batch = scanner
                    .scan_detached(bytes::Bytes::copy_from_slice(&json_buf))
                    .expect("scan should not fail");
                let result = transform.execute_blocking(batch).unwrap();
                std::hint::black_box(sink.encode_batch(&result, &meta));
            });
        });

        // Stage 1: CRI parse + reassemble only
        group.bench_with_input(BenchmarkId::new("cri_parse_only", n), &data, |b, data| {
            let mut reassembler = CriReassembler::new(1024 * 1024);
            let mut json_buf = Vec::with_capacity(data.len());
            b.iter(|| {
                cri_to_json(data, &mut reassembler, &mut json_buf);
                std::hint::black_box(&json_buf);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Grok chain: JSON → scan → grok+filter → JSON serialize
// ---------------------------------------------------------------------------

fn bench_grok_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_chain_grok");
    group.sample_size(20);

    let meta = generators::make_metadata();

    for &n in &[1_000usize, 10_000] {
        let data = generators::gen_production_mixed(n, 42);

        group.throughput(Throughput::Bytes(data.len() as u64));

        // Composed: scan → grok+filter → JSON serialize
        group.bench_with_input(BenchmarkId::new("composed", n), &data, |b, data| {
            let data_bytes = bytes::Bytes::from(data.clone());
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform = SqlTransform::new(
                "SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed \
                 FROM logs WHERE level != 'DEBUG'",
            )
            .unwrap();
            let mut sink = NullSink;
            let mut buf = Vec::with_capacity(n * 200);
            b.iter(|| {
                let batch = scanner
                    .scan_detached(data_bytes.clone())
                    .expect("scan should not fail");
                let result = transform.execute_blocking(batch).unwrap();
                std::hint::black_box(sink.send_batch(&result, &meta).unwrap());
                // Also serialize to JSON to measure the full output path
                let cols = logfwd_output::build_col_infos(&result);
                buf.clear();
                for row in 0..result.num_rows() {
                    logfwd_output::write_row_json(&result, row, &cols, &mut buf, false)
                        .expect("JSON serialization should not fail");
                    buf.push(b'\n');
                }
                std::hint::black_box(&buf);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(benches, bench_json_chain, bench_cri_chain, bench_grok_chain);
criterion_main!(benches);
