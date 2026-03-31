//! Criterion benchmarks for the logfwd pipeline.
//!
//! Run with: cargo bench -p logfwd-bench
//! JSON output: cargo bench -p logfwd-bench -- --output-format bencher 2>/dev/null

use std::fmt::Write;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_arrow::scanner::SimdScanner;
use logfwd_core::compress::ChunkCompressor;
use logfwd_core::cri::{CriReassembler, parse_cri_line};
use logfwd_core::scan_config::{FieldSpec, ScanConfig};
use logfwd_output::{BatchMetadata, OutputSink};
use logfwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// Test data generators
// ---------------------------------------------------------------------------

/// Generate N newline-delimited JSON log lines (~250 bytes each).
fn gen_json_lines(n: usize) -> Vec<u8> {
    let levels = ["INFO", "ERROR", "DEBUG", "WARN"];
    let paths = [
        "/api/users",
        "/api/orders",
        "/api/health",
        "/api/auth/login",
        "/api/metrics",
    ];
    let mut s = String::with_capacity(n * 260);
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"timestamp":"2024-01-15T10:30:{:02}.{:09}Z","level":"{}","message":"GET {} HTTP/1.1","status":{},"duration_ms":{},"request_id":"req-{:08x}","service":"api-gateway"}}"#,
            i % 60,
            i % 1_000_000_000,
            levels[i % levels.len()],
            paths[i % paths.len()],
            [200, 200, 200, 500, 404][i % 5],
            (i % 500) + 1,
            i,
        );
        s.push('\n');
    }
    s.into_bytes()
}

/// Generate N CRI-formatted log lines wrapping JSON.
fn gen_cri_lines(n: usize) -> Vec<u8> {
    let levels = ["INFO", "ERROR", "DEBUG", "WARN"];
    let mut s = String::with_capacity(n * 310);
    for i in 0..n {
        let _ = write!(
            s,
            r#"2024-01-15T10:30:{:02}.{:09}Z stdout F {{"level":"{}","message":"request handled","status":{},"duration_ms":{}}}"#,
            i % 60,
            i % 1_000_000_000,
            levels[i % levels.len()],
            [200, 500, 404][i % 3],
            (i % 500) + 1,
        );
        s.push('\n');
    }
    s.into_bytes()
}

fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: vec![("service.name".into(), "bench".into())],
        observed_time_ns: 0,
    }
}

/// Null sink that discards all data — measures pure serialization overhead.
struct NullSink;

impl OutputSink for NullSink {
    fn send_batch(
        &mut self,
        _batch: &arrow::record_batch::RecordBatch,
        _metadata: &BatchMetadata,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "null"
    }
}

// ---------------------------------------------------------------------------
// Scanner benchmarks
// ---------------------------------------------------------------------------

fn bench_scanner(c: &mut Criterion) {
    let mut group = c.benchmark_group("scanner");

    for &n in &[1_000, 10_000, 100_000] {
        let data = gen_json_lines(n);
        let bytes = data.len() as u64;

        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::new("scan_all_fields", n), &data, |b, data| {
            b.iter(|| {
                let mut scanner = SimdScanner::new(ScanConfig::default());
                scanner.scan(data).expect("bench: scan should not fail")
            })
        });

        // Pushdown: only extract 3 fields.
        group.bench_with_input(BenchmarkId::new("scan_3_fields", n), &data, |b, data| {
            b.iter(|| {
                let config = ScanConfig {
                    wanted_fields: vec![
                        FieldSpec {
                            name: "level".into(),
                            aliases: vec![],
                        },
                        FieldSpec {
                            name: "status".into(),
                            aliases: vec![],
                        },
                        FieldSpec {
                            name: "duration_ms".into(),
                            aliases: vec![],
                        },
                    ],
                    extract_all: false,
                    keep_raw: false,
                    validate_utf8: false,
                };
                let mut scanner = SimdScanner::new(config);
                scanner.scan(data).expect("bench: scan should not fail")
            })
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// CRI parse benchmarks
// ---------------------------------------------------------------------------

fn bench_cri(c: &mut Criterion) {
    let mut group = c.benchmark_group("cri");

    for &n in &[1_000, 10_000] {
        let data = gen_cri_lines(n);
        let bytes = data.len() as u64;

        // Benchmark just parsing (no reassembly allocation).
        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::new("parse_only", n), &data, |b, data| {
            b.iter(|| {
                let mut count = 0usize;
                let mut start = 0;
                while start < data.len() {
                    let end = memchr::memchr(b'\n', &data[start..])
                        .map(|p| start + p)
                        .unwrap_or(data.len());
                    if parse_cri_line(&data[start..end]).is_some() {
                        count += 1;
                    }
                    start = end + 1;
                }
                count
            })
        });

        // Benchmark parse + reassemble + collect into buffer.
        group.bench_with_input(BenchmarkId::new("parse_reassemble", n), &data, |b, data| {
            b.iter(|| {
                let mut reassembler = CriReassembler::new(1024 * 1024);
                let mut json_buf = Vec::with_capacity(data.len());
                let mut start = 0;
                while start < data.len() {
                    let end = memchr::memchr(b'\n', &data[start..])
                        .map(|p| start + p)
                        .unwrap_or(data.len());
                    if let Some(cri) = parse_cri_line(&data[start..end])
                        && let Some(msg) = reassembler.feed(&cri)
                    {
                        json_buf.extend_from_slice(msg);
                        json_buf.push(b'\n');
                    }
                    start = end + 1;
                }
                json_buf
            })
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Transform benchmarks (DataFusion SQL)
// ---------------------------------------------------------------------------

fn bench_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform");
    // Transforms are heavier; use fewer iterations.
    group.sample_size(20);

    let n = 10_000;
    let data = gen_json_lines(n);
    let mut scanner = SimdScanner::new(ScanConfig::default());
    let batch = scanner.scan(&data).expect("bench: scan should not fail");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("select_star", |b| {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        b.iter(|| transform.execute_blocking(batch.clone()).unwrap())
    });

    // Filter
    group.bench_function("where_filter", |b| {
        let mut transform =
            SqlTransform::new("SELECT * FROM logs WHERE level_str = 'ERROR'").unwrap();
        b.iter(|| transform.execute_blocking(batch.clone()).unwrap())
    });

    // Projection + computed column
    group.bench_function("projection_computed", |b| {
        let mut transform = SqlTransform::new(
            "SELECT level_str, message_str, status_int, duration_ms_int FROM logs",
        )
        .unwrap();
        b.iter(|| transform.execute_blocking(batch.clone()).unwrap())
    });

    // regexp_extract
    group.bench_function("regexp_extract", |b| {
        let mut transform = SqlTransform::new(
            "SELECT regexp_extract(message_str, '(GET|POST) (\\S+)', 1) AS method, \
             regexp_extract(message_str, '(GET|POST) (\\S+)', 2) AS path FROM logs",
        )
        .unwrap();
        b.iter(|| transform.execute_blocking(batch.clone()).unwrap())
    });

    // grok
    group.bench_function("grok_parse", |b| {
        let mut transform = SqlTransform::new(
            "SELECT grok(message_str, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed FROM logs",
        )
        .unwrap();
        b.iter(|| transform.execute_blocking(batch.clone()).unwrap())
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Compression benchmarks
// ---------------------------------------------------------------------------

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compress");

    // Compress JSON log data at different sizes.
    for &n in &[1_000, 10_000, 100_000] {
        let data = gen_json_lines(n);
        let bytes = data.len() as u64;

        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::new("zstd_level1", n), &data, |b, data| {
            let mut compressor = ChunkCompressor::new(1).unwrap();
            b.iter(|| compressor.compress(data).unwrap())
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Output sink benchmarks
// ---------------------------------------------------------------------------

fn bench_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("output");
    group.sample_size(20);

    let n = 10_000;
    let data = gen_json_lines(n);
    let mut scanner = SimdScanner::new(ScanConfig::default());
    let batch = scanner.scan(&data).expect("bench: scan should not fail");
    let meta = make_metadata();

    // NullSink (measures overhead of scan + batch creation only)
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("null_sink", |b| {
        let mut sink = NullSink;
        b.iter(|| sink.send_batch(&batch, &meta).unwrap())
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// End-to-end: scan → transform → output
// ---------------------------------------------------------------------------

fn bench_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("end_to_end");
    group.sample_size(20);

    let n = 10_000;
    let data = gen_json_lines(n);
    let meta = make_metadata();

    // Full pipeline: scan → SELECT * → capture sink
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("scan_passthrough_capture", |b| {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let mut scanner = SimdScanner::new(ScanConfig::default());
            let batch = scanner.scan(&data).expect("bench: scan should not fail");
            let result = transform.execute_blocking(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        })
    });

    // Full pipeline: scan → filter → capture sink
    group.bench_function("scan_filter_capture", |b| {
        let mut transform =
            SqlTransform::new("SELECT * FROM logs WHERE level_str = 'ERROR'").unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let mut scanner = SimdScanner::new(ScanConfig::default());
            let batch = scanner.scan(&data).expect("bench: scan should not fail");
            let result = transform.execute_blocking(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        })
    });

    // Full pipeline: scan → grok + filter → capture sink
    group.bench_function("scan_grok_filter_capture", |b| {
        let mut transform = SqlTransform::new(
            "SELECT grok(message_str, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed \
             FROM logs WHERE level_str != 'DEBUG'",
        )
        .unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let mut scanner = SimdScanner::new(ScanConfig::default());
            let batch = scanner.scan(&data).expect("bench: scan should not fail");
            let result = transform.execute_blocking(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_scanner,
    bench_cri,
    bench_transform,
    bench_compress,
    bench_output,
    bench_end_to_end,
);
criterion_main!(benches);
