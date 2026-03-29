//! Criterion benchmarks for the logfwd pipeline.
//!
//! Run with: cargo bench -p logfwd-bench
//! JSON output: cargo bench -p logfwd-bench -- --output-format bencher 2>/dev/null

use std::fmt::Write;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_core::compress::ChunkCompressor;
use logfwd_core::cri::{CriReassembler, parse_cri_line};
use logfwd_core::scanner::{ScanConfig, Scanner};
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
                let mut scanner = Scanner::new(ScanConfig::default(), n);
                scanner.scan(data)
            })
        });

        // Pushdown: only extract 3 fields.
        group.bench_with_input(BenchmarkId::new("scan_3_fields", n), &data, |b, data| {
            b.iter(|| {
                let config = ScanConfig {
                    wanted_fields: vec![
                        logfwd_core::scanner::FieldSpec {
                            name: "level".into(),
                            aliases: vec![],
                        },
                        logfwd_core::scanner::FieldSpec {
                            name: "status".into(),
                            aliases: vec![],
                        },
                        logfwd_core::scanner::FieldSpec {
                            name: "duration_ms".into(),
                            aliases: vec![],
                        },
                    ],
                    extract_all: false,
                    keep_raw: false,
                };
                let mut scanner = Scanner::new(config, n);
                scanner.scan(data)
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
    let mut scanner = Scanner::new(ScanConfig::default(), n);
    let batch = scanner.scan(&data);

    // Passthrough: SELECT *
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("select_star", |b| {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        b.iter(|| transform.execute(batch.clone()).unwrap())
    });

    // Filter
    group.bench_function("where_filter", |b| {
        let mut transform =
            SqlTransform::new("SELECT * FROM logs WHERE level_str = 'ERROR'").unwrap();
        b.iter(|| transform.execute(batch.clone()).unwrap())
    });

    // Projection + computed column
    group.bench_function("projection_computed", |b| {
        let mut transform = SqlTransform::new(
            "SELECT level_str, message_str, status_int, duration_ms_int FROM logs",
        )
        .unwrap();
        b.iter(|| transform.execute(batch.clone()).unwrap())
    });

    // regexp_extract
    group.bench_function("regexp_extract", |b| {
        let mut transform = SqlTransform::new(
            "SELECT regexp_extract(message_str, '(GET|POST) (\\S+)', 1) AS method, \
             regexp_extract(message_str, '(GET|POST) (\\S+)', 2) AS path FROM logs",
        )
        .unwrap();
        b.iter(|| transform.execute(batch.clone()).unwrap())
    });

    // grok
    group.bench_function("grok_parse", |b| {
        let mut transform = SqlTransform::new(
            "SELECT grok(message_str, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed FROM logs",
        )
        .unwrap();
        b.iter(|| transform.execute(batch.clone()).unwrap())
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
            let mut compressor = ChunkCompressor::new(1);
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
    let mut scanner = Scanner::new(ScanConfig::default(), n);
    let batch = scanner.scan(&data);
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
            let mut scanner = Scanner::new(ScanConfig::default(), n);
            let batch = scanner.scan(&data);
            let result = transform.execute(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        })
    });

    // Full pipeline: scan → filter → capture sink
    group.bench_function("scan_filter_capture", |b| {
        let mut transform =
            SqlTransform::new("SELECT * FROM logs WHERE level_str = 'ERROR'").unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let mut scanner = Scanner::new(ScanConfig::default(), n);
            let batch = scanner.scan(&data);
            let result = transform.execute(batch).unwrap();
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
            let mut scanner = Scanner::new(ScanConfig::default(), n);
            let batch = scanner.scan(&data);
            let result = transform.execute(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// JSON extraction mode comparison
//
// Compares two approaches to extracting fields from JSON log lines:
//
// 1. Scanner extraction (current):
//    scanner(extract fields → typed Arrow columns) → DataFusion SQL
//
// 2. DataFusion-only extraction:
//    scanner(keep _raw only) → DataFusion SQL with json_get_str / json_get_int UDFs
//
// The DataFusion-only approach trades upfront scanner work for per-row UDF calls
// inside DataFusion.  Potential advantages:
//   - DataFusion can apply WHERE before extracting all fields (predicate pushdown)
//   - Schema stays fixed (_raw: Utf8) — no schema evolution to manage
//   - Simpler scanner configuration
//
// Potential disadvantages:
//   - N UDF calls per batch (one per extracted field) vs one scanner pass
//   - Each UDF call re-scans the raw JSON for its key
//   - DataFusion plan/context overhead still applies per batch
// ---------------------------------------------------------------------------

fn bench_json_extraction_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_extraction_mode");
    group.sample_size(20);

    let n = 10_000;
    let data = gen_json_lines(n);

    // --- Approach 1: scanner extracts fields → DataFusion SQL ---

    // Baseline: scan all fields, passthrough SELECT *
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("scanner_select_star", |b| {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        b.iter(|| {
            let mut scanner = Scanner::new(ScanConfig::default(), n);
            let batch = scanner.scan(&data);
            transform.execute(batch).unwrap()
        })
    });

    // Scanner + DataFusion filter on pre-extracted columns
    group.bench_function("scanner_with_filter", |b| {
        let mut transform = SqlTransform::new(
            "SELECT level_str, status_int, duration_ms_int FROM logs \
             WHERE level_str = 'ERROR'",
        )
        .unwrap();
        b.iter(|| {
            let scan_cfg = transform.scan_config();
            let mut scanner = Scanner::new(scan_cfg, n);
            let batch = scanner.scan(&data);
            transform.execute(batch).unwrap()
        })
    });

    // --- Approach 2: raw-only scan → DataFusion json_get UDFs ---

    // Cost of the raw-only scan alone (no DataFusion)
    group.bench_function("raw_only_scan", |b| {
        b.iter(|| {
            let cfg = ScanConfig {
                wanted_fields: vec![],
                extract_all: false,
                keep_raw: true,
            };
            let mut scanner = Scanner::new(cfg, n);
            scanner.scan(&data)
        })
    });

    // DataFusion-only: extract 3 fields via UDFs, no filter
    group.bench_function("datafusion_json_get_projection", |b| {
        let sql = "SELECT \
                     json_get_str(_raw, 'level')       AS level, \
                     json_get_int(_raw, 'status')      AS status, \
                     json_get_int(_raw, 'duration_ms') AS duration_ms \
                   FROM logs";
        let mut transform = SqlTransform::new(sql).unwrap();
        b.iter(|| {
            let cfg = ScanConfig {
                wanted_fields: vec![],
                extract_all: false,
                keep_raw: true,
            };
            let mut scanner = Scanner::new(cfg, n);
            let batch = scanner.scan(&data);
            transform.execute(batch).unwrap()
        })
    });

    // DataFusion-only: extract 3 fields + WHERE filter via UDFs
    // This exercises the key advantage of late extraction: DataFusion can
    // (in principle) apply the filter before projecting non-filter columns.
    group.bench_function("datafusion_json_get_filter", |b| {
        let sql = "SELECT \
                     json_get_str(_raw, 'level')       AS level, \
                     json_get_int(_raw, 'status')      AS status, \
                     json_get_int(_raw, 'duration_ms') AS duration_ms \
                   FROM logs \
                   WHERE json_get_str(_raw, 'level') = 'ERROR'";
        let mut transform = SqlTransform::new(sql).unwrap();
        b.iter(|| {
            let cfg = ScanConfig {
                wanted_fields: vec![],
                extract_all: false,
                keep_raw: true,
            };
            let mut scanner = Scanner::new(cfg, n);
            let batch = scanner.scan(&data);
            transform.execute(batch).unwrap()
        })
    });

    // --- Equivalent scanner+filter for direct apples-to-apples comparison ---

    // Scanner with field pushdown (only extract the 3 needed fields), + filter
    group.bench_function("scanner_pushdown_filter", |b| {
        let mut transform = SqlTransform::new(
            "SELECT level_str, status_int, duration_ms_int FROM logs \
             WHERE level_str = 'ERROR'",
        )
        .unwrap();
        b.iter(|| {
            // Use scan_config() pushdown so scanner only extracts the 3 fields.
            let scan_cfg = transform.scan_config();
            let mut scanner = Scanner::new(scan_cfg, n);
            let batch = scanner.scan(&data);
            transform.execute(batch).unwrap()
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
    bench_json_extraction_mode,
);
criterion_main!(benches);
