//! Criterion benchmarks for the logfwd pipeline.
//!
//! Run with: cargo bench -p logfwd-bench
//! JSON output: cargo bench -p logfwd-bench -- --output-format bencher 2>/dev/null

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::{NullSink, generators};
use logfwd_core::cri::{AggregateResult, CriReassembler, parse_cri_line};
use logfwd_core::scan_config::{FieldSpec, ScanConfig};
use logfwd_io::compress::ChunkCompressor;
use logfwd_output::{ElasticsearchRequestMode, ElasticsearchSinkFactory};
use logfwd_transform::SqlTransform;
use logfwd_types::diagnostics::ComponentStats;

// ---------------------------------------------------------------------------
// Scanner benchmarks
// ---------------------------------------------------------------------------

fn bench_scanner(c: &mut Criterion) {
    let mut group = c.benchmark_group("scanner");

    for &n in &[1_000, 10_000, 100_000] {
        let data = generators::gen_production_mixed(n, 42);
        let bytes = data.len() as u64;

        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::new("scan_all_fields", n), &data, |b, data| {
            let data_bytes = bytes::Bytes::from(data.clone());
            let mut scanner = Scanner::new(ScanConfig::default());
            b.iter(|| {
                std::hint::black_box(
                    scanner
                        .scan_detached(data_bytes.clone())
                        .expect("bench: scan should not fail"),
                )
            });
        });

        // Pushdown: only extract 3 fields.
        group.bench_with_input(BenchmarkId::new("scan_3_fields", n), &data, |b, data| {
            let data_bytes = bytes::Bytes::from(data.clone());
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
                line_field_name: None,
                validate_utf8: false,
                row_predicate: None,
            };
            let mut scanner = Scanner::new(config);
            b.iter(|| {
                std::hint::black_box(
                    scanner
                        .scan_detached(data_bytes.clone())
                        .expect("bench: scan should not fail"),
                )
            });
        });

        // Predicate pushdown: level = 'error' (filters ~80-90% of rows).
        group.bench_with_input(
            BenchmarkId::new("scan_predicate_level_eq_error", n),
            &data,
            |b, data| {
                use logfwd_core::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};
                let data_bytes = bytes::Bytes::from(data.clone());
                let config = ScanConfig {
                    wanted_fields: vec![],
                    extract_all: true,
                    line_field_name: None,
                    validate_utf8: false,
                    row_predicate: Some(ScanPredicate::Compare {
                        field: "level".into(),
                        op: CmpOp::Eq,
                        value: ScalarValue::Str("error".into()),
                    }),
                };
                let mut scanner = Scanner::new(config);
                b.iter(|| {
                    std::hint::black_box(
                        scanner
                            .scan_detached(data_bytes.clone())
                            .expect("bench: scan should not fail"),
                    )
                });
            },
        );

        // Predicate pushdown: status >= 500 (filters ~60-70% of rows).
        group.bench_with_input(
            BenchmarkId::new("scan_predicate_status_ge_500", n),
            &data,
            |b, data| {
                use logfwd_core::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};
                let data_bytes = bytes::Bytes::from(data.clone());
                let config = ScanConfig {
                    wanted_fields: vec![],
                    extract_all: true,
                    line_field_name: None,
                    validate_utf8: false,
                    row_predicate: Some(ScanPredicate::Compare {
                        field: "status".into(),
                        op: CmpOp::Ge,
                        value: ScalarValue::Int(500),
                    }),
                };
                let mut scanner = Scanner::new(config);
                b.iter(|| {
                    std::hint::black_box(
                        scanner
                            .scan_detached(data_bytes.clone())
                            .expect("bench: scan should not fail"),
                    )
                });
            },
        );

        // Predicate pushdown: AND chain (level = 'error' AND status >= 500).
        group.bench_with_input(
            BenchmarkId::new("scan_predicate_and_chain", n),
            &data,
            |b, data| {
                use logfwd_core::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};
                let data_bytes = bytes::Bytes::from(data.clone());
                let config = ScanConfig {
                    wanted_fields: vec![],
                    extract_all: true,
                    line_field_name: None,
                    validate_utf8: false,
                    row_predicate: Some(ScanPredicate::And(vec![
                        ScanPredicate::Compare {
                            field: "level".into(),
                            op: CmpOp::Eq,
                            value: ScalarValue::Str("error".into()),
                        },
                        ScanPredicate::Compare {
                            field: "status".into(),
                            op: CmpOp::Ge,
                            value: ScalarValue::Int(500),
                        },
                    ])),
                };
                let mut scanner = Scanner::new(config);
                b.iter(|| {
                    std::hint::black_box(
                        scanner
                            .scan_detached(data_bytes.clone())
                            .expect("bench: scan should not fail"),
                    )
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// CRI parse benchmarks
// ---------------------------------------------------------------------------

fn bench_cri(c: &mut Criterion) {
    let mut group = c.benchmark_group("cri");

    for &n in &[1_000, 10_000] {
        let data = generators::gen_cri_k8s(n, 42);
        let bytes = data.len() as u64;

        // Benchmark just parsing (no reassembly allocation).
        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::new("parse_only", n), &data, |b, data| {
            b.iter(|| {
                let mut count = 0usize;
                let mut start = 0;
                while start < data.len() {
                    let end =
                        memchr::memchr(b'\n', &data[start..]).map_or(data.len(), |p| start + p);
                    if parse_cri_line(&data[start..end]).is_some() {
                        count += 1;
                    }
                    start = end + 1;
                }
                std::hint::black_box(count);
            });
        });

        // Benchmark parse + reassemble + collect into buffer.
        group.bench_with_input(BenchmarkId::new("parse_reassemble", n), &data, |b, data| {
            // Allocate outside b.iter to measure steady-state throughput, not alloc overhead.
            let mut reassembler = CriReassembler::new(1024 * 1024);
            let mut json_buf = Vec::with_capacity(data.len());
            b.iter(|| {
                reassembler.reset();
                json_buf.clear();
                let mut start = 0;
                while start < data.len() {
                    let end =
                        memchr::memchr(b'\n', &data[start..]).map_or(data.len(), |p| start + p);
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
                std::hint::black_box(json_buf.as_ptr());
            });
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
    let data = generators::gen_production_mixed(n, 42);
    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner
        .scan_detached(bytes::Bytes::from(data.clone()))
        .expect("bench: scan should not fail");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("select_star", |b| {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        b.iter(|| std::hint::black_box(transform.execute_blocking(batch.clone()).unwrap()));
    });

    // Filter
    group.bench_function("where_filter", |b| {
        let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
        b.iter(|| std::hint::black_box(transform.execute_blocking(batch.clone()).unwrap()));
    });

    // Projection + computed column
    group.bench_function("projection_computed", |b| {
        let mut transform =
            SqlTransform::new("SELECT level, message, status, duration_ms FROM logs").unwrap();
        b.iter(|| std::hint::black_box(transform.execute_blocking(batch.clone()).unwrap()));
    });

    // regexp_extract
    group.bench_function("regexp_extract", |b| {
        let mut transform = SqlTransform::new(
            "SELECT regexp_extract(message, '(GET|POST) (\\S+)', 1) AS method, \
             regexp_extract(message, '(GET|POST) (\\S+)', 2) AS path FROM logs",
        )
        .unwrap();
        b.iter(|| std::hint::black_box(transform.execute_blocking(batch.clone()).unwrap()));
    });

    // grok
    group.bench_function("grok_parse", |b| {
        let mut transform = SqlTransform::new(
            "SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed FROM logs",
        )
        .unwrap();
        b.iter(|| std::hint::black_box(transform.execute_blocking(batch.clone()).unwrap()));
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
        let data = generators::gen_production_mixed(n, 42);
        let bytes = data.len() as u64;

        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::new("zstd_level1", n), &data, |b, data| {
            let mut compressor = ChunkCompressor::new(1).unwrap();
            b.iter(|| std::hint::black_box(compressor.compress(data).unwrap()));
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
    let data = generators::gen_production_mixed(n, 42);
    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner
        .scan_detached(bytes::Bytes::from(data.clone()))
        .expect("bench: scan should not fail");
    let meta = generators::make_metadata();

    // NullSink (measures overhead of scan + batch creation only)
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("null_sink", |b| {
        let mut sink = NullSink;
        b.iter(|| std::hint::black_box(sink.send_batch(&batch, &meta).unwrap()));
    });

    // JSON serialization via write_row_json (measures build_col_infos + per-row dispatch)
    group.bench_function("json_serialize", |b| {
        let cols = logfwd_output::build_col_infos(&batch);
        let mut buf = Vec::with_capacity(n * 300);
        b.iter(|| {
            buf.clear();
            for row in 0..batch.num_rows() {
                logfwd_output::write_row_json(&batch, row, &cols, &mut buf)
                    .expect("JSON serialization should not fail");
                buf.push(b'\n');
            }
            std::hint::black_box(&buf);
        });
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
    let data = generators::gen_production_mixed(n, 42);
    let meta = generators::make_metadata();

    // Full pipeline: scan → SELECT * → capture sink
    group.throughput(Throughput::Bytes(data.len() as u64));
    let data_bytes = bytes::Bytes::from(data.clone());
    group.bench_function("scan_passthrough_capture", |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner
                .scan_detached(data_bytes.clone())
                .expect("bench: scan should not fail");
            let result = transform.execute_blocking(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        });
    });

    // Full pipeline: scan → filter → capture sink
    group.bench_function("scan_filter_capture", |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner
                .scan_detached(data_bytes.clone())
                .expect("bench: scan should not fail");
            let result = transform.execute_blocking(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        });
    });

    // Full pipeline: scan → grok + filter → capture sink
    group.bench_function("scan_grok_filter_capture", |b| {
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut transform = SqlTransform::new(
            "SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed \
             FROM logs WHERE level != 'DEBUG'",
        )
        .unwrap();
        let mut sink = NullSink;
        b.iter(|| {
            let batch = scanner
                .scan_detached(data_bytes.clone())
                .expect("bench: scan should not fail");
            let result = transform.execute_blocking(batch).unwrap();
            sink.send_batch(&result, &meta).unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Elasticsearch bulk serialization benchmark
// ---------------------------------------------------------------------------

/// Measures the throughput of `ElasticsearchSink::serialize_batch` ---
/// the hot path that converts Arrow RecordBatches into NDJSON bulk payloads
/// ready to POST to `/_bulk`.
///
/// No network I/O takes place; this isolates the serialization cost.
fn bench_elasticsearch_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("elasticsearch");
    group.sample_size(20);

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        "bench".to_string(),
        "http://localhost:9200".to_string(),
        "bench-index".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        Arc::clone(&stats),
    )
    .expect("factory creation failed");

    for &n in &[1_000usize, 10_000, 100_000] {
        let data = generators::gen_production_mixed(n, 42);
        let mut scanner = Scanner::new(ScanConfig::default());
        let batch = scanner
            .scan_detached(bytes::Bytes::from(data.clone()))
            .expect("bench: scan should not fail");
        let meta = generators::make_metadata();

        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("serialize_batch", n),
            &batch,
            |b, batch| {
                let mut sink = factory.create_sink();
                b.iter(|| {
                    // serialize_batch calls clear() at the start of every call, so
                    // the buffer does not grow unboundedly across iterations.
                    sink.serialize_batch(batch, &meta)
                        .expect("bench: serialize should not fail");
                    std::hint::black_box(&sink);
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
    bench_scanner,
    bench_cri,
    bench_transform,
    bench_compress,
    bench_output,
    bench_end_to_end,
    bench_elasticsearch_serialize,
);
criterion_main!(benches);
