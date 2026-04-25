//! Criterion benchmark: DataFusion SQL transform per-batch overhead decomposition.
//!
//! Profiles each phase of `SqlTransform::execute()`:
//!   1. SessionContext creation + UDF registration (first batch only)
//!   2. MemTable creation + table swap (deregister/register)
//!   3. SQL parse + logical plan + optimization (`ctx.sql()`)
//!   4. Physical execution + collect (`df.collect()`)
//!
//! Run with:
//!   cargo bench -p ffwd-bench --bench transform_overhead
//!   RUSTFLAGS="-C target-cpu=native" cargo bench -p ffwd-bench --bench transform_overhead

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

use ffwd_arrow::scanner::Scanner;
use ffwd_bench::generators;
use ffwd_core::scan_config::ScanConfig;
use ffwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// Phase decomposition: measure individual DataFusion per-batch costs
// ---------------------------------------------------------------------------

/// Measure the cost of creating a fresh SessionContext (no UDFs).
fn bench_context_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/context_creation");
    group.sample_size(100);

    group.bench_function("session_context_new", |b| {
        b.iter(|| {
            let ctx = SessionContext::new();
            std::hint::black_box(&ctx);
        });
    });

    group.finish();
}

/// Measure MemTable creation + table swap cost at different batch sizes.
fn bench_table_swap(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/table_swap");
    group.sample_size(50);

    for &n in &[1_000usize, 10_000, 100_000] {
        let batch = make_typed_batch(n);
        let ctx = SessionContext::new();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("memtable_create", n),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let schema = batch.schema();
                    let table =
                        MemTable::try_new(schema, vec![vec![batch.clone()]]).expect("memtable");
                    std::hint::black_box(&table);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("deregister_register", n),
            &batch,
            |b, batch| {
                // Pre-register so deregister has something to remove.
                let schema = batch.schema();
                let table = MemTable::try_new(schema, vec![vec![batch.clone()]]).expect("memtable");
                ctx.register_table("logs", Arc::new(table)).ok();

                b.iter(|| {
                    let schema = batch.schema();
                    let table =
                        MemTable::try_new(schema, vec![vec![batch.clone()]]).expect("memtable");
                    let _ = ctx.deregister_table("logs");
                    ctx.register_table("logs", Arc::new(table))
                        .expect("register");
                });
            },
        );
    }

    group.finish();
}

/// Measure SQL parse + plan + optimize cost (`ctx.sql()`) for various query types.
///
/// This isolates the planning overhead from execution. We call `ctx.sql()` but
/// do NOT call `df.collect()`.
fn bench_plan_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/plan_cost");
    group.sample_size(50);

    let batch = make_scanned_batch(10_000);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let queries: &[(&str, &str)] = &[
        ("select_star", "SELECT * FROM logs"),
        ("projection_3col", "SELECT level, message, status FROM logs"),
        ("where_eq", "SELECT * FROM logs WHERE level = 'ERROR'"),
        (
            "where_compound",
            "SELECT * FROM logs WHERE level = 'ERROR' AND status = '500'",
        ),
        (
            "group_by_count",
            "SELECT level, COUNT(*) as cnt FROM logs GROUP BY level",
        ),
        (
            "group_by_avg",
            "SELECT level, AVG(duration_ms) as avg_dur FROM logs GROUP BY level",
        ),
        (
            "order_limit",
            "SELECT * FROM logs ORDER BY duration_ms DESC LIMIT 100",
        ),
        (
            "like_filter",
            "SELECT * FROM logs WHERE message LIKE '%users%'",
        ),
        (
            "case_expr",
            "SELECT *, CASE WHEN duration_ms > 200 THEN 'slow' ELSE 'fast' END AS speed FROM logs",
        ),
    ];

    for &(name, sql) in queries {
        // Set up context with table registered.
        let ctx = SessionContext::new();
        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch.clone()]]).expect("memtable");
        ctx.register_table("logs", Arc::new(table))
            .expect("register");

        group.bench_function(BenchmarkId::new("plan", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let df = ctx.sql(sql).await.expect("sql");
                    std::hint::black_box(df.schema());
                });
            });
        });
    }

    group.finish();
}

/// Measure full execution cost (`ctx.sql()` + `df.collect()`) for various query types.
fn bench_execute_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/execute_cost");
    group.sample_size(30);

    let batch = make_scanned_batch(10_000);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let queries: &[(&str, &str)] = &[
        ("select_star", "SELECT * FROM logs"),
        ("projection_3col", "SELECT level, message, status FROM logs"),
        ("where_eq", "SELECT * FROM logs WHERE level = 'ERROR'"),
        (
            "group_by_count",
            "SELECT level, COUNT(*) as cnt FROM logs GROUP BY level",
        ),
        (
            "order_limit",
            "SELECT * FROM logs ORDER BY duration_ms DESC LIMIT 100",
        ),
    ];

    for &(name, sql) in queries {
        let ctx = SessionContext::new();
        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch.clone()]]).expect("memtable");
        ctx.register_table("logs", Arc::new(table))
            .expect("register");

        group.bench_function(BenchmarkId::new("execute", name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let df = ctx.sql(sql).await.expect("sql");
                    let batches = df.collect().await.expect("collect");
                    std::hint::black_box(&batches);
                });
            });
        });
    }

    group.finish();
}

/// Compare full SqlTransform::execute_blocking with and without context reuse.
///
/// "steady_state" — context already created (normal hot path).
/// "cold_start" — force context recreation each iteration.
fn bench_context_reuse(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/context_reuse");
    group.sample_size(20);

    let batch = make_scanned_batch(10_000);

    let queries: &[(&str, &str)] = &[
        ("select_star", "SELECT * FROM logs"),
        ("where_eq", "SELECT * FROM logs WHERE level = 'ERROR'"),
        (
            "regexp_extract",
            "SELECT regexp_extract(message, '(GET|POST) (\\S+)', 1) AS method FROM logs",
        ),
    ];

    for &(name, sql) in queries {
        // Steady state: context created once, reused across iterations.
        group.bench_function(BenchmarkId::new("steady_state", name), |b| {
            let mut transform = SqlTransform::new(sql).expect("sql");
            // Warm up: create context + first execution.
            transform.execute_blocking(batch.clone()).expect("warmup");

            b.iter(|| {
                std::hint::black_box(transform.execute_blocking(batch.clone()).expect("execute"));
            });
        });

        // Cold start: force context recreation each iteration by mutating schema.
        group.bench_function(BenchmarkId::new("cold_start", name), |b| {
            b.iter(|| {
                let mut transform = SqlTransform::new(sql).expect("sql");
                std::hint::black_box(transform.execute_blocking(batch.clone()).expect("execute"));
            });
        });
    }

    group.finish();
}

/// Measure how batch size affects per-batch overhead vs actual work.
fn bench_batch_size_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/batch_size_scaling");
    group.sample_size(20);

    for &n in &[100usize, 1_000, 10_000, 100_000] {
        let batch = make_scanned_batch(n);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("select_star", n), &batch, |b, batch| {
            let mut transform = SqlTransform::new("SELECT * FROM logs").expect("sql");
            transform.execute_blocking(batch.clone()).expect("warmup");
            b.iter(|| {
                std::hint::black_box(transform.execute_blocking(batch.clone()).expect("execute"));
            });
        });

        group.bench_with_input(BenchmarkId::new("where_filter", n), &batch, |b, batch| {
            let mut transform =
                SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").expect("sql");
            transform.execute_blocking(batch.clone()).expect("warmup");
            b.iter(|| {
                std::hint::black_box(transform.execute_blocking(batch.clone()).expect("execute"));
            });
        });

        group.bench_with_input(BenchmarkId::new("group_by", n), &batch, |b, batch| {
            let mut transform =
                SqlTransform::new("SELECT level, COUNT(*) as cnt FROM logs GROUP BY level")
                    .expect("sql");
            transform.execute_blocking(batch.clone()).expect("warmup");
            b.iter(|| {
                std::hint::black_box(transform.execute_blocking(batch.clone()).expect("execute"));
            });
        });
    }

    group.finish();
}

/// Measure query complexity scaling: simple → complex queries on the same data.
fn bench_query_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("transform_overhead/query_complexity");
    group.sample_size(20);

    let n = 10_000;
    let batch = make_scanned_batch(n);
    group.throughput(Throughput::Elements(n as u64));

    let queries: &[(&str, &str)] = &[
        ("01_passthrough", "SELECT * FROM logs"),
        ("02_projection", "SELECT level, message, status FROM logs"),
        ("03_except", "SELECT * EXCEPT (request_id) FROM logs"),
        ("04_where_eq", "SELECT * FROM logs WHERE level = 'ERROR'"),
        (
            "05_where_in",
            "SELECT * FROM logs WHERE level IN ('ERROR', 'WARN')",
        ),
        (
            "06_where_compound",
            "SELECT * FROM logs WHERE level = 'ERROR' AND status = '500'",
        ),
        (
            "07_computed_col",
            "SELECT *, CASE WHEN duration_ms > 200 THEN 'slow' ELSE 'fast' END AS speed FROM logs",
        ),
        (
            "08_group_by",
            "SELECT level, COUNT(*) as cnt FROM logs GROUP BY level",
        ),
        (
            "09_group_by_agg",
            "SELECT level, COUNT(*) as cnt, AVG(duration_ms) as avg_dur FROM logs GROUP BY level",
        ),
        (
            "10_order_limit",
            "SELECT * FROM logs ORDER BY duration_ms DESC LIMIT 100",
        ),
        ("11_like", "SELECT * FROM logs WHERE message LIKE '%users%'"),
        (
            "12_regexp_extract",
            "SELECT regexp_extract(message, '(GET|POST) (\\S+)', 1) AS method FROM logs",
        ),
        (
            "13_grok",
            "SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{WORD:proto}') AS parsed FROM logs",
        ),
    ];

    for &(name, sql) in queries {
        group.bench_function(name, |b| {
            let mut transform = SqlTransform::new(sql).expect("sql");
            transform.execute_blocking(batch.clone()).expect("warmup");
            b.iter(|| {
                std::hint::black_box(transform.execute_blocking(batch.clone()).expect("execute"));
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a simple typed batch (level: Utf8, message: Utf8, status: Utf8, duration_ms: Int64).
fn make_typed_batch(n: usize) -> RecordBatch {
    let levels = ["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
    let methods = ["GET", "POST", "PUT", "DELETE"];
    let paths = ["/api/users", "/api/orders", "/api/health", "/api/auth"];
    let statuses = ["200", "201", "400", "404", "500"];

    let level: Vec<&str> = (0..n).map(|i| levels[i % levels.len()]).collect();
    let message: Vec<String> = (0..n)
        .map(|i| {
            format!(
                "{} {} HTTP/1.1",
                methods[i % methods.len()],
                paths[i % paths.len()]
            )
        })
        .collect();
    let status: Vec<&str> = (0..n).map(|i| statuses[i % statuses.len()]).collect();
    let duration_ms: Vec<i64> = (0..n).map(|i| 1 + (i as i64 * 13) % 500).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("duration_ms", DataType::Int64, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(level)) as ArrayRef,
            Arc::new(StringArray::from(
                message.iter().map(String::as_str).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(status)) as ArrayRef,
            Arc::new(Int64Array::from(duration_ms)) as ArrayRef,
        ],
    )
    .expect("batch")
}

/// Create a batch using the real scanner pipeline (production-realistic data).
fn make_scanned_batch(n: usize) -> RecordBatch {
    let data = generators::gen_production_mixed(n, 42);
    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan_detached(bytes::Bytes::from(data))
        .expect("scan")
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_context_creation,
    bench_table_swap,
    bench_plan_cost,
    bench_execute_cost,
    bench_context_reuse,
    bench_batch_size_scaling,
    bench_query_complexity,
);
criterion_main!(benches);
