//! Side-by-side benchmark: current scanner pipeline vs raw-first UDF pipeline
//!
//! Path A (current): bytes -> scanner -> typed columns -> DataFusion SQL -> output
//! Path B (raw-first): bytes -> body Utf8 column -> DataFusion SQL with json UDFs -> output

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::*;
use logfwd_core::scan_config::ScanConfig;
use logfwd_transform::udf::{JsonExtractMode, JsonExtractUdf};
use std::sync::Arc;
use std::time::Instant;

// =========================================================================
// Test data generation
// =========================================================================

fn generate_ndjson(num_rows: usize, num_fields: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(num_rows * num_fields * 25);
    for row in 0..num_rows {
        buf.push(b'{');
        for f in 0..num_fields {
            if f > 0 {
                buf.push(b',');
            }
            match f % 3 {
                0 => buf.extend_from_slice(format!("\"f{}\":{}", f, row * 1000 + f).as_bytes()),
                1 => buf.extend_from_slice(
                    format!("\"f{}\":{:.2}", f, row as f64 * 0.1 + f as f64 * 0.01).as_bytes(),
                ),
                _ => buf.extend_from_slice(
                    format!("\"f{}\":\"val_{}_{}\"", f, f, row % 100).as_bytes(),
                ),
            }
        }
        buf.extend_from_slice(b"}\n");
    }
    buf
}

// =========================================================================
// Path A: Current pipeline (scanner -> typed columns -> SQL)
// =========================================================================

async fn path_a_extraction(json_data: &[u8], num_fields: usize) -> RecordBatch {
    // 1. Scanner -> typed columns
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
        row_predicate: None,
    };
    let mut scanner = logfwd_arrow::Scanner::new(config);
    let batch = scanner
        .scan(bytes::Bytes::copy_from_slice(json_data))
        .unwrap();

    // 2. Register with DataFusion, run SQL
    let schema = batch.schema();
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("logs", Arc::new(table)).unwrap();

    // Build SQL selecting specific fields (bare names — no type suffix)
    let select_cols: Vec<String> = (0..num_fields.min(10)).map(|f| format!("f{f}")).collect();
    let sql = format!(
        "SELECT {} FROM logs WHERE f0 > 5000",
        select_cols.join(", ")
    );

    let df = ctx.sql(&sql).await.unwrap();
    df.collect().await.unwrap().into_iter().next().unwrap()
}

async fn path_a_passthrough(json_data: &[u8]) -> RecordBatch {
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
        row_predicate: None,
    };
    let mut scanner = logfwd_arrow::Scanner::new(config);
    let batch = scanner
        .scan(bytes::Bytes::copy_from_slice(json_data))
        .unwrap();

    let schema = batch.schema();
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("logs", Arc::new(table)).unwrap();

    let df = ctx.sql("SELECT * FROM logs").await.unwrap();
    df.collect().await.unwrap().into_iter().next().unwrap()
}

// =========================================================================
// Path B: Raw-first (json UDFs)
// =========================================================================

fn make_raw_batch(json_data: &[u8]) -> RecordBatch {
    let lines: Vec<&str> = std::str::from_utf8(json_data)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(lines)) as ArrayRef]).unwrap()
}

fn make_ctx_with_udfs() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Str)));
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Int)));
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new(JsonExtractMode::Float)));
    ctx
}

async fn path_b_extraction(json_data: &[u8], num_fields: usize) -> RecordBatch {
    let batch = make_raw_batch(json_data);
    let schema = batch.schema();
    let ctx = make_ctx_with_udfs();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("logs", Arc::new(table)).unwrap();

    // Build SQL extracting fields with json UDFs
    let select_cols: Vec<String> = (0..num_fields.min(10))
        .map(|f| match f % 3 {
            0 => format!("json_int(body, 'f{f}') as f{f}"),
            1 => format!("json_float(body, 'f{f}') as f{f}"),
            _ => format!("json(body, 'f{f}') as f{f}"),
        })
        .collect();
    let sql = format!(
        "SELECT {} FROM logs WHERE json_int(body, 'f0') > 5000",
        select_cols.join(", ")
    );

    let df = ctx.sql(&sql).await.unwrap();
    df.collect().await.unwrap().into_iter().next().unwrap()
}

async fn path_b_passthrough(json_data: &[u8]) -> RecordBatch {
    let batch = make_raw_batch(json_data);
    let schema = batch.schema();
    let ctx = make_ctx_with_udfs();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("logs", Arc::new(table)).unwrap();

    let df = ctx.sql("SELECT body FROM logs").await.unwrap();
    df.collect().await.unwrap().into_iter().next().unwrap()
}

// =========================================================================
// Benchmark harness
// =========================================================================

async fn bench_scenario(label: &str, num_rows: usize, num_fields: usize, iters: usize) {
    let json_data = generate_ndjson(num_rows, num_fields);
    let data_mb = json_data.len() as f64 / (1024.0 * 1024.0);

    println!("  --- {label} ({num_rows} rows x {num_fields} fields, {data_mb:.1} MB) ---");

    // Path A: extraction
    {
        let _ = path_a_extraction(&json_data, num_fields).await;
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(path_a_extraction(&json_data, num_fields).await);
        }
        let avg = start.elapsed().as_secs_f64() / iters as f64 * 1000.0;
        let tp = data_mb / (start.elapsed().as_secs_f64() / iters as f64);
        println!(
            "    A (scanner+SQL) extract:   {:7.1}ms  ({:5.0} MB/s)",
            avg, tp
        );
    }

    // Path B: extraction
    {
        let _ = path_b_extraction(&json_data, num_fields).await;
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(path_b_extraction(&json_data, num_fields).await);
        }
        let avg = start.elapsed().as_secs_f64() / iters as f64 * 1000.0;
        let tp = data_mb / (start.elapsed().as_secs_f64() / iters as f64);
        println!(
            "    B (raw+json UDF) extract:  {:7.1}ms  ({:5.0} MB/s)",
            avg, tp
        );
    }

    // Path A: passthrough
    {
        let _ = path_a_passthrough(&json_data).await;
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(path_a_passthrough(&json_data).await);
        }
        let avg = start.elapsed().as_secs_f64() / iters as f64 * 1000.0;
        let tp = data_mb / (start.elapsed().as_secs_f64() / iters as f64);
        println!(
            "    A (scanner) passthrough:    {:7.1}ms  ({:5.0} MB/s)",
            avg, tp
        );
    }

    // Path B: passthrough
    {
        let _ = path_b_passthrough(&json_data).await;
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(path_b_passthrough(&json_data).await);
        }
        let avg = start.elapsed().as_secs_f64() / iters as f64 * 1000.0;
        let tp = data_mb / (start.elapsed().as_secs_f64() / iters as f64);
        println!(
            "    B (raw) passthrough:        {:7.1}ms  ({:5.0} MB/s)",
            avg, tp
        );
    }

    println!();
}

#[tokio::test]
#[ignore = "benchmark: run with `cargo nextest run -- --ignored side_by_side_benchmark`"]
async fn side_by_side_benchmark() {
    let iters = 5;
    println!("\n  ========== Raw-First vs Current Pipeline ==========\n");

    bench_scenario("Small/narrow", 10_000, 10, iters).await;
    bench_scenario("Medium/medium", 10_000, 50, iters).await;
    bench_scenario("Medium/wide", 10_000, 100, iters).await;
    bench_scenario("Large/narrow", 50_000, 10, iters).await;
    bench_scenario("Large/medium", 50_000, 50, iters).await;
}
