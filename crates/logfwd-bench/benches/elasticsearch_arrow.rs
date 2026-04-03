//! Benchmarks for Elasticsearch ES|QL with Arrow IPC format.
//!
//! These benchmarks measure the throughput of querying Elasticsearch using ES|QL
//! and receiving results in Arrow IPC format vs JSON format.
//!
//! ## Prerequisites
//! 1. Start Elasticsearch: `cd examples/elasticsearch && docker-compose up -d`
//! 2. Wait for cluster to be healthy: `curl http://localhost:9200/_cluster/health`
//! 3. Run benchmarks: `cargo bench --bench elasticsearch_arrow -- --ignored`
//!
//! ## Metrics
//! - Query latency (ms)
//! - Throughput (rows/sec, MB/sec)
//! - Arrow IPC vs JSON comparison
//! - Network transfer size
//!
//! Run with: cargo bench -p logfwd-bench --bench elasticsearch_arrow -- --ignored

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_io::diagnostics::ComponentStats;
use logfwd_output::{BatchMetadata, ElasticsearchSink, OutputSink};

const ES_ENDPOINT: &str = "http://localhost:9200";
const BENCH_INDEX: &str = "logfwd-bench-arrow";

/// Check if Elasticsearch is running and accessible.
fn check_elasticsearch_available() -> bool {
    ureq::get(&format!("{}/_cluster/health", ES_ENDPOINT))
        .call()
        .is_ok()
}

/// Generate realistic log data for benchmarking.
fn generate_log_batch(num_rows: usize) -> RecordBatch {
    let levels = ["INFO", "ERROR", "DEBUG", "WARN"];
    let paths = [
        "/api/users",
        "/api/orders",
        "/api/health",
        "/api/auth/login",
        "/api/metrics",
    ];

    let mut level_vec = Vec::with_capacity(num_rows);
    let mut message_vec = Vec::with_capacity(num_rows);
    let mut status_vec = Vec::with_capacity(num_rows);
    let mut duration_vec = Vec::with_capacity(num_rows);
    let mut timestamp_vec = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        level_vec.push(levels[i % levels.len()]);

        let msg = format!(
            "GET {} HTTP/1.1 user_id=usr_{:06} request_id=req-{:08x}",
            paths[i % paths.len()],
            i % 100_000,
            i,
        );
        message_vec.push(msg);

        status_vec.push([200, 200, 200, 500, 404][i % 5] as i64);
        duration_vec.push((i % 500 + 1) as f64);

        let ts = format!(
            "2024-01-15T10:{:02}:{:02}.{:03}Z",
            (i / 60) % 60,
            i % 60,
            i % 1000
        );
        timestamp_vec.push(ts);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_str", DataType::Utf8, false),
        Field::new("level_str", DataType::Utf8, false),
        Field::new("message_str", DataType::Utf8, false),
        Field::new("status_int", DataType::Int64, false),
        Field::new("duration_ms_float", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(timestamp_vec)),
            Arc::new(StringArray::from(level_vec)),
            Arc::new(StringArray::from(message_vec)),
            Arc::new(Int64Array::from(status_vec)),
            Arc::new(Float64Array::from(duration_vec)),
        ],
    )
    .expect("batch creation failed")
}

/// Index a batch of data into Elasticsearch.
fn index_batch(sink: &mut ElasticsearchSink, batch: &RecordBatch) -> Result<(), std::io::Error> {
    let metadata = BatchMetadata {
        resource_attrs: Arc::new(vec![]),
        observed_time_ns: 0,
    };
    sink.send_batch(batch, &metadata)
}

/// Delete and recreate the benchmark index.
fn reset_index() {
    let _ = ureq::delete(&format!("{}/{}", ES_ENDPOINT, BENCH_INDEX)).call();
    std::thread::sleep(Duration::from_millis(500));
}

/// Benchmark Arrow IPC query performance.
fn bench_arrow_query(
    sink: &ElasticsearchSink,
    query: &str,
    iterations: usize,
) -> (f64, usize, f64) {
    let mut total_rows = 0usize;
    let mut total_duration = Duration::ZERO;

    for _ in 0..iterations {
        let start = Instant::now();
        match sink.query_arrow(query) {
            Ok(batches) => {
                let elapsed = start.elapsed();
                total_duration += elapsed;
                total_rows += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            }
            Err(e) => {
                eprintln!("Arrow query failed: {}", e);
                return (0.0, 0, 0.0);
            }
        }
    }

    let avg_latency_ms = total_duration.as_secs_f64() * 1000.0 / iterations as f64;
    let throughput_rows_per_sec = (total_rows as f64) / total_duration.as_secs_f64();

    (avg_latency_ms, total_rows, throughput_rows_per_sec)
}

/// Benchmark JSON query performance for comparison.
fn bench_json_query(endpoint: &str, query: &str, iterations: usize) -> (f64, usize, f64) {
    let query_body = serde_json::json!({
        "query": query
    });
    let query_bytes = serde_json::to_vec(&query_body).unwrap();

    let mut total_rows = 0usize;
    let mut total_duration = Duration::ZERO;

    for _ in 0..iterations {
        let start = Instant::now();
        match ureq::post(&format!("{}/_query", endpoint))
            .header("Content-Type", "application/json")
            .send(&query_bytes)
        {
            Ok(response) => {
                let elapsed = start.elapsed();
                total_duration += elapsed;

                if let Ok(body) = response.into_body().read_to_vec() {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&body) {
                        if let Some(values) = json.get("values").and_then(|v| v.as_array()) {
                            total_rows += values.len();
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("JSON query failed: {}", e);
                return (0.0, 0, 0.0);
            }
        }
    }

    let avg_latency_ms = total_duration.as_secs_f64() * 1000.0 / iterations as f64;
    let throughput_rows_per_sec = (total_rows as f64) / total_duration.as_secs_f64();

    (avg_latency_ms, total_rows, throughput_rows_per_sec)
}

fn main() {
    println!("=== Elasticsearch ES|QL Arrow IPC Benchmarks ===\n");

    if !check_elasticsearch_available() {
        eprintln!("ERROR: Elasticsearch not available at {}", ES_ENDPOINT);
        eprintln!("Please start Elasticsearch:");
        eprintln!("  cd examples/elasticsearch");
        eprintln!("  docker-compose up -d");
        std::process::exit(1);
    }

    println!("✓ Elasticsearch connected at {}\n", ES_ENDPOINT);

    // Setup
    println!("Setting up test index...");
    reset_index();

    let stats = Arc::new(ComponentStats::default());
    let mut sink = ElasticsearchSink::new(
        "bench_arrow".to_string(),
        ES_ENDPOINT.to_string(),
        BENCH_INDEX.to_string(),
        vec![],
        stats.clone(),
    );

    // Index test data
    let batch_sizes = [1_000, 10_000, 50_000];

    for &size in &batch_sizes {
        println!("\n--- Indexing {} documents ---", size);
        let batch = generate_log_batch(size);

        let start = Instant::now();
        if let Err(e) = index_batch(&mut sink, &batch) {
            eprintln!("Failed to index batch: {}", e);
            continue;
        }
        let index_duration = start.elapsed();

        println!(
            "✓ Indexed in {:.2} ms",
            index_duration.as_secs_f64() * 1000.0
        );
        println!(
            "  Throughput: {:.0} docs/sec",
            size as f64 / index_duration.as_secs_f64()
        );

        // Wait for indexing to complete
        std::thread::sleep(Duration::from_millis(1000));
    }

    // Benchmarks
    println!("\n=== Query Benchmarks ===\n");

    let test_cases = vec![
        ("Full scan", format!("FROM {} | LIMIT 10000", BENCH_INDEX)),
        (
            "Filtered",
            format!(
                r#"FROM {} | WHERE level_str == "ERROR" | LIMIT 10000"#,
                BENCH_INDEX
            ),
        ),
        (
            "Projection",
            format!(
                "FROM {} | KEEP level_str, status_int | LIMIT 10000",
                BENCH_INDEX
            ),
        ),
        (
            "Aggregation",
            format!("FROM {} | STATS count() BY level_str", BENCH_INDEX),
        ),
    ];

    let iterations = 10;

    for (name, query) in test_cases {
        println!("--- {} ---", name);

        // Benchmark Arrow IPC
        print!("  Arrow IPC: ");
        std::io::stdout().flush().ok();
        let (arrow_latency, arrow_rows, arrow_throughput) =
            bench_arrow_query(&sink, &query, iterations);
        println!(
            "{:.2} ms avg, {:.0} rows/sec ({} total rows)",
            arrow_latency, arrow_throughput, arrow_rows
        );

        // Benchmark JSON
        print!("  JSON:      ");
        std::io::stdout().flush().ok();
        let (json_latency, json_rows, json_throughput) =
            bench_json_query(ES_ENDPOINT, &query, iterations);
        println!(
            "{:.2} ms avg, {:.0} rows/sec ({} total rows)",
            json_latency, json_throughput, json_rows
        );

        if arrow_throughput > 0.0 && json_throughput > 0.0 {
            let speedup = arrow_throughput / json_throughput;
            println!("  Speedup: {:.2}x (Arrow vs JSON)", speedup);
        }

        println!();
    }

    // Cleanup
    println!("Cleaning up...");
    reset_index();

    println!("\n=== Benchmark Complete ===");
}
