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
//! Run with: cargo bench -p ffwd-bench --bench elasticsearch_arrow -- --ignored

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use ffwd_output::sink::{Sink, SinkFactory};
use ffwd_output::{BatchMetadata, ElasticsearchRequestMode, ElasticsearchSinkFactory};
use ffwd_types::diagnostics::ComponentStats;
use tokio::runtime::Runtime;

const ES_ENDPOINT: &str = "http://localhost:9200";
const BENCH_INDEX: &str = "ffwd-bench-arrow";

/// Check if Elasticsearch is running and accessible.
fn check_elasticsearch_available(rt: &Runtime) -> bool {
    rt.block_on(async { reqwest::get(format!("{}/_cluster/health", ES_ENDPOINT)).await })
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
fn index_batch(
    rt: &Runtime,
    sink: &mut Box<dyn Sink>,
    batch: &RecordBatch,
) -> Result<(), std::io::Error> {
    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };
    let result = rt.block_on(sink.send_batch(batch, &metadata));
    match result {
        ffwd_output::SendResult::Ok => Ok(()),
        ffwd_output::SendResult::Rejected(reason) => {
            Err(std::io::Error::other(format!("batch rejected: {reason}")))
        }
        ffwd_output::SendResult::RetryAfter(delay) => Err(std::io::Error::other(format!(
            "batch not accepted: retry after {}s",
            delay.as_secs()
        ))),
        ffwd_output::SendResult::IoError(error) => Err(std::io::Error::other(format!(
            "batch not accepted: io error: {error}"
        ))),
        other => Err(std::io::Error::other(format!(
            "batch not accepted: unexpected send result: {other:?}"
        ))),
    }
}

/// Delete and recreate the benchmark index.
fn reset_index(rt: &Runtime) {
    let _ = rt.block_on(async {
        reqwest::Client::new()
            .delete(format!("{}/{}", ES_ENDPOINT, BENCH_INDEX))
            .send()
            .await
    });
    std::thread::sleep(Duration::from_millis(500));
}

/// Benchmark Arrow IPC query performance.
fn bench_arrow_query(rt: &Runtime, query: &str, iterations: usize) -> (f64, usize, f64) {
    let client = reqwest::Client::new();
    let query_body = serde_json::json!({ "query": query });
    let query_bytes = serde_json::to_vec(&query_body).expect("serialize query");

    let mut total_rows = 0usize;
    let mut total_duration = Duration::ZERO;

    for _ in 0..iterations {
        let start = Instant::now();
        match rt.block_on(async {
            client
                .post(format!("{}/_query", ES_ENDPOINT))
                .header("Content-Type", "application/json")
                .header("Accept", "application/vnd.elasticsearch+arrow+stream")
                .body(query_bytes.clone())
                .send()
                .await
        }) {
            Ok(response) => {
                if let Ok(body) = rt.block_on(response.bytes()) {
                    let cursor = std::io::Cursor::new(body);
                    if let Ok(reader) = StreamReader::try_new(cursor, None) {
                        for batch_result in reader {
                            if let Ok(batch) = batch_result {
                                total_rows += batch.num_rows();
                            }
                        }
                    }
                }
                // Stop clock after full response read + decode
                total_duration += start.elapsed();
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
fn bench_json_query(
    rt: &Runtime,
    endpoint: &str,
    query: &str,
    iterations: usize,
) -> (f64, usize, f64) {
    let client = reqwest::Client::new();
    let query_body = serde_json::json!({
        "query": query
    });
    let query_bytes = serde_json::to_vec(&query_body).expect("serialize query");

    let mut total_rows = 0usize;
    let mut total_duration = Duration::ZERO;

    for _ in 0..iterations {
        let start = Instant::now();
        match rt.block_on(async {
            client
                .post(format!("{}/_query", endpoint))
                .header("Content-Type", "application/json")
                .body(query_bytes.clone())
                .send()
                .await
        }) {
            Ok(response) => {
                let elapsed = start.elapsed();
                total_duration += elapsed;

                if let Ok(body) = rt.block_on(response.bytes()) {
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

    let rt = Runtime::new().expect("failed to create tokio runtime");

    if !check_elasticsearch_available(&rt) {
        eprintln!(
            "WARNING: Elasticsearch not available at {}. Skipping bench.",
            ES_ENDPOINT
        );
        return;
    }

    println!("✓ Elasticsearch connected at {}\n", ES_ENDPOINT);

    // Setup
    println!("Setting up test index...");
    reset_index(&rt);

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        "bench_arrow".to_string(),
        ES_ENDPOINT.to_string(),
        BENCH_INDEX.to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        stats,
    )
    .expect("failed to create sink factory");
    let mut sink = factory.create().expect("failed to create sink");

    // Index test data
    let batch_sizes = [1_000, 10_000, 50_000];

    for &size in &batch_sizes {
        println!("\n--- Indexing {} documents ---", size);
        let batch = generate_log_batch(size);

        let start = Instant::now();
        if let Err(e) = index_batch(&rt, &mut sink, &batch) {
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
            bench_arrow_query(&rt, &query, iterations);
        println!(
            "{:.2} ms avg, {:.0} rows/sec ({} total rows)",
            arrow_latency, arrow_throughput, arrow_rows
        );

        // Benchmark JSON
        print!("  JSON:      ");
        std::io::stdout().flush().ok();
        let (json_latency, json_rows, json_throughput) =
            bench_json_query(&rt, ES_ENDPOINT, &query, iterations);
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
    reset_index(&rt);

    println!("\n=== Benchmark Complete ===");
}
