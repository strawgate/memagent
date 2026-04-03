//! End-to-end Elasticsearch output throughput bench.
//!
//! Tests the full pipeline with configurable workers, batch size, and compression
//! using the async `ElasticsearchSinkFactory` (reqwest, connection pooling, gzip).
//!
//! Credentials are read from environment variables:
//!
//!   ES_URL or ES_ENDPOINT — base URL (e.g. https://my-cluster.es.us-central1.gcp.elastic.cloud:443)
//!   ES_API_KEY            — Elasticsearch API key (without the "ApiKey " prefix)
//!   ES_INDEX              — target index name (default: logfwd-bench)
//!
//! Usage:
//!   ES_URL=https://... ES_API_KEY=... ./es-throughput [duration_secs] [workers] [batch_lines] [compress: 0|1]
//!
//! Examples:
//!   ./es-throughput 30 1 1000 0     # baseline (single worker, no compress)
//!   ./es-throughput 30 4 1000 0     # 4 workers, no compress
//!   ./es-throughput 30 4 5000 1     # 4 workers, gzip, 5k batch
//!   ./es-throughput 30 1 1000 1     # single worker + gzip only

use std::fmt::Write as FmtWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use logfwd_arrow::scanner::SimdScanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_io::diagnostics::ComponentStats;
use logfwd_output::sink::{Sink, SinkFactory};
use logfwd_output::{BatchMetadata, ElasticsearchSinkFactory};
use logfwd_transform::SqlTransform;
use pprof::ProfilerGuardBuilder;

fn es_endpoint() -> String {
    std::env::var("ES_URL")
        .or_else(|_| std::env::var("ES_ENDPOINT"))
        .expect("ES_URL or ES_ENDPOINT env var required")
}

fn es_api_key() -> String {
    std::env::var("ES_API_KEY").expect("ES_API_KEY env var required")
}

fn es_index() -> String {
    std::env::var("ES_INDEX").unwrap_or_else(|_| "logfwd-bench".to_string())
}

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

fn run_worker(
    worker_id: usize,
    duration: std::time::Duration,
    batch_lines: usize,
    compress: bool,
    total_events: Arc<AtomicU64>,
    total_batches: Arc<AtomicU64>,
    total_errors: Arc<AtomicU64>,
    total_raw_bytes: Arc<AtomicU64>,
    total_wire_bytes: Arc<AtomicU64>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        format!("es-bench-{worker_id}"),
        es_endpoint(),
        es_index(),
        vec![("Authorization".to_string(), format!("ApiKey {}", es_api_key()))],
        compress,
        stats,
    )
    .expect("factory creation failed");
    let mut sink = factory.create().expect("sink creation failed");

    let mut scanner = SimdScanner::new(ScanConfig::default());
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    let meta = BatchMetadata {
        resource_attrs: Arc::new(vec![("service.name".into(), "es-bench".into())]),
        observed_time_ns: 0,
    };

    let line_buf = gen_json_lines(batch_lines);
    let deadline = Instant::now() + duration;

    while Instant::now() < deadline {
        let batch = match scanner.scan(&line_buf) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[worker {worker_id}] scan error: {e}");
                total_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let result = match transform.execute_blocking(batch) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[worker {worker_id}] transform error: {e}");
                total_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let rows = result.num_rows() as u64;
        let raw = rows as usize * 300; // approximate bytes per row

        match rt.block_on(sink.send_batch(&result, &meta)) {
            Ok(_) => {
                total_raw_bytes.fetch_add(raw as u64, Ordering::Relaxed);
                total_wire_bytes.fetch_add(raw as u64, Ordering::Relaxed);
                total_events.fetch_add(rows, Ordering::Relaxed);
                total_batches.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("[worker {worker_id}] send_batch error: {e}");
                total_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn run_scenario(
    label: &str,
    duration_secs: u64,
    workers: usize,
    batch_lines: usize,
    compress: bool,
) {
    println!("\n--- {label} ---");
    println!(
        "  workers={workers}  batch={batch_lines}  compress={}  duration={duration_secs}s",
        if compress { "gzip" } else { "none" }
    );

    let total_events = Arc::new(AtomicU64::new(0));
    let total_batches = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let total_raw_bytes = Arc::new(AtomicU64::new(0));
    let total_wire_bytes = Arc::new(AtomicU64::new(0));
    let duration = std::time::Duration::from_secs(duration_secs);

    let start = Instant::now();
    let mut handles = vec![];
    for i in 0..workers {
        let ev = Arc::clone(&total_events);
        let ba = Arc::clone(&total_batches);
        let er = Arc::clone(&total_errors);
        let rb = Arc::clone(&total_raw_bytes);
        let wb = Arc::clone(&total_wire_bytes);
        handles.push(std::thread::spawn(move || {
            run_worker(i, duration, batch_lines, compress, ev, ba, er, rb, wb);
        }));
    }
    for h in handles {
        h.join().ok();
    }
    let elapsed = start.elapsed().as_secs_f64();

    let events = total_events.load(Ordering::Relaxed);
    let batches = total_batches.load(Ordering::Relaxed);
    let errors = total_errors.load(Ordering::Relaxed);
    let raw_mb = total_raw_bytes.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0;
    let wire_mb = total_wire_bytes.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0;
    let eps = events as f64 / elapsed;
    let avg_lat_ms = if batches > 0 {
        elapsed * 1000.0 * workers as f64 / batches as f64
    } else {
        0.0
    };
    let ratio = if wire_mb > 0.0 { raw_mb / wire_mb } else { 1.0 };

    println!("  events      : {events:>10}");
    println!("  batches     : {batches:>10}  ({errors} errors)");
    println!(
        "  throughput  : {eps:>10.0} evt/s  ({:.1}% of 1M)",
        eps / 10_000.0
    );
    println!("  avg lat/batch: {avg_lat_ms:>9.1} ms  (per worker)");
    println!(
        "  raw payload : {raw_mb:>9.1} MB  ({:.1} MB/s)",
        raw_mb / elapsed
    );
    println!("  wire bytes  : {wire_mb:>9.1} MB  (ratio {ratio:.1}x)",);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let duration_secs: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let workers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let batch_lines: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let compress_flag: u8 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(255);

    let endpoint = es_endpoint();
    let index = es_index();

    println!("=== Elasticsearch Output Throughput Bench ===");
    println!("  endpoint : {endpoint}");
    println!("  index    : {index}");
    println!("  sink     : async (reqwest, connection pooling, gzip)");

    let guard = ProfilerGuardBuilder::default()
        .frequency(997)
        .build()
        .expect("pprof guard");

    if workers == 0 {
        // Run a progression of scenarios
        run_scenario(
            "Baseline (1 worker, 1k batch, no compress)",
            duration_secs,
            1,
            1_000,
            false,
        );
        run_scenario(
            "Larger batch (1 worker, 5k batch, no compress)",
            duration_secs,
            1,
            5_000,
            false,
        );
        run_scenario(
            "Gzip (1 worker, 1k batch, gzip)",
            duration_secs,
            1,
            1_000,
            true,
        );
        run_scenario(
            "Gzip + large batch (1 worker, 5k batch, gzip)",
            duration_secs,
            1,
            5_000,
            true,
        );
        run_scenario(
            "4 workers, 1k batch, no compress",
            duration_secs,
            4,
            1_000,
            false,
        );
        run_scenario("4 workers, 5k batch, gzip", duration_secs, 4, 5_000, true);
    } else {
        let compress = compress_flag != 0;
        let bl = if batch_lines == 0 { 1_000 } else { batch_lines };
        run_scenario("Custom", duration_secs, workers, bl, compress);
    }

    // Write flamegraph from the last scenario
    match guard.report().build() {
        Ok(report) => match std::fs::File::create("es-flamegraph.svg") {
            Ok(file) => {
                report.flamegraph(file).ok();
                println!("\n  flamegraph : es-flamegraph.svg");
            }
            Err(e) => eprintln!("flamegraph write failed: {e}"),
        },
        Err(e) => eprintln!("pprof report failed: {e}"),
    }
}
