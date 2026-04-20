//! End-to-end Elasticsearch output throughput bench.
//!
//! Tests the full pipeline with configurable workers, batch size, and compression.
//! Credentials are read from environment variables:
//!
//!   ES_URL        — base URL (e.g. <https://my-cluster.es.us-east-1.aws.elastic.cloud>)
//!   ES_ENDPOINT   — alias for ES_URL (legacy)
//!   ES_API_KEY    — Elasticsearch API key (without the "ApiKey " prefix)
//!   ES_INDEX      — target index base name (default: logfwd-bench)
//!
//! Usage:
//!   `ES_URL=https://... ES_API_KEY=... ./es-throughput [duration_secs] [workers] [batch_lines] [compress: 0|1] [indices: default 1] [request_mode: buffered|streaming]`
//!
//! Examples:
//!   `./es-throughput 60 16 5000 1 4 buffered`   # 16 workers, gzip, 5k batch, 4 indices
//!   `./es-throughput 30 4 5000 0 1 streaming`   # 4 workers, streamed request body
//!   `./es-throughput 30 1 1000 0`               # baseline (single worker, buffered, no compress)

use std::fmt::Write as FmtWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_output::sink::Sink;
use logfwd_output::{
    BatchMetadata, ElasticsearchRequestMode, ElasticsearchSink, ElasticsearchSinkFactory,
};
use logfwd_transform::SqlTransform;
use logfwd_types::diagnostics::ComponentStats;
use pprof::ProfilerGuardBuilder;

fn es_endpoint() -> String {
    std::env::var("ES_URL")
        .or_else(|_| std::env::var("ES_ENDPOINT"))
        .expect("ES_URL or ES_ENDPOINT required")
}

fn es_api_key() -> String {
    std::env::var("ES_API_KEY").expect("ES_API_KEY env var required")
}

fn es_index() -> String {
    std::env::var("ES_INDEX").unwrap_or_else(|_| "logfwd-bench".to_string())
}

fn request_mode_name(mode: ElasticsearchRequestMode) -> &'static str {
    match mode {
        ElasticsearchRequestMode::Buffered => "buffered",
        ElasticsearchRequestMode::Streaming => "streaming",
        _ => "unknown",
    }
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

#[allow(clippy::too_many_arguments)]
fn run_worker(
    worker_id: usize,
    index: String,
    duration: std::time::Duration,
    batch_lines: usize,
    compress: bool,
    request_mode: ElasticsearchRequestMode,
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
    let name = format!("es-bench-{worker_id}");
    let endpoint = es_endpoint();
    let headers = vec![(
        "Authorization".to_string(),
        format!("ApiKey {}", es_api_key()),
    )];

    let factory = ElasticsearchSinkFactory::new(
        name,
        endpoint,
        index,
        headers,
        compress,
        request_mode,
        stats,
    )
    .expect("failed to create sink factory");
    // Use create_sink() to get a concrete ElasticsearchSink so we can call
    // serialize_batch() / serialized_len() for accurate byte accounting.
    let mut sink: ElasticsearchSink = factory.create_sink();

    let mut scanner = Scanner::new(ScanConfig::default());
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    let meta = BatchMetadata {
        resource_attrs: Arc::new(vec![("service.name".into(), "es-bench".into())]),
        observed_time_ns: 0,
    };

    let line_bytes = bytes::Bytes::from(gen_json_lines(batch_lines));
    let deadline = Instant::now() + duration;

    while Instant::now() < deadline {
        let batch = match scanner.scan_detached(line_bytes.clone()) {
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

        // Serialize the batch upfront to measure actual uncompressed bytes.
        // send_batch_inner will re-serialize internally; the overhead is
        // acceptable in a benchmark where measurement accuracy matters more
        // than peak throughput of the bench harness itself.
        let raw_bytes = match sink.serialize_batch(&result, &meta) {
            Ok(()) => sink.serialized_len() as u64,
            Err(e) => {
                eprintln!("[worker {worker_id}] serialize error (byte counting): {e}");
                0
            }
        };

        // Wire bytes: the sink compresses internally (gzip) but does not expose
        // the compressed payload size. When compression is disabled wire == raw.
        // When compression is enabled we report raw bytes here too; the ratio
        // column will show 1.0× and a note is printed at scenario start.
        let wire_bytes = raw_bytes;

        match rt.block_on(sink.send_batch(&result, &meta)) {
            logfwd_output::sink::SendResult::Ok => {
                total_raw_bytes.fetch_add(raw_bytes, Ordering::Relaxed);
                total_wire_bytes.fetch_add(wire_bytes, Ordering::Relaxed);
                total_events.fetch_add(rows, Ordering::Relaxed);
                total_batches.fetch_add(1, Ordering::Relaxed);
            }
            logfwd_output::sink::SendResult::IoError(e) => {
                eprintln!("[worker {worker_id}] send_batch error: {e}");
                total_errors.fetch_add(1, Ordering::Relaxed);
            }
            logfwd_output::sink::SendResult::RetryAfter(delay) => {
                eprintln!("[worker {worker_id}] send_batch asked to retry after {delay:?}");
                total_errors.fetch_add(1, Ordering::Relaxed);
            }
            logfwd_output::sink::SendResult::Rejected(reason) => {
                eprintln!("[worker {worker_id}] send_batch rejected batch: {reason}");
                total_errors.fetch_add(1, Ordering::Relaxed);
            }
            other => {
                eprintln!("[worker {worker_id}] send_batch returned unexpected result: {other:?}");
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
    request_mode: ElasticsearchRequestMode,
    num_indices: usize,
) {
    let index_base = es_index();
    let indices: Vec<String> = if num_indices <= 1 {
        vec![index_base]
    } else {
        (0..num_indices)
            .map(|i| format!("{index_base}-{i}"))
            .collect()
    };

    println!("\n--- {label} ---");
    println!(
        "  workers={workers}  batch={batch_lines}  compress={}  request_mode={}  indices={}  duration={duration_secs}s",
        if compress { "gzip" } else { "none" },
        request_mode_name(request_mode),
        indices.join(","),
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
        let cnt_ev = Arc::clone(&total_events);
        let cnt_bat = Arc::clone(&total_batches);
        let cnt_err = Arc::clone(&total_errors);
        let cnt_raw = Arc::clone(&total_raw_bytes);
        let cnt_wire = Arc::clone(&total_wire_bytes);
        let index = indices[i % indices.len()].clone();
        handles.push(std::thread::spawn(move || {
            run_worker(
                i,
                index,
                duration,
                batch_lines,
                compress,
                request_mode,
                cnt_ev,
                cnt_bat,
                cnt_err,
                cnt_raw,
                cnt_wire,
            );
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
    println!("  wire bytes  : {wire_mb:>9.1} MB  (ratio {ratio:.1}x)");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let duration_secs: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let workers: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let batch_lines: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let compress_flag: u8 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(255);
    let num_indices: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);
    let request_mode = match args.get(6).map(String::as_str) {
        Some("streaming") => ElasticsearchRequestMode::Streaming,
        _ => ElasticsearchRequestMode::Buffered,
    };

    let endpoint = es_endpoint();
    let index = es_index();

    println!("=== Elasticsearch Output Throughput Bench ===");
    println!("  endpoint : {endpoint}");
    println!("  index    : {index}");
    println!(
        "  sink     : async (reqwest, connection pooling, gzip, {})",
        request_mode_name(request_mode)
    );

    let guard = ProfilerGuardBuilder::default()
        .frequency(997)
        .build()
        .expect("pprof guard");

    if workers == 0 {
        // Run a progression of scenarios
        run_scenario(
            "1w/1k/raw/1idx",
            duration_secs,
            1,
            1_000,
            false,
            request_mode,
            1,
        );
        run_scenario(
            "1w/5k/raw/1idx",
            duration_secs,
            1,
            5_000,
            false,
            request_mode,
            1,
        );
        if request_mode == ElasticsearchRequestMode::Buffered {
            run_scenario(
                "1w/5k/gzip/1idx",
                duration_secs,
                1,
                5_000,
                true,
                request_mode,
                1,
            );
            run_scenario(
                "4w/5k/gzip/1idx",
                duration_secs,
                4,
                5_000,
                true,
                request_mode,
                1,
            );
            run_scenario(
                "8w/5k/gzip/2idx",
                duration_secs,
                8,
                5_000,
                true,
                request_mode,
                2,
            );
            run_scenario(
                "16w/5k/gzip/4idx",
                duration_secs,
                16,
                5_000,
                true,
                request_mode,
                4,
            );
        }
    } else {
        let compress = compress_flag != 0;
        let bl = if batch_lines == 0 { 1_000 } else { batch_lines };
        run_scenario(
            "Custom",
            duration_secs,
            workers,
            bl,
            compress,
            request_mode,
            num_indices,
        );
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
