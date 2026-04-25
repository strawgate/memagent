#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Library evaluation benchmarks for I/O component refactor (issue #1283).
//!
//! Compares:
//! 1. `axum` HTTP throughput (sequential + concurrent)
//! 2. `async-compression` streaming vs `zstd::decode_all()` — decompression
//! 3. `backon` retry overhead vs hand-rolled loop
//! 4. `async-compression` write path — streaming compress throughput
//!
//! Run: `just bench` for standard benchmarks, or directly:
//!   `cargo run --release --bin library-eval -p ffwd-bench`
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_otlp_payload(size_hint: usize) -> Vec<u8> {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use prost::Message;

    let mut records = Vec::new();
    let line = "a]".repeat(size_hint.max(64) / 64);
    loop {
        records.push(LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(line.clone())),
            }),
            attributes: vec![KeyValue {
                key: "host".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("bench-host".into())),
                }),
            }],
            ..Default::default()
        });
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: records.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let encoded_len = req.encoded_len();
        if encoded_len >= size_hint {
            let mut buf = Vec::with_capacity(encoded_len);
            req.encode(&mut buf).unwrap();
            return buf;
        }
    }
}

fn compress_zstd(data: &[u8]) -> Vec<u8> {
    zstd::encode_all(data, 1).unwrap()
}

fn format_throughput(bytes: usize, elapsed: Duration) -> String {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    let secs = elapsed.as_secs_f64();
    format!("{:.1} MiB/s", mb / secs)
}

fn format_rate(count: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    format!("{:.0} req/s", count as f64 / secs)
}

// ---------------------------------------------------------------------------
// Axum HTTP server helper
// ---------------------------------------------------------------------------

struct AxumTestServer {
    addr: SocketAddr,
    counter: Arc<AtomicU64>,
    tx: mpsc::Sender<Bytes>,
    server_handle: tokio::task::JoinHandle<()>,
    drain_handle: tokio::task::JoinHandle<()>,
}

impl AxumTestServer {
    async fn start() -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<Bytes>(8192);

        let app = axum::Router::new().route(
            "/v1/logs",
            axum::routing::post({
                let tx = tx.clone();
                move |body: Bytes| {
                    let tx = tx.clone();
                    async move {
                        let _ = tx.try_send(body);
                        axum::http::StatusCode::OK
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let counter2 = counter.clone();
        let drain_handle = tokio::spawn(async move {
            while rx.recv().await.is_some() {
                counter2.fetch_add(1, Ordering::Relaxed);
            }
        });

        Self {
            addr,
            counter,
            tx,
            server_handle,
            drain_handle,
        }
    }

    fn received(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    async fn shutdown(self) {
        // Abort server first (it holds a tx clone)
        self.server_handle.abort();
        drop(self.tx);
        let _ = self.drain_handle.await;
    }
}

// ---------------------------------------------------------------------------
// Benchmark 1: axum HTTP — sequential + concurrent
// ---------------------------------------------------------------------------

async fn bench_axum_sequential(addr: SocketAddr, payload: &[u8], iterations: u64) -> Duration {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(4)
        .build()
        .unwrap();
    let url = format!("http://{addr}/v1/logs");

    // Warmup
    for _ in 0..20 {
        let _ = client
            .post(&url)
            .header("content-type", "application/x-protobuf")
            .body(payload.to_vec())
            .send()
            .await;
    }

    let start = Instant::now();
    for _ in 0..iterations {
        client
            .post(&url)
            .header("content-type", "application/x-protobuf")
            .body(payload.to_vec())
            .send()
            .await
            .unwrap();
    }
    start.elapsed()
}

async fn bench_axum_concurrent(
    addr: SocketAddr,
    payload: &[u8],
    concurrency: usize,
    per_task: u64,
) -> Duration {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()
        .unwrap();
    let url = format!("http://{addr}/v1/logs");

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let url = url.clone();
        let payload = payload.to_vec();
        handles.push(tokio::spawn(async move {
            for _ in 0..per_task {
                client
                    .post(&url)
                    .header("content-type", "application/x-protobuf")
                    .body(payload.clone())
                    .send()
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Benchmark 2: decompression
// ---------------------------------------------------------------------------

fn bench_zstd_decode_all(compressed: &[u8], iterations: u64) -> (Duration, usize) {
    let start = Instant::now();
    let mut total_bytes = 0usize;
    for _ in 0..iterations {
        let decompressed = zstd::decode_all(compressed).unwrap();
        total_bytes += decompressed.len();
    }
    (start.elapsed(), total_bytes)
}

async fn bench_async_compression_decompress(
    compressed: &[u8],
    iterations: u64,
) -> (Duration, usize) {
    use async_compression::tokio::bufread::ZstdDecoder;
    use tokio::io::AsyncReadExt;

    let start = Instant::now();
    let mut total_bytes = 0usize;
    for _ in 0..iterations {
        let reader = ZstdDecoder::new(tokio::io::BufReader::new(compressed));
        tokio::pin!(reader);
        let mut buf = Vec::with_capacity(compressed.len() * 4);
        reader.read_to_end(&mut buf).await.unwrap();
        total_bytes += buf.len();
    }
    (start.elapsed(), total_bytes)
}

// ---------------------------------------------------------------------------
// Benchmark 3: retry overhead
// ---------------------------------------------------------------------------

async fn bench_backon_retry(iterations: u64) -> Duration {
    use backon::{ExponentialBuilder, Retryable};

    let start = Instant::now();
    for _ in 0..iterations {
        let result: Result<(), &str> = (|| async { Ok(()) })
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(100))
                    .with_max_delay(Duration::from_secs(30))
                    .with_max_times(3)
                    .with_jitter()
                    .with_factor(2.0),
            )
            .when(|_e: &&str| true)
            .await;
        let _ = result;
    }
    start.elapsed()
}

fn bench_handrolled_retry(iterations: u64) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        let max_retries = 3u32;
        let mut _delay = Duration::from_millis(100);
        let mut attempts = 0u32;
        loop {
            // Use black_box to prevent loop elimination
            let success = std::hint::black_box(true);
            if success {
                break;
            }
            attempts += 1;
            if attempts >= max_retries {
                break;
            }
            _delay *= 2;
        }
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Benchmark 4: compression (write path)
// ---------------------------------------------------------------------------

fn bench_zstd_compress_sync(data: &[u8], iterations: u64) -> (Duration, usize) {
    let start = Instant::now();
    let mut total_bytes = 0usize;
    for _ in 0..iterations {
        let compressed = zstd::encode_all(data, 1).unwrap();
        total_bytes += compressed.len();
    }
    (start.elapsed(), total_bytes)
}

async fn bench_async_compression_compress(data: &[u8], iterations: u64) -> (Duration, usize) {
    use async_compression::tokio::bufread::ZstdEncoder;
    use tokio::io::AsyncReadExt;

    let start = Instant::now();
    let mut total_bytes = 0usize;
    for _ in 0..iterations {
        let encoder = ZstdEncoder::new(tokio::io::BufReader::new(data));
        tokio::pin!(encoder);
        let mut buf = Vec::with_capacity(data.len());
        encoder.read_to_end(&mut buf).await.unwrap();
        total_bytes += buf.len();
    }
    (start.elapsed(), total_bytes)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    println!("=== Library Evaluation Benchmarks (issue #1283) ===\n");

    // Generate test payloads
    let payload_1k = make_otlp_payload(1_024);
    let payload_10k = make_otlp_payload(10_240);
    let payload_100k = make_otlp_payload(102_400);
    let compressed_1k = compress_zstd(&payload_1k);
    let compressed_10k = compress_zstd(&payload_10k);
    let compressed_100k = compress_zstd(&payload_100k);

    println!(
        "Payloads: 1K={} bytes, 10K={} bytes, 100K={} bytes",
        payload_1k.len(),
        payload_10k.len(),
        payload_100k.len()
    );
    println!(
        "Compressed: 1K={} bytes, 10K={} bytes, 100K={} bytes\n",
        compressed_1k.len(),
        compressed_10k.len(),
        compressed_100k.len()
    );

    // -----------------------------------------------------------------------
    // 1. HTTP: axum throughput (sequential + concurrent)
    // -----------------------------------------------------------------------
    println!("--- 1. HTTP Server: axum throughput ---");
    println!(
        "  (tiny_http omitted — single-threaded blocking, known inferior for async pipelines)\n"
    );

    let http_iters = 2_000u64;

    for (label, payload) in [
        ("1K", &payload_1k),
        ("10K", &payload_10k),
        ("100K", &payload_100k),
    ] {
        let server = AxumTestServer::start().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let elapsed = bench_axum_sequential(server.addr, payload, http_iters).await;
        let received = server.received();
        server.shutdown().await;

        println!("  {label} payload ({} bytes) — sequential:", payload.len());
        println!(
            "    {} ({} received, {})",
            format_rate(http_iters, elapsed),
            received,
            format_throughput(payload.len() * http_iters as usize, elapsed),
        );
    }

    // Concurrent test
    println!("\n  Concurrent (8 tasks × 500 req, 10K payload):");
    let server = AxumTestServer::start().await;
    let conc_elapsed = bench_axum_concurrent(server.addr, &payload_10k, 8, 500).await;
    let total_conc = 8 * 500u64;
    let conc_received = server.received();
    server.shutdown().await;
    println!(
        "    {} ({} received, {})\n",
        format_rate(total_conc, conc_elapsed),
        conc_received,
        format_throughput(payload_10k.len() * total_conc as usize, conc_elapsed),
    );

    // -----------------------------------------------------------------------
    // 2. Decompression: zstd::decode_all vs async-compression
    // -----------------------------------------------------------------------
    println!("--- 2. Decompression: zstd::decode_all vs async-compression ---");
    let decomp_iters = 10_000;

    for (label, compressed, original_size) in [
        ("1K", compressed_1k.as_slice(), payload_1k.len()),
        ("10K", compressed_10k.as_slice(), payload_10k.len()),
        ("100K", compressed_100k.as_slice(), payload_100k.len()),
    ] {
        let (sync_elapsed, sync_bytes) = bench_zstd_decode_all(compressed, decomp_iters);
        let (async_elapsed, async_bytes) =
            bench_async_compression_decompress(compressed, decomp_iters).await;
        println!(
            "  {label} ({} → {} bytes):",
            compressed.len(),
            original_size
        );
        println!(
            "    zstd::decode_all:    {} ({:.1}μs/op)",
            format_throughput(sync_bytes, sync_elapsed),
            sync_elapsed.as_micros() as f64 / decomp_iters as f64,
        );
        println!(
            "    async-compression:   {} ({:.1}μs/op)",
            format_throughput(async_bytes, async_elapsed),
            async_elapsed.as_micros() as f64 / decomp_iters as f64,
        );
        let overhead = (async_elapsed.as_secs_f64() / sync_elapsed.as_secs_f64() - 1.0) * 100.0;
        println!("    → async overhead: {overhead:+.1}%\n");
    }

    // -----------------------------------------------------------------------
    // 3. Retry: backon vs hand-rolled
    // -----------------------------------------------------------------------
    println!("--- 3. Retry overhead: backon vs hand-rolled (success path) ---");
    let retry_iters = 1_000_000;

    let backon_elapsed = bench_backon_retry(retry_iters).await;
    let handrolled_elapsed = bench_handrolled_retry(retry_iters);
    println!(
        "  backon:      {:.1}ns/op ({:.2}s total)",
        backon_elapsed.as_nanos() as f64 / retry_iters as f64,
        backon_elapsed.as_secs_f64(),
    );
    println!(
        "  hand-rolled: {:.1}ns/op ({:.2}s total)",
        handrolled_elapsed.as_nanos() as f64 / retry_iters as f64,
        handrolled_elapsed.as_secs_f64(),
    );
    let overhead = (backon_elapsed.as_secs_f64() / handrolled_elapsed.as_secs_f64() - 1.0) * 100.0;
    println!("  → backon overhead: {overhead:+.1}%\n");

    // -----------------------------------------------------------------------
    // 4. Compression: sync zstd vs async-compression (write path)
    // -----------------------------------------------------------------------
    println!("--- 4. Compression (write path): sync zstd vs async-compression ---");
    let comp_iters = 5_000;

    let json_payload: Vec<u8> = (0..1000)
        .flat_map(|i| {
            format!(
                r#"{{"timestamp":"2026-04-05T22:00:00Z","level":"info","message":"benchmark log line {i}","host":"bench"}}"#
            )
            .into_bytes()
            .into_iter()
            .chain(std::iter::once(b'\n'))
        })
        .collect();
    println!("  Input: {} bytes of JSON lines", json_payload.len());

    let (sync_elapsed, sync_bytes) = bench_zstd_compress_sync(&json_payload, comp_iters);
    let (async_elapsed, async_bytes) =
        bench_async_compression_compress(&json_payload, comp_iters).await;
    println!(
        "  zstd::encode_all:   {} ({:.1}μs/op, ratio {:.1}x)",
        format_throughput(json_payload.len() * comp_iters as usize, sync_elapsed),
        sync_elapsed.as_micros() as f64 / comp_iters as f64,
        json_payload.len() as f64 / (sync_bytes as f64 / comp_iters as f64),
    );
    println!(
        "  async-compression:  {} ({:.1}μs/op, ratio {:.1}x)",
        format_throughput(json_payload.len() * comp_iters as usize, async_elapsed),
        async_elapsed.as_micros() as f64 / comp_iters as f64,
        json_payload.len() as f64 / (async_bytes as f64 / comp_iters as f64),
    );
    let overhead = (async_elapsed.as_secs_f64() / sync_elapsed.as_secs_f64() - 1.0) * 100.0;
    println!("  → async overhead: {overhead:+.1}%\n");

    // -----------------------------------------------------------------------
    // 5. Glob matching: glob vs globset
    // -----------------------------------------------------------------------
    println!("--- 5. Glob matching: glob vs globset ---");

    // Generate realistic file paths (k8s pod logs)
    let namespaces = [
        "default",
        "kube-system",
        "monitoring",
        "app-prod",
        "app-staging",
    ];
    let pods = [
        "nginx",
        "redis",
        "api-server",
        "worker",
        "scheduler",
        "etcd",
    ];
    let test_paths: Vec<String> = (0..10_000)
        .map(|i| {
            let ns = namespaces[i % namespaces.len()];
            let pod = pods[i % pods.len()];
            format!("/var/log/pods/{ns}/{pod}-{i}/0.log")
        })
        .collect();

    let patterns = [
        "/var/log/pods/*/nginx-*/0.log",
        "/var/log/pods/app-*/*/0.log",
        "/var/log/pods/kube-system/*/0.log",
        "/var/log/pods/**/worker-*/**",
    ];
    println!(
        "  {} file paths, {} patterns\n",
        test_paths.len(),
        patterns.len()
    );

    // glob crate — must re-parse pattern each time (simulated with string matching)
    let glob_iters = 1_000u64;
    let glob_start = Instant::now();
    let mut glob_matches = 0u64;
    for _ in 0..glob_iters {
        for pattern in &patterns {
            let compiled = glob::Pattern::new(pattern).unwrap();
            for path in &test_paths {
                if compiled.matches(path) {
                    glob_matches += 1;
                }
            }
        }
    }
    let glob_elapsed = glob_start.elapsed();

    // globset — compile once, match many
    // Compile GlobSet before starting the timer so only matching is measured.
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in &patterns {
        builder.add(globset::Glob::new(pattern).unwrap());
    }
    let set = builder.build().unwrap();
    let globset_start = Instant::now();
    let mut globset_matches = 0u64;
    for _ in 0..glob_iters {
        for path in &test_paths {
            if set.is_match(path) {
                globset_matches += 1;
            }
        }
    }
    let globset_elapsed = globset_start.elapsed();

    // glob::Pattern counts per-(pattern, path) pairs; globset counts paths
    // matching any pattern — use separate denominators for fair comparison.
    let total_checks = glob_iters * patterns.len() as u64 * test_paths.len() as u64;
    let total_checks_any = glob_iters * test_paths.len() as u64;
    println!(
        "  glob::Pattern:  {:.1}μs per rescan, {:.0} matches/s ({} matches)",
        glob_elapsed.as_micros() as f64 / glob_iters as f64,
        total_checks as f64 / glob_elapsed.as_secs_f64(),
        glob_matches,
    );
    println!(
        "  globset::GlobSet: {:.1}μs per rescan, {:.0} matches/s ({} matches)",
        globset_elapsed.as_micros() as f64 / glob_iters as f64,
        total_checks_any as f64 / globset_elapsed.as_secs_f64(),
        globset_matches,
    );
    let speedup = glob_elapsed.as_secs_f64() / globset_elapsed.as_secs_f64();
    println!("  → globset is {speedup:.1}x faster\n");

    // -----------------------------------------------------------------------
    // 6. JSON parsing: serde_json vs sonic-rs (OTLP JSON payloads)
    // -----------------------------------------------------------------------
    println!("--- 6. JSON parsing: serde_json vs sonic-rs ---");

    // Build OTLP JSON payloads of varying sizes
    fn make_otlp_json(num_records: usize) -> Vec<u8> {
        let mut records = String::new();
        for i in 0..num_records {
            if i > 0 {
                records.push(',');
            }
            use std::fmt::Write;
            let _ = write!(
                records,
                r#"{{"timeUnixNano":"1700000000000000000","severityNumber":9,"severityText":"INFO","body":{{"stringValue":"Log line {} with some realistic content that includes host=bench-host service=api latency=42ms"}},"attributes":[{{"key":"host","value":{{"stringValue":"bench-host-{}"}}}}]}}"#,
                i,
                i % 100
            );
        }
        format!(r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{records}]}}]}}]}}"#)
            .into_bytes()
    }

    let json_1k = make_otlp_json(3); // ~1KB
    let json_10k = make_otlp_json(30); // ~10KB
    let json_100k = make_otlp_json(300); // ~100KB

    let json_iters = 10_000u64;
    for (label, payload) in [
        ("~1K", &json_1k),
        ("~10K", &json_10k),
        ("~100K", &json_100k),
    ] {
        // serde_json
        let serde_start = Instant::now();
        for _ in 0..json_iters {
            let _: serde_json::Value =
                serde_json::from_slice(std::hint::black_box(payload)).unwrap();
        }
        let serde_elapsed = serde_start.elapsed();

        // sonic-rs
        // from_slice accepts an immutable slice directly — no copy needed.
        let sonic_start = Instant::now();
        for _ in 0..json_iters {
            let _: sonic_rs::Value = sonic_rs::from_slice(std::hint::black_box(payload)).unwrap();
        }
        let sonic_elapsed = sonic_start.elapsed();

        println!("  {label} ({} bytes, {} records):", payload.len(), label);
        println!(
            "    serde_json:  {} ({:.1}μs/op)",
            format_throughput(payload.len() * json_iters as usize, serde_elapsed),
            serde_elapsed.as_micros() as f64 / json_iters as f64,
        );
        println!(
            "    sonic-rs:    {} ({:.1}μs/op)",
            format_throughput(payload.len() * json_iters as usize, sonic_elapsed),
            sonic_elapsed.as_micros() as f64 / json_iters as f64,
        );
        let speedup = serde_elapsed.as_secs_f64() / sonic_elapsed.as_secs_f64();
        println!("    → sonic-rs is {speedup:.2}x vs serde_json\n");
    }

    println!("=== Done ===");
}
