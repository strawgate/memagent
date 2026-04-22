#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Profile each stage of the full pipeline: read → scan → transform → encode → "send"
//! Run with: cargo run -p logfwd-bench --release --features bench-tools --bin e2e_profile

use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_io::compress::ChunkCompressor;
use logfwd_output::BatchMetadata;
use logfwd_transform::SqlTransform;
use logfwd_types::diagnostics::ComponentStats;

fn main() {
    let lines = 100_000;
    let data = generate_simple(lines);
    let raw_mb = data.len() as f64 / 1_048_576.0;

    println!("=== End-to-End Stage Profiling ({lines} lines, {raw_mb:.1} MB) ===\n");

    // Stage 1: Scan
    let t0 = Instant::now();
    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner.scan(bytes::Bytes::from(data)).unwrap();
    let scan_ms = t0.elapsed().as_millis();

    // Stage 2: Transform (passthrough)
    let t1 = Instant::now();
    let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
    let result = transform.execute_blocking(batch).unwrap();
    let transform_ms = t1.elapsed().as_millis();

    // Stage 3: OTLP protobuf encoding
    let t3 = Instant::now();
    let mut otlp_sink = logfwd_output::OtlpSink::new(
        "bench".to_string(),
        "http://localhost:19877".to_string(),
        logfwd_output::OtlpProtocol::Http,
        logfwd_output::Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();
    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };
    otlp_sink.encode_batch(&result, &metadata);
    let otlp_ms = t3.elapsed().as_millis();
    let otlp_mb = otlp_sink.encoded_payload().len() as f64 / 1_048_576.0;

    // Stage 4: zstd compression of OTLP payload
    let t4 = Instant::now();
    let mut compressor = ChunkCompressor::new(1).unwrap();
    let compressed = compressor.compress(otlp_sink.encoded_payload()).unwrap();
    let zstd_ms = t4.elapsed().as_millis();
    let zstd_mb = compressed.data.len() as f64 / 1_048_576.0;

    let total_ms = scan_ms + transform_ms + otlp_ms + zstd_ms;
    let lps = if total_ms > 0 {
        lines as u64 * 1000 / total_ms as u64
    } else {
        0
    };
    let pct = |stage_ms: u128| {
        if total_ms > 0 {
            stage_ms as f64 / total_ms as f64 * 100.0
        } else {
            0.0
        }
    };

    println!(
        "  {:<25} {:>6}ms  {:>6.1} MB output  {:>5.1}%",
        "1. Scan (JSON→Arrow)",
        scan_ms,
        raw_mb,
        pct(scan_ms)
    );
    println!(
        "  {:<25} {:>6}ms  {:>6.1} MB output  {:>5.1}%",
        "2. Transform (SQL)",
        transform_ms,
        0.0,
        pct(transform_ms)
    );
    println!(
        "  {:<25} {:>6}ms  {:>6.1} MB output  {:>5.1}%",
        "3. OTLP protobuf encode",
        otlp_ms,
        otlp_mb,
        pct(otlp_ms)
    );
    println!(
        "  {:<25} {:>6}ms  {:>6.1} MB output  {:>5.1}%",
        "4. zstd compress",
        zstd_ms,
        zstd_mb,
        pct(zstd_ms)
    );
    println!("  {:<25} {:>6}ms", "───────────────────────", 0);
    println!(
        "  {:<25} {:>6}ms                {:>10} lines/sec",
        "TOTAL (CPU only)", total_ms, lps
    );
    println!();

    println!(
        "  Size pipeline: {raw_mb:.1}MB raw → {otlp_mb:.1}MB OTLP → {zstd_mb:.1}MB zstd ({:.1}x compression)",
        raw_mb / zstd_mb
    );

    // Now repeat at scale
    println!("\n=== Scaling to 1M lines ===\n");
    for n in [100_000, 500_000, 1_000_000] {
        let data = generate_simple(n);

        let t = Instant::now();
        let mut s = Scanner::new(ScanConfig::default());
        let batch = s.scan(bytes::Bytes::from(data)).unwrap();
        let scan = t.elapsed().as_millis();

        let t = Instant::now();
        let mut xf = SqlTransform::new("SELECT * FROM logs").unwrap();
        let result = xf.execute_blocking(batch).unwrap();
        let xform = t.elapsed().as_millis();

        let t = Instant::now();
        let mut sink = logfwd_output::OtlpSink::new(
            "b".into(),
            "http://x".into(),
            logfwd_output::OtlpProtocol::Http,
            logfwd_output::Compression::None,
            vec![],
            reqwest::Client::new(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();
        let meta = BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        };
        sink.encode_batch(&result, &meta);
        let encode = t.elapsed().as_millis();

        let t = Instant::now();
        let mut c = ChunkCompressor::new(1).unwrap();
        let _ = c.compress(sink.encoded_payload()).unwrap();
        let compress = t.elapsed().as_millis();

        let total = scan + xform + encode + compress;
        let lps = if total > 0 {
            n as u64 * 1000 / total as u64
        } else {
            0
        };

        println!(
            "  {n:>7} lines: scan={scan:>5}ms  xform={xform:>4}ms  otlp={encode:>5}ms  zstd={compress:>4}ms  total={total:>5}ms  {lps:>8} lines/sec"
        );
    }
}

fn generate_simple(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 180);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];
    for i in 0..n {
        write!(buf, r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{:016x}","service":"myapp"}}"#,
            i % 1000, levels[i % 4], paths[i % 5], 10000 + (i * 7) % 90000,
            1 + (i * 13) % 500, (i as u64).wrapping_mul(0x517cc1b727220a95)).unwrap();
        buf.push(b'\n');
    }
    buf
}
