#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Exploratory benchmark: scanner + SQL transform across many dimensions.
//! Run with: cargo run -p logfwd-bench --release --bin explore

use std::io::Write;
use std::time::Instant;

use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_transform::SqlTransform;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "--help" {
        eprintln!("Usage: explore [--quick]");
        eprintln!("  --quick: run smaller datasets only");
        return;
    }
    let quick = args.iter().any(|a| a == "--quick");

    println!("dataset,lines,fields,query,scan_ms,transform_ms,total_ms,lines_per_sec,scan_lines_per_sec");

    let sizes: Vec<usize> = if quick {
        vec![10_000, 100_000]
    } else {
        vec![10_000, 100_000, 1_000_000]
    };

    // === Dimension 1: Data size scaling ===
    eprintln!("\n=== Dimension 1: Data Size Scaling (passthrough) ===");
    for &n in &sizes {
        let data = generate_simple(n);
        bench("simple", n, 6, "SELECT * FROM logs", &data);
    }

    // === Dimension 2: Query complexity ===
    eprintln!("\n=== Dimension 2: Query Complexity (100K lines) ===");
    let data_100k = generate_simple(100_000);
    let queries = [
        "SELECT * FROM logs",
        "SELECT timestamp_str, level_str, message_str FROM logs",
        "SELECT * FROM logs WHERE level_str = 'ERROR'",
        "SELECT * FROM logs WHERE level_str IN ('ERROR', 'WARN')",
        "SELECT * FROM logs WHERE duration_ms_int > 100",
        "SELECT * FROM logs WHERE level_str = 'ERROR' AND duration_ms_int > 200",
        "SELECT level_str, COUNT(*) as cnt FROM logs GROUP BY level_str",
        "SELECT service_str, level_str, COUNT(*) FROM logs GROUP BY service_str, level_str",
        "SELECT level_str, AVG(duration_ms_int) as avg_dur, MAX(duration_ms_int) as max_dur FROM logs GROUP BY level_str",
        "SELECT *, CASE WHEN duration_ms_int > 200 THEN 'slow' ELSE 'fast' END as speed FROM logs",
        "SELECT * FROM logs WHERE message_str LIKE '%users%'",
        "SELECT * FROM logs ORDER BY duration_ms_int DESC LIMIT 100",
    ];
    for q in &queries {
        bench("simple", 100_000, 6, q, &data_100k);
    }

    // === Dimension 3: Field count ===
    eprintln!("\n=== Dimension 3: Field Count (100K lines) ===");
    let data_wide = generate_wide(100_000);
    let data_narrow = generate_narrow(100_000);
    bench("narrow(3f)", 100_000, 3, "SELECT * FROM logs", &data_narrow);
    bench("simple(6f)", 100_000, 6, "SELECT * FROM logs", &data_100k);
    bench("wide(20f)", 100_000, 20, "SELECT * FROM logs", &data_wide);

    // === Dimension 4: Projection pushdown on wide data ===
    eprintln!("\n=== Dimension 4: Projection Pushdown (wide 20-field, 100K) ===");
    bench("wide", 100_000, 20, "SELECT * FROM logs", &data_wide);
    bench("wide", 100_000, 20, "SELECT timestamp_str, level_str FROM logs", &data_wide);
    bench("wide", 100_000, 20, "SELECT timestamp_str, level_str, message_str, duration_ms_int FROM logs", &data_wide);
    bench("wide", 100_000, 20, "SELECT * FROM logs WHERE level_str = 'ERROR'", &data_wide);
    bench("wide", 100_000, 20, "SELECT level_str FROM logs WHERE status_code_int > 400", &data_wide);

    // === Dimension 5: Aggregations ===
    eprintln!("\n=== Dimension 5: Aggregations (100K) ===");
    bench("simple", 100_000, 6, "SELECT COUNT(*) FROM logs", &data_100k);
    bench("simple", 100_000, 6, "SELECT level_str, COUNT(*) FROM logs GROUP BY level_str", &data_100k);
    bench("simple", 100_000, 6, "SELECT level_str, AVG(duration_ms_int), MIN(duration_ms_int), MAX(duration_ms_int) FROM logs GROUP BY level_str", &data_100k);
    bench("wide", 100_000, 20, "SELECT region_str, namespace_str, COUNT(*) FROM logs GROUP BY region_str, namespace_str", &data_wide);
    bench("wide", 100_000, 20, "SELECT method_str, AVG(duration_ms_int), COUNT(*) FROM logs GROUP BY method_str", &data_wide);

    // === Dimension 6: String operations ===
    eprintln!("\n=== Dimension 6: String Operations (100K) ===");
    bench("simple", 100_000, 6, "SELECT *, UPPER(level_str) as level_upper FROM logs", &data_100k);
    bench("simple", 100_000, 6, "SELECT *, LENGTH(message_str) as msg_len FROM logs", &data_100k);
    bench("simple", 100_000, 6, "SELECT *, CONCAT(level_str, ':', service_str) as tag FROM logs", &data_100k);
    bench("simple", 100_000, 6, "SELECT * FROM logs WHERE message_str LIKE '%orders%'", &data_100k);
    bench("simple", 100_000, 6, "SELECT * FROM logs WHERE LOWER(level_str) = 'error'", &data_100k);

    // === Dimension 7: Scan-only (no transform overhead) ===
    eprintln!("\n=== Dimension 7: Scan-Only (no SQL) ===");
    for &n in &sizes {
        let data = generate_simple(n);
        bench_scan_only("simple", n, 6, &data);
    }
    bench_scan_only("wide(20f)", 100_000, 20, &data_wide);
    bench_scan_only("narrow(3f)", 100_000, 3, &data_narrow);

    // === Dimension 8: Real-world data (nginx JSON logs) ===
    eprintln!("\n=== Dimension 8: Real-World Data (nginx JSON logs) ===");
    let nginx_paths = [
        ("/tmp/bench-data/nginx.ndjson", 51462),
        ("/tmp/bench-data/nginx_500k.ndjson", 514620),
        ("/tmp/bench-data/nginx_1m.ndjson", 1029240),
    ];
    for (path, expected_lines) in &nginx_paths {
        if let Ok(data) = std::fs::read(path) {
            let lines = memchr::memchr_iter(b'\n', &data).count();
            bench_scan_only(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, &data);
            bench(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, "SELECT * FROM logs", &data);
            bench(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, "SELECT time_str, remote_ip_str, request_str, response_int FROM logs", &data);
            bench(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, "SELECT * FROM logs WHERE response_int >= 400", &data);
            bench(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, "SELECT remote_ip_str, COUNT(*) as cnt FROM logs GROUP BY remote_ip_str ORDER BY cnt DESC LIMIT 10", &data);
            bench(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, "SELECT response_int, COUNT(*) FROM logs GROUP BY response_int", &data);
            bench(&format!("nginx({})", fmt_count(*expected_lines)), lines, 8, "SELECT * FROM logs WHERE request_str LIKE '%product_1%'", &data);
        } else {
            eprintln!("  Skipping {} (not found). Run: curl -o {} <url>", path, path);
        }
    }

    // === Dimension 9: 1M synthetic ===
    if !quick {
        eprintln!("\n=== Dimension 9: 1M Lines Stress Test ===");
        let data_1m = generate_simple(1_000_000);
        bench_scan_only("simple", 1_000_000, 6, &data_1m);
        bench("simple", 1_000_000, 6, "SELECT * FROM logs", &data_1m);
        bench("simple", 1_000_000, 6, "SELECT * FROM logs WHERE level_str = 'ERROR'", &data_1m);
        bench("simple", 1_000_000, 6, "SELECT level_str, COUNT(*) FROM logs GROUP BY level_str", &data_1m);
    }

    eprintln!("\nDone.");
}

fn fmt_count(n: usize) -> String {
    if n >= 1_000_000 { format!("{}M", n / 1_000_000) }
    else if n >= 1_000 { format!("{}K", n / 1_000) }
    else { format!("{}", n) }
}

fn bench(dataset: &str, lines: usize, fields: usize, sql: &str, data: &[u8]) {
    let mut transform = SqlTransform::new(sql).expect("bad SQL");
    let config = transform.scan_config();
    let mut scanner = Scanner::new(config);

    let t0 = Instant::now();
    let batch = scanner.scan(bytes::Bytes::from(data.to_vec()))
        .expect("scan failed");
    let scan_ms = t0.elapsed().as_millis() as u64;

    let t1 = Instant::now();
    let _result = transform.execute_blocking(batch);
    let transform_ms = t1.elapsed().as_millis() as u64;

    let total_ms = scan_ms + transform_ms;
    let lps = if total_ms > 0 { lines as u64 * 1000 / total_ms } else { 0 };
    let scan_lps = if scan_ms > 0 { lines as u64 * 1000 / scan_ms } else { 0 };

    let short_sql: String = sql.chars().take(60).collect();
    println!("{dataset},{lines},{fields},{short_sql},{scan_ms},{transform_ms},{total_ms},{lps},{scan_lps}");
    eprintln!(
        "  {:<12} {:>7} lines  {:>2}f  scan={:>5}ms  xform={:>5}ms  total={:>5}ms  {:>8} lines/sec  {}",
        dataset, lines, fields, scan_ms, transform_ms, total_ms, lps, &short_sql
    );
}

fn bench_scan_only(dataset: &str, lines: usize, fields: usize, data: &[u8]) {
    let config = ScanConfig::default();
    let mut scanner = Scanner::new(config);

    let t0 = Instant::now();
    let batch = scanner.scan(bytes::Bytes::from(data.to_vec()))
        .expect("scan failed");
    let scan_ms = t0.elapsed().as_millis() as u64;
    let lps = if scan_ms > 0 { lines as u64 * 1000 / scan_ms } else { 0 };

    println!("{dataset},{lines},{fields},SCAN_ONLY,{scan_ms},0,{scan_ms},{lps},{lps}");
    eprintln!(
        "  {:<12} {:>7} lines  {:>2}f  scan={:>5}ms  {:>8} lines/sec  ({} rows)",
        dataset, lines, fields, scan_ms, lps, batch.num_rows()
    );
}

// --- Data generators ---

fn generate_simple(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 180);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = ["/api/v1/users", "/api/v1/orders", "/api/v2/products", "/health", "/api/v1/auth"];
    for i in 0..n {
        write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{:016x}","service":"myapp"}}"#,
            i % 1000,
            levels[i % 4],
            paths[i % 5],
            10000 + (i * 7) % 90000,
            1 + (i * 13) % 500,
            (i as u64).wrapping_mul(0x517cc1b727220a95),
        ).unwrap();
        buf.push(b'\n');
    }
    buf
}

fn generate_wide(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 600);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
    let namespaces = ["default", "kube-system", "monitoring", "logging"];
    for i in 0..n {
        write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request {}","duration_ms":{},"service":"myapp","host":"node-{}","pod":"app-{:04}","namespace":"{}","method":"{}","status_code":{},"region":"{}","user_id":"user-{}","trace_id":"{:032x}","response_bytes":{},"latency_p99_ms":{},"error_count":{},"cache_hit":{},"db_query_ms":{},"upstream":"svc-{}","version":"v{}.{}"}}"#,
            i % 1000, levels[i % 4], i, 1 + (i * 13) % 500,
            i % 10, i % 100, namespaces[i % 4], methods[i % 5],
            [200, 201, 400, 404, 500][i % 5], regions[i % 4],
            i % 1000, (i as u64).wrapping_mul(0x517cc1b727220a95),
            100 + (i * 37) % 10000, 10 + (i * 11) % 1000,
            if i % 20 == 0 { 1 } else { 0 },
            if i % 3 == 0 { "true" } else { "false" },
            (i * 7) % 200, i % 4, 1 + i % 5, i % 10,
        ).unwrap();
        buf.push(b'\n');
    }
    buf
}

fn generate_narrow(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 60);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    for i in 0..n {
        write!(buf, r#"{{"ts":"{}","lvl":"{}","msg":"event {}"}}"#, i, levels[i % 4], i).unwrap();
        buf.push(b'\n');
    }
    buf
}
