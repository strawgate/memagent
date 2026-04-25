#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Measure actual RSS (resident set size) at each pipeline stage.
//! Run with: cargo run -p ffwd-bench --release --features bench-tools --bin rss

use std::io::Write;

use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;
use ffwd_transform::SqlTransform;

fn rss_mb() -> f64 {
    // macOS: use mach API via libc
    #[cfg(target_os = "macos")]
    {
        use std::mem::{self, size_of};
        // SAFETY: `zeroed()` is valid for `mach_task_basic_info_data_t`
        // (all-zero is a valid bit pattern for this plain-data struct).
        let mut info: libc::mach_task_basic_info_data_t = unsafe { mem::zeroed() };
        let mut count = (size_of::<libc::mach_task_basic_info_data_t>()
            / size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
        // SAFETY: accessing the mach task self port is always valid for the
        // current process.
        #[allow(deprecated)]
        let task = unsafe { libc::mach_task_self_ };
        // SAFETY: `task` is a valid task port. `info` is a properly-sized
        // zeroed struct and `count` holds the matching element count.
        // `task_info` only writes into `info`.
        let kr = unsafe {
            libc::task_info(
                task,
                libc::MACH_TASK_BASIC_INFO,
                &raw mut info as libc::task_info_t,
                &raw mut count,
            )
        };
        if kr == libc::KERN_SUCCESS {
            return info.resident_size as f64 / 1_048_576.0;
        }
        0.0
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: f64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    return kb / 1024.0;
                }
            }
        }
        0.0
    }
}

fn main() {
    println!("=== RSS Measurement at Each Pipeline Stage ===\n");

    for (label, lines) in [("100K", 100_000), ("500K", 500_000), ("1M", 1_000_000)] {
        println!("--- {label} lines ---");

        let baseline = rss_mb();
        println!("  Baseline RSS:            {baseline:.1} MB");

        // Generate data
        let data = generate_simple(lines);
        let after_gen = rss_mb();
        println!(
            "  After generate ({:.1}MB):  {after_gen:.1} MB  (+{:.1} MB)",
            data.len() as f64 / 1_048_576.0,
            after_gen - baseline
        );

        // Scan
        let mut scanner = Scanner::new(ScanConfig::default());
        let batch = scanner.scan(bytes::Bytes::from(data.clone())).unwrap();
        let after_scan = rss_mb();
        println!(
            "  After scan ({} rows):    {after_scan:.1} MB  (+{:.1} MB)",
            batch.num_rows(),
            after_scan - after_gen
        );

        // Drop the input data
        drop(data);
        let after_drop_input = rss_mb();
        println!(
            "  After drop input:        {after_drop_input:.1} MB  (+{:.1} MB)",
            after_drop_input - after_scan
        );

        // Transform
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        let result = transform.execute_blocking(batch.clone()).unwrap();
        let after_transform = rss_mb();
        println!(
            "  After transform ({} rows): {after_transform:.1} MB  (+{:.1} MB)",
            result.num_rows(),
            after_transform - after_drop_input
        );

        // Drop everything
        drop(batch);
        drop(result);
        drop(scanner);
        drop(transform);
        let after_drop_all = rss_mb();
        println!(
            "  After drop all:          {after_drop_all:.1} MB  (freed {:.1} MB)",
            after_transform - after_drop_all
        );

        println!(
            "  Peak delta from baseline: {:.1} MB",
            after_transform - baseline
        );
        println!(
            "  Per line at peak:         {:.0} bytes",
            (after_transform - baseline) * 1_048_576.0 / lines as f64
        );
        println!();
    }

    // === scan_detached (copies strings) vs scan (zero-copy) ===
    println!("--- scan_detached vs scan (1M simple lines) ---");
    {
        let data = generate_simple(1_000_000);
        let raw_mb = data.len() as f64 / 1_048_576.0;

        let before = rss_mb();
        let mut detached_scanner = Scanner::new(ScanConfig::default());
        let batch = detached_scanner
            .scan_detached(bytes::Bytes::from(data.clone()))
            .unwrap();
        let after_detached = rss_mb();
        let detached_cols = batch.num_columns();
        let detached_reported = batch.get_array_memory_size() as f64 / 1_048_576.0;
        drop(batch);
        drop(detached_scanner);

        let mid = rss_mb();
        let mut streaming_scanner = Scanner::new(ScanConfig::default());
        let batch = streaming_scanner.scan(bytes::Bytes::from(data)).unwrap();
        let after_streaming = rss_mb();
        let streaming_cols = batch.num_columns();
        let streaming_reported = batch.get_array_memory_size() as f64 / 1_048_576.0;
        drop(batch);
        drop(streaming_scanner);

        println!("  Raw JSON:         {raw_mb:.1} MB");
        println!(
            "  scan_detached:    RSS +{:.1} MB  (reported {detached_reported:.1} MB)  {detached_cols} cols",
            after_detached - before
        );
        println!(
            "  scan (zero-copy): RSS +{:.1} MB  (reported {streaming_reported:.1} MB)  {streaming_cols} cols",
            after_streaming - mid
        );
        println!(
            "  Savings:          {:.1} MB ({:.0}% less RSS)",
            (after_detached - before) - (after_streaming - mid),
            100.0 * (1.0 - (after_streaming - mid) / (after_detached - before))
        );
        println!();
    }

    // Also test with projection pushdown
    println!("--- Projection Pushdown Effect (1M lines, wide 20f) ---");
    let data = generate_wide(1_000_000);
    let gen_mb = data.len() as f64 / 1_048_576.0;

    let baseline = rss_mb();
    println!("  Baseline:                {baseline:.1} MB");
    println!("  Wide data generated:     {gen_mb:.1} MB");

    // SELECT * (all 20 fields)
    {
        let transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        let config = transform.scan_config();
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan(bytes::Bytes::from(data.clone())).unwrap();
        let after = rss_mb();
        println!(
            "  SELECT * (20 fields):    {after:.1} MB  (delta {:.1} MB)",
            after - baseline
        );
        drop(batch);
        drop(scanner);
        drop(transform);
    }

    let mid = rss_mb();

    // SELECT 2 fields (pushdown)
    {
        let transform = SqlTransform::new("SELECT timestamp_str, level_str FROM logs").unwrap();
        let config = transform.scan_config();
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan(bytes::Bytes::from(data.clone())).unwrap();
        let after = rss_mb();
        println!(
            "  SELECT 2 fields:         {after:.1} MB  (delta {:.1} MB)",
            after - mid
        );
        drop(batch);
        drop(scanner);
        drop(transform);
    }

    drop(data);
    println!("  After cleanup:           {:.1} MB", rss_mb());
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

fn generate_wide(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 600);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
    let namespaces = ["default", "kube-system", "monitoring", "logging"];
    for i in 0..n {
        write!(buf, r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request {}","duration_ms":{},"service":"myapp","host":"node-{}","pod":"app-{:04}","namespace":"{}","method":"{}","status_code":{},"region":"{}","user_id":"user-{}","trace_id":"{:032x}","response_bytes":{},"latency_p99_ms":{},"error_count":{},"cache_hit":{},"db_query_ms":{},"upstream":"svc-{}","version":"v{}.{}"}}"#,
            i % 1000, levels[i % 4], i, 1 + (i * 13) % 500,
            i % 10, i % 100, namespaces[i % 4], methods[i % 5],
            [200, 201, 400, 404, 500][i % 5], regions[i % 4],
            i % 1000, (i as u64).wrapping_mul(0x517cc1b727220a95),
            100 + (i * 37) % 10000, 10 + (i * 11) % 1000,
            i32::from(i % 20 == 0),
            if i % 3 == 0 { "true" } else { "false" },
            (i * 7) % 200, i % 4, 1 + i % 5, i % 10).unwrap();
        buf.push(b'\n');
    }
    buf
}
