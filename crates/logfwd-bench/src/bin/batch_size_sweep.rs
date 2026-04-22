//! Sweep batch sizes to find where per-scan overhead becomes negligible.
//!
//! For each batch size from 4KB to 4MB, splits data into chunks of that size
//! and scans each independently. Measures total throughput.
//!
//! Run: cargo run -p logfwd-bench --release --features bench-tools \
//!        --bin batch_size_sweep -- 100000

use std::time::Instant;

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::generators;
use logfwd_core::scan_config::ScanConfig;

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".to_string())
        .parse()
        .expect("rows must be a number");

    eprintln!("Generating {rows} production-mixed JSON lines...");
    let raw_data = generators::gen_production_mixed(rows, 42);
    let raw_bytes = bytes::Bytes::from(raw_data);
    let data_size = raw_bytes.len();
    eprintln!(
        "Data size: {:.2} MiB\n",
        data_size as f64 / (1024.0 * 1024.0)
    );

    let batch_sizes: Vec<usize> = vec![
        4 * 1024,        // 4 KB
        8 * 1024,        // 8 KB
        16 * 1024,       // 16 KB
        32 * 1024,       // 32 KB
        64 * 1024,       // 64 KB
        128 * 1024,      // 128 KB
        256 * 1024,      // 256 KB (default read budget)
        512 * 1024,      // 512 KB
        1024 * 1024,     // 1 MB
        2 * 1024 * 1024, // 2 MB
        4 * 1024 * 1024, // 4 MB (default batch target)
        data_size,       // single batch (upper bound)
    ];

    eprintln!(
        "{:>10}  {:>6}  {:>8}  {:>10}  {:>10}",
        "batch_size", "scans", "time_ms", "MiB/s", "vs_4MB"
    );
    eprintln!("{}", "-".repeat(52));

    let mut baseline_mib_s = 0.0f64;

    for &batch_size in &batch_sizes {
        // Split into chunks at newline boundaries
        let chunks: Vec<bytes::Bytes> = {
            let mut v = Vec::new();
            let mut offset = 0;
            while offset < raw_bytes.len() {
                let target_end = (offset + batch_size).min(raw_bytes.len());
                // Find last newline at or before target_end
                let search_slice = &raw_bytes[offset..target_end];
                let actual_end = match memchr::memrchr(b'\n', search_slice) {
                    Some(pos) => offset + pos + 1,
                    None => target_end, // no newline — take the whole thing
                };
                v.push(raw_bytes.slice(offset..actual_end));
                offset = actual_end;
            }
            v
        };

        // Warm up
        {
            let mut scanner = Scanner::new(ScanConfig::default());
            for chunk in &chunks {
                let _ = scanner.scan(chunk.clone());
            }
        }

        // Measure (3 iterations, take best)
        let mut best_elapsed = std::time::Duration::from_secs(999);
        for _ in 0..3 {
            let mut scanner = Scanner::new(ScanConfig::default());
            let start = Instant::now();
            let mut total_rows = 0usize;
            for chunk in &chunks {
                let batch = scanner.scan(chunk.clone()).expect("scan");
                total_rows += batch.num_rows();
                std::hint::black_box(total_rows);
            }
            let elapsed = start.elapsed();
            if elapsed < best_elapsed {
                best_elapsed = elapsed;
            }
        }

        let mib_s = data_size as f64 / (1024.0 * 1024.0) / best_elapsed.as_secs_f64();
        let label = if batch_size >= 1024 * 1024 {
            format!("{} MB", batch_size / (1024 * 1024))
        } else {
            format!("{} KB", batch_size / 1024)
        };

        if batch_size == 4 * 1024 * 1024 {
            baseline_mib_s = mib_s;
        }

        let vs_4mb = if baseline_mib_s > 0.0 {
            format!("{:+.0}%", (mib_s / baseline_mib_s - 1.0) * 100.0)
        } else {
            "—".to_string()
        };

        eprintln!(
            "{:>10}  {:>6}  {:>8.1}  {:>10.0}  {:>10}",
            label,
            chunks.len(),
            best_elapsed.as_secs_f64() * 1000.0,
            mib_s,
            vs_4mb
        );
    }
}
