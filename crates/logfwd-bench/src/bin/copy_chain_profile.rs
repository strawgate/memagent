//! Allocation profiling for the file-read to Arrow-batch data path.
//!
//! Isolates framing, IO worker accumulation, and scanning to measure
//! per-stage allocation overhead.
//!
//! Run with dhat:
//!   cargo run -p logfwd-bench --release --features dhat-heap \
//!     --bin copy_chain_profile -- 100000
//!
//! Run without (timing only):
//!   cargo run -p logfwd-bench --release --bin copy_chain_profile -- 100000

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::time::Instant;

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::generators;
use logfwd_core::scan_config::ScanConfig;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let rows: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".to_string())
        .parse()
        .expect("rows must be a number");

    eprintln!("Generating {rows} production-mixed JSON lines...");
    let raw_data = generators::gen_production_mixed(rows, 42);
    let raw_bytes = bytes::Bytes::from(raw_data);
    let data_size = raw_bytes.len();
    eprintln!("Data size: {:.2} MiB", data_size as f64 / (1024.0 * 1024.0));

    // Stage 1: Scanner only (baseline — measures scan + Arrow build allocs)
    {
        let mut scanner = Scanner::new(ScanConfig::default());
        let start = Instant::now();
        let batch = scanner.scan_detached(raw_bytes.clone()).expect("scan");
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Stage 1 (scan only): {:.1} ms, {:.0} MiB/s, {} rows output",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            batch.num_rows()
        );
    }

    // Stage 2: Simulate IO worker accumulation copy
    // This is what io_worker.rs:458 does: extend_from_slice into BytesMut
    {
        let start = Instant::now();
        let mut buf = bytes::BytesMut::with_capacity(data_size);
        // Simulate receiving the data in ~4KB chunks (typical framed output size)
        let chunk_size = 4096;
        let mut offset = 0;
        while offset < raw_bytes.len() {
            let end = (offset + chunk_size).min(raw_bytes.len());
            buf.extend_from_slice(&raw_bytes[offset..end]);
            offset = end;
        }
        let frozen = buf.freeze();
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Stage 2 (IO worker copy): {:.1} ms, {:.0} MiB/s, {} bytes",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            frozen.len()
        );
    }

    // Stage 3: IO worker copy + scan (current pipeline path)
    {
        let mut buf = bytes::BytesMut::with_capacity(data_size);
        let chunk_size = 4096;
        let mut offset = 0;
        while offset < raw_bytes.len() {
            let end = (offset + chunk_size).min(raw_bytes.len());
            buf.extend_from_slice(&raw_bytes[offset..end]);
            offset = end;
        }
        let frozen = buf.freeze();

        let mut scanner = Scanner::new(ScanConfig::default());
        let start = Instant::now();
        let batch = scanner.scan_detached(frozen).expect("scan");
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Stage 3 (copy + scan): {:.1} ms, {:.0} MiB/s, {} rows output",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            batch.num_rows()
        );
    }

    // Stage 4: Zero-copy scan (ideal — no IO worker copy)
    {
        let mut scanner = Scanner::new(ScanConfig::default());
        let start = Instant::now();
        let batch = scanner.scan_detached(raw_bytes.clone()).expect("scan");
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Stage 4 (zero-copy scan): {:.1} ms, {:.0} MiB/s, {} rows output",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            batch.num_rows()
        );
    }

    eprintln!("\nCopy overhead = Stage 3 time - Stage 4 time");
    eprintln!("IO worker copy overhead = Stage 2 time");
}
