//! Benchmark different IO worker batching strategies.
//!
//! Simulates the IO worker's receive-and-batch loop with different approaches
//! to avoid the extend_from_slice copy.
//!
//! Run: cargo run -p logfwd-bench --release --features bench-tools \
//!        --bin io_worker_options -- 100000

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
        "Data size: {:.2} MiB ({} bytes)",
        data_size as f64 / (1024.0 * 1024.0),
        data_size
    );

    // Split into chunks simulating framed output (4KB typical, variable)
    let chunks: Vec<bytes::Bytes> = {
        let mut v = Vec::new();
        let mut offset = 0;
        let mut rng = fastrand::Rng::with_seed(42);
        while offset < raw_bytes.len() {
            let chunk_size = rng.usize(2048..8192).min(raw_bytes.len() - offset);
            // Find last newline in this chunk range
            let end = offset + chunk_size;
            let slice = &raw_bytes[offset..end];
            let last_nl = memchr::memrchr(b'\n', slice);
            let actual_end = match last_nl {
                Some(pos) => offset + pos + 1,
                None => end, // no newline — take the whole chunk
            };
            v.push(raw_bytes.slice(offset..actual_end));
            offset = actual_end;
        }
        v
    };
    let num_chunks = chunks.len();
    let batch_target = 256 * 1024; // 256KB batch target
    eprintln!("Split into {num_chunks} chunks, batch target: {batch_target} bytes");

    // -----------------------------------------------------------------------
    // Option A: Current approach — extend_from_slice into BytesMut
    // -----------------------------------------------------------------------
    {
        let start = Instant::now();
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut buf = bytes::BytesMut::with_capacity(batch_target);
        let mut total_rows = 0usize;
        for chunk in &chunks {
            buf.extend_from_slice(chunk);
            if buf.len() >= batch_target {
                let data = buf.split().freeze();
                let batch = scanner.scan(data).expect("scan");
                total_rows += batch.num_rows();
            }
        }
        if !buf.is_empty() {
            let data = buf.split().freeze();
            let batch = scanner.scan(data).expect("scan");
            total_rows += batch.num_rows();
        }
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "\nOption A (extend_from_slice): {:.1} ms, {:.0} MiB/s, {} rows",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            total_rows
        );
    }

    // -----------------------------------------------------------------------
    // Option B: Collect Vec<Bytes>, concatenate once at flush
    // -----------------------------------------------------------------------
    {
        let start = Instant::now();
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut pending: Vec<bytes::Bytes> = Vec::new();
        let mut pending_len = 0usize;
        let mut total_rows = 0usize;
        for chunk in &chunks {
            pending_len += chunk.len();
            pending.push(chunk.clone());
            if pending_len >= batch_target {
                // Concatenate once
                let mut combined = bytes::BytesMut::with_capacity(pending_len);
                for p in &pending {
                    combined.extend_from_slice(p);
                }
                pending.clear();
                pending_len = 0;
                let data = combined.freeze();
                let batch = scanner.scan(data).expect("scan");
                total_rows += batch.num_rows();
            }
        }
        if !pending.is_empty() {
            let mut combined = bytes::BytesMut::with_capacity(pending_len);
            for p in &pending {
                combined.extend_from_slice(p);
            }
            let data = combined.freeze();
            let batch = scanner.scan(data).expect("scan");
            total_rows += batch.num_rows();
        }
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Option B (Vec<Bytes> + concat once): {:.1} ms, {:.0} MiB/s, {} rows",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            total_rows
        );
    }

    // -----------------------------------------------------------------------
    // Option C: Single-chunk fast path — if one chunk >= target, send directly
    // -----------------------------------------------------------------------
    {
        let start = Instant::now();
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut buf = bytes::BytesMut::with_capacity(batch_target);
        let mut total_rows = 0usize;
        for chunk in &chunks {
            if buf.is_empty() && chunk.len() >= batch_target {
                // Fast path: send directly without copy
                let batch = scanner.scan(chunk.clone()).expect("scan");
                total_rows += batch.num_rows();
            } else {
                buf.extend_from_slice(chunk);
                if buf.len() >= batch_target {
                    let data = buf.split().freeze();
                    let batch = scanner.scan(data).expect("scan");
                    total_rows += batch.num_rows();
                }
            }
        }
        if !buf.is_empty() {
            let data = buf.split().freeze();
            let batch = scanner.scan(data).expect("scan");
            total_rows += batch.num_rows();
        }
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Option C (single-chunk fast path): {:.1} ms, {:.0} MiB/s, {} rows",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            total_rows
        );
    }

    // -----------------------------------------------------------------------
    // Option D: No batching — scan each chunk individually
    // -----------------------------------------------------------------------
    {
        let start = Instant::now();
        let mut scanner = Scanner::new(ScanConfig::default());
        let mut total_rows = 0usize;
        for chunk in &chunks {
            let batch = scanner.scan(chunk.clone()).expect("scan");
            total_rows += batch.num_rows();
        }
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Option D (no batching, scan each): {:.1} ms, {:.0} MiB/s, {} rows",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            total_rows
        );
    }

    // -----------------------------------------------------------------------
    // Option E: Ideal — scan the whole thing as one Bytes (upper bound)
    // -----------------------------------------------------------------------
    {
        let start = Instant::now();
        let mut scanner = Scanner::new(ScanConfig::default());
        let batch = scanner.scan(raw_bytes.clone()).expect("scan");
        let elapsed = start.elapsed();
        let mib_s = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        eprintln!(
            "Option E (single scan, ideal): {:.1} ms, {:.0} MiB/s, {} rows",
            elapsed.as_secs_f64() * 1000.0,
            mib_s,
            batch.num_rows()
        );
    }
}
