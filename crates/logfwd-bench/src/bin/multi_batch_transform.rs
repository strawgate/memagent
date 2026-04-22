//! Benchmark: scan many small batches, then execute SQL once via multi-batch.
//!
//! Compares:
//! A: scan(4MB) + transform(4MB)              — current approach
//! B: scan(256KB)×16 + transform_multi(all)    — multi-batch approach
//! C: scan(256KB)×16 + transform(each)         — per-chunk transform
//!
//! Run: cargo run -p logfwd-bench --release --features bench-tools \
//!        --bin multi_batch_transform -- 100000

use std::time::Instant;

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::generators;
use logfwd_core::scan_config::ScanConfig;
use logfwd_transform::SqlTransform;

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

    let small_chunks = split_at_newlines(&raw_bytes, 256 * 1024);
    let big_chunks = split_at_newlines(&raw_bytes, 4 * 1024 * 1024);

    for sql in &["SELECT * FROM logs", "SELECT * FROM logs WHERE level = 'error'"] {
        eprintln!("=== SQL: {sql} ===");

        // A: Current — 4MB scan + transform
        {
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform = SqlTransform::new(sql).unwrap();
            // warm
            for c in &big_chunks {
                let b = scanner.scan(c.clone()).unwrap();
                let _ = transform.execute_blocking(b);
            }
            let start = Instant::now();
            let mut total = 0usize;
            for c in &big_chunks {
                let b = scanner.scan(c.clone()).unwrap();
                let r = transform.execute_blocking(b).unwrap();
                total += r.num_rows();
            }
            let elapsed = start.elapsed();
            eprintln!(
                "  A: 4MB scan+xform:          {:>6.1} ms  {:>5.0} MiB/s  {} rows",
                elapsed.as_secs_f64() * 1000.0,
                data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                total
            );
        }

        // B: Multi-batch — scan 256KB each, group by schema, transform per group
        {
            use std::collections::HashMap;
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform = SqlTransform::new(sql).unwrap();
            // warm
            {
                let batches: Vec<_> = small_chunks
                    .iter()
                    .map(|c| scanner.scan(c.clone()).unwrap())
                    .collect();
                let groups = group_by_schema(&batches);
                for group in groups.values() {
                    let _ = transform.execute_multi_batch_blocking(group.clone());
                }
            }
            let start = Instant::now();
            let batches: Vec<_> = small_chunks
                .iter()
                .map(|c| scanner.scan(c.clone()).unwrap())
                .collect();
            let groups = group_by_schema(&batches);
            let mut total = 0usize;
            for group in groups.values() {
                let r = transform.execute_multi_batch_blocking(group.clone()).unwrap();
                total += r.num_rows();
            }
            let elapsed = start.elapsed();
            eprintln!(
                "  B: 256KB scan×{} + group({})+multi: {:>6.1} ms  {:>5.0} MiB/s  {} rows",
                small_chunks.len(),
                groups.len(),
                elapsed.as_secs_f64() * 1000.0,
                data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                total
            );
        }

        // C: Per-chunk — scan 256KB + transform each
        {
            let mut scanner = Scanner::new(ScanConfig::default());
            let mut transform = SqlTransform::new(sql).unwrap();
            // warm
            for c in &small_chunks {
                let b = scanner.scan(c.clone()).unwrap();
                let _ = transform.execute_blocking(b);
            }
            let start = Instant::now();
            let mut total = 0usize;
            for c in &small_chunks {
                let b = scanner.scan(c.clone()).unwrap();
                let r = transform.execute_blocking(b).unwrap();
                total += r.num_rows();
            }
            let elapsed = start.elapsed();
            eprintln!(
                "  C: 256KB scan+xform each:   {:>6.1} ms  {:>5.0} MiB/s  {} rows",
                elapsed.as_secs_f64() * 1000.0,
                data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                total
            );
        }

        eprintln!();
    }
}

/// Group batches by schema identity. Returns a map from schema fingerprint
/// to the batches sharing that schema.
fn group_by_schema(
    batches: &[arrow::record_batch::RecordBatch],
) -> std::collections::HashMap<u64, Vec<arrow::record_batch::RecordBatch>> {
    use std::collections::HashMap;
    use std::hash::{Hash, Hasher};

    let mut groups: HashMap<u64, Vec<arrow::record_batch::RecordBatch>> = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for field in schema.fields() {
            field.name().hash(&mut hasher);
            format!("{:?}", field.data_type()).hash(&mut hasher);
        }
        let key = hasher.finish();
        groups.entry(key).or_default().push(batch.clone());
    }
    groups
}

fn split_at_newlines(data: &bytes::Bytes, batch_size: usize) -> Vec<bytes::Bytes> {
    let mut v = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let target_end = (offset + batch_size).min(data.len());
        let search_slice = &data[offset..target_end];
        let actual_end = match memchr::memrchr(b'\n', search_slice) {
            Some(pos) => offset + pos + 1,
            None => target_end,
        };
        v.push(data.slice(offset..actual_end));
        offset = actual_end;
    }
    v
}
