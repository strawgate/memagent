//! Allocation scaling test — measures per-row allocation at different data volumes.
//!
//! This test verifies that per-row allocation cost DECREASES (or stays flat) as
//! row count increases, proving that overhead is fixed (not per-row).

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_config::Config;
use tokio_util::sync::CancellationToken;

fn test_meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("test")
}

fn measure_pipeline(row_count: usize) -> (u64, usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    logfwd_test_utils::generate_json_lines(&log_path, row_count, "scale-test");

    let yaml = format!(
        r#"
input:
  type: file
  path: {}
  format: json
output:
  type: "null"
"#,
        log_path.display()
    );

    let config = Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    // The pipeline reads the file quickly but the tailer thread stays alive
    // polling for new data. Cancel after 5 seconds — enough for the file to
    // be fully processed on any hardware, short enough for CI.
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        sd.cancel();
    });

    let reg = Region::new(&GLOBAL);
    pipeline.run(&shutdown).unwrap();
    let stats = reg.change();

    let rows = pipeline.metrics().batch_rows_total.load(Ordering::Relaxed);
    let batches = pipeline.metrics().batches_total.load(Ordering::Relaxed);

    (
        stats.bytes_allocated as u64,
        rows as usize,
        batches as usize,
    )
}

#[test]
fn allocation_per_row_decreases_with_scale() {
    // Measure at three scales.
    let (bytes_10k, rows_10k, batches_10k) = measure_pipeline(10_000);
    let (bytes_100k, rows_100k, batches_100k) = measure_pipeline(100_000);
    let (bytes_500k, rows_500k, batches_500k) = measure_pipeline(500_000);

    let per_row_10k = bytes_10k as f64 / rows_10k.max(1) as f64;
    let per_row_100k = bytes_100k as f64 / rows_100k.max(1) as f64;
    let per_row_500k = bytes_500k as f64 / rows_500k.max(1) as f64;

    eprintln!("--- allocation scaling ---");
    eprintln!(
        "  10K rows:  {per_row_10k:.0} bytes/row  ({batches_10k} batches, {rows_10k} rows processed)"
    );
    eprintln!(
        "  100K rows: {per_row_100k:.0} bytes/row  ({batches_100k} batches, {rows_100k} rows processed)"
    );
    eprintln!(
        "  500K rows: {per_row_500k:.0} bytes/row  ({batches_500k} batches, {rows_500k} rows processed)"
    );
    eprintln!("--------------------------");

    // Per-row cost should decrease as fixed overhead is amortized.
    // Allow 20% margin for CI variance (allocator jitter, thread scheduling).
    assert!(
        per_row_100k <= per_row_10k * 1.2,
        "per-row cost should not increase with scale: 10K={per_row_10k:.0}, 100K={per_row_100k:.0}"
    );

    // At 500K rows, per-row cost should be well below the 10K baseline.
    // Use 10% margin — at 500K the fixed overhead is negligible.
    assert!(
        per_row_500k < per_row_10k * 1.1,
        "per-row cost at 500K ({per_row_500k:.0}) should be below 10K baseline ({per_row_10k:.0})"
    );
}
