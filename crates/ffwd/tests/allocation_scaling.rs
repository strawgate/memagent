//! Allocation scaling test — measures per-row allocation at different data volumes.
//!
//! This test verifies that per-row allocation cost DECREASES (or stays flat) as
//! row count increases, proving that overhead is fixed (not per-row).

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ffwd::pipeline::Pipeline;
use ffwd_config::Config;
use tokio_util::sync::CancellationToken;

fn test_meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("test")
}

fn measure_pipeline(row_count: usize) -> (u64, usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    ffwd_test_utils::generate_json_lines(&log_path, row_count, "scale-test");

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

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let metrics = Arc::clone(pipeline.metrics());
    let expected = row_count as u64;

    // Cancel as soon as all expected rows are processed, or after 30s safety timeout.
    // This makes the test data-driven, not time-driven.
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if metrics.batch_rows_total.load(Ordering::Relaxed) >= expected {
                // All rows processed — give a brief moment for output flush,
                // then cancel.
                std::thread::sleep(Duration::from_millis(100));
                sd.cancel();
                return;
            }
            if std::time::Instant::now() > deadline {
                sd.cancel();
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    });

    let reg = Region::new(GLOBAL);
    pipeline.run(&shutdown).unwrap();
    let stats = reg.change();

    let rows = pipeline.metrics().batch_rows_total.load(Ordering::Relaxed);
    let batches = pipeline.metrics().batches_total.load(Ordering::Relaxed);

    // Don't accept partial runs from safety timeout.
    assert_eq!(
        rows, expected,
        "pipeline processed {rows}/{expected} rows — safety timeout may have fired"
    );

    (
        stats.bytes_allocated as u64,
        rows as usize,
        batches as usize,
    )
}

#[test]
#[ignore = "scaling benchmark: run with `cargo nextest run -- --ignored allocation_per_row_decreases_with_scale`"]
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

    // At 500K rows, per-row cost should be strictly below the 10K baseline.
    // Fixed overhead (~8MB) is amortized over 50x more rows — per-row cost
    // must decrease. Baseline: 10K≈2089, 500K≈905.
    assert!(
        per_row_500k < per_row_10k,
        "per-row cost at 500K ({per_row_500k:.0}) should be below 10K baseline ({per_row_10k:.0})"
    );
}
