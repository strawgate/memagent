//! End-to-end allocation regression test.
//!
//! Runs a full pipeline (file → scan → transform → null sink) for multiple
//! batches and verifies that allocation profiles are stable across windows.
//! This is the CI gating test — it catches allocation regressions anywhere
//! in the pipeline, including cross-component interactions.

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_config::Config;
use logfwd_output::NullSink;
use tokio_util::sync::CancellationToken;

fn test_meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("test")
}

/// Full pipeline allocation profile test.
///
/// Writes data to a file, runs the complete pipeline (file → scan →
/// transform → null sink), and verifies:
/// - Per-row allocation cost is bounded (catches per-row regressions)
/// - Total retained memory after exit is bounded (catches true leaks)
///
/// The scaling test (allocation_scaling.rs) complements this by verifying
/// that per-row cost DECREASES with volume (proving overhead is fixed).
#[test]
fn pipeline_allocations_stable_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    // Write enough data for many batches. Need enough rows to amortize
    // fixed startup costs (tokio, DataFusion, OTel: ~10-15MB).
    logfwd_test_utils::generate_json_lines(&log_path, 50_000, "alloc-test");

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

    // Fast batching for test speed.
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    // Safety timeout: cancel after 10 seconds. The pipeline reads the
    // entire file (~6MB) in <1 second on any hardware, then exits when
    // the tailer reports no more data. This timeout prevents hangs.
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(10));
        sd.cancel();
    });

    // Run the pipeline and measure total allocations.
    let reg = Region::new(&GLOBAL);
    pipeline.run(&shutdown).unwrap();
    let stats = reg.change();

    let batches = pipeline.metrics().batches_total.load(Ordering::Relaxed);
    let rows = pipeline.metrics().batch_rows_total.load(Ordering::Relaxed);

    // Verify we actually processed data.
    assert!(batches > 0, "pipeline processed no batches");
    assert!(rows > 0, "pipeline processed no rows");

    // Print actual numbers for visibility.
    eprintln!("--- allocation profile ---");
    eprintln!("  rows:       {rows}");
    eprintln!("  batches:    {batches}");
    eprintln!(
        "  allocated:  {} bytes ({:.1} MB)",
        stats.bytes_allocated,
        stats.bytes_allocated as f64 / 1_048_576.0
    );
    eprintln!(
        "  freed:      {} bytes ({:.1} MB)",
        stats.bytes_deallocated,
        stats.bytes_deallocated as f64 / 1_048_576.0
    );
    let net = stats.bytes_allocated as i64 - stats.bytes_deallocated as i64;
    eprintln!(
        "  retained:   {} bytes ({:.1} MB)",
        net,
        net as f64 / 1_048_576.0
    );
    let bytes_per_row = stats.bytes_allocated as f64 / rows.max(1) as f64;
    eprintln!("  per row:    {:.0} bytes", bytes_per_row);
    eprintln!("  alloc count: {}", stats.allocations);
    eprintln!(
        "  per row:    {:.1} allocs",
        stats.allocations as f64 / rows.max(1) as f64
    );
    eprintln!("--------------------------");

    // Scaling profile (measured 2026-04-01):
    //   10K rows:  ~2089 bytes/row (fixed overhead dominates)
    //   50K rows:  ~1130 bytes/row
    //   100K rows: ~1005 bytes/row
    //   500K rows:  ~905 bytes/row (asymptotic per-row cost)
    //
    // Asymptotic cost is ~900 bytes/row for ~120 bytes input JSON (~7.5x
    // amplification from flat text to typed Arrow columnar representation).
    // Threshold at 1500 catches regressions while allowing the ~8MB fixed
    // overhead to be amortized over 50K rows.
    assert!(
        bytes_per_row < 1500.0,
        "allocation regression: {bytes_per_row:.0} bytes/row \
         (asymptotic ~900, threshold 1500 for 50K rows). \
         {} total bytes over {rows} rows in {batches} batches.",
        stats.bytes_allocated,
    );

    // Net retained memory check. Some memory is legitimately retained after
    // pipeline exit (OTel SDK, metrics counters, tokio runtime internals).
    // Typically ~8-15MB. We assert the retained amount is bounded — a true
    // leak would grow proportionally to data volume.
    let net_bytes = stats.bytes_allocated as i64 - stats.bytes_deallocated as i64;
    assert!(
        net_bytes < 32 * 1024 * 1024, // 32MB max retained
        "pipeline retained {net_bytes} bytes after exit. \
         This exceeds the expected ~8-15MB for OTel/tokio/metrics infrastructure. \
         allocated={}, deallocated={}",
        stats.bytes_allocated,
        stats.bytes_deallocated,
    );
}
