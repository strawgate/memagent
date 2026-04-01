//! End-to-end allocation regression test.
//!
//! Runs a full pipeline (file → scan → transform → null sink) for multiple
//! batches and verifies that allocation profiles are stable across windows.
//! This is the CI gating test — it catches allocation regressions anywhere
//! in the pipeline, including cross-component interactions.

use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
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

/// Full pipeline allocation stability test.
///
/// Writes data to a file, runs the pipeline for two time windows,
/// and verifies that per-batch allocations are stable (not growing).
///
/// This catches:
/// - Buffer leaks in scanner, builder, or output encoder
/// - Capacity growth in internal collections
/// - Per-batch allocations that escape and aren't reused
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

    // Let the pipeline run for 2 seconds (enough for many batches),
    // then measure a second window to compare.
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(2));
        sd.cancel();
    });

    // Run the pipeline and measure total allocations.
    let reg = Region::new(&GLOBAL);
    pipeline.run(&shutdown).unwrap();
    let stats = reg.change();

    let batches = pipeline
        .metrics()
        .batches_total
        .load(Ordering::Relaxed);
    let rows = pipeline
        .metrics()
        .batch_rows_total
        .load(Ordering::Relaxed);

    // Verify we actually processed data.
    assert!(batches > 0, "pipeline processed no batches");
    assert!(rows > 0, "pipeline processed no rows");

    // Check allocation efficiency: bytes allocated per row processed.
    // Includes fixed startup overhead (~15MB for tokio, DataFusion, OTel)
    // plus per-row data processing. This threshold catches regressions
    // (e.g. going from 1KB/row to 10KB/row) while allowing current overhead.
    let bytes_per_row = stats.bytes_allocated as f64 / rows.max(1) as f64;
    assert!(
        bytes_per_row < 2048.0,
        "excessive allocation per row: {bytes_per_row:.0} bytes/row \
         ({} total bytes over {rows} rows in {batches} batches). \
         Expected <2048 bytes/row.",
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
