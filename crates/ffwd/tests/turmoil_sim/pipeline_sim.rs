//! Pipeline lifecycle simulation tests.
//!
//! These test the pipeline's batch accumulation, timeout flushing,
//! and graceful shutdown drain path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ffwd::pipeline::Pipeline;
use ffwd_test_utils::sinks::CountingSink;
use ffwd_types::pipeline::SourceId;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

/// Test: graceful shutdown drains all buffered data.
///
/// Invariant probed: the shutdown drain path (channel drain + accumulator.drain())
/// must flush all data even when the batch timeout is too long to fire.
///
/// The 60s batch timeout ensures data CANNOT flush via timeout — it MUST be
/// drained via the shutdown path. Mutation: delete accumulator.drain() -> fails.
#[test]
fn graceful_shutdown_drains_buffered_data() {
    let mut sim = super::build_sim(30, 10);

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = delivered.clone();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(10);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        // 60s timeout — too long to fire during the 500ms test window.
        // Data MUST be flushed by the shutdown drain path.
        pipeline.set_batch_timeout(Duration::from_secs(60));
        pipeline.set_batch_target_bytes(1024 * 1024);
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    let count = delivered.load(Ordering::Relaxed);
    assert_eq!(
        count, 10,
        "expected all 10 rows drained on shutdown, got {count}"
    );
}

/// Test: batch timeout flush delivers data before shutdown.
///
/// Invariant probed: the flush_interval.tick() branch in select! must flush
/// data below batch_target_bytes after the batch timeout expires.
///
/// Mid-test assertion at 500ms verifies data was delivered by timeout,
/// NOT by the shutdown drain. Mutation: remove flush_interval.tick() arm ->
/// mid-test assertion fails.
#[test]
fn batch_flushes_on_timeout() {
    let mut sim = super::build_sim(30, 1);

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = delivered.clone();
    let mid_test_check = delivered.clone();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(5);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(50));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        let mid = mid_test_check.clone();
        tokio::spawn(async move {
            // Check at 500ms — well after 50ms timeout, well before 2s shutdown.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let count = mid.load(Ordering::Relaxed);
            assert!(
                count > 0,
                "mid-test: expected data delivered by batch timeout flush before shutdown, got 0"
            );
        });

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    let count = delivered.load(Ordering::Relaxed);
    assert_eq!(
        count, 5,
        "expected timeout flush to deliver all 5 rows, got {count}"
    );
}
