//! Pipeline lifecycle simulation tests.
//!
//! These test the pipeline's batch accumulation, timeout flushing,
//! and graceful shutdown drain path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_test_utils::sinks::CountingSink;
use logfwd_types::pipeline::SourceId;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSink};
use super::observable_checkpoint::ObservableCheckpointStore;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

fn total_bytes(lines: &[Vec<u8>]) -> u64 {
    lines.iter().map(|line| line.len() as u64).sum()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunOutcome {
    delivered: u64,
    durable: Option<u64>,
    updates: usize,
    calls: u64,
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
    let lines = generate_json_lines(50);
    let expected_rows = lines.len() as u64;
    let expected_bytes = total_bytes(&lines);

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = delivered.clone();
    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_target_bytes(128 * 1024);
        // 60s timeout — too long to fire during the 500ms test window.
        // Data MUST be flushed by the shutdown drain path.
        pipeline.set_batch_timeout(Duration::from_secs(60));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(0));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    let count = delivered.load(Ordering::Relaxed);
    assert_eq!(
        count, expected_rows,
        "expected all {expected_rows} rows drained on shutdown, got {count}"
    );
    assert_eq!(
        handle.durable_offset(1),
        Some(expected_bytes),
        "expected durable checkpoint to reach {expected_bytes} bytes"
    );
    handle.assert_monotonic(1);
    handle.assert_durable_not_ahead_of_updates(1);
    assert!(
        handle.update_count(1) > 0,
        "expected checkpoint updates for source 1"
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

/// Test: shutdown during slow output still completes and persists checkpoints.
///
/// Invariant probed: the drain path must wait for an in-flight slow `send_batch`
/// to complete without hanging, and the final durable checkpoint must reflect
/// all delivered rows.
#[test]
fn shutdown_waits_for_slow_output_and_persists_checkpoint() {
    let mut sim = super::build_sim(30, 1);
    let lines = generate_json_lines(24);
    let expected_rows = lines.len() as u64;
    let expected_bytes = total_bytes(&lines);

    let sink = InstrumentedSink::new(vec![FailureAction::Delay(Duration::from_millis(200))]);
    let delivered_counter = sink.delivered_counter();
    let call_counter = sink.call_counter();
    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_target_bytes(128 * 1024);
        pipeline.set_batch_timeout(Duration::from_secs(60));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(0));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    let delivered = delivered_counter.load(Ordering::Relaxed);
    let calls = call_counter.load(Ordering::Relaxed);
    assert_eq!(
        delivered, expected_rows,
        "expected slow output to deliver all {expected_rows} rows, got {delivered}"
    );
    assert_eq!(
        calls, 1,
        "expected one send_batch call for the single batch"
    );
    assert_eq!(
        handle.durable_offset(1),
        Some(expected_bytes),
        "expected durable checkpoint to reach {expected_bytes} bytes after slow output"
    );
    handle.assert_monotonic(1);
    handle.assert_durable_not_ahead_of_updates(1);
    assert!(
        handle.update_count(1) > 0,
        "expected checkpoint updates for source 1 after slow output"
    );
}

fn run_retry_then_delay_shutdown_once() -> RunOutcome {
    let mut sim = super::build_sim(30, 1);
    let lines = generate_json_lines(12);
    let sink = InstrumentedSink::new(vec![
        FailureAction::RetryAfter(Duration::from_millis(20)),
        FailureAction::Delay(Duration::from_millis(100)),
    ]);
    let delivered_counter = sink.delivered_counter();
    let call_counter = sink.call_counter();
    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_target_bytes(128 * 1024);
        pipeline.set_batch_timeout(Duration::from_secs(60));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(0));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    RunOutcome {
        delivered: delivered_counter.load(Ordering::Relaxed),
        durable: handle.durable_offset(1),
        updates: handle.update_count(1),
        calls: call_counter.load(Ordering::Relaxed),
    }
}

/// Test: shutdown during retry-then-delay work is deterministic under the same seed.
///
/// Invariant probed: the worker pool must survive a retry path followed by a
/// delayed second attempt, then shut down cleanly once cancellation arrives.
/// Running the same simulation twice with the same Turmoil seed should yield
/// the same outcome.
#[test]
fn shutdown_during_retry_then_delay_is_deterministic_under_same_seed() {
    let first = run_retry_then_delay_shutdown_once();
    let second = run_retry_then_delay_shutdown_once();
    let expected_bytes = total_bytes(&generate_json_lines(12));

    assert_eq!(
        first, second,
        "expected repeated runs with the same TURMOIL_SEED to match exactly"
    );
    assert_eq!(
        first.delivered, 12,
        "expected all rows delivered after retry and delay"
    );
    assert_eq!(
        first.durable,
        Some(expected_bytes),
        "expected durable checkpoint to advance after retry and delay"
    );
    assert!(
        first.updates > 0,
        "expected checkpoint updates after retry and delay"
    );
    assert_eq!(first.calls, 2, "expected one retry and one successful send");
}
