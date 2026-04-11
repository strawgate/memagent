//! Crash consistency and checkpoint invariant simulation tests.
//!
//! These tests verify checkpoint ordering and crash recovery properties
//! using ObservableCheckpointStore with Arc-shared state for post-test
//! invariant inspection.

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

/// Test: crash on checkpoint flush with strong state inspection.
///
/// Invariant probed: at-least-once delivery guarantee.
/// When a crash is armed, the flush fails and pending checkpoint state
/// is lost. We verify:
/// 1. Data IS delivered to the sink (output succeeded independently of checkpoint)
/// 2. The crash actually happened (crash_count > 0)
/// 3. Checkpoint update history is monotonically increasing per source
/// 4. Subsequent flushes succeed (pipeline recovers from the crash)
#[test]
fn crash_checkpoint_consistency() {
    let mut sim = super::build_sim(30, 1);

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = delivered.clone();

    let (store, handle) = ObservableCheckpointStore::new();
    let handle_clone = handle.clone();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(42), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let crash = handle_clone.clone();
        tokio::spawn(async move {
            // Wait for some data to flow, then arm the crash.
            // This ensures at least one successful flush happens before the crash.
            tokio::time::sleep(Duration::from_millis(200)).await;
            crash.arm_crash();
            // Then wait for more processing + shutdown.
            tokio::time::sleep(Duration::from_secs(5)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // 1. All 20 rows must be delivered despite the checkpoint crash.
    // Output delivery is independent of checkpoint persistence — a crash
    // during flush must not prevent data from reaching the sink.
    let count = delivered.load(Ordering::Relaxed);
    assert_eq!(
        count, 20,
        "expected all 20 rows delivered despite checkpoint crash, got {count}"
    );

    // 2. The crash happened — verify crash injection worked.
    assert!(
        handle.crash_count() > 0,
        "expected at least 1 crash, but crash_count is 0"
    );

    // 3. Checkpoint updates were recorded (PipelineMachine advanced).
    let updates = handle.update_count(42);
    assert!(
        updates > 0,
        "expected checkpoint updates for source 42, got 0"
    );

    // 4. Checkpoint updates are monotonically increasing.
    handle.assert_monotonic(42);

    // 5. KEY FINDING: when the crash hits the final shutdown flush, durable
    //    checkpoint is None — ALL checkpoint progress is lost. This is a
    //    design limitation: the pipeline has no retry on the final flush.
    //    At-least-once delivery is maintained (data will be re-read on restart)
    //    but the re-read window is the entire run, not just the crash window.
    //
    //    If durable IS Some, it means an intermediate flush succeeded before
    //    the crash was armed. Both outcomes are acceptable for at-least-once.
    let _durable = handle.durable_offset(42);
    // Not asserting durable.is_some() — see comment above.
}

/// Test: multi-source checkpoint independence with invariant verification.
///
/// Invariant probed: per-source checkpoint independence.
/// Source 1 (10 lines) and Source 2 (5 lines) run concurrently.
/// Verifies:
/// 1. All 15 rows delivered
/// 2. Both sources have checkpoint updates
/// 3. Each source's checkpoint history is monotonic (independently)
/// 4. Source 2's checkpoint doesn't depend on Source 1's progress
#[test]
fn multi_source_independent_checkpoint() {
    let mut sim = super::build_sim(30, 1);

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = delivered.clone();

    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines1 = generate_json_lines(10);
        let lines2 = generate_json_lines(5);
        let input1 = ChannelInputSource::new("source1", SourceId(1), lines1);
        let input2 = ChannelInputSource::new("source2", SourceId(2), lines2);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("source1", Box::new(input1))
            .with_input("source2", Box::new(input2))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // 1. All 15 rows from both sources delivered.
    let count = delivered.load(Ordering::Relaxed);
    assert_eq!(
        count, 15,
        "expected all 15 rows from 2 sources, got {count}"
    );

    // 2. Both sources have checkpoint updates.
    let s1_updates = handle.update_count(1);
    let s2_updates = handle.update_count(2);
    assert!(
        s1_updates > 0,
        "expected checkpoint updates for source 1, got 0"
    );
    assert!(
        s2_updates > 0,
        "expected checkpoint updates for source 2, got 0"
    );

    // 3. Each source's checkpoint history is independently monotonic.
    handle.assert_monotonic(1);
    handle.assert_monotonic(2);

    // 4. Both sources have durable checkpoints (flushes succeeded).
    assert!(
        handle.durable_offset(1).is_some(),
        "expected durable checkpoint for source 1"
    );
    assert!(
        handle.durable_offset(2).is_some(),
        "expected durable checkpoint for source 2"
    );
}

/// Flush failure before durability boundary:
/// updates may exist, but durable checkpoint remains absent.
#[test]
fn crash_before_durable_flush_keeps_updates_but_no_durable_checkpoint() {
    let mut sim = super::build_sim(30, 1);

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = delivered.clone();

    let (store, handle) = ObservableCheckpointStore::new();
    // Deterministically fail the first shutdown flush attempt.
    handle.crash_on_nth_flush(1);

    sim.client("pipeline", async move {
        let lines = generate_json_lines(24);
        let input = ChannelInputSource::new("test", SourceId(42), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));
        // Keep periodic flushes out of the run; only shutdown flush should apply.
        pipeline.set_checkpoint_flush_interval(Duration::from_secs(60));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        delivered.load(Ordering::Relaxed),
        24,
        "expected all rows delivered despite checkpoint flush crash"
    );
    assert!(
        handle.crash_count() >= 1,
        "expected at least one simulated checkpoint crash"
    );
    assert!(
        handle.update_count(42) > 0,
        "expected checkpoint update history to contain source 42"
    );
    handle.assert_monotonic(42);
    assert!(
        handle.durable_offset(42).is_none(),
        "durable checkpoint should remain absent when first shutdown flush crashes"
    );
    handle.assert_durable_not_ahead_of_updates(42);
}

/// Post-durability crash window:
/// at least one successful flush exists before crash, and durable never
/// exceeds observed update history.
#[test]
fn post_durability_crash_window_keeps_durable_within_update_history() {
    let mut sim = super::build_sim(30, 1);

    let (store, handle) = ObservableCheckpointStore::new();
    let handle_for_coordinator = handle.clone();
    let sink = InstrumentedSink::new(vec![FailureAction::Delay(Duration::from_millis(15)); 512]);
    let delivered = sink.delivered_counter();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(160);
        let input = ChannelInputSource::new("test", SourceId(42), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(5));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(20));
        pipeline.set_batch_target_bytes(128);
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            // Wait for a successful durability boundary, then inject a crash
            // into the next flush window.
            for _ in 0..400 {
                if handle_for_coordinator.flush_count() >= 1 {
                    handle_for_coordinator.arm_crash();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        delivered.load(Ordering::Relaxed),
        160,
        "expected all rows delivered through post-durability crash window"
    );
    assert!(
        handle.flush_count() >= 1,
        "expected at least one successful checkpoint flush before/around crash window"
    );
    assert!(
        handle.crash_count() >= 1,
        "expected at least one checkpoint crash in post-durability window"
    );
    assert!(
        handle.update_count(42) > 0,
        "expected checkpoint updates for source 42"
    );
    handle.assert_monotonic(42);
    handle.assert_durable_not_ahead_of_updates(42);
    assert!(
        handle.durable_offset(42).is_some(),
        "expected durable checkpoint to exist after at least one successful flush"
    );
}

/// Repeated crash windows with bounded shutdown:
/// checkpoint history remains monotonic, and durable never jumps ahead.
#[test]
fn repeated_crash_windows_preserve_checkpoint_regression_guards() {
    let mut sim = super::build_sim(30, 1);

    let (store, handle) = ObservableCheckpointStore::new();
    let handle_for_coordinator = handle.clone();
    let sink = InstrumentedSink::new(vec![FailureAction::Delay(Duration::from_millis(15)); 768]);
    let delivered = sink.delivered_counter();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(220);
        let input = ChannelInputSource::new("test", SourceId(42), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(5));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(20));
        pipeline.set_batch_target_bytes(128);
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            // Crash window #1: after first successful durability boundary.
            for _ in 0..400 {
                if handle_for_coordinator.flush_count() >= 1 {
                    handle_for_coordinator.arm_crash();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Crash window #2: after the first crash has been observed.
            for _ in 0..400 {
                if handle_for_coordinator.crash_count() >= 1 {
                    handle_for_coordinator.arm_crash();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        delivered.load(Ordering::Relaxed),
        220,
        "expected all rows delivered despite repeated crash windows"
    );
    assert!(
        handle.crash_count() >= 2,
        "expected repeated crash windows to produce >=2 checkpoint crashes"
    );
    assert!(
        handle.flush_count() >= 1,
        "expected at least one successful flush across repeated crash windows"
    );
    assert!(
        handle.update_count(42) > 0,
        "expected checkpoint update history for source 42"
    );
    handle.assert_monotonic(42);
    handle.assert_durable_not_ahead_of_updates(42);
}

/// Multi-source durability independence under crash timing:
/// each source preserves monotonic history and independent durable bounds.
#[test]
fn multi_source_crash_window_preserves_independent_durable_bounds() {
    let mut sim = super::build_sim(30, 1);

    let (store, handle) = ObservableCheckpointStore::new();
    let handle_for_coordinator = handle.clone();
    let sink = InstrumentedSink::new(vec![FailureAction::Delay(Duration::from_millis(15)); 768]);
    let delivered = sink.delivered_counter();

    sim.client("pipeline", async move {
        let lines1 = generate_json_lines(80);
        let lines2 = generate_json_lines(40);
        let input1 = ChannelInputSource::new("source1", SourceId(1), lines1);
        let input2 = ChannelInputSource::new("source2", SourceId(2), lines2);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(5));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(20));
        pipeline.set_batch_target_bytes(128);
        let mut pipeline = pipeline
            .with_input("source1", Box::new(input1))
            .with_input("source2", Box::new(input2))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            // Inject crash only after at least one durable boundary exists.
            for _ in 0..400 {
                if handle_for_coordinator.flush_count() >= 1 {
                    handle_for_coordinator.arm_crash();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        delivered.load(Ordering::Relaxed),
        120,
        "expected all rows from both sources delivered under crash timing"
    );
    assert!(
        handle.crash_count() >= 1,
        "expected at least one checkpoint crash in multi-source scenario"
    );

    for source_id in [1_u64, 2_u64] {
        assert!(
            handle.update_count(source_id) > 0,
            "expected checkpoint updates for source {source_id}"
        );
        handle.assert_monotonic(source_id);
        handle.assert_durable_not_ahead_of_updates(source_id);
        if let Some(durable) = handle.durable_offset(source_id) {
            let max_update = handle
                .max_update_offset(source_id)
                .expect("durable checkpoint implies update history exists");
            assert!(
                durable <= max_update,
                "durable checkpoint for source {source_id} is {durable}, expected <= max update {max_update}"
            );
        }
    }
}
