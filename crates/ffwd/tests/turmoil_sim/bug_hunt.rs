//! Bug-hunting simulation tests targeting specific audit-identified issues.
//!
//! Each test targets a concrete bug hypothesis from a code audit. Tests
//! that expose real bugs document the actual (possibly wrong) behavior
//! with comments explaining why it differs from the expected behavior.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ffwd::pipeline::Pipeline;
use ffwd_types::pipeline::SourceId;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSinkFactory};
use super::observable_checkpoint::ObservableCheckpointStore;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

fn generate_fixed_width_json_lines(n: usize, payload_bytes: usize) -> Vec<Vec<u8>> {
    let payload = "x".repeat(payload_bytes);
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"payload\":\"{payload}\"}}\n").into_bytes())
        .collect()
}

fn total_bytes(lines: &[Vec<u8>]) -> u64 {
    lines.iter().map(|line| line.len() as u64).sum()
}

// ---------------------------------------------------------------------------
// Test 1: Ack starvation under sustained load
// ---------------------------------------------------------------------------
//
// Bug hypothesis: The `select!` loop in `run_async` is unbiased. Under
// sustained input, `rx.recv()` always wins over `ack_rx.recv()`, causing
// in_flight to grow and checkpoints to never advance during normal
// processing. Acks would only be processed after the select! loop breaks.
//
// Expected outcome if bug exists:
//   - checkpoint update count == 0 or 1 (only final shutdown flush)
//   - in_flight stays high until drain
//
// Expected outcome if no bug:
//   - checkpoint advances during processing (update count > 1)
//   - in_flight returns to 0

#[test]
fn ack_starvation_under_sustained_load() {
    let mut sim = super::build_sim(120, 1);

    // 1 worker with 50ms delay per batch: slow enough that acks queue up
    // while new data keeps arriving.
    let mut script = Vec::new();
    for _ in 0..200 {
        script.push(FailureAction::Delay(Duration::from_millis(50)));
    }
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![script]));
    let delivered_counter = factory.delivered_counter();

    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        // 100 lines with small batch_target_bytes forces many batches.
        let lines = generate_json_lines(100);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_batch_target_bytes(1024);
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(100));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // All 100 rows must be delivered.
    let delivered = delivered_counter.load(Ordering::Relaxed);
    assert_eq!(
        delivered, 100,
        "expected all 100 rows delivered, got {delivered}"
    );

    // KEY ASSERTION: checkpoint must have advanced more than once.
    // If ack starvation exists, we'd see 0 or 1 updates (only the final
    // shutdown flush), not multiple intermediate advances.
    let updates = ckpt_handle.update_count(1);
    assert!(
        updates > 1,
        "BUG: ack starvation detected — only {updates} checkpoint update(s). \
         Expected multiple intermediate checkpoint advances during processing. \
         The select! loop may be starving ack_rx.recv() in favor of rx.recv()."
    );

    // Durable checkpoint must exist and be > 0.
    let durable = ckpt_handle.durable_offset(1);
    assert!(
        durable.is_some() && durable.unwrap() > 0,
        "expected durable checkpoint offset > 0, got {durable:?}"
    );

    // Checkpoint history must be monotonic.
    ckpt_handle.assert_monotonic(1);
}

// ---------------------------------------------------------------------------
// Test 2: Worker panic does not block drain
// ---------------------------------------------------------------------------
//
// Bug hypothesis: If `Sink::send_batch()` panics, the worker task dies,
// the `BatchTicket<Sending>` is leaked (never acked/rejected), and
// `PipelineMachine::in_flight` permanently contains the batch.
// `draining.stop()` returns `Err`, pipeline falls to `force_stop()`.
//
// Expected outcome: pipeline completes without hanging. Some rows delivered
// (batches after the panic succeed via worker respawn or other workers).

#[test]
fn worker_panic_does_not_block_drain() {
    let mut sim = super::build_sim(120, 1);

    // Worker script: first batch panics, subsequent batches succeed.
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![vec![
        FailureAction::Panic,
    ]]));
    let delivered_counter = factory.delivered_counter();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            sd.cancel();
        });

        // The pipeline must NOT hang despite the worker panic.
        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    // If the pipeline deadlocks due to the leaked ticket, the simulation
    // will time out and sim.run() will return an error.
    let result = sim.run();
    assert!(
        result.is_ok(),
        "pipeline hung after worker panic — likely deadlocked on in-flight batch. \
         Error: {result:?}"
    );

    // Some rows may or may not be delivered depending on whether the worker
    // pool can recover from the panic. Document actual behavior.
    let delivered = delivered_counter.load(Ordering::Relaxed);
    // NOTE: With 1 worker, if the worker panics and the pool doesn't respawn,
    // no subsequent batches can be delivered. The key assertion is that the
    // pipeline COMPLETES (above), not that all rows are delivered.
    //
    // If delivered > 0, the pool successfully recovered from the panic.
    // If delivered == 0, all batches after the panic were also lost (the
    // single worker died and wasn't replaced).
    eprintln!(
        "worker_panic test: {delivered} rows delivered out of 20 \
         (0 is acceptable if worker pool doesn't respawn after panic)"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Explicit permanent reject checkpoint behavior
// ---------------------------------------------------------------------------
//
// Contract: explicit permanent rejects still advance checkpoints. If a sink
// returns a true non-retriable reject, replaying the same bytes forever would
// create an infinite loop.
//
// This test documents the intended behavior for explicit permanent rejects.

#[test]
fn rejected_batch_checkpoint_behavior() {
    let mut sim = super::build_sim(120, 1);

    // Script: first batch returns Reject, rest succeed.
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![vec![
        FailureAction::Reject("test rejection".to_string()),
    ]]));
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();

    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(100));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    let delivered = delivered_counter.load(Ordering::Relaxed);
    let calls = call_counter.load(Ordering::Relaxed);

    // The first batch was rejected, so delivered < 20 (unless all lines
    // ended up in the first batch and got rejected, then delivered could be 0).
    // Subsequent batches succeed.
    eprintln!("rejected_batch test: {delivered} rows delivered, {calls} send_batch calls");

    // DOCUMENT: explicit permanent rejects still advance the checkpoint.
    // In pipeline.rs `ack_all_tickets`, reject calls `ticket.reject()` which
    // still produces an AckReceipt that feeds into `record_ack_and_advance`.
    let durable = ckpt_handle.durable_offset(1);
    let updates = ckpt_handle.update_count(1);

    eprintln!("rejected_batch test: durable offset = {durable:?}, checkpoint updates = {updates}");

    // All 20 lines fit in 1 batch (64KB target). That batch is rejected.
    assert_eq!(
        delivered, 0,
        "expected 0 rows delivered (batch was rejected)"
    );
    assert_eq!(
        calls, 1,
        "expected exactly 1 send_batch call (1 batch, rejected)"
    );

    assert!(
        durable.is_some() && durable.unwrap() > 0,
        "permanent rejects must still advance checkpoint progress; \
         durable={durable:?}"
    );

    // Checkpoint history must be monotonic regardless of reject behavior.
    ckpt_handle.assert_monotonic(1);
}

// ---------------------------------------------------------------------------
// Test 4: Persistent transient failures must not advance checkpoints
// ---------------------------------------------------------------------------
//
// Contract: retry/control-plane failures should hold checkpoint progress until
// delivery eventually succeeds or shutdown cancels the in-flight batch.

#[test]
fn retry_exhausted_does_not_advance_checkpoint() {
    let mut sim = super::build_sim(120, 1);

    let factory = Arc::new(InstrumentedSinkFactory::new(vec![vec![
        FailureAction::RepeatIoError(std::io::ErrorKind::TimedOut),
    ]]));
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();

    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(100));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    let delivered = delivered_counter.load(Ordering::Relaxed);
    let calls = call_counter.load(Ordering::Relaxed);
    let durable = ckpt_handle.durable_offset(1);
    let updates = ckpt_handle.update_count(1);

    eprintln!(
        "persistent_failure test: delivered={delivered} calls={calls} durable={durable:?} updates={updates}"
    );

    assert_eq!(
        delivered, 0,
        "expected no rows delivered while failures persist through shutdown"
    );
    assert!(
        calls >= 4,
        "expected at least 4 retry attempts before shutdown cancellation"
    );
    assert!(
        durable.is_none(),
        "persistent transient failures must not advance checkpoint progress; durable={durable:?}"
    );
    assert_eq!(
        updates, 0,
        "persistent transient failures must not write checkpoint updates"
    );
}

// ---------------------------------------------------------------------------
// Test 5: RetryAfter respects server-specified backoff timing
// ---------------------------------------------------------------------------
//
// Bug hypothesis: `process_item` resets `delay = Duration::from_millis(100)`
// after every `RetryAfter`, defeating exponential backoff. A server that
// responds with `RetryAfter(1s)` gets hammered every 100ms on subsequent
// IO errors.
//
// However, the delay reset only affects IO-error backoff AFTER a RetryAfter.
// The RetryAfter itself uses the server-specified duration. So the real
// concern is: does the pipeline honor the server's RetryAfter duration and
// not make extra calls?

#[test]
fn retry_after_respects_server_backoff() {
    let mut sim = super::build_sim(120, 1);

    // Script: 2 RetryAfter(1s), then succeed.
    // With 1 worker, all batches go to the same worker, and this script
    // applies to the FIRST batch only (subsequent batches use default Succeed).
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![vec![
        FailureAction::RetryAfter(Duration::from_secs(1)),
        FailureAction::RetryAfter(Duration::from_secs(1)),
        FailureAction::Succeed,
    ]]));
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(10);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // All 10 rows must be delivered.
    let delivered = delivered_counter.load(Ordering::Relaxed);
    assert_eq!(
        delivered, 10,
        "expected all 10 rows delivered, got {delivered}"
    );

    // KEY ASSERTION: The sink should have been called at least 3 times for
    // the first batch (2 RetryAfter + 1 Succeed), plus at least 1 more for
    // subsequent batches. The exact count depends on how many batches are
    // formed from 10 lines.
    let calls = call_counter.load(Ordering::Relaxed);

    // At minimum: 3 calls for first batch (2 retry + 1 succeed) + 0 or more
    // for additional batches.
    assert!(
        calls >= 3,
        "expected >= 3 send_batch calls (2 retries + 1 success for first batch), \
         got {calls}"
    );

    eprintln!("retry_after test: {calls} total send_batch calls, {delivered} rows delivered");

    // DOCUMENT: The delay reset after RetryAfter (`delay = Duration::from_millis(100)`)
    // is intentional per the code comment: "Reset delay to initial - server specified
    // the backoff, don't compound it." This means:
    //
    //   RetryAfter(1s) -> sleep 1s -> RetryAfter(1s) -> sleep 1s -> Succeed
    //
    // The delay variable (used for IO error exponential backoff) is reset to 100ms
    // after each RetryAfter. This is correct behavior: the server explicitly told
    // us how long to wait, so we shouldn't compound it with our own backoff.
    //
    // If an IO error follows a RetryAfter, the backoff restarts from 100ms.
    // This is arguably a minor issue (the server's rate-limit hint is lost) but
    // in practice, RetryAfter followed by IO error is an unusual sequence.
}

// ---------------------------------------------------------------------------
// Test 6: Panic on first batch triggers terminal shutdown, checkpoint held
// ---------------------------------------------------------------------------
//
// Bug hypothesis: a sink panic could unwind before the ack/checkpoint seam is
// resolved, leaving unresolved tickets and either hanging shutdown or
// accidentally advancing checkpoints.
//
// Expected invariant (post #1808 terminal-hold model):
// - pipeline terminates (no hang bound breach),
// - held ticket triggers immediate shutdown (no worker recycling),
// - durable checkpoint never advances while the first failed ticket remains held.

#[test]
fn panic_first_batch_triggers_terminal_hold_without_checkpoint_advance() {
    let mut sim = super::build_sim(90, 1);

    // Factory scripts are popped from the end:
    // 1st worker => [Panic]. Under the terminal-hold model, the pipeline
    // shuts down after the panic ack without recycling workers.
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![
        vec![],
        vec![FailureAction::Panic],
    ]));
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();
    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_fixed_width_json_lines(10, 96);
        let input = ChannelInputSource::new("panic-first", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_target_bytes(1);
        pipeline.set_batch_timeout(Duration::from_millis(10));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(25));
        let mut pipeline = pipeline
            .with_input("panic-first", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    let run_result = sim.run();
    assert!(
        run_result.is_ok(),
        "pipeline must terminate after panic path; got {run_result:?}"
    );

    let calls = call_counter.load(Ordering::Relaxed);
    let delivered = delivered_counter.load(Ordering::Relaxed);
    eprintln!(
        "panic_first_batch test: {delivered} rows delivered, {calls} calls \
         (terminal-hold model stops ingestion after panic ack)"
    );

    // Under the terminal-hold model, the pipeline shuts down immediately
    // after the panic ack. The first batch panics and no subsequent batches
    // are processed because ingestion is stopped — no worker recycling.
    assert_eq!(
        calls, 1,
        "terminal-hold model must issue exactly 1 send call (the panic); calls={calls}"
    );

    // Terminal hold means zero deliveries: the only batch panicked.
    assert_eq!(
        delivered, 0,
        "terminal-hold model must not deliver any rows after panic; delivered={delivered}"
    );

    // Held first ticket must block checkpoint advancement entirely.
    assert!(
        ckpt_handle.durable_offset(1).is_none(),
        "durable checkpoint must remain absent while first failed ticket is held"
    );
    assert_eq!(
        ckpt_handle.update_count(1),
        0,
        "held panic outcome must not emit checkpoint updates"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Mid-stream panic does not silently advance past unresolved gap
// ---------------------------------------------------------------------------
//
// Bug hypothesis: after at least one successful ack, a later panic could still
// let checkpoints drift forward via subsequent successful batches.
//
// Expected invariant (post #1808 terminal-hold model):
// - some data is delivered before the panic,
// - terminal hold stops ingestion after the panic ack (no recovery batches),
// - checkpoint remains strictly behind end-of-input once a gap is introduced,
// - monotonicity and "durable not ahead of updates" are preserved.

#[test]
fn panic_after_initial_success_does_not_advance_checkpoint_past_gap() {
    let mut sim = super::build_sim(90, 1);

    // First worker: success then panic. Under the terminal-hold model, the
    // pipeline shuts down after the panic ack without recycling workers.
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![
        vec![],
        vec![FailureAction::Succeed, FailureAction::Panic],
    ]));
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();
    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    let lines = generate_fixed_width_json_lines(12, 96);
    let input_total_bytes = total_bytes(&lines);

    sim.client("pipeline", async move {
        let input = ChannelInputSource::new("panic-midstream", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_target_bytes(1);
        pipeline.set_batch_timeout(Duration::from_millis(10));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(25));
        let mut pipeline = pipeline
            .with_input("panic-midstream", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    let run_result = sim.run();
    assert!(
        run_result.is_ok(),
        "pipeline must terminate under mid-stream panic path; got {run_result:?}"
    );

    let delivered = delivered_counter.load(Ordering::Relaxed);
    let calls = call_counter.load(Ordering::Relaxed);
    let durable = ckpt_handle.durable_offset(1);

    eprintln!(
        "panic_after_initial_success test: {delivered} rows delivered, {calls} calls, durable={durable:?}"
    );
    assert!(
        delivered > 0,
        "expected at least some successful deliveries before panic; delivered={delivered}"
    );
    // Under the terminal-hold model: exactly 1 success + 1 panic = 2 calls.
    // No recovery calls because ingestion stops after the panic ack.
    assert_eq!(
        calls, 2,
        "terminal-hold model must issue exactly 2 send calls (1 success + 1 panic); calls={calls}"
    );
    // The first successful batch may or may not have triggered a checkpoint
    // update depending on flush timing. Either way, checkpoint must not
    // advance past the gap.
    if let Some(durable_val) = durable {
        assert!(
            durable_val < input_total_bytes,
            "checkpoint must stay behind full input after unresolved panic gap; durable={durable:?} input_total_bytes={input_total_bytes}"
        );
    }
    ckpt_handle.assert_monotonic(1);
    if durable.is_some() {
        ckpt_handle.assert_durable_not_ahead_of_updates(1);
    }
}

// ---------------------------------------------------------------------------
// Test 8: Retry-exhausted gap holds checkpoint despite later successes
// ---------------------------------------------------------------------------
//
// Bug hypothesis: non-terminal worker failures (RetryExhausted / Internal)
// could accidentally advance checkpoints when later batches succeed.
//
// Expected invariant:
// - retries are attempted (high send call count),
// - later batches may still deliver,
// - durable checkpoint never reaches end-of-input because failed gap is held.

/// Transient IoError failures are retried indefinitely. After a burst of
/// errors the worker eventually succeeds, all batches deliver, and the
/// checkpoint advances to cover the full input — no data is held or lost.
#[test]
fn transient_io_errors_retry_until_delivery_succeeds() {
    let mut sim = super::build_sim(90, 1);

    let factory = Arc::new(InstrumentedSinkFactory::new(vec![vec![
        FailureAction::Succeed,
        FailureAction::IoError(std::io::ErrorKind::TimedOut),
        FailureAction::IoError(std::io::ErrorKind::TimedOut),
        FailureAction::IoError(std::io::ErrorKind::TimedOut),
        FailureAction::IoError(std::io::ErrorKind::TimedOut),
        FailureAction::Succeed,
        FailureAction::Succeed,
    ]]));
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();
    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    let lines = generate_fixed_width_json_lines(3, 96);
    let input_total_bytes = total_bytes(&lines);

    sim.client("pipeline", async move {
        let input = ChannelInputSource::new("retry-gap", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_target_bytes(1);
        pipeline.set_batch_timeout(Duration::from_millis(10));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(25));
        let mut pipeline = pipeline
            .with_input("retry-gap", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(8)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    let run_result = sim.run();
    assert!(
        run_result.is_ok(),
        "pipeline must terminate cleanly; got {run_result:?}"
    );

    let delivered = delivered_counter.load(Ordering::Relaxed);
    let calls = call_counter.load(Ordering::Relaxed);
    let durable = ckpt_handle.durable_offset(1);

    assert!(
        calls >= 6,
        "expected retry attempts plus successful sends; calls={calls}"
    );
    assert!(
        delivered >= 3,
        "all batches should eventually deliver after transient errors; delivered={delivered}"
    );
    assert!(
        durable.unwrap_or_default() >= input_total_bytes,
        "checkpoint should advance past all input once every batch delivers; durable={durable:?} input_total_bytes={input_total_bytes}"
    );
    ckpt_handle.assert_monotonic(1);
    ckpt_handle.assert_durable_not_ahead_of_updates(1);
}
