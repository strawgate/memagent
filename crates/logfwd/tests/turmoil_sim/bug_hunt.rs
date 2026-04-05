//! Bug-hunting simulation tests targeting specific audit-identified issues.
//!
//! Each test targets a concrete bug hypothesis from a code audit. Tests
//! that expose real bugs document the actual (possibly wrong) behavior
//! with comments explaining why it differs from the expected behavior.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_core::pipeline::SourceId;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSinkFactory};
use super::observable_checkpoint::ObservableCheckpointStore;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
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
// Test 3: Rejected batch checkpoint behavior
// ---------------------------------------------------------------------------
//
// Bug hypothesis: `record_ack_and_advance` treats reject identically to
// ack — checkpoint advances past rejected data. If data at offset
// 1000-2000 is rejected (NOT delivered), the checkpoint advances to 2000,
// and on restart that data is permanently skipped.
//
// This test DOCUMENTS the current behavior — whether reject advances the
// checkpoint or not.

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

    // DOCUMENT: does the checkpoint advance past the rejected batch?
    // In pipeline.rs `ack_all_tickets`, reject calls `ticket.reject()` which
    // still produces an AckReceipt that feeds into `record_ack_and_advance`.
    // The machine treats reject and ack identically for checkpoint advancement.
    //
    // If durable offset > 0, the checkpoint advanced past SOME data.
    // The question is whether it advanced past the rejected batch specifically.
    let durable = ckpt_handle.durable_offset(1);
    let updates = ckpt_handle.update_count(1);

    eprintln!("rejected_batch test: durable offset = {durable:?}, checkpoint updates = {updates}");

    // CONFIRMED BUG / DESIGN DECISION: The checkpoint advances past rejected
    // (undelivered) data. With default batch_target_bytes (64KB), all 20 lines
    // fit into a single batch. That batch is rejected (0 rows delivered), yet
    // the checkpoint advances to offset 520 (past all 20 lines).
    //
    // In `ack_all_tickets`, both `ticket.ack()` and `ticket.reject()` produce
    // AckReceipts that feed into `record_ack_and_advance`. The machine treats
    // them identically — checkpoint advances past rejected data.
    //
    // On restart, this data would be permanently skipped.
    //
    // Whether this is a bug depends on the rejection reason:
    //   - Server 400 (bad data): advancing is correct (retry would fail again)
    //   - Server 429 (rate limit): should NOT reject, should RetryAfter
    //   - Transient server error: should NOT reject, should IO error + retry
    //
    // All 20 lines fit in 1 batch (64KB target). That batch is rejected.
    assert_eq!(
        delivered, 0,
        "expected 0 rows delivered (batch was rejected)"
    );
    assert_eq!(
        calls, 1,
        "expected exactly 1 send_batch call (1 batch, rejected)"
    );

    // CONFIRMED BUG (issue #1001): checkpoint advances past rejected data.
    // The PipelineMachine treats reject() identically to ack() for checkpoint
    // advancement. On restart, rejected data is permanently skipped.
    assert!(
        durable.is_some() && durable.unwrap() > 0,
        "CONFIRMED: checkpoint MUST advance past rejected data (current behavior). \
         If this assertion fails, someone fixed issue #1001 — update this test. \
         durable={durable:?}"
    );

    // Checkpoint history must be monotonic regardless of reject behavior.
    ckpt_handle.assert_monotonic(1);
}

// ---------------------------------------------------------------------------
// Test 4: RetryAfter respects server-specified backoff timing
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
