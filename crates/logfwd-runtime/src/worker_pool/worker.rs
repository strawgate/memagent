use std::sync::Arc;
use std::time::Duration;
use std::{any::Any, panic::AssertUnwindSafe};

use arrow::record_batch::RecordBatch;
use backon::{BackoffBuilder, ExponentialBuilder};
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tracing::Instrument;

use logfwd_output::BatchMetadata;
use logfwd_output::sink::{OutputHealthEvent, SendResult, Sink};

use super::pool::{OutputHealthTracker, WorkerConfig, WorkerSlotCleanup};
use super::types::{AckItem, DeliveryOutcome, WorkItem, WorkerMsg, bound_rejection_reason};

// Worker task
// ---------------------------------------------------------------------------

/// Long-lived tokio task that owns one `Sink` and processes `WorkItem`s.
///
/// Exits when:
/// - The `rx` channel is closed (pool dropped the sender).
/// - No item arrives within `idle_timeout` (self-terminate to free connection).
/// - The `cancel` token is fired (hard shutdown).
pub(super) async fn worker_task(
    id: usize,
    mut sink: Box<dyn Sink>,
    mut rx: mpsc::Receiver<WorkerMsg>,
    ack_tx: mpsc::UnboundedSender<AckItem>,
    cfg: WorkerConfig,
) {
    let WorkerConfig {
        idle_timeout,
        cancel,
        max_retry_delay,
        metrics,
        output_health,
    } = cfg;
    let _slot_cleanup = WorkerSlotCleanup {
        output_health: Arc::clone(&output_health),
        worker_id: id,
    };
    loop {
        tokio::select! {
            biased; // check cancel first
            () = cancel.cancelled() => break,
            msg = recv_with_idle_timeout(&mut rx, idle_timeout) => {
                match msg {
                    None => break, // idle timeout or channel closed
                    Some(WorkerMsg::Shutdown) => break,
                    Some(WorkerMsg::Work(item)) => {
                        let WorkItem {
                            batch,
                            metadata,
                            tickets,
                            num_rows,
                            submitted_at,
                            scan_ns,
                            transform_ns,
                            batch_id,
                            span,
                        } = item;
                        let queue_wait_ns = submitted_at.elapsed().as_nanos() as u64;
                        span.record("queue_wait_ns", queue_wait_ns);
                        // Record which worker picked up this batch for the live dashboard.
                        let now_ns = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as u64;
                        metrics.assign_worker_to_active_batch(batch_id, id, now_ns);
                        let output_span = tracing::info_span!(
                            parent: &span, "output",
                            worker_id = id,
                            status_code = tracing::field::Empty,
                            send_ns   = tracing::field::Empty,
                            recv_ns   = tracing::field::Empty,
                            took_ms   = tracing::field::Empty,
                            retries   = tracing::field::Empty,
                            req_bytes = tracing::field::Empty,
                            cmp_bytes = tracing::field::Empty,
                            resp_bytes = tracing::field::Empty,
                        );
                        let output_name = sink.name().to_string();
                        let process_result = AssertUnwindSafe(
                            process_item(
                                id,
                                &mut *sink,
                                &output_health,
                                batch,
                                &metadata,
                                max_retry_delay,
                                &cancel,
                                batch_id,
                            )
                            .instrument(output_span.clone()),
                        )
                        .catch_unwind()
                        .await;
                        let mut recycle_worker_after_ack = false;
                        let (outcome, send_latency_ns, retries) = match process_result {
                            Ok(result) => result,
                            Err(payload) => {
                                recycle_worker_after_ack = true;
                                output_health.apply_worker_event(id, OutputHealthEvent::FatalFailure);
                                tracing::error!(
                                    worker_id = id,
                                    panic = %panic_payload_message(payload.as_ref()),
                                    "worker_pool: sink send panicked; emitting failure ack and recycling worker"
                                );
                                (DeliveryOutcome::InternalFailure, 0, 0)
                            }
                        };
                        output_span.record("retries", retries);
                        output_span.record("send_ns", send_latency_ns);
                        let output_ns = submitted_at.elapsed().as_nanos() as u64 - queue_wait_ns;
                        // Export child span first so the SpanBuffer has complete data
                        // before we remove the active batch entry. Without this ordering
                        // the diagnostics HTTP server can observe a window where neither
                        // the active batch nor the completed output child exists, causing
                        // the trace to vanish from the dashboard's worker lane.
                        drop(output_span);
                        drop(span);
                        // Remove from active_batches immediately — don't wait for the pipeline's
                        // ack select loop, which can be starved by flush_batch.await blocking.
                        metrics.finish_active_batch(batch_id);
                        let ack = AckItem {
                            tickets,
                            outcome,
                            num_rows,
                            submitted_at,
                            scan_ns,
                            transform_ns,
                            output_ns,
                            queue_wait_ns,
                            send_latency_ns,
                            batch_id,
                            output_name,
                        };
                        #[cfg(feature = "turmoil")]
                        crate::turmoil_barriers::trigger(
                            crate::turmoil_barriers::RuntimeBarrierEvent::BeforeWorkerAckSend {
                                worker_id: id,
                                batch_id: ack.batch_id,
                                outcome: ack.outcome.clone(),
                                retries,
                                num_rows: ack.num_rows,
                            },
                        )
                        .await;
                        if let Err(send_err) = ack_tx.send(ack) {
                            let mut lost_ack = send_err.0;
                            let unresolved_tickets = lost_ack.tickets.len();
                            for ticket in lost_ack.tickets.drain(..) {
                                // Convert Sending -> Queued before drop to satisfy
                                // typestate invariants when the ack channel is gone.
                                let _ = ticket.fail();
                            }
                            output_health
                                .apply_worker_event(id, OutputHealthEvent::FatalFailure);
                            tracing::error!(
                                worker_id = id,
                                batch_id = lost_ack.batch_id,
                                ticket_count = unresolved_tickets,
                                num_rows = lost_ack.num_rows,
                                "outcome" = ?lost_ack.outcome,
                                "worker: ack channel closed; cancelling worker pool to avoid unresolved checkpoint state"
                            );
                            cancel.cancel();
                            break;
                        }
                        if recycle_worker_after_ack {
                            break;
                        }
                    }
                }
            }
        }
    }
    // Always shut the sink down when the worker exits so resources are flushed
    // and released. Health transitions for pipeline drain are driven by the
    // pool-level drain path; idle worker expiry should not make outputs appear
    // permanently unready because the pool can respawn workers on demand.
    if let Err(e) = sink.shutdown().await {
        tracing::error!(worker_id = id, error = %e, "worker_pool: sink shutdown failed");
        metrics.output_error(sink.name());
    }
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(msg) = payload.downcast_ref::<&'static str>() {
        msg
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.as_str()
    } else {
        "<non-string panic payload>"
    }
}

/// Receive with idle timeout — returns `None` on timeout or channel close.
pub(super) async fn recv_with_idle_timeout(
    rx: &mut mpsc::Receiver<WorkerMsg>,
    idle_timeout: Duration,
) -> Option<WorkerMsg> {
    tokio::time::timeout(idle_timeout, rx.recv())
        .await
        .ok() // Err = timed out → None
        .flatten() // None = channel closed → None
}

/// Process one batch with retry on `RetryAfter` and server errors.
///
/// Returns `(outcome, send_latency_ns, retries)` where `send_latency_ns` is
/// cumulative wall time inside `sink.send_batch()` across all attempts
/// (excludes backoff sleep).
///
/// Retries indefinitely for transient failures (`IoError`, `RetryAfter`,
/// timeouts) using exponential backoff capped at `max_retry_delay`. The
/// worker blocks on its current batch, which propagates backpressure
/// through the bounded worker channels to the pipeline and ultimately to
/// inputs — matching Filebeat's delivery model. The only exits are:
///
/// - `Delivered` — batch was sent successfully
/// - `Rejected` — sink permanently rejected the data (4xx, schema error)
/// - `PoolClosed` — shutdown cancellation was observed
/// - `InternalFailure` — unknown `SendResult` variant
#[allow(clippy::too_many_arguments)] // Tracked by PR #2542
pub(super) async fn process_item(
    worker_id: usize,
    sink: &mut dyn Sink,
    output_health: &OutputHealthTracker,
    batch: RecordBatch,
    metadata: &BatchMetadata,
    max_retry_delay: Duration,
    cancel: &tokio_util::sync::CancellationToken,
    #[cfg_attr(not(feature = "turmoil"), allow(unused_variables))] batch_id: u64,
) -> (DeliveryOutcome, u64, usize) {
    sink.begin_batch();

    const BATCH_TIMEOUT_SECS: u64 = 60;
    /// After this many retries, downgrade repeated warnings to debug level
    /// to avoid unbounded log volume during extended outages.
    const WARN_RETRY_LIMIT: usize = 5;

    /// Build a fresh exponential backoff iterator. Once exhausted the caller
    /// should fall back to `max_retry_delay` directly (stay at cap) rather
    /// than resetting to min — this avoids sawtooth retry spikes against an
    /// unhealthy sink.
    fn new_backoff(max_retry_delay: Duration) -> backon::ExponentialBackoff {
        let min_delay = Duration::from_millis(100).min(max_retry_delay);
        ExponentialBuilder::default()
            .with_min_delay(min_delay)
            .with_max_delay(max_retry_delay)
            .with_factor(2.0)
            .with_max_times(10)
            .with_jitter()
            .build()
    }

    /// Get the next backoff delay, staying at `max_retry_delay` once the
    /// iterator is exhausted (no panic, no cycle reset).
    fn next_delay(backoff: &mut backon::ExponentialBackoff, max_retry_delay: Duration) -> Duration {
        backoff.next().unwrap_or(max_retry_delay)
    }

    let mut backoff = new_backoff(max_retry_delay);
    let mut send_latency_ns: u64 = 0;
    let mut retries_count = 0;

    loop {
        // Hard per-batch timeout: prevents one slow/broken batch from
        // tying up the worker indefinitely.
        let send_start = std::time::Instant::now();
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::warn!(worker_id, "worker_pool: cancellation observed during batch send");
                send_latency_ns += send_start.elapsed().as_nanos() as u64;
                return (DeliveryOutcome::PoolClosed, send_latency_ns, retries_count);
            }
            result = tokio::time::timeout(
                Duration::from_secs(BATCH_TIMEOUT_SECS),
                sink.send_batch(&batch, metadata),
            ) => result,
        };
        send_latency_ns += send_start.elapsed().as_nanos() as u64;

        match result {
            Err(_elapsed) => {
                // Timeout — treat like a transient error: back off and retry.
                // A hung send doesn't mean the output is permanently dead.
                retries_count += 1;
                let delay = next_delay(&mut backoff, max_retry_delay);
                output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                #[cfg(feature = "turmoil")]
                crate::turmoil_barriers::trigger(
                    crate::turmoil_barriers::RuntimeBarrierEvent::RetryAttempt {
                        worker_id,
                        batch_id,
                        attempt: retries_count,
                        backoff_ms: delay.as_millis() as u64,
                        reason: crate::turmoil_barriers::RetryReason::Timeout,
                    },
                )
                .await;
                if retries_count <= WARN_RETRY_LIMIT {
                    tracing::warn!(
                        worker_id,
                        timeout_secs = BATCH_TIMEOUT_SECS,
                        retries = retries_count,
                        sleep_ms = delay.as_millis() as u64,
                        "worker_pool: batch send timed out, retrying"
                    );
                } else {
                    tracing::debug!(
                        worker_id,
                        timeout_secs = BATCH_TIMEOUT_SECS,
                        retries = retries_count,
                        sleep_ms = delay.as_millis() as u64,
                        "worker_pool: batch send timed out, retrying"
                    );
                }
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        tracing::warn!(worker_id, "worker_pool: cancellation observed during timeout backoff");
                        return (DeliveryOutcome::PoolClosed, send_latency_ns, retries_count);
                    }
                    () = tokio::time::sleep(delay) => {}
                }
            }
            Ok(SendResult::Ok) => {
                output_health.apply_worker_event(worker_id, OutputHealthEvent::DeliverySucceeded);
                return (DeliveryOutcome::Delivered, send_latency_ns, retries_count);
            }
            Ok(SendResult::Rejected(reason)) => {
                tracing::warn!(worker_id, %reason, "worker_pool: batch rejected");
                output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                return (
                    DeliveryOutcome::Rejected {
                        reason: bound_rejection_reason(reason),
                    },
                    send_latency_ns,
                    retries_count,
                );
            }
            Ok(SendResult::RetryAfter(retry_dur)) => {
                // Server specified delay — use the server's delay (capped).
                // Don't advance the backoff iterator; server controls timing.
                retries_count += 1;
                let sleep_for = retry_dur.min(max_retry_delay);
                output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                #[cfg(feature = "turmoil")]
                crate::turmoil_barriers::trigger(
                    crate::turmoil_barriers::RuntimeBarrierEvent::RetryAttempt {
                        worker_id,
                        batch_id,
                        attempt: retries_count,
                        backoff_ms: sleep_for.as_millis() as u64,
                        reason: crate::turmoil_barriers::RetryReason::RetryAfter,
                    },
                )
                .await;
                tracing::warn!(
                    worker_id,
                    ?sleep_for,
                    retries = retries_count,
                    "worker_pool: rate-limited, retrying"
                );
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        tracing::warn!(worker_id, "worker_pool: cancellation observed during retry-after backoff");
                        return (DeliveryOutcome::PoolClosed, send_latency_ns, retries_count);
                    }
                    () = tokio::time::sleep(sleep_for) => {}
                }
            }
            Ok(SendResult::IoError(e)) => {
                // Transient I/O error — retry indefinitely with backoff.
                // Backpressure propagates naturally: while this worker blocks,
                // the pipeline cannot submit new batches to it, slowing inputs.
                retries_count += 1;
                let delay = next_delay(&mut backoff, max_retry_delay);
                output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                #[cfg(feature = "turmoil")]
                crate::turmoil_barriers::trigger(
                    crate::turmoil_barriers::RuntimeBarrierEvent::RetryAttempt {
                        worker_id,
                        batch_id,
                        attempt: retries_count,
                        backoff_ms: delay.as_millis() as u64,
                        reason: crate::turmoil_barriers::RetryReason::IoError,
                    },
                )
                .await;
                if retries_count <= WARN_RETRY_LIMIT {
                    tracing::warn!(
                        worker_id,
                        sleep_ms = delay.as_millis() as u64,
                        retries = retries_count,
                        error = %e,
                        "worker_pool: transient error, retrying"
                    );
                } else {
                    tracing::debug!(
                        worker_id,
                        sleep_ms = delay.as_millis() as u64,
                        retries = retries_count,
                        error = %e,
                        "worker_pool: transient error, retrying"
                    );
                }
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        tracing::warn!(worker_id, "worker_pool: cancellation observed during retry backoff");
                        return (DeliveryOutcome::PoolClosed, send_latency_ns, retries_count);
                    }
                    () = tokio::time::sleep(delay) => {}
                }
            }
            // Future SendResult variants (#[non_exhaustive]) — treat as failure.
            Ok(_) => {
                output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                return (
                    DeliveryOutcome::InternalFailure,
                    send_latency_ns,
                    retries_count,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use logfwd_diagnostics::diagnostics::PipelineMetrics;
    use logfwd_output::BatchMetadata;
    use logfwd_output::sink::{SendResult, Sink};

    use crate::worker_pool::pool::OutputHealthTracker;

    use super::super::pool::WorkerConfig;
    use super::super::types::{DeliveryOutcome, WorkItem, WorkerMsg};
    use super::{process_item, worker_task};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TerminalizationAction {
        AckDelivered,
        AckChannelClosed,
    }

    #[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
    struct TerminalizationState {
        terminalizations: u8,
        held_for_replay: bool,
        cancel_requested: bool,
    }

    fn reduce_terminalization(
        mut state: TerminalizationState,
        action: TerminalizationAction,
    ) -> TerminalizationState {
        if state.terminalizations == 0 {
            state.terminalizations = 1;
            match action {
                TerminalizationAction::AckDelivered => {
                    state.held_for_replay = false;
                }
                TerminalizationAction::AckChannelClosed => {
                    state.held_for_replay = true;
                    state.cancel_requested = true;
                }
            }
        }
        state
    }

    struct OkSink;

    impl Sink for OkSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            Box::pin(async { SendResult::Ok })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            "ok-sink"
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct HangingSink;

    impl Sink for HangingSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            Box::pin(std::future::pending())
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            "hanging-sink"
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap()
    }

    fn make_metrics() -> Arc<PipelineMetrics> {
        let meter = logfwd_test_utils::test_meter();
        Arc::new(PipelineMetrics::new("test", "", &meter))
    }

    #[tokio::test]
    async fn worker_cancels_pool_when_ack_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        drop(ack_rx);

        let cancel = CancellationToken::new();
        let cfg = WorkerConfig {
            idle_timeout: Duration::from_secs(5),
            cancel: cancel.clone(),
            max_retry_delay: Duration::from_millis(10),
            metrics: make_metrics(),
            output_health: Arc::new(OutputHealthTracker::new(vec![])),
        };

        let join = tokio::spawn(worker_task(0, Box::new(OkSink), rx, ack_tx, cfg));

        tx.send(WorkerMsg::Work(WorkItem {
            batch: make_batch(),
            metadata: BatchMetadata {
                resource_attrs: Arc::default(),
                observed_time_ns: 0,
            },
            tickets: vec![],
            num_rows: 1,
            submitted_at: tokio::time::Instant::now(),
            scan_ns: 0,
            transform_ns: 0,
            batch_id: 42,
            span: tracing::Span::none(),
        }))
        .await
        .expect("worker should accept work item");

        tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("worker should exit promptly when ack channel is closed")
            .expect("worker task should not panic");

        assert!(
            cancel.is_cancelled(),
            "worker should cancel the pool when ack channel closes"
        );
    }

    #[tokio::test]
    async fn process_item_returns_pool_closed_when_cancelled_before_send() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut sink = HangingSink;
        let output_health = Arc::new(OutputHealthTracker::new(vec![]));
        let metadata = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };

        let (outcome, _send_latency_ns, retries) = process_item(
            0,
            &mut sink,
            &output_health,
            make_batch(),
            &metadata,
            Duration::from_millis(10),
            &cancel,
            0, // batch_id (test only)
        )
        .await;

        assert_eq!(outcome, DeliveryOutcome::PoolClosed);
        assert_eq!(retries, 0);
    }

    #[test]
    fn terminalization_reducer_is_idempotent_for_any_two_step_schedule() {
        let actions = [
            TerminalizationAction::AckDelivered,
            TerminalizationAction::AckChannelClosed,
        ];
        let mut outcomes = BTreeSet::new();

        for first in actions {
            for second in actions {
                let state = reduce_terminalization(
                    reduce_terminalization(TerminalizationState::default(), first),
                    second,
                );
                assert!(
                    state.terminalizations <= 1,
                    "terminalization must occur at most once"
                );
                outcomes.insert(state);
            }
        }

        assert!(
            outcomes.contains(&TerminalizationState {
                terminalizations: 1,
                held_for_replay: false,
                cancel_requested: false,
            }),
            "delivered schedule should be reachable"
        );
        assert!(
            outcomes.contains(&TerminalizationState {
                terminalizations: 1,
                held_for_replay: true,
                cancel_requested: true,
            }),
            "ack-channel-closed schedule should be reachable"
        );
    }

    #[cfg(feature = "loom-tests")]
    #[test]
    fn loom_terminalization_race_resolves_once() {
        loom::model(|| {
            let state =
                loom::sync::Arc::new(loom::sync::Mutex::new(TerminalizationState::default()));

            let state_ack = loom::sync::Arc::clone(&state);
            let ack = loom::thread::spawn(move || {
                let mut guard = state_ack
                    .lock()
                    .expect("loom mutex poisoned during ack path");
                *guard = reduce_terminalization(*guard, TerminalizationAction::AckDelivered);
            });

            let state_closed = loom::sync::Arc::clone(&state);
            let closed = loom::thread::spawn(move || {
                let mut guard = state_closed
                    .lock()
                    .expect("loom mutex poisoned during ack-closed path");
                *guard = reduce_terminalization(*guard, TerminalizationAction::AckChannelClosed);
            });

            ack.join().expect("ack thread should not panic");
            closed
                .join()
                .expect("ack-channel-closed thread should not panic");

            let final_state = *state
                .lock()
                .expect("loom mutex poisoned while validating terminalization state");

            assert_eq!(
                final_state.terminalizations, 1,
                "checkpoint seam must resolve each ticket exactly once"
            );
            assert!(
                final_state
                    == TerminalizationState {
                        terminalizations: 1,
                        held_for_replay: false,
                        cancel_requested: false,
                    }
                    || final_state
                        == TerminalizationState {
                            terminalizations: 1,
                            held_for_replay: true,
                            cancel_requested: true,
                        },
                "terminalization outcome must be either delivered or held-for-replay"
            );
        });
    }
}
