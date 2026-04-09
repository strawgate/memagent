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
                        let process_result = AssertUnwindSafe(
                            process_item(
                                id,
                                &mut *sink,
                                &output_health,
                                batch,
                                &metadata,
                                max_retry_delay,
                                &cancel,
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
                        // Remove from active_batches immediately — don't wait for the pipeline's
                        // ack select loop, which can be starved by flush_batch.await blocking.
                        metrics.finish_active_batch(batch_id);
                        drop(span);
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
                            output_name: sink.name().to_string(),
                        };
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
pub(super) async fn process_item(
    worker_id: usize,
    sink: &mut dyn Sink,
    output_health: &OutputHealthTracker,
    batch: RecordBatch,
    metadata: &BatchMetadata,
    max_retry_delay: Duration,
    cancel: &tokio_util::sync::CancellationToken,
) -> (DeliveryOutcome, u64, usize) {
    sink.begin_batch();

    const MAX_RETRIES: usize = 3; // 1 initial + 3 retries = 4 total attempts
    const BATCH_TIMEOUT_SECS: u64 = 60;

    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(max_retry_delay)
        .with_factor(2.0)
        .with_max_times(MAX_RETRIES)
        .with_jitter()
        .build();

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
                tracing::error!(
                    worker_id,
                    timeout_secs = BATCH_TIMEOUT_SECS,
                    "worker_pool: batch send timed out"
                );
                output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                return (DeliveryOutcome::TimedOut, send_latency_ns, retries_count);
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
                // Server specified delay — consume a backoff slot but use
                // the server's delay (capped at max_retry_delay).
                if backoff.next().is_none() {
                    tracing::error!(
                        worker_id,
                        max_retries = MAX_RETRIES,
                        "worker_pool: RetryAfter exceeded max retries"
                    );
                    output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                    return (
                        DeliveryOutcome::RetryExhausted,
                        send_latency_ns,
                        retries_count,
                    );
                }
                retries_count += 1;
                let sleep_for = retry_dur.min(max_retry_delay);
                output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                tracing::warn!(worker_id, ?sleep_for, "worker_pool: rate-limited, retrying");
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        tracing::warn!(worker_id, "worker_pool: cancellation observed during retry-after backoff");
                        return (DeliveryOutcome::PoolClosed, send_latency_ns, retries_count);
                    }
                    () = tokio::time::sleep(sleep_for) => {}
                }
            }
            Ok(SendResult::IoError(e)) => match backoff.next() {
                Some(delay) => {
                    retries_count += 1;
                    output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                    tracing::warn!(
                        worker_id,
                        sleep_ms = delay.as_millis() as u64,
                        error = %e,
                        "worker_pool: transient error, retrying with jitter"
                    );
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {
                            tracing::warn!(worker_id, "worker_pool: cancellation observed during jitter backoff");
                            return (DeliveryOutcome::PoolClosed, send_latency_ns, retries_count);
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                None => {
                    tracing::error!(
                        worker_id,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "worker_pool: gave up after retries"
                    );
                    output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                    return (
                        DeliveryOutcome::RetryExhausted,
                        send_latency_ns,
                        retries_count,
                    );
                }
            },
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
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use logfwd_io::diagnostics::PipelineMetrics;
    use logfwd_output::BatchMetadata;
    use logfwd_output::sink::{SendResult, Sink};

    use crate::worker_pool::pool::OutputHealthTracker;

    use super::super::pool::WorkerConfig;
    use super::super::types::{DeliveryOutcome, WorkItem, WorkerMsg};
    use super::{process_item, worker_task};

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
        )
        .await;

        assert_eq!(outcome, DeliveryOutcome::PoolClosed);
        assert_eq!(retries, 0);
    }
}
