#[cfg(feature = "turmoil")]
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "turmoil")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "turmoil")]
use logfwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(feature = "turmoil")]
use logfwd_io::tail::ByteOffset;
use logfwd_output::BatchMetadata;
#[cfg(feature = "turmoil")]
use logfwd_types::pipeline::SourceId;
use tokio_util::sync::CancellationToken;

use super::internal_faults;
use super::processor_stage::{ProcessorStageResult, run_processor_stage};
#[cfg(feature = "turmoil")]
use super::scan_maybe_blocking;
use super::{ChannelMsg, Pipeline, now_nanos};
#[cfg(feature = "turmoil")]
use super::{InputState, InputTransform};

impl Pipeline {
    /// Submit a scanned and SQL-transformed batch to the processor chain and output pool.
    ///
    /// This is the single path for all inputs (both production CPU workers
    /// and turmoil simulation tasks). The batch has already been scanned
    /// and SQL-transformed by the sender.
    ///
    /// Ticket lifecycle:
    /// 1. Create Queued tickets from checkpoints
    /// 2. Transition to Sending (must ack or reject after this point)
    /// 3. Run processor chain (reject on error)
    /// 4. Concatenate outputs, submit to pool
    pub(super) async fn submit_batch(
        &mut self,
        msg: ChannelMsg,
        shutdown: &CancellationToken,
    ) -> bool {
        let ChannelMsg {
            batch,
            checkpoints,
            queued_at,
            scan_ns,
            transform_ns,
            ..
        } = msg;
        let batch_id = self.metrics.alloc_batch_id();
        self.metrics
            .begin_active_batch(batch_id, now_nanos(), scan_ns, transform_ns);

        // Record queue wait time.
        if let Some(qt) = queued_at {
            let wait_ns = qt.elapsed().as_nanos() as u64;
            self.metrics.record_queue_wait(wait_ns);
        }

        // Create Queued tickets, then transition to Sending.
        // After begin_send, tickets MUST be acked or rejected.
        let sending: Vec<_> = if let Some(ref mut machine) = self.machine {
            checkpoints
                .iter()
                .map(|(&sid, &offset)| {
                    let queued = machine.create_batch(sid, offset.0);
                    machine.begin_send(queued)
                })
                .collect()
        } else {
            Vec::new()
        };

        let num_rows = batch.num_rows();

        // Handle zero-row results (SQL WHERE filtered all rows).
        // Ack tickets so the input doesn't re-read the same data.
        if num_rows == 0 {
            self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Ack);
            self.metrics.record_batch(0, scan_ns, transform_ns, 0);
            self.metrics.finish_active_batch(batch_id);
            return false;
        }
        let metadata = BatchMetadata {
            resource_attrs: Arc::clone(&self.resource_attrs),
            observed_time_ns: now_nanos(),
        };
        let (result, total_rows) = match run_processor_stage(&mut self.processors, batch, &metadata)
        {
            ProcessorStageResult::Forward { batch, output_rows } => (batch, output_rows),
            ProcessorStageResult::Hold { reason } => {
                tracing::warn!(reason = %reason, "processor stage requested hold");
                let (held, _advances) = self
                    .ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Hold);
                self.metrics.finish_active_batch(batch_id);
                if held {
                    shutdown.cancel();
                }
                return held;
            }
            ProcessorStageResult::PermanentError { reason } => {
                tracing::warn!(reason = %reason, "processor stage permanent error; dropping batch");
                self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Ack);
                self.metrics.inc_dropped_batch();
                self.metrics.record_batch(0, scan_ns, transform_ns, 0);
                self.metrics.finish_active_batch(batch_id);
                return false;
            }
            ProcessorStageResult::ZeroRow { reason } => {
                tracing::debug!(reason = %reason, "processor stage emitted zero rows");
                self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Ack);
                self.metrics.record_batch(0, scan_ns, transform_ns, 0);
                self.metrics.finish_active_batch(batch_id);
                return false;
            }
            ProcessorStageResult::Fatal { reason } => {
                tracing::error!(reason = %reason, "processor stage requested shutdown");
                self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Hold);
                self.metrics.finish_active_batch(batch_id);
                shutdown.cancel();
                return true;
            }
            ProcessorStageResult::Reject { reason } => {
                tracing::error!(reason = %reason, "processor stage rejected batch");
                self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Reject);
                self.metrics.finish_active_batch(batch_id);
                return false;
            }
        };
        self.metrics.transform_out.inc_lines(total_rows);

        let input_rows = num_rows as u64;
        let out_rows = result.num_rows() as u64;
        let submitted_at = tokio::time::Instant::now();

        if internal_faults::submit_before_pool_should_hold_and_shutdown() {
            tracing::warn!("internal failpoint: submit pre-pool hold+shutdown");
            self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Hold);
            self.metrics.finish_active_batch(batch_id);
            shutdown.cancel();
            return true;
        }

        let batch_span = tracing::info_span!(
            "batch",
            pipeline = %self.name,
            scan_ns = scan_ns,
            transform_ns = transform_ns,
            input_rows = input_rows,
            queue_wait_ns = tracing::field::Empty,
        );

        // Track in-flight before yielding to pool submission so ack handling
        // cannot observe a finished batch without a corresponding increment.
        self.metrics
            .inflight_batches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "turmoil")]
        let mut submitted_checkpoints: Vec<(u64, u64)> = checkpoints
            .iter()
            .map(|(&sid, &offset)| (sid.0, offset.0))
            .collect();
        #[cfg(feature = "turmoil")]
        submitted_checkpoints.sort_unstable();
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::BatchSubmitted {
                batch_id,
                checkpoints: submitted_checkpoints,
            },
        )
        .await;
        self.pool
            .submit(crate::worker_pool::WorkItem {
                num_rows: out_rows,
                batch: result,
                metadata,
                tickets: sending,
                submitted_at,
                scan_ns,
                transform_ns,
                batch_id,
                span: batch_span,
            })
            .await;
        false
    }
}

/// Scan + SQL transform accumulated bytes into a `ChannelMsg`.
///
/// Used by the turmoil async input loop. On scan/transform error,
/// returns a sentinel empty-batch message carrying the checkpoints
/// so the pipeline can reject those offsets.
#[cfg(feature = "turmoil")]
pub(super) async fn scan_and_transform_for_send(
    input: &mut InputState,
    transform: &mut InputTransform,
    metrics: &PipelineMetrics,
    input_index: usize,
) -> Option<ChannelMsg> {
    let data = input.buf.split().freeze();
    let checkpoints: HashMap<SourceId, ByteOffset> =
        input.source.checkpoint_data().into_iter().collect();
    let queued_at = tokio::time::Instant::now();

    let t0 = tokio::time::Instant::now();
    let batch = match scan_maybe_blocking(&mut transform.scanner, data) {
        Ok(b) => b,
        Err(e) => {
            metrics.inc_scan_error();
            metrics.inc_parse_error();
            metrics.inc_dropped_batch();
            tracing::warn!(input = transform.input_name.as_str(), error = %e, "scan error");
            // Checkpoints dropped — at-least-once: data re-read on restart.
            return None;
        }
    };
    let scan_ns = t0.elapsed().as_nanos() as u64;

    let num_rows = batch.num_rows();
    if num_rows > 0 {
        metrics.transform_in.inc_lines(num_rows as u64);
    }

    let t1 = tokio::time::Instant::now();
    let result = match transform.transform.execute(batch).await {
        Ok(r) => r,
        Err(e) => {
            metrics.inc_transform_error();
            metrics.inc_dropped_batch();
            tracing::warn!(input = transform.input_name.as_str(), error = %e, "transform error");
            // Checkpoints dropped — at-least-once: data re-read on restart.
            return None;
        }
    };
    let transform_ns = t1.elapsed().as_nanos() as u64;

    Some(ChannelMsg {
        batch: result,
        checkpoints,
        queued_at: Some(queued_at),
        input_index,
        scan_ns,
        transform_ns,
    })
}

#[allow(clippy::needless_pass_by_ref_mut)]
#[cfg(feature = "turmoil")]
pub(super) async fn transform_direct_batch_for_send(
    input: &mut InputState,
    transform: &mut InputTransform,
    metrics: &PipelineMetrics,
    input_index: usize,
    batch: RecordBatch,
) -> Option<ChannelMsg> {
    let checkpoints: HashMap<SourceId, ByteOffset> =
        input.source.checkpoint_data().into_iter().collect();
    let queued_at = tokio::time::Instant::now();

    let num_rows = batch.num_rows();
    if num_rows > 0 {
        metrics.transform_in.inc_lines(num_rows as u64);
    }

    let t0 = tokio::time::Instant::now();
    let result = match transform.transform.execute(batch).await {
        Ok(r) => r,
        Err(e) => {
            metrics.inc_transform_error();
            metrics.inc_dropped_batch();
            tracing::warn!(
                input = transform.input_name.as_str(),
                error = %e,
                "transform error"
            );
            return None;
        }
    };
    let transform_ns = t0.elapsed().as_nanos() as u64;

    Some(ChannelMsg {
        batch: result,
        checkpoints,
        queued_at: Some(queued_at),
        input_index,
        scan_ns: 0,
        transform_ns,
    })
}
