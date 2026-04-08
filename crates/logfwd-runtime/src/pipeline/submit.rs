#[cfg(feature = "turmoil")]
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "turmoil")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "turmoil")]
use logfwd_io::diagnostics::PipelineMetrics;
#[cfg(feature = "turmoil")]
use logfwd_io::tail::ByteOffset;
use logfwd_output::BatchMetadata;
#[cfg(feature = "turmoil")]
use logfwd_types::pipeline::SourceId;

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
    pub(super) async fn submit_batch(&mut self, msg: ChannelMsg) {
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
            return;
        }
        // Run through processor chain.
        let metadata = BatchMetadata {
            resource_attrs: Arc::clone(&self.resource_attrs),
            observed_time_ns: now_nanos(),
        };
        let results = if self.processors.is_empty() {
            smallvec::smallvec![batch]
        } else {
            match crate::processor::run_chain(&mut self.processors, batch, &metadata) {
                Ok(batches) => batches,
                Err(crate::processor::ProcessorError::Transient(e)) => {
                    tracing::warn!(error = %e, "transient processor error, holding batch");
                    self.ack_all_tickets(
                        sending,
                        super::checkpoint_policy::TicketDisposition::Hold,
                    );
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
                Err(crate::processor::ProcessorError::Permanent(e)) => {
                    tracing::error!(error = %e, "permanent processor error, dropping batch");
                    self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Ack); // ack but don't forward
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
                Err(crate::processor::ProcessorError::Fatal(e)) => {
                    tracing::error!(error = %e, "fatal processor error, holding batch");
                    self.ack_all_tickets(
                        sending,
                        super::checkpoint_policy::TicketDisposition::Hold,
                    );
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
            }
        };

        let total_rows: u64 = results.iter().map(|b| b.num_rows() as u64).sum();
        self.metrics.transform_out.inc_lines(total_rows);

        // Handle post-processor zero-row results.
        if total_rows == 0 {
            self.ack_all_tickets(sending, super::checkpoint_policy::TicketDisposition::Ack);
            self.metrics.record_batch(0, scan_ns, transform_ns, 0);
            self.metrics.finish_active_batch(batch_id);
            return;
        }

        // Concatenate multiple processor outputs into a single batch
        // (matches flush_batch_from behavior — one ticket set per submission).
        let result = if results.len() == 1 {
            results.into_iter().next().expect("checked len == 1")
        } else {
            match arrow::compute::concat_batches(&results[0].schema(), results.iter()) {
                Ok(batch) => batch,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "processor chain returned incompatible schemas; rejecting batch"
                    );
                    self.ack_all_tickets(
                        sending,
                        super::checkpoint_policy::TicketDisposition::Reject,
                    );
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
            }
        };

        let input_rows = num_rows as u64;
        let out_rows = result.num_rows() as u64;
        let submitted_at = tokio::time::Instant::now();

        let batch_span = tracing::info_span!(
            "batch",
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
