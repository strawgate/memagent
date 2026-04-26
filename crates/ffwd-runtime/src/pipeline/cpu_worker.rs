//! CPU worker: scans bytes into RecordBatch, runs SQL transform.

#[cfg(not(feature = "turmoil"))]
use std::collections::HashMap;
#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;
#[cfg(not(feature = "turmoil"))]
use std::time::Instant;

#[cfg(not(feature = "turmoil"))]
use tokio::sync::mpsc;

#[cfg(not(feature = "turmoil"))]
use ffwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(not(feature = "turmoil"))]
use ffwd_io::tail::ByteOffset;
#[cfg(not(feature = "turmoil"))]
use ffwd_types::pipeline::SourceId;

#[cfg(not(feature = "turmoil"))]
use super::SourcePipeline;
#[cfg(not(feature = "turmoil"))]
use super::io_worker::IoWorkItem;
#[cfg(not(feature = "turmoil"))]
use super::source_metadata::{cri_metadata_for_batch, source_metadata_for_batch};

/// Run the CPU worker loop for one input.
///
/// Receives raw bytes from the I/O worker, scans them into Arrow RecordBatches,
/// runs the per-input SQL transform via `execute_blocking`, and sends
/// `ProcessedBatch` to the pipeline's main select loop.
#[cfg(not(feature = "turmoil"))]
pub(super) fn cpu_worker_loop(
    mut rx: mpsc::Receiver<IoWorkItem>,
    tx: mpsc::Sender<super::ProcessedBatch>,
    mut transform: SourcePipeline,
    metrics: Arc<PipelineMetrics>,
) {
    /// After this many consecutive transform errors with no successes, escalate
    /// to ERROR with guidance — the SQL likely references columns the input
    /// format does not produce.
    const CONSECUTIVE_TRANSFORM_ERROR_ESCALATION: u32 = 10;
    let mut consecutive_transform_errors: u32 = 0;

    while let Some(item) = rx.blocking_recv() {
        let (batch, checkpoints, queued_at, input_index, scan_ns) = match item {
            IoWorkItem::RawBatch(chunk) => {
                let t0 = Instant::now();
                let batch = match transform.scanner.scan(chunk.bytes) {
                    Ok(b) => b,
                    Err(e) => {
                        metrics.inc_scan_error();
                        metrics.inc_parse_error();
                        metrics.inc_dropped_batch();
                        tracing::warn!(
                            input = transform.input_name.as_str(),
                            error = %e,
                            "cpu_worker: scan error (batch dropped)"
                        );
                        continue;
                    }
                };
                let batch = match source_metadata_for_batch(
                    batch,
                    &chunk.row_origins,
                    &chunk.source_paths,
                    transform.source_metadata_plan,
                ) {
                    Ok(batch) => batch,
                    Err(e) => {
                        metrics.inc_transform_error();
                        metrics.inc_dropped_batch();
                        tracing::warn!(
                            input = transform.input_name.as_str(),
                            error = %e,
                            "cpu_worker: source metadata attach error (batch dropped)"
                        );
                        continue;
                    }
                };
                let batch = match cri_metadata_for_batch(batch, chunk.cri_metadata) {
                    Ok(batch) => batch,
                    Err(e) => {
                        metrics.inc_transform_error();
                        metrics.inc_dropped_batch();
                        tracing::warn!(
                            input = transform.input_name.as_str(),
                            error = %e,
                            "cpu_worker: CRI metadata attach error (batch dropped)"
                        );
                        continue;
                    }
                };
                (
                    batch,
                    chunk.checkpoints,
                    chunk.queued_at,
                    chunk.input_index,
                    t0.elapsed().as_nanos() as u64,
                )
            }
            IoWorkItem::Batch {
                batch,
                checkpoints,
                row_origins,
                source_paths,
                queued_at,
                input_index,
            } => {
                let batch = match source_metadata_for_batch(
                    batch,
                    &row_origins,
                    &source_paths,
                    transform.source_metadata_plan,
                ) {
                    Ok(batch) => batch,
                    Err(e) => {
                        metrics.inc_transform_error();
                        metrics.inc_dropped_batch();
                        tracing::warn!(
                            input = transform.input_name.as_str(),
                            error = %e,
                            "cpu_worker: source metadata attach error (batch dropped)"
                        );
                        continue;
                    }
                };
                (batch, checkpoints, queued_at, input_index, 0)
            }
        };

        let num_rows = batch.num_rows();
        if num_rows > 0 {
            metrics.transform_in.inc_lines(num_rows as u64);
        }

        let t1 = Instant::now();
        let result = match transform.transform.execute_blocking(batch) {
            Ok(r) => {
                consecutive_transform_errors = 0;
                r
            }
            Err(e) => {
                metrics.inc_transform_error();
                metrics.inc_dropped_batch();
                consecutive_transform_errors = consecutive_transform_errors.saturating_add(1);
                if consecutive_transform_errors == CONSECUTIVE_TRANSFORM_ERROR_ESCALATION {
                    tracing::error!(
                        input = transform.input_name.as_str(),
                        error = %e,
                        consecutive_errors = consecutive_transform_errors,
                        "cpu_worker: {consecutive_transform_errors} consecutive transform errors \
                         with no successes — verify that the SQL column names match the input format \
                         (e.g. CRI produces _timestamp, _stream, and JSON body keys; \
                         generator/logs/simple produces timestamp, level, message, etc.)"
                    );
                } else {
                    tracing::warn!(
                        input = transform.input_name.as_str(),
                        error = %e,
                        "cpu_worker: transform error (batch dropped)"
                    );
                }
                continue;
            }
        };

        let transform_ns = t1.elapsed().as_nanos() as u64;
        let checkpoint_map: HashMap<SourceId, ByteOffset> = checkpoints.into_iter().collect();

        let msg = super::ProcessedBatch {
            batch: result,
            checkpoints: checkpoint_map,
            queued_at: Some(queued_at),
            input_index,
            scan_ns,
            transform_ns,
        };

        // Use blocking_send (not shutdown-aware) so we deliver all
        // remaining batches during shutdown drain. The CPU worker exits
        // when the I/O worker drops its sender (rx returns None).
        if tx.blocking_send(msg).is_err() {
            break;
        }
        metrics.inc_channel_depth();
    }
}
