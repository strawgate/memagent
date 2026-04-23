//! CPU worker: scans bytes into RecordBatch, runs SQL transform.
//!
//! The worker drains available items from the channel (up to
//! `MAX_DRAIN_ITEMS`) so that multiple chunks arriving in a burst are
//! scanned independently but grouped by schema and transformed in a
//! single DataFusion SQL execution. This reduces per-batch overhead
//! when the I/O layer produces data faster than the CPU worker processes it.

#[cfg(not(feature = "turmoil"))]
use std::collections::HashMap;
#[cfg(not(feature = "turmoil"))]
use std::collections::hash_map::DefaultHasher;
#[cfg(not(feature = "turmoil"))]
use std::hash::{Hash, Hasher};
#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;
#[cfg(not(feature = "turmoil"))]
use std::time::Instant;

#[cfg(not(feature = "turmoil"))]
use arrow::datatypes::SchemaRef;
#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatch;
#[cfg(not(feature = "turmoil"))]
use tokio::sync::mpsc;

#[cfg(not(feature = "turmoil"))]
use logfwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(not(feature = "turmoil"))]
use logfwd_io::tail::ByteOffset;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::pipeline::SourceId;

#[cfg(not(feature = "turmoil"))]
use super::InputTransform;
#[cfg(not(feature = "turmoil"))]
use super::io_worker::IoWorkItem;
#[cfg(not(feature = "turmoil"))]
use super::source_metadata::{cri_metadata_for_batch, source_metadata_for_batch};

/// Maximum number of items to drain from the channel per iteration.
/// Caps accumulation so the worker does not hold too many batches in
/// memory before transforming/sending them downstream.
#[cfg(not(feature = "turmoil"))]
const MAX_DRAIN_ITEMS: usize = 16;

/// A scanned batch with its associated checkpoint and timing metadata.
#[cfg(not(feature = "turmoil"))]
struct ScannedBatch {
    batch: RecordBatch,
    checkpoints: Vec<(SourceId, ByteOffset)>,
    queued_at: tokio::time::Instant,
    input_index: usize,
    scan_ns: u64,
    schema_hash: u64,
}

/// Hash an Arrow schema for grouping batches by compatible shape.
#[cfg(not(feature = "turmoil"))]
fn hash_schema(schema: &SchemaRef) -> u64 {
    let mut hasher = DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        field.data_type().hash(&mut hasher);
        field.is_nullable().hash(&mut hasher);
    }
    hasher.finish()
}

/// Merge checkpoint vectors: later entries for the same source override
/// earlier ones (the later checkpoint is always >= the earlier).
#[cfg(not(feature = "turmoil"))]
fn merge_checkpoints(
    items: impl IntoIterator<Item = Vec<(SourceId, ByteOffset)>>,
) -> HashMap<SourceId, ByteOffset> {
    let mut merged = HashMap::new();
    for checkpoints in items {
        for (sid, offset) in checkpoints {
            merged
                .entry(sid)
                .and_modify(|existing: &mut ByteOffset| {
                    if offset.0 > existing.0 {
                        *existing = offset;
                    }
                })
                .or_insert(offset);
        }
    }
    merged
}

/// Run the CPU worker loop for one input.
///
/// Receives raw bytes from the I/O worker, scans them into Arrow RecordBatches,
/// runs the per-input SQL transform, and sends `ChannelMsg` to the
/// pipeline's main select loop.
///
/// When multiple items are ready in the channel, the worker drains them
/// (up to [`MAX_DRAIN_ITEMS`]), scans each independently, groups the
/// resulting batches by schema, and transforms each group with a single
/// `execute_multi_batch_blocking` call. This amortizes DataFusion SQL
/// compilation and execution overhead across bursts of data.
///
/// Creates a lightweight tokio current-thread runtime for DataFusion SQL
/// execution (which is async internally). The runtime is created once and
/// reused for all batches.
#[cfg(not(feature = "turmoil"))]
pub(super) fn cpu_worker_loop(
    mut rx: mpsc::Receiver<IoWorkItem>,
    tx: mpsc::Sender<super::ChannelMsg>,
    mut transform: InputTransform,
    metrics: Arc<PipelineMetrics>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(
                input = transform.input_name.as_str(),
                error = %e,
                "cpu_worker: failed to create tokio runtime — input disabled"
            );
            return;
        }
    };

    /// After this many consecutive transform errors with no successes, escalate
    /// to ERROR with guidance — the SQL likely references columns the input
    /// format does not produce.
    const CONSECUTIVE_TRANSFORM_ERROR_ESCALATION: u32 = 10;
    let mut consecutive_transform_errors: u32 = 0;

    while let Some(first_item) = rx.blocking_recv() {
        // Drain any additional ready items from the channel so we can
        // batch-transform them in a single SQL execution.
        let mut items = vec![first_item];
        while items.len() < MAX_DRAIN_ITEMS {
            match rx.try_recv() {
                Ok(item) => items.push(item),
                Err(_) => break,
            }
        }

        // Phase 1: scan each item independently, collecting ScannedBatches.
        let mut scanned: Vec<ScannedBatch> = Vec::with_capacity(items.len());
        for item in items {
            if let Some(sb) = scan_item(item, &mut transform, &metrics) {
                scanned.push(sb);
            }
        }
        if scanned.is_empty() {
            continue;
        }

        // Phase 2: group by schema hash for multi-batch transform.
        // Preserve insertion order so the earliest queued_at is easy to find.
        let groups = group_by_schema(scanned);

        // Phase 3: transform each group and send downstream.
        let mut should_break = false;
        for (_schema_hash, group) in groups {
            let earliest_queued_at = group
                .iter()
                .map(|sb| sb.queued_at)
                .min()
                .expect("group is non-empty");
            let total_scan_ns: u64 = group.iter().map(|sb| sb.scan_ns).sum();
            let input_index = group[0].input_index;

            let total_rows: u64 = group.iter().map(|sb| sb.batch.num_rows() as u64).sum();
            if total_rows > 0 {
                metrics.transform_in.inc_lines(total_rows);
            }

            // Consume the group in a single pass to avoid cloning checkpoints.
            let mut batches = Vec::with_capacity(group.len());
            let mut checkpoint_vecs = Vec::with_capacity(group.len());
            for sb in group {
                batches.push(sb.batch);
                checkpoint_vecs.push(sb.checkpoints);
            }
            let merged_checkpoints = merge_checkpoints(checkpoint_vecs);

            let t1 = Instant::now();
            let result = if batches.len() == 1 {
                rt.block_on(
                    transform
                        .transform
                        .execute(batches.into_iter().next().expect("verified len==1")),
                )
            } else {
                rt.block_on(transform.transform.execute_multi_batch(batches))
            };
            let result = match result {
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

            let msg = super::ChannelMsg {
                batch: result,
                checkpoints: merged_checkpoints,
                queued_at: Some(earliest_queued_at),
                input_index,
                scan_ns: total_scan_ns,
                transform_ns,
            };

            // Use blocking_send (not shutdown-aware) so we deliver all
            // remaining batches during shutdown drain. The CPU worker exits
            // when the I/O worker drops its sender (rx returns None).
            if tx.blocking_send(msg).is_err() {
                should_break = true;
                break;
            }
            metrics.inc_channel_depth();
        }
        if should_break {
            break;
        }
    }
}

/// Scan a single `IoWorkItem` into a `ScannedBatch`, handling errors.
///
/// Returns `None` when the item must be dropped (scan error, metadata
/// attach error). Metrics are updated before returning.
#[cfg(not(feature = "turmoil"))]
fn scan_item(
    item: IoWorkItem,
    transform: &mut InputTransform,
    metrics: &PipelineMetrics,
) -> Option<ScannedBatch> {
    match item {
        IoWorkItem::Bytes(chunk) => {
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
                    return None;
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
                    return None;
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
                    return None;
                }
            };
            let scan_ns = t0.elapsed().as_nanos() as u64;
            let sh = hash_schema(&batch.schema());
            Some(ScannedBatch {
                batch,
                checkpoints: chunk.checkpoints,
                queued_at: chunk.queued_at,
                input_index: chunk.input_index,
                scan_ns,
                schema_hash: sh,
            })
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
                    return None;
                }
            };
            let sh = hash_schema(&batch.schema());
            Some(ScannedBatch {
                batch,
                checkpoints,
                queued_at,
                input_index,
                scan_ns: 0,
                schema_hash: sh,
            })
        }
    }
}

/// Group scanned batches by schema hash, preserving insertion order
/// within each group.
#[cfg(not(feature = "turmoil"))]
fn group_by_schema(batches: Vec<ScannedBatch>) -> Vec<(u64, Vec<ScannedBatch>)> {
    let mut groups: Vec<(u64, Vec<ScannedBatch>)> = Vec::new();
    for sb in batches {
        if let Some((_hash, group)) = groups.iter_mut().find(|(h, _)| *h == sb.schema_hash) {
            group.push(sb);
        } else {
            let hash = sb.schema_hash;
            groups.push((hash, vec![sb]));
        }
    }
    groups
}
