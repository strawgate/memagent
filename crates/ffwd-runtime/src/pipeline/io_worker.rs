//! I/O worker: polls input source, accumulates bytes, sends chunks to CPU worker.

#[cfg(not(feature = "turmoil"))]
use std::collections::HashMap;
#[cfg(not(feature = "turmoil"))]
use std::path::PathBuf;
#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;
#[cfg(not(feature = "turmoil"))]
use std::time::{Duration, Instant};

#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatch;
#[cfg(not(feature = "turmoil"))]
use bytes::Bytes;
#[cfg(not(feature = "turmoil"))]
use tokio::sync::mpsc;
#[cfg(not(feature = "turmoil"))]
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "turmoil"))]
use ffwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(not(feature = "turmoil"))]
use ffwd_io::input::{CriMetadata, FramedReadEvent, SourceEvent};
#[cfg(not(feature = "turmoil"))]
use ffwd_io::poll_cadence::AdaptivePollController;
#[cfg(not(feature = "turmoil"))]
use ffwd_types::pipeline::SourceId;
#[cfg(not(feature = "turmoil"))]
use ffwd_types::source_metadata::SourceMetadataPlan;

#[cfg(not(feature = "turmoil"))]
use ffwd_io::tail::ByteOffset;

#[cfg(not(feature = "turmoil"))]
use std::collections::HashSet;

#[cfg(not(feature = "turmoil"))]
use super::buffered_input_policy::{
    should_flush_large_single_shared_buffer_chunk, should_repoll_buffered_shutdown,
};
#[cfg(not(feature = "turmoil"))]
use super::health::{
    HealthTransitionEvent, reduce_component_health, reduce_component_health_after_poll_failure,
};
#[cfg(not(feature = "turmoil"))]
use super::{IngestState, RowOriginSpan};

#[cfg(not(feature = "turmoil"))]
const SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS: usize = 64;
#[cfg(not(feature = "turmoil"))]
const MAX_SHUTDOWN_POLL_ROUNDS: usize = 4096;

#[cfg(not(feature = "turmoil"))]
pub(crate) fn should_repoll_shutdown(
    events: &[SourceEvent],
    is_finished: bool,
    had_source_payload: bool,
) -> bool {
    if is_finished {
        return false;
    }
    if events
        .iter()
        .any(|event| matches!(event, SourceEvent::EndOfFile { source_id: None }))
    {
        return false;
    }
    if events.is_empty() {
        return had_source_payload;
    }
    if had_source_payload
        && !events
            .iter()
            .any(|event| matches!(event, SourceEvent::Data { .. } | SourceEvent::Batch { .. }))
    {
        return true;
    }
    events.iter().any(|event| {
        let payload_source_id = match event {
            SourceEvent::Data { source_id, .. } | SourceEvent::Batch { source_id, .. } => {
                *source_id
            }
            SourceEvent::Rotated { .. }
            | SourceEvent::Truncated { .. }
            | SourceEvent::EndOfFile { .. } => {
                return false;
            }
        };
        !events.iter().any(|event| {
            matches!(
                event,
                SourceEvent::EndOfFile { source_id }
                    if *source_id == payload_source_id
            )
        })
    })
}

#[cfg(not(feature = "turmoil"))]
fn framed_read_events_have_payload(events: &[FramedReadEvent]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, FramedReadEvent::Data { .. }))
}

#[cfg(not(feature = "turmoil"))]
fn is_large_single_buffered_data_event(
    events: &[FramedReadEvent],
    buffer_was_empty_at_poll: bool,
) -> bool {
    let Some(FramedReadEvent::Data { range, .. }) = events.first() else {
        return false;
    };
    should_flush_large_single_shared_buffer_chunk(
        buffer_was_empty_at_poll,
        events.len(),
        range.start == 0,
        range.len(),
    )
}

#[cfg(not(feature = "turmoil"))]
pub(crate) fn should_repoll_shutdown_buffered(
    events: &[FramedReadEvent],
    is_finished: bool,
    had_source_payload: bool,
) -> bool {
    should_repoll_buffered_shutdown(
        is_finished,
        had_source_payload,
        !events.is_empty(),
        framed_read_events_have_payload(events),
    )
}

// ---------------------------------------------------------------------------
// IoChunk / IoWorkItem types
// ---------------------------------------------------------------------------

/// Chunk of accumulated bytes sent from I/O worker to CPU worker.
#[cfg(not(feature = "turmoil"))]
pub(crate) struct IoChunk {
    pub bytes: Bytes,
    pub checkpoints: Vec<(SourceId, ByteOffset)>,
    pub row_origins: Vec<RowOriginSpan>,
    pub source_paths: HashMap<SourceId, String>,
    pub cri_metadata: Option<CriMetadata>,
    pub queued_at: tokio::time::Instant,
    pub input_index: usize,
}

#[cfg(not(feature = "turmoil"))]
pub(crate) enum IoWorkItem {
    RawBatch(IoChunk),
    Batch {
        batch: RecordBatch,
        checkpoints: Vec<(SourceId, ByteOffset)>,
        row_origins: Vec<RowOriginSpan>,
        source_paths: HashMap<SourceId, String>,
        queued_at: tokio::time::Instant,
        input_index: usize,
    },
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

#[cfg(not(feature = "turmoil"))]
fn source_paths_by_id(
    paths: &[(SourceId, PathBuf)],
    row_origins: &[RowOriginSpan],
) -> HashMap<SourceId, String> {
    let wanted: HashSet<SourceId> = row_origins
        .iter()
        .filter_map(|origin| origin.source_id)
        .collect();
    if wanted.is_empty() {
        return HashMap::new();
    }

    paths
        .iter()
        .filter(|(sid, _)| wanted.contains(sid))
        .map(|(sid, path)| (*sid, path.to_string_lossy().into_owned()))
        .collect()
}

#[cfg(not(feature = "turmoil"))]
fn capture_source_path(
    buffered_source_paths: &mut HashMap<SourceId, String>,
    source_paths: &[(SourceId, PathBuf)],
    source_id: Option<SourceId>,
) {
    let Some(source_id) = source_id else {
        return;
    };
    if buffered_source_paths.contains_key(&source_id) {
        return;
    }
    if let Some((_, path)) = source_paths.iter().find(|(sid, _)| *sid == source_id) {
        buffered_source_paths.insert(source_id, path.to_string_lossy().into_owned());
    }
}

#[cfg(not(feature = "turmoil"))]
pub(super) fn scanner_ready_row_count(bytes: &[u8]) -> usize {
    let mut rows = 0usize;
    let mut line_start = 0usize;
    for newline in memchr::memchr_iter(b'\n', bytes) {
        let line_end = if newline > line_start && bytes.get(newline - 1).copied() == Some(b'\r') {
            newline - 1
        } else {
            newline
        };
        if line_end > line_start {
            rows += 1;
        }
        line_start = newline + 1;
    }

    if line_start < bytes.len() {
        let line_end = if bytes.last().copied() == Some(b'\r') {
            bytes.len() - 1
        } else {
            bytes.len()
        };
        if line_end > line_start {
            rows += 1;
        }
    }

    rows
}

#[cfg(not(feature = "turmoil"))]
pub(super) fn append_cri_metadata_for_data(
    buffered_metadata: &mut CriMetadata,
    event_metadata: Option<CriMetadata>,
    buffered_bytes: &[u8],
    event_bytes: &[u8],
) {
    match event_metadata {
        Some(metadata) => {
            if buffered_metadata.is_empty() && !buffered_bytes.is_empty() {
                buffered_metadata.append_null_rows(scanner_ready_row_count(buffered_bytes));
            }
            buffered_metadata.append(metadata);
        }
        None if !buffered_metadata.is_empty() => {
            buffered_metadata.append_null_rows(scanner_ready_row_count(event_bytes));
        }
        None => {}
    }
}

#[cfg(not(feature = "turmoil"))]
pub(super) fn take_cri_metadata_preserving_capacity(metadata: &mut CriMetadata) -> CriMetadata {
    let replacement = metadata.empty_with_preserved_capacity();
    std::mem::replace(metadata, replacement)
}

#[cfg(not(feature = "turmoil"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingRowOrigin {
    pub source_id: Option<SourceId>,
    pub input_name: Arc<str>,
    pub has_content: bool,
}

#[cfg(not(feature = "turmoil"))]
fn scanner_line_has_content(bytes: &[u8]) -> bool {
    let line_end = if bytes.last().copied() == Some(b'\r') {
        bytes.len().saturating_sub(1)
    } else {
        bytes.len()
    };
    line_end > 0
}

#[cfg(not(feature = "turmoil"))]
pub(super) fn append_row_origin(
    row_origins: &mut Vec<RowOriginSpan>,
    source_id: Option<SourceId>,
    input_name: &Arc<str>,
    rows: usize,
) {
    if rows == 0 {
        return;
    }
    if let Some(last) = row_origins.last_mut()
        && last.source_id == source_id
        && Arc::ptr_eq(&last.input_name, input_name)
    {
        last.rows += rows;
        return;
    }
    row_origins.push(RowOriginSpan {
        source_id,
        input_name: Arc::clone(input_name),
        rows,
    });
}

#[cfg(not(feature = "turmoil"))]
pub(super) fn append_data_row_origins(
    row_origins: &mut Vec<RowOriginSpan>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
    source_id: Option<SourceId>,
    input_name: &Arc<str>,
    bytes: &[u8],
) {
    if bytes.is_empty() {
        return;
    }

    let mut line_start = 0usize;
    for newline in memchr::memchr_iter(b'\n', bytes) {
        let segment_has_content = scanner_line_has_content(&bytes[line_start..newline]);
        if let Some(pending) = pending_row_origin
            && pending.source_id != source_id
        {
            tracing::debug!(
                pending_source_id = ?pending.source_id,
                current_source_id = ?source_id,
                "split line crosses source boundary; attributing to first fragment"
            );
        }
        let origin = pending_row_origin
            .take()
            .unwrap_or_else(|| PendingRowOrigin {
                source_id,
                input_name: Arc::clone(input_name),
                has_content: false,
            });
        if origin.has_content || segment_has_content {
            append_row_origin(row_origins, origin.source_id, &origin.input_name, 1);
        }
        line_start = newline + 1;
    }

    let tail = &bytes[line_start..];
    if !tail.is_empty() {
        let tail_has_content = scanner_line_has_content(tail);
        match pending_row_origin {
            Some(origin) => {
                origin.has_content |= tail_has_content;
            }
            None => {
                *pending_row_origin = Some(PendingRowOrigin {
                    source_id,
                    input_name: Arc::clone(input_name),
                    has_content: tail_has_content,
                });
            }
        }
    }
}

#[cfg(not(feature = "turmoil"))]
pub(super) fn finalize_pending_row_origin(
    row_origins: &mut Vec<RowOriginSpan>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
) {
    if let Some(origin) = pending_row_origin.take()
        && origin.has_content
    {
        append_row_origin(row_origins, origin.source_id, &origin.input_name, 1);
    }
}

// ---------------------------------------------------------------------------
// flush_buf
// ---------------------------------------------------------------------------

/// Flush `buf` to the channel as an `IoWorkItem::RawBatch` chunk.
///
/// Returns `false` if the channel is closed (caller should exit the I/O loop).
/// On backpressure (channel full), logs a warning at most once per 5 s and
/// blocks until the CPU worker drains.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
pub(super) fn flush_buf(
    buf: &mut bytes::BytesMut,
    row_origins: &mut Vec<RowOriginSpan>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
    buffered_source_paths: &mut HashMap<SourceId, String>,
    cri_metadata: &mut CriMetadata,
    source: &dyn ffwd_io::input::InputSource,
    tx: &mpsc::Sender<IoWorkItem>,
    metrics: &PipelineMetrics,
    last_bp_warn: &mut Option<Instant>,
    input_index: usize,
    source_metadata_plan: SourceMetadataPlan,
) -> bool {
    if buf.is_empty() {
        row_origins.clear();
        *pending_row_origin = None;
        buffered_source_paths.clear();
        cri_metadata.clear();
        return true;
    }
    if source_metadata_plan.has_any() {
        finalize_pending_row_origin(row_origins, pending_row_origin);
    } else {
        *pending_row_origin = None;
    }
    let data = buf.split().freeze();
    let cri_metadata = if cri_metadata.is_empty() {
        cri_metadata.clear();
        None
    } else {
        Some(take_cri_metadata_preserving_capacity(cri_metadata))
    };
    let checkpoints = source.checkpoint_data();
    let source_paths = if source_metadata_plan.has_source_path() {
        std::mem::take(buffered_source_paths)
    } else {
        buffered_source_paths.clear();
        HashMap::new()
    };
    let chunk = IoWorkItem::RawBatch(IoChunk {
        bytes: data,
        checkpoints,
        row_origins: std::mem::take(row_origins),
        source_paths,
        cri_metadata,
        queued_at: tokio::time::Instant::now(),
        input_index,
    });
    match tx.try_send(chunk) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(chunk)) => {
            if last_bp_warn.is_none_or(|t| t.elapsed() >= Duration::from_secs(5)) {
                tracing::warn!(input = source.name(), "input.backpressure");
                *last_bp_warn = Some(Instant::now());
            }
            metrics.inc_backpressure_stall();
            tx.blocking_send(chunk).is_ok()
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

/// Flush a `Bytes` directly to the CPU worker without copying into BytesMut.
///
/// Used when the accumulation buffer is empty and a single chunk from
/// framing already meets the batch target size. Avoids the
/// `extend_from_slice` copy in the common case of large file reads.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
fn flush_bytes_direct(
    data: Bytes,
    row_origins: &mut Vec<RowOriginSpan>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
    buffered_source_paths: &mut HashMap<SourceId, String>,
    cri_metadata: &mut CriMetadata,
    source: &dyn ffwd_io::input::InputSource,
    tx: &mpsc::Sender<IoWorkItem>,
    metrics: &PipelineMetrics,
    last_bp_warn: &mut Option<Instant>,
    input_index: usize,
    source_metadata_plan: SourceMetadataPlan,
) -> bool {
    if source_metadata_plan.has_any() {
        finalize_pending_row_origin(row_origins, pending_row_origin);
    } else {
        *pending_row_origin = None;
    }
    let cri_md = if cri_metadata.is_empty() {
        cri_metadata.clear();
        None
    } else {
        Some(take_cri_metadata_preserving_capacity(cri_metadata))
    };
    let checkpoints = source.checkpoint_data();
    let source_paths = if source_metadata_plan.has_source_path() {
        std::mem::take(buffered_source_paths)
    } else {
        buffered_source_paths.clear();
        HashMap::new()
    };
    let chunk = IoWorkItem::RawBatch(IoChunk {
        bytes: data,
        checkpoints,
        row_origins: std::mem::take(row_origins),
        source_paths,
        cri_metadata: cri_md,
        queued_at: tokio::time::Instant::now(),
        input_index,
    });
    match tx.try_send(chunk) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(chunk)) => {
            if last_bp_warn.is_none_or(|t| t.elapsed() >= Duration::from_secs(5)) {
                tracing::warn!(input = source.name(), "input.backpressure");
                *last_bp_warn = Some(Instant::now());
            }
            metrics.inc_backpressure_stall();
            tx.blocking_send(chunk).is_ok()
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

// ---------------------------------------------------------------------------
// process_ingest_events
// ---------------------------------------------------------------------------

#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
pub(super) fn process_ingest_events(
    input: &mut IngestState,
    input_name: &Arc<str>,
    events: Vec<SourceEvent>,
    tx: &mpsc::Sender<IoWorkItem>,
    metrics: &PipelineMetrics,
    last_bp_warn: &mut Option<Instant>,
    input_index: usize,
    safe_batch_target_bytes: usize,
    buffered_since: &mut Option<Instant>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
    source_metadata_plan: SourceMetadataPlan,
) -> bool {
    let source_path_snapshot = if source_metadata_plan.has_source_path() {
        input.source.source_paths()
    } else {
        Vec::new()
    };

    for event in events {
        match event {
            SourceEvent::Data {
                bytes,
                source_id,
                cri_metadata,
                ..
            } => {
                if source_metadata_plan.has_any() {
                    if source_metadata_plan.has_source_path() {
                        capture_source_path(
                            &mut input.source_paths,
                            &source_path_snapshot,
                            source_id,
                        );
                    }
                    append_data_row_origins(
                        &mut input.row_origins,
                        pending_row_origin,
                        source_id,
                        input_name,
                        &bytes,
                    );
                }
                append_cri_metadata_for_data(
                    &mut input.cri_metadata,
                    cri_metadata,
                    &input.buf,
                    &bytes,
                );

                // Fast path: when the accumulation buffer is empty and this
                // single chunk is large enough for efficient scanning, send
                // it directly without copying into BytesMut.
                //
                // Benchmark data (batch_size_sweep): per-scan overhead is
                // negligible above 64KB, so we flush directly any chunk that
                // exceeds this threshold. This makes the common case of a
                // 256KB file read zero-copy end-to-end.
                const MIN_DIRECT_FLUSH_BYTES: usize = 64 * 1024;
                if input.buf.is_empty() && bytes.len() >= MIN_DIRECT_FLUSH_BYTES {
                    metrics.inc_flush_by_size();
                    if !flush_bytes_direct(
                        bytes,
                        &mut input.row_origins,
                        pending_row_origin,
                        &mut input.source_paths,
                        &mut input.cri_metadata,
                        &*input.source,
                        tx,
                        metrics,
                        last_bp_warn,
                        input_index,
                        source_metadata_plan,
                    ) {
                        return false;
                    }
                    *buffered_since = None;
                    continue;
                }

                input.buf.extend_from_slice(&bytes);
                // Flush eagerly when the buffer reaches the target so
                // that a single poll returning many Data events cannot
                // accumulate an unbounded buffer.
                if input.buf.len() >= safe_batch_target_bytes {
                    metrics.inc_flush_by_size();
                    if !flush_buf(
                        &mut input.buf,
                        &mut input.row_origins,
                        pending_row_origin,
                        &mut input.source_paths,
                        &mut input.cri_metadata,
                        &*input.source,
                        tx,
                        metrics,
                        last_bp_warn,
                        input_index,
                        source_metadata_plan,
                    ) {
                        return false;
                    }
                    *buffered_since = None;
                }
            }
            SourceEvent::Batch {
                batch,
                source_id,
                accounted_bytes,
            } => {
                input.stats.inc_lines(batch.num_rows() as u64);
                input.stats.inc_bytes(accounted_bytes);
                if !flush_buf(
                    &mut input.buf,
                    &mut input.row_origins,
                    pending_row_origin,
                    &mut input.source_paths,
                    &mut input.cri_metadata,
                    &*input.source,
                    tx,
                    metrics,
                    last_bp_warn,
                    input_index,
                    source_metadata_plan,
                ) {
                    return false;
                }
                *buffered_since = None;

                let row_origins = if source_metadata_plan.has_any() {
                    vec![RowOriginSpan {
                        source_id,
                        input_name: Arc::clone(input_name),
                        rows: batch.num_rows(),
                    }]
                } else {
                    Vec::new()
                };
                let source_paths = if source_metadata_plan.has_source_path() {
                    source_paths_by_id(&source_path_snapshot, &row_origins)
                } else {
                    HashMap::new()
                };
                let item = IoWorkItem::Batch {
                    batch,
                    checkpoints: input.source.checkpoint_data(),
                    row_origins,
                    source_paths,
                    queued_at: tokio::time::Instant::now(),
                    input_index,
                };
                match tx.try_send(item) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(item)) => {
                        if last_bp_warn.is_none_or(|t| t.elapsed() >= Duration::from_secs(5)) {
                            tracing::warn!(input = input.source.name(), "input.backpressure");
                            *last_bp_warn = Some(Instant::now());
                        }
                        metrics.inc_backpressure_stall();
                        if tx.blocking_send(item).is_err() {
                            return false;
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return false,
                }
            }
            SourceEvent::Rotated { .. } => {
                input.stats.inc_rotations();
                tracing::info!(input = input.source.name(), "input.file_rotated");
            }
            SourceEvent::Truncated { .. } => {
                input.stats.inc_rotations();
                tracing::info!(input = input.source.name(), "input.file_truncated");
            }
            SourceEvent::EndOfFile { .. } => {}
        }
    }
    if buffered_since.is_none() && !input.buf.is_empty() {
        *buffered_since = Some(Instant::now());
    }
    true
}

#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
pub(super) fn process_buffered_events(
    input: &mut IngestState,
    input_name: &Arc<str>,
    events: Vec<FramedReadEvent>,
    tx: &mpsc::Sender<IoWorkItem>,
    metrics: &PipelineMetrics,
    last_bp_warn: &mut Option<Instant>,
    input_index: usize,
    safe_batch_target_bytes: usize,
    buffered_since: &mut Option<Instant>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
    buffer_was_empty_at_poll: bool,
    source_metadata_plan: SourceMetadataPlan,
) -> bool {
    let source_path_snapshot = if source_metadata_plan.has_source_path() {
        input.source.source_paths()
    } else {
        Vec::new()
    };
    let should_flush_large_single_chunk =
        is_large_single_buffered_data_event(&events, buffer_was_empty_at_poll);

    for event in events {
        match event {
            FramedReadEvent::Data {
                range,
                source_id,
                cri_metadata,
            } => {
                if source_metadata_plan.has_any() {
                    if source_metadata_plan.has_source_path() {
                        capture_source_path(
                            &mut input.source_paths,
                            &source_path_snapshot,
                            source_id,
                        );
                    }
                    append_data_row_origins(
                        &mut input.row_origins,
                        pending_row_origin,
                        source_id,
                        input_name,
                        &input.buf[range.clone()],
                    );
                }
                append_cri_metadata_for_data(
                    &mut input.cri_metadata,
                    cri_metadata,
                    &input.buf[..range.start],
                    &input.buf[range],
                );
            }
            FramedReadEvent::Rotated { .. } => {
                input.stats.inc_rotations();
                tracing::info!(input = input.source.name(), "input.file_rotated");
            }
            FramedReadEvent::Truncated { .. } => {
                input.stats.inc_rotations();
                tracing::info!(input = input.source.name(), "input.file_truncated");
            }
        }
    }
    let flush_by_size = input.buf.len() >= safe_batch_target_bytes;
    if flush_by_size || should_flush_large_single_chunk {
        metrics.inc_flush_by_size();
        if !flush_buf(
            &mut input.buf,
            &mut input.row_origins,
            pending_row_origin,
            &mut input.source_paths,
            &mut input.cri_metadata,
            &*input.source,
            tx,
            metrics,
            last_bp_warn,
            input_index,
            source_metadata_plan,
        ) {
            return false;
        }
        *buffered_since = None;
        return true;
    }
    if buffered_since.is_none() && !input.buf.is_empty() {
        *buffered_since = Some(Instant::now());
    }
    true
}

// ---------------------------------------------------------------------------
// I/O worker loop
// ---------------------------------------------------------------------------

/// Run the I/O worker loop for one input.
///
/// Polls the input source, accumulates bytes to `batch_target_bytes`,
/// and sends chunks to the CPU worker via `tx`.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
pub(super) fn io_worker_loop(
    mut input: IngestState,
    input_name: Arc<str>,
    tx: mpsc::Sender<IoWorkItem>,
    metrics: Arc<PipelineMetrics>,
    mut control_rx: tokio::sync::broadcast::Receiver<super::ControlMessage>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    input_index: usize,
    source_metadata_plan: SourceMetadataPlan,
) {
    let mut buffered_since: Option<Instant> = None;
    let mut pending_row_origin: Option<PendingRowOrigin> = None;
    let mut last_bp_warn: Option<Instant> = None;
    let mut consecutive_poll_failures: u32 = 0;
    let mut adaptive_poll =
        AdaptivePollController::new(input.source.get_cadence().adaptive_fast_polls_max);
    let safe_batch_target_bytes = batch_target_bytes.max(1);

    'io_loop: loop {
        // Check for control messages (non-blocking)
        while let Ok(msg) = control_rx.try_recv() {
            match msg {
                super::ControlMessage::Shutdown => {
                    tracing::debug!(input = %input_name, "io_worker: received shutdown control, entering drain mode");
                    shutdown.cancel();
                }
                super::ControlMessage::DrainIngress => {
                    tracing::debug!(input = %input_name, "io_worker: received drain ingress control");
                }
                super::ControlMessage::Flush => {
                    tracing::debug!(input = %input_name, "io_worker: received flush control");
                }
                super::ControlMessage::Reconfigure => {
                    tracing::debug!(input = %input_name, "io_worker: received reconfigure control");
                }
            }
        }

        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            let mut shutdown_poll_rounds = 0usize;
            loop {
                let buffer_was_empty_at_poll = input.buf.is_empty();
                let buffered_events = match input.source.poll_shutdown_into(&mut input.buf) {
                    Ok(events) => events,
                    Err(e) => {
                        tracing::warn!(
                            input = input.source.name(),
                            error = %e,
                            "input.shutdown_poll_error"
                        );
                        break;
                    }
                };

                match buffered_events {
                    Some(events) => {
                        let cadence = input.source.get_cadence();
                        let should_repoll = should_repoll_shutdown_buffered(
                            &events,
                            input.source.is_finished(),
                            cadence.signal.had_data,
                        );
                        if !process_buffered_events(
                            &mut input,
                            &input_name,
                            events,
                            &tx,
                            &metrics,
                            &mut last_bp_warn,
                            input_index,
                            safe_batch_target_bytes,
                            &mut buffered_since,
                            &mut pending_row_origin,
                            buffer_was_empty_at_poll,
                            source_metadata_plan,
                        ) {
                            break 'io_loop;
                        }
                        if !should_repoll {
                            break;
                        }
                        shutdown_poll_rounds = shutdown_poll_rounds.saturating_add(1);
                        if shutdown_poll_rounds
                            .is_multiple_of(SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS)
                        {
                            tracing::warn!(
                                input = input.source.name(),
                                rounds = shutdown_poll_rounds,
                                "input.shutdown_drain_still_active"
                            );
                        }
                        if shutdown_poll_rounds >= MAX_SHUTDOWN_POLL_ROUNDS {
                            tracing::error!(
                                input = input.source.name(),
                                rounds = shutdown_poll_rounds,
                                "input.shutdown_drain_aborted_hard_limit"
                            );
                            break;
                        }
                    }
                    None => match input.source.poll_shutdown() {
                        Ok(events) => {
                            let cadence = input.source.get_cadence();
                            let should_repoll = should_repoll_shutdown(
                                &events,
                                input.source.is_finished(),
                                cadence.signal.had_data,
                            );
                            if !process_ingest_events(
                                &mut input,
                                &input_name,
                                events,
                                &tx,
                                &metrics,
                                &mut last_bp_warn,
                                input_index,
                                safe_batch_target_bytes,
                                &mut buffered_since,
                                &mut pending_row_origin,
                                source_metadata_plan,
                            ) {
                                break 'io_loop;
                            }
                            if !should_repoll {
                                break;
                            }
                            shutdown_poll_rounds = shutdown_poll_rounds.saturating_add(1);
                            if shutdown_poll_rounds
                                .is_multiple_of(SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS)
                            {
                                tracing::warn!(
                                    input = input.source.name(),
                                    rounds = shutdown_poll_rounds,
                                    "input.shutdown_drain_still_active"
                                );
                            }
                            if shutdown_poll_rounds >= MAX_SHUTDOWN_POLL_ROUNDS {
                                tracing::error!(
                                    input = input.source.name(),
                                    rounds = shutdown_poll_rounds,
                                    "input.shutdown_drain_aborted_hard_limit"
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                input = input.source.name(),
                                error = %e,
                                "input.shutdown_poll_error"
                            );
                            break;
                        }
                    },
                }
            }
            break;
        }

        let buffer_was_empty_at_poll = input.buf.is_empty();
        let buffered_events = match input.source.poll_into(&mut input.buf) {
            Ok(events) => events,
            Err(e) => {
                adaptive_poll.reset_fast_polls();
                input.stats.inc_errors();
                consecutive_poll_failures = consecutive_poll_failures.saturating_add(1);
                input
                    .stats
                    .set_health(reduce_component_health_after_poll_failure(
                        input.stats.health(),
                        consecutive_poll_failures,
                    ));
                tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        consecutive_poll_failures = 0;

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));
        let cadence = input.source.get_cadence();
        adaptive_poll.observe_signal(cadence.signal);

        match buffered_events {
            Some(events) => {
                if events.is_empty() {
                    if adaptive_poll.should_fast_poll() {
                        metrics.inc_cadence_fast_repoll();
                    } else {
                        metrics.inc_cadence_idle_sleep();
                        std::thread::sleep(poll_interval);
                    }
                } else if !process_buffered_events(
                    &mut input,
                    &input_name,
                    events,
                    &tx,
                    &metrics,
                    &mut last_bp_warn,
                    input_index,
                    safe_batch_target_bytes,
                    &mut buffered_since,
                    &mut pending_row_origin,
                    buffer_was_empty_at_poll,
                    source_metadata_plan,
                ) {
                    break 'io_loop;
                }
            }
            None => {
                let events = match input.source.poll() {
                    Ok(e) => e,
                    Err(e) => {
                        adaptive_poll.reset_fast_polls();
                        input.stats.inc_errors();
                        consecutive_poll_failures = consecutive_poll_failures.saturating_add(1);
                        input
                            .stats
                            .set_health(reduce_component_health_after_poll_failure(
                                input.stats.health(),
                                consecutive_poll_failures,
                            ));
                        tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                        std::thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                };

                if events.is_empty() {
                    if adaptive_poll.should_fast_poll() {
                        metrics.inc_cadence_fast_repoll();
                    } else {
                        metrics.inc_cadence_idle_sleep();
                        std::thread::sleep(poll_interval);
                    }
                } else if !process_ingest_events(
                    &mut input,
                    &input_name,
                    events,
                    &tx,
                    &metrics,
                    &mut last_bp_warn,
                    input_index,
                    safe_batch_target_bytes,
                    &mut buffered_since,
                    &mut pending_row_origin,
                    source_metadata_plan,
                ) {
                    break 'io_loop;
                }
            }
        }

        if input.source.is_finished() {
            if !input.buf.is_empty() {
                metrics.inc_flush_by_timeout();
                if !flush_buf(
                    &mut input.buf,
                    &mut input.row_origins,
                    &mut pending_row_origin,
                    &mut input.source_paths,
                    &mut input.cri_metadata,
                    &*input.source,
                    &tx,
                    &metrics,
                    &mut last_bp_warn,
                    input_index,
                    source_metadata_plan,
                ) {
                    break;
                }
            }
            break;
        }

        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let flush_by_size = input.buf.len() >= safe_batch_target_bytes;
        let flush_by_timeout = !input.buf.is_empty() && timeout_elapsed;

        if flush_by_size || flush_by_timeout {
            if flush_by_size {
                metrics.inc_flush_by_size();
            } else {
                metrics.inc_flush_by_timeout();
            }

            if !flush_buf(
                &mut input.buf,
                &mut input.row_origins,
                &mut pending_row_origin,
                &mut input.source_paths,
                &mut input.cri_metadata,
                &*input.source,
                &tx,
                &metrics,
                &mut last_bp_warn,
                input_index,
                source_metadata_plan,
            ) {
                break;
            }
            buffered_since = None;
        }
    }

    // Drain remaining buffered data on shutdown.
    // Use blocking_send (not shutdown-aware) because shutdown is already
    // cancelled and we want to deliver remaining data to the CPU worker.
    if !input.buf.is_empty() {
        if source_metadata_plan.has_any() {
            finalize_pending_row_origin(&mut input.row_origins, &mut pending_row_origin);
        } else {
            let _ = pending_row_origin.take();
        }
        let data = input.buf.split().freeze();
        let cri_metadata = if input.cri_metadata.is_empty() {
            input.cri_metadata.clear();
            None
        } else {
            Some(take_cri_metadata_preserving_capacity(
                &mut input.cri_metadata,
            ))
        };
        let checkpoints = input.source.checkpoint_data();
        let source_paths = if source_metadata_plan.has_source_path() {
            std::mem::take(&mut input.source_paths)
        } else {
            input.source_paths.clear();
            HashMap::new()
        };
        let chunk = IoWorkItem::RawBatch(IoChunk {
            bytes: data,
            checkpoints,
            row_origins: std::mem::take(&mut input.row_origins),
            source_paths,
            cri_metadata,
            queued_at: tokio::time::Instant::now(),
            input_index,
        });
        if tx.blocking_send(chunk).is_err() {
            tracing::warn!(
                input = input.source.name(),
                "input.channel_closed_on_shutdown_drain"
            );
        }
    }

    input.stats.set_health(reduce_component_health(
        input.stats.health(),
        HealthTransitionEvent::ShutdownCompleted,
    ));
}
