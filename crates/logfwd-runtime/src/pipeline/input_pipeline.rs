//! Input pipeline: I/O → CPU worker separation.
//!
//! Every input gets two dedicated OS threads:
//! - **I/O worker**: polls the input source, accumulates bytes, sends chunks
//! - **CPU worker**: scans bytes into RecordBatch, runs SQL transform
//!
//! The two workers are connected by a bounded channel (capacity 4). Backpressure
//! cascades: output pool blocks → pipeline blocks → CPU worker blocks → I/O
//! worker blocks on its source.
//!
//! # Architecture
//!
//! ```text
//! Per input:
//! ┌──────────┐  bounded(4)  ┌──────────┐  ChannelMsg
//! │ I/O      │─────────────▶│ CPU      │──────────────────────▶ pipeline rx
//! │ Worker   │              │ Worker   │
//! │ poll()   │              │ scan()   │
//! │ accum()  │              │ sql()    │
//! └──────────┘              └──────────┘
//!  OS thread                 OS thread
//! ```

#[cfg(not(feature = "turmoil"))]
use std::collections::{HashMap, HashSet};
#[cfg(not(feature = "turmoil"))]
use std::path::PathBuf;
#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;
#[cfg(not(feature = "turmoil"))]
use std::time::{Duration, Instant};

#[cfg(not(feature = "turmoil"))]
use arrow::array::{Array, ArrayRef, StringViewBuilder, UInt64Builder};
#[cfg(not(feature = "turmoil"))]
use arrow::buffer::Buffer;
#[cfg(not(feature = "turmoil"))]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(not(feature = "turmoil"))]
use arrow::error::ArrowError;
#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatch;
#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatchOptions;
#[cfg(not(feature = "turmoil"))]
use bytes::Bytes;
#[cfg(not(feature = "turmoil"))]
use tokio::sync::mpsc;
#[cfg(not(feature = "turmoil"))]
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "turmoil"))]
use logfwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(not(feature = "turmoil"))]
use logfwd_io::input::InputEvent;
#[cfg(not(feature = "turmoil"))]
use logfwd_io::poll_cadence::AdaptivePollController;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::field_names;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::pipeline::SourceId;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::source_metadata::SourceMetadataPlan;

#[cfg(not(feature = "turmoil"))]
use logfwd_io::tail::ByteOffset;

#[cfg(not(feature = "turmoil"))]
use super::health::{
    HealthTransitionEvent, reduce_component_health, reduce_component_health_after_poll_failure,
};
#[cfg(not(feature = "turmoil"))]
use super::{InputState, InputTransform, RowOriginSpan};

/// Capacity of the bounded channel between I/O worker and CPU worker.
#[cfg(not(feature = "turmoil"))]
const IO_CPU_CHANNEL_CAPACITY: usize = 4;

#[cfg(not(feature = "turmoil"))]
const SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS: usize = 64;
#[cfg(not(feature = "turmoil"))]
const MAX_SHUTDOWN_POLL_ROUNDS: usize = 4096;

#[cfg(not(feature = "turmoil"))]
fn should_repoll_shutdown(
    events: &[InputEvent],
    is_finished: bool,
    had_source_payload: bool,
) -> bool {
    if is_finished {
        return false;
    }
    if events
        .iter()
        .any(|event| matches!(event, InputEvent::EndOfFile { source_id: None }))
    {
        return false;
    }
    if events.is_empty() {
        return had_source_payload;
    }
    if had_source_payload
        && !events
            .iter()
            .any(|event| matches!(event, InputEvent::Data { .. } | InputEvent::Batch { .. }))
    {
        return true;
    }
    events.iter().any(|event| {
        let payload_source_id = match event {
            InputEvent::Data { source_id, .. } | InputEvent::Batch { source_id, .. } => *source_id,
            InputEvent::Rotated { .. }
            | InputEvent::Truncated { .. }
            | InputEvent::EndOfFile { .. } => {
                return false;
            }
        };
        !events.iter().any(|event| {
            matches!(
                event,
                InputEvent::EndOfFile { source_id }
                    if *source_id == payload_source_id
            )
        })
    })
}

/// Flush `buf` to the channel as an `IoWorkItem::Bytes` chunk.
///
/// Returns `false` if the channel is closed (caller should exit the I/O loop).
/// On backpressure (channel full), logs a warning at most once per 5 s and
/// blocks until the CPU worker drains.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
fn flush_buf(
    buf: &mut bytes::BytesMut,
    row_origins: &mut Vec<RowOriginSpan>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
    buffered_source_paths: &mut HashMap<SourceId, String>,
    source: &dyn logfwd_io::input::InputSource,
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
        return true;
    }
    if source_metadata_plan.has_any() {
        finalize_pending_row_origin(row_origins, pending_row_origin);
    } else {
        *pending_row_origin = None;
    }
    let data = buf.split().freeze();
    let checkpoints = source.checkpoint_data();
    let source_paths = if source_metadata_plan.has_source_path() {
        std::mem::take(buffered_source_paths)
    } else {
        buffered_source_paths.clear();
        HashMap::new()
    };
    let chunk = IoWorkItem::Bytes(IoChunk {
        bytes: data,
        checkpoints,
        row_origins: std::mem::take(row_origins),
        source_paths,
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

#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
fn process_io_events(
    input: &mut InputState,
    input_name: &Arc<str>,
    events: Vec<InputEvent>,
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
            InputEvent::Data {
                bytes, source_id, ..
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
            InputEvent::Batch {
                batch, source_id, ..
            } => {
                if !flush_buf(
                    &mut input.buf,
                    &mut input.row_origins,
                    pending_row_origin,
                    &mut input.source_paths,
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
            InputEvent::Rotated { .. } => {
                input.stats.inc_rotations();
                tracing::info!(input = input.source.name(), "input.file_rotated");
            }
            InputEvent::Truncated { .. } => {
                input.stats.inc_rotations();
                tracing::info!(input = input.source.name(), "input.file_truncated");
            }
            InputEvent::EndOfFile { .. } => {}
        }
    }
    if buffered_since.is_none() && !input.buf.is_empty() {
        *buffered_since = Some(Instant::now());
    }
    true
}

// ---------------------------------------------------------------------------
// I/O worker — reads bytes from source, accumulates, sends to CPU worker
// ---------------------------------------------------------------------------

/// Chunk of accumulated bytes sent from I/O worker to CPU worker.
#[cfg(not(feature = "turmoil"))]
pub(crate) struct IoChunk {
    pub bytes: Bytes,
    pub checkpoints: Vec<(SourceId, ByteOffset)>,
    pub row_origins: Vec<RowOriginSpan>,
    pub source_paths: HashMap<SourceId, String>,
    pub queued_at: tokio::time::Instant,
    pub input_index: usize,
}

#[cfg(not(feature = "turmoil"))]
pub(crate) enum IoWorkItem {
    Bytes(IoChunk),
    Batch {
        batch: RecordBatch,
        checkpoints: Vec<(SourceId, ByteOffset)>,
        row_origins: Vec<RowOriginSpan>,
        source_paths: HashMap<SourceId, String>,
        queued_at: tokio::time::Instant,
        input_index: usize,
    },
}

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

#[cfg(all(test, not(feature = "turmoil")))]
fn scanner_ready_row_count(bytes: &[u8]) -> usize {
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
#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingRowOrigin {
    source_id: Option<SourceId>,
    input_name: Arc<str>,
    has_content: bool,
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
fn append_row_origin(
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
fn append_data_row_origins(
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
fn finalize_pending_row_origin(
    row_origins: &mut Vec<RowOriginSpan>,
    pending_row_origin: &mut Option<PendingRowOrigin>,
) {
    if let Some(origin) = pending_row_origin.take()
        && origin.has_content
    {
        append_row_origin(row_origins, origin.source_id, &origin.input_name, 1);
    }
}

#[cfg(not(feature = "turmoil"))]
fn source_metadata_for_batch(
    batch: RecordBatch,
    row_origins: &[RowOriginSpan],
    source_paths: &HashMap<SourceId, String>,
    plan: SourceMetadataPlan,
) -> Result<RecordBatch, ArrowError> {
    if !plan.has_any() {
        return Ok(batch);
    }

    let rows_from_origins: usize = row_origins.iter().map(|span| span.rows).sum();
    if rows_from_origins != batch.num_rows() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "source metadata sidecar row count mismatch: spans={rows_from_origins}, batch={}",
            batch.num_rows()
        )));
    }

    let mut columns = Vec::with_capacity(2);
    if plan.has_source_id {
        let mut builder = UInt64Builder::with_capacity(batch.num_rows());
        for span in row_origins {
            for _ in 0..span.rows {
                match span.source_id {
                    Some(sid) => builder.append_value(sid.0),
                    None => builder.append_null(),
                }
            }
        }
        columns.push(MetadataColumn {
            name: field_names::SOURCE_ID,
            data_type: DataType::UInt64,
            array: Arc::new(builder.finish()) as ArrayRef,
        });
    }

    if let Some(source_path_column) = plan.source_path.to_column_name() {
        let array = source_path_metadata_array(batch.num_rows(), row_origins, source_paths)?;
        columns.push(MetadataColumn {
            name: source_path_column,
            data_type: DataType::Utf8View,
            array,
        });
    }

    replace_or_append_columns(batch, columns)
}

#[cfg(not(feature = "turmoil"))]
fn source_path_metadata_array(
    num_rows: usize,
    row_origins: &[RowOriginSpan],
    source_paths: &HashMap<SourceId, String>,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = StringViewBuilder::new();
    let mut blocks: HashMap<SourceId, (u32, u32)> = HashMap::new();
    for span in row_origins {
        let Some(source_id) = span.source_id else {
            append_null_metadata_views(&mut builder, span.rows);
            continue;
        };
        let Some(value) = source_paths.get(&source_id).map(String::as_str) else {
            append_null_metadata_views(&mut builder, span.rows);
            continue;
        };
        let (block, len) = if let Some(&(block, len)) = blocks.get(&source_id) {
            (block, len)
        } else {
            let len = u32::try_from(value.len()).map_err(|_e| {
                ArrowError::InvalidArgumentError(
                    "source metadata string is too large for Utf8View".to_string(),
                )
            })?;
            let block = builder.append_block(Buffer::from(value.as_bytes().to_vec()));
            blocks.insert(source_id, (block, len));
            (block, len)
        };
        append_block_metadata_views(&mut builder, block, len, span.rows)?;
    }
    finish_string_metadata_array(builder, num_rows)
}

#[cfg(not(feature = "turmoil"))]
fn append_block_metadata_views(
    builder: &mut StringViewBuilder,
    block: u32,
    len: u32,
    rows: usize,
) -> Result<(), ArrowError> {
    for _ in 0..rows {
        builder.try_append_view(block, 0, len)?;
    }
    Ok(())
}

#[cfg(not(feature = "turmoil"))]
fn append_null_metadata_views(builder: &mut StringViewBuilder, rows: usize) {
    for _ in 0..rows {
        builder.append_null();
    }
}

#[cfg(not(feature = "turmoil"))]
fn finish_string_metadata_array(
    mut builder: StringViewBuilder,
    num_rows: usize,
) -> Result<ArrayRef, ArrowError> {
    let array = builder.finish();
    if array.len() != num_rows {
        return Err(ArrowError::InvalidArgumentError(format!(
            "source metadata string column length mismatch: column={}, batch={num_rows}",
            array.len()
        )));
    }
    Ok(Arc::new(array) as ArrayRef)
}

#[cfg(not(feature = "turmoil"))]
struct MetadataColumn {
    name: &'static str,
    data_type: DataType,
    array: ArrayRef,
}

#[cfg(not(feature = "turmoil"))]
fn replace_or_append_columns(
    batch: RecordBatch,
    columns: Vec<MetadataColumn>,
) -> Result<RecordBatch, ArrowError> {
    if columns.is_empty() {
        return Ok(batch);
    }

    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len() + columns.len());
    let mut arrays = Vec::with_capacity(batch.num_columns() + columns.len());
    let mut replaced = HashSet::with_capacity(columns.len());

    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column) = columns
            .iter()
            .find(|column| field.name().as_str() == column.name)
        {
            fields.push(Field::new(column.name, column.data_type.clone(), true));
            arrays.push(Arc::clone(&column.array));
            replaced.insert(column.name);
        } else {
            fields.push((**field).clone());
            arrays.push(Arc::clone(batch.column(idx)));
        }
    }

    for column in columns {
        if !replaced.contains(column.name) {
            fields.push(Field::new(column.name, column.data_type, true));
            arrays.push(column.array);
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new_with_options(
        new_schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

/// Run the I/O worker loop for one input.
///
/// Polls the input source, accumulates bytes to `batch_target_bytes`,
/// and sends chunks to the CPU worker via `tx`.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
fn io_worker_loop(
    mut input: InputState,
    input_name: Arc<str>,
    tx: mpsc::Sender<IoWorkItem>,
    metrics: Arc<PipelineMetrics>,
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
        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            let mut shutdown_poll_rounds = 0usize;
            loop {
                match input.source.poll_shutdown() {
                    Ok(events) => {
                        let cadence = input.source.get_cadence();
                        let should_repoll = should_repoll_shutdown(
                            &events,
                            input.source.is_finished(),
                            cadence.signal.had_data,
                        );
                        if !process_io_events(
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
                }
            }
            break;
        }

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
        consecutive_poll_failures = 0;

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));
        let cadence = input.source.get_cadence();
        adaptive_poll.observe_signal(cadence.signal);

        if events.is_empty() {
            if adaptive_poll.should_fast_poll() {
                metrics.inc_cadence_fast_repoll();
            } else {
                metrics.inc_cadence_idle_sleep();
                std::thread::sleep(poll_interval);
            }
        } else if !process_io_events(
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

        if input.source.is_finished() {
            if !input.buf.is_empty() {
                metrics.inc_flush_by_timeout();
                if !flush_buf(
                    &mut input.buf,
                    &mut input.row_origins,
                    &mut pending_row_origin,
                    &mut input.source_paths,
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
        let checkpoints = input.source.checkpoint_data();
        let source_paths = if source_metadata_plan.has_source_path() {
            std::mem::take(&mut input.source_paths)
        } else {
            input.source_paths.clear();
            HashMap::new()
        };
        let chunk = IoWorkItem::Bytes(IoChunk {
            bytes: data,
            checkpoints,
            row_origins: std::mem::take(&mut input.row_origins),
            source_paths,
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

// ---------------------------------------------------------------------------
// CPU worker — scans bytes into RecordBatch, runs SQL transform
// ---------------------------------------------------------------------------

/// Run the CPU worker loop for one input.
///
/// Receives raw bytes from the I/O worker, scans them into Arrow RecordBatches,
/// runs the per-input SQL transform, and sends `ChannelMsg` to the
/// pipeline's main select loop.
///
/// Creates a lightweight tokio current-thread runtime for DataFusion SQL
/// execution (which is async internally). The runtime is created once and
/// reused for all batches.
#[cfg(not(feature = "turmoil"))]
fn cpu_worker_loop(
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

    while let Some(item) = rx.blocking_recv() {
        let (batch, checkpoints, queued_at, input_index, scan_ns) = match item {
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
        let result = match rt.block_on(transform.transform.execute(batch)) {
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

        let msg = super::ChannelMsg {
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
    }
}

// ---------------------------------------------------------------------------
// InputPipelineManager — spawns and joins I/O + CPU workers
// ---------------------------------------------------------------------------

/// Manages the I/O and CPU worker threads for all pipeline inputs.
///
/// Each input gets two OS threads connected by a bounded channel:
/// - I/O worker: polls source, accumulates bytes, sends `IoChunk`
/// - CPU worker: scans, SQL transforms, sends `ChannelMsg`
///
/// Shutdown cascade: shutdown token → I/O workers exit → drop io_tx →
/// CPU workers drain remaining chunks → CPU workers exit → drop pipeline_tx →
/// pipeline's rx.recv() returns None.
#[cfg(not(feature = "turmoil"))]
pub(crate) struct InputPipelineManager {
    io_handles: Vec<std::thread::JoinHandle<()>>,
    cpu_handles: Vec<std::thread::JoinHandle<()>>,
}

#[cfg(not(feature = "turmoil"))]
impl InputPipelineManager {
    /// Spawn I/O + CPU worker pairs for each input.
    ///
    /// Consumes `inputs` and `transforms` — they're moved to their
    /// respective worker threads.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn(
        inputs: Vec<InputState>,
        transforms: Vec<InputTransform>,
        pipeline_tx: mpsc::Sender<super::ChannelMsg>,
        metrics: Arc<PipelineMetrics>,
        shutdown: CancellationToken,
        batch_target_bytes: usize,
        batch_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        assert_eq!(
            inputs.len(),
            transforms.len(),
            "inputs ({}) and transforms ({}) must match",
            inputs.len(),
            transforms.len(),
        );

        let mut io_handles = Vec::with_capacity(inputs.len());
        let mut cpu_handles = Vec::with_capacity(inputs.len());

        for (idx, (input, transform)) in inputs.into_iter().zip(transforms).enumerate() {
            let (io_tx, io_rx) = mpsc::channel::<IoWorkItem>(IO_CPU_CHANNEL_CAPACITY);
            let source_metadata_plan = transform.source_metadata_plan;
            let input_name: Arc<str> = Arc::from(transform.input_name.as_str());

            // Spawn I/O worker.
            let shutdown_io = shutdown.clone();
            let metrics_io = Arc::clone(&metrics);
            io_handles.push(std::thread::spawn(move || {
                io_worker_loop(
                    input,
                    input_name,
                    io_tx,
                    metrics_io,
                    shutdown_io,
                    batch_target_bytes,
                    batch_timeout,
                    poll_interval,
                    idx,
                    source_metadata_plan,
                );
            }));

            // Spawn CPU worker.
            let pipeline_tx = pipeline_tx.clone();
            let metrics_cpu = Arc::clone(&metrics);
            cpu_handles.push(std::thread::spawn(move || {
                cpu_worker_loop(io_rx, pipeline_tx, transform, metrics_cpu);
            }));
        }

        Self {
            io_handles,
            cpu_handles,
        }
    }

    /// Join all worker threads. Call after the pipeline channel is fully drained.
    ///
    /// Joins CPU workers first (they exit when I/O workers drop their senders),
    /// then I/O workers.
    pub(super) fn join(self) {
        for h in self.cpu_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: cpu worker panicked");
            }
        }
        for h in self.io_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: io worker panicked");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, StringViewArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use logfwd_arrow::Scanner;
    use logfwd_types::source_metadata::{SourceMetadataPlan, SourcePathColumn};
    use proptest::prelude::*;

    // --- InputPipelineManager integration tests ---
    // These test the full split pipeline via from_config + run, so they
    // don't need direct access to private types.

    #[test]
    fn shutdown_repoll_continues_for_payload_without_matching_eof() {
        let events = vec![
            InputEvent::Data {
                bytes: b"a\n".to_vec(),
                source_id: Some(SourceId(1)),
                accounted_bytes: 2,
            },
            InputEvent::EndOfFile {
                source_id: Some(SourceId(2)),
            },
        ];

        assert!(should_repoll_shutdown(&events, false, false));
    }

    #[test]
    fn shutdown_repoll_stops_when_payload_source_reaches_eof() {
        let events = vec![
            InputEvent::Data {
                bytes: b"a\n".to_vec(),
                source_id: Some(SourceId(1)),
                accounted_bytes: 2,
            },
            InputEvent::EndOfFile {
                source_id: Some(SourceId(1)),
            },
        ];

        assert!(!should_repoll_shutdown(&events, false, false));
    }

    #[test]
    fn shutdown_repoll_stops_on_global_eof() {
        let events = vec![
            InputEvent::Data {
                bytes: b"a\n".to_vec(),
                source_id: Some(SourceId(1)),
                accounted_bytes: 2,
            },
            InputEvent::EndOfFile { source_id: None },
        ];

        assert!(!should_repoll_shutdown(&events, false, true));
    }

    #[test]
    fn shutdown_repoll_continues_for_empty_framed_output_after_source_payload() {
        assert!(should_repoll_shutdown(&[], false, true));
    }

    #[test]
    fn shutdown_repoll_continues_for_eof_only_output_after_source_payload() {
        let events = vec![InputEvent::EndOfFile {
            source_id: Some(SourceId(2)),
        }];

        assert!(should_repoll_shutdown(&events, false, true));
    }

    #[test]
    fn source_metadata_attach_adds_row_columns_after_scan() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .expect("batch");
        let input_name: Arc<str> = Arc::from("pods");
        let row_origins = vec![
            RowOriginSpan {
                source_id: Some(SourceId(10)),
                input_name: Arc::clone(&input_name),
                rows: 2,
            },
            RowOriginSpan {
                source_id: Some(SourceId(11)),
                input_name,
                rows: 1,
            },
        ];
        let source_paths = HashMap::from([
            (SourceId(10), "/var/log/pods/ns_pod_uid/c/0.log".to_string()),
            (SourceId(11), "/var/log/pods/ns_pod_uid/c/1.log".to_string()),
        ]);

        let out = source_metadata_for_batch(
            batch,
            &row_origins,
            &source_paths,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::Ecs,
            },
        )
        .expect("metadata attach should succeed");

        let source_id = out
            .column_by_name(field_names::SOURCE_ID)
            .expect("source id column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("source id type");
        assert_eq!(source_id.value(0), 10);
        assert_eq!(source_id.value(1), 10);
        assert_eq!(source_id.value(2), 11);

        let source_path = out
            .column_by_name(field_names::ECS_FILE_PATH)
            .expect("source path column")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("source path type");
        assert_eq!(source_path.value(0), "/var/log/pods/ns_pod_uid/c/0.log");
        assert_eq!(source_path.value(1), "/var/log/pods/ns_pod_uid/c/0.log");
        assert_eq!(source_path.value(2), "/var/log/pods/ns_pod_uid/c/1.log");
    }

    #[test]
    fn scanner_ready_row_count_skips_empty_lines_like_scanner() {
        assert_eq!(
            scanner_ready_row_count(b"\n\r\n{\"msg\":\"x\"}\nplain\nlast"),
            3
        );
        assert_eq!(scanner_ready_row_count(b"   \n\t\r\n"), 2);
    }

    #[test]
    fn scanner_ready_row_count_matches_scanner_for_crlf_blank_lines() {
        let mut scanner = Scanner::new(logfwd_core::scan_config::ScanConfig::default());
        let batch = scanner.scan(Bytes::from_static(b"\r\n")).expect("scan");
        assert_eq!(batch.num_rows(), scanner_ready_row_count(b"\r\n"));

        let mut config = logfwd_core::scan_config::ScanConfig::default();
        config.line_field_name = Some(field_names::BODY.to_string());
        let mut raw_scanner = Scanner::new(config);
        let raw_batch = raw_scanner
            .scan(Bytes::from_static(b"\r\n"))
            .expect("scan raw");
        assert_eq!(raw_batch.num_rows(), scanner_ready_row_count(b"\r\n"));
    }

    proptest! {
        #[test]
        fn scanner_ready_row_count_matches_scanner_for_generated_bytes(
            bytes in prop::collection::vec(any::<u8>(), 0..256),
            line_field in any::<bool>(),
        ) {
            let mut config = logfwd_core::scan_config::ScanConfig::default();
            if line_field {
                config.line_field_name = Some(field_names::BODY.to_string());
            }
            let mut scanner = Scanner::new(config);
            let batch = scanner.scan(Bytes::from(bytes.clone())).expect("scan");
            prop_assert_eq!(batch.num_rows(), scanner_ready_row_count(&bytes));
        }
    }

    #[test]
    fn row_origins_count_split_line_once() {
        let input_name: Arc<str> = Arc::from("file");
        let mut row_origins = Vec::new();
        let mut pending = None;
        let source_id = Some(SourceId(5));

        append_data_row_origins(
            &mut row_origins,
            &mut pending,
            source_id,
            &input_name,
            b"{\"msg\":\"hel",
        );
        assert!(row_origins.is_empty());

        append_data_row_origins(
            &mut row_origins,
            &mut pending,
            source_id,
            &input_name,
            b"lo\"}\n{\"msg\":\"next\"}\n",
        );
        finalize_pending_row_origin(&mut row_origins, &mut pending);

        let mut scanner = Scanner::new(logfwd_core::scan_config::ScanConfig::default());
        let batch = scanner
            .scan(Bytes::from_static(
                b"{\"msg\":\"hello\"}\n{\"msg\":\"next\"}\n",
            ))
            .expect("scan");
        let rows_from_origins = row_origins.iter().map(|span| span.rows).sum::<usize>();
        assert_eq!(rows_from_origins, batch.num_rows());
        assert_eq!(row_origins.len(), 1);
        assert_eq!(row_origins[0].source_id, source_id);
        assert_eq!(row_origins[0].rows, 2);
    }

    #[test]
    fn row_origins_finalize_partial_line_on_flush() {
        let input_name: Arc<str> = Arc::from("file");
        let mut row_origins = Vec::new();
        let mut pending = None;
        let source_id = Some(SourceId(9));

        append_data_row_origins(
            &mut row_origins,
            &mut pending,
            source_id,
            &input_name,
            b"{\"msg\":\"partial\"}",
        );
        finalize_pending_row_origin(&mut row_origins, &mut pending);

        let mut scanner = Scanner::new(logfwd_core::scan_config::ScanConfig::default());
        let batch = scanner
            .scan(Bytes::from_static(b"{\"msg\":\"partial\"}"))
            .expect("scan");
        assert_eq!(
            row_origins.iter().map(|span| span.rows).sum::<usize>(),
            batch.num_rows()
        );
        assert_eq!(row_origins[0].source_id, source_id);
    }

    #[test]
    fn source_paths_by_id_filters_to_buffered_source_ids() {
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(2)),
            input_name: Arc::from("file"),
            rows: 1,
        }];
        let paths = vec![
            (SourceId(1), PathBuf::from("/var/log/one.log")),
            (SourceId(2), PathBuf::from("/var/log/two.log")),
        ];

        let filtered = source_paths_by_id(&paths, &row_origins);

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered.get(&SourceId(2)).map(String::as_str),
            Some("/var/log/two.log")
        );
    }

    struct SingleDataSource {
        emitted: bool,
    }

    impl logfwd_io::input::InputSource for SingleDataSource {
        fn poll(&mut self) -> std::io::Result<Vec<InputEvent>> {
            if self.emitted {
                return Ok(Vec::new());
            }
            self.emitted = true;
            Ok(vec![InputEvent::Data {
                bytes: b"{\"msg\":\"x\"}\n".to_vec(),
                source_id: Some(SourceId(7)),
                accounted_bytes: 12,
            }])
        }

        fn name(&self) -> &'static str {
            "source-implementation-name"
        }

        fn health(&self) -> logfwd_types::diagnostics::ComponentHealth {
            logfwd_types::diagnostics::ComponentHealth::Healthy
        }

        fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
            vec![
                (SourceId(7), PathBuf::from("/var/log/configured.log")),
                (SourceId(8), PathBuf::from("/var/log/other.log")),
            ]
        }
    }

    struct SplitLineSource {
        emitted: bool,
    }

    impl logfwd_io::input::InputSource for SplitLineSource {
        fn poll(&mut self) -> std::io::Result<Vec<InputEvent>> {
            if self.emitted {
                return Ok(Vec::new());
            }
            self.emitted = true;
            Ok(vec![
                InputEvent::Data {
                    bytes: b"{\"msg\":\"hel".to_vec(),
                    source_id: Some(SourceId(7)),
                    accounted_bytes: 11,
                },
                InputEvent::Data {
                    bytes: b"lo\"}\n{\"msg\":\"next\"}\n".to_vec(),
                    source_id: Some(SourceId(7)),
                    accounted_bytes: 21,
                },
            ])
        }

        fn name(&self) -> &'static str {
            "split-line-source"
        }

        fn health(&self) -> logfwd_types::diagnostics::ComponentHealth {
            logfwd_types::diagnostics::ComponentHealth::Healthy
        }

        fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
            vec![(SourceId(7), PathBuf::from("/var/log/split.log"))]
        }
    }

    struct BatchSource {
        emitted: bool,
    }

    impl logfwd_io::input::InputSource for BatchSource {
        fn poll(&mut self) -> std::io::Result<Vec<InputEvent>> {
            if self.emitted {
                return Ok(Vec::new());
            }
            self.emitted = true;
            let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
                    .expect("batch");
            Ok(vec![InputEvent::Batch {
                batch,
                source_id: Some(SourceId(12)),
                accounted_bytes: 2,
            }])
        }

        fn name(&self) -> &'static str {
            "batch-source"
        }

        fn health(&self) -> logfwd_types::diagnostics::ComponentHealth {
            logfwd_types::diagnostics::ComponentHealth::Healthy
        }

        fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
            vec![(SourceId(12), PathBuf::from("/var/log/batch.log"))]
        }
    }

    struct ShutdownDrainSource {
        emitted: bool,
    }

    impl logfwd_io::input::InputSource for ShutdownDrainSource {
        fn poll(&mut self) -> std::io::Result<Vec<InputEvent>> {
            if self.emitted {
                return Ok(Vec::new());
            }
            self.emitted = true;
            Ok(vec![InputEvent::Data {
                bytes: b"{\"msg\":\"drain\"}\n".to_vec(),
                source_id: Some(SourceId(13)),
                accounted_bytes: 16,
            }])
        }

        fn name(&self) -> &'static str {
            "shutdown-drain-source"
        }

        fn health(&self) -> logfwd_types::diagnostics::ComponentHealth {
            logfwd_types::diagnostics::ComponentHealth::Healthy
        }

        fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
            vec![(SourceId(13), PathBuf::from("/var/log/drain.log"))]
        }
    }

    struct MultiShutdownPollSource {
        remaining: usize,
        emitted: usize,
        finished: bool,
    }

    impl logfwd_io::input::InputSource for MultiShutdownPollSource {
        fn poll(&mut self) -> std::io::Result<Vec<InputEvent>> {
            Ok(Vec::new())
        }

        fn poll_shutdown(&mut self) -> std::io::Result<Vec<InputEvent>> {
            if self.remaining == 0 {
                if self.finished {
                    return Ok(Vec::new());
                }
                self.finished = true;
                return Ok(vec![InputEvent::EndOfFile {
                    source_id: Some(SourceId(14)),
                }]);
            }

            self.emitted += 1;
            self.remaining -= 1;
            let bytes = format!("{{\"msg\":\"shutdown-{}\"}}\n", self.emitted).into_bytes();
            let accounted_bytes = bytes.len() as u64;
            Ok(vec![InputEvent::Data {
                bytes,
                source_id: Some(SourceId(14)),
                accounted_bytes,
            }])
        }

        fn name(&self) -> &'static str {
            "multi-shutdown-poll-source"
        }

        fn health(&self) -> logfwd_types::diagnostics::ComponentHealth {
            logfwd_types::diagnostics::ComponentHealth::Healthy
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
            vec![(SourceId(14), PathBuf::from("/var/log/shutdown.log"))]
        }
    }

    #[test]
    fn io_worker_uses_configured_input_name_for_source_metadata() {
        let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        let stats = Arc::new(logfwd_types::diagnostics::ComponentStats::new_with_health(
            logfwd_types::diagnostics::ComponentHealth::Starting,
        ));
        let input = InputState {
            source: Box::new(SingleDataSource { emitted: false }),
            buf: bytes::BytesMut::new(),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            stats,
        };
        let meter = opentelemetry::global::meter("io_worker_configured_input_name");
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let worker_shutdown = shutdown.clone();

        let handle = std::thread::spawn(move || {
            io_worker_loop(
                input,
                Arc::from("configured-input"),
                tx,
                metrics,
                worker_shutdown,
                1,
                Duration::from_secs(60),
                Duration::from_millis(1),
                0,
                SourceMetadataPlan {
                    has_source_id: true,
                    source_path: SourcePathColumn::Ecs,
                },
            );
        });

        let item = rx.blocking_recv().expect("io worker item");
        shutdown.cancel();
        drop(rx);
        handle.join().expect("io worker exits");

        let IoWorkItem::Bytes(chunk) = item else {
            panic!("expected byte chunk");
        };
        assert_eq!(chunk.row_origins.len(), 1);
        assert_eq!(chunk.row_origins[0].input_name.as_ref(), "configured-input");
        assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(7)));
        assert_eq!(chunk.row_origins[0].rows, 1);
        assert_eq!(
            chunk.source_paths.get(&SourceId(7)).map(String::as_str),
            Some("/var/log/configured.log")
        );
        assert!(!chunk.source_paths.contains_key(&SourceId(8)));
    }

    #[test]
    fn io_worker_counts_split_line_origin_once() {
        let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        let stats = Arc::new(logfwd_types::diagnostics::ComponentStats::new_with_health(
            logfwd_types::diagnostics::ComponentHealth::Starting,
        ));
        let input = InputState {
            source: Box::new(SplitLineSource { emitted: false }),
            buf: bytes::BytesMut::new(),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            stats,
        };
        let meter = opentelemetry::global::meter("io_worker_split_line_origin");
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let worker_shutdown = shutdown.clone();

        let handle = std::thread::spawn(move || {
            io_worker_loop(
                input,
                Arc::from("configured-input"),
                tx,
                metrics,
                worker_shutdown,
                usize::MAX,
                Duration::ZERO,
                Duration::from_millis(1),
                0,
                SourceMetadataPlan {
                    has_source_id: true,
                    source_path: SourcePathColumn::Ecs,
                },
            );
        });

        let item = rx.blocking_recv().expect("io worker item");
        shutdown.cancel();
        drop(rx);
        handle.join().expect("io worker exits");

        let IoWorkItem::Bytes(chunk) = item else {
            panic!("expected byte chunk");
        };
        assert_eq!(
            chunk.bytes.as_ref(),
            b"{\"msg\":\"hello\"}\n{\"msg\":\"next\"}\n"
        );
        assert_eq!(chunk.row_origins.len(), 1);
        assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(7)));
        assert_eq!(chunk.row_origins[0].rows, 2);
        assert_eq!(
            chunk.source_paths.get(&SourceId(7)).map(String::as_str),
            Some("/var/log/split.log")
        );
    }

    #[test]
    fn io_worker_attaches_source_metadata_to_batch_event() {
        let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        let stats = Arc::new(logfwd_types::diagnostics::ComponentStats::new_with_health(
            logfwd_types::diagnostics::ComponentHealth::Starting,
        ));
        let input = InputState {
            source: Box::new(BatchSource { emitted: false }),
            buf: bytes::BytesMut::new(),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            stats,
        };
        let meter = opentelemetry::global::meter("io_worker_batch_source_metadata");
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let worker_shutdown = shutdown.clone();

        let handle = std::thread::spawn(move || {
            io_worker_loop(
                input,
                Arc::from("configured-input"),
                tx,
                metrics,
                worker_shutdown,
                usize::MAX,
                Duration::from_secs(60),
                Duration::from_millis(1),
                0,
                SourceMetadataPlan {
                    has_source_id: true,
                    source_path: SourcePathColumn::Ecs,
                },
            );
        });

        let item = rx.blocking_recv().expect("io worker item");
        shutdown.cancel();
        drop(rx);
        handle.join().expect("io worker exits");

        let IoWorkItem::Batch {
            batch,
            row_origins,
            source_paths,
            ..
        } = item
        else {
            panic!("expected batch item");
        };
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(row_origins.len(), 1);
        assert_eq!(row_origins[0].source_id, Some(SourceId(12)));
        assert_eq!(row_origins[0].rows, 2);
        assert_eq!(
            source_paths.get(&SourceId(12)).map(String::as_str),
            Some("/var/log/batch.log")
        );
    }

    #[test]
    fn io_worker_shutdown_drains_buffered_source_metadata() {
        let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        let stats = Arc::new(logfwd_types::diagnostics::ComponentStats::new_with_health(
            logfwd_types::diagnostics::ComponentHealth::Starting,
        ));
        let input = InputState {
            source: Box::new(ShutdownDrainSource { emitted: false }),
            buf: bytes::BytesMut::new(),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            stats,
        };
        let meter = opentelemetry::global::meter("io_worker_shutdown_drain");
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let worker_shutdown = shutdown.clone();

        let handle = std::thread::spawn(move || {
            io_worker_loop(
                input,
                Arc::from("configured-input"),
                tx,
                metrics,
                worker_shutdown,
                usize::MAX,
                Duration::from_secs(60),
                Duration::from_millis(1),
                0,
                SourceMetadataPlan {
                    has_source_id: true,
                    source_path: SourcePathColumn::Ecs,
                },
            );
        });

        std::thread::sleep(Duration::from_millis(20));
        shutdown.cancel();
        let item = rx.blocking_recv().expect("shutdown drain item");
        drop(rx);
        handle.join().expect("io worker exits");

        let IoWorkItem::Bytes(chunk) = item else {
            panic!("expected byte chunk");
        };
        assert_eq!(chunk.bytes.as_ref(), b"{\"msg\":\"drain\"}\n");
        assert_eq!(chunk.row_origins.len(), 1);
        assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(13)));
        assert_eq!(chunk.row_origins[0].rows, 1);
        assert_eq!(
            chunk.source_paths.get(&SourceId(13)).map(String::as_str),
            Some("/var/log/drain.log")
        );
    }

    #[test]
    fn io_worker_shutdown_repolls_until_source_finishes() {
        let (tx, mut rx) = mpsc::channel(IO_CPU_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        let stats = Arc::new(logfwd_types::diagnostics::ComponentStats::new_with_health(
            logfwd_types::diagnostics::ComponentHealth::Starting,
        ));
        let input = InputState {
            source: Box::new(MultiShutdownPollSource {
                remaining: SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS + 2,
                emitted: 0,
                finished: false,
            }),
            buf: bytes::BytesMut::new(),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            stats,
        };
        let meter = opentelemetry::global::meter("io_worker_shutdown_repolls");
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let worker_shutdown = shutdown.clone();

        let handle = std::thread::spawn(move || {
            io_worker_loop(
                input,
                Arc::from("configured-input"),
                tx,
                metrics,
                worker_shutdown,
                usize::MAX,
                Duration::from_secs(60),
                Duration::from_millis(1),
                0,
                SourceMetadataPlan {
                    has_source_id: true,
                    source_path: SourcePathColumn::Ecs,
                },
            );
        });

        std::thread::sleep(Duration::from_millis(20));
        shutdown.cancel();
        let item = rx.blocking_recv().expect("shutdown drain item");
        drop(rx);
        handle.join().expect("io worker exits");

        let IoWorkItem::Bytes(chunk) = item else {
            panic!("expected byte chunk");
        };
        let expected = (1..=SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS + 2)
            .map(|index| format!("{{\"msg\":\"shutdown-{index}\"}}\n"))
            .collect::<String>();
        assert_eq!(chunk.bytes.as_ref(), expected.as_bytes());
        assert_eq!(chunk.row_origins.len(), 1);
        assert_eq!(chunk.row_origins[0].source_id, Some(SourceId(14)));
        assert_eq!(
            chunk.row_origins[0].rows,
            SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS + 2
        );
        assert_eq!(
            chunk.source_paths.get(&SourceId(14)).map(String::as_str),
            Some("/var/log/shutdown.log")
        );
    }

    #[test]
    fn source_metadata_attach_replaces_payload_reserved_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_names::ECS_FILE_PATH,
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["payload-value"]))],
        )
        .expect("batch");
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(5)),
            input_name: Arc::from("file"),
            rows: 1,
        }];
        let source_paths = HashMap::from([(SourceId(5), "/actual/path.log".to_string())]);

        let out = source_metadata_for_batch(
            batch,
            &row_origins,
            &source_paths,
            SourceMetadataPlan {
                source_path: SourcePathColumn::Ecs,
                ..SourceMetadataPlan::default()
            },
        )
        .expect("metadata attach should succeed");

        assert_eq!(out.num_columns(), 1);
        let source_path = out
            .column_by_name(field_names::ECS_FILE_PATH)
            .expect("source path column")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("source path type");
        assert_eq!(source_path.value(0), "/actual/path.log");
    }

    #[test]
    fn source_metadata_attach_rejects_row_count_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
            .expect("batch");
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(1)),
            input_name: Arc::from("file"),
            rows: 1,
        }];

        let err = source_metadata_for_batch(
            batch,
            &row_origins,
            &HashMap::new(),
            SourceMetadataPlan {
                has_source_id: true,
                ..SourceMetadataPlan::default()
            },
        )
        .expect_err("row count mismatch should fail");
        assert!(err.to_string().contains("row count mismatch"));
    }

    #[test]
    fn public_source_path_attached_before_sql_is_queryable() {
        let mut transform =
            crate::transform::SqlTransform::new(r#"SELECT "file.path", msg FROM logs"#)
                .expect("sql");
        let mut scanner = Scanner::new(transform.scan_config());
        let scanned = scanner
            .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
            .expect("scan");
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(99)),
            input_name: Arc::from("pods"),
            rows: 1,
        }];
        let source_paths =
            HashMap::from([(SourceId(99), "/var/log/pods/ns_pod_uid/c/0.log".to_string())]);
        let attached = source_metadata_for_batch(
            scanned,
            &row_origins,
            &source_paths,
            SourceMetadataPlan {
                has_source_id: false,
                source_path: SourcePathColumn::Ecs,
            },
        )
        .expect("attach");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt
            .block_on(transform.execute(attached))
            .expect("transform should see source path");
        let source_path = result
            .column_by_name(field_names::ECS_FILE_PATH)
            .expect("source path column")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("source path type");
        assert_eq!(source_path.value(0), "/var/log/pods/ns_pod_uid/c/0.log");
    }

    #[test]
    fn select_star_includes_public_source_metadata_style_columns() {
        let mut transform = crate::transform::SqlTransform::new(
            r#"SELECT * FROM logs WHERE "file.path" = '/var/log/pods/ns_pod_uid/c/0.log'"#,
        )
        .expect("sql");
        let mut scanner = Scanner::new(transform.scan_config());
        let scanned = scanner
            .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
            .expect("scan");
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(99)),
            input_name: Arc::from("pods"),
            rows: 1,
        }];
        let source_paths =
            HashMap::from([(SourceId(99), "/var/log/pods/ns_pod_uid/c/0.log".to_string())]);
        let attached = source_metadata_for_batch(
            scanned,
            &row_origins,
            &source_paths,
            SourceMetadataPlan {
                has_source_id: false,
                source_path: SourcePathColumn::Ecs,
            },
        )
        .expect("attach");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt
            .block_on(transform.execute(attached))
            .expect("transform should filter on source metadata");

        assert_eq!(result.num_rows(), 1);
        assert!(result.column_by_name(field_names::ECS_FILE_PATH).is_some());
        assert!(result.column_by_name("msg").is_some());
    }

    #[test]
    fn select_star_does_not_attach_source_metadata() {
        let mut transform = crate::transform::SqlTransform::new("SELECT * FROM logs").expect("sql");
        let mut scanner = Scanner::new(transform.scan_config());
        let scanned = scanner
            .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
            .expect("scan");
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(99)),
            input_name: Arc::from("pods"),
            rows: 1,
        }];
        let source_paths =
            HashMap::from([(SourceId(99), "/var/log/pods/ns_pod_uid/c/0.log".to_string())]);
        let attached = source_metadata_for_batch(
            scanned,
            &row_origins,
            &source_paths,
            transform.analyzer().source_metadata_plan(),
        )
        .expect("attach");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt
            .block_on(transform.execute(attached))
            .expect("transform should keep wildcard projection narrow");

        assert!(result.column_by_name(field_names::SOURCE_ID).is_none());
    }

    #[test]
    fn explicit_projection_preserves_new_source_metadata_columns() {
        let mut transform =
            crate::transform::SqlTransform::new("SELECT msg, __source_id FROM logs").expect("sql");
        let mut scanner = Scanner::new(transform.scan_config());
        let scanned = scanner
            .scan(Bytes::from_static(b"{\"msg\":\"hello\"}\n"))
            .expect("scan");
        let row_origins = vec![RowOriginSpan {
            source_id: Some(SourceId(99)),
            input_name: Arc::from("pods"),
            rows: 1,
        }];
        let attached = source_metadata_for_batch(
            scanned,
            &row_origins,
            &HashMap::new(),
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::None,
            },
        )
        .expect("attach");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt
            .block_on(transform.execute(attached))
            .expect("transform should preserve explicit source metadata");

        let source_id = result
            .column_by_name(field_names::SOURCE_ID)
            .expect("source id column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("source id type");
        assert_eq!(source_id.value(0), 99);
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn manager_spawns_and_joins_with_empty_inputs() {
        // Verify InputPipelineManager handles zero inputs gracefully.
        // We test this through the pipeline's run path with a generator
        // that immediately shuts down.
        let yaml = r"
input:
  type: generator
output:
  type: 'null'
";
        let config = logfwd_config::Config::load_str(yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should shut down cleanly: {result:?}"
        );
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_processes_file_with_sql_filter() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("split_test.log");

        // Write lines: half INFO, half DEBUG.
        let mut data = String::new();
        for i in 0..20 {
            let level = if i % 2 == 0 { "INFO" } else { "DEBUG" };
            data.push_str(&format!(r#"{{"level":"{}","seq":{}}}"#, level, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level = 'INFO'"
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = pipeline.metrics().clone();

        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                // Wait until transform_in shows all rows were scanned.
                if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 20 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "split pipeline with SQL should work: {result:?}"
        );

        // CPU worker scans 20 lines.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(lines_in, 20, "expected 20 lines scanned, got {lines_in}");

        // SQL WHERE filters half → 10 rows reach output.
        let lines_out = pipeline
            .metrics
            .transform_out
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_out, 10,
            "expected 10 lines after SQL filter, got {lines_out}"
        );
    }

    /// Two file inputs processed simultaneously through InputPipelineManager.
    /// Verifies both inputs' data reaches the output.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_two_inputs_both_processed() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log1 = dir.path().join("input1.log");
        let log2 = dir.path().join("input2.log");

        // 10 lines each, different content.
        let mut data1 = String::new();
        let mut data2 = String::new();
        for i in 0..10 {
            data1.push_str(&format!(r#"{{"src":"a","seq":{}}}"#, i));
            data1.push('\n');
            data2.push_str(&format!(r#"{{"src":"b","seq":{}}}"#, i));
            data2.push('\n');
        }
        std::fs::write(&log1, data1.as_bytes()).unwrap();
        std::fs::write(&log2, data2.as_bytes()).unwrap();

        let yaml = format!(
            r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
      - type: file
        path: {}
        format: json
    outputs:
      - type: 'null'
"#,
            log1.display(),
            log2.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = pipeline.metrics().clone();

        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                // Wait until both files' data is scanned (20 lines total).
                if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 20 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "two-input pipeline should work: {result:?}");

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_in, 20,
            "expected 20 lines from both inputs, got {lines_in}"
        );
    }

    /// End-to-end regression: file-tail read-budget signals should propagate
    /// through framed input into the runtime adaptive cadence loop.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_records_adaptive_fast_repolls() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("adaptive_fast_repolls.log");

        let payload = "x".repeat(160);
        let mut data = String::new();
        for i in 0..40 {
            data.push_str(&format!(
                r#"{{"level":"INFO","seq":{},"msg":"{}"}}"#,
                i, payload
            ));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
  poll_interval_ms: 200
  per_file_read_budget_bytes: 64
  adaptive_fast_polls_max: 4
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = pipeline.metrics().clone();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            loop {
                let lines_in = metrics.transform_in.lines_total.load(Ordering::Relaxed);
                let fast_repolls = metrics.cadence_fast_repolls.load(Ordering::Relaxed);
                if lines_in >= 40 && fast_repolls > 0 {
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "adaptive cadence pipeline should run cleanly: {result:?}"
        );

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 40,
            "expected to process test file rows, got {lines_in}"
        );

        let fast_repolls = pipeline
            .metrics
            .cadence_fast_repolls
            .load(Ordering::Relaxed);
        assert!(
            fast_repolls > 0,
            "expected adaptive cadence fast repolls to be recorded, got {fast_repolls}"
        );
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_shutdown_flushes_file_remainder_before_idle_eof() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("shutdown_partial.log");
        std::fs::write(&log_path, br#"{"level":"INFO","seq":775}"#).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
  poll_interval_ms: 60000
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_secs(60));

        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "shutdown flush pipeline should run cleanly: {result:?}"
        );

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_in, 1,
            "shutdown must flush final no-newline file record before normal idle EOF"
        );
    }

    /// Transform error: CPU worker drops the batch and does NOT advance
    /// the checkpoint (correct at-least-once semantics — data will be
    /// re-read on restart).
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_transform_error_drops_batch() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("transform_err.log");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        // SQL references a column that doesn't exist → DataFusion error.
        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT nonexistent_col FROM logs"
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should survive transform errors: {result:?}"
        );

        let errors = pipeline.metrics.transform_errors.load(Ordering::Relaxed);
        assert!(
            errors > 0,
            "CPU worker should report transform errors, got {errors}"
        );

        // No data reaches the pipeline — checkpoints NOT advanced
        // (at-least-once: data will be re-read on restart).
        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert!(
            dropped > 0,
            "failed batches should be counted as dropped, got {dropped}"
        );
    }

    /// Processor that returns a Transient error — tickets should be held
    /// (checkpoint does NOT advance), and the pipeline continues.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_processor_transient_error_continues() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("proc_err.log");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        // Processor that always returns a transient error.
        #[derive(Debug)]
        struct FailingProcessor;
        impl crate::processor::Processor for FailingProcessor {
            fn process(
                &mut self,
                _batch: RecordBatch,
                _meta: &logfwd_output::BatchMetadata,
            ) -> Result<smallvec::SmallVec<[RecordBatch; 1]>, crate::processor::ProcessorError>
            {
                Err(crate::processor::ProcessorError::Transient(
                    "test transient error".to_string(),
                ))
            }

            fn flush(&mut self) -> smallvec::SmallVec<[RecordBatch; 1]> {
                smallvec::SmallVec::new()
            }

            fn name(&self) -> &'static str {
                "failing"
            }

            fn is_stateful(&self) -> bool {
                false
            }
        }

        pipeline = pipeline.with_processor(Box::new(FailingProcessor));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should survive processor errors: {result:?}"
        );

        // Data was scanned (transform_in > 0) but rejected by processor
        // so nothing reached output (transform_out == 0).
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(lines_in > 0, "data should be scanned, got {lines_in}");

        let lines_out = pipeline
            .metrics
            .transform_out
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_out, 0,
            "no data should pass through failing processor, got {lines_out}"
        );
    }
}
