//! Composable framing wrapper for input sources.
//!
//! `FramedInput` wraps any [`InputSource`] and handles newline framing
//! (remainder management across polls) and format processing (CRI, Auto,
//! passthrough). The pipeline receives scanner-ready bytes without knowing
//! about formats or line boundaries.
//!
//! Remainder buffers and format state are tracked per-source so that
//! interleaved data from multiple files (or TCP connections) never
//! cross-contaminates partial lines or CRI P/F aggregation state.

use bytes::{Bytes, BytesMut};

use crate::filter_hints::FilterHints;
use crate::format::FormatDecoder;
use crate::input::{CriMetadata, FramedReadEvent, InputCadence, InputSource, SourceEvent};
#[cfg(test)]
use crate::poll_cadence::PollCadenceSignal;
use crate::tail::ByteOffset;
use ffwd_core::checkpoint_tracker::CheckpointTracker;
use ffwd_types::diagnostics::{ComponentHealth, ComponentStats};
use ffwd_types::pipeline::SourceId;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Maximum remainder buffer size before discarding (prevents OOM on
/// input without newlines). Applied per source.
const MAX_REMAINDER_BYTES: usize = 2 * 1024 * 1024;

const INITIAL_CRI_METADATA_SPANS: usize = 16;
const INITIAL_CRI_TIMESTAMP_BYTES: usize = 1024;

/// Per-source state for framing and checkpoint tracking.
///
/// Each logical source (identified by `Option<SourceId>`) gets its own
/// remainder buffer, format processor, and checkpoint tracker. This
/// prevents cross-contamination between sources for both newline framing
/// and stateful format processing (e.g., CRI P/F aggregation).
struct SourceState {
    /// Partial-line bytes after the last newline.
    remainder: Vec<u8>,
    /// Per-source format processor (CRI aggregator state is source-scoped).
    format: FormatDecoder,
    /// Kani-proven checkpoint offset tracker. Tracks the relationship
    /// between file read position and the last complete newline boundary.
    tracker: CheckpointTracker,
    /// True when remainder bytes are known to start mid-line due to overflow
    /// truncation. The first completed line formed from this remainder must be
    /// discarded to avoid emitting a garbled fragment (#1030).
    overflow_tainted: bool,
}

impl SourceState {
    fn is_reclaimable(&self) -> bool {
        self.remainder.is_empty() && !self.overflow_tainted && !self.format.has_pending_state()
    }
}

/// Wraps an [`InputSource`] with newline framing and format processing.
///
/// The inner source provides raw bytes (from file, TCP, UDP, etc.). This
/// wrapper splits on newlines, manages partial-line remainders across polls,
/// and runs format-specific processing (CRI extraction, passthrough, etc.).
/// The output is scanner-ready bytes. Structured batch events are forwarded by
/// the allocation-owning [`InputSource::poll`] path; the shared-buffer
/// [`InputSource::poll_into`] path remains byte-only.
///
/// All per-source state (remainder, format, checkpoint tracker) is keyed by
/// `Option<SourceId>` so that interleaved data from multiple sources never
/// mixes partial lines or CRI aggregation state. Sources without identity
/// (`None`) share a single state entry.
pub struct FramedInput {
    inner: Box<dyn InputSource>,
    /// Template format processor — cloned per-source on first data arrival.
    format_template: FormatDecoder,
    /// Per-source state: remainder, format processor, checkpoint tracker.
    sources: HashMap<Option<SourceId>, SourceState>,
    out_buf: Vec<u8>,
    cri_metadata_buf: CriMetadata,
    /// Spare buffer swapped in when out_buf is emitted, preserving capacity
    /// across polls without allocating.
    spare_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
    last_raw_had_payload: bool,
}

impl FramedInput {
    pub fn new(
        inner: Box<dyn InputSource>,
        format: FormatDecoder,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let cri_metadata_buf = if format.emits_cri_metadata() {
            CriMetadata::with_capacity(INITIAL_CRI_METADATA_SPANS, INITIAL_CRI_TIMESTAMP_BYTES)
        } else {
            CriMetadata::default()
        };
        Self {
            inner,
            format_template: format,
            sources: HashMap::new(),
            out_buf: Vec::with_capacity(64 * 1024),
            cri_metadata_buf,
            spare_buf: Vec::with_capacity(64 * 1024),
            stats,
            last_raw_had_payload: false,
        }
    }

    fn cri_metadata_for_emitted_data(&mut self) -> Option<CriMetadata> {
        if self.cri_metadata_buf.is_empty() {
            return None;
        }
        let replacement = self.cri_metadata_buf.empty_with_preserved_capacity();
        let metadata = std::mem::replace(&mut self.cri_metadata_buf, replacement);
        Some(metadata)
    }

    fn append_buffered_data(
        dst: &mut BytesMut,
        data: &[u8],
        source_id: Option<SourceId>,
        result_events: &mut Vec<FramedReadEvent>,
    ) {
        if data.is_empty() {
            return;
        }
        let start = dst.len();
        dst.extend_from_slice(data);
        result_events.push(FramedReadEvent::Data {
            range: start..dst.len(),
            source_id,
            cri_metadata: None,
        });
    }

    fn unsupported_structured_event_error(event_name: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("FramedInput only supports raw byte events; inner source emitted {event_name}"),
        )
    }

    fn process_raw_events_into(
        &mut self,
        raw_events: Vec<SourceEvent>,
        dst: &mut BytesMut,
    ) -> io::Result<Vec<FramedReadEvent>> {
        self.last_raw_had_payload = raw_events
            .iter()
            .any(|event| matches!(event, SourceEvent::Data { .. } | SourceEvent::Batch { .. }));
        if raw_events.is_empty() {
            return Ok(vec![]);
        }

        let mut result_events: Vec<FramedReadEvent> = Vec::new();

        for event in raw_events {
            match event {
                SourceEvent::Data {
                    bytes,
                    source_id,
                    accounted_bytes,
                    ..
                } => {
                    self.stats.inc_bytes(accounted_bytes);

                    let key = source_id;
                    let n_bytes = bytes.len() as u64;
                    let state = {
                        let template = &self.format_template;
                        self.sources.entry(key).or_insert_with(|| SourceState {
                            remainder: Vec::new(),
                            format: template.new_instance(),
                            tracker: CheckpointTracker::new(0),
                            overflow_tainted: false,
                        })
                    };
                    let tainted_on_entry = state.overflow_tainted;

                    if state.format.is_passthrough()
                        && state.remainder.is_empty()
                        && !tainted_on_entry
                    {
                        let last_nl = memchr::memrchr(b'\n', &bytes);
                        match last_nl {
                            Some(pos) if pos + 1 == bytes.len() => {
                                state.tracker.apply_read(n_bytes, Some(pos as u64));
                                let line_count = memchr::memchr_iter(b'\n', &bytes).count();
                                self.stats.inc_lines(line_count as u64);
                                if let FormatDecoder::PassthroughJson { stats, .. } = &state.format
                                {
                                    crate::format::count_json_parse_errors(&bytes, stats);
                                }
                                Self::append_buffered_data(
                                    dst,
                                    &bytes,
                                    source_id,
                                    &mut result_events,
                                );
                                if key.is_some()
                                    && self.inner.should_reclaim_completed_source_state()
                                    && self
                                        .sources
                                        .get(&key)
                                        .is_some_and(SourceState::is_reclaimable)
                                {
                                    self.sources.remove(&key);
                                }
                                continue;
                            }
                            Some(pos) => {
                                let complete = bytes.slice(0..=pos);
                                state.tracker.apply_read(n_bytes, Some(pos as u64));
                                let tail = &bytes[pos + 1..];
                                if tail.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        tail_bytes = tail.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — tail after newline \
                                         exceeds MAX_REMAINDER_BYTES; keeping last \
                                         MAX_REMAINDER_BYTES bytes"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let start = tail.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = tail[start..].to_vec();
                                    state.format.reset();
                                    state.overflow_tainted = true;
                                } else {
                                    state.remainder = tail.to_vec();
                                }
                                let line_count = memchr::memchr_iter(b'\n', &complete).count();
                                self.stats.inc_lines(line_count as u64);
                                if let FormatDecoder::PassthroughJson { stats, .. } = &state.format
                                {
                                    crate::format::count_json_parse_errors(&complete, stats);
                                }
                                Self::append_buffered_data(
                                    dst,
                                    &complete,
                                    source_id,
                                    &mut result_events,
                                );
                                continue;
                            }
                            None => {
                                state.tracker.apply_read(n_bytes, None);
                                if bytes.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        chunk_bytes = bytes.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — partial line exceeds \
                                         MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                         bytes and resetting format state"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let start = bytes.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = bytes[start..].to_vec();
                                    state.format.reset();
                                    state.overflow_tainted = true;
                                } else {
                                    state.remainder = bytes.to_vec();
                                }
                                continue;
                            }
                        }
                    }

                    let mut chunk = std::mem::take(&mut state.remainder);
                    chunk.extend_from_slice(&bytes);
                    let last_newline_pos = memchr::memrchr(b'\n', &chunk);
                    let remainder_prefix_len = chunk.len() - bytes.len();
                    let last_newline_in_new_bytes = last_newline_pos.and_then(|pos| {
                        (pos >= remainder_prefix_len).then(|| (pos - remainder_prefix_len) as u64)
                    });
                    state.tracker.apply_read(n_bytes, last_newline_in_new_bytes);

                    match last_newline_pos {
                        Some(pos) => {
                            if pos + 1 < chunk.len() {
                                let mut tail = chunk.split_off(pos + 1);
                                if tail.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        tail_bytes = tail.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — partial line exceeds \
                                         MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                         bytes and resetting format state"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let state = self.sources.get_mut(&key).expect("just inserted");
                                    state.format.reset();
                                    let start = tail.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = tail.split_off(start);
                                    state.overflow_tainted = true;
                                } else {
                                    let state = self.sources.get_mut(&key).expect("just inserted");
                                    state.remainder = tail;
                                }
                                chunk.truncate(pos + 1);
                            }
                        }
                        None => {
                            if chunk.len() > MAX_REMAINDER_BYTES {
                                tracing::warn!(
                                    source_key = ?key,
                                    chunk_bytes = chunk.len(),
                                    max_remainder_bytes = MAX_REMAINDER_BYTES,
                                    "framed.remainder_overflow — partial line exceeds \
                                     MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                     bytes and resetting format state"
                                );
                                self.stats.inc_parse_errors(1);
                                let state = self.sources.get_mut(&key).expect("just inserted");
                                state.format.reset();
                                let start = chunk.len() - MAX_REMAINDER_BYTES;
                                state.remainder = chunk.split_off(start);
                                state.overflow_tainted = true;
                            } else {
                                let state = self.sources.get_mut(&key).expect("just inserted");
                                state.remainder = chunk;
                            }
                            continue;
                        }
                    }

                    let state = self.sources.get_mut(&key).expect("just inserted");
                    let mut process_start = 0usize;
                    if tainted_on_entry && let Some(first_newline) = memchr::memchr(b'\n', &chunk) {
                        process_start = first_newline + 1;
                        state.overflow_tainted = false;
                    }
                    if process_start < chunk.len() {
                        let chunk = &chunk[process_start..];
                        let line_count = memchr::memchr_iter(b'\n', chunk).count();
                        self.stats.inc_lines(line_count as u64);
                        let start = dst.len();
                        self.cri_metadata_buf.clear();
                        state.format.process_lines_with_metadata_into(
                            chunk,
                            dst,
                            Some(&mut self.cri_metadata_buf),
                        );
                        if dst.len() > start {
                            result_events.push(FramedReadEvent::Data {
                                range: start..dst.len(),
                                source_id,
                                cri_metadata: self.cri_metadata_for_emitted_data(),
                            });
                        }
                    }

                    if key.is_some()
                        && self.inner.should_reclaim_completed_source_state()
                        && self
                            .sources
                            .get(&key)
                            .is_some_and(SourceState::is_reclaimable)
                    {
                        self.sources.remove(&key);
                    }
                }
                SourceEvent::Batch { .. } => {
                    return Err(Self::unsupported_structured_event_error(
                        "SourceEvent::Batch",
                    ));
                }
                SourceEvent::Rotated { source_id } => {
                    match source_id {
                        Some(_) => {
                            self.sources.remove(&source_id);
                        }
                        None => {
                            self.sources.clear();
                        }
                    }
                    result_events.push(FramedReadEvent::Rotated { source_id });
                }
                SourceEvent::Truncated { source_id } => {
                    match source_id {
                        Some(_) => {
                            self.sources.remove(&source_id);
                        }
                        None => {
                            self.sources.clear();
                        }
                    }
                    result_events.push(FramedReadEvent::Truncated { source_id });
                }
                SourceEvent::EndOfFile { source_id } => {
                    let keys_to_flush: Vec<Option<SourceId>> = match source_id {
                        Some(_) => vec![source_id],
                        None => self.sources.keys().copied().collect(),
                    };
                    for key in keys_to_flush {
                        if let Some(state) = self.sources.get_mut(&key)
                            && !state.remainder.is_empty()
                        {
                            let mut remainder = std::mem::take(&mut state.remainder);
                            remainder.push(b'\n');
                            let mut process_start = 0usize;
                            if state.overflow_tainted {
                                process_start =
                                    memchr::memchr(b'\n', &remainder).map_or(0, |i| i + 1);
                                state.overflow_tainted = false;
                            }
                            self.cri_metadata_buf.clear();
                            let state = self.sources.get_mut(&key).expect("just checked existence");
                            if process_start < remainder.len() {
                                let emitted = &remainder[process_start..];
                                let line_count = memchr::memchr_iter(b'\n', emitted).count();
                                self.stats.inc_lines(line_count as u64);
                                let start = dst.len();
                                state.format.process_lines_with_metadata_into(
                                    emitted,
                                    dst,
                                    Some(&mut self.cri_metadata_buf),
                                );
                                if dst.len() > start {
                                    result_events.push(FramedReadEvent::Data {
                                        range: start..dst.len(),
                                        source_id: key,
                                        cri_metadata: self.cri_metadata_for_emitted_data(),
                                    });
                                }
                            }
                            let state = self.sources.get_mut(&key).expect("just checked existence");
                            state.tracker.apply_remainder_consumed();
                        }
                        if self
                            .sources
                            .get(&key)
                            .is_some_and(SourceState::is_reclaimable)
                        {
                            self.sources.remove(&key);
                        }
                    }
                }
            }
        }

        Ok(result_events)
    }

    fn process_raw_events(&mut self, raw_events: Vec<SourceEvent>) -> Vec<SourceEvent> {
        self.last_raw_had_payload = raw_events
            .iter()
            .any(|event| matches!(event, SourceEvent::Data { .. } | SourceEvent::Batch { .. }));
        if raw_events.is_empty() {
            return vec![];
        }

        let mut result_events: Vec<SourceEvent> = Vec::new();

        for event in raw_events {
            match event {
                SourceEvent::Data {
                    bytes,
                    source_id,
                    accounted_bytes,
                    ..
                } => {
                    self.stats.inc_bytes(accounted_bytes);

                    let key = source_id;
                    let n_bytes = bytes.len() as u64;

                    // Get or create per-source state.
                    let state = {
                        let template = &self.format_template;
                        self.sources.entry(key).or_insert_with(|| SourceState {
                            remainder: Vec::new(),
                            format: template.new_instance(),
                            tracker: CheckpointTracker::new(0),
                            overflow_tainted: false,
                        })
                    };
                    let tainted_on_entry = state.overflow_tainted;

                    // --- ZERO-COPY FAST PATH ---
                    // For passthrough formats with no remainder and no tainted
                    // state, forward the Bytes directly without copying.
                    if state.format.is_passthrough()
                        && state.remainder.is_empty()
                        && !tainted_on_entry
                    {
                        let last_nl = memchr::memrchr(b'\n', &bytes);
                        match last_nl {
                            Some(pos) if pos + 1 == bytes.len() => {
                                // Entire chunk is complete lines — zero-copy pass.
                                state.tracker.apply_read(n_bytes, Some(pos as u64));
                                let line_count = memchr::memchr_iter(b'\n', &bytes).count();
                                self.stats.inc_lines(line_count as u64);
                                if let FormatDecoder::PassthroughJson { stats, .. } = &state.format
                                {
                                    crate::format::count_json_parse_errors(&bytes, stats);
                                }
                                result_events.push(SourceEvent::Data {
                                    bytes,
                                    source_id,
                                    accounted_bytes: 0,
                                    cri_metadata: None,
                                });
                                if key.is_some()
                                    && self.inner.should_reclaim_completed_source_state()
                                    && self
                                        .sources
                                        .get(&key)
                                        .is_some_and(SourceState::is_reclaimable)
                                {
                                    self.sources.remove(&key);
                                }
                                continue;
                            }
                            Some(pos) => {
                                // Complete lines + remainder tail.
                                let complete = bytes.slice(0..=pos);
                                state.tracker.apply_read(n_bytes, Some(pos as u64));
                                let tail = &bytes[pos + 1..];
                                if tail.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        tail_bytes = tail.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — tail after newline \
                                         exceeds MAX_REMAINDER_BYTES; keeping last \
                                         MAX_REMAINDER_BYTES bytes"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let start = tail.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = tail[start..].to_vec();
                                    state.format.reset();
                                    state.overflow_tainted = true;
                                } else {
                                    state.remainder = tail.to_vec();
                                }
                                let line_count = memchr::memchr_iter(b'\n', &complete).count();
                                self.stats.inc_lines(line_count as u64);
                                if let FormatDecoder::PassthroughJson { stats, .. } = &state.format
                                {
                                    crate::format::count_json_parse_errors(&complete, stats);
                                }
                                result_events.push(SourceEvent::Data {
                                    bytes: complete,
                                    source_id,
                                    accounted_bytes: 0,
                                    cri_metadata: None,
                                });
                                continue;
                            }
                            None => {
                                // No newline — entire chunk is remainder.
                                state.tracker.apply_read(n_bytes, None);
                                if bytes.len() > MAX_REMAINDER_BYTES {
                                    tracing::warn!(
                                        source_key = ?key,
                                        chunk_bytes = bytes.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — partial line exceeds \
                                         MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                         bytes and resetting format state"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let start = bytes.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = bytes[start..].to_vec();
                                    state.format.reset();
                                    state.overflow_tainted = true;
                                } else {
                                    state.remainder = bytes.to_vec();
                                }
                                continue;
                            }
                        }
                    }
                    // --- END ZERO-COPY FAST PATH ---

                    // General path: prepend remainder from last poll,
                    // reusing the Vec's capacity.
                    let mut chunk = std::mem::take(&mut state.remainder);
                    chunk.extend_from_slice(&bytes);

                    // Find last newline — everything before is complete lines,
                    // everything after is the new remainder.
                    let last_newline_pos = memchr::memrchr(b'\n', &chunk);

                    // Compute the last newline position relative to the NEW
                    // bytes only (for the checkpoint tracker). The tracker
                    // only sees bytes read from the file, not the remainder
                    // prefix.
                    let remainder_prefix_len = chunk.len() - bytes.len();
                    // If the last newline is inside the old remainder (not
                    // the new bytes), the tracker sees no newline in the read.
                    let last_newline_in_new_bytes = last_newline_pos.and_then(|pos| {
                        (pos >= remainder_prefix_len).then(|| (pos - remainder_prefix_len) as u64)
                    });

                    // Update checkpoint tracker with the new read.
                    state.tracker.apply_read(n_bytes, last_newline_in_new_bytes);

                    match last_newline_pos {
                        Some(pos) => {
                            if pos + 1 < chunk.len() {
                                // Move tail to remainder without allocating.
                                let mut tail = chunk.split_off(pos + 1);
                                if tail.len() > MAX_REMAINDER_BYTES {
                                    // Tail exceeds the per-source cap. Discard the
                                    // oldest bytes and keep the most recent
                                    // MAX_REMAINDER_BYTES so the source can
                                    // eventually emit a complete line. Emit a warning
                                    // so the data loss is not silent.
                                    tracing::warn!(
                                        source_key = ?key,
                                        tail_bytes = tail.len(),
                                        max_remainder_bytes = MAX_REMAINDER_BYTES,
                                        "framed.remainder_overflow — partial line exceeds \
                                         MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                         bytes and resetting format state"
                                    );
                                    self.stats.inc_parse_errors(1);
                                    let state = self.sources.get_mut(&key).expect("just inserted");
                                    // Reset format so the next line starts from a clean
                                    // state (the discarded prefix may have broken CRI P/F
                                    // sequence alignment).
                                    state.format.reset();
                                    // Keep the tail of the overflow data in the remainder
                                    // buffer so the next newline can complete it. Do NOT
                                    // call apply_remainder_consumed() — the data is still
                                    // pending and the checkpoint must not advance past it.
                                    let start = tail.len() - MAX_REMAINDER_BYTES;
                                    state.remainder = tail.split_off(start);
                                    state.overflow_tainted = true;
                                } else {
                                    let state = self.sources.get_mut(&key).expect("just inserted");
                                    state.remainder = tail;
                                }
                                chunk.truncate(pos + 1);
                            }
                        }
                        None => {
                            // No newline at all — entire chunk is remainder.
                            if chunk.len() > MAX_REMAINDER_BYTES {
                                // Same overflow policy as the tail case: warn, reset
                                // format state, and keep the most recent bytes.
                                tracing::warn!(
                                    source_key = ?key,
                                    chunk_bytes = chunk.len(),
                                    max_remainder_bytes = MAX_REMAINDER_BYTES,
                                    "framed.remainder_overflow — partial line exceeds \
                                     MAX_REMAINDER_BYTES; keeping last MAX_REMAINDER_BYTES \
                                     bytes and resetting format state"
                                );
                                self.stats.inc_parse_errors(1);
                                let state = self.sources.get_mut(&key).expect("just inserted");
                                state.format.reset();
                                let start = chunk.len() - MAX_REMAINDER_BYTES;
                                state.remainder = chunk.split_off(start);
                                state.overflow_tainted = true;
                                // Do NOT call apply_remainder_consumed() — data is preserved.
                            } else {
                                let state = self.sources.get_mut(&key).expect("just inserted");
                                state.remainder = chunk;
                            }
                            continue;
                        }
                    }

                    // Process complete lines through per-source format handler.
                    self.out_buf.clear();
                    self.cri_metadata_buf.clear();
                    let state = self.sources.get_mut(&key).expect("just inserted");
                    let mut process_start = 0usize;
                    if tainted_on_entry {
                        // Overflow truncation preserves a suffix of a long line.
                        // Once a newline arrives, that first "line" is a
                        // synthetic mid-line fragment and must be dropped.
                        if let Some(first_newline) = memchr::memchr(b'\n', &chunk) {
                            process_start = first_newline + 1;
                            state.overflow_tainted = false;
                        }
                    }
                    if process_start < chunk.len() {
                        state.format.process_lines_with_metadata(
                            &chunk[process_start..],
                            &mut self.out_buf,
                            Some(&mut self.cri_metadata_buf),
                        );
                    }
                    let line_count = memchr::memchr_iter(b'\n', &chunk[process_start..]).count();
                    self.stats.inc_lines(line_count as u64);

                    if !self.out_buf.is_empty() {
                        // Take out_buf's content, swap in spare_buf's capacity
                        // for next iteration. No allocation — the 64KB bounces
                        // between the two buffers.
                        let data = std::mem::take(&mut self.out_buf);
                        let cri_metadata = self.cri_metadata_for_emitted_data();
                        std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                        result_events.push(SourceEvent::Data {
                            bytes: Bytes::from(data),
                            source_id,
                            accounted_bytes: 0,
                            cri_metadata,
                        });
                    }

                    if key.is_some()
                        && self.inner.should_reclaim_completed_source_state()
                        && self
                            .sources
                            .get(&key)
                            .is_some_and(SourceState::is_reclaimable)
                    {
                        self.sources.remove(&key);
                    }
                }
                SourceEvent::Batch {
                    batch,
                    source_id,
                    accounted_bytes,
                } => {
                    self.stats.inc_lines(batch.num_rows() as u64);
                    self.stats.inc_bytes(accounted_bytes);
                    result_events.push(SourceEvent::Batch {
                        batch,
                        source_id,
                        accounted_bytes: 0,
                    });
                }
                // Rotation/truncation: clear framing state + forward event.
                //
                // When source_id is known, clear only the affected source's
                // state. When unknown (None), clear all sources as a
                // conservative fallback.
                event @ (SourceEvent::Rotated { source_id }
                | SourceEvent::Truncated { source_id }) => {
                    match source_id {
                        Some(_) => {
                            self.sources.remove(&source_id);
                        }
                        None => {
                            self.sources.clear();
                        }
                    }
                    result_events.push(event);
                }
                // End of file: flush any partial-line remainder.
                //
                // When a file ends without a trailing newline the last record
                // sits in the remainder indefinitely.  Appending a synthetic
                // `\n` lets the format processor treat it as a complete line so
                // it reaches the scanner instead of being silently dropped.
                //
                // When source_id is known, flush only the affected source's
                // remainder. When unknown (None), flush all remainders as a
                // conservative fallback.
                SourceEvent::EndOfFile { source_id } => {
                    let keys_to_flush: Vec<Option<SourceId>> = match source_id {
                        Some(_) => vec![source_id],
                        None => self.sources.keys().copied().collect(),
                    };
                    for key in keys_to_flush {
                        if let Some(state) = self.sources.get_mut(&key)
                            && !state.remainder.is_empty()
                        {
                            let mut remainder = std::mem::take(&mut state.remainder);
                            remainder.push(b'\n');
                            let mut process_start = 0usize;
                            if state.overflow_tainted {
                                process_start =
                                    memchr::memchr(b'\n', &remainder).map_or(0, |i| i + 1);
                                state.overflow_tainted = false;
                            }

                            self.out_buf.clear();
                            self.cri_metadata_buf.clear();
                            let state = self.sources.get_mut(&key).expect("just checked existence");
                            if process_start < remainder.len() {
                                state.format.process_lines_with_metadata(
                                    &remainder[process_start..],
                                    &mut self.out_buf,
                                    Some(&mut self.cri_metadata_buf),
                                );
                            }
                            let emitted_line_count =
                                memchr::memchr_iter(b'\n', &remainder[process_start..]).count();
                            self.stats.inc_lines(emitted_line_count as u64);

                            // Remainder was flushed — update tracker so
                            // checkpointable_offset advances past the
                            // flushed bytes.
                            let state = self.sources.get_mut(&key).expect("just checked existence");
                            state.tracker.apply_remainder_consumed();

                            if !self.out_buf.is_empty() {
                                let data = std::mem::take(&mut self.out_buf);
                                let cri_metadata = self.cri_metadata_for_emitted_data();
                                std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                                result_events.push(SourceEvent::Data {
                                    bytes: Bytes::from(data),
                                    source_id: key,
                                    accounted_bytes: 0,
                                    cri_metadata,
                                });
                            }
                        }
                        // Reclaim only completed EOF state. CRI P/F assembly
                        // can hold pending data even when the line remainder
                        // is empty, and file-tail EOF can be an idle signal
                        // rather than a terminal source event.
                        if self
                            .sources
                            .get(&key)
                            .is_some_and(SourceState::is_reclaimable)
                        {
                            self.sources.remove(&key);
                        }
                    }
                }
            }
        }

        result_events
    }
}

impl InputSource for FramedInput {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        let raw_events = self.inner.poll()?;
        Ok(self.process_raw_events(raw_events))
    }

    fn poll_into(&mut self, dst: &mut BytesMut) -> io::Result<Option<Vec<FramedReadEvent>>> {
        let raw_events = self.inner.poll()?;
        Ok(Some(self.process_raw_events_into(raw_events, dst)?))
    }

    fn poll_shutdown(&mut self) -> io::Result<Vec<SourceEvent>> {
        let raw_events = self.inner.poll_shutdown()?;
        Ok(self.process_raw_events(raw_events))
    }

    fn poll_shutdown_into(
        &mut self,
        dst: &mut BytesMut,
    ) -> io::Result<Option<Vec<FramedReadEvent>>> {
        let raw_events = self.inner.poll_shutdown()?;
        Ok(Some(self.process_raw_events_into(raw_events, dst)?))
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn health(&self) -> ComponentHealth {
        self.inner.health()
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    fn apply_hints(&mut self, hints: &FilterHints) {
        self.inner.apply_hints(hints);
    }

    fn get_cadence(&self) -> InputCadence {
        let mut cadence = self.inner.get_cadence();
        cadence.signal.had_data |= self.last_raw_had_payload;
        cadence
    }

    /// Return checkpoint offsets from the Kani-proven CheckpointTracker.
    ///
    /// Each per-source tracker maintains the relationship between the file
    /// read offset and the last complete newline boundary. The
    /// `checkpointable_offset()` is always at a newline boundary, so a
    /// crash + restart from that offset will not skip any unprocessed data.
    ///
    /// This replaces ad-hoc `offset.saturating_sub(remainder_len)` with
    /// the same proven arithmetic from `CheckpointTracker`.
    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        self.inner
            .checkpoint_data()
            .into_iter()
            .map(|(sid, offset)| {
                let checkpointable = self.sources.get(&Some(sid)).map_or(offset.0, |state| {
                    // The tracker's checkpointable_offset is relative to
                    // the tracker's cumulative read_offset. We need to
                    // translate: the inner source reports absolute file
                    // offset, and the tracker reports how much remainder
                    // to subtract.
                    let remainder_len = state.tracker.remainder_len();
                    offset.0.saturating_sub(remainder_len)
                });
                (sid, ByteOffset(checkpointable))
            })
            .collect()
    }

    fn source_paths(&self) -> Vec<(SourceId, std::path::PathBuf)> {
        self.inner.source_paths()
    }

    fn should_reclaim_completed_source_state(&self) -> bool {
        self.inner.should_reclaim_completed_source_state()
    }

    fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) {
        self.inner.set_offset_by_source(source_id, offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::VecDeque;

    /// Mock input source for testing.
    struct MockSource {
        name: String,
        events: VecDeque<Vec<SourceEvent>>,
        shutdown_events: VecDeque<Vec<SourceEvent>>,
        offsets: Vec<(SourceId, ByteOffset)>,
        source_paths: Vec<(SourceId, std::path::PathBuf)>,
        health: ComponentHealth,
        cadence_signal: PollCadenceSignal,
        cadence_max: u8,
    }

    impl MockSource {
        fn new(batches: Vec<Vec<SourceEvent>>) -> Self {
            Self {
                name: "mock".to_string(),
                events: batches.into(),
                shutdown_events: VecDeque::new(),
                offsets: vec![],
                source_paths: vec![],
                health: ComponentHealth::Healthy,
                cadence_signal: PollCadenceSignal::default(),
                cadence_max: 0,
            }
        }

        fn from_chunks(chunks: Vec<&[u8]>) -> Self {
            Self::new(
                chunks
                    .into_iter()
                    .map(|c| {
                        vec![SourceEvent::Data {
                            bytes: Bytes::from(c.to_vec()),
                            source_id: None,
                            accounted_bytes: c.len() as u64,
                            cri_metadata: None,
                        }]
                    })
                    .collect(),
            )
        }

        /// Like `from_chunks`, but tags every event with `Some(sid)` so that
        /// `FramedInput::checkpoint_data()` can actually find the per-source
        /// state under `Some(SourceId)` instead of falling back to the raw
        /// offset.  Use this whenever a test needs to assert `checkpoint_data`.
        fn from_chunks_with_source(chunks: Vec<&[u8]>, sid: SourceId) -> Self {
            Self::new(
                chunks
                    .into_iter()
                    .map(|c| {
                        vec![SourceEvent::Data {
                            bytes: Bytes::from(c.to_vec()),
                            source_id: Some(sid),
                            accounted_bytes: c.len() as u64,
                            cri_metadata: None,
                        }]
                    })
                    .collect(),
            )
        }

        fn with_offsets(mut self, offsets: Vec<(SourceId, ByteOffset)>) -> Self {
            self.offsets = offsets;
            self
        }

        fn with_shutdown_events(mut self, events: Vec<Vec<SourceEvent>>) -> Self {
            self.shutdown_events = events.into();
            self
        }

        fn with_source_paths(mut self, source_paths: Vec<(SourceId, std::path::PathBuf)>) -> Self {
            self.source_paths = source_paths;
            self
        }

        fn with_health(mut self, health: ComponentHealth) -> Self {
            self.health = health;
            self
        }

        fn with_cadence(mut self, signal: PollCadenceSignal, max: u8) -> Self {
            self.cadence_signal = signal;
            self.cadence_max = max;
            self
        }
    }

    impl InputSource for MockSource {
        fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
            Ok(self.events.pop_front().unwrap_or_default())
        }

        fn poll_shutdown(&mut self) -> io::Result<Vec<SourceEvent>> {
            Ok(self.shutdown_events.pop_front().unwrap_or_default())
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn health(&self) -> ComponentHealth {
            self.health
        }

        fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
            self.offsets.clone()
        }

        fn source_paths(&self) -> Vec<(SourceId, std::path::PathBuf)> {
            self.source_paths.clone()
        }

        fn get_cadence(&self) -> InputCadence {
            InputCadence {
                signal: self.cadence_signal,
                adaptive_fast_polls_max: self.cadence_max,
            }
        }
    }

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("seq", DataType::Int64, true),
        ]));
        let msg = StringArray::from(vec![Some("alpha"), Some("beta")]);
        let seq = Int64Array::from(vec![Some(1), Some(2)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(seq)]).expect("batch")
    }

    fn collect_data(events: Vec<SourceEvent>) -> Vec<u8> {
        let mut out = Vec::new();
        for e in events {
            if let SourceEvent::Data { bytes, .. } = e {
                out.extend_from_slice(&bytes);
            }
        }
        out
    }

    #[test]
    fn passthrough_complete_lines() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"line1\nline2\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"line1\nline2\n");
    }

    #[test]
    fn passthrough_poll_into_appends_complete_lines() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"line1\nline2\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );
        let mut dst = BytesMut::from(&b"seed\n"[..]);

        let events = framed
            .poll_into(&mut dst)
            .expect("poll_into")
            .expect("passthrough path available");

        assert_eq!(dst.as_ref(), b"seed\nline1\nline2\n");
        assert_eq!(events.len(), 1);
        let FramedReadEvent::Data {
            range, source_id, ..
        } = &events[0]
        else {
            panic!("expected appended data event");
        };
        assert_eq!(source_id, &None);
        assert_eq!(&dst[range.clone()], b"line1\nline2\n");
    }

    #[test]
    fn passthrough_poll_shutdown_into_flushes_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"partial"),
            source_id: None,
            accounted_bytes: 7,
            cri_metadata: None,
        }]])
        .with_shutdown_events(vec![vec![SourceEvent::EndOfFile { source_id: None }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );
        let mut dst = BytesMut::new();

        let first = framed
            .poll_into(&mut dst)
            .expect("poll_into")
            .expect("passthrough path available");
        assert!(first.is_empty());
        assert!(dst.is_empty());

        let shutdown = framed
            .poll_shutdown_into(&mut dst)
            .expect("shutdown poll_into")
            .expect("passthrough path available");

        assert_eq!(shutdown.len(), 1);
        assert_eq!(dst.as_ref(), b"partial\n");
    }

    #[test]
    fn cri_poll_into_reassembles_and_tracks_metadata() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout P {\"msg\":\n"),
                source_id: None,
                accounted_bytes: 40,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout F \"hello\"}\n"),
                source_id: None,
                accounted_bytes: 43,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );
        let mut dst = BytesMut::from(&b"seed\n"[..]);

        let first = framed
            .poll_into(&mut dst)
            .expect("first poll_into")
            .expect("buffered path available");
        assert!(first.is_empty());
        assert_eq!(dst.as_ref(), b"seed\n");

        let second = framed
            .poll_into(&mut dst)
            .expect("second poll_into")
            .expect("buffered path available");

        assert_eq!(second.len(), 1);
        let FramedReadEvent::Data {
            range,
            source_id,
            cri_metadata,
        } = &second[0]
        else {
            panic!("expected data event");
        };
        assert_eq!(*source_id, None);
        assert_eq!(&dst[range.clone()], b"{\"msg\":\"hello\"}\n");

        let metadata = cri_metadata.as_ref().expect("CRI metadata");
        assert_eq!(metadata.rows, 1);
        assert!(metadata.has_values);
        assert_eq!(metadata.spans.len(), 1);
        let values = metadata.spans[0]
            .values
            .as_ref()
            .expect("non-null span values");
        assert_eq!(metadata.timestamp(values), b"2024-01-15T10:30:00Z");
        assert_eq!(values.stream.as_str(), "stdout");
    }

    #[test]
    fn cri_poll_into_clears_metadata_between_complete_messages() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout F first\n"),
                source_id: None,
                accounted_bytes: 37,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:01Z stderr F second\n"),
                source_id: None,
                accounted_bytes: 38,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );
        let mut dst = BytesMut::new();

        let first = framed
            .poll_into(&mut dst)
            .expect("first poll_into")
            .expect("buffered path available");
        let second = framed
            .poll_into(&mut dst)
            .expect("second poll_into")
            .expect("buffered path available");

        let FramedReadEvent::Data {
            cri_metadata: first_metadata,
            ..
        } = &first[0]
        else {
            panic!("expected first data event");
        };
        let FramedReadEvent::Data {
            cri_metadata: second_metadata,
            ..
        } = &second[0]
        else {
            panic!("expected second data event");
        };

        let first_metadata = first_metadata.as_ref().expect("first CRI metadata");
        let second_metadata = second_metadata.as_ref().expect("second CRI metadata");
        assert_eq!(first_metadata.rows, 1);
        assert_eq!(second_metadata.rows, 1);
        let first_values = first_metadata.spans[0]
            .values
            .as_ref()
            .expect("first non-null metadata");
        let second_values = second_metadata.spans[0]
            .values
            .as_ref()
            .expect("second non-null metadata");
        assert_eq!(first_values.stream.as_str(), "stdout");
        assert_eq!(second_values.stream.as_str(), "stderr");
        assert_eq!(
            second_metadata.timestamp(second_values),
            b"2024-01-15T10:30:01Z"
        );
    }

    #[test]
    fn auto_poll_shutdown_into_flushes_rewritten_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"not a cri line"),
            source_id: None,
            accounted_bytes: 14,
            cri_metadata: None,
        }]])
        .with_shutdown_events(vec![vec![SourceEvent::EndOfFile { source_id: None }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::auto(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );
        let mut dst = BytesMut::new();

        let first = framed
            .poll_into(&mut dst)
            .expect("poll_into")
            .expect("buffered path available");
        assert!(first.is_empty());
        assert!(dst.is_empty());

        let shutdown = framed
            .poll_shutdown_into(&mut dst)
            .expect("shutdown poll_into")
            .expect("buffered path available");

        assert_eq!(shutdown.len(), 1);
        let FramedReadEvent::Data {
            range,
            cri_metadata,
            ..
        } = &shutdown[0]
        else {
            panic!("expected data event");
        };
        assert_eq!(&dst[range.clone()], b"{\"body\":\"not a cri line\"}\n");

        let metadata = cri_metadata.as_ref().expect("null-row CRI sidecar");
        assert_eq!(metadata.rows, 1);
        assert!(!metadata.has_values);
        assert_eq!(metadata.spans.len(), 1);
        assert!(metadata.spans[0].values.is_none());
    }

    #[test]
    #[cfg_attr(not(miri), ignore = "miri-targeted shared-buffer regression")]
    fn poll_into_miri_shared_buffer_alias_regression() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"line-1\nline-2\n"),
                source_id: Some(SourceId(7)),
                accounted_bytes: 14,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"line-3\n"),
                source_id: Some(SourceId(7)),
                accounted_bytes: 7,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );
        let mut dst = BytesMut::with_capacity(64);

        let first = framed
            .poll_into(&mut dst)
            .expect("first poll_into")
            .expect("shared-buffer path available");
        assert_eq!(first.len(), 1);
        let first_range = match &first[0] {
            FramedReadEvent::Data {
                range, source_id, ..
            } => {
                assert_eq!(*source_id, Some(SourceId(7)));
                range.clone()
            }
            _ => panic!("expected data event"),
        };
        let first_snapshot = dst[first_range.clone()].to_vec();
        assert_eq!(first_snapshot, b"line-1\nline-2\n");

        let second = framed
            .poll_into(&mut dst)
            .expect("second poll_into")
            .expect("shared-buffer path available");
        assert_eq!(second.len(), 1);
        let second_range = match &second[0] {
            FramedReadEvent::Data {
                range, source_id, ..
            } => {
                assert_eq!(*source_id, Some(SourceId(7)));
                range.clone()
            }
            _ => panic!("expected data event"),
        };

        assert_eq!(&dst[first_range], b"line-1\nline-2\n");
        assert_eq!(&dst[second_range], b"line-3\n");
    }

    #[test]
    #[ignore = "benchmark (directional)"]
    fn bench_poll_into_shared_buffer_directional() {
        use std::time::Instant;

        const LINES: usize = 4_096;
        const ITERS: usize = 100;

        let payload: Vec<u8> = (0..LINES)
            .flat_map(|idx| format!("{{\"msg\":\"line-{idx}\"}}\n").into_bytes())
            .collect();

        let mut poll_elapsed = std::time::Duration::ZERO;
        let mut poll_into_elapsed = std::time::Duration::ZERO;

        for _ in 0..ITERS {
            let stats = make_stats();
            let source = MockSource::from_chunks(vec![payload.as_slice()]);
            let mut framed = FramedInput::new(
                Box::new(source),
                FormatDecoder::passthrough(Arc::clone(&stats)),
                stats,
            );
            let start = Instant::now();
            let events = framed.poll().expect("poll");
            poll_elapsed += start.elapsed();
            assert_eq!(collect_data(events), payload);

            let stats = make_stats();
            let source = MockSource::from_chunks(vec![payload.as_slice()]);
            let mut framed = FramedInput::new(
                Box::new(source),
                FormatDecoder::passthrough(Arc::clone(&stats)),
                stats,
            );
            let mut dst = BytesMut::new();
            let start = Instant::now();
            let events = framed
                .poll_into(&mut dst)
                .expect("poll_into")
                .expect("shared-buffer path available");
            poll_into_elapsed += start.elapsed();
            assert_eq!(events.len(), 1);
            assert_eq!(dst.as_ref(), payload.as_slice());
        }

        eprintln!(
            "framed shared-buffer directional bench: poll={:?} poll_into={:?} ratio={:.3}",
            poll_elapsed,
            poll_into_elapsed,
            poll_into_elapsed.as_secs_f64() / poll_elapsed.as_secs_f64()
        );
    }

    #[test]
    fn framed_input_forwards_inner_health() {
        let stats = make_stats();
        let source = MockSource::new(vec![]).with_health(ComponentHealth::Degraded);
        let framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        assert_eq!(framed.health(), ComponentHealth::Degraded);
    }

    #[test]
    fn framed_input_forwards_cadence_snapshot() {
        let stats = make_stats();
        let source = MockSource::new(vec![]).with_cadence(
            PollCadenceSignal {
                had_data: true,
                hit_read_budget: true,
            },
            7,
        );
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let _ = framed.poll().expect("framed poll should succeed");
        assert_eq!(
            framed.get_cadence(),
            InputCadence {
                signal: PollCadenceSignal {
                    had_data: true,
                    hit_read_budget: true,
                },
                adaptive_fast_polls_max: 7,
            }
        );
    }

    #[test]
    fn framed_input_reports_raw_shutdown_payload_when_output_is_empty() {
        let stats = make_stats();
        let source = MockSource::new(vec![]).with_shutdown_events(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"partial"),
            source_id: Some(SourceId(1)),
            accounted_bytes: 7,
            cri_metadata: None,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let events = framed
            .poll_shutdown()
            .expect("shutdown poll should succeed");
        assert!(events.is_empty());
        assert!(framed.get_cadence().signal.had_data);
    }

    #[test]
    fn remainder_across_polls() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"hello\nwor", b"ld\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // First poll: "hello\n" is complete, "wor" is remainder
        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"hello\n");

        // Second poll: remainder "wor" + "ld\n" → "world\n"
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"world\n");
    }

    #[test]
    fn no_newline_becomes_remainder() {
        let stats = make_stats();
        let source = MockSource::from_chunks(vec![b"partial", b"more\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: no newline, everything goes to remainder
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Second poll: remainder + new data → complete line
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"partialmore\n");
    }

    #[test]
    fn batch_events_from_inner_source_are_forwarded() {
        let stats = make_stats();
        let batch = make_batch();
        let source = MockSource::new(vec![vec![SourceEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes: 1234,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events = framed.poll().expect("structured event should be forwarded");
        assert_eq!(events.len(), 1);
        match &events[0] {
            SourceEvent::Batch {
                batch,
                source_id,
                accounted_bytes,
            } => {
                assert_eq!(batch.num_rows(), 2);
                assert_eq!(*source_id, None);
                assert_eq!(*accounted_bytes, 0);
            }
            _ => panic!("expected batch event"),
        }
        assert_eq!(stats.lines(), 2);
        assert_eq!(stats.bytes(), 1234);
    }

    #[test]
    fn poll_into_rejects_batch_events_from_inner_source() {
        let stats = make_stats();
        let batch = make_batch();
        let source = MockSource::new(vec![vec![SourceEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes: 1234,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let mut dst = BytesMut::new();
        let err = match framed.poll_into(&mut dst) {
            Ok(_) => panic!("shared-buffer path should reject structured event"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("SourceEvent::Batch"));
        assert!(dst.is_empty());
    }

    #[test]
    fn data_events_use_accounted_bytes_for_stats() {
        let stats = make_stats();
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"line\n"),
            source_id: None,
            accounted_bytes: 99,
            cri_metadata: None,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"line\n");
        assert_eq!(stats.lines(), 1);
        assert_eq!(stats.bytes(), 99);
    }

    #[test]
    fn remainder_capped_at_max_and_tainted_line_is_dropped() {
        let stats = make_stats();
        // Send > 2 MiB without a newline.
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let source = MockSource::from_chunks(vec![&big, b"\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: no newline, overflow triggers parse_error.
        let events = framed.poll().unwrap();
        assert!(collect_data(events).is_empty());
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        // The overflow remainder is capped but NOT discarded.
        let state = framed.sources.get(&None).unwrap();
        assert_eq!(
            state.remainder.len(),
            MAX_REMAINDER_BYTES,
            "overflow remainder must be capped to MAX_REMAINDER_BYTES, not dropped"
        );

        // Second poll: newline completes only the tainted fragment; it must be dropped.
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert!(
            data2.is_empty(),
            "tainted overflow remainder must be discarded when the first newline arrives"
        );
    }

    #[test]
    fn tail_after_newline_is_capped_at_max_and_tainted_line_is_dropped() {
        let stats = make_stats();
        let mut chunk = b"ok\n".to_vec();
        chunk.extend(vec![b'x'; MAX_REMAINDER_BYTES + 1]);
        let source = MockSource::from_chunks(vec![&chunk, b"\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: "ok\n" emitted; overflow tail triggers parse_error and
        // is preserved as remainder (last MAX_REMAINDER_BYTES bytes).
        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"ok\n");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        // The overflow remainder is preserved (not silently dropped).
        let state = framed.sources.get(&None).unwrap();
        assert_eq!(
            state.remainder.len(),
            MAX_REMAINDER_BYTES,
            "overflow tail must be truncated to MAX_REMAINDER_BYTES, not dropped"
        );

        // Second poll: newline terminates only tainted overflow bytes; that
        // line must be discarded.
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert!(
            data2.is_empty(),
            "tainted overflow remainder must be discarded when the first newline arrives"
        );
    }

    /// Regression for #1030: after remainder overflow, the first completed
    /// line is a truncated mid-line fragment and must be discarded.
    #[test]
    fn overflow_fragment_is_discarded_when_newline_arrives() {
        let stats = make_stats();
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let source = MockSource::from_chunks(vec![&big, b"\nreal-line\n"]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: overflow, nothing emitted.
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Second poll: truncated overflow fragment must be dropped, while the
        // next complete real line is preserved.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"real-line\n");
    }

    #[test]
    fn checkpoint_advances_after_tainted_fragment_is_discarded() {
        let stats = make_stats();
        let sid = SourceId(9);
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let first = big.len() as u64;
        let second_bytes = b"\nreal-line\n";
        let total = first + second_bytes.len() as u64;
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from(big),
                source_id: Some(sid),
                accounted_bytes: 0,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from(second_bytes.to_vec()),
                source_id: Some(sid),
                accounted_bytes: 0,
                cri_metadata: None,
            }],
        ])
        .with_offsets(vec![(sid, ByteOffset(total))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let _ = framed.poll().unwrap();
        let cp1 = framed.checkpoint_data();
        assert!(
            cp1[0].1.0 < total,
            "checkpoint must stay behind raw offset while overflow remainder is buffered"
        );

        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"real-line\n");
        let cp2 = framed.checkpoint_data();
        assert_eq!(cp2[0].1, ByteOffset(total));
    }

    /// `checkpoint_data()` must account for overflow remainder when the source
    /// has a real `SourceId`.  With `source_id: None` the lookup in
    /// `self.sources.get(&Some(sid))` silently falls back to the raw offset,
    /// making the assertion trivially true regardless of correctness.  This
    /// test uses `from_chunks_with_source` so the per-source state is actually
    /// keyed under `Some(SourceId(1))` and the checkpoint path is exercised.
    #[test]
    fn checkpoint_data_accounts_for_overflow_remainder() {
        let stats = make_stats();
        let sid = SourceId(1);
        // The inner source claims it has read `big.len()` bytes.  We set the
        // reported offset to exactly that many bytes so the checkpoint must
        // subtract the remainder to be correct.
        let big_len = MAX_REMAINDER_BYTES + 1;
        let big = vec![b'x'; big_len];
        // Reported offset equals the number of bytes the inner source has
        // "read" so far: big_len bytes with no newline.
        let reported_offset = big_len as u64;
        let source = MockSource::from_chunks_with_source(vec![&big, b"\n"], sid)
            .with_offsets(vec![(sid, ByteOffset(reported_offset))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: no newline — overflow triggers parse_error and remainder
        // is capped to MAX_REMAINDER_BYTES.
        let _ = framed.poll().unwrap();

        // The per-source state must be reachable under Some(sid).
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.remainder.len(), MAX_REMAINDER_BYTES);

        // checkpoint_data() must subtract the remainder length from the raw
        // offset, not return the raw offset unchanged.
        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1, "expected exactly one checkpoint entry");
        let (cp_sid, cp_offset) = cp[0];
        assert_eq!(cp_sid, sid);
        // The checkpointable offset must be strictly less than the reported
        // offset because there is an undelivered remainder buffered.
        assert!(
            cp_offset.0 < reported_offset,
            "checkpoint offset ({}) must be less than raw offset ({}) when remainder is buffered",
            cp_offset.0,
            reported_offset
        );
    }

    /// `checkpoint_data()` must account for the overflow tail remainder when
    /// a newline precedes the overflow.  Uses `from_chunks_with_source` so the
    /// assertion exercises the real `self.sources.get(&Some(sid))` path.
    #[test]
    fn checkpoint_data_accounts_for_overflow_tail_remainder() {
        let stats = make_stats();
        let sid = SourceId(1);
        let mut chunk = b"ok\n".to_vec();
        chunk.extend(vec![b'x'; MAX_REMAINDER_BYTES + 1]);
        let chunk_len = chunk.len() as u64;
        let source = MockSource::from_chunks_with_source(vec![&chunk, b"\n"], sid)
            .with_offsets(vec![(sid, ByteOffset(chunk_len))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        // First poll: "ok\n" is emitted and the overflow tail becomes the
        // remainder (capped to MAX_REMAINDER_BYTES).
        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"ok\n");

        // Per-source state is keyed under Some(sid).
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.remainder.len(), MAX_REMAINDER_BYTES);

        // The checkpoint must be behind the raw offset because the remainder
        // has not yet been delivered as a complete line.
        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1);
        let (cp_sid, cp_offset) = cp[0];
        assert_eq!(cp_sid, sid);
        assert!(
            cp_offset.0 < chunk_len,
            "checkpoint offset ({}) must be less than raw offset ({}) when overflow tail is buffered",
            cp_offset.0,
            chunk_len
        );
    }

    #[test]
    fn rotated_clears_remainder_and_format() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"partial"),
                source_id: None,
                accounted_bytes: 7,
                cri_metadata: None,
            }],
            vec![SourceEvent::Rotated { source_id: None }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"fresh\n"),
                source_id: None,
                accounted_bytes: 6,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Partial goes to remainder
        let _ = framed.poll().unwrap();

        // Rotation clears remainder
        let events2 = framed.poll().unwrap();
        assert!(
            events2
                .iter()
                .any(|e| matches!(e, SourceEvent::Rotated { .. }))
        );

        // Fresh data starts clean (no stale "partial" prefix)
        let events3 = framed.poll().unwrap();
        assert_eq!(collect_data(events3), b"fresh\n");
    }

    #[test]
    fn cri_format_extracts_messages() {
        let stats = make_stats();
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let source = MockSource::from_chunks(vec![input.as_slice()]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn cri_format_emits_metadata_sidecar() {
        let stats = make_stats();
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let source = MockSource::from_chunks(vec![input.as_slice()]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        let SourceEvent::Data {
            bytes,
            cri_metadata: Some(metadata),
            ..
        } = &events[0]
        else {
            panic!("expected data event with CRI metadata");
        };
        assert_eq!(bytes.as_ref(), b"{\"msg\":\"hello\"}\n");
        assert_eq!(metadata.rows, 1);
        let values = metadata.spans[0].values.as_ref().expect("metadata values");
        assert_eq!(metadata.timestamp(values), b"2024-01-15T10:30:00Z");
        assert_eq!(values.stream.as_str(), "stdout");
    }

    #[test]
    fn split_anywhere_produces_same_output() {
        let stats = make_stats();
        let full_input = b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n";

        // Reference: process entire input at once
        let source_full = MockSource::from_chunks(vec![full_input.as_slice()]);
        let mut framed_full = FramedInput::new(
            Box::new(source_full),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );
        let reference = collect_data(framed_full.poll().unwrap());

        // Split at every possible byte position
        for split_at in 1..full_input.len() {
            let stats2 = make_stats();
            let chunk1 = &full_input[..split_at];
            let chunk2 = &full_input[split_at..];
            let source = MockSource::from_chunks(vec![chunk1, chunk2]);
            let mut framed = FramedInput::new(
                Box::new(source),
                FormatDecoder::passthrough(Arc::clone(&stats2)),
                stats2,
            );

            let mut collected = collect_data(framed.poll().unwrap());
            collected.extend_from_slice(&collect_data(framed.poll().unwrap()));

            assert_eq!(
                collected, reference,
                "split at byte {split_at} produced different output"
            );
        }
    }

    /// A file (or any source) that ends without a trailing newline must not
    /// silently drop its last record.  The `EndOfFile` event causes
    /// `FramedInput` to flush the remainder buffer with a synthetic newline.
    #[test]
    fn eof_flushes_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"no-newline"),
                source_id: None,
                accounted_bytes: 10,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile { source_id: None }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        // First poll: data with no newline — goes to remainder, nothing emitted.
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Second poll: EndOfFile flushes the remainder as a complete line.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"no-newline\n");
    }

    /// Runtime shutdown is a terminal lifecycle event: when the wrapped source
    /// emits EOF from `poll_shutdown`, `FramedInput` must flush bytes that were
    /// already held in its per-source remainder buffer.
    #[test]
    fn poll_shutdown_flushes_existing_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"no-newline"),
            source_id: None,
            accounted_bytes: 10,
            cri_metadata: None,
        }]])
        .with_shutdown_events(vec![vec![SourceEvent::EndOfFile { source_id: None }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        let events2 = framed.poll_shutdown().unwrap();
        assert_eq!(collect_data(events2), b"no-newline\n");
    }

    /// Multiple records in a file where only the last one lacks a newline:
    /// all records must be emitted.
    #[test]
    fn eof_flushes_only_partial_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"complete\npartial"),
                source_id: None,
                accounted_bytes: 16,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile { source_id: None }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        // First poll: "complete\n" is emitted; "partial" stays in remainder.
        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"complete\n");

        // Second poll: EndOfFile flushes "partial" with a synthetic newline.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"partial\n");
    }

    /// A redundant EndOfFile (no bytes in remainder) must produce no output.
    #[test]
    fn eof_with_empty_remainder_is_noop() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"line\n"),
                source_id: None,
                accounted_bytes: 5,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile { source_id: None }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"line\n");

        let events2 = framed.poll().unwrap();
        assert!(collect_data(events2).is_empty());
    }

    #[test]
    fn eof_flushes_remainder_and_advances_checkpoint() {
        let stats = make_stats();
        let sid = SourceId(42);
        let chunk = b"partial-line";
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from(chunk.to_vec()),
                source_id: Some(sid),
                accounted_bytes: chunk.len() as u64,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile {
                source_id: Some(sid),
            }],
        ])
        .with_offsets(vec![(sid, ByteOffset(chunk.len() as u64))]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events1 = framed.poll().expect("first poll");
        assert!(collect_data(events1).is_empty());
        let before_flush = framed.checkpoint_data();
        assert_eq!(before_flush[0].0, sid);
        assert!(
            before_flush[0].1.0 < chunk.len() as u64,
            "checkpoint should remain behind raw offset while remainder is buffered"
        );

        let events2 = framed.poll().expect("eof poll");
        assert_eq!(collect_data(events2), b"partial-line\n");
        let after_flush = framed.checkpoint_data();
        assert_eq!(after_flush[0], (sid, ByteOffset(chunk.len() as u64)));
    }

    #[test]
    fn eof_for_source_only_flushes_matching_remainder() {
        let stats = make_stats();
        let sid_a = SourceId(10);
        let sid_b = SourceId(11);
        let source = MockSource::new(vec![
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"alpha"),
                    source_id: Some(sid_a),
                    accounted_bytes: 5,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"beta"),
                    source_id: Some(sid_b),
                    accounted_bytes: 4,
                    cri_metadata: None,
                },
            ],
            vec![SourceEvent::EndOfFile {
                source_id: Some(sid_a),
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let first = framed.poll().expect("first poll");
        assert!(collect_data(first).is_empty());

        let flushed = framed.poll().expect("second poll");
        assert_eq!(collect_data(flushed), b"alpha\n");

        let state_b = framed
            .sources
            .get(&Some(sid_b))
            .expect("sid_b state should still exist");
        assert_eq!(state_b.remainder, b"beta");
    }

    // -----------------------------------------------------------------------
    // Per-source remainder tests
    // -----------------------------------------------------------------------

    /// Two sources interleaving with partial lines -- no cross-contamination.
    #[test]
    fn two_sources_interleaved_no_cross_contamination() {
        let stats = make_stats();
        let sid_a = SourceId(100);
        let sid_b = SourceId(200);

        let source = MockSource::new(vec![
            // Poll 1: partial lines from both sources in one batch
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"hello-from-A"),
                    source_id: Some(sid_a),
                    accounted_bytes: 12,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"hello-from-B"),
                    source_id: Some(sid_b),
                    accounted_bytes: 12,
                    cri_metadata: None,
                },
            ],
            // Poll 2: complete the lines from each source
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"-done\n"),
                    source_id: Some(sid_a),
                    accounted_bytes: 6,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"-done\n"),
                    source_id: Some(sid_b),
                    accounted_bytes: 6,
                    cri_metadata: None,
                },
            ],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Poll 1: no complete lines -- everything in remainders.
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Poll 2: each source gets its own remainder prepended.
        let events2 = framed.poll().unwrap();
        let mut output_a = Vec::new();
        let mut output_b = Vec::new();
        for e in events2 {
            if let SourceEvent::Data {
                bytes, source_id, ..
            } = e
            {
                match source_id {
                    Some(sid) if sid == sid_a => output_a.extend_from_slice(&bytes),
                    Some(sid) if sid == sid_b => output_b.extend_from_slice(&bytes),
                    _ => panic!("unexpected source_id"),
                }
            }
        }
        assert_eq!(output_a, b"hello-from-A-done\n");
        assert_eq!(output_b, b"hello-from-B-done\n");
    }

    /// Truncation clears all remainders (current behavior).
    #[test]
    fn truncation_clears_all_remainders() {
        let stats = make_stats();
        let sid_a = SourceId(100);
        let sid_b = SourceId(200);

        let source = MockSource::new(vec![
            // Partial lines from two sources
            vec![
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"partial-A"),
                    source_id: Some(sid_a),
                    accounted_bytes: 9,
                    cri_metadata: None,
                },
                SourceEvent::Data {
                    bytes: Bytes::from_static(b"partial-B"),
                    source_id: Some(sid_b),
                    accounted_bytes: 9,
                    cri_metadata: None,
                },
            ],
            // Truncation
            vec![SourceEvent::Truncated { source_id: None }],
            // Fresh data from source A
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"fresh-A\n"),
                source_id: Some(sid_a),
                accounted_bytes: 8,
                cri_metadata: None,
            }],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Partials go into remainders
        let _ = framed.poll().unwrap();

        // Truncation clears all
        let _ = framed.poll().unwrap();

        // Fresh data from A -- must NOT include "partial-A" prefix
        let events3 = framed.poll().unwrap();
        assert_eq!(collect_data(events3), b"fresh-A\n");
    }

    /// checkpoint_data() subtracts remainder length from inner offsets.
    #[test]
    fn checkpoint_data_subtracts_remainder() {
        let stats = make_stats();
        let sid = SourceId(42);

        // The inner source reports offset 1000 for our source.
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"hello\nwor"),
            source_id: Some(sid),
            accounted_bytes: 9,
            cri_metadata: None,
        }]])
        .with_offsets(vec![(sid, ByteOffset(1000))]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // After poll, "wor" (3 bytes) is in the remainder for sid.
        let _ = framed.poll().unwrap();

        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1);
        assert_eq!(cp[0].0, sid);
        // 1000 - 3 = 997
        assert_eq!(cp[0].1, ByteOffset(997));
    }

    /// checkpoint_data() returns the raw offset when no remainder is buffered.
    #[test]
    fn checkpoint_data_no_remainder() {
        let stats = make_stats();
        let sid = SourceId(42);

        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"complete\n"),
            source_id: Some(sid),
            accounted_bytes: 9,
            cri_metadata: None,
        }]])
        .with_offsets(vec![(sid, ByteOffset(500))]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let _ = framed.poll().unwrap();

        let cp = framed.checkpoint_data();
        assert_eq!(cp.len(), 1);
        assert_eq!(cp[0].1, ByteOffset(500));
    }

    // -----------------------------------------------------------------------
    // Per-source CRI isolation tests
    // -----------------------------------------------------------------------

    /// CRI P/F aggregation state is isolated between sources.
    ///
    /// Source A sends a P (partial) line, source B sends an F (full) line.
    /// Without per-source format state, B's F would complete A's P, merging
    /// data from two different files into one record.
    #[test]
    fn cri_pf_state_isolated_between_sources() {
        let stats = make_stats();
        let sid_a = SourceId(100);
        let sid_b = SourceId(200);

        let source = MockSource::new(vec![
            // Source A: CRI partial line
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout P hello \n"),
                source_id: Some(sid_a),
                accounted_bytes: 38,
                cri_metadata: None,
            }],
            // Source B: CRI full line (must NOT merge with A's partial)
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:01Z stderr F {\"msg\":\"world\"}\n"),
                source_id: Some(sid_b),
                accounted_bytes: 50,
                cri_metadata: None,
            }],
            // Source A: CRI full line (completes A's partial)
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-15T10:30:02Z stdout F from-A\n"),
                source_id: Some(sid_a),
                accounted_bytes: 39,
                cri_metadata: None,
            }],
        ]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        // Poll 1: A's partial — nothing emitted
        let events1 = framed.poll().unwrap();
        assert!(collect_data(events1).is_empty());

        // Poll 2: B's full line — emitted as standalone
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert_eq!(data2, b"{\"msg\":\"world\"}\n");
        assert!(
            !data2.windows(5).any(|w| w == b"hello"),
            "B's output must NOT contain A's partial"
        );

        // Poll 3: A's full line — completes A's P+F sequence
        let events3 = framed.poll().unwrap();
        let data3 = collect_data(events3);
        assert!(
            data3.windows(5).any(|w| w == b"hello"),
            "A's output should contain the merged P+F data"
        );
    }

    /// CheckpointTracker is updated correctly through framing operations.
    #[test]
    fn checkpoint_tracker_tracks_remainder() {
        let stats = make_stats();
        let sid = SourceId(42);

        let source = MockSource::new(vec![
            // First read: 9 bytes, newline at position 5
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"hello\nwor"),
                source_id: Some(sid),
                accounted_bytes: 9,
                cri_metadata: None,
            }],
            // Second read: 3 bytes, newline at position 1 (the 'd\n')
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"ld\n"),
                source_id: Some(sid),
                accounted_bytes: 3,
                cri_metadata: None,
            }],
        ])
        .with_offsets(vec![(sid, ByteOffset(12))]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        // After first poll: remainder is "wor" (3 bytes)
        let _ = framed.poll().unwrap();
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.tracker.remainder_len(), 3);

        // After second poll: remainder is empty (all consumed)
        let _ = framed.poll().unwrap();
        assert!(
            !framed.sources.contains_key(&Some(sid)),
            "complete source state should be reclaimed once no remainder is buffered"
        );

        // Checkpoint should fall back to the raw source offset when no
        // remainder state remains: 12 - 0 = 12.
        let cp = framed.checkpoint_data();
        assert_eq!(cp[0].1, ByteOffset(12));
    }

    #[test]
    fn file_json_does_not_inject_source_path_column() {
        let stats = make_stats();
        let sid = SourceId(7);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/main.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn file_json_preserves_source_id_without_rewriting_payload() {
        let stats = make_stats();
        let sid = SourceId(17);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/main.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let events = framed.poll().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            SourceEvent::Data {
                bytes,
                source_id: Some(actual_sid),
                ..
            } => {
                assert_eq!(&bytes[..], b"{\"msg\":\"hello\"}\n");
                assert_eq!(*actual_sid, sid);
            }
            _ => panic!("expected data event with preserved source_id"),
        }
    }

    #[test]
    fn file_cri_does_not_inject_source_path_alongside_cri_metadata() {
        let stats = make_stats();
        let sid = SourceId(8);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/0.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn file_json_preserves_leading_whitespace_without_source_path_rewrite() {
        let stats = make_stats();
        let sid = SourceId(9);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"  \t{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/1.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"  \t{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn file_json_empty_object_is_not_rewritten_for_source_path() {
        let stats = make_stats();
        let sid = SourceId(11);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/2.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{}\n");
    }

    #[test]
    fn file_raw_passthrough_does_not_inject_source_path() {
        let stats = make_stats();
        let sid = SourceId(10);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"hello\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 0,
            cri_metadata: None,
        }]])
        .with_source_paths(vec![(sid, "/var/log/pods/ns_pod_uid/c/raw.log".into())]);

        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(Arc::clone(&stats)),
            stats,
        );

        let out = collect_data(framed.poll().unwrap());
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    /// EndOfFile must reclaim per-source state so long-running inputs (S3)
    /// don't accumulate dead entries in the `sources` HashMap.
    #[test]
    fn eof_removes_source_state() {
        let stats = make_stats();
        let sid = SourceId(42);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"hello\npartial"),
                source_id: Some(sid),
                accounted_bytes: 13,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile {
                source_id: Some(sid),
            }],
            // New data for the same SourceId after EOF — must work.
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"world\n"),
                source_id: Some(sid),
                accounted_bytes: 6,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough(stats.clone()),
            stats,
        );

        // Poll 1: "hello\n" emitted, "partial" in remainder.
        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"hello\n");
        assert!(framed.sources.contains_key(&Some(sid)));

        // Poll 2: EOF flushes "partial\n" and removes source state.
        let events2 = framed.poll().unwrap();
        assert_eq!(collect_data(events2), b"partial\n");
        assert!(
            !framed.sources.contains_key(&Some(sid)),
            "source state should be removed after EOF"
        );

        // Poll 3: new data for the same SourceId creates fresh state.
        let events3 = framed.poll().unwrap();
        assert_eq!(collect_data(events3), b"world\n");
    }

    #[test]
    fn complete_source_state_is_reclaimed_without_eof() {
        let stats = make_stats();
        let sid = SourceId(42);
        let source = MockSource::new(vec![vec![SourceEvent::Data {
            bytes: Bytes::from_static(b"{\"msg\":\"done\"}\n"),
            source_id: Some(sid),
            accounted_bytes: 15,
            cri_metadata: None,
        }]]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::passthrough_json(stats.clone()),
            stats,
        );

        let events = framed.poll().unwrap();

        assert_eq!(collect_data(events), b"{\"msg\":\"done\"}\n");
        assert!(
            framed.sources.is_empty(),
            "complete source state should not accumulate for ephemeral sources"
        );
    }

    #[test]
    fn cri_partial_source_state_survives_until_full_record() {
        let stats = make_stats();
        let sid = SourceId(42);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(
                    b"2024-01-01T00:00:00.000000000Z stdout P {\"msg\":\"part",
                ),
                source_id: Some(sid),
                accounted_bytes: 57,
                cri_metadata: None,
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"ial\"}\n2024-01-01T00:00:00.000000000Z stdout F \n"),
                source_id: Some(sid),
                accounted_bytes: 54,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(1024, stats.clone()),
            stats,
        );

        let first = framed.poll().unwrap();
        assert!(first.is_empty());
        assert!(
            framed.sources.contains_key(&Some(sid)),
            "CRI partial state must survive across polls"
        );

        let second = framed.poll().unwrap();
        let output = collect_data(second);
        assert!(
            output.ends_with(b"\"msg\":\"partial\"}\n"),
            "unexpected CRI output: {}",
            String::from_utf8_lossy(&output)
        );
        assert!(
            framed.sources.is_empty(),
            "CRI source state should be reclaimed after the full record completes"
        );
    }

    #[test]
    fn eof_does_not_reclaim_pending_cri_fragment() {
        let stats = make_stats();
        let sid = SourceId(43);
        let source = MockSource::new(vec![
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(
                    b"2024-01-01T00:00:00.000000000Z stdout P {\"msg\":\"part\n",
                ),
                source_id: Some(sid),
                accounted_bytes: 58,
                cri_metadata: None,
            }],
            vec![SourceEvent::EndOfFile {
                source_id: Some(sid),
            }],
            vec![SourceEvent::Data {
                bytes: Bytes::from_static(b"2024-01-01T00:00:00.000000000Z stdout F ial\"}\n"),
                source_id: Some(sid),
                accounted_bytes: 53,
                cri_metadata: None,
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatDecoder::cri(1024, stats.clone()),
            stats,
        );

        assert!(framed.poll().unwrap().is_empty());
        assert!(
            framed.sources.contains_key(&Some(sid)),
            "CRI P fragment should leave pending format state"
        );

        assert!(framed.poll().unwrap().is_empty());
        assert!(
            framed.sources.contains_key(&Some(sid)),
            "EOF must not remove pending CRI state"
        );

        let output = collect_data(framed.poll().unwrap());
        assert!(
            output.ends_with(b"\"msg\":\"partial\"}\n"),
            "unexpected CRI output after EOF: {}",
            String::from_utf8_lossy(&output)
        );
    }
}
