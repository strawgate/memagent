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

use crate::diagnostics::ComponentStats;
use crate::filter_hints::FilterHints;
use crate::format::FormatDecoder;
use crate::input::{InputEvent, InputSource};
use crate::tail::ByteOffset;
use logfwd_core::checkpoint_tracker::CheckpointTracker;
use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::pipeline::SourceId;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Maximum remainder buffer size before discarding (prevents OOM on
/// input without newlines). Applied per source.
const MAX_REMAINDER_BYTES: usize = 2 * 1024 * 1024;

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
}

/// Wraps a raw [`InputSource`] with newline framing and format processing.
///
/// The inner source provides raw bytes (from file, TCP, UDP, etc.). This
/// wrapper splits on newlines, manages partial-line remainders across polls,
/// and runs format-specific processing (CRI extraction, passthrough, etc.).
/// The output is scanner-ready bytes.
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
    /// Spare buffer swapped in when out_buf is emitted, preserving capacity
    /// across polls without allocating.
    spare_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl FramedInput {
    pub fn new(
        inner: Box<dyn InputSource>,
        format: FormatDecoder,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            inner,
            format_template: format,
            sources: HashMap::new(),
            out_buf: Vec::with_capacity(64 * 1024),
            spare_buf: Vec::with_capacity(64 * 1024),
            stats,
        }
    }
}

impl InputSource for FramedInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let raw_events = self.inner.poll()?;
        if raw_events.is_empty() {
            return Ok(vec![]);
        }

        let mut result_events: Vec<InputEvent> = Vec::new();

        for event in raw_events {
            match event {
                InputEvent::Data { bytes, source_id } => {
                    self.stats.inc_bytes(bytes.len() as u64);

                    let key = source_id;
                    let n_bytes = bytes.len() as u64;

                    // Get or create per-source state.
                    let state = {
                        let template = &self.format_template;
                        self.sources.entry(key).or_insert_with(|| SourceState {
                            remainder: Vec::new(),
                            format: template.new_instance(),
                            tracker: CheckpointTracker::new(0),
                        })
                    };

                    // Prepend remainder from last poll for this source,
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
                    let last_newline_in_new_bytes = last_newline_pos.and_then(|pos| {
                        if pos >= remainder_prefix_len {
                            Some((pos - remainder_prefix_len) as u64)
                        } else {
                            // The last newline is inside the old remainder,
                            // not in the new bytes. From the tracker's
                            // perspective, the new bytes have no newline.
                            None
                        }
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
                                        source_id = ?key,
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
                                    source_id = ?key,
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
                    let state = self.sources.get_mut(&key).expect("just inserted");
                    state.format.process_lines(&chunk, &mut self.out_buf);

                    let line_count = memchr::memchr_iter(b'\n', &chunk).count();
                    self.stats.inc_lines(line_count as u64);

                    if !self.out_buf.is_empty() {
                        // Take out_buf's content, swap in spare_buf's capacity
                        // for next iteration. No allocation — the 64KB bounces
                        // between the two buffers.
                        let data = std::mem::take(&mut self.out_buf);
                        std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                        result_events.push(InputEvent::Data {
                            bytes: data,
                            source_id,
                        });
                    }
                }
                // Rotation/truncation: clear framing state + forward event.
                //
                // When source_id is known, clear only the affected source's
                // state. When unknown (None), clear all sources as a
                // conservative fallback.
                event @ (InputEvent::Rotated { source_id }
                | InputEvent::Truncated { source_id }) => {
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
                InputEvent::EndOfFile { source_id } => {
                    let keys_to_flush: Vec<Option<SourceId>> = match source_id {
                        Some(_) => vec![source_id],
                        None => self.sources.keys().copied().collect(),
                    };
                    for key in keys_to_flush {
                        if let Some(state) = self.sources.get_mut(&key) {
                            if !state.remainder.is_empty() {
                                let mut remainder = std::mem::take(&mut state.remainder);
                                remainder.push(b'\n');

                                self.out_buf.clear();
                                let state =
                                    self.sources.get_mut(&key).expect("just checked existence");
                                state.format.process_lines(&remainder, &mut self.out_buf);

                                self.stats.inc_lines(1);

                                // Remainder was flushed — update tracker so
                                // checkpointable_offset advances past the
                                // flushed bytes.
                                let state =
                                    self.sources.get_mut(&key).expect("just checked existence");
                                state.tracker.apply_remainder_consumed();

                                if !self.out_buf.is_empty() {
                                    let data = std::mem::take(&mut self.out_buf);
                                    std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                                    result_events.push(InputEvent::Data {
                                        bytes: data,
                                        source_id: key,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result_events)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn health(&self) -> ComponentHealth {
        self.inner.health()
    }

    fn apply_hints(&mut self, hints: &FilterHints) {
        self.inner.apply_hints(hints);
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

    fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) {
        self.inner.set_offset_by_source(source_id, offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// Mock input source for testing.
    struct MockSource {
        name: String,
        events: VecDeque<Vec<InputEvent>>,
        offsets: Vec<(SourceId, ByteOffset)>,
    }

    impl MockSource {
        fn new(batches: Vec<Vec<InputEvent>>) -> Self {
            Self {
                name: "mock".to_string(),
                events: batches.into(),
                offsets: vec![],
            }
        }

        fn from_chunks(chunks: Vec<&[u8]>) -> Self {
            Self::new(
                chunks
                    .into_iter()
                    .map(|c| {
                        vec![InputEvent::Data {
                            bytes: c.to_vec(),
                            source_id: None,
                        }]
                    })
                    .collect(),
            )
        }

        fn with_offsets(mut self, offsets: Vec<(SourceId, ByteOffset)>) -> Self {
            self.offsets = offsets;
            self
        }
    }

    impl InputSource for MockSource {
        fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
            Ok(self.events.pop_front().unwrap_or_default())
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
            self.offsets.clone()
        }
    }

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    fn collect_data(events: Vec<InputEvent>) -> Vec<u8> {
        let mut out = Vec::new();
        for e in events {
            if let InputEvent::Data { bytes, .. } = e {
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
    fn remainder_capped_at_max() {
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

        // Second poll: a newline terminates the preserved data.
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert!(
            !data2.is_empty(),
            "preserved overflow remainder must be emitted when a newline arrives"
        );
    }

    #[test]
    fn tail_after_newline_is_capped_at_max() {
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

        // Second poll: a newline terminates the preserved remainder — the line
        // is emitted (proves data was preserved, not silently dropped).
        let events2 = framed.poll().unwrap();
        let data2 = collect_data(events2);
        assert!(
            !data2.is_empty(),
            "preserved overflow remainder must be emitted when a newline arrives"
        );
    }

    #[test]
    fn rotated_clears_remainder_and_format() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: b"partial".to_vec(),
                source_id: None,
            }],
            vec![InputEvent::Rotated { source_id: None }],
            vec![InputEvent::Data {
                bytes: b"fresh\n".to_vec(),
                source_id: None,
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
                .any(|e| matches!(e, InputEvent::Rotated { .. }))
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
        assert_eq!(
            collect_data(events),
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"msg\":\"hello\"}\n"
        );
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
            vec![InputEvent::Data {
                bytes: b"no-newline".to_vec(),
                source_id: None,
            }],
            vec![InputEvent::EndOfFile { source_id: None }],
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

    /// Multiple records in a file where only the last one lacks a newline:
    /// all records must be emitted.
    #[test]
    fn eof_flushes_only_partial_remainder() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: b"complete\npartial".to_vec(),
                source_id: None,
            }],
            vec![InputEvent::EndOfFile { source_id: None }],
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
            vec![InputEvent::Data {
                bytes: b"line\n".to_vec(),
                source_id: None,
            }],
            vec![InputEvent::EndOfFile { source_id: None }],
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
                InputEvent::Data {
                    bytes: b"hello-from-A".to_vec(),
                    source_id: Some(sid_a),
                },
                InputEvent::Data {
                    bytes: b"hello-from-B".to_vec(),
                    source_id: Some(sid_b),
                },
            ],
            // Poll 2: complete the lines from each source
            vec![
                InputEvent::Data {
                    bytes: b"-done\n".to_vec(),
                    source_id: Some(sid_a),
                },
                InputEvent::Data {
                    bytes: b"-done\n".to_vec(),
                    source_id: Some(sid_b),
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
            if let InputEvent::Data { bytes, source_id } = e {
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
                InputEvent::Data {
                    bytes: b"partial-A".to_vec(),
                    source_id: Some(sid_a),
                },
                InputEvent::Data {
                    bytes: b"partial-B".to_vec(),
                    source_id: Some(sid_b),
                },
            ],
            // Truncation
            vec![InputEvent::Truncated { source_id: None }],
            // Fresh data from source A
            vec![InputEvent::Data {
                bytes: b"fresh-A\n".to_vec(),
                source_id: Some(sid_a),
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
        let source = MockSource::new(vec![vec![InputEvent::Data {
            bytes: b"hello\nwor".to_vec(),
            source_id: Some(sid),
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

        let source = MockSource::new(vec![vec![InputEvent::Data {
            bytes: b"complete\n".to_vec(),
            source_id: Some(sid),
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
            vec![InputEvent::Data {
                bytes: b"2024-01-15T10:30:00Z stdout P hello \n".to_vec(),
                source_id: Some(sid_a),
            }],
            // Source B: CRI full line (must NOT merge with A's partial)
            vec![InputEvent::Data {
                bytes: b"2024-01-15T10:30:01Z stderr F {\"msg\":\"world\"}\n".to_vec(),
                source_id: Some(sid_b),
            }],
            // Source A: CRI full line (completes A's partial)
            vec![InputEvent::Data {
                bytes: b"2024-01-15T10:30:02Z stdout F from-A\n".to_vec(),
                source_id: Some(sid_a),
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
        assert!(
            data2.starts_with(b"{\"_timestamp\":\"2024-01-15T10:30:01Z\""),
            "B's line should be emitted independently"
        );
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
            vec![InputEvent::Data {
                bytes: b"hello\nwor".to_vec(),
                source_id: Some(sid),
            }],
            // Second read: 3 bytes, newline at position 1 (the 'd\n')
            vec![InputEvent::Data {
                bytes: b"ld\n".to_vec(),
                source_id: Some(sid),
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
        let state = framed.sources.get(&Some(sid)).unwrap();
        assert_eq!(state.tracker.remainder_len(), 0);

        // Checkpoint should reflect: 12 - 0 = 12
        let cp = framed.checkpoint_data();
        assert_eq!(cp[0].1, ByteOffset(12));
    }
}
