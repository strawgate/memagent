//! Composable framing wrapper for input sources.
//!
//! `FramedInput` wraps any [`InputSource`] and handles newline framing
//! (remainder management across polls) and format processing (CRI, Auto,
//! passthrough). The pipeline receives scanner-ready bytes without knowing
//! about formats or line boundaries.

use crate::diagnostics::ComponentStats;
use crate::filter_hints::FilterHints;
use crate::format::FormatProcessor;
use crate::input::{InputEvent, InputSource};
use crate::tail::ByteOffset;
use logfwd_core::pipeline::SourceId;
use std::io;
use std::sync::Arc;

/// Maximum remainder buffer size before discarding (prevents OOM on
/// input without newlines).
const MAX_REMAINDER_BYTES: usize = 2 * 1024 * 1024;

/// Wraps a raw [`InputSource`] with newline framing and format processing.
///
/// The inner source provides raw bytes (from file, TCP, UDP, etc.). This
/// wrapper splits on newlines, manages partial-line remainders across polls,
/// and runs format-specific processing (CRI extraction, passthrough, etc.).
/// The output is scanner-ready bytes.
pub struct FramedInput {
    inner: Box<dyn InputSource>,
    format: FormatProcessor,
    remainder: Vec<u8>,
    out_buf: Vec<u8>,
    /// Spare buffer swapped in when out_buf is emitted, preserving capacity
    /// across polls without allocating.
    spare_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl FramedInput {
    pub fn new(
        inner: Box<dyn InputSource>,
        format: FormatProcessor,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            inner,
            format,
            remainder: Vec::new(),
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
                InputEvent::Data { bytes } => {
                    self.stats.inc_bytes(bytes.len() as u64);

                    // Prepend remainder from last poll, reusing the Vec's capacity.
                    let mut chunk = std::mem::take(&mut self.remainder);
                    chunk.extend_from_slice(&bytes);

                    // Find last newline — everything before is complete lines,
                    // everything after is the new remainder.
                    match memchr::memrchr(b'\n', &chunk) {
                        Some(pos) => {
                            if pos + 1 < chunk.len() {
                                // Move tail to remainder without allocating.
                                self.remainder = chunk.split_off(pos + 1);
                                if self.remainder.len() > MAX_REMAINDER_BYTES {
                                    self.stats.inc_parse_errors(1);
                                    self.remainder.clear();
                                    self.format.reset();
                                }
                                chunk.truncate(pos + 1);
                            }
                        }
                        None => {
                            // No newline at all — entire chunk is remainder.
                            self.remainder = chunk;
                            if self.remainder.len() > MAX_REMAINDER_BYTES {
                                self.stats.inc_parse_errors(1);
                                self.remainder.clear();
                                self.format.reset();
                            }
                            continue;
                        }
                    }

                    // Process complete lines through format handler.
                    self.out_buf.clear();
                    self.format.process_lines(&chunk, &mut self.out_buf);

                    let line_count = memchr::memchr_iter(b'\n', &chunk).count();
                    self.stats.inc_lines(line_count as u64);

                    if !self.out_buf.is_empty() {
                        // Take out_buf's content, swap in spare_buf's capacity
                        // for next iteration. No allocation — the 64KB bounces
                        // between the two buffers.
                        let data = std::mem::take(&mut self.out_buf);
                        std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                        result_events.push(InputEvent::Data { bytes: data });
                    }
                }
                // Rotation/truncation: clear framing state + forward event.
                event @ (InputEvent::Rotated | InputEvent::Truncated) => {
                    self.remainder.clear();
                    self.format.reset();
                    result_events.push(event);
                }
                // End of file: flush any partial-line remainder.
                //
                // When a file ends without a trailing newline the last record
                // sits in `self.remainder` indefinitely.  Appending a synthetic
                // `\n` lets the format processor treat it as a complete line so
                // it reaches the scanner instead of being silently dropped.
                InputEvent::EndOfFile => {
                    if !self.remainder.is_empty() {
                        self.remainder.push(b'\n');
                        let chunk = std::mem::take(&mut self.remainder);

                        self.out_buf.clear();
                        self.format.process_lines(&chunk, &mut self.out_buf);

                        self.stats.inc_lines(1);

                        if !self.out_buf.is_empty() {
                            let data = std::mem::take(&mut self.out_buf);
                            std::mem::swap(&mut self.out_buf, &mut self.spare_buf);
                            result_events.push(InputEvent::Data { bytes: data });
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

    fn apply_hints(&mut self, hints: &FilterHints) {
        self.inner.apply_hints(hints);
    }

    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        self.inner.checkpoint_data()
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
    }

    impl MockSource {
        fn new(batches: Vec<Vec<InputEvent>>) -> Self {
            Self {
                name: "mock".to_string(),
                events: batches.into(),
            }
        }

        fn from_chunks(chunks: Vec<&[u8]>) -> Self {
            Self::new(
                chunks
                    .into_iter()
                    .map(|c| vec![InputEvent::Data { bytes: c.to_vec() }])
                    .collect(),
            )
        }
    }

    impl InputSource for MockSource {
        fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
            Ok(self.events.pop_front().unwrap_or_default())
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    fn collect_data(events: Vec<InputEvent>) -> Vec<u8> {
        let mut out = Vec::new();
        for e in events {
            if let InputEvent::Data { bytes } = e {
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
            FormatProcessor::passthrough(Arc::clone(&stats)),
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
            FormatProcessor::passthrough(Arc::clone(&stats)),
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
            FormatProcessor::passthrough(Arc::clone(&stats)),
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
        // Send > 2 MiB without a newline
        let big = vec![b'x'; MAX_REMAINDER_BYTES + 1];
        let source = MockSource::from_chunks(vec![&big]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events = framed.poll().unwrap();
        assert!(collect_data(events).is_empty());
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn tail_after_newline_is_capped_at_max() {
        let stats = make_stats();
        let mut chunk = b"ok\n".to_vec();
        chunk.extend(vec![b'x'; MAX_REMAINDER_BYTES + 1]);
        let source = MockSource::from_chunks(vec![&chunk]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::passthrough(Arc::clone(&stats)),
            Arc::clone(&stats),
        );

        let events = framed.poll().unwrap();
        assert_eq!(collect_data(events), b"ok\n");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn rotated_clears_remainder_and_format() {
        let stats = make_stats();
        let source = MockSource::new(vec![
            vec![InputEvent::Data {
                bytes: b"partial".to_vec(),
            }],
            vec![InputEvent::Rotated],
            vec![InputEvent::Data {
                bytes: b"fresh\n".to_vec(),
            }],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::passthrough(Arc::clone(&stats)),
            stats,
        );

        // Partial goes to remainder
        let _ = framed.poll().unwrap();

        // Rotation clears remainder
        let events2 = framed.poll().unwrap();
        assert!(events2.iter().any(|e| matches!(e, InputEvent::Rotated)));

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
            FormatProcessor::cri(2 * 1024 * 1024, Arc::clone(&stats)),
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
            FormatProcessor::passthrough(Arc::clone(&stats)),
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
                FormatProcessor::passthrough(Arc::clone(&stats2)),
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
            }],
            vec![InputEvent::EndOfFile],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::passthrough(stats.clone()),
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
            }],
            vec![InputEvent::EndOfFile],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::passthrough(stats.clone()),
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
            }],
            vec![InputEvent::EndOfFile],
        ]);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::passthrough(stats.clone()),
            stats,
        );

        let events1 = framed.poll().unwrap();
        assert_eq!(collect_data(events1), b"line\n");

        let events2 = framed.poll().unwrap();
        assert!(collect_data(events2).is_empty());
    }
}
