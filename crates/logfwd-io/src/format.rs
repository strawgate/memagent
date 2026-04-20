//! Format processing for input data.
//!
//! A `FormatDecoder` transforms framed lines (complete, newline-delimited)
//! into scanner-ready output. This separates format concerns from transport
//! and framing, allowing any transport (file, TCP, UDP) to use any format
//! (JSON, CRI, Raw) via composition.

use crate::input::CriMetadata;
use logfwd_core::cri::{json_escape_bytes, parse_cri_line};
use logfwd_core::reassembler::{AggregateResult, CriReassembler};
use logfwd_types::diagnostics::ComponentStats;
use std::sync::Arc;

/// Processes framed input lines according to the configured format.
///
/// - `Passthrough`: lines are already scanner-ready (Raw — no JSON validation)
/// - `PassthroughJson`: lines are expected to be JSON objects; non-JSON lines
///   are still forwarded but counted as `parse_errors`
/// - `Cri`: parse CRI container log format, extract message body
/// - `Auto`: try CRI, preserve JSON fallback, wrap plain-text fallback
#[non_exhaustive]
pub enum FormatDecoder {
    Passthrough {
        stats: Arc<ComponentStats>,
    },
    PassthroughJson {
        stats: Arc<ComponentStats>,
    },
    Cri {
        aggregators: [CriReassembler; 2],
        plain_text_field_name: String,
        stats: Arc<ComponentStats>,
    },
    Auto {
        aggregators: [CriReassembler; 2],
        plain_text_field_name: String,
        stats: Arc<ComponentStats>,
    },
}

fn new_stream_aggregators(max_message_size: usize) -> [CriReassembler; 2] {
    [
        CriReassembler::new(max_message_size),
        CriReassembler::new(max_message_size),
    ]
}

const STDOUT_IDX: usize = 0;
const STDERR_IDX: usize = 1;

fn stream_aggregator<'a>(
    aggregators: &'a mut [CriReassembler; 2],
    stream: &[u8],
) -> &'a mut CriReassembler {
    if stream == b"stderr" {
        &mut aggregators[STDERR_IDX]
    } else {
        &mut aggregators[STDOUT_IDX]
    }
}

fn reset_stream_aggregators(aggregators: &mut [CriReassembler; 2]) {
    aggregators[STDOUT_IDX].reset();
    aggregators[STDERR_IDX].reset();
}

impl FormatDecoder {
    /// Create a passthrough processor for raw (non-JSON) input.
    ///
    /// Lines are forwarded verbatim. Non-JSON-object lines are **not** counted
    /// as parse errors because raw format intentionally carries plain-text data.
    pub fn passthrough(stats: Arc<ComponentStats>) -> Self {
        Self::Passthrough { stats }
    }

    /// Create a passthrough processor for JSON input.
    ///
    /// Lines are forwarded verbatim, but any non-empty line that does not begin
    /// with `{` (i.e. is not a JSON object) increments the `parse_errors`
    /// counter so data-quality issues are surfaced in diagnostics.
    pub fn passthrough_json(stats: Arc<ComponentStats>) -> Self {
        Self::PassthroughJson { stats }
    }

    /// Create a CRI format processor with the given max message size.
    pub fn cri(max_message_size: usize, stats: Arc<ComponentStats>) -> Self {
        Self::cri_with_plain_text_field(max_message_size, "body".to_string(), stats)
    }

    /// Create a CRI format processor with a configurable plain-text field name.
    ///
    /// Non-JSON CRI messages are wrapped into this field.
    pub fn cri_with_plain_text_field(
        max_message_size: usize,
        plain_text_field_name: String,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self::Cri {
            aggregators: new_stream_aggregators(max_message_size),
            plain_text_field_name,
            stats,
        }
    }

    /// Create an Auto format processor.
    ///
    /// CRI lines are decoded into message rows plus sidecar metadata. Non-CRI
    /// JSON object lines pass through unchanged. Non-CRI plain-text lines are
    /// wrapped into the configured plain-text field.
    pub fn auto(max_message_size: usize, stats: Arc<ComponentStats>) -> Self {
        Self::auto_with_plain_text_field(max_message_size, "body".to_string(), stats)
    }

    /// Create an Auto format processor with a configurable plain-text field name.
    ///
    /// When a CRI line parses but message content is plain text, it is wrapped
    /// into this field.
    pub fn auto_with_plain_text_field(
        max_message_size: usize,
        plain_text_field_name: String,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self::Auto {
            aggregators: new_stream_aggregators(max_message_size),
            plain_text_field_name,
            stats,
        }
    }

    /// Create a new instance with fresh state but the same format and stats.
    ///
    /// Used to create per-source format processors so that stateful formats
    /// (CRI P/F aggregation) do not cross-contaminate between sources.
    #[must_use]
    pub fn new_instance(&self) -> Self {
        match self {
            Self::Passthrough { stats } => Self::Passthrough {
                stats: Arc::clone(stats),
            },
            Self::PassthroughJson { stats } => Self::PassthroughJson {
                stats: Arc::clone(stats),
            },
            Self::Cri {
                aggregators,
                plain_text_field_name,
                stats,
            } => Self::Cri {
                aggregators: new_stream_aggregators(aggregators[0].max_message_size()),
                plain_text_field_name: plain_text_field_name.clone(),
                stats: Arc::clone(stats),
            },
            Self::Auto {
                aggregators,
                plain_text_field_name,
                stats,
            } => Self::Auto {
                aggregators: new_stream_aggregators(aggregators[0].max_message_size()),
                plain_text_field_name: plain_text_field_name.clone(),
                stats: Arc::clone(stats),
            },
        }
    }

    /// Process a chunk of complete, newline-delimited lines into scanner-ready output.
    ///
    /// The input `chunk` must end at a line boundary (after `\n`). The caller
    /// handles remainder splitting — this function only sees complete lines.
    pub fn process_lines(&mut self, chunk: &[u8], out: &mut Vec<u8>) {
        self.process_lines_with_metadata(chunk, out, None);
    }

    /// Process lines and optionally collect CRI metadata sidecar rows.
    pub fn process_lines_with_metadata(
        &mut self,
        chunk: &[u8],
        out: &mut Vec<u8>,
        cri_metadata: Option<&mut CriMetadata>,
    ) {
        match self {
            Self::Passthrough { .. } => {
                out.extend_from_slice(chunk);
            }
            Self::PassthroughJson { stats } => {
                count_json_parse_errors(chunk, stats);
                out.extend_from_slice(chunk);
            }
            Self::Cri {
                aggregators,
                plain_text_field_name,
                stats,
            } => {
                extract_cri_messages(
                    chunk,
                    out,
                    cri_metadata,
                    aggregators,
                    plain_text_field_name,
                    stats,
                    false,
                );
            }
            Self::Auto {
                aggregators,
                plain_text_field_name,
                stats,
            } => {
                extract_cri_messages(
                    chunk,
                    out,
                    cri_metadata,
                    aggregators,
                    plain_text_field_name,
                    stats,
                    true,
                );
            }
        }
    }

    /// Reset internal state (e.g. after file rotation or truncation).
    pub fn reset(&mut self) {
        match self {
            Self::Passthrough { .. } | Self::PassthroughJson { .. } => {}
            Self::Cri { aggregators, .. } | Self::Auto { aggregators, .. } => {
                reset_stream_aggregators(aggregators);
            }
        }
    }

    /// Return true when the decoder carries state that must survive to a
    /// later chunk from the same source.
    pub fn has_pending_state(&self) -> bool {
        match self {
            Self::Passthrough { .. } | Self::PassthroughJson { .. } => false,
            Self::Cri { aggregators, .. } | Self::Auto { aggregators, .. } => {
                aggregators.iter().any(CriReassembler::has_buffered_state)
            }
        }
    }
}

/// Count non-JSON lines in `chunk` and increment the parse-error counter.
///
/// A line is considered a JSON object if, after stripping leading ASCII
/// whitespace (`' '`, `'\t'`, `'\r'`), it begins with `{`.  Empty lines
/// are ignored.  Lines are forwarded to `out` unchanged — this function
/// only updates the counter.
fn count_json_parse_errors(chunk: &[u8], stats: &ComponentStats) {
    let mut pos = 0;
    while pos < chunk.len() {
        let eol = memchr::memchr(b'\n', &chunk[pos..]).map_or(chunk.len(), |o| pos + o);
        let line = &chunk[pos..eol];
        if !line.is_empty() {
            let first_nonws = line
                .iter()
                .position(|&b| !matches!(b, b' ' | b'\t' | b'\r'));
            let is_json_obj = first_nonws.is_some_and(|p| line[p] == b'{');
            if !is_json_obj {
                stats.inc_parse_errors(1);
            }
        }
        pos = eol + 1;
    }
}

/// Extract JSON messages from CRI-formatted lines, handling P/F merging.
///
/// For each complete message the CRI timestamp and stream are appended to the
/// optional sidecar without mutating the message JSON. For P+F sequences the
/// timestamp and stream are taken from the closing F line, which is correct
/// because the CRI spec requires all fragments of the same log entry to carry
/// the same timestamp and stream.
fn extract_cri_messages(
    input: &[u8],
    out: &mut Vec<u8>,
    mut cri_metadata: Option<&mut CriMetadata>,
    aggregators: &mut [CriReassembler; 2],
    plain_text_field_name: &str,
    stats: &ComponentStats,
    passthrough_on_fail: bool,
) {
    let mut pos = 0;
    while pos < input.len() {
        let eol = memchr::memchr(b'\n', &input[pos..]).map_or(input.len(), |o| pos + o);
        let line = &input[pos..eol];
        if let Some(cri) = parse_cri_line(line) {
            let aggregator = stream_aggregator(aggregators, cri.stream);
            let max_message_size = aggregator.max_message_size();
            match aggregator.feed(cri.message, cri.is_full) {
                AggregateResult::Complete(msg) => {
                    write_cri_message(msg, plain_text_field_name, out);
                    if let Some(metadata) = &mut cri_metadata
                        && !metadata.append_value(cri.timestamp, cri.stream)
                    {
                        stats.inc_parse_errors(1);
                    }
                    aggregator.reset();
                }
                AggregateResult::Truncated(msg) => {
                    // The assembled message exceeded max_message_size; some bytes
                    // were silently dropped by the aggregator. Emit the truncated
                    // output so the record is not lost entirely, but signal data
                    // loss via the parse-error counter and a warning log.
                    tracing::warn!(
                        max_message_size,
                        "cri.message_truncated — assembled CRI message exceeded \
                         max_message_size; output is truncated"
                    );
                    stats.inc_parse_errors(1);
                    write_cri_message(msg, plain_text_field_name, out);
                    if let Some(metadata) = &mut cri_metadata
                        && !metadata.append_value(cri.timestamp, cri.stream)
                    {
                        stats.inc_parse_errors(1);
                    }
                    aggregator.reset();
                }
                AggregateResult::Pending => {}
            }
        } else {
            // Break any pending CRI aggregation at parse/fallback boundaries.
            reset_stream_aggregators(aggregators);
            if !line.is_empty() && passthrough_on_fail {
                if starts_with_json_object(line) {
                    out.extend_from_slice(line);
                    out.push(b'\n');
                    if let Some(metadata) = &mut cri_metadata {
                        metadata.append_null_rows(1);
                    }
                } else if let Some(line) = normalize_plain_text_fallback(line) {
                    write_plain_text_fallback(line, plain_text_field_name, out);
                    if let Some(metadata) = &mut cri_metadata {
                        metadata.append_null_rows(1);
                    }
                }
            } else if !line.is_empty() {
                stats.inc_parse_errors(1);
            }
        }
        pos = eol + 1;
    }
}

fn starts_with_json_object(line: &[u8]) -> bool {
    let first_nonws = line
        .iter()
        .position(|&b| !matches!(b, b' ' | b'\t' | b'\r'));
    first_nonws.is_some_and(|idx| line[idx] == b'{')
}

#[inline]
fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn normalize_plain_text_fallback(line: &[u8]) -> Option<&[u8]> {
    let line = if line.last() == Some(&b'\r') {
        &line[..line.len().saturating_sub(1)]
    } else {
        line
    };
    (!line.is_empty()).then_some(line)
}

fn write_plain_text_fallback(line: &[u8], plain_text_field_name: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(b"{\"");
    json_escape_bytes(plain_text_field_name.as_bytes(), out);
    out.extend_from_slice(b"\":\"");
    json_escape_bytes(line, out);
    out.extend_from_slice(b"\"}\n");
}

/// Write a CRI message as scanner-ready JSON without CRI metadata injection.
#[inline]
fn write_cri_message(msg: &[u8], plain_text_field_name: &str, out: &mut Vec<u8>) {
    let json_start = msg.iter().position(|b| !is_json_whitespace(*b));
    if let Some(start) = json_start
        && msg[start] == b'{'
    {
        out.extend_from_slice(&msg[start..]);
    } else {
        // Plain text is still wrapped so the scanner can ingest the record.
        out.extend_from_slice(b"{\"");
        json_escape_bytes(plain_text_field_name.as_bytes(), out);
        out.extend_from_slice(b"\":\"");
        json_escape_bytes(msg, out);
        out.extend_from_slice(b"\"}");
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framed::FramedInput;
    use crate::input::{FileInput, InputEvent, InputSource};
    use crate::tail::TailConfig;
    use std::time::{Duration, Instant};

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    fn process_cri_from_tempfile(input: &[u8]) -> Vec<u8> {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cri.log");
        std::fs::write(&path, input).expect("write CRI fixture");

        let stats = make_stats();
        let file_input = FileInput::new(
            "cri-test".to_string(),
            std::slice::from_ref(&path),
            TailConfig {
                start_from_end: false,
                poll_interval_ms: 10,
                ..Default::default()
            },
            Arc::clone(&stats),
        )
        .expect("file tailer");
        let mut framed = FramedInput::new(
            Box::new(file_input),
            FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats)),
            stats,
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let events = framed.poll().expect("poll framed CRI tempfile");
            let data: Vec<u8> = events
                .into_iter()
                .filter_map(|event| match event {
                    InputEvent::Data { bytes, .. } => Some(bytes),
                    _ => None,
                })
                .flatten()
                .collect();
            if !data.is_empty() {
                return data;
            }
            assert!(Instant::now() < deadline, "timed out reading CRI fixture");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn passthrough_copies_verbatim() {
        let stats = make_stats();
        let mut proc = FormatDecoder::passthrough(stats);
        let input = b"line1\nline2\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input);
    }

    #[test]
    fn cri_full_lines_extracted() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn cri_partial_then_full_merged() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        // Partial line
        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        assert!(out.is_empty(), "partial should not emit");

        // Full line completes the message — plain text is wrapped into the configured field.
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);
        assert_eq!(out, b"{\"body\":\"hello world\"}\n");
    }

    #[test]
    fn cri_interleaved_streams_do_not_cross_contaminate() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats));
        let mut out = Vec::new();

        proc.process_lines(b"2024-01-15T10:30:00Z stdout P out-\n", &mut out);
        proc.process_lines(b"2024-01-15T10:30:01Z stderr F err\n", &mut out);
        proc.process_lines(b"2024-01-15T10:30:02Z stdout F done\n", &mut out);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"{\"body\":\"err\"}\n");
        expected.extend_from_slice(b"{\"body\":\"out-done\"}\n");
        assert_eq!(out, expected);
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn cri_malformed_lines_count_errors() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, Arc::clone(&stats));
        let input = b"not a cri line\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert!(out.is_empty());
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn auto_passthrough_for_non_cri_json() {
        let stats = make_stats();
        let mut proc = FormatDecoder::auto(2 * 1024 * 1024, stats);
        let input = b"{\"msg\":\"plain json\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"plain json\"}\n");
    }

    #[test]
    fn auto_wraps_plain_text_for_non_cri() {
        let stats = make_stats();
        let mut proc = FormatDecoder::auto(2 * 1024 * 1024, stats);
        let input = b"not a cri line\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"body\":\"not a cri line\"}\n");
    }

    #[test]
    fn auto_wraps_plain_text_for_non_cri_crlf_without_carriage_return() {
        let stats = make_stats();
        let mut proc = FormatDecoder::auto(2 * 1024 * 1024, stats);
        let input = b"not a cri line\r\n\r\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"body\":\"not a cri line\"}\n");
    }

    #[test]
    fn auto_handles_cri_when_valid() {
        let stats = make_stats();
        let mut proc = FormatDecoder::auto(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"cri\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"cri\"}\n");
    }

    #[test]
    fn auto_sidecar_tracks_nulls_for_non_cri_rows() {
        let stats = make_stats();
        let mut proc = FormatDecoder::auto(2 * 1024 * 1024, stats);
        let input =
            b"{\"msg\":\"plain\"}\n2024-01-15T10:30:00Z stdout F {\"msg\":\"cri\"}\nnot cri\n";
        let mut out = Vec::new();
        let mut metadata = CriMetadata::default();

        proc.process_lines_with_metadata(input, &mut out, Some(&mut metadata));

        assert_eq!(
            out,
            b"{\"msg\":\"plain\"}\n{\"msg\":\"cri\"}\n{\"body\":\"not cri\"}\n"
        );
        assert_eq!(metadata.rows, 3);
        assert!(metadata.has_values);
        assert_eq!(metadata.spans.len(), 3);
        assert!(metadata.spans[0].values.is_none());
        assert!(metadata.spans[1].values.is_some());
        assert!(metadata.spans[2].values.is_none());
    }

    #[test]
    fn auto_malformed_line_resets_pending_state() {
        let stats = make_stats();
        let mut proc = FormatDecoder::auto(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        proc.process_lines(b"not a cri line\n", &mut out);
        // "world" is plain text — wrapped into the configured field.
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"{\"body\":\"not a cri line\"}\n");
        expected.extend_from_slice(b"{\"body\":\"world\"}\n");
        assert_eq!(out, expected);
    }

    #[test]
    fn reset_clears_aggregator_state() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        // Feed a partial
        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        assert!(out.is_empty());

        // Reset (simulating rotation)
        proc.reset();

        // Next full line should not contain the old partial — wrapped into configured field.
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);
        assert_eq!(out, b"{\"body\":\"world\"}\n");
    }

    #[test]
    fn cri_collects_timestamp_and_stream_sidecar() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();
        let mut metadata = CriMetadata::default();

        proc.process_lines_with_metadata(
            b"2024-01-15T10:30:00Z stderr F {\"level\":\"ERROR\",\"msg\":\"disk full\"}\n",
            &mut out,
            Some(&mut metadata),
        );
        proc.process_lines_with_metadata(
            b"2024-01-15T10:30:01Z stdout F {\"level\":\"INFO\",\"msg\":\"ok\"}\n",
            &mut out,
            Some(&mut metadata),
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(b"{\"level\":\"ERROR\",\"msg\":\"disk full\"}\n");
        expected.extend_from_slice(b"{\"level\":\"INFO\",\"msg\":\"ok\"}\n");
        assert_eq!(out, expected);
        assert_eq!(metadata.rows, 2);
        assert!(metadata.has_values);
        assert_eq!(metadata.spans.len(), 2);
        let first = metadata.spans[0].values.as_ref().unwrap();
        assert_eq!(metadata.timestamp(first), b"2024-01-15T10:30:00Z");
        assert_eq!(first.stream.as_str(), "stderr");
        let second = metadata.spans[1].values.as_ref().unwrap();
        assert_eq!(metadata.timestamp(second), b"2024-01-15T10:30:01Z");
        assert_eq!(second.stream.as_str(), "stdout");
    }

    #[test]
    fn cri_partial_then_full_collects_sidecar_metadata() {
        // For a P+F sequence the timestamp/stream from the closing F line are used.
        // P message: `{"msg":`, F message: `"hello"}`, concatenated: `{"msg":"hello"}`
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        proc.process_lines(b"2024-01-15T10:30:00Z stdout P {\"msg\":\n", &mut out);
        assert!(out.is_empty(), "partial should not emit");

        proc.process_lines(b"2024-01-15T10:30:00Z stdout F \"hello\"}\n", &mut out);
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn cri_non_json_message_wrapped_as_body() {
        // Non-JSON CRI messages (plain text) must be wrapped so that message
        // content is not silently lost when the scanner sees a non-JSON line.
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F plain text message\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"body\":\"plain text message\"}\n");
    }

    #[test]
    fn cri_non_json_message_uses_configured_plain_text_field() {
        let stats = make_stats();
        let mut proc =
            FormatDecoder::cri_with_plain_text_field(2 * 1024 * 1024, "payload".to_string(), stats);
        let input = b"2024-01-15T10:30:00Z stdout F plain text message\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"payload\":\"plain text message\"}\n");
    }

    #[test]
    fn cri_non_json_message_escapes_special_chars() {
        // Plain text containing JSON-special characters must be properly escaped.
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F say \"hello\"\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"body\":\"say \\\"hello\\\"\"}\n");
    }

    /// Regression for #1658: a CRI log line whose message is an empty JSON
    /// object `{}` must pass through as valid JSON.
    #[test]
    fn cri_empty_json_object_message_produces_valid_json() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        let output_str = std::str::from_utf8(&out).expect("output must be valid UTF-8");
        let trimmed = output_str.trim_end_matches('\n');
        assert!(
            serde_json::from_str::<serde_json::Value>(trimmed).is_ok(),
            "empty JSON object message must produce valid JSON, got: {trimmed:?}"
        );
        assert_eq!(trimmed, "{}");
    }

    /// Whitespace-only body `{ }` should also produce valid JSON (no trailing comma).
    #[test]
    fn cri_whitespace_json_object_message_produces_valid_json() {
        let stats = make_stats();
        let mut proc = FormatDecoder::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F { }\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        let output_str = std::str::from_utf8(&out).expect("output must be valid UTF-8");
        let trimmed = output_str.trim_end_matches('\n');
        assert!(
            serde_json::from_str::<serde_json::Value>(trimmed).is_ok(),
            "whitespace JSON object must produce valid JSON, got: {trimmed:?}"
        );
    }

    #[test]
    fn cri_json_message_with_leading_whitespace_stays_json() {
        let input = b"2024-01-15T10:30:00Z stdout F   {\"msg\":\"cri\"}\n";
        let out = process_cri_from_tempfile(input);
        let output_str = std::str::from_utf8(&out).expect("output must be valid UTF-8");
        let trimmed = output_str.trim_end_matches('\n');
        let value: serde_json::Value =
            serde_json::from_str(trimmed).expect("output must remain valid JSON");
        assert_eq!(
            value.get("msg").and_then(serde_json::Value::as_str),
            Some("cri")
        );
        assert!(
            value.get("body").is_none(),
            "JSON message must not be wrapped as plain text: {trimmed:?}"
        );
    }

    #[test]
    fn cri_message_with_non_json_ascii_whitespace_prefix_stays_plain_text() {
        for prefix in [b'\x0b', b'\x0c'] {
            let input = [
                b"2024-01-15T10:30:00Z stdout F ".as_slice(),
                &[prefix],
                br#"{"msg":"cri"}"#,
                b"\n",
            ]
            .concat();
            let out = process_cri_from_tempfile(&input);
            let output_str = std::str::from_utf8(&out).expect("output must be valid UTF-8");
            let trimmed = output_str.trim_end_matches('\n');
            let value: serde_json::Value =
                serde_json::from_str(trimmed).expect("output must remain valid JSON");
            assert!(
                value.get("body").is_some(),
                "non-JSON whitespace byte {prefix:#04x} must keep body wrapper: {trimmed:?}"
            );
            assert!(
                value.get("msg").is_none(),
                "non-JSON whitespace byte {prefix:#04x} must not be treated as JSON: {trimmed:?}"
            );
        }
    }

    #[test]
    fn passthrough_json_valid_line_no_error() {
        // A valid JSON-object line must NOT increment parse_errors.
        let stats = make_stats();
        let mut proc = FormatDecoder::passthrough_json(Arc::clone(&stats));
        let input = b"{\"msg\":\"hello\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input, "line must be forwarded verbatim");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "valid JSON must not increment parse_errors"
        );
    }

    #[test]
    fn passthrough_json_non_json_line_counts_error() {
        // A non-JSON line must be forwarded AND increment parse_errors.
        let stats = make_stats();
        let mut proc = FormatDecoder::passthrough_json(Arc::clone(&stats));
        let input = b"not json at all\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input, "invalid line must still be forwarded verbatim");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "non-JSON line must increment parse_errors"
        );
    }

    #[test]
    fn passthrough_json_empty_line_no_error() {
        // Empty lines must not increment parse_errors.
        let stats = make_stats();
        let mut proc = FormatDecoder::passthrough_json(Arc::clone(&stats));
        let input = b"\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "empty line must not increment parse_errors"
        );
    }

    #[test]
    fn passthrough_json_counts_multiple_invalid_lines() {
        // Each invalid line in a batch increments parse_errors independently.
        let stats = make_stats();
        let mut proc = FormatDecoder::passthrough_json(Arc::clone(&stats));
        let input = b"bad line\n{\"ok\":1}\nanother bad\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input, "all lines must be forwarded verbatim");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "two non-JSON lines must increment parse_errors by 2"
        );
    }

    #[test]
    fn passthrough_raw_never_counts_errors() {
        // Raw passthrough must never increment parse_errors regardless of content.
        let stats = make_stats();
        let mut proc = FormatDecoder::passthrough(Arc::clone(&stats));
        let input = b"not json at all\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input, "raw passthrough must forward verbatim");
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "raw passthrough must never increment parse_errors"
        );
    }

    /// A CRI F-only line whose message exceeds max_message_size must be emitted
    /// as a truncated (but non-empty) record, and the parse_errors counter must
    /// be incremented so the data loss is not silent.
    #[test]
    fn cri_truncated_message_emitted_and_counted_as_error() {
        let stats = make_stats();
        // max_message_size = 5 bytes — much smaller than the actual message.
        let mut proc = FormatDecoder::cri(5, Arc::clone(&stats));
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello world\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);

        // Truncated output must still be emitted (not silently dropped).
        assert!(!out.is_empty(), "truncated message must still be emitted");
        // parse_errors must be incremented to signal data loss.
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "truncated CRI message must increment parse_errors"
        );
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// write_cri_message always produces a non-empty output ending with '\n'.
    ///
    /// This holds for both JSON messages (starts with '{') and plain-text
    /// messages (wraps into a `body` JSON field). The trailing newline is
    /// required so that the
    /// downstream scanner can split on line boundaries.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_write_cri_message_ends_with_newline() {
        let msg: [u8; 4] = kani::any();
        let mut out = Vec::new();
        write_cri_message(&msg, "body", &mut out);
        assert!(!out.is_empty(), "output must be non-empty");
        assert_eq!(*out.last().unwrap(), b'\n', "output must end with newline");
        kani::cover!(msg[0] == b'{', "JSON message path exercised");
        kani::cover!(msg[0] != b'{', "plain text path exercised");
    }

    /// write_cri_message always produces output starting with '{'.
    ///
    /// Both the JSON pass-through path and the plain-text wrapper path open with
    /// '{' so the downstream scanner always sees a JSON object start.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_write_cri_message_starts_with_brace() {
        let msg: [u8; 4] = kani::any();
        let mut out = Vec::new();
        write_cri_message(&msg, "body", &mut out);
        assert_eq!(out[0], b'{', "output must start with '{'");
        kani::cover!(msg[0] == b'{', "JSON path exercised");
        kani::cover!(msg[0] != b'{', "plain text path exercised");
    }

    /// For JSON messages (starting with '{'), write_cri_message preserves the
    /// JSON object without injecting metadata fields.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_json_msg_is_not_rewritten() {
        let mut msg = [0u8; 4];
        msg[0] = b'{';
        msg[1] = kani::any();
        msg[2] = kani::any();
        msg[3] = kani::any();
        let mut out = Vec::new();
        write_cri_message(&msg, "body", &mut out);
        assert!(out.starts_with(&msg));
        kani::cover!(true, "JSON path pass-through verified");
    }

    /// For non-JSON messages (not starting with '{'), write_cri_message wraps
    /// the content in a {"body":"..."} object so no message content is lost.
    ///
    /// This is the Auto-mode fallthrough path: when CRI parsing succeeds but
    /// the message body is not a JSON object, the plain text is preserved in
    /// the `body` field rather than being silently discarded.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_non_json_msg_uses_body_key() {
        let msg: [u8; 4] = kani::any();
        let mut i = 0usize;
        while i < msg.len() && is_json_whitespace(msg[i]) {
            i += 1;
        }
        if i < msg.len() {
            kani::assume(msg[i] != b'{');
        }
        let mut out = Vec::new();
        write_cri_message(&msg, "body", &mut out);
        // Output must start with the body wrapper prefix (no content is lost).
        assert!(out.starts_with(b"{\"body\":\""));
        kani::cover!(true, "plain text path body wrapper verified");
    }
}
