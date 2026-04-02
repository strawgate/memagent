//! Format processing for input data.
//!
//! A `FormatProcessor` transforms framed lines (complete, newline-delimited)
//! into scanner-ready output. This separates format concerns from transport
//! and framing, allowing any transport (file, TCP, UDP) to use any format
//! (JSON, CRI, Raw) via composition.

use crate::diagnostics::ComponentStats;
use logfwd_core::aggregator::{AggregateResult, CriAggregator};
use logfwd_core::cri::{json_escape_bytes, parse_cri_line};
use std::sync::Arc;

/// Processes framed input lines according to the configured format.
///
/// - `Passthrough`: lines are already scanner-ready (JSON, Raw)
/// - `Cri`: parse CRI container log format, extract message body
/// - `Auto`: try CRI, fall through to passthrough on parse failure
#[non_exhaustive]
pub enum FormatProcessor {
    Passthrough,
    Cri {
        aggregator: CriAggregator,
        stats: Arc<ComponentStats>,
    },
    Auto {
        aggregator: CriAggregator,
        stats: Arc<ComponentStats>,
    },
}

impl FormatProcessor {
    /// Create a CRI format processor with the given max message size.
    pub fn cri(max_message_size: usize, stats: Arc<ComponentStats>) -> Self {
        Self::Cri {
            aggregator: CriAggregator::new(max_message_size),
            stats,
        }
    }

    /// Create an Auto format processor (tries CRI, falls through to passthrough).
    pub fn auto(max_message_size: usize, stats: Arc<ComponentStats>) -> Self {
        Self::Auto {
            aggregator: CriAggregator::new(max_message_size),
            stats,
        }
    }

    /// Process a chunk of complete, newline-delimited lines into scanner-ready output.
    ///
    /// The input `chunk` must end at a line boundary (after `\n`). The caller
    /// handles remainder splitting — this function only sees complete lines.
    pub fn process_lines(&mut self, chunk: &[u8], out: &mut Vec<u8>) {
        match self {
            Self::Passthrough => {
                out.extend_from_slice(chunk);
            }
            Self::Cri { aggregator, stats } => {
                extract_cri_messages(chunk, out, aggregator, stats, false);
            }
            Self::Auto { aggregator, stats } => {
                extract_cri_messages(chunk, out, aggregator, stats, true);
            }
        }
    }

    /// Reset internal state (e.g. after file rotation or truncation).
    pub fn reset(&mut self) {
        match self {
            Self::Passthrough => {}
            Self::Cri { aggregator, .. } | Self::Auto { aggregator, .. } => {
                aggregator.reset();
            }
        }
    }
}

/// Extract JSON messages from CRI-formatted lines, handling P/F merging.
///
/// For each complete message the CRI `_timestamp` and `_stream` fields are
/// injected into the JSON output (see [`inject_cri_metadata`]).  For P+F
/// sequences the timestamp and stream are taken from the closing F line,
/// which is correct because the CRI spec requires all fragments of the same
/// log entry to carry the same timestamp and stream.
fn extract_cri_messages(
    input: &[u8],
    out: &mut Vec<u8>,
    aggregator: &mut CriAggregator,
    stats: &ComponentStats,
    passthrough_on_fail: bool,
) {
    let mut pos = 0;
    while pos < input.len() {
        let eol = memchr::memchr(b'\n', &input[pos..]).map_or(input.len(), |o| pos + o);
        let line = &input[pos..eol];
        if let Some(cri) = parse_cri_line(line) {
            match aggregator.feed(cri.message, cri.is_full) {
                AggregateResult::Complete(msg) => {
                    inject_cri_metadata(msg, cri.timestamp, cri.stream, out);
                    aggregator.reset();
                }
                AggregateResult::Pending => {}
            }
        } else {
            // Break any pending CRI aggregation at parse/fallback boundaries.
            aggregator.reset();
            if !line.is_empty() && passthrough_on_fail {
                out.extend_from_slice(line);
                out.push(b'\n');
            } else if !line.is_empty() {
                stats.inc_parse_errors(1);
            }
        }
        pos = eol + 1;
    }
}

/// Inject `_timestamp` and `_stream` CRI metadata into a JSON message and
/// write the result with a trailing newline to `out`.
///
/// If `msg` starts with `{`, produces:
///   `{"_timestamp":"<ts>","_stream":"<stream>",<rest of msg>}\n`
///
/// Otherwise wraps the plain-text message so no content is lost:
///   `{"_timestamp":"<ts>","_stream":"<stream>","_raw":"<json-escaped msg>"}\n`
///
/// # Safety invariants
///
/// `timestamp` and `stream` are inserted without JSON-escaping.  Both are
/// taken directly from a successfully parsed CRI line: `timestamp` is an
/// RFC 3339 timestamp (digits, `-`, `T`, `Z`, `:`, `.`) and `stream` is
/// exactly `"stdout"` or `"stderr"` — neither can contain characters that
/// require escaping in a JSON string.
///
/// The `{` guard is a best-effort check identical to the one used by
/// `write_json_line` in `cri.rs`.  It is not a full JSON validator;
/// a CRI message that begins with `{` but is otherwise malformed will
/// produce an output line that is also malformed.  This is consistent
/// with the broader pipeline philosophy: the scanner validates the final
/// output rather than the format layer.
#[inline]
fn inject_cri_metadata(msg: &[u8], timestamp: &[u8], stream: &[u8], out: &mut Vec<u8>) {
    if msg.first() == Some(&b'{') {
        out.push(b'{');
        out.extend_from_slice(b"\"_timestamp\":\"");
        out.extend_from_slice(timestamp);
        out.extend_from_slice(b"\",\"_stream\":\"");
        out.extend_from_slice(stream);
        out.extend_from_slice(b"\",");
        out.extend_from_slice(&msg[1..]);
    } else {
        // Plain text: wrap as {"_timestamp":"...","_stream":"...","_raw":"<escaped>"}
        // so that message content is preserved and the scanner can ingest the record.
        out.extend_from_slice(b"{\"_timestamp\":\"");
        out.extend_from_slice(timestamp);
        out.extend_from_slice(b"\",\"_stream\":\"");
        out.extend_from_slice(stream);
        out.extend_from_slice(b"\",\"_raw\":\"");
        json_escape_bytes(msg, out);
        out.extend_from_slice(b"\"}");
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    #[test]
    fn passthrough_copies_verbatim() {
        let mut proc = FormatProcessor::Passthrough;
        let input = b"line1\nline2\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input);
    }

    #[test]
    fn cri_full_lines_extracted() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"msg\":\"hello\"}\n"
        );
    }

    #[test]
    fn cri_partial_then_full_merged() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        // Partial line
        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        assert!(out.is_empty(), "partial should not emit");

        // Full line completes the message — plain text → wrapped as _raw
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"_raw\":\"hello world\"}\n"
        );
    }

    #[test]
    fn cri_malformed_lines_count_errors() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, Arc::clone(&stats));
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
    fn auto_passthrough_for_non_cri() {
        let stats = make_stats();
        let mut proc = FormatProcessor::auto(2 * 1024 * 1024, stats);
        let input = b"{\"msg\":\"plain json\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"plain json\"}\n");
    }

    #[test]
    fn auto_handles_cri_when_valid() {
        let stats = make_stats();
        let mut proc = FormatProcessor::auto(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"cri\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"msg\":\"cri\"}\n"
        );
    }

    #[test]
    fn auto_malformed_line_resets_pending_state() {
        let stats = make_stats();
        let mut proc = FormatProcessor::auto(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        proc.process_lines(b"not a cri line\n", &mut out);
        // "world" is plain text → wrapped as _raw
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"not a cri line\n");
        expected.extend_from_slice(
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"_raw\":\"world\"}\n",
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn reset_clears_aggregator_state() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        // Feed a partial
        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        assert!(out.is_empty());

        // Reset (simulating rotation)
        proc.reset();

        // Next full line should not contain the old partial — plain text → wrapped as _raw
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"_raw\":\"world\"}\n"
        );
    }

    #[test]
    fn cri_injects_timestamp_and_stream() {
        // Verify that _timestamp and _stream are injected for both stdout and stderr.
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        proc.process_lines(
            b"2024-01-15T10:30:00Z stderr F {\"level\":\"ERROR\",\"msg\":\"disk full\"}\n",
            &mut out,
        );
        proc.process_lines(
            b"2024-01-15T10:30:01Z stdout F {\"level\":\"INFO\",\"msg\":\"ok\"}\n",
            &mut out,
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stderr\",\"level\":\"ERROR\",\"msg\":\"disk full\"}\n",
        );
        expected.extend_from_slice(
            b"{\"_timestamp\":\"2024-01-15T10:30:01Z\",\"_stream\":\"stdout\",\"level\":\"INFO\",\"msg\":\"ok\"}\n",
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn cri_partial_then_full_injects_metadata() {
        // For a P+F sequence the timestamp/stream from the closing F line are used.
        // P message: `{"msg":`, F message: `"hello"}`, concatenated: `{"msg":"hello"}`
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        proc.process_lines(b"2024-01-15T10:30:00Z stdout P {\"msg\":\n", &mut out);
        assert!(out.is_empty(), "partial should not emit");

        proc.process_lines(b"2024-01-15T10:30:00Z stdout F \"hello\"}\n", &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"msg\":\"hello\"}\n"
        );
    }

    #[test]
    fn cri_non_json_message_wrapped_as_raw() {
        // Non-JSON CRI messages (plain text) must be wrapped as
        // {"_timestamp":"...","_stream":"...","_raw":"<text>"} so that message
        // content is not silently lost when the scanner sees a non-JSON line.
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F plain text message\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"_raw\":\"plain text message\"}\n"
        );
    }

    #[test]
    fn cri_non_json_message_escapes_special_chars() {
        // Plain text containing JSON-special characters must be properly escaped.
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F say \"hello\"\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(
            out,
            b"{\"_timestamp\":\"2024-01-15T10:30:00Z\",\"_stream\":\"stdout\",\"_raw\":\"say \\\"hello\\\"\"}\n"
        );
    }
}
