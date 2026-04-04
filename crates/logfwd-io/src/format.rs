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
/// - `Passthrough`: lines are already scanner-ready (Raw — no JSON validation)
/// - `PassthroughJson`: lines are expected to be JSON objects; non-JSON lines
///   are still forwarded but counted as `parse_errors`
/// - `Cri`: parse CRI container log format, extract message body
/// - `Auto`: try CRI, fall through to passthrough on parse failure
#[non_exhaustive]
pub enum FormatProcessor {
    Passthrough {
        stats: Arc<ComponentStats>,
    },
    PassthroughJson {
        stats: Arc<ComponentStats>,
    },
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

    /// Create a new instance with fresh state but the same format and stats.
    ///
    /// Used to create per-source format processors so that stateful formats
    /// (CRI P/F aggregation) do not cross-contaminate between sources.
    pub fn new_instance(&self) -> Self {
        match self {
            Self::Passthrough { stats } => Self::Passthrough {
                stats: Arc::clone(stats),
            },
            Self::PassthroughJson { stats } => Self::PassthroughJson {
                stats: Arc::clone(stats),
            },
            Self::Cri { aggregator, stats } => Self::Cri {
                aggregator: CriAggregator::new(aggregator.max_message_size()),
                stats: Arc::clone(stats),
            },
            Self::Auto { aggregator, stats } => Self::Auto {
                aggregator: CriAggregator::new(aggregator.max_message_size()),
                stats: Arc::clone(stats),
            },
        }
    }

    /// Process a chunk of complete, newline-delimited lines into scanner-ready output.
    ///
    /// The input `chunk` must end at a line boundary (after `\n`). The caller
    /// handles remainder splitting — this function only sees complete lines.
    pub fn process_lines(&mut self, chunk: &[u8], out: &mut Vec<u8>) {
        match self {
            Self::Passthrough { .. } => {
                out.extend_from_slice(chunk);
            }
            Self::PassthroughJson { stats } => {
                count_json_parse_errors(chunk, stats);
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
            Self::Passthrough { .. } | Self::PassthroughJson { .. } => {}
            Self::Cri { aggregator, .. } | Self::Auto { aggregator, .. } => {
                aggregator.reset();
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
            let max_message_size = aggregator.max_message_size();
            match aggregator.feed(cri.message, cri.is_full) {
                AggregateResult::Complete(msg) => {
                    inject_cri_metadata(msg, cri.timestamp, cri.stream, out);
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
        let stats = make_stats();
        let mut proc = FormatProcessor::passthrough(stats);
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

    #[test]
    fn passthrough_json_valid_line_no_error() {
        // A valid JSON-object line must NOT increment parse_errors.
        let stats = make_stats();
        let mut proc = FormatProcessor::passthrough_json(Arc::clone(&stats));
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
        let mut proc = FormatProcessor::passthrough_json(Arc::clone(&stats));
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
        let mut proc = FormatProcessor::passthrough_json(Arc::clone(&stats));
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
        let mut proc = FormatProcessor::passthrough_json(Arc::clone(&stats));
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
        let mut proc = FormatProcessor::passthrough(Arc::clone(&stats));
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
        let mut proc = FormatProcessor::cri(5, Arc::clone(&stats));
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

    /// inject_cri_metadata always produces a non-empty output ending with '\n'.
    ///
    /// This holds for both JSON messages (starts with '{') and plain-text
    /// messages (wraps as _raw).  The trailing newline is required so that the
    /// downstream scanner can split on line boundaries.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_inject_cri_metadata_ends_with_newline() {
        let msg: [u8; 4] = kani::any();
        let ts = b"TS";
        let stream = b"S";
        let mut out = Vec::new();
        inject_cri_metadata(&msg, ts, stream, &mut out);
        assert!(!out.is_empty(), "output must be non-empty");
        assert_eq!(*out.last().unwrap(), b'\n', "output must end with newline");
        kani::cover!(msg[0] == b'{', "JSON message path exercised");
        kani::cover!(msg[0] != b'{', "plain text path exercised");
    }

    /// inject_cri_metadata always produces output starting with '{'.
    ///
    /// Both the JSON injection path and the _raw wrapper path open with '{'
    /// so the downstream scanner always sees a well-formed JSON object start.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_inject_cri_metadata_starts_with_brace() {
        let msg: [u8; 4] = kani::any();
        let ts = b"TS";
        let stream = b"S";
        let mut out = Vec::new();
        inject_cri_metadata(&msg, ts, stream, &mut out);
        assert_eq!(out[0], b'{', "output must start with '{'");
        kani::cover!(msg[0] == b'{', "JSON path exercised");
        kani::cover!(msg[0] != b'{', "plain text path exercised");
    }

    /// For JSON messages (starting with '{'), inject_cri_metadata injects the
    /// _timestamp and _stream fields at the front of the object.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_inject_json_msg_gets_metadata_prefix() {
        let mut msg = [0u8; 4];
        msg[0] = b'{';
        msg[1] = kani::any();
        msg[2] = kani::any();
        msg[3] = kani::any();
        let ts = b"TS";
        let stream = b"S";
        let mut out = Vec::new();
        inject_cri_metadata(&msg, ts, stream, &mut out);
        // Output must start with the timestamp+stream injection prefix.
        assert!(out.starts_with(b"{\"_timestamp\":\"TS\",\"_stream\":\"S\","));
        kani::cover!(true, "JSON path metadata prefix verified");
    }

    /// For non-JSON messages (not starting with '{'), inject_cri_metadata wraps
    /// the content in a {"_raw":"..."} object so no message content is lost.
    ///
    /// This is the Auto-mode fallthrough path: when CRI parsing succeeds but
    /// the message body is not a JSON object, the plain text is preserved in
    /// the _raw field rather than being silently discarded.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_inject_non_json_msg_uses_raw_key() {
        let msg: [u8; 4] = kani::any();
        kani::assume(msg[0] != b'{');
        let ts = b"TS";
        let stream = b"S";
        let mut out = Vec::new();
        inject_cri_metadata(&msg, ts, stream, &mut out);
        // Output must start with the _raw wrapper prefix (no message content is lost).
        assert!(out.starts_with(b"{\"_timestamp\":\"TS\",\"_stream\":\"S\",\"_raw\":\""));
        kani::cover!(true, "plain text path _raw wrapper verified");
    }
}
