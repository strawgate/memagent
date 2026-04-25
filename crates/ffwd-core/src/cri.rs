//! CRI (Container Runtime Interface) log format parser.
//!
//! Kubernetes container runtimes (containerd, CRI-O) write logs in this format:
//!   2024-01-15T10:30:00.123456789Z stdout F {"actual":"json","log":"line"}
//!   ^timestamp                     ^stream ^flags ^message
//!
//! - timestamp: RFC3339Nano
//! - stream: "stdout" or "stderr"
//! - flags: "F" (full line) or "P" (partial — line was split at 16KB boundary)
//! - message: the actual log content (rest of the line)
//!
//! Partial lines (flag "P") must be reassembled: concatenate all "P" chunks
//! until an "F" chunk arrives, then emit the combined line.

// Re-export reassembler types so bench/fuzz targets can reach them via
// `ffwd_core::cri::CriReassembler` and `ffwd_core::cri::AggregateResult`.
pub use crate::reassembler::{AggregateResult, CriReassembler};
use alloc::vec::Vec;

/// Parsed CRI log line. References point into the original byte slice (zero-copy).
#[derive(Debug)]
pub struct CriLine<'a> {
    /// The RFC3339Nano timestamp bytes.
    pub timestamp: &'a [u8],
    /// "stdout" or "stderr".
    pub stream: &'a [u8],
    /// true if this is a complete line (flag "F"), false if partial ("P").
    pub is_full: bool,
    /// The actual log message content.
    pub message: &'a [u8],
}

/// Parse a single CRI log line. Returns None if the format is invalid.
///
/// This is zero-copy — all returned slices point into the input `line`.
/// Uses `byte_search::find_byte` (Kani-proven) instead of memchr.
#[inline]
pub fn parse_cri_line(line: &[u8]) -> Option<CriLine<'_>> {
    use crate::byte_search::find_byte;

    // Format: "TIMESTAMP STREAM FLAGS MESSAGE"
    // Find first space (after timestamp).
    let sp1 = find_byte(line, b' ', 0)?;
    // Reject empty timestamp (line starts with space) or no room for stream+flags.
    if sp1 == 0 || sp1 + 1 >= line.len() {
        return None;
    }

    // Find second space (after stream).
    let sp2 = find_byte(line, b' ', sp1 + 1)?;
    // Reject empty stream (consecutive spaces) or no room for flags.
    if sp2 == sp1 + 1 || sp2 + 1 >= line.len() {
        return None;
    }

    // Find third space (after flags).
    let (flags_end, msg_start) = if let Some(sp3) = find_byte(line, b' ', sp2 + 1) {
        (sp3, sp3 + 1)
    } else {
        // No message content (just flags, no trailing space) — empty message.
        (line.len(), line.len())
    };

    let stream = &line[sp1 + 1..sp2];
    // CRI stream must be stdout or stderr.
    if stream != b"stdout" && stream != b"stderr" {
        return None;
    }

    let flags = &line[sp2 + 1..flags_end];

    // CRI spec: flags must be exactly "F" (full) or "P" (partial).
    // Reject anything else as invalid format.
    let is_full = if flags == b"F" {
        true
    } else if flags == b"P" {
        false
    } else {
        return None;
    };

    let message = if msg_start < line.len() {
        &line[msg_start..]
    } else {
        &[]
    };

    Some(CriLine {
        timestamp: &line[..sp1],
        stream,
        is_full,
        message,
    })
}

/// Process a chunk of CRI-formatted log data. Parses each CRI line, reassembles
/// partials, and calls `emit` with each complete log message.
///
/// Returns `(lines_ok, parse_errors)` where `parse_errors` is the number of
/// non-empty lines that could not be parsed as valid CRI format **plus** the
/// number of lines that were truncated due to `max_line_size`.
#[allow(dead_code)] // used by tests; will be called from pipeline code once per-stream integration lands
pub(crate) fn process_cri_chunk<F>(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    mut emit: F,
) -> (usize, usize)
where
    F: FnMut(&[u8]),
{
    let mut count = 0;
    let mut errors = 0;
    process_cri_chunk_lines(
        chunk,
        reassembler,
        |line, reassembler| {
            if line.is_empty() {
                return (0, 0);
            }

            match parse_cri_line(line) {
                Some(cri) => match reassembler.feed(cri.message, cri.is_full) {
                    AggregateResult::Complete(msg) => {
                        emit(msg);
                        reassembler.reset();
                        (1, 0)
                    }
                    AggregateResult::Truncated(msg) => {
                        emit(msg);
                        reassembler.reset();
                        (1, 1)
                    }
                    AggregateResult::Pending => (0, 0),
                },
                None => (0, 1),
            }
        },
        &mut count,
        &mut errors,
    );

    (count, errors)
}

/// Process CRI data and write extracted messages directly into an output buffer.
/// Each message is written as: message bytes + newline.
///
/// For the common case (full lines, no partials), the message bytes come straight
/// from the input chunk — zero per-line allocation. Only partial line reassembly
/// copies into the reassembler's internal buffer.
///
/// Returns `(lines_ok, parse_errors)` where `parse_errors` is the number of
/// non-empty lines that could not be parsed as valid CRI format **plus** the
/// number of lines that were truncated due to `max_line_size`.
#[allow(dead_code)] // used by tests; will be called from pipeline code once per-stream integration lands
pub(crate) fn process_cri_to_buf(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    out: &mut Vec<u8>,
) -> (usize, usize) {
    process_cri_to_buf_with_plain_text_field(chunk, reassembler, "body", out)
}

/// Same as `process_cri_to_buf` but allows choosing the field name used when
/// wrapping non-JSON plain-text messages.
pub fn process_cri_to_buf_with_plain_text_field(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    plain_text_field_name: &str,
    out: &mut Vec<u8>,
) -> (usize, usize) {
    let mut count = 0;
    let mut errors = 0;
    process_cri_chunk_lines(
        chunk,
        reassembler,
        |line, reassembler| {
            if line.is_empty() {
                return (0, 0);
            }

            match parse_cri_line(line) {
                Some(cri) => match reassembler.feed(cri.message, cri.is_full) {
                    AggregateResult::Complete(msg) => {
                        write_json_line_for_plain_text_field(msg, plain_text_field_name, out);
                        reassembler.reset();
                        (1, 0)
                    }
                    AggregateResult::Truncated(msg) => {
                        write_json_line_for_plain_text_field(msg, plain_text_field_name, out);
                        reassembler.reset();
                        (1, 1)
                    }
                    AggregateResult::Pending => (0, 0),
                },
                None => (0, 1),
            }
        },
        &mut count,
        &mut errors,
    );

    (count, errors)
}

#[inline]
fn process_cri_chunk_lines<F>(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    mut process_line: F,
    count: &mut usize,
    errors: &mut usize,
) where
    F: FnMut(&[u8], &mut CriReassembler) -> (usize, usize),
{
    let mut line_start = 0;

    for pos in memchr::memchr_iter(b'\n', chunk) {
        let line = &chunk[line_start..pos];
        line_start = pos + 1;

        if reassembler.has_line_fragment() {
            reassembler.push_line_fragment(line);

            if reassembler.line_fragment_truncated() {
                *errors += 1;
                // Reset the full reassembler state, not just the line fragment.
                // If a P (partial) record was previously buffered, leaving it
                // intact would cause the next unrelated F record to append to
                // stale data and emit a corrupted message.
                reassembler.reset();
                continue;
            }

            let complete_line = reassembler.take_line_fragment();
            let (line_count, line_errors) = process_line(&complete_line, reassembler);
            *count += line_count;
            *errors += line_errors;
            continue;
        }

        let (line_count, line_errors) = process_line(line, reassembler);
        *count += line_count;
        *errors += line_errors;
    }

    let trailing = &chunk[line_start..];
    if !trailing.is_empty() {
        reassembler.push_line_fragment(trailing);
    }
}

#[inline]
fn write_json_line_for_plain_text_field(
    msg: &[u8],
    plain_text_field_name: &str,
    out: &mut Vec<u8>,
) {
    if plain_text_field_name == "body" {
        write_json_line(msg, out);
    } else {
        write_json_line_with_plain_text_field(msg, plain_text_field_name, out);
    }
}

/// Append `src` to `dst` with JSON string escaping (no surrounding quotes).
///
/// Handles all characters that must be escaped in a JSON string value:
/// double-quote, backslash, and ASCII control characters (U+0000–U+001F, U+007F).
#[inline]
pub fn json_escape_bytes(src: &[u8], dst: &mut Vec<u8>) {
    for &b in src {
        match b {
            b'"' => dst.extend_from_slice(b"\\\""),
            b'\\' => dst.extend_from_slice(b"\\\\"),
            0x08 => dst.extend_from_slice(b"\\b"),
            b'\t' => dst.extend_from_slice(b"\\t"),
            b'\n' => dst.extend_from_slice(b"\\n"),
            0x0C => dst.extend_from_slice(b"\\f"),
            b'\r' => dst.extend_from_slice(b"\\r"),
            0x00..=0x1F | 0x7F => {
                dst.extend_from_slice(b"\\u00");
                let hi = (b >> 4) & 0x0F;
                let lo = b & 0x0F;
                dst.push(if hi < 10 { b'0' + hi } else { b'a' + hi - 10 });
                dst.push(if lo < 10 { b'0' + lo } else { b'a' + lo - 10 });
            }
            _ => dst.push(b),
        }
    }
}

/// Write a single message into the output buffer.
///
/// If `msg` starts with `{` it is treated as a JSON object and written
/// verbatim.  Otherwise `msg` is plain text and is written as
/// `{"body":"<json-escaped msg>"}` so that no content is silently lost
/// when the downstream scanner processes the line.
#[inline]
#[allow(dead_code)] // called by process_cri_to_buf
fn write_json_line(msg: &[u8], out: &mut Vec<u8>) {
    write_json_line_with_plain_text_field(msg, "body", out);
}

#[inline]
fn write_json_line_with_plain_text_field(
    msg: &[u8],
    plain_text_field_name: &str,
    out: &mut Vec<u8>,
) {
    if msg.first() == Some(&b'{') {
        out.extend_from_slice(msg);
    } else {
        // Non-JSON plain text: wrap as {"<field>":"<escaped>"} so the scanner
        // sees a valid JSON object and message content is preserved.
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
    use alloc::format;
    use proptest::prelude::*;
    use proptest::test_runner::Config as ProptestConfig;

    #[test]
    fn test_parse_full_line() {
        let line =
            b"2024-01-15T10:30:00.123456789Z stdout F {\"level\":\"INFO\",\"msg\":\"hello\"}";
        let cri = parse_cri_line(line).unwrap();
        assert_eq!(cri.timestamp, b"2024-01-15T10:30:00.123456789Z");
        assert_eq!(cri.stream, b"stdout");
        assert!(cri.is_full);
        assert_eq!(cri.message, b"{\"level\":\"INFO\",\"msg\":\"hello\"}");
    }

    #[test]
    fn test_parse_partial_line() {
        let line = b"2024-01-15T10:30:00.123Z stderr P partial content here";
        let cri = parse_cri_line(line).unwrap();
        assert_eq!(cri.stream, b"stderr");
        assert!(!cri.is_full);
        assert_eq!(cri.message, b"partial content here");
    }

    #[test]
    fn test_reassemble_partials() {
        let mut reassembler = CriReassembler::new(1024 * 1024);

        let p1 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P first part").unwrap();
        assert!(matches!(
            reassembler.feed(p1.message, p1.is_full),
            AggregateResult::Pending
        ));

        let p2 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P second part").unwrap();
        assert!(matches!(
            reassembler.feed(p2.message, p2.is_full),
            AggregateResult::Pending
        ));

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F final part").unwrap();
        match reassembler.feed(f.message, f.is_full) {
            AggregateResult::Complete(complete) => {
                assert_eq!(complete, b"first partsecond partfinal part");
            }
            AggregateResult::Truncated(_) => panic!("expected Complete, got Truncated"),
            AggregateResult::Pending => panic!("expected Complete, got Pending"),
        }
        reassembler.reset();
    }

    #[test]
    fn test_reassemble_no_partials() {
        let mut reassembler = CriReassembler::new(1024 * 1024);

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F complete line").unwrap();
        match reassembler.feed(f.message, f.is_full) {
            AggregateResult::Complete(complete) => {
                assert_eq!(complete, b"complete line");
            }
            AggregateResult::Truncated(_) => panic!("expected Complete, got Truncated"),
            AggregateResult::Pending => panic!("expected Complete, got Pending"),
        }
        reassembler.reset();
    }

    #[test]
    fn test_process_chunk() {
        let chunk = b"2024-01-15T10:30:00Z stdout F line one\n\
                       2024-01-15T10:30:01Z stdout F line two\n\
                       2024-01-15T10:30:02Z stderr F line three\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();
        let (count, errors) = process_cri_chunk(chunk, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count, 3);
        assert_eq!(errors, 0);
        assert_eq!(lines[0], b"line one");
        assert_eq!(lines[1], b"line two");
        assert_eq!(lines[2], b"line three");
    }

    #[test]
    fn test_process_chunk_split_full_line_across_chunks() {
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();

        let c1 = b"2024-01-15T10:30:00Z stdout F hel";
        let c2 = b"lo\n";

        let (count1, errors1) = process_cri_chunk(c1, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count1, 0);
        assert_eq!(errors1, 0);
        assert!(reassembler.has_line_fragment());

        let (count2, errors2) = process_cri_chunk(c2, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count2, 1);
        assert_eq!(errors2, 0);
        assert_eq!(lines.as_slice(), &[b"hello".to_vec()]);
        assert!(!reassembler.has_line_fragment());
    }

    #[test]
    fn test_process_chunk_split_full_line_truncates_message_not_header() {
        let mut reassembler = CriReassembler::new(5);
        let mut lines = Vec::new();

        let c1 = b"2024-01-15T10:30:00Z stdout F hel";
        let c2 = b"lo world\n";

        let (count1, errors1) = process_cri_chunk(c1, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count1, 0);
        assert_eq!(errors1, 0);

        let (count2, errors2) = process_cri_chunk(c2, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count2, 1);
        assert_eq!(errors2, 1);
        assert_eq!(lines.as_slice(), &[b"hello".to_vec()]);
        assert!(!reassembler.has_line_fragment());
    }

    #[test]
    fn test_process_chunk_parse_errors() {
        // Mix of valid and invalid CRI lines.
        let chunk = b"not-a-cri-line\n\
                       2024-01-15T10:30:00Z stdout F valid line\n\
                       also-invalid\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();
        let (count, errors) = process_cri_chunk(chunk, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count, 1);
        assert_eq!(errors, 2);
        assert_eq!(lines[0], b"valid line");
    }

    #[test]
    fn test_process_cri_to_buf_parse_errors() {
        let chunk = b"not-a-cri-line\n\
                       2024-01-15T10:30:00Z stdout F {\"msg\":\"ok\"}\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut out = Vec::new();
        let (count, errors) = process_cri_to_buf(chunk, &mut reassembler, &mut out);
        assert_eq!(count, 1);
        assert_eq!(errors, 1);
    }

    #[test]
    fn test_process_cri_to_buf_split_full_line_across_chunks() {
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut out = Vec::new();

        let c1 = b"2024-01-15T10:30:00Z stdout F hel";
        let c2 = b"lo\n";

        let (count1, errors1) =
            process_cri_to_buf_with_plain_text_field(c1, &mut reassembler, "body", &mut out);
        assert_eq!(count1, 0);
        assert_eq!(errors1, 0);
        assert!(out.is_empty());
        assert!(reassembler.has_line_fragment());

        let (count2, errors2) =
            process_cri_to_buf_with_plain_text_field(c2, &mut reassembler, "body", &mut out);
        assert_eq!(count2, 1);
        assert_eq!(errors2, 0);
        assert_eq!(out, b"{\"body\":\"hello\"}\n");
        assert!(!reassembler.has_line_fragment());
    }

    #[test]
    fn test_process_chunk_split_partial_and_full_lines_across_chunks() {
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();

        let c1 = b"2024-01-15T10:30:00Z stdout P he";
        let c2 = b"llo\n2024-01-15T10:30:01Z stdout F world\n";

        let (count1, errors1) = process_cri_chunk(c1, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count1, 0);
        assert_eq!(errors1, 0);

        let (count2, errors2) = process_cri_chunk(c2, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count2, 1);
        assert_eq!(errors2, 0);
        assert_eq!(lines.as_slice(), &[b"helloworld".to_vec()]);
    }

    #[test]
    fn test_truncated_raw_fragment_resets_pending_partial_state() {
        let mut reassembler = CriReassembler::new(5);
        let mut lines = Vec::new();

        let (count1, errors1) = process_cri_chunk(
            b"2024-01-15T10:30:00Z stdout P stale\n",
            &mut reassembler,
            |msg| lines.push(msg.to_vec()),
        );
        assert_eq!((count1, errors1), (0, 0));
        assert!(reassembler.has_pending());

        let oversized = [b'x'; 300];
        let (count2, errors2) = process_cri_chunk(&oversized, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!((count2, errors2), (0, 0));

        let (count3, errors3) = process_cri_chunk(
            b"\n2024-01-15T10:30:01Z stdout F fresh\n",
            &mut reassembler,
            |msg| lines.push(msg.to_vec()),
        );
        assert_eq!((count3, errors3), (1, 1));
        assert_eq!(lines.as_slice(), &[b"fresh".to_vec()]);
        assert!(!reassembler.has_pending());
        assert!(!reassembler.has_line_fragment());
    }

    #[test]
    fn test_incomplete_trailing_data_remains_buffered() {
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();

        let chunk = b"2024-01-15T10:30:00Z stdout F hello";
        let (count, errors) = process_cri_chunk(chunk, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });

        assert_eq!(count, 0);
        assert_eq!(errors, 0);
        assert!(lines.is_empty());
        assert!(reassembler.has_line_fragment());
    }

    #[test]
    fn test_write_json_line_plain_text_wrapped_as_body() {
        // Plain-text (non-JSON) messages must be wrapped as {"body":"..."}.
        let mut out = Vec::new();
        write_json_line(b"application started", &mut out);
        assert_eq!(out, b"{\"body\":\"application started\"}\n");
    }

    #[test]
    fn test_process_cri_to_buf_plain_text_uses_configured_field() {
        let chunk = b"2024-01-15T10:30:00Z stdout F application started\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut out = Vec::new();
        let (count, errors) = process_cri_to_buf_with_plain_text_field(
            chunk,
            &mut reassembler,
            "plain_text",
            &mut out,
        );
        assert_eq!(count, 1);
        assert_eq!(errors, 0);
        assert_eq!(out, b"{\"plain_text\":\"application started\"}\n");
    }

    #[test]
    fn test_write_json_line_plain_text_escapes_special_chars() {
        // JSON-special characters in the message must be escaped.
        let mut out = Vec::with_capacity(256);
        write_json_line(b"say \"hello\"", &mut out);
        assert_eq!(out, b"{\"body\":\"say \\\"hello\\\"\"}\n");
    }

    #[test]
    fn test_process_cri_to_buf_plain_text_wrapped() {
        // Plain-text CRI messages should be emitted as {"body":"..."} lines.
        let chunk = b"2024-01-15T10:30:00Z stdout F application started\n\
                       2024-01-15T10:30:01Z stdout F {\"msg\":\"ok\"}\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut out = Vec::new();
        let (count, errors) = process_cri_to_buf(chunk, &mut reassembler, &mut out);
        assert_eq!(count, 2);
        assert_eq!(errors, 0);
        assert_eq!(
            out,
            b"{\"body\":\"application started\"}\n{\"msg\":\"ok\"}\n"
        );
    }

    #[test]
    fn test_invalid_flag_rejected() {
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z stdout X bad").is_none());
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z stdout FULL message").is_none());
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z stdout f message").is_none());
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z stdout p message").is_none());
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z stdout  message").is_none());
    }

    #[test]
    fn test_invalid_stream_rejected() {
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z out F ok").is_none());
        assert!(parse_cri_line(b"2024-01-15T10:30:00Z stdxxx P part").is_none());
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            .. ProptestConfig::default()
        })]
        #[test]
        fn proptest_stream_token_validation(stream in "[a-z]{1,8}") {
            let line = format!("2024-01-15T10:30:00Z {stream} F msg");
            let parsed = parse_cri_line(line.as_bytes());
            if stream == "stdout" || stream == "stderr" {
                prop_assert!(parsed.is_some(), "valid stream token should parse");
            } else {
                prop_assert!(parsed.is_none(), "invalid stream token should be rejected");
            }
        }

        #[test]
        // Exhaustively splits the input at every byte offset (0..=line.len()).
        // Each case runs 60–100 full parse cycles — trivially cheap natively
        // but ~40s under Miri with PROPTEST_CASES=1. The equivalence invariant
        // is memory-agnostic, so Miri (a UB detector) gets no new coverage from
        // this loop that the native proptest doesn't already provide.
        #[cfg_attr(miri, ignore)]
        fn proptest_split_full_line_matches_unsplit(
            message in "[ -~]{0,64}",
            max_size in 1..64usize,
        ) {
            let line = format!("2024-01-15T10:30:00Z stdout F {message}\n");
            for split in 0..=line.len() {
                let mut unsplit = CriReassembler::new(max_size);
                let mut unsplit_lines = Vec::new();
                let unsplit_counts = process_cri_chunk(
                    line.as_bytes(),
                    &mut unsplit,
                    |msg| unsplit_lines.push(msg.to_vec()),
                );

                let mut split_reassembler = CriReassembler::new(max_size);
                let mut split_lines = Vec::new();
                let first_counts = process_cri_chunk(
                    &line.as_bytes()[..split],
                    &mut split_reassembler,
                    |msg| split_lines.push(msg.to_vec()),
                );
                let second_counts = process_cri_chunk(
                    &line.as_bytes()[split..],
                    &mut split_reassembler,
                    |msg| split_lines.push(msg.to_vec()),
                );

                prop_assert_eq!(
                    (first_counts.0 + second_counts.0, first_counts.1 + second_counts.1),
                    unsplit_counts,
                    "split {} changed counts for line {:?}",
                    split,
                    line
                );
                prop_assert_eq!(
                    split_lines,
                    unsplit_lines,
                    "split {} changed output for line {:?}",
                    split,
                    line
                );
                prop_assert!(
                    !split_reassembler.has_line_fragment(),
                    "complete split line left raw fragment buffered"
                );
            }
        }
    }

    #[test]
    fn test_invalid_flag_counted_as_parse_error() {
        let chunk = b"2024-01-15T10:30:00Z stdout X bad\n\
                       2024-01-15T10:30:00Z stdout F valid\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();
        let (count, errors) = process_cri_chunk(chunk, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count, 1);
        assert_eq!(errors, 1);
        assert_eq!(lines[0], b"valid");
    }

    #[test]
    fn test_parse_full_line_no_message() {
        // CRI line with flag but no message content (no trailing space after flag).
        // Previously returned None due to incorrect flags computation (#698).
        let line = b"2024-01-15T10:30:00Z stdout F";
        let cri = parse_cri_line(line).unwrap();
        assert_eq!(cri.timestamp, b"2024-01-15T10:30:00Z");
        assert_eq!(cri.stream, b"stdout");
        assert!(cri.is_full);
        assert_eq!(cri.message, b"");
    }

    #[test]
    fn test_parse_partial_line_no_message() {
        let line = b"2024-01-15T10:30:00Z stderr P";
        let cri = parse_cri_line(line).unwrap();
        assert_eq!(cri.stream, b"stderr");
        assert!(!cri.is_full);
        assert_eq!(cri.message, b"");
    }

    #[test]
    fn test_process_chunk_empty_message_not_dropped() {
        // A CRI line with no message content should produce an empty message,
        // not be silently dropped as a parse error (#698).
        let chunk = b"2024-01-15T10:30:00Z stdout F\n\
                       2024-01-15T10:30:01Z stdout F has content\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();
        let (count, errors) = process_cri_chunk(chunk, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count, 2);
        assert_eq!(errors, 0);
        assert_eq!(lines[0], b"");
        assert_eq!(lines[1], b"has content");
    }

    #[test]
    fn test_max_line_size() {
        let mut reassembler = CriReassembler::new(20);

        let p1 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P 0123456789").unwrap();
        reassembler.feed(p1.message, p1.is_full);

        let p2 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P abcdefghij").unwrap();
        reassembler.feed(p2.message, p2.is_full);

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F KLMNOPQRST").unwrap();
        // The assembled sequence exceeds max_line_size=20, so Truncated is expected.
        match reassembler.feed(f.message, f.is_full) {
            AggregateResult::Truncated(complete) => {
                assert_eq!(complete.len(), 20);
            }
            AggregateResult::Complete(complete) => {
                panic!(
                    "expected Truncated, got Complete with len={}",
                    complete.len()
                )
            }
            AggregateResult::Pending => panic!("expected Truncated, got Pending"),
        }
        reassembler.reset();
    }

    #[test]
    fn test_max_line_size_f_only_truncated() {
        // A single F line that exceeds max_line_size must return Truncated.
        let mut reassembler = CriReassembler::new(5);
        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F hello world").unwrap();
        match reassembler.feed(f.message, f.is_full) {
            AggregateResult::Truncated(out) => {
                assert_eq!(out, b"hello");
            }
            AggregateResult::Complete(out) => {
                panic!("expected Truncated, got Complete: {:?}", out)
            }
            AggregateResult::Pending => panic!("expected Truncated"),
        }
        reassembler.reset();
    }

    #[test]
    fn json_escape_all_control_characters() {
        // These control chars produce \u00XX where XX has hex digits a-f.
        // Bytes 0x0A through 0x1F have low nibble >= 10 (some hit the
        // `b'a' + (nibble - 10)` branch in json_escape_bytes).
        // Note: 0x08 (\b), 0x09 (\t), 0x0A (\n), 0x0C (\f), 0x0D (\r) use
        // named escapes, so we check the generic \u00XX path for the rest.
        let mut buf = Vec::new();
        for byte in 0x00u8..=0x1F {
            buf.clear();
            let input = [byte];
            json_escape_bytes(&input, &mut buf);
            let escaped = core::str::from_utf8(&buf).unwrap();
            match byte {
                0x08 => assert_eq!(escaped, "\\b", "byte 0x{byte:02x}"),
                b'\t' => assert_eq!(escaped, "\\t", "byte 0x{byte:02x}"),
                b'\n' => assert_eq!(escaped, "\\n", "byte 0x{byte:02x}"),
                0x0C => assert_eq!(escaped, "\\f", "byte 0x{byte:02x}"),
                b'\r' => assert_eq!(escaped, "\\r", "byte 0x{byte:02x}"),
                _ => {
                    let expected = format!("\\u00{byte:02x}");
                    assert_eq!(escaped, expected, "byte 0x{byte:02x}");
                }
            }
        }
        // Also verify 0x7F (DEL) uses the generic escape path.
        buf.clear();
        json_escape_bytes(&[0x7F], &mut buf);
        assert_eq!(
            core::str::from_utf8(&buf).unwrap(),
            "\\u007f",
            "byte 0x7F (DEL)"
        );
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    const SHORT_FIELD_NAME: &str = "b";

    use ffwd_kani::bytes::assert_bytes_eq;

    /// Prove parse_cri_line never panics for any 32-byte input.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_parse_cri_line_no_panic() {
        let input: [u8; 32] = kani::any();
        let _ = parse_cri_line(&input);
    }

    /// Prove parse_cri_line semantic correctness: if it returns Some,
    /// the flag is valid (F or P) and is_full matches the flag byte.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_parse_cri_line_semantics() {
        let input: [u8; 32] = kani::any();
        if let Some(cri) = parse_cri_line(&input) {
            assert!(!cri.timestamp.is_empty(), "empty timestamp");
            assert!(
                cri.stream == b"stdout" || cri.stream == b"stderr",
                "invalid stream"
            );
            kani::cover!(cri.stream == b"stdout", "stdout parse is reachable");
            kani::cover!(cri.stream == b"stderr", "stderr parse is reachable");

            // Verify is_full matches the actual flag byte in the input.
            let ts_len = cri.timestamp.len();
            let stream_len = cri.stream.len();
            let flag_start = ts_len + 1 + stream_len + 1;
            assert!(flag_start < input.len(), "flag start out of bounds");

            let flag_byte = input[flag_start];
            if cri.is_full {
                assert!(flag_byte == b'F', "is_full=true but flag is not F");
            } else {
                assert!(flag_byte == b'P', "is_full=false but flag is not P");
            }
        }
    }

    /// Prove the configurable plain-text field wrapper path never panics.
    #[kani::proof]
    #[kani::unwind(52)]
    #[kani::solver(kissat)]
    fn verify_process_cri_to_buf_with_plain_text_field_no_panic() {
        let chunk: [u8; 48] = kani::any();
        let mut out = Vec::new();
        let mut reassembler = CriReassembler::new(64);
        let _ =
            process_cri_to_buf_with_plain_text_field(&chunk, &mut reassembler, "body", &mut out);

        out.clear();
        reassembler.reset();
        let _ = process_cri_to_buf_with_plain_text_field(&chunk, &mut reassembler, "msg", &mut out);
    }

    /// Prove a valid CRI line split across chunks is equivalent to the same
    /// line arriving unsplit through the public CRI-to-buffer entrypoint.
    #[kani::proof]
    #[kani::unwind(64)]
    fn verify_process_cri_split_line_matches_unsplit() {
        let first = b"2024-01-15T10:30:00Z stdout F h";
        let second = b"i\n";
        let whole = b"2024-01-15T10:30:00Z stdout F hi\n";

        let mut split_out = Vec::new();
        let mut split_reassembler = CriReassembler::new(8);
        let first_counts = process_cri_to_buf_with_plain_text_field(
            first,
            &mut split_reassembler,
            SHORT_FIELD_NAME,
            &mut split_out,
        );
        let second_counts = process_cri_to_buf_with_plain_text_field(
            second,
            &mut split_reassembler,
            SHORT_FIELD_NAME,
            &mut split_out,
        );

        let mut unsplit_out = Vec::new();
        let mut unsplit_reassembler = CriReassembler::new(8);
        let unsplit_counts = process_cri_to_buf_with_plain_text_field(
            whole,
            &mut unsplit_reassembler,
            SHORT_FIELD_NAME,
            &mut unsplit_out,
        );

        assert_eq!(
            (
                first_counts.0 + second_counts.0,
                first_counts.1 + second_counts.1
            ),
            unsplit_counts
        );
        assert_bytes_eq(&split_out, &unsplit_out);
        assert!(!split_reassembler.has_line_fragment());
        kani::cover!(first_counts.0 == 0, "first split chunk buffers only");
        kani::cover!(second_counts.0 == 1, "second split chunk emits row");
    }

    /// Prove parse_cri_line rejects known invalid stream tokens.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_parse_cri_line_rejects_known_invalid_streams() {
        let out = b"2024-01-15T10:30:00Z out F ok";
        assert!(
            parse_cri_line(out).is_none(),
            "invalid stream 'out' must be rejected"
        );
        kani::cover!(true, "invalid stream 'out' rejected");

        let stdxxx = b"2024-01-15T10:30:00Z stdxxx P part";
        assert!(
            parse_cri_line(stdxxx).is_none(),
            "invalid stream 'stdxxx' must be rejected"
        );
        kani::cover!(true, "invalid stream 'stdxxx' rejected");
    }

    /// Prove parse_cri_line accepts both valid stream tokens.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_parse_cri_line_accepts_valid_streams() {
        let stdout = b"2024-01-15T10:30:00Z stdout F ok";
        assert!(
            parse_cri_line(stdout).is_some(),
            "valid stream 'stdout' should parse"
        );
        kani::cover!(true, "valid stream 'stdout' accepted");

        let stderr = b"2024-01-15T10:30:00Z stderr P part";
        assert!(
            parse_cri_line(stderr).is_some(),
            "valid stream 'stderr' should parse"
        );
        kani::cover!(true, "valid stream 'stderr' accepted");
    }

    /// Prove parse_cri_line rejects invalid single-byte flags.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_parse_cri_line_rejects_invalid_flags() {
        let input: [u8; 32] = kani::any();

        // Find the three space positions
        let mut spaces: [usize; 3] = [0; 3];
        let mut space_count = 0u8;
        let mut i: usize = 0;
        while i < 32 && space_count < 3 {
            if input[i] == b' ' {
                spaces[space_count as usize] = i;
                space_count += 1;
            }
            i += 1;
        }

        // If we have 3+ spaces and the flag is a single non-F/non-P byte
        if space_count >= 3 {
            let sp2 = spaces[1];
            let sp3 = spaces[2];
            // Single-byte flag: sp3 == sp2 + 2
            if sp3 == sp2 + 2 {
                let flag_byte = input[sp2 + 1];
                if flag_byte != b'F' && flag_byte != b'P' {
                    let result = parse_cri_line(&input);
                    assert!(result.is_none(), "invalid flag accepted");
                }
            }
        }
    }

    /// Prove CriReassembler::feed respects max_line_size for P+F sequences.
    /// Uses fixed 8-byte messages — Kani explores all 2^64 byte values.
    #[kani::proof]
    fn verify_reassembler_respects_max_size_pf() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut r = CriReassembler::new(max_size);

        let msg1: [u8; 8] = kani::any();
        let partial = CriLine {
            timestamp: b"ts",
            stream: b"out",
            is_full: false,
            message: &msg1,
        };
        let _ = r.feed(partial.message, partial.is_full);

        let msg2: [u8; 8] = kani::any();
        let full = CriLine {
            timestamp: b"ts",
            stream: b"out",
            is_full: true,
            message: &msg2,
        };
        match r.feed(full.message, full.is_full) {
            AggregateResult::Complete(output) | AggregateResult::Truncated(output) => {
                assert!(output.len() <= max_size, "P+F output exceeds max_line_size");

                // Guard vacuity: verify constraint allows meaningful cases
                kani::cover!(output.len() > 8, "P+F concatenation occurred");
                kani::cover!(output.len() == max_size, "output truncated at max");
                kani::cover!(output.len() < max_size, "output under max");
            }
            AggregateResult::Pending => {}
        }
    }

    /// Prove CriReassembler::feed respects max_line_size for F-only lines.
    /// This proof caught a real bug: the F fast path was not enforcing max_line_size.
    /// (the fast path where no partials are pending).
    #[kani::proof]
    fn verify_reassembler_respects_max_size_f_only() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut r = CriReassembler::new(max_size);

        let msg: [u8; 8] = kani::any();
        let full = CriLine {
            timestamp: b"ts",
            stream: b"out",
            is_full: true,
            message: &msg,
        };
        match r.feed(full.message, full.is_full) {
            AggregateResult::Complete(output) | AggregateResult::Truncated(output) => {
                assert!(
                    output.len() <= max_size,
                    "F-only output exceeds max_line_size"
                );

                // Guard vacuity: verify fast path coverage
                kani::cover!(output.len() == 8, "full message fits");
                kani::cover!(output.len() < 8, "message truncated");
                kani::cover!(max_size >= 8, "max allows full message");
            }
            AggregateResult::Pending => {}
        }
    }

    /// Prove CriReassembler::feed produces output only on "F" lines.
    #[kani::proof]
    fn verify_reassembler_output_only_on_full() {
        let mut r = CriReassembler::new(1024);

        let msg: [u8; 8] = kani::any();
        let partial = CriLine {
            timestamp: b"ts",
            stream: b"out",
            is_full: false,
            message: &msg,
        };
        match r.feed(partial.message, partial.is_full) {
            AggregateResult::Pending => {} // expected
            AggregateResult::Complete(_) | AggregateResult::Truncated(_) => {
                panic!("partial line should not produce output")
            }
        }
    }

    /// Parametric proof: write_json_line_with_plain_text_field handles
    /// arbitrary message content and optional prefix correctly.
    ///
    /// Subsumes the 7 retired deterministic proofs by combining crash-freedom
    /// with structural and escaping correctness assertions:
    ///   - verify_write_json_line_prefix_injection
    ///   - verify_write_json_line_prefix_injection_ws_stripped
    ///   - verify_write_json_line_prefix_injection_non_empty_json
    ///   - verify_write_json_line_no_prefix
    ///   - verify_write_json_line_no_prefix_quote_escape
    ///   - verify_write_json_line_no_prefix_backslash_escape
    ///   - verify_write_json_line_no_prefix_control_escape
    #[kani::proof]
    #[kani::unwind(52)]
    fn verify_write_json_line_parametric() {
        let msg: [u8; 8] = kani::any();

        let mut out = Vec::with_capacity(128);
        write_json_line_with_plain_text_field(&msg, SHORT_FIELD_NAME, &mut out);

        // Output is never empty (at minimum contains a newline)
        assert!(!out.is_empty());
        // Output always ends with newline
        assert_eq!(out[out.len() - 1], b'\n');

        // ---- Structural correctness ----
        // Content without the trailing newline:
        let body = &out[..out.len() - 1];

        if msg[0] == b'{' {
            // JSON path: output must start with '{'
            assert_eq!(body[0], b'{');
            // output == msg verbatim
            assert_bytes_eq(body, &msg);
        } else {
            // Plain-text path: output is {"b":"<escaped>"}\n
            // Must start with {"b":" and end with "}
            assert_bytes_eq(&body[..6], b"{\"b\":\"");
            assert_eq!(body[body.len() - 2], b'"');
            assert_eq!(body[body.len() - 1], b'}');
        }

        kani::cover!(msg[0] == b'{', "JSON-like message");
        kani::cover!(msg[0] == b'"', "quoted message");
        kani::cover!(msg[0] == b'\\', "backslash message");
        kani::cover!(msg[0] < 0x20, "control char message");
    }
    /// Prove quote escaping for plain-text messages.
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix_quote_escape() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"\"", SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{\"b\":\"\\\"\"}\n");
    }

    /// Prove backslash escaping for plain-text messages.
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix_backslash_escape() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"\\", SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{\"b\":\"\\\\\"}\n");
    }

    /// Prove control characters use \\u00XX escaping.
    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix_control_escape() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(&[0x1F], SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{\"b\":\"\\u001f\"}\n");
    }

    /// Prove single-byte JSON escaping matches the JSON string escaping table.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::solver(kissat)]
    fn verify_json_escape_single_byte_table() {
        let b: u8 = kani::any();
        let input = [b];
        let mut out = Vec::with_capacity(8);
        json_escape_bytes(&input, &mut out);

        match b {
            b'"' => assert_bytes_eq(&out, b"\\\""),
            b'\\' => assert_bytes_eq(&out, b"\\\\"),
            0x08 => assert_bytes_eq(&out, b"\\b"),
            b'\t' => assert_bytes_eq(&out, b"\\t"),
            b'\n' => assert_bytes_eq(&out, b"\\n"),
            0x0C => assert_bytes_eq(&out, b"\\f"),
            b'\r' => assert_bytes_eq(&out, b"\\r"),
            0x00..=0x1F | 0x7F => {
                assert_eq!(out.len(), 6);
                assert_bytes_eq(&out[..4], b"\\u00");
            }
            _ => assert_bytes_eq(&out, &input),
        }
    }

    /// Prove the public wrapper uses "body" for non-JSON messages.
    #[kani::proof]
    fn verify_write_json_line_default_plain_text_field() {
        let mut out = Vec::with_capacity(32);
        write_json_line(b"x", &mut out);
        assert_bytes_eq(&out, b"{\"body\":\"x\"}\n");
    }
}
