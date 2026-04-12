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

/// Parsed CRI log line. References point into the original byte slice (zero-copy).
use alloc::vec::Vec;
#[derive(Debug)]
/// Parsed CRI log line fields.
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

/// Result of feeding a line to [`CriReassembler`].
pub enum ReassembleResult<'a> {
    /// A complete (un-truncated) message is ready.
    Complete(&'a [u8]),
    /// A complete message is ready, but one or more chunks were truncated
    /// because the assembled size exceeded [`CriReassembler`]'s `max_line_size`.
    /// Callers should log a warning and/or increment a diagnostics counter.
    Truncated(&'a [u8]),
    /// Partial line buffered internally; no output yet.
    Pending,
}

/// CRI partial line reassembler. Buffers "P" (partial) chunks and emits
/// the combined line when an "F" (full) chunk arrives.
pub struct CriReassembler {
    /// Buffer for accumulating partial line chunks.
    partial_buf: Vec<u8>,
    /// Maximum assembled line size. Lines exceeding this are truncated.
    max_line_size: usize,
    /// Set to `true` when any chunk in the current P/F sequence was truncated
    /// due to `max_line_size`. Reset in [`CriReassembler::reset`].
    truncated: bool,
}

impl CriReassembler {
    /// Create a new reassembler with the given max line size.
    pub fn new(max_line_size: usize) -> Self {
        CriReassembler {
            partial_buf: Vec::new(),
            max_line_size,
            truncated: false,
        }
    }

    /// Feed a parsed CRI line. Returns a [`ReassembleResult`] indicating whether
    /// a complete message is ready, whether it was truncated, or whether more
    /// partial chunks are still expected.
    ///
    /// - `ReassembleResult::Complete` — F line with no truncation.
    /// - `ReassembleResult::Truncated` — F line, but one or more chunks exceeded
    ///   `max_line_size` and some bytes were silently dropped. Callers should
    ///   log a warning.
    /// - `ReassembleResult::Pending` — P line; data has been buffered.
    pub fn feed<'a>(&'a mut self, cri: &CriLine<'_>) -> ReassembleResult<'a> {
        if cri.is_full {
            if self.partial_buf.is_empty() {
                // Common fast path: complete line, no partials pending.
                let to_add = cri.message.len().min(self.max_line_size);
                let was_truncated = to_add < cri.message.len();
                self.partial_buf.extend_from_slice(&cri.message[..to_add]);
                if was_truncated {
                    ReassembleResult::Truncated(&self.partial_buf)
                } else {
                    ReassembleResult::Complete(&self.partial_buf)
                }
            } else {
                // Append the final chunk to the partial buffer.
                let remaining = self.max_line_size.saturating_sub(self.partial_buf.len());
                let to_add = cri.message.len().min(remaining);
                if to_add < cri.message.len() {
                    self.truncated = true;
                }
                self.partial_buf.extend_from_slice(&cri.message[..to_add]);
                if self.truncated {
                    ReassembleResult::Truncated(&self.partial_buf)
                } else {
                    ReassembleResult::Complete(&self.partial_buf)
                }
            }
        } else {
            // Partial line — buffer it.
            let remaining = self.max_line_size.saturating_sub(self.partial_buf.len());
            let to_add = cri.message.len().min(remaining);
            if to_add < cri.message.len() {
                self.truncated = true;
            }
            self.partial_buf.extend_from_slice(&cri.message[..to_add]);
            ReassembleResult::Pending
        }
    }

    /// Reset the partial buffer and truncation flag (call after consuming the emitted line).
    pub fn reset(&mut self) {
        self.partial_buf.clear();
        self.truncated = false;
    }
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
    let mut line_start = 0;

    for pos in memchr::memchr_iter(b'\n', chunk) {
        let line = &chunk[line_start..pos];
        line_start = pos + 1;

        if line.is_empty() {
            continue;
        }

        match parse_cri_line(line) {
            Some(cri) => match reassembler.feed(&cri) {
                ReassembleResult::Complete(msg) => {
                    emit(msg);
                    count += 1;
                    reassembler.reset();
                }
                ReassembleResult::Truncated(msg) => {
                    emit(msg);
                    count += 1;
                    errors += 1;
                    reassembler.reset();
                }
                ReassembleResult::Pending => {}
            },
            None => errors += 1,
        }
    }

    (count, errors)
}

/// Process CRI data and write extracted messages directly into an output buffer.
/// Each message is written as: optional JSON prefix + message bytes + newline.
///
/// For the common case (full lines, no partials), the message bytes come straight
/// from the input chunk — zero per-line allocation. Only partial line reassembly
/// copies into the reassembler's internal buffer.
///
/// `json_prefix`: if Some, injected after the opening `{` of each JSON message.
///   Example: `Some(b"\"kubernetes.pod_name\":\"my-pod\",")` turns
///   `{"msg":"hi"}` into `{"kubernetes.pod_name":"my-pod","msg":"hi"}`
///
/// Returns `(lines_ok, parse_errors)` where `parse_errors` is the number of
/// non-empty lines that could not be parsed as valid CRI format **plus** the
/// number of lines that were truncated due to `max_line_size`.
#[allow(dead_code)] // used by tests; will be called from pipeline code once per-stream integration lands
pub(crate) fn process_cri_to_buf(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    json_prefix: Option<&[u8]>,
    out: &mut Vec<u8>,
) -> (usize, usize) {
    process_cri_to_buf_with_plain_text_field(chunk, reassembler, json_prefix, "body", out)
}

/// Same as `process_cri_to_buf` but allows choosing the field name used when
/// wrapping non-JSON plain-text messages.
pub fn process_cri_to_buf_with_plain_text_field(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    json_prefix: Option<&[u8]>,
    plain_text_field_name: &str,
    out: &mut Vec<u8>,
) -> (usize, usize) {
    let mut count = 0;
    let mut errors = 0;
    let mut line_start = 0;

    for pos in memchr::memchr_iter(b'\n', chunk) {
        let line = &chunk[line_start..pos];
        line_start = pos + 1;

        if line.is_empty() {
            continue;
        }

        match parse_cri_line(line) {
            Some(cri) => match reassembler.feed(&cri) {
                ReassembleResult::Complete(msg) => {
                    write_json_line_for_plain_text_field(
                        msg,
                        json_prefix,
                        plain_text_field_name,
                        out,
                    );
                    count += 1;
                    reassembler.reset();
                }
                ReassembleResult::Truncated(msg) => {
                    write_json_line_for_plain_text_field(
                        msg,
                        json_prefix,
                        plain_text_field_name,
                        out,
                    );
                    count += 1;
                    errors += 1;
                    reassembler.reset();
                }
                ReassembleResult::Pending => {}
            },
            None => errors += 1,
        }
    }

    (count, errors)
}

#[inline]
fn write_json_line_for_plain_text_field(
    msg: &[u8],
    json_prefix: Option<&[u8]>,
    plain_text_field_name: &str,
    out: &mut Vec<u8>,
) {
    if plain_text_field_name == "body" {
        write_json_line(msg, json_prefix, out);
    } else {
        write_json_line_with_plain_text_field(msg, json_prefix, plain_text_field_name, out);
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

/// Write a single message into the output buffer with optional JSON prefix injection.
///
/// If `msg` starts with `{` it is treated as a JSON object and the optional
/// `json_prefix` is injected after the opening brace.  Otherwise `msg` is
/// plain text and is written as
/// `{"<plain_text_field_name>":"<json-escaped msg>"}` so that no
/// content is silently lost when the downstream scanner processes the line.
#[inline]
#[allow(dead_code)] // called by process_cri_to_buf
fn write_json_line(msg: &[u8], json_prefix: Option<&[u8]>, out: &mut Vec<u8>) {
    write_json_line_with_plain_text_field(msg, json_prefix, "body", out);
}

#[inline]
fn write_json_line_with_plain_text_field(
    msg: &[u8],
    json_prefix: Option<&[u8]>,
    plain_text_field_name: &str,
    out: &mut Vec<u8>,
) {
    if msg.first() == Some(&b'{') {
        if let Some(prefix) = json_prefix {
            out.push(b'{');

            let mut is_empty_obj = false;
            let mut i = 1;
            while i < msg.len() {
                match msg[i] {
                    b' ' | b'\t' | b'\r' | b'\n' => i += 1,
                    b'}' => {
                        is_empty_obj = true;
                        break;
                    }
                    _ => break,
                }
            }

            let mut prefix_end = prefix.len();
            while prefix_end > 0 && matches!(prefix[prefix_end - 1], b' ' | b'\t' | b'\r' | b'\n') {
                prefix_end -= 1;
            }

            if is_empty_obj && prefix_end > 0 && prefix[prefix_end - 1] == b',' {
                out.extend_from_slice(&prefix[..prefix_end - 1]);
                // Append the trailing whitespace we stripped from prefix
                out.extend_from_slice(&prefix[prefix_end..]);
            } else {
                out.extend_from_slice(prefix);
            }

            out.extend_from_slice(&msg[1..]);
        } else {
            out.extend_from_slice(msg);
        }
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
        assert!(matches!(reassembler.feed(&p1), ReassembleResult::Pending));

        let p2 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P second part").unwrap();
        assert!(matches!(reassembler.feed(&p2), ReassembleResult::Pending));

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F final part").unwrap();
        match reassembler.feed(&f) {
            ReassembleResult::Complete(complete) => {
                assert_eq!(complete, b"first partsecond partfinal part");
            }
            ReassembleResult::Truncated(_) => panic!("expected Complete, got Truncated"),
            ReassembleResult::Pending => panic!("expected Complete, got Pending"),
        }
        reassembler.reset();
    }

    #[test]
    fn test_reassemble_no_partials() {
        let mut reassembler = CriReassembler::new(1024 * 1024);

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F complete line").unwrap();
        match reassembler.feed(&f) {
            ReassembleResult::Complete(complete) => {
                assert_eq!(complete, b"complete line");
            }
            ReassembleResult::Truncated(_) => panic!("expected Complete, got Truncated"),
            ReassembleResult::Pending => panic!("expected Complete, got Pending"),
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
        let (count, errors) = process_cri_to_buf(chunk, &mut reassembler, None, &mut out);
        assert_eq!(count, 1);
        assert_eq!(errors, 1);
    }

    #[test]
    fn test_write_json_line_empty_object_no_trailing_comma() {
        // If msg is an empty JSON object like {}, the injected prefix should not
        // end with a comma. Otherwise, the result is `{"prefix":"val",}` which
        // is invalid JSON.
        let mut out = Vec::new();
        write_json_line(b"{}", Some(b"\"prefix\":\"val\","), &mut out);
        assert_eq!(out, b"{\"prefix\":\"val\"}\n");

        let mut out = Vec::new();
        write_json_line(b"{ \t\r\n}", Some(b"\"prefix\":\"val\","), &mut out);
        assert_eq!(out, b"{\"prefix\":\"val\" \t\r\n}\n");
    }

    #[test]
    fn test_write_json_line_empty_object_prefix_trailing_whitespace_after_comma() {
        // Regression: prefix like `"k":"v", ` (comma then whitespace) must have
        // the comma stripped and the trailing whitespace preserved.
        let mut out = Vec::new();
        write_json_line(b"{}", Some(b"\"k\":\"v\", "), &mut out);
        assert_eq!(out, b"{\"k\":\"v\" }\n");

        // Tab and CRLF after comma
        let mut out = Vec::new();
        write_json_line(b"{}", Some(b"\"k\":\"v\",\t\r\n"), &mut out);
        assert_eq!(out, b"{\"k\":\"v\"\t\r\n}\n");
    }

    #[test]
    fn test_write_json_line_plain_text_wrapped_as_body() {
        // Plain-text (non-JSON) messages must be wrapped as {"body":"..."}.
        let mut out = Vec::new();
        write_json_line(b"application started", None, &mut out);
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
            None,
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
        write_json_line(b"say \"hello\"", None, &mut out);
        assert_eq!(out, b"{\"body\":\"say \\\"hello\\\"\"}\n");
    }

    #[test]
    fn test_process_cri_to_buf_plain_text_wrapped() {
        // Plain-text CRI messages should be emitted as {"body":"..."} lines.
        let chunk = b"2024-01-15T10:30:00Z stdout F application started\n\
                       2024-01-15T10:30:01Z stdout F {\"msg\":\"ok\"}\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut out = Vec::new();
        let (count, errors) = process_cri_to_buf(chunk, &mut reassembler, None, &mut out);
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
        reassembler.feed(&p1);

        let p2 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P abcdefghij").unwrap();
        reassembler.feed(&p2);

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F KLMNOPQRST").unwrap();
        // The assembled sequence exceeds max_line_size=20, so Truncated is expected.
        match reassembler.feed(&f) {
            ReassembleResult::Truncated(complete) => {
                assert_eq!(complete.len(), 20);
            }
            ReassembleResult::Complete(complete) => {
                panic!(
                    "expected Truncated, got Complete with len={}",
                    complete.len()
                )
            }
            ReassembleResult::Pending => panic!("expected Truncated, got Pending"),
        }
        reassembler.reset();
    }

    #[test]
    fn test_max_line_size_f_only_truncated() {
        // A single F line that exceeds max_line_size must return Truncated.
        let mut reassembler = CriReassembler::new(5);
        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F hello world").unwrap();
        match reassembler.feed(&f) {
            ReassembleResult::Truncated(out) => {
                assert_eq!(out, b"hello");
            }
            ReassembleResult::Complete(out) => {
                panic!("expected Truncated, got Complete: {:?}", out)
            }
            ReassembleResult::Pending => panic!("expected Truncated"),
        }
        reassembler.reset();
    }
}

// ---------------------------------------------------------------------------
// Kani formal verification proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    const SHORT_FIELD_NAME: &str = "b";

    fn assert_bytes_eq(actual: &[u8], expected: &[u8]) {
        assert_eq!(actual.len(), expected.len());
        let mut i = 0;
        while i < expected.len() {
            assert_eq!(actual[i], expected[i]);
            i += 1;
        }
    }

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
    #[kani::unwind(40)]
    fn verify_process_cri_to_buf_with_plain_text_field_no_panic() {
        let chunk: [u8; 32] = kani::any();
        let prefix: [u8; 4] = kani::any();

        let mut out = Vec::new();
        let mut reassembler = CriReassembler::new(64);
        let _ = process_cri_to_buf_with_plain_text_field(
            &chunk,
            &mut reassembler,
            Some(&prefix),
            "body",
            &mut out,
        );

        out.clear();
        reassembler.reset();
        let _ = process_cri_to_buf_with_plain_text_field(
            &chunk,
            &mut reassembler,
            Some(&prefix),
            "msg",
            &mut out,
        );
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
        let _ = r.feed(&partial);

        let msg2: [u8; 8] = kani::any();
        let full = CriLine {
            timestamp: b"ts",
            stream: b"out",
            is_full: true,
            message: &msg2,
        };
        match r.feed(&full) {
            ReassembleResult::Complete(output) | ReassembleResult::Truncated(output) => {
                assert!(output.len() <= max_size, "P+F output exceeds max_line_size");

                // Guard vacuity: verify constraint allows meaningful cases
                kani::cover!(output.len() > 8, "P+F concatenation occurred");
                kani::cover!(output.len() == max_size, "output truncated at max");
                kani::cover!(output.len() < max_size, "output under max");
            }
            ReassembleResult::Pending => {}
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
        match r.feed(&full) {
            ReassembleResult::Complete(output) | ReassembleResult::Truncated(output) => {
                assert!(
                    output.len() <= max_size,
                    "F-only output exceeds max_line_size"
                );

                // Guard vacuity: verify fast path coverage
                kani::cover!(output.len() == 8, "full message fits");
                kani::cover!(output.len() < 8, "message truncated");
                kani::cover!(max_size >= 8, "max allows full message");
            }
            ReassembleResult::Pending => {}
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
        match r.feed(&partial) {
            ReassembleResult::Pending => {} // expected
            ReassembleResult::Complete(_) | ReassembleResult::Truncated(_) => {
                panic!("partial line should not produce output")
            }
        }
    }

    /// Prove the prefix injector strips a trailing comma for empty objects.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_prefix_injection() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"{}", Some(b"a,"), SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{a}\n");
    }

    /// Prove comma stripping also works when the prefix ends in whitespace.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_prefix_injection_ws_stripped() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"{}", Some(b", "), SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{ }\n");
    }

    /// Prove non-empty JSON gets the prefix injected without comma stripping.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_prefix_injection_non_empty_json() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"{x", Some(b"ab"), SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{abx\n");
    }

    /// Prove JSON messages pass through unchanged when no prefix is present.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"{}", None, SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{}\n");
    }

    /// Prove quote escaping for plain-text messages.
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix_quote_escape() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"\"", None, SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{\"b\":\"\\\"\"}\n");
    }

    /// Prove backslash escaping for plain-text messages.
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix_backslash_escape() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(b"\\", None, SHORT_FIELD_NAME, &mut out);
        assert_bytes_eq(&out, b"{\"b\":\"\\\\\"}\n");
    }

    /// Prove control characters use \\u00XX escaping.
    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::solver(kissat)]
    fn verify_write_json_line_no_prefix_control_escape() {
        let mut out = Vec::with_capacity(64);
        write_json_line_with_plain_text_field(&[0x1F], None, SHORT_FIELD_NAME, &mut out);
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
        write_json_line(b"x", None, &mut out);
        assert_bytes_eq(&out, b"{\"body\":\"x\"}\n");
    }
}
