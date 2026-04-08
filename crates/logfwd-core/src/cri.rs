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
        stream: &line[sp1 + 1..sp2],
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
pub fn process_cri_chunk<F>(
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
pub fn process_cri_to_buf(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    json_prefix: Option<&[u8]>,
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
                    write_json_line(msg, json_prefix, out);
                    count += 1;
                    reassembler.reset();
                }
                ReassembleResult::Truncated(msg) => {
                    write_json_line(msg, json_prefix, out);
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
/// plain text and is written as `{"_raw":"<json-escaped msg>"}` so that no
/// content is silently lost when the downstream scanner processes the line.
#[inline]
fn write_json_line(msg: &[u8], json_prefix: Option<&[u8]>, out: &mut Vec<u8>) {
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
        // Non-JSON plain text: wrap as {"_raw":"<escaped>"} so the scanner
        // sees a valid JSON object and message content is preserved.
        out.extend_from_slice(b"{\"_raw\":\"");
        json_escape_bytes(msg, out);
        out.extend_from_slice(b"\"}");
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_write_json_line_plain_text_wrapped_as_raw() {
        // Plain-text (non-JSON) messages must be wrapped as {"_raw":"..."}.
        let mut out = Vec::new();
        write_json_line(b"application started", None, &mut out);
        assert_eq!(out, b"{\"_raw\":\"application started\"}\n");
    }

    #[test]
    fn test_write_json_line_plain_text_escapes_special_chars() {
        // JSON-special characters in the message must be escaped.
        let mut out = Vec::new();
        write_json_line(b"say \"hello\"", None, &mut out);
        assert_eq!(out, b"{\"_raw\":\"say \\\"hello\\\"\"}\n");
    }

    #[test]
    fn test_process_cri_to_buf_plain_text_wrapped() {
        // Plain-text CRI messages should be emitted as {"_raw":"..."} lines.
        let chunk = b"2024-01-15T10:30:00Z stdout F application started\n\
                       2024-01-15T10:30:01Z stdout F {\"msg\":\"ok\"}\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut out = Vec::new();
        let (count, errors) = process_cri_to_buf(chunk, &mut reassembler, None, &mut out);
        assert_eq!(count, 2);
        assert_eq!(errors, 0);
        assert_eq!(
            out,
            b"{\"_raw\":\"application started\"}\n{\"msg\":\"ok\"}\n"
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
            assert!(!cri.stream.is_empty(), "empty stream");

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

    /// Prove write_json_line with prefix correctly injects after opening brace,
    /// and wraps non-JSON messages as {"_raw":"..."}.
    ///
    /// Input: 2-byte msg + 2-byte prefix = 4 symbolic bytes. Reduced from
    /// 8+4->4+4->2+2 to stay under CI timeout: 4+4 bytes generated ~21K VCCs /
    /// ~5K post-simplification that timed out kissat on ubuntu-latest runners.
    /// 2+2 still covers:
    ///   - msg[0] drives JSON vs non-JSON branch
    ///   - msg[1] (non-JSON: through json_escape_bytes; JSON: verbatim copy)
    ///   - prefix injection, including the empty-object path that strips a
    ///     trailing comma when the JSON payload is just "{}"
    /// Vec::with_capacity(64) pre-allocates to avoid realloc VCC explosion.
    #[kani::proof]
    #[kani::unwind(4)] // 2 iterations + 2 margin
    #[kani::solver(kissat)] // json_escape_bytes loop × 2 symbolic bytes: kissat outperforms cadical
    fn verify_write_json_line_prefix_injection() {
        let msg: [u8; 2] = kani::any();
        let prefix: [u8; 2] = kani::any();
        // Pre-allocate: { (1) + prefix (2) + msg[1..] (1) + \n (1) = 5 bytes JSON path;
        // or {"_raw":"..."} path up to 64 bytes. Capacity 64 avoids all reallocs.
        let mut out = Vec::with_capacity(64);

        // Guard vacuity: ensure both paths are reachable
        kani::cover!(msg[0] == b'{', "JSON path reachable");
        kani::cover!(msg[0] != b'{', "non-JSON path reachable");

        // Guard vacuity for json_escape_bytes arms in the non-JSON path.
        // msg[1] drives the escape since msg[0] controls the JSON/plain split.
        kani::cover!(
            msg[0] != b'{' && msg[1] == b'"',
            "quote escape arm reachable"
        );
        kani::cover!(
            msg[0] != b'{' && msg[1] == b'\\',
            "backslash escape arm reachable"
        );
        kani::cover!(
            msg[0] != b'{' && msg[1] < 0x20,
            "control-char escape arm reachable"
        );

        // Guard vacuity for the empty-object comma-strip path (is_empty_obj branch).
        // For a 2-byte msg, is_empty_obj triggers only when msg == b"{}".
        // The code strips trailing whitespace from prefix before checking for a
        // trailing comma, so we cover both the direct case (prefix[1] == b',')
        // and the whitespace-stripped case (prefix[0] == b',' with ws suffix).
        kani::cover!(
            msg[0] == b'{' && msg[1] == b'}' && prefix[1] == b',',
            "empty-object comma-strip path reachable (direct)"
        );
        kani::cover!(
            msg[0] == b'{'
                && msg[1] == b'}'
                && prefix[0] == b','
                && matches!(prefix[1], b' ' | b'\t' | b'\r' | b'\n'),
            "empty-object comma-strip path reachable (ws-stripped)"
        );
        kani::cover!(
            msg[0] == b'{' && msg[1] != b'}',
            "JSON path without comma-strip reachable"
        );

        write_json_line(&msg, Some(&prefix), &mut out);

        if msg[0] == b'{' {
            // Mirror the code's prefix_end whitespace-stripping logic.
            let is_ws = |b: u8| matches!(b, b' ' | b'\t' | b'\r' | b'\n');
            let prefix_end: usize = if is_ws(prefix[1]) {
                if is_ws(prefix[0]) { 0 } else { 1 }
            } else {
                2
            };

            let is_empty_obj = msg[1] == b'}';
            let strips_trailing_comma =
                is_empty_obj && prefix_end > 0 && prefix[prefix_end - 1] == b',';

            // Output: { + (possibly trimmed) prefix + msg[1..] + \n
            assert_eq!(out[0], b'{');

            if strips_trailing_comma {
                // Code emits prefix[..prefix_end-1] then prefix[prefix_end..],
                // then msg[1..], then \n.
                let trimmed_len = (prefix_end - 1) + (2 - prefix_end);
                let expected_len = 1 + trimmed_len + 1 + 1; // '{' + prefix parts + msg[1] + '\n'
                assert_eq!(out.len(), expected_len);
                assert_eq!(out[out.len() - 1], b'\n');
                assert_eq!(out[out.len() - 2], msg[1]);
            } else {
                // Full prefix emitted: { + prefix[0] + prefix[1] + msg[1] + \n
                assert_eq!(out.len(), 5);
                assert_eq!(out[1], prefix[0]);
                assert_eq!(out[2], prefix[1]);
                assert_eq!(out[3], msg[1]);
                assert_eq!(out[4], b'\n');
            }
        } else {
            // Non-JSON: wrapped as {"_raw":"..."}\n — ends with \n
            assert_eq!(out[out.len() - 1], b'\n');
            // Output starts with {"_raw":"  — check byte-by-byte (no memcmp).
            assert_eq!(out[0], b'{');
            assert_eq!(out[1], b'"');
            assert_eq!(out[2], b'_');
            assert_eq!(out[3], b'r');
            assert_eq!(out[4], b'a');
            assert_eq!(out[5], b'w');
            assert_eq!(out[6], b'"');
            assert_eq!(out[7], b':');
            assert_eq!(out[8], b'"');
        }
    }

    /// Prove write_json_line without prefix passes JSON through and wraps plain text.
    ///
    /// Input: 2 symbolic bytes. Reduced from 8→4→3→2 to keep SAT solving under
    /// CI timeout (3 bytes still produced thousands of VCCs that timed out in
    /// kissat on ubuntu-latest runners). 2 bytes still covers every escape path
    /// and both JSON/non-JSON branches — the second byte independently exercises
    /// the json_escape_bytes match arms.
    /// Vec::with_capacity(64) pre-allocates to avoid realloc VCC explosion.
    #[kani::proof]
    #[kani::unwind(4)] // 2 iterations + 2 margin
    #[kani::solver(kissat)] // json_escape_bytes loop × 2 symbolic bytes: kissat outperforms cadical
    fn verify_write_json_line_no_prefix() {
        let msg: [u8; 2] = kani::any();
        let mut out = Vec::with_capacity(64);

        // Guard vacuity: ensure both paths are reachable
        kani::cover!(msg[0] == b'{', "JSON path reachable");
        kani::cover!(msg[0] != b'{', "non-JSON path reachable");

        // Guard vacuity for json_escape_bytes arms — Kani must find a model
        // where each escape branch is exercised (second byte drives escape
        // since first byte is fixed to non-{ for the non-JSON path).
        kani::cover!(
            msg[0] != b'{' && msg[1] == b'"',
            "quote escape arm reachable"
        );
        kani::cover!(
            msg[0] != b'{' && msg[1] == b'\\',
            "backslash escape arm reachable"
        );
        kani::cover!(
            msg[0] != b'{' && msg[1] < 0x20,
            "control-char escape arm reachable"
        );

        write_json_line(&msg, None, &mut out);

        // Always ends with \n
        assert_eq!(out[out.len() - 1], b'\n');

        if msg[0] == b'{' {
            // JSON message passed through unchanged: msg + \n
            assert_eq!(out.len(), 3);
            // Check each byte individually to avoid memcmp VCC explosion
            assert_eq!(out[0], msg[0]);
            assert_eq!(out[1], msg[1]);
        } else {
            // Non-JSON: wrapped as {"_raw":"..."}\n — check prefix byte by byte
            assert_eq!(out[0], b'{');
            assert_eq!(out[1], b'"');
            assert_eq!(out[2], b'_');
            assert_eq!(out[3], b'r');
            assert_eq!(out[4], b'a');
            assert_eq!(out[5], b'w');
            assert_eq!(out[6], b'"');
            assert_eq!(out[7], b':');
            assert_eq!(out[8], b'"');
        }
    }
}
