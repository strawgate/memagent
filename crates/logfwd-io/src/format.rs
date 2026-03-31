//! Format parsers: convert raw input bytes into newline-delimited JSON.
//!
//! Each format (CRI, JSON, Raw) implements the [`FormatParser`] trait.
//! The pipeline feeds raw bytes in and gets back newline-terminated JSON
//! lines suitable for the SIMD scanner.

use logfwd_core::cri::{self, CriReassembler};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Converts raw input bytes into newline-delimited JSON lines.
///
/// Implementations carry state across calls (partial lines, CRI reassembly).
/// All methods are called from a single thread — no `Sync` required.
pub trait FormatParser: Send {
    /// Process a chunk of raw bytes, appending complete newline-delimited
    /// JSON lines to `out`. Returns `(lines_ok, parse_errors)` where
    /// `parse_errors` counts lines that failed format-specific parsing
    /// (e.g. malformed CRI lines).
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> (usize, usize);

    /// Reset internal state (partial line buffers, CRI reassembly).
    /// Called on file rotation or truncation.
    fn reset(&mut self);
}

// ---------------------------------------------------------------------------
// JSON / Auto
// ---------------------------------------------------------------------------

/// Passes through newline-delimited JSON, carrying partial lines across calls.
pub struct JsonParser {
    partial: Vec<u8>,
    max_line_size: usize,
}

impl JsonParser {
    pub fn new(max_line_size: usize) -> Self {
        Self {
            partial: Vec::new(),
            max_line_size,
        }
    }
}

impl FormatParser for JsonParser {
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> (usize, usize) {
        let mut count = 0;
        let mut start = 0;
        for pos in memchr::memchr_iter(b'\n', bytes) {
            let chunk = &bytes[start..pos];
            if self.partial.is_empty() {
                let to_add = chunk.len().min(self.max_line_size);
                if to_add > 0 {
                    out.extend_from_slice(&chunk[..to_add]);
                    out.push(b'\n');
                    count += 1;
                }
            } else {
                let remaining = self.max_line_size.saturating_sub(self.partial.len());
                let to_add = chunk.len().min(remaining);
                self.partial.extend_from_slice(&chunk[..to_add]);
                if !self.partial.is_empty() {
                    out.extend_from_slice(&self.partial);
                    out.push(b'\n');
                    count += 1;
                }
                self.partial.clear();
            }
            start = pos + 1;
        }
        if start < bytes.len() {
            let remaining = self.max_line_size.saturating_sub(self.partial.len());
            let to_add = bytes[start..].len().min(remaining);
            self.partial.extend_from_slice(&bytes[start..start + to_add]);
        }
        (count, 0)
    }

    fn reset(&mut self) {
        self.partial.clear();
    }
}

// ---------------------------------------------------------------------------
// Raw
// ---------------------------------------------------------------------------

/// Wraps each line as `{"_raw":"<escaped>"}\n`.
pub struct RawParser {
    partial: Vec<u8>,
    max_line_size: usize,
}

impl RawParser {
    pub fn new(max_line_size: usize) -> Self {
        Self {
            partial: Vec::new(),
            max_line_size,
        }
    }
}

impl FormatParser for RawParser {
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> (usize, usize) {
        let mut count = 0;
        let mut start = 0;
        for pos in memchr::memchr_iter(b'\n', bytes) {
            let chunk = &bytes[start..pos];
            let line = if self.partial.is_empty() {
                let to_add = chunk.len().min(self.max_line_size);
                &chunk[..to_add]
            } else {
                let remaining = self.max_line_size.saturating_sub(self.partial.len());
                let to_add = chunk.len().min(remaining);
                self.partial.extend_from_slice(&chunk[..to_add]);
                self.partial.as_slice()
            };

            if !line.is_empty() {
                out.extend_from_slice(b"{\"_raw\":\"");
                for &b in line {
                    match b {
                        b'"' => out.extend_from_slice(b"\\\""),
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        b'\t' => out.extend_from_slice(b"\\t"),
                        b if b < 0x20 => {
                            // Escape control characters per RFC 8259.
                            let _ = std::io::Write::write_fmt(out, format_args!("\\u{:04x}", b));
                        }
                        _ => out.push(b),
                    }
                }
                out.extend_from_slice(b"\"}\n");
                count += 1;
            }

            if !self.partial.is_empty() {
                self.partial.clear();
            }
            start = pos + 1;
        }
        if start < bytes.len() {
            let remaining = self.max_line_size.saturating_sub(self.partial.len());
            let to_add = bytes[start..].len().min(remaining);
            self.partial.extend_from_slice(&bytes[start..start + to_add]);
        }
        (count, 0)
    }

    fn reset(&mut self) {
        self.partial.clear();
    }
}

// ---------------------------------------------------------------------------
// CRI
// ---------------------------------------------------------------------------

/// Parses CRI container log format, reassembles partial lines, and emits
/// the extracted message as a JSON line.
pub struct CriParser {
    reassembler: CriReassembler,
    /// Bytes from the previous chunk that did not end with a newline.
    partial: Vec<u8>,
    /// Max line size for both reassembler and the outer partial buffer.
    /// Outer buffer limit is slightly larger to account for CRI envelope
    /// overhead (timestamp, stream, flags).
    max_line_size: usize,
}

impl CriParser {
    pub fn new(max_line_size: usize) -> Self {
        CriParser {
            reassembler: CriReassembler::new(max_line_size),
            partial: Vec::new(),
            max_line_size,
        }
    }
}

impl FormatParser for CriParser {
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> (usize, usize) {
        // Enforce max_line_size on outer partial buffer too.
        // A CRI line is typically: <timestamp> <stream> <flag> <message>\n
        // Timestamp is ~30 bytes, stream is ~7, flag is 1, plus spaces.
        // 128 bytes of overhead is plenty.
        let limit = self.max_line_size.saturating_add(128);

        let mut start = 0;
        let mut total_lines = 0;
        let mut total_errors = 0;

        for pos in memchr::memchr_iter(b'\n', bytes) {
            let chunk = &bytes[start..pos + 1];
            let (n, err) = if self.partial.is_empty() {
                cri::process_cri_to_buf(chunk, &mut self.reassembler, None, out)
            } else {
                let remaining = limit.saturating_sub(self.partial.len());
                let to_add = chunk.len().min(remaining);
                self.partial.extend_from_slice(&chunk[..to_add]);
                let (n, err) =
                    cri::process_cri_to_buf(&self.partial, &mut self.reassembler, None, out);
                self.partial.clear();
                (n, err)
            };
            total_lines += n;
            total_errors += err;
            start = pos + 1;
        }

        if start < bytes.len() {
            let remaining = limit.saturating_sub(self.partial.len());
            let to_add = bytes[start..].len().min(remaining);
            self.partial.extend_from_slice(&bytes[start..start + to_add]);
        }

        (total_lines, total_errors)
    }

    fn reset(&mut self) {
        self.reassembler.reset();
        self.partial.clear();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_basic() {
        let mut parser = JsonParser::new(1024);
        let mut out = Vec::new();
        let (n, errors) = parser.process(b"{\"a\":1}\n{\"b\":2}\n", &mut out);
        assert_eq!(n, 2);
        assert_eq!(errors, 0);
        assert_eq!(out, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn json_partial_carry() {
        let mut parser = JsonParser::new(1024);
        let mut out = Vec::new();

        let (n1, _) = parser.process(b"{\"a\":1}\n{\"b\":", &mut out);
        assert_eq!(n1, 1);

        let (n2, _) = parser.process(b"2}\n", &mut out);
        assert_eq!(n2, 1);

        assert_eq!(out, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn json_reset_clears_partial() {
        let mut parser = JsonParser::new(1024);
        let mut out = Vec::new();

        parser.process(b"{\"partial\":", &mut out);
        assert_eq!(out.len(), 0);

        parser.reset();
        let (n, _) = parser.process(b"{\"fresh\":1}\n", &mut out);
        assert_eq!(n, 1);
        assert_eq!(out, b"{\"fresh\":1}\n");
    }

    #[test]
    fn raw_basic() {
        let mut parser = RawParser::new(1024);
        let mut out = Vec::new();
        let (n, errors) = parser.process(b"plain line\nanother\n", &mut out);
        assert_eq!(n, 2);
        assert_eq!(errors, 0);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("{\"_raw\":\"plain line\"}\n"));
        assert!(s.contains("{\"_raw\":\"another\"}\n"));
    }

    #[test]
    fn raw_escaping() {
        let mut parser = RawParser::new(1024);
        let mut out = Vec::new();
        let (n, _) = parser.process(b"has \"quotes\" and \\backslash\n", &mut out);
        assert_eq!(n, 1);
        let s = String::from_utf8(out).unwrap();
        assert!(
            s.contains(r#"{"_raw":"has \"quotes\" and \\backslash"}"#),
            "got: {s}"
        );
    }

    #[test]
    fn cri_basic() {
        let mut parser = CriParser::new(2 * 1024 * 1024);
        let mut out = Vec::new();
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let (n, errors) = parser.process(input, &mut out);
        assert_eq!(n, 1);
        assert_eq!(errors, 0);
        assert!(out.ends_with(b"\n"));
    }

    #[test]
    fn cri_parse_errors_counted() {
        // Mix of valid and invalid CRI lines — parse errors must be reported.
        let mut parser = CriParser::new(2 * 1024 * 1024);
        let mut out = Vec::new();
        let input = b"not-a-cri-line\n\
                      2024-01-15T10:30:00Z stdout F {\"msg\":\"ok\"}\n\
                      also-bad\n";
        let (n, errors) = parser.process(input, &mut out);
        assert_eq!(n, 1);
        assert_eq!(errors, 2);
    }

    #[test]
    fn cri_partial_carry_at_chunk_boundary() {
        // A CRI line split across two read buffers must be reassembled.
        let mut parser = CriParser::new(2 * 1024 * 1024);
        let mut out = Vec::new();

        // First chunk has no newline — nothing should be emitted yet.
        let chunk1 = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hel";
        let (n1, _) = parser.process(chunk1, &mut out);
        assert_eq!(n1, 0);
        assert_eq!(out.len(), 0);

        // Second chunk completes the line.
        let chunk2 = b"lo\"}\n";
        let (n2, _) = parser.process(chunk2, &mut out);
        assert_eq!(n2, 1);
        assert!(out.ends_with(b"\n"));
        // The reassembled message must contain the full payload.
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("hello"), "got: {s}");
    }

    #[test]
    fn cri_partial_carry_multiple_chunks() {
        // Three chunks, line only complete at end of third.
        let mut parser = CriParser::new(2 * 1024 * 1024);
        let mut out = Vec::new();

        assert_eq!(parser.process(b"2024-01-15T10:30:00Z ", &mut out), (0, 0));
        assert_eq!(parser.process(b"stdout F line_content", &mut out), (0, 0));
        assert_eq!(out.len(), 0);

        let (n, _) = parser.process(b"\n", &mut out);
        assert_eq!(n, 1);
        assert!(out.ends_with(b"\n"));
    }

    #[test]
    fn cri_partial_remainder_after_newline() {
        // Chunk contains a complete line followed by a partial one.
        let mut parser = CriParser::new(2 * 1024 * 1024);
        let mut out = Vec::new();

        // First chunk: one complete line + start of a second (no trailing newline).
        let chunk1 =
            b"2024-01-15T10:30:00Z stdout F {\"n\":1}\n2024-01-15T10:30:01Z stdout F {\"n\":2}";
        let (n1, _) = parser.process(chunk1, &mut out);
        assert_eq!(n1, 1);

        // Second chunk: just the closing newline.
        let (n2, _) = parser.process(b"\n", &mut out);
        assert_eq!(n2, 1);

        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"n\":1"), "got: {s}");
        assert!(s.contains("\"n\":2"), "got: {s}");
    }

    #[test]
    fn cri_reset_clears_partial() {
        let mut parser = CriParser::new(2 * 1024 * 1024);
        let mut out = Vec::new();

        // Feed a partial chunk (no newline) to populate the partial buffer.
        parser.process(b"2024-01-15T10:30:00Z stdout F incomplete", &mut out);
        assert_eq!(out.len(), 0);

        // After reset the stale partial is discarded.
        parser.reset();
        let (n, _) = parser.process(b"2024-01-15T10:30:01Z stdout F {\"fresh\":1}\n", &mut out);
        assert_eq!(n, 1);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("fresh"), "got: {s}");
    }

    #[test]
    fn json_unbounded_partial_growth() {
        let mut parser = JsonParser::new(1024);
        let mut out = Vec::new();
        let large_data = vec![b'a'; 10 * 1024 * 1024];
        parser.process(&large_data, &mut out);
        // Capped at 1024
        assert_eq!(parser.partial.len(), 1024);
    }

    #[test]
    fn raw_unbounded_partial_growth() {
        let mut parser = RawParser::new(1024);
        let mut out = Vec::new();
        let large_data = vec![b'a'; 10 * 1024 * 1024];
        parser.process(&large_data, &mut out);
        assert_eq!(parser.partial.len(), 1024);
    }

    #[test]
    fn cri_unbounded_outer_partial_growth() {
        let mut parser = CriParser::new(1024);
        let mut out = Vec::new();
        let large_data = vec![b'a'; 10 * 1024 * 1024];
        parser.process(&large_data, &mut out);
        // Outer partial buffer must be capped.
        assert!(parser.partial.len() <= 1024 + 128);
    }
}
