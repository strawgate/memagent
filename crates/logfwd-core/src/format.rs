//! Format parsers: convert raw input bytes into newline-delimited JSON.
//!
//! Each format (CRI, JSON, Raw) implements the [`FormatParser`] trait.
//! The pipeline feeds raw bytes in and gets back newline-terminated JSON
//! lines suitable for the SIMD scanner.

use crate::cri::{self, CriReassembler};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Converts raw input bytes into newline-delimited JSON lines.
///
/// Implementations carry state across calls (partial lines, CRI reassembly).
/// All methods are called from a single thread — no `Sync` required.
pub trait FormatParser: Send {
    /// Process a chunk of raw bytes, appending complete newline-delimited
    /// JSON lines to `out`. Returns the number of lines produced.
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> usize;

    /// Reset internal state (partial line buffers, CRI reassembly).
    /// Called on file rotation or truncation.
    fn reset(&mut self);
}

// ---------------------------------------------------------------------------
// JSON / Auto
// ---------------------------------------------------------------------------

/// Passes through newline-delimited JSON, carrying partial lines across calls.
#[derive(Default)]
pub struct JsonParser {
    partial: Vec<u8>,
}

impl JsonParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FormatParser for JsonParser {
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> usize {
        let mut count = 0;
        let mut start = 0;
        for pos in memchr::memchr_iter(b'\n', bytes) {
            if self.partial.is_empty() {
                let line = &bytes[start..pos];
                if !line.is_empty() {
                    out.extend_from_slice(line);
                    out.push(b'\n');
                    count += 1;
                }
            } else {
                self.partial.extend_from_slice(&bytes[start..pos]);
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
            self.partial.extend_from_slice(&bytes[start..]);
        }
        count
    }

    fn reset(&mut self) {
        self.partial.clear();
    }
}

// ---------------------------------------------------------------------------
// Raw
// ---------------------------------------------------------------------------

/// Wraps each line as `{"_raw":"<escaped>"}\n`.
#[derive(Default)]
pub struct RawParser {
    partial: Vec<u8>,
}

impl RawParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FormatParser for RawParser {
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> usize {
        let mut count = 0;
        let mut start = 0;
        for pos in memchr::memchr_iter(b'\n', bytes) {
            let line = if self.partial.is_empty() {
                &bytes[start..pos]
            } else {
                self.partial.extend_from_slice(&bytes[start..pos]);
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
            self.partial.extend_from_slice(&bytes[start..]);
        }
        count
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
}

impl CriParser {
    pub fn new(max_line_size: usize) -> Self {
        CriParser {
            reassembler: CriReassembler::new(max_line_size),
        }
    }
}

impl FormatParser for CriParser {
    fn process(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> usize {
        cri::process_cri_to_buf(bytes, &mut self.reassembler, None, out)
    }

    fn reset(&mut self) {
        self.reassembler.reset();
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
        let mut parser = JsonParser::new();
        let mut out = Vec::new();
        let n = parser.process(b"{\"a\":1}\n{\"b\":2}\n", &mut out);
        assert_eq!(n, 2);
        assert_eq!(out, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn json_partial_carry() {
        let mut parser = JsonParser::new();
        let mut out = Vec::new();

        let n1 = parser.process(b"{\"a\":1}\n{\"b\":", &mut out);
        assert_eq!(n1, 1);

        let n2 = parser.process(b"2}\n", &mut out);
        assert_eq!(n2, 1);

        assert_eq!(out, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn json_reset_clears_partial() {
        let mut parser = JsonParser::new();
        let mut out = Vec::new();

        parser.process(b"{\"partial\":", &mut out);
        assert_eq!(out.len(), 0);

        parser.reset();
        let n = parser.process(b"{\"fresh\":1}\n", &mut out);
        assert_eq!(n, 1);
        assert_eq!(out, b"{\"fresh\":1}\n");
    }

    #[test]
    fn raw_basic() {
        let mut parser = RawParser::new();
        let mut out = Vec::new();
        let n = parser.process(b"plain line\nanother\n", &mut out);
        assert_eq!(n, 2);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("{\"_raw\":\"plain line\"}\n"));
        assert!(s.contains("{\"_raw\":\"another\"}\n"));
    }

    #[test]
    fn raw_escaping() {
        let mut parser = RawParser::new();
        let mut out = Vec::new();
        let n = parser.process(b"has \"quotes\" and \\backslash\n", &mut out);
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
        let n = parser.process(input, &mut out);
        assert_eq!(n, 1);
        assert!(out.ends_with(b"\n"));
    }
}
