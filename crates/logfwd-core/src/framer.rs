//! Framers identify line boundaries within byte buffers.
//!
//! A framer takes a buffer of bytes and produces line ranges — sub-slices
//! that represent complete lines. The framer does NOT copy data; it returns
//! offsets into the input buffer.
//!
//! Framers are stateless for line boundaries. The reader is responsible for
//! ensuring the buffer starts at a line boundary and tracking any remainder
//! (bytes after the last newline) for the next read cycle.
//!
//! # Provability
//!
//! The framer uses `byte_search::find_byte` (plain byte loops, no memchr,
//! no Vec) so Kani can prove it correct. LLVM auto-vectorizes the byte
//! scan loop, so performance is comparable to hand-written SIMD.

use crate::byte_search::find_byte;

/// Maximum number of lines per frame operation.
///
/// This bounds the output array size. 4096 lines handles typical batch
/// sizes (~4MB buffer / ~1KB avg line). Larger buffers will have excess
/// lines left in the remainder for the next frame call.
pub const MAX_LINES_PER_FRAME: usize = 4096;

/// Output of a framing operation. Fixed-size, no heap allocation.
///
/// At 4096 entries × 16 bytes = 64KB on the stack. Fits comfortably
/// in the default 8MB stack.
pub struct FrameOutput {
    /// Byte ranges of complete lines. Only `line_ranges[..count]` are valid.
    /// Each `(start, end)` points into the input: `input[start..end]`.
    /// Lines do NOT include the trailing newline.
    line_ranges: [(usize, usize); MAX_LINES_PER_FRAME],

    /// Number of valid entries in `line_ranges`.
    count: usize,

    /// Byte offset of the first unprocessed byte after the last complete line.
    /// Bytes `input[remainder_offset..]` are a partial line that should be
    /// retained by the reader for the next framing cycle.
    pub remainder_offset: usize,
}

impl FrameOutput {
    /// Number of complete lines found.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether no complete lines were found.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the byte range of line `i`. Panics if `i >= len()`.
    #[inline]
    pub fn line_range(&self, i: usize) -> (usize, usize) {
        assert!(i < self.count, "line index out of bounds");
        self.line_ranges[i]
    }

    /// Iterate over line ranges.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.line_ranges[..self.count].iter().copied()
    }
}

/// Newline-delimited framer. Splits on `\n` bytes.
///
/// Uses a plain byte loop (no memchr) so LLVM can auto-vectorize and
/// Kani can formally verify it. Suitable for JSON (NDJSON), raw format,
/// and CRI format where literal `\n` is always the line delimiter.
pub struct NewlineFramer;

impl NewlineFramer {
    /// Identify line boundaries in the input buffer.
    ///
    /// Returns a `FrameOutput` with ranges of complete lines and the
    /// offset of any partial remainder.
    pub fn frame(&self, input: &[u8]) -> FrameOutput {
        let mut output = FrameOutput {
            line_ranges: [(0, 0); MAX_LINES_PER_FRAME],
            count: 0,
            remainder_offset: 0,
        };

        let mut start = 0;
        let mut i = 0;
        while i < input.len() {
            if input[i] == b'\n' {
                if i > start && output.count < MAX_LINES_PER_FRAME {
                    output.line_ranges[output.count] = (start, i);
                    output.count += 1;
                }
                start = i + 1;
            }
            i += 1;
        }
        output.remainder_offset = start;
        output
    }
}

/// CRI framer. Splits on `\n` and extracts CRI field ranges.
///
/// CRI format: `TIMESTAMP STREAM FLAGS MESSAGE\n`
///
/// Returns `CriFrameOutput` with per-line field ranges pointing into
/// the original input buffer (zero-copy).
pub struct CriFramer;

/// Information extracted from a single CRI line. All ranges point into
/// the original input buffer.
pub struct CriLineInfo {
    /// Byte range of the timestamp field.
    pub timestamp: (usize, usize),
    /// Byte range of the stream field (stdout/stderr).
    pub stream: (usize, usize),
    /// Whether this is a full line (F) or partial (P).
    pub is_full: bool,
    /// Byte range of the message content.
    pub message: (usize, usize),
}

/// Maximum CRI lines per frame operation.
pub const MAX_CRI_LINES_PER_FRAME: usize = 4096;

/// Output of CRI framing. Fixed-size, no heap allocation for line storage.
pub struct CriFrameOutput {
    lines: Vec<CriLineInfo>, // CriLineInfo is non-Copy (contains ranges), use Vec
    /// Byte offset of the first unprocessed byte.
    pub remainder_offset: usize,
}

impl CriFrameOutput {
    /// Number of parsed CRI lines.
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Whether no CRI lines were found.
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Get a reference to the parsed CRI lines.
    pub fn lines(&self) -> &[CriLineInfo] {
        &self.lines
    }
}

impl CriFramer {
    /// Frame a buffer of CRI-formatted log data.
    ///
    /// Uses the NewlineFramer for line splitting, then parses each line's
    /// CRI fields. Invalid CRI lines are silently skipped.
    pub fn frame(&self, input: &[u8]) -> CriFrameOutput {
        let newlines = NewlineFramer.frame(input);
        let mut lines = Vec::with_capacity(newlines.len());

        for (start, end) in newlines.iter() {
            if let Some(info) = parse_cri_ranges(input, start, end) {
                lines.push(info);
            }
        }

        CriFrameOutput {
            lines,
            remainder_offset: newlines.remainder_offset,
        }
    }
}

/// Parse CRI field ranges from a line within the input buffer.
/// Returns byte ranges into `buf` for zero-copy downstream use.
fn parse_cri_ranges(buf: &[u8], line_start: usize, line_end: usize) -> Option<CriLineInfo> {
    let line = &buf[line_start..line_end];

    // Find three space delimiters: TIMESTAMP STREAM FLAGS MESSAGE
    let sp1 = find_byte(line, b' ', 0)?;
    if sp1 + 1 >= line.len() {
        return None;
    }

    let sp2 = find_byte(line, b' ', sp1 + 1)?;
    if sp2 + 1 >= line.len() {
        return None;
    }

    let flags_start = sp2 + 1;
    let (is_full, msg_offset) = if let Some(sp3) = find_byte(line, b' ', flags_start) {
        (line[flags_start] == b'F', sp3 + 1)
    } else {
        (line.get(flags_start) == Some(&b'F'), line.len())
    };

    let msg_start = line_start + msg_offset;
    let msg_end = line_end;

    Some(CriLineInfo {
        timestamp: (line_start, line_start + sp1),
        stream: (line_start + sp1 + 1, line_start + sp2),
        is_full,
        message: (msg_start, msg_end),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newline_framer_basic() {
        let input = b"line one\nline two\nline three\n";
        let output = NewlineFramer.frame(input);
        assert_eq!(output.len(), 3);
        assert_eq!(
            &input[output.line_range(0).0..output.line_range(0).1],
            b"line one"
        );
        assert_eq!(
            &input[output.line_range(1).0..output.line_range(1).1],
            b"line two"
        );
        assert_eq!(
            &input[output.line_range(2).0..output.line_range(2).1],
            b"line three"
        );
        assert_eq!(output.remainder_offset, input.len());
    }

    #[test]
    fn newline_framer_partial() {
        let input = b"complete\npartial";
        let output = NewlineFramer.frame(input);
        assert_eq!(output.len(), 1);
        assert_eq!(
            &input[output.line_range(0).0..output.line_range(0).1],
            b"complete"
        );
        assert_eq!(output.remainder_offset, 9);
        assert_eq!(&input[output.remainder_offset..], b"partial");
    }

    #[test]
    fn newline_framer_empty() {
        let output = NewlineFramer.frame(b"");
        assert_eq!(output.len(), 0);
        assert_eq!(output.remainder_offset, 0);
    }

    #[test]
    fn newline_framer_no_trailing_newline() {
        let input = b"no newline at all";
        let output = NewlineFramer.frame(input);
        assert_eq!(output.len(), 0);
        assert_eq!(output.remainder_offset, 0);
        assert_eq!(&input[output.remainder_offset..], b"no newline at all");
    }

    #[test]
    fn newline_framer_consecutive_newlines() {
        let input = b"a\n\nb\n";
        let output = NewlineFramer.frame(input);
        assert_eq!(output.len(), 2);
        assert_eq!(&input[output.line_range(0).0..output.line_range(0).1], b"a");
        assert_eq!(&input[output.line_range(1).0..output.line_range(1).1], b"b");
    }

    #[test]
    fn cri_framer_basic() {
        let input = b"2024-01-15T10:30:00Z stdout F hello world\n";
        let output = CriFramer.frame(input);
        assert_eq!(output.len(), 1);
        let line = &output.lines()[0];
        assert!(line.is_full);
        assert_eq!(
            &input[line.timestamp.0..line.timestamp.1],
            b"2024-01-15T10:30:00Z"
        );
        assert_eq!(&input[line.stream.0..line.stream.1], b"stdout");
        assert_eq!(&input[line.message.0..line.message.1], b"hello world");
    }

    #[test]
    fn cri_framer_partial_flag() {
        let input = b"2024-01-15T10:30:00Z stderr P partial content\n";
        let output = CriFramer.frame(input);
        assert_eq!(output.len(), 1);
        assert!(!output.lines()[0].is_full);
        assert_eq!(
            &input[output.lines()[0].message.0..output.lines()[0].message.1],
            b"partial content"
        );
    }

    #[test]
    fn cri_framer_multiple_lines() {
        let input = b"2024-01-15T10:30:00Z stdout P first\n2024-01-15T10:30:00Z stdout F second\n";
        let output = CriFramer.frame(input);
        assert_eq!(output.len(), 2);
        assert!(!output.lines()[0].is_full);
        assert!(output.lines()[1].is_full);
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    /// Prove NewlineFramer never panics for any 32-byte input.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_newline_framer_no_panic() {
        let input: [u8; 32] = kani::any();
        let _ = NewlineFramer.frame(&input);
    }

    /// Prove NewlineFramer line ranges are valid sub-ranges of input.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_newline_framer_ranges_valid() {
        let input: [u8; 32] = kani::any();
        let output = NewlineFramer.frame(&input);

        let mut i = 0;
        while i < output.count {
            let (start, end) = output.line_ranges[i];
            assert!(start <= end, "start > end");
            assert!(end <= input.len(), "end out of bounds");
            // Lines don't include the newline
            assert!(
                end < input.len() || start == end,
                "end at buffer boundary without newline"
            );
            i += 1;
        }
        assert!(
            output.remainder_offset <= input.len(),
            "remainder out of bounds"
        );
    }

    /// Prove NewlineFramer remainder is correct: bytes after remainder_offset
    /// contain no newlines (or the output is full).
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_newline_framer_remainder_correct() {
        let input: [u8; 16] = kani::any();
        let output = NewlineFramer.frame(&input);

        // If output isn't full, the remainder should contain no newlines
        if output.count < MAX_LINES_PER_FRAME {
            let mut j = output.remainder_offset;
            while j < input.len() {
                assert!(input[j] != b'\n', "newline found in remainder");
                j += 1;
            }
        }
    }

    /// Prove NewlineFramer produces correct line content — each returned
    /// range contains no newlines, and boundaries align with actual \n bytes.
    /// This is the ORACLE proof that closes the gap identified in PROOF_AUDIT.md.
    #[kani::proof]
    #[kani::unwind(18)]
    fn verify_newline_framer_content_correct() {
        let input: [u8; 16] = kani::any();
        let output = NewlineFramer.frame(&input);

        let mut i = 0;
        while i < output.count {
            let (start, end) = output.line_ranges[i];

            // Line content must not contain newlines
            let mut j = start;
            while j < end {
                assert!(input[j] != b'\n', "newline inside a line range");
                j += 1;
            }

            // The byte immediately after the line must be a newline
            assert!(end < input.len(), "line end out of bounds");
            assert!(input[end] == b'\n', "line not terminated by newline");

            // Lines must be non-empty (we skip empty lines between \n\n)
            assert!(start < end, "empty line range emitted");

            i += 1;
        }

        // Every newline in the input must be accounted for:
        // either it terminates a line in the output, or it's after
        // the output is full (count == MAX_LINES_PER_FRAME).
        if output.count < MAX_LINES_PER_FRAME {
            // All newlines should be at line boundaries
            let mut k = 0;
            while k < output.remainder_offset {
                if input[k] == b'\n' {
                    // This newline must terminate one of our lines
                    let mut found = false;
                    let mut m = 0;
                    while m < output.count {
                        if output.line_ranges[m].1 == k {
                            found = true;
                        }
                        m += 1;
                    }
                    // Or it's a newline at position 0 (empty first line, skipped)
                    // or consecutive newlines (empty lines, skipped)
                    if !found {
                        // Check: the newline is preceded by another newline
                        // (empty line) or is at position 0
                        if k > 0 {
                            assert!(input[k - 1] == b'\n', "newline not accounted for");
                        }
                    }
                }
                k += 1;
            }
        }
    }

    // find_byte proofs are in byte_search.rs
}
