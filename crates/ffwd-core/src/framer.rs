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
//! The framer uses plain byte loops (no memchr, no Vec) so Kani can prove
//! it correct. LLVM auto-vectorizes the byte scan loop, so performance is
//! comparable to hand-written SIMD.

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
                if output.count >= MAX_LINES_PER_FRAME {
                    // Output full — stop here. Remainder starts at `start`
                    // (the beginning of the current unprocessed line).
                    break;
                }
                if i > start {
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

// CRI framing deferred to #313 (unified SIMD structural character detection).
// CRI line parsing lives in cri.rs (parse_cri_line + CriReassembler).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newline_framer_basic() {
        let input = b"line one\nline two\nline three\n";
        let output = NewlineFramer.frame(input);
        assert_eq!(output.len(), 3);
        assert!(!output.is_empty());
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
        assert!(output.is_empty());
        assert_eq!(output.remainder_offset, 0);
    }

    #[test]
    fn newline_framer_no_trailing_newline() {
        let input = b"no newline at all";
        let output = NewlineFramer.frame(input);
        assert_eq!(output.len(), 0);
        assert!(output.is_empty());
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
}

#[cfg(kani)]
mod verification {
    use super::*;

    /// Prove NewlineFramer never panics for any 32-byte input.
    #[kani::proof]
    #[kani::unwind(34)]
    fn verify_newline_framer_no_panic() {
        let input: [u8; 32] = kani::any();
        let output = NewlineFramer.frame(&input);
        // Vacuity: confirm interesting cases are explored
        kani::cover!(output.count > 0, "at least one line found");
        kani::cover!(
            output.remainder_offset < input.len(),
            "partial remainder exists"
        );
        kani::cover!(
            output.remainder_offset == input.len(),
            "no remainder (all consumed)"
        );
    }

    /// Prove NewlineFramer line ranges are valid sub-ranges of input.
    ///
    /// Gated behind `kani-slow`: [u8; 32] symbolic + unwind 34 + kissat takes >30s.
    #[cfg(feature = "kani-slow")]
    #[kani::solver(kissat)]
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
        kani::cover!(output.count >= 2, "multiple lines found");
        kani::cover!(
            output.count == 0 && output.remainder_offset > 0,
            "no newlines, non-empty remainder"
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
    /// This is the oracle proof — verifies the framer produces the same lines as naive split-on-newline.
    #[kani::solver(kissat)]
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

    /// Line ranges are non-overlapping and strictly ordered.
    ///
    /// Consecutive ranges [start_i, end_i) and [start_j, end_j) must satisfy
    /// start_j > end_i — they are separated by at least one newline byte.
    /// This is stronger than the bounds proof: it rules out duplicated or
    /// reversed ranges that would cause double-processing of bytes.
    #[kani::solver(kissat)]
    #[kani::proof]
    #[kani::unwind(18)]
    fn verify_newline_framer_ranges_non_overlapping() {
        let input: [u8; 16] = kani::any();
        let output = NewlineFramer.frame(&input);

        let mut i = 0;
        while i + 1 < output.count {
            let (_, end_i) = output.line_ranges[i];
            let (start_j, _) = output.line_ranges[i + 1];
            // Next line must start strictly after the newline terminating this one.
            // end_i is the position of the newline, so next start >= end_i + 1.
            assert!(
                start_j > end_i,
                "line ranges overlap or are not separated by newline"
            );
            i += 1;
        }

        kani::cover!(output.count >= 2, "at least two lines found");
    }

    /// Line count never exceeds MAX_LINES_PER_FRAME.
    ///
    /// Even for inputs densely packed with newlines, the output struct
    /// cannot overflow its fixed-size array.
    #[kani::proof]
    #[kani::unwind(18)]
    fn verify_newline_framer_count_bounded() {
        let input: [u8; 16] = kani::any();
        let output = NewlineFramer.frame(&input);
        assert!(
            output.count <= MAX_LINES_PER_FRAME,
            "count exceeded MAX_LINES_PER_FRAME"
        );
    }

    // find_byte proofs are in byte_search.rs
}
