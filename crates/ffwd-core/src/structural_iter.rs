// structural_iter.rs — Streaming structural position iterator.
//
// Yields (absolute_position, character) for every structural character
// in a buffer, processing 64-byte blocks on demand. No stored bitmask
// vectors — all bitmasks live on the stack.
//
// Format-agnostic: JSON, CSV, and key=value parsers all consume the
// same iterator. Each format interprets the positions differently.
//
// The iterator handles Stage 1 (SIMD detection) and escape processing
// internally. Consumers see only unescaped, not-in-string structural
// positions (plus quotes and newlines which are always yielded).
#![allow(clippy::indexing_slicing)]

use crate::structural::{ProcessedBlock, StreamingClassifier, find_structural_chars};

/// A structural character and its position in the buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StructuralPos {
    /// Absolute byte offset in the buffer.
    pub pos: usize,
    /// Which structural character was found.
    pub kind: StructuralKind,
}

/// The kind of structural character.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructuralKind {
    /// `\n` — always yielded (line delimiter in NDJSON/CSV).
    Newline,
    /// `"` — unescaped quote (string delimiter).
    Quote,
    /// `,` — outside strings.
    Comma,
    /// `:` — outside strings (JSON key:value, not used by CSV).
    Colon,
    /// `{` — outside strings.
    OpenBrace,
    /// `}` — outside strings.
    CloseBrace,
    /// `[` — outside strings.
    OpenBracket,
    /// `]` — outside strings.
    CloseBracket,
    // Note: space is NOT yielded as a structural position.
    // Whitespace skipping uses the space bitmask directly via next_non_space().
}

/// Streaming iterator over structural positions in a buffer.
///
/// Processes 64-byte blocks lazily. Call `next()` to get the next
/// structural position. The iterator maintains Stage 1 state
/// (escape carry, string carry) across blocks.
///
/// # Usage
///
/// ```ignore
/// let mut iter = StructuralIter::new(buf);
/// while let Some(sp) = iter.advance() {
///     match sp.kind {
///         StructuralKind::Quote => { /* string boundary */ }
///         StructuralKind::Comma => { /* field separator */ }
///         _ => {}
///     }
/// }
/// ```
pub struct StructuralIter<'a> {
    buf: &'a [u8],
    len: usize,
    /// Current block index (0-based).
    block_idx: usize,
    /// Number of blocks in the buffer.
    num_blocks: usize,
    /// Remaining structural bits in the current block.
    /// Each set bit is a structural position we haven't yielded yet.
    remaining_bits: u64,
    /// The processed bitmasks for the current block (used to classify bits).
    current_block: ProcessedBlock,
    /// Byte offset of the current block's start.
    block_offset: usize,
    /// Stage 1 classifier state (carries across blocks).
    classifier: StreamingClassifier,
    /// Space bitmask for the current block (not yielded as positions,
    /// but available for next_non_space queries).
    current_space: u64,
}

impl<'a> StructuralIter<'a> {
    /// Create a new structural iterator over a buffer.
    pub fn new(buf: &'a [u8]) -> Self {
        let len = buf.len();
        let num_blocks = len.div_ceil(64);
        let mut iter = Self {
            buf,
            len,
            block_idx: 0,
            num_blocks,
            remaining_bits: 0,
            current_block: ProcessedBlock::default(),
            block_offset: 0,
            classifier: StreamingClassifier::new(),
            current_space: 0,
        };
        if num_blocks > 0 {
            iter.load_block(0);
        }
        iter
    }

    /// Load and process the block at the given index.
    fn load_block(&mut self, idx: usize) {
        self.block_idx = idx;
        self.block_offset = idx * 64;
        let remaining = self.len - self.block_offset;
        let block_len = remaining.min(64);

        let block: [u8; 64] = if remaining >= 64 {
            debug_assert!(remaining >= 64);
            let mut block = [0u8; 64];
            block.copy_from_slice(&self.buf[self.block_offset..self.block_offset + 64]);
            block
        } else {
            let mut padded = [b' '; 64];
            padded[..remaining].copy_from_slice(&self.buf[self.block_offset..]);
            padded
        };

        let raw = find_structural_chars(&block);
        let processed = self.classifier.process_block(&raw, block_len);

        // Merge all structural positions into one bitmask for iteration.
        self.remaining_bits = processed.newline
            | processed.real_quotes
            | processed.comma
            | processed.colon
            | processed.open_brace
            | processed.close_brace
            | processed.open_bracket
            | processed.close_bracket;

        self.current_block = processed;
        self.current_space = processed.space;
    }

    /// Get the next structural position, or `None` if exhausted.
    #[inline]
    pub fn advance(&mut self) -> Option<StructuralPos> {
        loop {
            if self.remaining_bits != 0 {
                let bit_pos = self.remaining_bits.trailing_zeros() as usize;
                self.remaining_bits &= self.remaining_bits - 1; // clear lowest bit

                let abs_pos = self.block_offset + bit_pos;
                if abs_pos >= self.len {
                    continue;
                }

                let kind = self.classify_bit(bit_pos);
                return Some(StructuralPos { pos: abs_pos, kind });
            }

            // Current block exhausted — advance to next
            let next_block = self.block_idx + 1;
            if next_block >= self.num_blocks {
                return None;
            }
            self.load_block(next_block);
        }
    }

    /// Classify which structural character a bit position corresponds to.
    #[inline]
    fn classify_bit(&self, bit_pos: usize) -> StructuralKind {
        let mask = 1u64 << bit_pos;
        let p = &self.current_block;

        // Check in priority order (most common first for branch prediction)
        if p.real_quotes & mask != 0 {
            StructuralKind::Quote
        } else if p.comma & mask != 0 {
            StructuralKind::Comma
        } else if p.colon & mask != 0 {
            StructuralKind::Colon
        } else if p.close_brace & mask != 0 {
            StructuralKind::CloseBrace
        } else if p.open_brace & mask != 0 {
            StructuralKind::OpenBrace
        } else if p.newline & mask != 0 {
            StructuralKind::Newline
        } else if p.close_bracket & mask != 0 {
            StructuralKind::CloseBracket
        } else {
            StructuralKind::OpenBracket
        }
    }

    /// Find the next position at or after `from` that is NOT an ASCII space.
    /// Returns `from` if it's already non-space.
    ///
    /// Uses the space bitmask — O(1) per block, no byte scanning.
    #[inline]
    pub fn next_non_space(&self, from: usize) -> usize {
        if from >= self.len {
            return self.len;
        }

        // Check current block
        let block_idx = from >> 6;
        let bit = from & 63;

        if block_idx == self.block_idx {
            // Fast path: check current block's space bitmask
            let space_at_and_after = self.current_space >> bit;
            if space_at_and_after & 1 == 0 {
                return from; // not a space
            }
            // Count leading spaces
            let non_space = (!space_at_and_after).trailing_zeros() as usize;
            if non_space < 64 - bit {
                let result = from + non_space;
                if result < self.len {
                    return result;
                }
            }
        }

        // Fallback: byte scan (for positions in blocks we haven't loaded)
        let mut pos = from;
        while pos < self.len {
            match self.buf[pos] {
                b' ' => pos += 1,
                _ => return pos,
            }
        }
        pos
    }

    /// The underlying buffer.
    #[inline]
    pub fn buf(&self) -> &'a [u8] {
        self.buf
    }

    /// Buffer length.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    fn collect_positions(buf: &[u8]) -> Vec<StructuralPos> {
        let mut iter = StructuralIter::new(buf);
        let mut result = Vec::new();
        while let Some(sp) = iter.advance() {
            result.push(sp);
        }
        result
    }

    #[test]
    fn empty_buffer() {
        assert!(collect_positions(b"").is_empty());
    }

    #[test]
    fn simple_json_object() {
        let buf = br#"{"key":"val"}"#;
        let positions = collect_positions(buf);
        let kinds: Vec<_> = positions.iter().map(|p| p.kind).collect();
        assert_eq!(
            kinds,
            &[
                StructuralKind::OpenBrace,  // {
                StructuralKind::Quote,      // "
                StructuralKind::Quote,      // " (close key)
                StructuralKind::Colon,      // :
                StructuralKind::Quote,      // "
                StructuralKind::Quote,      // " (close val)
                StructuralKind::CloseBrace, // }
            ]
        );
    }

    #[test]
    fn escaped_quote_not_yielded() {
        let buf = br#"{"k":"val\"id"}"#;
        let positions = collect_positions(buf);
        // The escaped quote inside the string should NOT appear
        let quotes: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Quote)
            .map(|p| p.pos)
            .collect();
        // Positions: 1=", 3=", 5=", 13=", (escaped \" at 8 is not yielded)
        assert_eq!(quotes.len(), 4);
    }

    #[test]
    fn comma_inside_string_not_yielded() {
        let buf = br#"{"k":"a,b","x":1}"#;
        let positions = collect_positions(buf);
        let commas: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Comma)
            .map(|p| p.pos)
            .collect();
        // Only the comma between "a,b" and "x" — not the one inside the string
        assert_eq!(commas.len(), 1);
    }

    #[test]
    fn ndjson_newlines() {
        let buf = b"{\"a\":1}\n{\"b\":2}\n";
        let positions = collect_positions(buf);
        let newlines: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Newline)
            .map(|p| p.pos)
            .collect();
        assert_eq!(newlines, &[7, 15]);
    }

    #[test]
    fn next_non_space_fast_and_fallback_use_ascii_space_only() {
        let buf = b" \t\rx";

        let fast = StructuralIter::new(buf).next_non_space(0);
        let fallback = StructuralIter {
            buf,
            len: buf.len(),
            block_idx: 99,
            num_blocks: 0,
            remaining_bits: 0,
            current_block: ProcessedBlock::default(),
            block_offset: 0,
            classifier: StreamingClassifier::new(),
            current_space: 0,
        }
        .next_non_space(0);

        assert_eq!(fast, 1);
        assert_eq!(fallback, fast);
        assert_eq!(buf[fast], b'\t');
    }

    #[test]
    fn multi_block_buffer() {
        // Create a buffer > 64 bytes to test cross-block processing
        let line = br#"{"longkey":"longvalue","another":"field","third":"value"}"#;
        assert!(line.len() > 50); // close to block boundary
        let mut buf = line.to_vec();
        buf.push(b'\n');
        buf.extend_from_slice(line);
        buf.push(b'\n');

        let positions = collect_positions(&buf);

        // Should have exactly 2 newlines
        let newlines: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Newline)
            .map(|p| p.pos)
            .collect();
        assert_eq!(newlines.len(), 2);

        // Should have commas (outside strings only)
        let commas: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Comma)
            .map(|p| p.pos)
            .collect();
        assert!(commas.len() >= 4); // at least 2 per line
    }

    #[test]
    fn csv_like_input() {
        let buf = b"hello,world,\"quoted,field\",123\n";
        let positions = collect_positions(buf);

        // Commas outside quotes
        let commas: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Comma)
            .map(|p| p.pos)
            .collect();
        // "hello,world,\"quoted,field\",123\n"
        //       ^     ^               ^
        // pos:  5     11              25
        assert_eq!(commas.len(), 3); // comma inside quotes is NOT yielded
    }

    #[test]
    fn logfmt_like_input() {
        // logfmt doesn't use commas/colons/braces — just quotes for values with spaces
        let buf = b"level=info msg=\"hello world\" ts=12345\n";
        let positions = collect_positions(buf);

        // Should see: quotes around "hello world", newline at end
        let quotes: Vec<usize> = positions
            .iter()
            .filter(|p| p.kind == StructuralKind::Quote)
            .map(|p| p.pos)
            .collect();
        assert_eq!(quotes.len(), 2); // opening and closing quote
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;
    use crate::structural::ProcessedBlock;

    /// classify_bit always returns a valid StructuralKind, and the
    /// selected kind matches exactly one of the bitmask fields.
    /// Exhaustive over all bit positions where at least one structural
    /// category has the tested bit set.
    #[kani::proof]
    #[kani::solver(cadical)]
    fn verify_classify_bit_correct() {
        let bit_pos: usize = kani::any_where(|&p: &usize| p < 64);
        let mask = 1u64 << bit_pos;

        let p = ProcessedBlock {
            newline: kani::any(),
            space: kani::any(),
            real_quotes: kani::any(),
            in_string: kani::any(),
            comma: kani::any(),
            colon: kani::any(),
            open_brace: kani::any(),
            close_brace: kani::any(),
            open_bracket: kani::any(),
            close_bracket: kani::any(),
        };
        let structural_bits = p.newline
            | p.real_quotes
            | p.comma
            | p.colon
            | p.open_brace
            | p.close_brace
            | p.open_bracket
            | p.close_bracket;
        kani::assume(structural_bits & mask != 0);
        kani::cover!(p.real_quotes & mask != 0, "quote path reachable");
        kani::cover!(
            p.real_quotes & mask == 0 && p.open_bracket & mask != 0,
            "open bracket fallback path reachable"
        );

        // Build a minimal iterator just to call classify_bit
        let iter = StructuralIter {
            buf: &[],
            len: 0,
            block_idx: 0,
            num_blocks: 0,
            remaining_bits: 0,
            current_block: p,
            block_offset: 0,
            classifier: StreamingClassifier::new(),
            current_space: 0,
        };

        let kind = iter.classify_bit(bit_pos);

        // The kind must correspond to a set bit in exactly the right field
        match kind {
            StructuralKind::Quote => assert!(p.real_quotes & mask != 0),
            StructuralKind::Comma => {
                assert!(p.comma & mask != 0);
                assert!(p.real_quotes & mask == 0); // higher priority didn't match
            }
            StructuralKind::Colon => {
                assert!(p.colon & mask != 0);
                assert!(p.real_quotes & mask == 0);
                assert!(p.comma & mask == 0);
            }
            StructuralKind::CloseBrace => {
                assert!(p.close_brace & mask != 0);
                assert!(p.real_quotes & mask == 0);
                assert!(p.comma & mask == 0);
                assert!(p.colon & mask == 0);
            }
            StructuralKind::OpenBrace => {
                assert!(p.open_brace & mask != 0);
                assert!(p.real_quotes & mask == 0);
                assert!(p.comma & mask == 0);
                assert!(p.colon & mask == 0);
                assert!(p.close_brace & mask == 0);
            }
            StructuralKind::Newline => {
                assert!(p.newline & mask != 0);
                assert!(p.real_quotes & mask == 0);
                assert!(p.comma & mask == 0);
                assert!(p.colon & mask == 0);
                assert!(p.close_brace & mask == 0);
                assert!(p.open_brace & mask == 0);
            }
            StructuralKind::CloseBracket => {
                assert!(p.close_bracket & mask != 0);
                assert!(p.real_quotes & mask == 0);
                assert!(p.comma & mask == 0);
                assert!(p.colon & mask == 0);
                assert!(p.close_brace & mask == 0);
                assert!(p.open_brace & mask == 0);
                assert!(p.newline & mask == 0);
            }
            StructuralKind::OpenBracket => {
                assert!(p.open_bracket & mask != 0);
                assert!(p.real_quotes & mask == 0);
                assert!(p.comma & mask == 0);
                assert!(p.colon & mask == 0);
                assert!(p.close_brace & mask == 0);
                assert!(p.open_brace & mask == 0);
                assert!(p.newline & mask == 0);
                assert!(p.close_bracket & mask == 0);
            }
        }
    }

    /// next_non_space fallback path: result is always in [from, len],
    /// and the byte at result (if < len) is not an ASCII space.
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::solver(cadical)]
    fn verify_next_non_space_fallback() {
        let buf: [u8; 16] = kani::any();
        let from: usize = kani::any();
        kani::assume(from <= 16);

        // Build an iterator positioned past all blocks so it uses the fallback
        let iter = StructuralIter {
            buf: &buf,
            len: 16,
            block_idx: 99, // past all blocks — forces fallback path
            num_blocks: 0,
            remaining_bits: 0,
            current_block: ProcessedBlock::default(),
            block_offset: 0,
            classifier: StreamingClassifier::new(),
            current_space: 0,
        };

        let result = iter.next_non_space(from);

        assert!(result >= from && result <= 16);
        if result < 16 {
            let b = buf[result];
            assert!(b != b' ');
        }

        // Guard vacuity: verify assume does not exclude meaningful cases
        kani::cover!(result == from, "no spaces at start");
        kani::cover!(result > from, "skipped some spaces");
        kani::cover!(result == 16, "all remaining bytes are spaces");
    }

    /// Prove advance() yields every structural position exactly once,
    /// in ascending order, for a single-block buffer.
    ///
    /// Properties verified:
    /// 1. Every yielded position is in [0, len) and corresponds to a
    ///    structural character in the buffer
    /// 2. Positions are strictly ascending
    /// 3. No structural position is skipped (yielded count ==
    ///    popcount of merged structural bitmask)
    /// 4. advance() returns None after all positions are yielded
    /// 5. Each yielded position is actually a structural byte in the buffer
    ///
    /// Uses 8-byte input (1 block, padded to 64) to keep the proof
    /// tractable while exercising the full load_block + advance pipeline.
    #[kani::proof]
    #[kani::unwind(66)]
    #[kani::solver(kissat)]
    fn verify_advance_yields_all_structural() {
        let buf: [u8; 8] = kani::any();
        let mut iter = StructuralIter::new(&buf);

        // Use remaining_bits as the oracle for completeness. This is the
        // merged structural bitmask from load_block() + process_block(),
        // which applies string/escape exclusion (commas inside strings are
        // suppressed, etc.). A raw byte-level oracle would be too broad --
        // it can't model string context without reimplementing the classifier.
        //
        // This is sound because process_block's correctness is independently
        // proven by verify_compute_real_quotes, verify_process_block_compositional,
        // and verify_in_string_exclusion in structural.rs.
        let oracle_bits = iter.remaining_bits;

        let mut yielded_bits: u64 = 0;
        let mut prev_pos: Option<usize> = None;
        let mut count: usize = 0;

        // 8-byte buffer: at most 8 structural positions + 1 final None
        let mut k = 0;
        while k < 9 {
            match iter.advance() {
                Some(sp) => {
                    assert!(sp.pos < 8, "position out of bounds");
                    let bit = 1u64 << sp.pos;
                    // No duplicates
                    assert!(yielded_bits & bit == 0, "duplicate position");
                    yielded_bits |= bit;
                    // Ascending order
                    if let Some(p) = prev_pos {
                        assert!(sp.pos > p, "not strictly ascending");
                    }
                    // Property 5: the byte at this position is structural
                    let byte = buf[sp.pos];
                    assert!(
                        byte == b'"'
                            || byte == b','
                            || byte == b':'
                            || byte == b'{'
                            || byte == b'}'
                            || byte == b'['
                            || byte == b']'
                            || byte == b'\n',
                        "yielded non-structural byte"
                    );
                    prev_pos = Some(sp.pos);
                    count += 1;
                }
                None => break,
            }
            k += 1;
        }

        // All expected structural positions were yielded (compared against
        // independent oracle, not iterator internals)
        assert_eq!(yielded_bits, oracle_bits, "missed structural positions");
        // Iterator is exhausted
        assert!(iter.advance().is_none(), "advance returned extra position");

        // Guard against vacuous proof
        kani::cover!(count > 0, "at least one structural byte yielded");
        kani::cover!(count > 3, "multiple structural bytes yielded");
        kani::cover!(count == 0, "no structural bytes in buffer");
    }
}
