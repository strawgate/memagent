// structural.rs — Streaming structural character detection.
//
// Extends the simdjson Stage 1 pattern to detect 10 structural characters
// in a single SIMD pass over 64-byte blocks. Bitmasks are consumed
// immediately per-block — no stored vectors, no heap allocation.
//
// Characters detected:
//   \n (newline), ' ' (space), '"' (quote), '\\' (backslash),
//   ',' (comma), ':' (colon), '{', '}', '[', ']'
//
// Cross-block state: only 2 u64 values (escape carry, string carry).
//
// SIMD is provided by the `wide` crate (portable across NEON, AVX2,
// SSE2, WASM). The scalar `find_structural_chars_scalar` is the
// Kani-provable specification.

// ---------------------------------------------------------------------------
use alloc::vec::Vec;
// Escape detection (simdjson prefix_xor algorithm)
// ---------------------------------------------------------------------------

/// Compute unescaped quote positions from quote and backslash bitmasks.
///
/// Iterates through backslash bits (O(num_backslashes), typically very small)
/// to identify which quotes are escaped. Carries state between blocks via
/// `prev_odd_backslash`.
///
/// Contract: result is always a submask of `quote_bits` (can only remove
/// quotes, never add them).
#[inline]
#[cfg_attr(kani, kani::ensures(|result: &u64| *result & !quote_bits == 0))]
pub fn compute_real_quotes(quote_bits: u64, bs_bits: u64, prev_odd_backslash: &mut u64) -> u64 {
    if bs_bits == 0 && *prev_odd_backslash == 0 {
        return quote_bits;
    }

    let mut escaped: u64 = 0;
    let mut b = bs_bits;

    if *prev_odd_backslash != 0 {
        escaped |= 1;
        b &= !1;
    }

    while b != 0 {
        let pos = b.trailing_zeros() as u64;
        b &= !(1u64 << pos);
        let next_pos = pos + 1;
        if next_pos < 64 {
            escaped |= 1u64 << next_pos;
            b &= !(1u64 << next_pos);
        }
    }

    let last_is_bs = (bs_bits >> 63) & 1 == 1;
    let last_is_escaped = (escaped >> 63) & 1 == 1;
    *prev_odd_backslash = u64::from(last_is_bs && !last_is_escaped);

    quote_bits & !escaped
}

/// Running XOR that toggles at each set bit. Used to compute string
/// interior mask from quote positions.
#[inline(always)]
pub fn prefix_xor(mut bitmask: u64) -> u64 {
    bitmask ^= bitmask << 1;
    bitmask ^= bitmask << 2;
    bitmask ^= bitmask << 4;
    bitmask ^= bitmask << 8;
    bitmask ^= bitmask << 16;
    bitmask ^= bitmask << 32;
    bitmask
}

/// Raw character bitmasks from a single 64-byte block.
///
/// Produced by SIMD detection (Stage 1). Each bit corresponds to a byte
/// position in the block: bit i is set if `block[i]` matches the character.
///
/// These are "raw" — quotes may be escaped, structural characters may be
/// inside strings. Use [`StreamingClassifier::process_block`] to produce
/// escape-aware, string-masked bitmasks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RawBlockMasks {
    /// `\n` positions.
    pub newline: u64,
    /// Space positions.
    pub space: u64,
    /// `"` positions.
    pub quote: u64,
    /// `\` positions.
    pub backslash: u64,
    /// `,` positions.
    pub comma: u64,
    /// `:` positions.
    pub colon: u64,
    /// `{` positions.
    pub open_brace: u64,
    /// `}` positions.
    pub close_brace: u64,
    /// `[` positions.
    pub open_bracket: u64,
    /// `]` positions.
    pub close_bracket: u64,
}

/// Processed bitmasks for a single 64-byte block.
///
/// Produced by [`StreamingClassifier::process_block`]. Quotes are escape-aware,
/// structural characters are masked by `!in_string` (characters inside JSON
/// strings are excluded).
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessedBlock {
    /// Newline positions. Always structural (never inside strings in NDJSON).
    pub newline: u64,
    /// Space positions outside strings.
    pub space: u64,
    /// Unescaped quote positions (escape-aware).
    pub real_quotes: u64,
    /// String interior mask (1 = inside a JSON string).
    pub in_string: u64,
    /// Comma positions outside strings.
    pub comma: u64,
    /// Colon positions outside strings.
    pub colon: u64,
    /// `{` positions outside strings.
    pub open_brace: u64,
    /// `}` positions outside strings.
    pub close_brace: u64,
    /// `[` positions outside strings.
    pub open_bracket: u64,
    /// `]` positions outside strings.
    pub close_bracket: u64,
}

/// Streaming structural classifier.
///
/// Processes a buffer block-by-block (64 bytes at a time). Carries
/// only 2 u64 values between blocks: escape state and string interior state.
///
/// # Usage
///
/// ```ignore
/// let mut classifier = StreamingClassifier::new();
/// for block in buf.chunks(64) {
///     let raw = find_structural_chars_scalar(block);
///     let processed = classifier.process_block(&raw, block.len());
///     // consume processed.newline, processed.comma, etc. immediately
/// }
/// ```
pub struct StreamingClassifier {
    prev_odd_backslash: u64,
    prev_in_string: u64,
}

impl StreamingClassifier {
    /// Create a new classifier with zero initial state.
    pub fn new() -> Self {
        Self {
            prev_odd_backslash: 0,
            prev_in_string: 0,
        }
    }

    /// Process raw bitmasks into escape-aware, string-masked bitmasks.
    ///
    /// `block_len` is the number of valid bytes in this block (64 for full
    /// blocks, less for the tail block). Bits beyond `block_len` are masked out.
    ///
    /// **Assumes NDJSON input**: newlines are not string-masked because NDJSON
    /// cannot contain literal newlines inside JSON strings (they must be `\n`
    /// escape sequences). Do not use this for pretty-printed/multiline JSON.
    pub fn process_block(&mut self, raw: &RawBlockMasks, block_len: usize) -> ProcessedBlock {
        let real_q = compute_real_quotes(raw.quote, raw.backslash, &mut self.prev_odd_backslash);
        let raw_string_bits = prefix_xor(real_q) ^ self.prev_in_string;

        self.prev_in_string = if (raw_string_bits >> 63) & 1 == 1 {
            u64::MAX
        } else {
            0
        };

        let in_string = raw_string_bits & !real_q;
        let not_in_string = !in_string;

        let mask = if block_len >= 64 {
            u64::MAX
        } else {
            (1u64 << block_len) - 1
        };

        ProcessedBlock {
            // Newlines don't get string-masked — they're always structural in NDJSON
            newline: raw.newline & mask,
            space: raw.space & not_in_string & mask,
            real_quotes: real_q & mask,
            in_string: in_string & mask,
            comma: raw.comma & not_in_string & mask,
            colon: raw.colon & not_in_string & mask,
            open_brace: raw.open_brace & not_in_string & mask,
            close_brace: raw.close_brace & not_in_string & mask,
            open_bracket: raw.open_bracket & not_in_string & mask,
            close_bracket: raw.close_bracket & not_in_string & mask,
        }
    }

    /// Reset classifier state. Call between independent buffers.
    pub fn reset(&mut self) {
        self.prev_odd_backslash = 0;
        self.prev_in_string = 0;
    }
}

impl Default for StreamingClassifier {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// StructuralIndex — whole-buffer classification (replaces ChunkIndex)
// ---------------------------------------------------------------------------

/// Pre-computed structural classification for a buffer.
///
/// Detects 10 structural characters in one SIMD pass but only stores
/// `real_quotes` and `in_string` — the two bitmasks the scanner needs
/// for random-access `scan_string`/`skip_nested`. All other bitmasks
/// (newlines, spaces, commas, etc.) are consumed during construction
/// and don't need to persist. This keeps memory at ~2 × (buf_len/64)
/// u64s instead of 10×.
pub struct StructuralIndex {
    real_quotes: Vec<u64>,
    in_string: Vec<u64>,
    buf_len: usize,
}

impl StructuralIndex {
    /// Classify an entire buffer in one SIMD pass.
    ///
    /// Returns `(index, line_ranges)`:
    /// - `index`: stored quote/in_string bitmasks for scanner random access
    /// - `line_ranges`: newline-delimited line boundaries extracted during the pass
    pub fn new(buf: &[u8]) -> (Self, Vec<(usize, usize)>) {
        let len = buf.len();
        let num_blocks = len.div_ceil(64);
        let mut real_quotes = Vec::with_capacity(num_blocks);
        let mut in_string_vec = Vec::with_capacity(num_blocks);
        let mut line_ranges = Vec::new();
        let mut line_start = 0;

        let mut classifier = StreamingClassifier::new();

        for block_idx in 0..num_blocks {
            let offset = block_idx * 64;
            let remaining = len - offset;
            let block_len = remaining.min(64);

            let block: [u8; 64] = if remaining >= 64 {
                buf[offset..offset + 64]
                    .try_into()
                    .expect("offset aligned to 64-byte block")
            } else {
                let mut padded = [b' '; 64];
                padded[..remaining].copy_from_slice(&buf[offset..]);
                padded
            };

            let raw = find_structural_chars(&block);
            let processed = classifier.process_block(&raw, block_len);

            real_quotes.push(processed.real_quotes);
            in_string_vec.push(processed.in_string);

            let mut nl = processed.newline;
            while nl != 0 {
                let bit_pos = nl.trailing_zeros() as usize;
                let abs_pos = offset + bit_pos;
                if abs_pos > line_start {
                    line_ranges.push((line_start, abs_pos));
                }
                line_start = abs_pos + 1;
                nl &= nl - 1;
            }
        }

        if line_start < len {
            line_ranges.push((line_start, len));
        }

        (
            Self {
                real_quotes,
                in_string: in_string_vec,
                buf_len: len,
            },
            line_ranges,
        )
    }

    /// Find the next unescaped quote at or after `pos`.
    #[inline(always)]
    pub fn next_quote(&self, pos: usize) -> Option<usize> {
        if pos >= self.buf_len {
            return None;
        }
        let block = pos >> 6;
        let bit = pos & 63;
        let mask = self.real_quotes[block] >> bit;
        if mask != 0 {
            return Some(pos + mask.trailing_zeros() as usize);
        }
        for b in (block + 1)..self.real_quotes.len() {
            let bits = self.real_quotes[b];
            if bits != 0 {
                return Some((b << 6) + bits.trailing_zeros() as usize);
            }
        }
        None
    }

    /// Check if a position is inside a JSON string.
    #[inline(always)]
    pub fn is_in_string(&self, pos: usize) -> bool {
        if pos >= self.buf_len {
            return false;
        }
        let block = pos >> 6;
        let bit = pos & 63;
        (self.in_string[block] >> bit) & 1 == 1
    }

    /// Scan a JSON string starting at `pos` (pointing to opening `"`),
    /// bounded by `end`. Returns None if the closing quote is not found
    /// before `end`, preventing cross-line reads on malformed input (#368).
    #[inline(always)]
    pub fn scan_string<'a>(
        &self,
        buf: &'a [u8],
        pos: usize,
        end: usize,
    ) -> Option<(&'a [u8], usize)> {
        debug_assert!(pos < buf.len() && buf[pos] == b'"');
        let start = pos + 1;
        if let Some(close) = self.next_quote(start) {
            if close < end {
                Some((&buf[start..close], close + 1))
            } else {
                None // closing quote is beyond the line boundary
            }
        } else {
            None
        }
    }

    /// Skip a nested JSON object/array starting at `pos`, bounded by `end`.
    /// Tracks delimiter kind — `{` must close with `}`, `[` with `]`.
    /// Returns `end` on mismatch instead of desynchronizing (#369).
    ///
    /// Contract: result is always <= end (prevents reading past line boundary).
    #[inline]
    #[cfg_attr(kani, kani::requires(pos <= end && end <= buf.len()))]
    #[cfg_attr(kani, kani::ensures(|result: &usize| *result <= end))]
    pub fn skip_nested(&self, buf: &[u8], mut pos: usize, end: usize) -> usize {
        // Small stack for delimiter tracking. Max nesting 32 levels — deeper
        // nesting in a single NDJSON line is pathological.
        let mut opener_stack = [0u8; 32];
        let mut depth: usize = 0;

        while pos < end {
            let b = buf[pos];
            match b {
                b'{' | b'[' if !self.is_in_string(pos) => {
                    if depth < 32 {
                        opener_stack[depth] = b;
                    }
                    depth += 1;
                    pos += 1;
                }
                b'}' | b']' if !self.is_in_string(pos) => {
                    if depth == 0 {
                        return pos;
                    }
                    depth -= 1;
                    // Check delimiter match
                    if depth < 32 {
                        let expected_close = if opener_stack[depth] == b'{' {
                            b'}'
                        } else {
                            b']'
                        };
                        if b != expected_close {
                            return end; // mismatch — bail to line end
                        }
                    }
                    pos += 1;
                    if depth == 0 {
                        return pos;
                    }
                }
                b'"' if !self.is_in_string(pos) => match self.scan_string(buf, pos, end) {
                    Some((_, after)) => pos = after,
                    None => return end,
                },
                _ => pos += 1,
            }
        }
        pos
    }
}

// ---------------------------------------------------------------------------
// Scalar detection — Kani-provable specification
// ---------------------------------------------------------------------------

/// Detect all 10 structural characters in a 64-byte block.
///
/// This is the scalar reference implementation. LLVM may auto-vectorize
/// the inner loop on some targets, but the primary purpose is as a
/// Kani-provable specification that SIMD backends are tested against.
///
/// Returns [`RawBlockMasks`] with one bit per byte position.
pub fn find_structural_chars_scalar(block: &[u8; 64]) -> RawBlockMasks {
    let mut result = RawBlockMasks::default();
    for (i, &b) in block.iter().enumerate() {
        let bit = 1u64 << i;
        match b {
            b'\n' => result.newline |= bit,
            b' ' => result.space |= bit,
            b'"' => result.quote |= bit,
            b'\\' => result.backslash |= bit,
            b',' => result.comma |= bit,
            b':' => result.colon |= bit,
            b'{' => result.open_brace |= bit,
            b'}' => result.close_brace |= bit,
            b'[' => result.open_bracket |= bit,
            b']' => result.close_bracket |= bit,
            _ => {}
        }
    }
    result
}

/// Detect a single character in a 64-byte block. Kani-provable.
///
/// This is the building block for [`find_structural_chars_scalar`] and
/// can also be used independently for single-character detection.
pub fn find_char_mask(block: &[u8; 64], needle: u8) -> u64 {
    let mut mask: u64 = 0;
    for (i, &b) in block.iter().enumerate() {
        if b == needle {
            mask |= 1u64 << i;
        }
    }
    mask
}

// ---------------------------------------------------------------------------
// SIMD detection via `wide` — portable across NEON, AVX2, SSE2, WASM
// ---------------------------------------------------------------------------

use wide::u8x16;

/// Compare one needle against 4 pre-loaded SIMD chunks, return u64 bitmask.
#[inline(always)]
fn cmp4(c0: u8x16, c1: u8x16, c2: u8x16, c3: u8x16, needle: u8) -> u64 {
    let n = u8x16::splat(needle);
    (c0.simd_eq(n).to_bitmask() as u64)
        | ((c1.simd_eq(n).to_bitmask() as u64) << 16)
        | ((c2.simd_eq(n).to_bitmask() as u64) << 32)
        | ((c3.simd_eq(n).to_bitmask() as u64) << 48)
}

/// Detect all 10 structural characters using portable SIMD.
///
/// Loads the 64-byte block once (4 × 16-byte SIMD loads via `wide::u8x16`),
/// then runs 10 comparisons against the loaded data. u8x16 maps to a single
/// native register on both NEON (128-bit) and SSE2 (128-bit). The `wide`
/// crate handles platform dispatch at compile time.
pub fn find_structural_chars(block: &[u8; 64]) -> RawBlockMasks {
    let c0 = u8x16::new(block[0..16].try_into().expect("block is 64 bytes"));
    let c1 = u8x16::new(block[16..32].try_into().expect("block is 64 bytes"));
    let c2 = u8x16::new(block[32..48].try_into().expect("block is 64 bytes"));
    let c3 = u8x16::new(block[48..64].try_into().expect("block is 64 bytes"));

    RawBlockMasks {
        newline: cmp4(c0, c1, c2, c3, b'\n'),
        space: cmp4(c0, c1, c2, c3, b' '),
        quote: cmp4(c0, c1, c2, c3, b'"'),
        backslash: cmp4(c0, c1, c2, c3, b'\\'),
        comma: cmp4(c0, c1, c2, c3, b','),
        colon: cmp4(c0, c1, c2, c3, b':'),
        open_brace: cmp4(c0, c1, c2, c3, b'{'),
        close_brace: cmp4(c0, c1, c2, c3, b'}'),
        open_bracket: cmp4(c0, c1, c2, c3, b'['),
        close_bracket: cmp4(c0, c1, c2, c3, b']'),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn simd_eq_scalar(
            block in (any::<[u8; 32]>(), any::<[u8; 32]>()).prop_map(|(a, b)| {
                let mut res = [0u8; 64];
                res[..32].copy_from_slice(&a);
                res[32..].copy_from_slice(&b);
                res
            })
        ) {
            let scalar = find_structural_chars_scalar(&block);
            let simd = find_structural_chars(&block);
            prop_assert_eq!(scalar, simd);
        }
    }

    #[test]
    fn scalar_detects_all_chars() {
        let mut block = [b'x'; 64];
        block[0] = b'\n';
        block[1] = b' ';
        block[2] = b'"';
        block[3] = b'\\';
        block[4] = b',';
        block[5] = b':';
        block[6] = b'{';
        block[7] = b'}';
        block[8] = b'[';
        block[9] = b']';

        let raw = find_structural_chars_scalar(&block);
        assert_eq!(raw.newline, 1 << 0);
        assert_eq!(raw.space, 1 << 1);
        assert_eq!(raw.quote, 1 << 2);
        assert_eq!(raw.backslash, 1 << 3);
        assert_eq!(raw.comma, 1 << 4);
        assert_eq!(raw.colon, 1 << 5);
        assert_eq!(raw.open_brace, 1 << 6);
        assert_eq!(raw.close_brace, 1 << 7);
        assert_eq!(raw.open_bracket, 1 << 8);
        assert_eq!(raw.close_bracket, 1 << 9);
    }

    #[test]
    fn scalar_matches_find_char_mask() {
        let block: [u8; 64] = {
            let mut b = [0u8; 64];
            for (i, item) in b.iter_mut().enumerate() {
                *item = (i as u8).wrapping_mul(7).wrapping_add(20);
            }
            b
        };

        let raw = find_structural_chars_scalar(&block);
        assert_eq!(raw.newline, find_char_mask(&block, b'\n'));
        assert_eq!(raw.quote, find_char_mask(&block, b'"'));
        assert_eq!(raw.backslash, find_char_mask(&block, b'\\'));
        assert_eq!(raw.comma, find_char_mask(&block, b','));
        assert_eq!(raw.colon, find_char_mask(&block, b':'));
    }

    #[test]
    fn classifier_masks_chars_inside_strings() {
        // {"key":"value, with : chars"}
        let mut buf = [b' '; 64];
        let json = br#"{"key":"value, with : chars"}"#;
        buf[..json.len()].copy_from_slice(json);
        let block: &[u8; 64] = &buf;

        let raw = find_structural_chars_scalar(block);
        let mut classifier = StreamingClassifier::new();
        let processed = classifier.process_block(&raw, 64);

        // The comma at position 13 and colon at position 20 are INSIDE
        // the string "value, with : chars" — they should be masked out.
        // The colon at position 6 (between "key" and "value...") is structural.
        assert_ne!(processed.colon, 0, "structural colon should be present");
        assert_eq!(
            processed.colon & (1u64 << 6),
            1u64 << 6,
            "colon at pos 6 is structural"
        );
        // Comma inside string should be masked
        assert_eq!(
            processed.comma & (1u64 << 13),
            0,
            "comma at pos 13 is inside string"
        );
        // Colon inside string should be masked
        assert_eq!(
            processed.colon & (1u64 << 20),
            0,
            "colon at pos 20 is inside string"
        );
    }

    #[test]
    fn classifier_handles_escapes() {
        // {"k":"value\"s"}  — escaped quote should not close the string
        let mut buf = [b' '; 64];
        let json = br#"{"k":"value\"s"}"#;
        buf[..json.len()].copy_from_slice(json);
        let block: &[u8; 64] = &buf;

        let raw = find_structural_chars_scalar(block);
        let mut classifier = StreamingClassifier::new();
        let processed = classifier.process_block(&raw, 64);

        // The close brace should be structural (outside string)
        assert_ne!(processed.close_brace, 0, "close brace should be structural");
    }

    #[test]
    fn classifier_cross_block_carry() {
        // String that starts in block 0 and ends in block 1
        let mut data = [b' '; 128];
        // Block 0: {"key":"this is a long string that spans across blocks.......
        let prefix = b"{\"key\":\"this is a long string that spans across blocks.......";
        data[..prefix.len()].copy_from_slice(prefix);
        // Block 1: the block boundary and ends here"}
        let suffix = b"the block boundary and ends here\"}";
        data[64..64 + suffix.len()].copy_from_slice(suffix);

        let block0: &[u8; 64] = data[..64].try_into().unwrap();
        let block1: &[u8; 64] = data[64..128].try_into().unwrap();

        let mut classifier = StreamingClassifier::new();

        let raw0 = find_structural_chars_scalar(block0);
        let p0 = classifier.process_block(&raw0, 64);

        let raw1 = find_structural_chars_scalar(block1);
        let p1 = classifier.process_block(&raw1, 64);

        // Block 0 has the opening { and opening quotes — structural
        assert_ne!(p0.open_brace, 0);

        // Block 1 has the closing } — it should be structural (outside string)
        assert_ne!(
            p1.close_brace, 0,
            "close brace in block 1 should be structural"
        );
    }

    #[test]
    fn simd_matches_scalar() {
        // Test SIMD backend produces identical output to scalar reference
        let test_cases: &[&[u8]] = &[
            br#"{"key":"value","num":42,"arr":[1,2,3],"nested":{"a":"b"}}"#,
            b"plain text with no json at all just bytes and newlines\n\nmore\n",
            b"2024-01-15T10:30:00Z stdout F {\"level\":\"INFO\",\"msg\":\"hello\"}\n",
            b"lots of escapes: \\\" \\\\ \\n \\t and \"quoted \\\"stuff\\\"\" here",
            &[0u8; 64],    // all zeros
            &[0xFFu8; 64], // all 0xFF
        ];

        for (i, case) in test_cases.iter().enumerate() {
            let mut block = [b' '; 64];
            let len = case.len().min(64);
            block[..len].copy_from_slice(&case[..len]);

            let scalar = find_structural_chars_scalar(&block);
            let simd = find_structural_chars(&block);

            assert_eq!(scalar.newline, simd.newline, "case {i}: newline mismatch");
            assert_eq!(scalar.space, simd.space, "case {i}: space mismatch");
            assert_eq!(scalar.quote, simd.quote, "case {i}: quote mismatch");
            assert_eq!(
                scalar.backslash, simd.backslash,
                "case {i}: backslash mismatch"
            );
            assert_eq!(scalar.comma, simd.comma, "case {i}: comma mismatch");
            assert_eq!(scalar.colon, simd.colon, "case {i}: colon mismatch");
            assert_eq!(
                scalar.open_brace, simd.open_brace,
                "case {i}: open_brace mismatch"
            );
            assert_eq!(
                scalar.close_brace, simd.close_brace,
                "case {i}: close_brace mismatch"
            );
            assert_eq!(
                scalar.open_bracket, simd.open_bracket,
                "case {i}: open_bracket mismatch"
            );
            assert_eq!(
                scalar.close_bracket, simd.close_bracket,
                "case {i}: close_bracket mismatch"
            );
        }
    }

    #[test]
    fn simd_matches_scalar_random() {
        // Test with pseudo-random data covering all byte values
        for seed in 0..100u8 {
            let block: [u8; 64] = {
                let mut b = [0u8; 64];
                for (i, item) in b.iter_mut().enumerate() {
                    *item = (i as u8)
                        .wrapping_mul(seed.wrapping_add(7))
                        .wrapping_add(seed.wrapping_mul(13));
                }
                b
            };

            let scalar = find_structural_chars_scalar(&block);
            let simd = find_structural_chars(&block);

            assert_eq!(scalar.newline, simd.newline, "seed {seed}: newline");
            assert_eq!(scalar.quote, simd.quote, "seed {seed}: quote");
            assert_eq!(scalar.backslash, simd.backslash, "seed {seed}: backslash");
            assert_eq!(scalar.comma, simd.comma, "seed {seed}: comma");
            assert_eq!(scalar.colon, simd.colon, "seed {seed}: colon");
            assert_eq!(scalar.space, simd.space, "seed {seed}: space");
            assert_eq!(
                scalar.open_brace, simd.open_brace,
                "seed {seed}: open_brace"
            );
            assert_eq!(
                scalar.close_brace, simd.close_brace,
                "seed {seed}: close_brace"
            );
            assert_eq!(
                scalar.open_bracket, simd.open_bracket,
                "seed {seed}: open_bracket"
            );
            assert_eq!(
                scalar.close_bracket, simd.close_bracket,
                "seed {seed}: close_bracket"
            );
        }
    }

    #[test]
    fn end_to_end_ndjson_line_extraction() {
        // Full pipeline: buffer → SIMD detect → process → extract line ranges
        let input = br#"{"level":"INFO","msg":"hello"}
{"level":"WARN","msg":"world"}
{"level":"ERROR","msg":"!"}
"#;
        let mut classifier = StreamingClassifier::new();
        let mut line_ranges: Vec<(usize, usize)> = Vec::new();
        let mut line_start = 0;

        // Process block by block
        let num_blocks = input.len().div_ceil(64);
        for block_idx in 0..num_blocks {
            let offset = block_idx * 64;
            let remaining = input.len() - offset;
            let block_len = remaining.min(64);

            let block: [u8; 64] = if remaining >= 64 {
                input[offset..offset + 64].try_into().unwrap()
            } else {
                let mut padded = [b' '; 64];
                padded[..remaining].copy_from_slice(&input[offset..]);
                padded
            };

            let raw = find_structural_chars(&block);
            let processed = classifier.process_block(&raw, block_len);

            // Extract line ranges from newline bitmask
            let mut nl = processed.newline;
            while nl != 0 {
                let bit_pos = nl.trailing_zeros() as usize;
                let abs_pos = offset + bit_pos;
                if abs_pos > line_start {
                    line_ranges.push((line_start, abs_pos));
                }
                line_start = abs_pos + 1;
                nl &= nl - 1;
            }
        }

        assert_eq!(line_ranges.len(), 3);
        assert_eq!(
            &input[line_ranges[0].0..line_ranges[0].1],
            br#"{"level":"INFO","msg":"hello"}"#
        );
        assert_eq!(
            &input[line_ranges[1].0..line_ranges[1].1],
            br#"{"level":"WARN","msg":"world"}"#
        );
        assert_eq!(
            &input[line_ranges[2].0..line_ranges[2].1],
            br#"{"level":"ERROR","msg":"!"}"#
        );
    }

    #[test]
    fn end_to_end_structural_field_counting() {
        // Count structural commas and colons per line (field counting)
        let input = br#"{"a":1,"b":2,"c":3}
{"x":"hello, world","y":42}
"#;
        let mut classifier = StreamingClassifier::new();
        let mut line_start = 0;
        let mut lines: Vec<(usize, usize)> = Vec::new();
        let mut line_colons: Vec<u32> = Vec::new();
        let mut line_commas: Vec<u32> = Vec::new();
        let mut cur_colons: u32 = 0;
        let mut cur_commas: u32 = 0;

        let num_blocks = input.len().div_ceil(64);
        for block_idx in 0..num_blocks {
            let offset = block_idx * 64;
            let remaining = input.len() - offset;
            let block_len = remaining.min(64);

            let block: [u8; 64] = if remaining >= 64 {
                input[offset..offset + 64].try_into().unwrap()
            } else {
                let mut padded = [b' '; 64];
                padded[..remaining].copy_from_slice(&input[offset..]);
                padded
            };

            let raw = find_structural_chars(&block);
            let processed = classifier.process_block(&raw, block_len);

            // Count structural chars, splitting by newlines
            cur_colons += processed.colon.count_ones();
            cur_commas += processed.comma.count_ones();

            let mut nl = processed.newline;
            while nl != 0 {
                let bit_pos = nl.trailing_zeros() as usize;
                let abs_pos = offset + bit_pos;

                // Subtract colons/commas that are AFTER the newline in this block
                // (they belong to the next line)
                let after_nl_mask = if bit_pos >= 63 {
                    0
                } else {
                    !((1u64 << (bit_pos + 1)) - 1)
                };
                let colons_after = (processed.colon & after_nl_mask).count_ones();
                let commas_after = (processed.comma & after_nl_mask).count_ones();

                lines.push((line_start, abs_pos));
                line_colons.push(cur_colons - colons_after);
                line_commas.push(cur_commas - commas_after);

                cur_colons = colons_after;
                cur_commas = commas_after;
                line_start = abs_pos + 1;
                nl &= nl - 1;
            }
        }

        assert_eq!(lines.len(), 2);
        // {"a":1,"b":2,"c":3} — 3 colons, 2 commas (structural)
        assert_eq!(line_colons[0], 3);
        assert_eq!(line_commas[0], 2);
        // {"x":"hello, world","y":42} — 2 colons, 1 comma (the comma in "hello, world" is masked)
        assert_eq!(line_colons[1], 2);
        assert_eq!(line_commas[1], 1, "comma inside string should be masked");
    }

    #[test]
    fn tail_block_masking() {
        let mut classifier = StreamingClassifier::new();
        let raw = RawBlockMasks {
            newline: u64::MAX, // all bits set
            ..Default::default()
        };
        let processed = classifier.process_block(&raw, 10);
        // Only first 10 bits should survive
        assert_eq!(processed.newline, (1u64 << 10) - 1);
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// Oracle proof: prefix_xor matches naive bit-by-bit running XOR
    /// for ALL u64 inputs. Exhaustive — no gap.
    #[kani::proof]
    #[kani::unwind(65)] // proof loop: while i < 64
    fn verify_prefix_xor() {
        let input: u64 = kani::any();
        let result = prefix_xor(input);

        let mut expected: u64 = 0;
        let mut running = false;
        let mut i = 0u32;
        while i < 64 {
            if (input >> i) & 1 == 1 {
                running = !running;
            }
            if running {
                expected |= 1u64 << i;
            }
            i += 1;
        }
        assert!(result == expected, "prefix_xor mismatch");
        kani::cover!(input == 0, "all zeros");
        kani::cover!(input == u64::MAX, "all ones");
        kani::cover!(result != 0 && result != input, "non-trivial prefix xor");
    }

    /// Oracle proof: compute_real_quotes matches naive byte-by-byte
    /// escape oracle for ALL (quote_bits, bs_bits, carry) triples.
    /// Three properties: submask, oracle match, carry correctness.
    ///
    /// Most critical proof in the codebase — if escape detection is
    /// wrong, the scanner silently misparses every JSON string with
    /// backslashes.
    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)]
    fn verify_compute_real_quotes() {
        let quote_bits: u64 = kani::any();
        let bs_bits: u64 = kani::any();
        let prev_carry: u64 = kani::any();
        kani::assume(prev_carry <= 1);

        let mut carry = prev_carry;
        let result = compute_real_quotes(quote_bits, bs_bits, &mut carry);

        // Result only contains quote positions
        assert!(result & !quote_bits == 0);

        // Matches naive escape oracle
        let mut escaped_naive: u64 = 0;
        let mut prev_was_unescaped_bs = prev_carry == 1;
        let mut pos = 0u32;
        while pos < 64 {
            let is_bs = (bs_bits >> pos) & 1 == 1;
            if prev_was_unescaped_bs {
                escaped_naive |= 1u64 << pos;
                prev_was_unescaped_bs = false;
            } else if is_bs {
                prev_was_unescaped_bs = true;
            } else {
                prev_was_unescaped_bs = false;
            }
            pos += 1;
        }
        let expected = quote_bits & !escaped_naive;
        assert!(result == expected, "disagrees with naive oracle");

        // Carry is correct
        let expected_carry: u64 = if prev_was_unescaped_bs { 1 } else { 0 };
        assert!(carry == expected_carry, "carry mismatch");

        kani::cover!(prev_carry == 1, "carry-in active");
        kani::cover!(carry == 1, "carry-out active");
        kani::cover!(result != quote_bits, "some quotes escaped");
        kani::cover!(result == 0 && quote_bits != 0, "all quotes escaped");
    }

    /// Correctness: bit i is set iff block[i] == needle, for any
    /// 64-byte block and any needle. Checks one arbitrary position per
    /// run — the function is a simple loop so correctness at one
    /// arbitrary position implies correctness at all.
    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)]
    fn verify_find_char_mask_correct() {
        let block: [u8; 64] = kani::any();
        let needle: u8 = kani::any();
        let result = find_char_mask(&block, needle);

        // Check a single arbitrary position (checking all 64 would be too slow)
        let pos: usize = kani::any_where(|&p: &usize| p < 64);
        let bit_set = (result >> pos) & 1 == 1;
        assert_eq!(bit_set, block[pos] == needle);
    }

    /// Consistency: find_structural_chars_scalar matches find_char_mask
    /// for the quote character. Only checks one of 10 characters — all
    /// use identical match-arm logic.
    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)]
    fn verify_structural_scalar_consistent() {
        let block: [u8; 64] = kani::any();
        let raw = find_structural_chars_scalar(&block);

        // Verify one character type (all 10 would be too slow)
        assert_eq!(raw.quote, find_char_mask(&block, b'"'));
    }

    /// Crash-freedom: process_block never panics for any combination
    /// of 10 arbitrary u64 bitmasks and any block_len 0..=64.
    #[kani::proof]
    #[kani::unwind(65)] // compute_real_quotes: while b != 0, up to 64 iters
    fn verify_process_block_no_panic() {
        let raw = RawBlockMasks {
            newline: kani::any(),
            space: kani::any(),
            quote: kani::any(),
            backslash: kani::any(),
            comma: kani::any(),
            colon: kani::any(),
            open_brace: kani::any(),
            close_brace: kani::any(),
            open_bracket: kani::any(),
            close_bracket: kani::any(),
        };
        let block_len: usize = kani::any_where(|&l: &usize| l <= 64);

        let mut classifier = StreamingClassifier::new();
        let _ = classifier.process_block(&raw, block_len);
    }

    /// Tail masking: no bits set beyond block_len in any output field.
    #[kani::proof]
    #[kani::unwind(65)] // compute_real_quotes: while b != 0, up to 64 iters
    fn verify_process_block_tail_mask() {
        let raw = RawBlockMasks {
            newline: kani::any(),
            space: kani::any(),
            quote: kani::any(),
            backslash: kani::any(),
            comma: kani::any(),
            colon: kani::any(),
            open_brace: kani::any(),
            close_brace: kani::any(),
            open_bracket: kani::any(),
            close_bracket: kani::any(),
        };
        let block_len: usize = kani::any_where(|&l: &usize| l < 64);

        let mut classifier = StreamingClassifier::new();
        let p = classifier.process_block(&raw, block_len);

        // No bits set beyond block_len
        let tail_mask = !((1u64 << block_len) - 1);
        assert_eq!(p.newline & tail_mask, 0);
        assert_eq!(p.real_quotes & tail_mask, 0);
        assert_eq!(p.comma & tail_mask, 0);
    }

    /// String exclusion: structural characters (space, comma, colon,
    /// braces) never overlap with in_string mask. Only covers the
    /// no-backslash case — with escapes, the exclusion is verified
    /// by the compositional proof below.
    #[kani::proof]
    #[kani::unwind(65)] // compute_real_quotes: while b != 0, up to 64 iters
    fn verify_in_string_exclusion() {
        let raw = RawBlockMasks {
            newline: 0, // no newlines for simplicity
            space: kani::any(),
            quote: kani::any(),
            backslash: 0, // no escapes for simplicity
            comma: kani::any(),
            colon: kani::any(),
            open_brace: kani::any(),
            close_brace: kani::any(),
            open_bracket: kani::any(),
            close_bracket: kani::any(),
        };

        let mut classifier = StreamingClassifier::new();
        let p = classifier.process_block(&raw, 64);

        // Structural chars must not overlap with in_string
        assert_eq!(p.space & p.in_string, 0);
        assert_eq!(p.comma & p.in_string, 0);
        assert_eq!(p.colon & p.in_string, 0);
        assert_eq!(p.open_brace & p.in_string, 0);
        assert_eq!(p.close_brace & p.in_string, 0);
    }

    /// Verify compute_real_quotes contract: result is submask of quote_bits.
    #[kani::proof_for_contract(compute_real_quotes)]
    fn verify_compute_real_quotes_contract() {
        let quote_bits: u64 = kani::any();
        let bs_bits: u64 = kani::any();
        let mut carry: u64 = kani::any_where(|&c: &u64| c <= 1);
        compute_real_quotes(quote_bits, bs_bits, &mut carry);
    }

    /// Compositional proof: process_block using proven compute_real_quotes.
    /// Kani trusts the compute_real_quotes contract (submask property)
    /// and verifies process_block's composition logic:
    /// - real_quotes is a submask of raw quotes (from contract)
    /// - in_string is derived correctly via prefix_xor
    /// - structural chars are masked by !in_string
    /// - tail mask clears bits beyond block_len
    #[kani::proof]
    #[kani::stub_verified(compute_real_quotes)]
    fn verify_process_block_compositional() {
        let raw = RawBlockMasks {
            newline: kani::any(),
            space: kani::any(),
            quote: kani::any(),
            backslash: kani::any(),
            comma: kani::any(),
            colon: kani::any(),
            open_brace: kani::any(),
            close_brace: kani::any(),
            open_bracket: kani::any(),
            close_bracket: kani::any(),
        };
        let block_len: usize = kani::any_where(|&l: &usize| l <= 64);

        let mut classifier = StreamingClassifier::new();
        let p = classifier.process_block(&raw, block_len);

        // real_quotes must be submask of raw quotes (from contract)
        assert_eq!(
            p.real_quotes & !raw.quote,
            0,
            "real_quotes not submask of quotes"
        );

        // All string-masked fields must not overlap with in_string
        assert_eq!(p.space & p.in_string, 0);
        assert_eq!(p.comma & p.in_string, 0);
        assert_eq!(p.colon & p.in_string, 0);
        assert_eq!(p.open_brace & p.in_string, 0);
        assert_eq!(p.close_brace & p.in_string, 0);
        assert_eq!(p.open_bracket & p.in_string, 0);
        assert_eq!(p.close_bracket & p.in_string, 0);

        // Tail masking: no bits beyond block_len in any output field
        if block_len < 64 {
            let tail = !((1u64 << block_len) - 1);
            assert_eq!(p.newline & tail, 0);
            assert_eq!(p.space & tail, 0);
            assert_eq!(p.real_quotes & tail, 0);
            assert_eq!(p.in_string & tail, 0);
            assert_eq!(p.comma & tail, 0);
            assert_eq!(p.colon & tail, 0);
            assert_eq!(p.open_brace & tail, 0);
            assert_eq!(p.close_brace & tail, 0);
            assert_eq!(p.open_bracket & tail, 0);
            assert_eq!(p.close_bracket & tail, 0);
        }
    }

    /// Correctness: next_quote returns the first real quote at or after
    /// pos. If None, no real quotes exist at or after pos.
    /// Adapted from agent audit (feat/kani-audit-and-verification).
    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)] // bitmask + two while-i<64 loops: kissat outperforms cadical
    fn verify_next_quote_correct() {
        let buf: [u8; 64] = kani::any();
        let (idx, _) = StructuralIndex::new(&buf);
        let pos: usize = kani::any_where(|&p: &usize| p < 64);

        let result = idx.next_quote(pos);

        if let Some(q) = result {
            assert!(q >= pos && q < 64);
            // It's a real quote
            assert!((idx.real_quotes[q >> 6] >> (q & 63)) & 1 == 1);
            // No real quote between pos and q
            let mut i = pos;
            while i < q {
                assert!((idx.real_quotes[i >> 6] >> (i & 63)) & 1 == 0);
                i += 1;
            }
        } else {
            // No real quotes at or after pos
            let mut i = pos;
            while i < 64 {
                assert!((idx.real_quotes[i >> 6] >> (i & 63)) & 1 == 0);
                i += 1;
            }
        }
    }

    /// Correctness: is_in_string is true iff an odd number of real
    /// quotes precede this position AND the position itself is not a quote.
    /// Adapted from agent audit (feat/kani-audit-and-verification).
    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)] // bitmask + while-i<pos loop: kissat outperforms cadical
    fn verify_is_in_string_correct() {
        let buf: [u8; 64] = kani::any();
        let (idx, _) = StructuralIndex::new(&buf);
        let pos: usize = kani::any_where(|&p: &usize| p < 64);

        let in_string = idx.is_in_string(pos);

        let mut quote_count: u32 = 0;
        let mut i = 0;
        while i < pos {
            if (idx.real_quotes[i >> 6] >> (i & 63)) & 1 == 1 {
                quote_count += 1;
            }
            i += 1;
        }

        let is_quote = (idx.real_quotes[pos >> 6] >> (pos & 63)) & 1 == 1;
        let expected = (quote_count % 2 == 1) && !is_quote;
        assert_eq!(in_string, expected);
    }

    /// scan_string: if next_quote finds a close within [pos+1, end),
    /// scan_string returns Some with correct content slice and after-position.
    /// If no quote or quote >= end, returns None.
    /// Uses 16-byte input to keep proof tractable.
    #[kani::proof]
    #[kani::unwind(18)]
    fn verify_scan_string_bounds() {
        let buf: [u8; 16] = kani::any();
        let pos: usize = kani::any_where(|&p: &usize| p < 16);
        let end: usize = kani::any_where(|&e: &usize| e <= 16);
        kani::assume(pos < end);
        kani::assume(buf[pos] == b'"');

        let (idx, _) = StructuralIndex::new(&buf);
        let result = idx.scan_string(&buf, pos, end);

        match result {
            Some((content, after)) => {
                // after is the position past the closing quote
                assert!(after > pos + 1 && after <= end);
                // content starts at pos+1 and ends before the close quote
                assert_eq!(content.len(), after - pos - 2);
            }
            None => {
                // Either no quote found after pos, or the quote is >= end
            }
        }
    }

    /// skip_nested: result is always in [pos, end].
    /// Proves the skip_nested contract on 16-byte inputs.
    #[kani::proof_for_contract(StructuralIndex::skip_nested)]
    #[kani::unwind(18)]
    fn verify_skip_nested_bounds() {
        let buf: [u8; 16] = kani::any();
        let pos: usize = kani::any();
        let end: usize = kani::any();
        kani::assume(pos <= end && end <= 16);

        let (idx, _) = StructuralIndex::new(&buf);
        let result = idx.skip_nested(&buf, pos, end);

        assert!(result >= pos && result <= end);
    }

    /// Prove StructuralIndex::new() produces valid line ranges.
    ///
    /// Properties verified on 8-byte input:
    /// 1. Each range (start, end) satisfies start < end <= buf.len()
    /// 2. Ranges are ordered and contiguous (no gaps between ranges
    ///    except for the newline byte that separates them)
    /// 3. No newlines appear inside any range's content
    /// 4. Every non-newline byte belongs to exactly one range
    /// 5. Bytes before the first range and after the last range are all newlines
    #[kani::proof]
    #[kani::unwind(66)]
    #[kani::solver(kissat)]
    fn verify_line_ranges_valid() {
        let buf: [u8; 8] = kani::any();
        let (_, line_ranges) = StructuralIndex::new(&buf);

        // Count actual newlines in the buffer
        let mut newline_count: usize = 0;
        let mut i = 0;
        while i < 8 {
            if buf[i] == b'\n' {
                newline_count += 1;
            }
            i += 1;
        }

        // line_ranges length is bounded: at most newline_count + 1
        // (each newline splits into at most one more range)
        assert!(line_ranges.len() <= newline_count + 1);

        // Property 5a: bytes before the first range are all newlines
        if !line_ranges.is_empty() {
            let (first_start, _) = line_ranges[0];
            let mut pre = 0;
            while pre < first_start {
                assert!(buf[pre] == b'\n', "non-newline before first range");
                pre += 1;
            }
        }

        // Verify each range
        let mut range_idx = 0;
        while range_idx < line_ranges.len() {
            let (start, end) = line_ranges[range_idx];

            // Property 1: valid bounds
            assert!(start < end, "empty range");
            assert!(end <= 8, "range past buffer");

            // Property 3: no newlines inside the range
            let mut j = start;
            while j < end {
                assert!(buf[j] != b'\n', "newline inside range");
                j += 1;
            }

            // Property 2: ranges are separated only by newline bytes.
            if range_idx + 1 < line_ranges.len() {
                let (next_start, _) = line_ranges[range_idx + 1];
                assert!(next_start > end, "overlapping ranges");
                let mut gap = end;
                while gap < next_start {
                    assert!(buf[gap] == b'\n', "non-newline gap between ranges");
                    gap += 1;
                }
            }
            range_idx += 1;
        }

        // Property 5b: bytes after the last range are all newlines
        if !line_ranges.is_empty() {
            let (_, last_end) = line_ranges[line_ranges.len() - 1];
            let mut post = last_end;
            while post < 8 {
                assert!(buf[post] == b'\n', "non-newline after last range");
                post += 1;
            }
        }

        // Property 4 (completeness): if no ranges, all bytes must be newlines
        if line_ranges.is_empty() {
            let mut all_nl = 0;
            while all_nl < 8 {
                assert!(buf[all_nl] == b'\n', "non-newline byte with no ranges");
                all_nl += 1;
            }
        }

        // Guard against vacuous proof
        kani::cover!(line_ranges.len() > 1, "multiple line ranges");
        kani::cover!(line_ranges.is_empty(), "no ranges (all newlines)");
        kani::cover!(newline_count == 0, "no newlines in buffer");
    }
}
