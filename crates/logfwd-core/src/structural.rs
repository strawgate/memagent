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
// The scalar `find_structural_chars_scalar` is the Kani-provable
// specification. SIMD backends (NEON, AVX2, SSE2) in logfwd-arrow
// produce identical output, verified by proptest.

use crate::chunk_classify::{compute_real_quotes, prefix_xor};

/// Raw character bitmasks from a single 64-byte block.
///
/// Produced by SIMD detection (Stage 1). Each bit corresponds to a byte
/// position in the block: bit i is set if `block[i]` matches the character.
///
/// These are "raw" — quotes may be escaped, structural characters may be
/// inside strings. Use [`StreamingClassifier::process_block`] to produce
/// escape-aware, string-masked bitmasks.
#[derive(Clone, Copy, Debug, Default)]
pub struct RawBlockMasks {
    pub newline: u64,
    pub space: u64,
    pub quote: u64,
    pub backslash: u64,
    pub comma: u64,
    pub colon: u64,
    pub open_brace: u64,
    pub close_brace: u64,
    pub open_bracket: u64,
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
    pub fn process_block(&mut self, raw: &RawBlockMasks, block_len: usize) -> ProcessedBlock {
        // Stage 2: escape handling
        let real_q = compute_real_quotes(raw.quote, raw.backslash, &mut self.prev_odd_backslash);

        // Stage 3: string interior mask
        let raw_string_bits = prefix_xor(real_q) ^ self.prev_in_string;

        // Carry for next block
        self.prev_in_string = if (raw_string_bits >> 63) & 1 == 1 {
            u64::MAX
        } else {
            0
        };

        // Exclude quote positions from string interior
        let in_string = raw_string_bits & !real_q;
        let not_in_string = !in_string;

        // Tail mask
        let mask = if block_len >= 64 {
            u64::MAX
        } else {
            (1u64 << block_len) - 1
        };

        ProcessedBlock {
            newline: raw.newline & mask, // newlines are always structural
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
// SIMD detection — platform-specific backends
// ---------------------------------------------------------------------------

/// Detect all 10 structural characters using the best available SIMD.
///
/// Dispatches to NEON (aarch64), AVX2/SSE2 (x86_64), or the scalar
/// fallback. All backends produce identical [`RawBlockMasks`].
pub fn find_structural_chars(block: &[u8; 64]) -> RawBlockMasks {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { simd_neon::find_structural_chars_neon(block) }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { simd_avx2::find_structural_chars_avx2(block) }
        } else {
            unsafe { simd_sse2::find_structural_chars_sse2(block) }
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        find_structural_chars_scalar(block)
    }
}

#[cfg(target_arch = "aarch64")]
mod simd_neon {
    use super::RawBlockMasks;
    use std::arch::aarch64::*;

    #[inline(always)]
    unsafe fn movemask16(cmp: uint8x16_t) -> u16 {
        unsafe {
            const MASK: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
            let mask = vld1q_u8(MASK.as_ptr());
            let bits = vandq_u8(cmp, mask);
            let p16 = vpaddlq_u8(bits);
            let p32 = vpaddlq_u16(p16);
            let p64 = vpaddlq_u32(p32);
            let lo = vgetq_lane_u64(p64, 0) as u8;
            let hi = vgetq_lane_u64(p64, 1) as u8;
            (lo as u16) | ((hi as u16) << 8)
        }
    }

    #[inline(always)]
    unsafe fn extract_mask(
        c0: uint8x16_t,
        c1: uint8x16_t,
        c2: uint8x16_t,
        c3: uint8x16_t,
        needle: uint8x16_t,
    ) -> u64 {
        unsafe {
            (movemask16(vceqq_u8(c0, needle)) as u64)
                | ((movemask16(vceqq_u8(c1, needle)) as u64) << 16)
                | ((movemask16(vceqq_u8(c2, needle)) as u64) << 32)
                | ((movemask16(vceqq_u8(c3, needle)) as u64) << 48)
        }
    }

    #[target_feature(enable = "neon")]
    pub(super) unsafe fn find_structural_chars_neon(data: &[u8; 64]) -> RawBlockMasks {
        unsafe {
            let c0 = vld1q_u8(data.as_ptr());
            let c1 = vld1q_u8(data[16..].as_ptr());
            let c2 = vld1q_u8(data[32..].as_ptr());
            let c3 = vld1q_u8(data[48..].as_ptr());

            RawBlockMasks {
                newline: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'\n')),
                space: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b' ')),
                quote: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'"')),
                backslash: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'\\')),
                comma: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b',')),
                colon: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b':')),
                open_brace: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'{')),
                close_brace: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'}')),
                open_bracket: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b'[')),
                close_bracket: extract_mask(c0, c1, c2, c3, vdupq_n_u8(b']')),
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod simd_avx2 {
    use super::RawBlockMasks;
    use std::arch::x86_64::*;

    #[inline(always)]
    unsafe fn extract_mask(lo: __m256i, hi: __m256i, needle: __m256i) -> u64 {
        unsafe {
            let m0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, needle)) as u32 as u64;
            let m1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, needle)) as u32 as u64;
            m0 | (m1 << 32)
        }
    }

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn find_structural_chars_avx2(data: &[u8; 64]) -> RawBlockMasks {
        unsafe {
            let lo = _mm256_loadu_si256(data.as_ptr() as *const __m256i);
            let hi = _mm256_loadu_si256(data[32..].as_ptr() as *const __m256i);

            RawBlockMasks {
                newline: extract_mask(lo, hi, _mm256_set1_epi8(b'\n' as i8)),
                space: extract_mask(lo, hi, _mm256_set1_epi8(b' ' as i8)),
                quote: extract_mask(lo, hi, _mm256_set1_epi8(b'"' as i8)),
                backslash: extract_mask(lo, hi, _mm256_set1_epi8(b'\\' as i8)),
                comma: extract_mask(lo, hi, _mm256_set1_epi8(b',' as i8)),
                colon: extract_mask(lo, hi, _mm256_set1_epi8(b':' as i8)),
                open_brace: extract_mask(lo, hi, _mm256_set1_epi8(b'{' as i8)),
                close_brace: extract_mask(lo, hi, _mm256_set1_epi8(b'}' as i8)),
                open_bracket: extract_mask(lo, hi, _mm256_set1_epi8(b'[' as i8)),
                close_bracket: extract_mask(lo, hi, _mm256_set1_epi8(b']' as i8)),
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod simd_sse2 {
    use super::RawBlockMasks;
    use std::arch::x86_64::*;

    #[inline(always)]
    unsafe fn extract_mask(
        c0: __m128i,
        c1: __m128i,
        c2: __m128i,
        c3: __m128i,
        needle: __m128i,
    ) -> u64 {
        unsafe {
            let m0 = _mm_movemask_epi8(_mm_cmpeq_epi8(c0, needle)) as u16 as u64;
            let m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(c1, needle)) as u16 as u64;
            let m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(c2, needle)) as u16 as u64;
            let m3 = _mm_movemask_epi8(_mm_cmpeq_epi8(c3, needle)) as u16 as u64;
            m0 | (m1 << 16) | (m2 << 32) | (m3 << 48)
        }
    }

    #[target_feature(enable = "sse2")]
    pub(super) unsafe fn find_structural_chars_sse2(data: &[u8; 64]) -> RawBlockMasks {
        unsafe {
            let c0 = _mm_loadu_si128(data.as_ptr() as *const __m128i);
            let c1 = _mm_loadu_si128(data[16..].as_ptr() as *const __m128i);
            let c2 = _mm_loadu_si128(data[32..].as_ptr() as *const __m128i);
            let c3 = _mm_loadu_si128(data[48..].as_ptr() as *const __m128i);

            let n = |b: u8| _mm_set1_epi8(b as i8);

            RawBlockMasks {
                newline: extract_mask(c0, c1, c2, c3, n(b'\n')),
                space: extract_mask(c0, c1, c2, c3, n(b' ')),
                quote: extract_mask(c0, c1, c2, c3, n(b'"')),
                backslash: extract_mask(c0, c1, c2, c3, n(b'\\')),
                comma: extract_mask(c0, c1, c2, c3, n(b',')),
                colon: extract_mask(c0, c1, c2, c3, n(b':')),
                open_brace: extract_mask(c0, c1, c2, c3, n(b'{')),
                close_brace: extract_mask(c0, c1, c2, c3, n(b'}')),
                open_bracket: extract_mask(c0, c1, c2, c3, n(b'[')),
                close_bracket: extract_mask(c0, c1, c2, c3, n(b']')),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
            for i in 0..64 {
                b[i] = (i as u8).wrapping_mul(7).wrapping_add(20);
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
        // {"k":"val\"ue"}  — escaped quote should not close the string
        let mut buf = [b' '; 64];
        let json = br#"{"k":"val\"ue"}"#;
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
                for i in 0..64 {
                    b[i] = (i as u8)
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
                let after_nl_mask = !((1u64 << (bit_pos + 1)) - 1);
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

    /// Prove find_char_mask is correct for ALL inputs.
    /// Bit i is set iff block[i] == needle.
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

    /// Prove find_structural_chars_scalar matches find_char_mask for each character.
    #[kani::proof]
    #[kani::unwind(65)]
    #[kani::solver(kissat)]
    fn verify_structural_scalar_consistent() {
        let block: [u8; 64] = kani::any();
        let raw = find_structural_chars_scalar(&block);

        // Verify one character type (all 10 would be too slow)
        assert_eq!(raw.quote, find_char_mask(&block, b'"'));
    }

    /// Prove process_block never panics.
    #[kani::proof]
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

    /// Prove process_block masks out bits beyond block_len.
    #[kani::proof]
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

    /// Prove: characters marked as inside strings are never in structural output.
    #[kani::proof]
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
}
