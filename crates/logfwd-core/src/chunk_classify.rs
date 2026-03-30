// chunk_classify.rs — Whole-buffer structural classification.
//
// Pre-classifies an entire NDJSON chunk buffer in one pass, producing
// bitmasks that the scanner can query in O(1) instead of doing per-string
// byte-at-a-time comparison.
//
// This is the simdjson "stage 1" algorithm adapted for our use case.
//
// Platform-specific SIMD is used for the hot `find_quotes_and_backslashes`
// function:
//   - x86_64: AVX2 (2 × 32-byte loads) with SSE2 fallback (4 × 16-byte loads).
//   - aarch64: NEON (4 × 16-byte loads with vpaddl reduction chain).
//   - other: portable scalar fallback; LLVM may auto-vectorize.
//
// All paths produce the same (quote_bits, bs_bits) u64 bitmask and are
// covered by the same conformance tests.

/// Pre-computed structural classification for a buffer.
///
/// Each entry covers 64 bytes of input. Bit i = byte (block_index * 64 + i).
pub struct ChunkIndex {
    /// Unescaped quote positions.
    real_quotes: Vec<u64>,
    /// String interior mask (1 = inside a JSON string, 0 = structural context).
    in_string: Vec<u64>,
    /// Buffer length.
    buf_len: usize,
}

impl ChunkIndex {
    /// Classify an entire buffer in one SIMD pass.
    ///
    /// Processes the buffer in 64-byte blocks (4x 16-byte SIMD loads).
    /// For each block, produces bitmasks for unescaped quote positions
    /// and string interior positions.
    pub fn new(buf: &[u8]) -> Self {
        let len = buf.len();
        let num_blocks = len.div_ceil(64);
        let mut real_quotes = Vec::with_capacity(num_blocks);
        let mut in_string = Vec::with_capacity(num_blocks);

        let mut prev_odd_backslash: u64 = 0;
        let mut prev_in_string: u64 = 0;

        for block_idx in 0..num_blocks {
            let offset = block_idx * 64;
            let remaining = len - offset;

            let (quote_bits, bs_bits) = if remaining >= 64 {
                // Full block — cast slice to &[u8; 64] for exact trip count.
                let block: &[u8; 64] = buf[offset..offset + 64].try_into().unwrap();
                find_quotes_and_backslashes(block)
            } else {
                // Tail block — pad with spaces.
                let mut padded = [b' '; 64];
                padded[..remaining].copy_from_slice(&buf[offset..offset + remaining]);
                find_quotes_and_backslashes(&padded)
            };
            let real_q = compute_real_quotes(quote_bits, bs_bits, &mut prev_odd_backslash);

            #[cfg(test)]
            #[allow(unexpected_cfgs)]
            if cfg!(feature = "debug_bitmask") {
                eprintln!(
                    "block {block_idx}: offset={offset} quotes=0x{quote_bits:016x} bs=0x{bs_bits:016x} real=0x{real_q:016x}"
                );
            }

            // String interior mask: everything BETWEEN unescaped quotes.
            // prefix_xor toggles at each quote.
            let raw_string_bits = prefix_xor(real_q) ^ prev_in_string;

            // Carry for next block: are we inside a string after this block?
            // Compute BEFORE masking out quotes, since an opening quote at
            // position 63 means the next block starts inside a string.
            prev_in_string = if (raw_string_bits >> 63) & 1 == 1 {
                u64::MAX
            } else {
                0
            };

            // Exclude the quote positions themselves from the string interior
            // so that opening/closing quotes are structural, not "in string."
            let string_bits = raw_string_bits & !real_q;

            let mask = if remaining >= 64 {
                u64::MAX
            } else {
                (1u64 << remaining) - 1
            };

            real_quotes.push(real_q & mask);
            in_string.push(string_bits & mask);
        }

        ChunkIndex {
            real_quotes,
            in_string,
            buf_len: len,
        }
    }

    /// Find the next unescaped quote at or after `pos`.
    #[inline(always)]
    pub fn next_quote(&self, pos: usize) -> Option<usize> {
        if pos >= self.buf_len {
            return None;
        }
        let block = pos >> 6; // pos / 64
        let bit = pos & 63; // pos % 64
        // SAFETY: block < self.real_quotes.len() because pos < buf_len
        let mask = unsafe { *self.real_quotes.get_unchecked(block) } >> bit;
        if mask != 0 {
            return Some(pos + mask.trailing_zeros() as usize);
        }
        let len = self.real_quotes.len();
        for b in (block + 1)..len {
            let bits = unsafe { *self.real_quotes.get_unchecked(b) };
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
        // SAFETY: block < len because pos < buf_len
        (unsafe { *self.in_string.get_unchecked(block) } >> bit) & 1 == 1
    }

    /// Scan a JSON string starting at `pos` (pointing to opening `"`).
    /// O(1) closing-quote lookup via pre-computed index.
    #[inline(always)]
    pub fn scan_string<'a>(&self, buf: &'a [u8], pos: usize) -> Option<(&'a [u8], usize)> {
        debug_assert!(pos < buf.len() && buf[pos] == b'"');
        let start = pos + 1;
        if let Some(close) = self.next_quote(start) {
            Some((&buf[start..close], close + 1))
        } else {
            None
        }
    }

    /// Skip a nested JSON object/array starting at `pos`.
    /// Uses the string mask to only count structural braces/brackets.
    #[inline]
    pub fn skip_nested(&self, buf: &[u8], mut pos: usize) -> usize {
        let len = buf.len();
        let mut depth: u32 = 0;
        while pos < len {
            let b = buf[pos];
            match b {
                b'{' | b'[' if !self.is_in_string(pos) => {
                    depth += 1;
                    pos += 1;
                }
                b'}' | b']' if !self.is_in_string(pos) => {
                    if depth == 0 {
                        return pos; // hit outer boundary, stop
                    }
                    depth -= 1;
                    pos += 1;
                    if depth == 0 {
                        return pos;
                    }
                }
                b'"' if !self.is_in_string(pos) => {
                    // Opening quote of a string inside the nested value — skip it
                    match self.scan_string(buf, pos) {
                        Some((_, after)) => pos = after,
                        None => return len,
                    }
                }
                _ => pos += 1,
            }
        }
        pos
    }
}

// ---------------------------------------------------------------------------
// Escape detection (simdjson prefix_xor algorithm)
// ---------------------------------------------------------------------------

#[inline]
#[allow(dead_code)]
fn compute_real_quotes(quote_bits: u64, bs_bits: u64, prev_odd_backslash: &mut u64) -> u64 {
    if bs_bits == 0 && *prev_odd_backslash == 0 {
        return quote_bits;
    }

    // simdjson escape detection algorithm:
    //
    // 1. Find starts of consecutive backslash runs.
    //    A "start" is a backslash NOT preceded by another backslash.
    let follows_bs = (bs_bits << 1) | *prev_odd_backslash;
    let _run_starts = bs_bits & !follows_bs;

    // 2. Use prefix_xor on run starts to get alternating parity WITHIN runs.
    //    After prefix_xor, each run start toggles a running bit.
    //    Within a run of N consecutive backslashes starting at S:
    //    - prefix_xor(run_starts) has bit S set (toggles ON)
    //    - All positions S..S+N-1 have the same prefix_xor bit (ON)
    //    - The run end toggles it back OFF
    //
    //    We need alternating parity within each run. XOR the run start
    //    parity with the actual backslash positions to get that:
    //    odd_bs = positions where (distance from run start is even) AND is a backslash
    //
    //    Actually, the simpler approach: every backslash escapes the next
    //    character, UNLESS the backslash itself was escaped. A backslash is
    //    escaped if it's at an even position within its consecutive run.
    //
    //    The "odd within run" positions = prefix_xor(bs_bits) restricted to
    //    consecutive runs. But prefix_xor(bs_bits) gives running XOR of ALL
    //    backslash bits, not per-run.
    //
    //    The correct simdjson trick: XOR the even-run-start mask.
    //    even_starts = run starts of even-length runs
    //    odd_ends = prefix_xor(run_starts) & bs_bits gives us the last
    //    backslash of each odd-length run.

    // Easier correct approach: compute "escaped" positions directly.
    // A position is escaped if the character before it is a backslash AND
    // that backslash is not itself escaped.
    //
    // escaped = (unescaped_backslashes << 1)
    // unescaped_backslashes = bs_bits & !escaped
    //
    // This is circular. Break the circularity with the run-based approach:
    //
    // Within a consecutive run of backslashes: positions 0,2,4,... (even offset
    // from start) are "escaping" backslashes. Positions 1,3,5,... are "escaped"
    // backslashes (they got escaped by the one before them).
    //
    // To identify even-offset positions within runs:
    //   run_starts = bs_bits & !(bs_bits << 1 | carry)
    //   The run start is at offset 0 (even) within its run.
    //   XOR propagation from run starts through consecutive bs gives alternating.

    // Step: prefix_xor of run_starts gives us a mask that is ON for the
    // duration of each run (toggles ON at start, OFF at the bit after the run ends).
    // Within the run, we can XOR with bs_bits shifted to get alternating.

    // Simplest correct implementation (from simdjson step-by-step):
    // Within each run, odd-indexed positions (0-based from run start)
    // are the "escaping" ones. Use prefix_xor(run_starts) XOR'd with bs_bits:
    // - At the run start: prefix_xor = 1, bs = 1 → XOR = 0 (even offset = escaping)
    // - At start+1: prefix_xor = 1, bs = 1 → need to account for the XOR differently

    // OK, let me just implement the straightforward version.
    // For each consecutive backslash run, mark alternating positions:
    // Even offset from start → escaping (odd position in 1-based = "active")
    // Odd offset from start → escaped (even position in 1-based)
    //
    // The escaped CHARACTER is the one AFTER each "escaping" backslash.

    // A backslash is "escaping" if (its distance from the run start) is even.
    // Run starts are at even distance (0). Start+1 is odd. Start+2 is even. etc.
    //
    // Use the identity: within a run starting at S, position S+k is at even
    // distance iff k is even. k is even iff (S+k - S) is even iff both have
    // same parity in their offset within the run.
    //
    // Compute: even_within_run = prefix_xor(run_starts) XOR'd appropriately.
    //
    // Actually: the EVEN positions within runs = run_starts propagated forward
    // through consecutive positions, alternating.
    // odd_within_run = bs_bits XOR even_within_run

    // Let me use the method from simdjson's code directly:
    // They compute: follows_odd_bs = (odd_sequence_starts << 1) with carry

    // SIMPLEST CORRECT APPROACH: iterate through backslash bits.
    // This is O(number of backslashes per block), which is typically very small.
    let mut escaped: u64 = 0;
    let mut b = bs_bits;

    if *prev_odd_backslash != 0 {
        // Previous block ended with an unescaped backslash — first byte is escaped
        escaped |= 1;
        // If first byte is also a backslash, it's escaped (not escaping)
        b &= !1;
    }

    while b != 0 {
        let pos = b.trailing_zeros() as u64;
        b &= !(1u64 << pos);

        // This backslash escapes the next position
        let next_pos = pos + 1;
        if next_pos < 64 {
            escaped |= 1u64 << next_pos;
            // If the next position is ALSO a backslash, it's been escaped
            // (not escaping), so remove it from the remaining set
            b &= !(1u64 << next_pos);
        }
    }

    // Carry: does the last byte escape into the next block?
    // If position 63 is a backslash AND it's not itself escaped, carry forward.
    let last_is_bs = (bs_bits >> 63) & 1 == 1;
    let last_is_escaped = (escaped >> 63) & 1 == 1;
    *prev_odd_backslash = if last_is_bs && !last_is_escaped { 1 } else { 0 };

    quote_bits & !escaped
}

#[inline(always)]
#[allow(dead_code)]
fn prefix_xor(mut bitmask: u64) -> u64 {
    bitmask ^= bitmask << 1;
    bitmask ^= bitmask << 2;
    bitmask ^= bitmask << 4;
    bitmask ^= bitmask << 8;
    bitmask ^= bitmask << 16;
    bitmask ^= bitmask << 32;
    bitmask
}

// ---------------------------------------------------------------------------
// Platform-specific SIMD character detection
// ---------------------------------------------------------------------------

/// Find quote and backslash positions in a 64-byte block.
///
/// Returns `(quote_bits, bs_bits)`: u64 bitmasks where bit i is set if
/// `data[i]` is a `"` (or `\`, respectively).
///
/// Dispatches to the best available SIMD implementation for the current
/// target: AVX2 or SSE2 on x86_64, NEON on aarch64, scalar elsewhere.
#[cfg(target_arch = "x86_64")]
#[inline]
fn find_quotes_and_backslashes(data: &[u8; 64]) -> (u64, u64) {
    x86::find_quotes_and_backslashes(data)
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn find_quotes_and_backslashes(data: &[u8; 64]) -> (u64, u64) {
    aarch64_impl::find_quotes_and_backslashes(data)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn find_quotes_and_backslashes(data: &[u8; 64]) -> (u64, u64) {
    (find_char_mask(data, b'"'), find_char_mask(data, b'\\'))
}

/// Portable scalar fallback: build a u64 bitmask where bit i is set if
/// `data[i] == needle`.
///
/// LLVM auto-vectorizes this when compiled as a standalone function
/// (verified: pcmpeqb on SSE2, vpcmpeqb on AVX2/512), but thin LTO
/// inlines it into ChunkIndex::new where LLVM's cost model skips
/// vectorization.  Kept as documentation and as the non-x86/non-aarch64
/// fallback.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn find_char_mask(data: &[u8; 64], needle: u8) -> u64 {
    let mut bits: u64 = 0;
    for (i, &byte) in data.iter().enumerate() {
        bits |= ((byte == needle) as u64) << i;
    }
    bits
}

// ---------------------------------------------------------------------------
// x86_64 SIMD — AVX2 (2 × 32-byte) with SSE2 fallback (4 × 16-byte)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    /// AVX2 path: 2 × 256-bit loads, `_mm256_movemask_epi8` extracts 32-bit
    /// masks that are combined into a single 64-bit result.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn find_quotes_and_backslashes_avx2(data: &[u8; 64]) -> (u64, u64) {
        // SAFETY: intrinsics require AVX2, which is guaranteed by #[target_feature].
        unsafe {
            // Load two 32-byte halves.
            let lo = _mm256_loadu_si256(data.as_ptr() as *const __m256i);
            let hi = _mm256_loadu_si256(data[32..].as_ptr() as *const __m256i);

            let vq = _mm256_set1_epi8(b'"' as i8);
            let vbs = _mm256_set1_epi8(b'\\' as i8);

            // movemask returns i32; cast to u32 to avoid sign-extension, then widen.
            let q0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, vq)) as u32 as u64;
            let q1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, vq)) as u32 as u64;
            let quote_bits = q0 | (q1 << 32);

            let b0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, vbs)) as u32 as u64;
            let b1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, vbs)) as u32 as u64;
            let bs_bits = b0 | (b1 << 32);

            (quote_bits, bs_bits)
        }
    }

    /// SSE2 path: 4 × 128-bit loads, `_mm_movemask_epi8` extracts 16-bit
    /// masks that are combined into a single 64-bit result.
    ///
    /// SSE2 is always available on x86_64, so this serves as the baseline.
    #[target_feature(enable = "sse2")]
    pub(super) unsafe fn find_quotes_and_backslashes_sse2(data: &[u8; 64]) -> (u64, u64) {
        // SAFETY: intrinsics require SSE2, which is guaranteed by #[target_feature].
        unsafe {
            let c0 = _mm_loadu_si128(data.as_ptr() as *const __m128i);
            let c1 = _mm_loadu_si128(data[16..].as_ptr() as *const __m128i);
            let c2 = _mm_loadu_si128(data[32..].as_ptr() as *const __m128i);
            let c3 = _mm_loadu_si128(data[48..].as_ptr() as *const __m128i);

            let vq = _mm_set1_epi8(b'"' as i8);
            let vbs = _mm_set1_epi8(b'\\' as i8);

            // movemask returns i32 with only the low 16 bits set; cast to u16
            // to discard the sign-extended upper bits, then widen to u64.
            let q0 = _mm_movemask_epi8(_mm_cmpeq_epi8(c0, vq)) as u16 as u64;
            let q1 = _mm_movemask_epi8(_mm_cmpeq_epi8(c1, vq)) as u16 as u64;
            let q2 = _mm_movemask_epi8(_mm_cmpeq_epi8(c2, vq)) as u16 as u64;
            let q3 = _mm_movemask_epi8(_mm_cmpeq_epi8(c3, vq)) as u16 as u64;
            let quote_bits = q0 | (q1 << 16) | (q2 << 32) | (q3 << 48);

            let b0 = _mm_movemask_epi8(_mm_cmpeq_epi8(c0, vbs)) as u16 as u64;
            let b1 = _mm_movemask_epi8(_mm_cmpeq_epi8(c1, vbs)) as u16 as u64;
            let b2 = _mm_movemask_epi8(_mm_cmpeq_epi8(c2, vbs)) as u16 as u64;
            let b3 = _mm_movemask_epi8(_mm_cmpeq_epi8(c3, vbs)) as u16 as u64;
            let bs_bits = b0 | (b1 << 16) | (b2 << 32) | (b3 << 48);

            (quote_bits, bs_bits)
        }
    }

    /// Runtime dispatch: prefer AVX2, fall back to SSE2.
    ///
    /// `is_x86_feature_detected!` caches the CPUID result in a static
    /// atomic, so repeated calls add only a memory load + branch.
    #[inline]
    pub(super) fn find_quotes_and_backslashes(data: &[u8; 64]) -> (u64, u64) {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: we just verified AVX2 is available.
            unsafe { find_quotes_and_backslashes_avx2(data) }
        } else {
            // SAFETY: SSE2 is always available on x86_64.
            unsafe { find_quotes_and_backslashes_sse2(data) }
        }
    }
}

// ---------------------------------------------------------------------------
// aarch64 NEON — 4 × 16-byte loads with vpaddl reduction
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
mod aarch64_impl {
    use std::arch::aarch64::*;

    /// Extract a 16-bit movemask from a `vceqq_u8` comparison result.
    ///
    /// Each byte of `cmp` is `0xFF` (matched) or `0x00` (not matched).
    /// The function returns a `u16` where bit `i` is set iff byte `i`
    /// matched.  This is the NEON equivalent of `_mm_movemask_epi8`.
    ///
    /// Algorithm: AND each lane with its positional bit value (1, 2, 4, …,
    /// 128 for lanes 0-7, same pattern for lanes 8-15), then reduce with
    /// three pairwise-add steps:
    ///   u8×16 → u16×8 → u32×4 → u64×2
    /// The two u64 lanes hold the low and high bytes of the 16-bit mask.
    #[inline(always)]
    unsafe fn movemask16(cmp: uint8x16_t) -> u16 {
        // SAFETY: intrinsics are safe when NEON is available (always on aarch64).
        unsafe {
            // Positional bit values for each lane.
            const MASK: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
            let mask = vld1q_u8(MASK.as_ptr());
            // Each lane is now 0 or its positional bit value.
            let bits = vandq_u8(cmp, mask);
            // Pairwise-add reduction collapses the 16 values into two bytes.
            let p16 = vpaddlq_u8(bits); // u8×16 → u16×8
            let p32 = vpaddlq_u16(p16); // u16×8  → u32×4
            let p64 = vpaddlq_u32(p32); // u32×4  → u64×2
            // Lane 0: sum of lanes 0-7  (bits 0-7  of the mask, fits in u8).
            // Lane 1: sum of lanes 8-15 (bits 8-15 of the mask, fits in u8).
            let lo = vgetq_lane_u64(p64, 0) as u8;
            let hi = vgetq_lane_u64(p64, 1) as u8;
            (lo as u16) | ((hi as u16) << 8)
        }
    }

    /// NEON implementation: 4 × 16-byte `vld1q_u8` loads.
    ///
    /// NEON is mandatory on all `aarch64` Rust targets, so no runtime
    /// feature detection is needed.
    #[target_feature(enable = "neon")]
    pub(super) unsafe fn find_quotes_and_backslashes_neon(data: &[u8; 64]) -> (u64, u64) {
        // SAFETY: intrinsics require NEON, which is guaranteed by #[target_feature].
        unsafe {
            let c0 = vld1q_u8(data.as_ptr());
            let c1 = vld1q_u8(data[16..].as_ptr());
            let c2 = vld1q_u8(data[32..].as_ptr());
            let c3 = vld1q_u8(data[48..].as_ptr());

            let vq = vdupq_n_u8(b'"');
            let vbs = vdupq_n_u8(b'\\');

            let quote_bits = (movemask16(vceqq_u8(c0, vq)) as u64)
                | ((movemask16(vceqq_u8(c1, vq)) as u64) << 16)
                | ((movemask16(vceqq_u8(c2, vq)) as u64) << 32)
                | ((movemask16(vceqq_u8(c3, vq)) as u64) << 48);

            let bs_bits = (movemask16(vceqq_u8(c0, vbs)) as u64)
                | ((movemask16(vceqq_u8(c1, vbs)) as u64) << 16)
                | ((movemask16(vceqq_u8(c2, vbs)) as u64) << 32)
                | ((movemask16(vceqq_u8(c3, vbs)) as u64) << 48);

            (quote_bits, bs_bits)
        }
    }

    #[inline]
    pub(super) fn find_quotes_and_backslashes(data: &[u8; 64]) -> (u64, u64) {
        // SAFETY: NEON is always available on aarch64 Rust targets.
        unsafe { find_quotes_and_backslashes_neon(data) }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_quotes() {
        let buf = br#"{"a":"b"}"#;
        let idx = ChunkIndex::new(buf);
        assert_eq!(idx.next_quote(0), Some(1));
        assert_eq!(idx.next_quote(2), Some(3));
        assert_eq!(idx.next_quote(4), Some(5));
        assert_eq!(idx.next_quote(6), Some(7));
    }

    #[test]
    fn test_escaped_quote() {
        let buf_str = r#"{"a":"hello \"world\""}"#;
        let buf = buf_str.as_bytes();
        let idx = ChunkIndex::new(buf);
        let (content, after) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(content, b"a");
        assert_eq!(after, 4);
        let (content, _) = idx.scan_string(buf, 5).unwrap();
        assert!(std::str::from_utf8(content).unwrap().contains("world"));
    }

    #[test]
    fn test_escaped_backslash_then_quote() {
        let buf = b"\"test\\\\\"";
        let idx = ChunkIndex::new(buf);
        let (content, after) = idx.scan_string(buf, 0).unwrap();
        assert_eq!(content, b"test\\\\");
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_long_buffer() {
        let mut buf = Vec::new();
        for i in 0..100 {
            let line = format!(r#"{{"n":{}}}"#, i);
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
        }
        let idx = ChunkIndex::new(&buf);
        let (content, _) = idx.scan_string(&buf, 1).unwrap();
        assert_eq!(content, b"n");
    }

    #[test]
    fn test_no_backslashes_fast_path() {
        let buf = br#"{"level":"INFO","status":200}"#;
        let idx = ChunkIndex::new(buf);
        let (k1, after1) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(k1, b"level");
        let (v1, _) = idx.scan_string(buf, after1 + 1).unwrap();
        assert_eq!(v1, b"INFO");
    }

    #[test]
    fn test_backslash_at_block_boundary() {
        let mut buf = Vec::new();
        buf.push(b'"');
        buf.extend(std::iter::repeat_n(b'x', 62));
        buf.push(b'\\');
        buf.push(b'"');
        buf.extend_from_slice(b"tail\"");
        let idx = ChunkIndex::new(&buf);
        let (content, _) = idx.scan_string(&buf, 0).unwrap();
        let s = std::str::from_utf8(content).unwrap();
        assert!(s.ends_with("tail"), "got: {s}");
    }

    #[test]
    fn test_string_mask() {
        // {"a":"b"}
        // 012345678
        let buf = br#"{"a":"b"}"#;
        let idx = ChunkIndex::new(buf);
        assert!(!idx.is_in_string(0)); // {
        assert!(!idx.is_in_string(1)); // " (opening quote, not interior)
        assert!(idx.is_in_string(2)); // a (inside string)
        assert!(!idx.is_in_string(3)); // " (closing quote)
        assert!(!idx.is_in_string(4)); // :
        assert!(!idx.is_in_string(5)); // " (opening quote)
        assert!(idx.is_in_string(6)); // b (inside string)
        assert!(!idx.is_in_string(7)); // " (closing quote)
        assert!(!idx.is_in_string(8)); // }
    }

    #[test]
    fn test_backslash_n_backslash_quote() {
        // "\n\"" — bytes: " \ n \ " "
        // Real quotes at 0 and 5 only. Quote at 4 is escaped.
        let buf = b"\"\\n\\\"\"";
        let idx = ChunkIndex::new(buf);
        let (content, after) = idx.scan_string(buf, 0).unwrap();
        assert_eq!(
            content,
            b"\\n\\\"",
            "got: {:?}",
            std::str::from_utf8(content)
        );
        assert_eq!(after, 6);
    }

    #[test]
    fn test_value_with_escapes_then_more_fields() {
        // Full line: {"A1":"\n\"","B":"x"}
        let buf = br#"{"A1":"\n\"","B":"x"}"#;
        let idx = ChunkIndex::new(buf);
        // Key "A1" at positions 1-4
        let (k, _after_k) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(k, b"A1");
        // Value "\n\"" at positions 6-11
        let (v, _after_v) = idx.scan_string(buf, 6).unwrap();
        assert_eq!(v, b"\\n\\\"", "got: {:?}", std::str::from_utf8(v));
        // Key "B" should be findable after the comma
        let (k2, _) = idx.scan_string(buf, 13).unwrap();
        assert_eq!(k2, b"B");
    }

    #[test]
    fn test_many_escaped_quotes_then_array() {
        // Escaped quotes followed by another field with array value
        let buf = br#"{"a":"\"\"","b":[]}"#;
        let idx = ChunkIndex::new(buf);
        // Key "a" at 1
        let (k, _) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(k, b"a");
        // Value "\"\"" at 5
        let (v, after_v) = idx.scan_string(buf, 5).unwrap();
        assert_eq!(v, b"\\\"\\\"", "got: {:?}", std::str::from_utf8(v));
        // Key "b" should be at after_v + 1 (after comma)
        let (k2, after_k2) = idx.scan_string(buf, after_v + 1).unwrap();
        assert_eq!(k2, b"b");
        // Array [] at after_k2 + 1
        let arr_start = after_k2 + 1;
        assert_eq!(buf[arr_start], b'[');
        let after_arr = idx.skip_nested(buf, arr_start);
        assert_eq!(&buf[arr_start..after_arr], b"[]");
    }

    #[test]
    fn test_conformance_many_escapes() {
        // The exact failing input from proptest
        let buf = br#"{"a":"\"0\"\"\"\" 0\"\"\"\"\"","b":"","c":"","d":[]}"#;
        let idx = ChunkIndex::new(buf);

        // Verify: real quotes should be at {1,3,5,29,31,33,35,36,38,40,42,43,45,47}
        // Key "a" at 1, closing at 3
        let (k, _ak) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(k, b"a", "key a");

        // Value string opens at 5, should close at 29
        let (v, av) = idx.scan_string(buf, 5).unwrap();
        assert_eq!(av, 30, "value should end after pos 29, got {av}");
        assert_eq!(v.len(), 23, "value should be 23 bytes, got {}", v.len());

        // Scan all remaining strings
        let mut pos = av;
        let mut strings = Vec::new();
        while pos < buf.len() {
            if buf[pos] == b'"'
                && let Some((content, after)) = idx.scan_string(buf, pos)
            {
                strings.push((pos, std::str::from_utf8(content).unwrap_or("?").to_string()));
                pos = after;
                continue;
            }
            pos += 1;
        }
        // After the value: , "b" : "" , "c" : "" , "d" : []
        // Strings: b, "", c, "", d
        let keys: Vec<&str> = strings.iter().map(|(_, s)| s.as_str()).collect();
        assert!(
            keys.contains(&"d"),
            "Field 'd' not found. Strings after value: {:?}",
            strings
        );
    }

    #[test]
    fn test_skip_nested() {
        let buf = br#"{"a":{"b":"c"},"d":1}"#;
        // Skip the nested object starting at position 5
        let idx = ChunkIndex::new(buf);
        let after = idx.skip_nested(buf, 5);
        assert_eq!(after, 14); // after {"b":"c"}
        assert_eq!(buf[after], b',');
    }

    #[test]
    fn test_skip_nested_braces_in_string() {
        let buf = br#"{"data":{"msg":"has } and {"},"ok":1}"#;
        let idx = ChunkIndex::new(buf);
        let after = idx.skip_nested(buf, 8);
        // Should skip past {"msg":"has } and {"} correctly
        assert_eq!(buf[after], b',');
    }

    // -----------------------------------------------------------------------
    // SIMD bitmask extraction tests
    //
    // These tests call the low-level SIMD helpers directly to verify that the
    // movemask-equivalent reduction produces the expected u64 bitmask, and
    // cross-check against the scalar `find_char_mask` reference.
    // -----------------------------------------------------------------------

    /// Build the expected bitmask the slow but obviously correct way.
    fn scalar_mask(data: &[u8; 64], needle: u8) -> u64 {
        let mut bits: u64 = 0;
        for (i, &b) in data.iter().enumerate() {
            bits |= ((b == needle) as u64) << i;
        }
        bits
    }

    /// Build a 64-byte block with `needle` at each position in `positions`.
    fn block_with(positions: &[usize], needle: u8) -> [u8; 64] {
        let mut data = [b' '; 64];
        for &p in positions {
            data[p] = needle;
        }
        data
    }

    #[cfg(target_arch = "x86_64")]
    mod x86_tests {
        use super::*;
        use crate::chunk_classify::x86::{
            find_quotes_and_backslashes_avx2, find_quotes_and_backslashes_sse2,
        };

        fn check(positions: &[usize], needle: u8) {
            let data = block_with(positions, needle);
            let expected_q = scalar_mask(&data, b'"');
            let expected_bs = scalar_mask(&data, b'\\');

            // SSE2 (always available on x86_64)
            let (q_sse2, bs_sse2) =
                unsafe { find_quotes_and_backslashes_sse2(&data) };
            assert_eq!(
                q_sse2, expected_q,
                "SSE2 quote mismatch at positions {positions:?}"
            );
            assert_eq!(
                bs_sse2, expected_bs,
                "SSE2 backslash mismatch at positions {positions:?}"
            );

            // AVX2 (skip at runtime if not available)
            if is_x86_feature_detected!("avx2") {
                let (q_avx2, bs_avx2) =
                    unsafe { find_quotes_and_backslashes_avx2(&data) };
                assert_eq!(
                    q_avx2, expected_q,
                    "AVX2 quote mismatch at positions {positions:?}"
                );
                assert_eq!(
                    bs_avx2, expected_bs,
                    "AVX2 backslash mismatch at positions {positions:?}"
                );
            }
        }

        #[test]
        fn simd_no_special_chars() {
            check(&[], b'"');
        }

        #[test]
        fn simd_all_positions_quote() {
            let positions: Vec<usize> = (0..64).collect();
            check(&positions, b'"');
        }

        #[test]
        fn simd_all_positions_backslash() {
            let positions: Vec<usize> = (0..64).collect();
            check(&positions, b'\\');
        }

        #[test]
        fn simd_boundary_positions() {
            // Test positions at the 16-byte and 32-byte chunk boundaries.
            check(&[0, 15, 16, 31, 32, 47, 48, 63], b'"');
        }

        #[test]
        fn simd_mixed_quote_and_backslash() {
            let mut data = [b' '; 64];
            data[0] = b'"';
            data[31] = b'"';
            data[32] = b'\\';
            data[63] = b'"';
            let expected_q = scalar_mask(&data, b'"');
            let expected_bs = scalar_mask(&data, b'\\');

            let (q_sse2, bs_sse2) =
                unsafe { find_quotes_and_backslashes_sse2(&data) };
            assert_eq!(q_sse2, expected_q);
            assert_eq!(bs_sse2, expected_bs);

            if is_x86_feature_detected!("avx2") {
                let (q_avx2, bs_avx2) =
                    unsafe { find_quotes_and_backslashes_avx2(&data) };
                assert_eq!(q_avx2, expected_q);
                assert_eq!(bs_avx2, expected_bs);
            }
        }

        #[test]
        fn simd_dispatcher_matches_scalar() {
            // The public dispatcher should agree with the scalar reference
            // for every bit position.
            for pos in 0..64usize {
                let data = block_with(&[pos], b'"');
                let (q, _) = super::super::find_quotes_and_backslashes(&data);
                assert_eq!(q, 1u64 << pos, "dispatcher quote mismatch at pos {pos}");

                let data = block_with(&[pos], b'\\');
                let (_, bs) = super::super::find_quotes_and_backslashes(&data);
                assert_eq!(bs, 1u64 << pos, "dispatcher backslash mismatch at pos {pos}");
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    mod neon_tests {
        use super::*;
        use crate::chunk_classify::aarch64_impl::find_quotes_and_backslashes_neon;

        fn check(positions: &[usize], needle: u8) {
            let data = block_with(positions, needle);
            let expected_q = scalar_mask(&data, b'"');
            let expected_bs = scalar_mask(&data, b'\\');

            let (q, bs) = unsafe { find_quotes_and_backslashes_neon(&data) };
            assert_eq!(
                q, expected_q,
                "NEON quote mismatch at positions {positions:?}"
            );
            assert_eq!(
                bs, expected_bs,
                "NEON backslash mismatch at positions {positions:?}"
            );
        }

        #[test]
        fn neon_no_special_chars() {
            check(&[], b'"');
        }

        #[test]
        fn neon_all_positions_quote() {
            let positions: Vec<usize> = (0..64).collect();
            check(&positions, b'"');
        }

        #[test]
        fn neon_all_positions_backslash() {
            let positions: Vec<usize> = (0..64).collect();
            check(&positions, b'\\');
        }

        #[test]
        fn neon_boundary_positions() {
            check(&[0, 15, 16, 31, 32, 47, 48, 63], b'"');
        }

        #[test]
        fn neon_dispatcher_matches_scalar() {
            for pos in 0..64usize {
                let data = block_with(&[pos], b'"');
                let (q, _) = super::super::find_quotes_and_backslashes(&data);
                assert_eq!(q, 1u64 << pos, "NEON dispatcher quote mismatch at pos {pos}");

                let data = block_with(&[pos], b'\\');
                let (_, bs) = super::super::find_quotes_and_backslashes(&data);
                assert_eq!(bs, 1u64 << pos, "NEON dispatcher backslash mismatch at pos {pos}");
            }
        }
    }
}
