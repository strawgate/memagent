// chunk_classify.rs — Whole-buffer SIMD structural classification.
//
// Pre-classifies an entire NDJSON chunk buffer in one SIMD pass, producing
// bitmasks that the scanner can query in O(1) instead of doing per-string
// SIMD loads.
//
// This is the simdjson "stage 1" algorithm adapted for our use case.

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::*;

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
    #[cfg(target_arch = "aarch64")]
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
                // Full block — read directly from buffer, no copy.
                // SAFETY: remaining >= 64 guarantees 64 readable bytes.
                unsafe {
                    find_quotes_and_backslashes(&*(buf.as_ptr().add(offset) as *const [u8; 64]))
                }
            } else {
                // Tail block — pad with spaces.
                let mut padded = [b' '; 64];
                padded[..remaining].copy_from_slice(&buf[offset..offset + remaining]);
                unsafe { find_quotes_and_backslashes(&padded) }
            };
            let real_q = compute_real_quotes(quote_bits, bs_bits, &mut prev_odd_backslash);

            #[cfg(test)]
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

    /// Scalar fallback for non-aarch64 platforms.
    #[cfg(not(target_arch = "aarch64"))]
    pub fn new(buf: &[u8]) -> Self {
        let len = buf.len();
        let num_blocks = len.div_ceil(64);
        let mut real_quotes = Vec::with_capacity(num_blocks);
        let mut in_string_vec = Vec::with_capacity(num_blocks);
        let mut in_string = false;
        let mut escaped_next = false;
        let mut i = 0;

        for _ in 0..num_blocks {
            let mut q_bits: u64 = 0;
            let mut s_bits: u64 = 0;
            for bit in 0..64u64 {
                if i >= len {
                    break;
                }
                if escaped_next {
                    escaped_next = false;
                    if in_string {
                        s_bits |= 1u64 << bit;
                    }
                    i += 1;
                    continue;
                }
                if in_string {
                    if buf[i] == b'\\' {
                        s_bits |= 1u64 << bit;
                        escaped_next = true;
                        i += 1;
                        continue;
                    }
                    if buf[i] == b'"' {
                        q_bits |= 1u64 << bit;
                        in_string = false;
                        i += 1;
                        continue;
                    }
                    s_bits |= 1u64 << bit;
                } else if buf[i] == b'"' {
                    q_bits |= 1u64 << bit;
                    in_string = true;
                }
                i += 1;
            }
            real_quotes.push(q_bits);
            in_string_vec.push(s_bits);
        }

        ChunkIndex {
            real_quotes,
            in_string: in_string_vec,
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
// NEON intrinsics
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn find_quotes_and_backslashes(data: &[u8; 64]) -> (u64, u64) {
    unsafe {
        let ptr = data.as_ptr();
        let v0 = vld1q_u8(ptr);
        let v1 = vld1q_u8(ptr.add(16));
        let v2 = vld1q_u8(ptr.add(32));
        let v3 = vld1q_u8(ptr.add(48));

        let quote = vdupq_n_u8(b'"');
        let backslash = vdupq_n_u8(b'\\');

        let q0 = vceqq_u8(v0, quote);
        let q1 = vceqq_u8(v1, quote);
        let q2 = vceqq_u8(v2, quote);
        let q3 = vceqq_u8(v3, quote);

        let b0 = vceqq_u8(v0, backslash);
        let b1 = vceqq_u8(v1, backslash);
        let b2 = vceqq_u8(v2, backslash);
        let b3 = vceqq_u8(v3, backslash);

        (
            neon_to_bitmask64(q0, q1, q2, q3),
            neon_to_bitmask64(b0, b1, b2, b3),
        )
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn neon_to_bitmask64(v0: uint8x16_t, v1: uint8x16_t, v2: uint8x16_t, v3: uint8x16_t) -> u64 {
    unsafe {
        let bit_mask: uint8x16_t = core::mem::transmute([
            0x01u8, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20,
            0x40, 0x80,
        ]);
        let t0 = vandq_u8(v0, bit_mask);
        let t1 = vandq_u8(v1, bit_mask);
        let t2 = vandq_u8(v2, bit_mask);
        let t3 = vandq_u8(v3, bit_mask);
        let pair01 = vpaddq_u8(t0, t1);
        let pair23 = vpaddq_u8(t2, t3);
        let quad = vpaddq_u8(pair01, pair23);
        let octa = vpaddq_u8(quad, quad);
        vgetq_lane_u64(vreinterpretq_u64_u8(octa), 0)
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
        for _ in 0..62 {
            buf.push(b'x');
        }
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
        let (k, after_k) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(k, b"A1");
        // Value "\n\"" at positions 6-11
        let (v, after_v) = idx.scan_string(buf, 6).unwrap();
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
        let (k, ak) = idx.scan_string(buf, 1).unwrap();
        assert_eq!(k, b"a", "key a");

        // Value string opens at 5, should close at 29
        let (v, av) = idx.scan_string(buf, 5).unwrap();
        assert_eq!(av, 30, "value should end after pos 29, got {av}");
        assert_eq!(v.len(), 23, "value should be 23 bytes, got {}", v.len());

        // Scan all remaining strings
        let mut pos = av;
        let mut strings = Vec::new();
        while pos < buf.len() {
            if buf[pos] == b'"' {
                if let Some((content, after)) = idx.scan_string(buf, pos) {
                    strings.push((pos, std::str::from_utf8(content).unwrap_or("?").to_string()));
                    pos = after;
                    continue;
                }
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
}
