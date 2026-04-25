//! CRI partial line reassembler.
//!
//! Merges CRI partial ("P") and full ("F") lines into complete messages.
//!
//! # Zero-copy design
//!
//! - **F lines with no pending partials (99% case):** the message bytes
//!   pass through without any copy. The caller receives a reference to the
//!   original input buffer.
//! - **P lines:** message bytes are copied into an internal `Vec<u8>` buffer.
//!   This frees the read buffer's refcount so it can be reused.
//! - **F lines after P lines:** the final message is appended to the buffer
//!   and the concatenated result is returned.
//!
//! The internal buffer reuses its capacity across P/F sequences via `clear()`,
//! so allocation only happens once (on the first P line seen).
#![allow(clippy::indexing_slicing)]

/// Aggregates CRI partial lines into complete messages.
///
/// Feed CRI message bytes with their P/F flag. The aggregator returns
/// [`AggregateResult::Complete`] when a complete message is ready (on F lines),
/// or [`AggregateResult::Pending`] for P lines (buffered internally).
/// Returns [`AggregateResult::Truncated`] when any chunk exceeded `max_message_size`.
///
/// # Lifecycle
///
/// ```text
/// P "hello "  → Pending (buffered)
/// P "world"   → Pending (buffered)
/// F "!"       → Complete("hello world!")
/// F "simple"  → Complete("simple")  // zero-copy fast path
/// ```
use alloc::vec::Vec;

/// Slack added to `max_message_size` when capping the raw CRI line fragment buffer.
///
/// The fragment buffer holds an entire raw CRI line (header + message) until a
/// newline delimiter arrives. The CRI header (timestamp + stream + flag) is
/// typically ~31-45 bytes, but the raw line also contains the full *unsplit*
/// message which may exceed `max_message_size` (truncation happens later at
/// the message layer, not the raw-line layer). Using a generous constant
/// ensures that split lines with small `max_message_size` settings are still
/// parsed correctly rather than silently discarded as fragment-truncation errors.
const CRI_RAW_LINE_OVERHEAD: usize = 256;

/// CRI partial line aggregator. Merges P/F lines into complete messages.
pub struct CriReassembler {
    pending: Vec<u8>,
    line_fragment: Vec<u8>,
    max_message_size: usize,
    /// Set to `true` when any chunk in the current P/F sequence was truncated
    /// due to `max_message_size`. Reset in `reset()`.
    truncated: bool,
    line_fragment_truncated: bool,
}

/// Result of feeding a line to the aggregator.
pub enum AggregateResult<'a> {
    /// Complete message ready. Either a reference to the input buffer
    /// (zero-copy F line) or to the aggregator's internal buffer
    /// (concatenated P+F).
    Complete(&'a [u8]),
    /// Complete but truncated message ready: one or more chunks exceeded
    /// `max_message_size` and were silently dropped in the original code.
    /// Callers should log a warning and/or increment a diagnostics counter.
    Truncated(&'a [u8]),
    /// Partial line buffered. No output yet.
    Pending,
}

impl CriReassembler {
    /// Create a new aggregator with the given maximum message size.
    ///
    /// Messages exceeding this size are truncated. Set to a generous
    /// default (e.g., 256KB) to handle Java stack traces and large
    /// structured log lines.
    pub fn new(max_message_size: usize) -> Self {
        CriReassembler {
            pending: Vec::new(),
            line_fragment: Vec::new(),
            max_message_size,
            truncated: false,
            line_fragment_truncated: false,
        }
    }

    /// Feed a CRI message. Returns the complete message on F lines.
    ///
    /// - `message`: the message bytes from the CRI line (not the full
    ///   CRI line — just the message field after timestamp/stream/flags).
    /// - `is_full`: true for F (full) lines, false for P (partial) lines.
    ///
    /// For the common case (F line, no pending partials), returns a
    /// reference to the input `message` with zero copies.
    ///
    /// Returns [`AggregateResult::Truncated`] instead of
    /// [`AggregateResult::Complete`] when any chunk in the current P/F
    /// sequence exceeded `max_message_size`. Callers should log a warning.
    pub fn feed<'a>(&'a mut self, message: &'a [u8], is_full: bool) -> AggregateResult<'a> {
        if is_full {
            if self.pending.is_empty() {
                // Fast path: F line, no partials. Zero copy when not truncated.
                let end = message.len().min(self.max_message_size);
                let was_truncated = end < message.len();
                if was_truncated {
                    AggregateResult::Truncated(&message[..end])
                } else {
                    AggregateResult::Complete(&message[..end])
                }
            } else {
                // Append final chunk, respecting max size.
                let remaining = self.max_message_size.saturating_sub(self.pending.len());
                let to_add = message.len().min(remaining);
                if to_add < message.len() {
                    self.truncated = true;
                }
                self.pending.extend_from_slice(&message[..to_add]);
                if self.truncated {
                    AggregateResult::Truncated(&self.pending)
                } else {
                    AggregateResult::Complete(&self.pending)
                }
            }
        } else {
            // P line: copy into buffer, frees the read buffer.
            let remaining = self.max_message_size.saturating_sub(self.pending.len());
            let to_add = message.len().min(remaining);
            if to_add < message.len() {
                self.truncated = true;
            }
            self.pending.extend_from_slice(&message[..to_add]);
            AggregateResult::Pending
        }
    }

    /// Reset the aggregator after consuming a complete message.
    ///
    /// Must be called after each `AggregateResult::Complete` or
    /// `AggregateResult::Truncated` before feeding the next line. Clears the
    /// internal buffer and truncation flag but preserves allocated capacity.
    pub fn reset(&mut self) {
        self.pending.clear();
        self.line_fragment.clear();
        self.truncated = false;
        self.line_fragment_truncated = false;
    }

    /// Returns true if there are pending partial lines.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Returns true if the reassembler has any buffered state.
    ///
    /// This includes assembled CRI `P`/`F` message bytes and raw CRI line
    /// fragments buffered across chunk boundaries.
    pub fn has_buffered_state(&self) -> bool {
        self.has_pending() || self.has_line_fragment()
    }

    /// Returns the configured maximum message size.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    fn raw_line_fragment_limit(&self) -> usize {
        self.max_message_size.saturating_add(CRI_RAW_LINE_OVERHEAD)
    }

    /// Append raw bytes for an unterminated CRI line split across chunk boundaries.
    ///
    /// The buffered bytes are bounded by `max_message_size + CRI_RAW_LINE_OVERHEAD`
    /// to prevent unbounded growth while still allowing the full CRI header
    /// (timestamp + stream + flag) plus the message to be buffered. Using only
    /// `max_message_size` would truncate the raw line before the header is fully
    /// received, turning a valid split line into a parse error.
    pub(crate) fn push_line_fragment(&mut self, bytes: &[u8]) {
        let remaining = self
            .raw_line_fragment_limit()
            .saturating_sub(self.line_fragment.len());
        let to_add = bytes.len().min(remaining);
        if to_add < bytes.len() {
            self.line_fragment_truncated = true;
        }
        self.line_fragment.extend_from_slice(&bytes[..to_add]);
    }

    /// Returns true if raw CRI bytes are buffered for the next chunk.
    pub(crate) fn has_line_fragment(&self) -> bool {
        !self.line_fragment.is_empty()
    }

    /// Returns true if the buffered raw CRI line fragment was truncated.
    pub(crate) fn line_fragment_truncated(&self) -> bool {
        self.line_fragment_truncated
    }

    /// Move out the buffered raw CRI line fragment and clear truncation state.
    ///
    /// Preserves the internal buffer capacity so the next split-line sequence
    /// can reuse the existing allocation instead of starting from zero capacity.
    pub(crate) fn take_line_fragment(&mut self) -> Vec<u8> {
        let capacity = self.line_fragment.capacity();
        self.line_fragment_truncated = false;
        core::mem::replace(&mut self.line_fragment, Vec::with_capacity(capacity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f_only_zero_copy() {
        let mut agg = CriReassembler::new(1024);
        let msg = b"hello world";
        match agg.feed(msg, true) {
            AggregateResult::Complete(out) => {
                assert_eq!(out, b"hello world");
                // Verify zero-copy: output points to the same memory as input
                assert!(core::ptr::eq(out.as_ptr(), msg.as_ptr()));
            }
            AggregateResult::Truncated(_) => panic!("expected Complete, not Truncated"),
            AggregateResult::Pending => panic!("expected Complete"),
        }
        agg.reset();
    }

    #[test]
    fn p_then_f() {
        let mut agg = CriReassembler::new(1024);

        assert!(matches!(
            agg.feed(b"hello ", false),
            AggregateResult::Pending
        ));
        assert!(matches!(
            agg.feed(b"world", false),
            AggregateResult::Pending
        ));

        match agg.feed(b"!", true) {
            AggregateResult::Complete(out) => assert_eq!(out, b"hello world!"),
            AggregateResult::Truncated(out) => panic!("unexpected truncation: {:?}", out),
            AggregateResult::Pending => panic!("expected Complete"),
        }
        agg.reset();
    }

    #[test]
    fn max_size_truncation() {
        let mut agg = CriReassembler::new(10);

        // P line with 8 bytes
        agg.feed(b"12345678", false);

        // F line with 8 bytes — only 2 should fit; result is Truncated
        match agg.feed(b"abcdefgh", true) {
            AggregateResult::Truncated(out) => {
                assert_eq!(out.len(), 10);
                assert_eq!(out, b"12345678ab");
            }
            AggregateResult::Complete(out) => {
                panic!("expected Truncated, got Complete: {:?}", out)
            }
            AggregateResult::Pending => panic!("expected Truncated"),
        }
        agg.reset();
    }

    #[test]
    fn max_size_f_only() {
        let mut agg = CriReassembler::new(5);
        match agg.feed(b"truncated", true) {
            AggregateResult::Truncated(out) => {
                assert_eq!(out, b"trunc");
            }
            AggregateResult::Complete(out) => {
                panic!("expected Truncated, got Complete: {:?}", out)
            }
            AggregateResult::Pending => panic!("expected Truncated"),
        }
        agg.reset();
    }

    #[test]
    fn reset_preserves_capacity() {
        let mut agg = CriReassembler::new(1024);
        agg.feed(b"some data", false);
        let cap_before = agg.pending.capacity();
        agg.push_line_fragment(b"partial-cri-line");
        agg.reset();
        assert_eq!(agg.pending.capacity(), cap_before);
        assert!(agg.pending.is_empty());
        assert!(!agg.has_line_fragment());
        assert!(!agg.truncated);
        assert!(!agg.line_fragment_truncated);
    }

    #[test]
    fn has_buffered_state_tracks_pending_and_line_fragments() {
        let mut agg = CriReassembler::new(32);
        assert!(!agg.has_buffered_state());

        assert!(matches!(
            agg.feed(b"partial", false),
            AggregateResult::Pending
        ));
        assert!(agg.has_buffered_state());

        agg.reset();
        assert!(!agg.has_buffered_state());

        agg.push_line_fragment(b"2024-01-15T10:30:00Z stdout F hel");
        assert!(agg.has_buffered_state());

        agg.reset();
        assert!(!agg.has_buffered_state());
    }

    #[test]
    fn multiple_sequences() {
        let mut agg = CriReassembler::new(1024);

        // Sequence 1: P + F
        agg.feed(b"a", false);
        match agg.feed(b"b", true) {
            AggregateResult::Complete(out) => assert_eq!(out, b"ab"),
            _ => panic!("expected Complete"),
        }
        agg.reset();

        // Sequence 2: F only
        match agg.feed(b"standalone", true) {
            AggregateResult::Complete(out) => {
                assert_eq!(out, b"standalone");
            }
            _ => panic!("expected Complete"),
        }
        agg.reset();

        // Sequence 3: P + P + F
        agg.feed(b"x", false);
        agg.feed(b"y", false);
        match agg.feed(b"z", true) {
            AggregateResult::Complete(out) => assert_eq!(out, b"xyz"),
            _ => panic!("expected Complete"),
        }
        agg.reset();
    }
}

// ---------------------------------------------------------------------------
// Proptest: random P/F sequences
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::Config as ProptestConfig;

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            .. ProptestConfig::default()
        })]
        /// Output never exceeds max_message_size for any sequence of P and F feeds.
        ///
        /// Extends the Kani proofs (fixed depth P+F, P+P+F) to arbitrary-length
        /// partial-line sequences. The truncation invariant must hold regardless
        /// of how many P chunks accumulate before the final F.
        #[test]
        fn reassembler_output_never_exceeds_max_size(
            max_size in 1..256usize,
            sequence in proptest::collection::vec(
                (proptest::collection::vec(proptest::num::u8::ANY, 0..64usize), proptest::bool::ANY),
                1..20usize,
            )
        ) {
            let mut agg = CriReassembler::new(max_size);
            for (msg, is_full) in sequence {
                match agg.feed(&msg, is_full) {
                    AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                        prop_assert!(
                            out.len() <= max_size,
                            "output {} exceeded max_size {}",
                            out.len(),
                            max_size
                        );
                        agg.reset();
                    }
                    AggregateResult::Pending => {}
                }
            }
        }

        /// P-only sequences (no F line) never produce output.
        ///
        /// Even a very long sequence of P chunks must remain Pending — no output
        /// until a Full line arrives. Tests the pending buffer growth without
        /// accidental early output.
        #[test]
        fn reassembler_p_only_stays_pending(
            max_size in 1..256usize,
            p_chunks in proptest::collection::vec(
                proptest::collection::vec(proptest::num::u8::ANY, 0..32usize),
                1..20usize,
            )
        ) {
            let mut agg = CriReassembler::new(max_size);
            for chunk in p_chunks {
                match agg.feed(&chunk, false) {
                    AggregateResult::Pending => {}
                    AggregateResult::Complete(_) | AggregateResult::Truncated(_) => {
                        prop_assert!(false, "P line must not produce Complete or Truncated");
                    }
                }
            }
        }

        /// After reset(), the aggregator behaves identically to a freshly constructed one.
        ///
        /// Verifies that reset() is a true reset: the next F line takes the
        /// zero-copy fast path and ignores any prior pending data.
        #[test]
        fn reassembler_reset_is_clean_slate(
            max_size in 1..256usize,
            p_chunk in proptest::collection::vec(proptest::num::u8::ANY, 0..32usize),
            f_chunk in proptest::collection::vec(proptest::num::u8::ANY, 0..32usize),
        ) {
            let mut agg = CriReassembler::new(max_size);

            // Accumulate some P data, then reset
            let _ = agg.feed(&p_chunk, false);
            agg.reset();
            prop_assert!(!agg.has_pending(), "reset should clear pending");
            prop_assert!(!agg.truncated, "reset should clear truncated flag");

            // After reset, F line takes fast path: output matches f_chunk[..min(len, max_size)]
            let expected_len = f_chunk.len().min(max_size);
            match agg.feed(&f_chunk, true) {
                AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                    prop_assert_eq!(out.len(), expected_len);
                    // Guard: zip truncates to the shorter slice, so assert out
                    // is no longer than f_chunk to ensure every byte is checked.
                    prop_assert!(out.len() <= f_chunk.len(), "output longer than input");
                    for (i, (&actual, &expected)) in out.iter().zip(f_chunk.iter()).enumerate() {
                        prop_assert_eq!(actual, expected, "byte {} mismatch after reset", i);
                    }
                }
                AggregateResult::Pending => prop_assert!(false, "F should produce Complete"),
            }
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    /// Prove CriReassembler::feed respects max_message_size for F-only lines.
    #[kani::proof]
    fn verify_reassembler_f_only_max_size() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut agg = CriReassembler::new(max_size);
        let msg: [u8; 16] = kani::any();

        match agg.feed(&msg, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                assert!(out.len() <= max_size, "F-only output exceeds max");

                // Guard vacuity: verify constraint doesn't eliminate all paths
                kani::cover!(out.len() > 0, "non-empty output");
                kani::cover!(out.len() == max_size, "output at max size");
                kani::cover!(out.len() < max_size, "output under max size");
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove CriReassembler::feed respects max_message_size for P+F sequences.
    #[kani::proof]
    fn verify_reassembler_pf_max_size() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut agg = CriReassembler::new(max_size);

        let msg1: [u8; 8] = kani::any();
        let _ = agg.feed(&msg1, false);

        let msg2: [u8; 8] = kani::any();
        match agg.feed(&msg2, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                assert!(out.len() <= max_size, "P+F output exceeds max");

                // Guard vacuity: verify constraint allows meaningful paths.
                // "P+F concatenation happened" = F bytes were appended after P bytes,
                // requiring max_size > 8 so both chunks contributed.
                kani::cover!(out.len() > 8, "F bytes appended after full P chunk");
                kani::cover!(out.len() == max_size, "truncated at max_size boundary");
                kani::cover!(out.len() < max_size, "output fits without truncation");
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove P lines never produce output.
    #[kani::proof]
    fn verify_reassembler_p_returns_pending() {
        let mut agg = CriReassembler::new(1024);
        let msg: [u8; 8] = kani::any();
        match agg.feed(&msg, false) {
            AggregateResult::Pending => {} // expected
            AggregateResult::Complete(_) | AggregateResult::Truncated(_) => {
                panic!("P line should not produce Complete or Truncated")
            }
        }
    }

    /// Prove 3-step P+P+F respects max_message_size.
    #[kani::proof]
    fn verify_reassembler_ppf_max_size() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut agg = CriReassembler::new(max_size);

        let msg1: [u8; 8] = kani::any();
        let _ = agg.feed(&msg1, false);

        let msg2: [u8; 8] = kani::any();
        let _ = agg.feed(&msg2, false);

        let msg3: [u8; 8] = kani::any();
        match agg.feed(&msg3, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                assert!(out.len() <= max_size, "P+P+F output exceeds max");

                // Guard vacuity: confirm interesting multi-partial cases are explored.
                // "all three chunks contributed" requires max_size > 16 (so both P chunks fit).
                kani::cover!(out.len() > 16, "all three chunks contributed bytes");
                kani::cover!(out.len() == max_size, "truncated at max_size boundary");
                // "all three chunks fit without truncation" requires max_size >= 24.
                kani::cover!(out.len() == 24, "all three chunks fit without truncation");
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove reset after Complete allows a clean new sequence.
    #[kani::proof]
    fn verify_reassembler_reset_clears_state() {
        let mut agg = CriReassembler::new(64);

        let msg1: [u8; 4] = kani::any();
        let _ = agg.feed(&msg1, false);
        assert!(agg.has_pending());
        assert!(agg.has_buffered_state());

        agg.reset();
        assert!(!agg.has_pending());
        assert!(!agg.has_buffered_state());
        assert!(!agg.truncated, "reset must clear truncated flag");

        // After reset, a new F line works as zero-copy
        let msg2: [u8; 4] = kani::any();
        match agg.feed(&msg2, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                assert_eq!(out.len(), 4);
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove buffered-state reporting covers both pending P/F bytes and raw
    /// line fragments.
    #[kani::proof]
    fn verify_reassembler_buffered_state_tracks_all_internal_buffers() {
        let mut agg = CriReassembler::new(64);
        assert!(!agg.has_buffered_state());

        let msg: [u8; 4] = kani::any();
        let _ = agg.feed(&msg, false);
        assert!(agg.has_buffered_state());

        agg.reset();
        assert!(!agg.has_buffered_state());

        let fragment: [u8; 4] = kani::any();
        agg.push_line_fragment(&fragment);
        assert!(agg.has_buffered_state());

        agg.reset();
        assert!(!agg.has_buffered_state());
    }

    /// max_message_size=0 never panics — output is always empty.
    ///
    /// Edge case: zero-sized limit. P lines return Pending (no allocation),
    /// F lines return Truncated(&[]) without slicing panics.
    #[kani::proof]
    fn verify_reassembler_max_size_zero() {
        let mut agg = CriReassembler::new(0);
        let msg: [u8; 8] = kani::any();

        // P line: no output, no panic
        match agg.feed(&msg, false) {
            AggregateResult::Pending => {}
            AggregateResult::Complete(_) | AggregateResult::Truncated(_) => {
                panic!("P should not produce Complete or Truncated")
            }
        }

        // F line with max=0: any non-empty message is truncated to empty.
        match agg.feed(&msg, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                assert!(out.is_empty(), "max_size=0 must produce empty output");
            }
            AggregateResult::Pending => panic!("F should produce Complete or Truncated"),
        }
    }

    /// F-only fast path: when no partials are pending, output is a byte-for-byte
    /// prefix of the input (zero-copy — no data transformation or allocation).
    #[kani::proof]
    fn verify_reassembler_f_only_is_input_prefix() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32); // consistent with other reassembler proofs
        let mut agg = CriReassembler::new(max_size);
        assert!(!agg.has_pending()); // fast path requires no pending

        let msg: [u8; 8] = kani::any();
        match agg.feed(&msg, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                let expected_len = msg.len().min(max_size);
                assert_eq!(out.len(), expected_len, "output length incorrect");

                // Every output byte must match the corresponding input byte
                let mut i = 0;
                while i < out.len() {
                    assert_eq!(out[i], msg[i], "output byte {} differs from input", i);
                    i += 1;
                }

                kani::cover!(out.len() < msg.len(), "truncated by max_size");
                kani::cover!(out.len() == msg.len(), "full message fits");
            }
            AggregateResult::Pending => panic!("F should produce Complete"),
        }
    }

    /// P+F slow path: output is a byte-for-byte prefix of the P chunk followed
    /// by a prefix of the F chunk, bounded by max_message_size.
    #[kani::proof]
    fn verify_reassembler_pf_content_correct() {
        let max_size: usize = kani::any_where(|&s: &usize| s >= 1 && s <= 16);
        let mut agg = CriReassembler::new(max_size);

        let p_msg: [u8; 4] = kani::any();
        let _ = agg.feed(&p_msg, false); // P chunk

        let f_msg: [u8; 4] = kani::any();
        match agg.feed(&f_msg, true) {
            AggregateResult::Complete(out) | AggregateResult::Truncated(out) => {
                assert!(out.len() <= max_size);

                // First out.len().min(4) bytes must match p_msg
                let p_bytes = p_msg.len().min(max_size).min(out.len());
                let mut i = 0;
                while i < p_bytes {
                    assert_eq!(out[i], p_msg[i], "P chunk byte {} wrong", i);
                    i += 1;
                }

                // Remaining bytes come from f_msg
                let f_start = p_bytes;
                let mut j = 0;
                while f_start + j < out.len() {
                    assert_eq!(out[f_start + j], f_msg[j], "F chunk byte {} wrong", j);
                    j += 1;
                }

                kani::cover!(out.len() == p_msg.len() + f_msg.len(), "full concat fits");
                kani::cover!(out.len() == max_size && max_size < 8, "truncated concat");
            }
            AggregateResult::Pending => panic!("F should produce Complete"),
        }
    }
}
