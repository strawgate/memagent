//! CRI partial line aggregator.
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

/// Aggregates CRI partial lines into complete messages.
///
/// Feed CRI message bytes with their P/F flag. The aggregator returns
/// `Some(message)` when a complete message is ready (on F lines), or
/// `None` for P lines (buffered internally).
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
/// CRI partial line aggregator. Merges P/F lines into complete messages.
pub struct CriAggregator {
    pending: Vec<u8>,
    max_message_size: usize,
}

/// Result of feeding a line to the aggregator.
pub enum AggregateResult<'a> {
    /// Complete message ready. Either a reference to the input buffer
    /// (zero-copy F line) or to the aggregator's internal buffer
    /// (concatenated P+F).
    Complete(&'a [u8]),
    /// Partial line buffered. No output yet.
    Pending,
}

impl CriAggregator {
    /// Create a new aggregator with the given maximum message size.
    ///
    /// Messages exceeding this size are truncated. Set to a generous
    /// default (e.g., 256KB) to handle Java stack traces and large
    /// structured log lines.
    pub fn new(max_message_size: usize) -> Self {
        CriAggregator {
            pending: Vec::new(),
            max_message_size,
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
    pub fn feed<'a>(&'a mut self, message: &'a [u8], is_full: bool) -> AggregateResult<'a> {
        if is_full {
            if self.pending.is_empty() {
                // Fast path: F line, no partials. Zero copy.
                // Enforce max size even on single F lines.
                let end = message.len().min(self.max_message_size);
                AggregateResult::Complete(&message[..end])
            } else {
                // Append final chunk, respecting max size.
                let remaining = self.max_message_size.saturating_sub(self.pending.len());
                let to_add = message.len().min(remaining);
                self.pending.extend_from_slice(&message[..to_add]);
                AggregateResult::Complete(&self.pending)
            }
        } else {
            // P line: copy into buffer, frees the read buffer.
            let remaining = self.max_message_size.saturating_sub(self.pending.len());
            let to_add = message.len().min(remaining);
            self.pending.extend_from_slice(&message[..to_add]);
            AggregateResult::Pending
        }
    }

    /// Reset the aggregator after consuming a complete message.
    ///
    /// Must be called after each `AggregateResult::Complete` before
    /// feeding the next line. Clears the internal buffer but preserves
    /// its allocated capacity.
    pub fn reset(&mut self) {
        self.pending.clear();
    }

    /// Returns true if there are pending partial lines.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f_only_zero_copy() {
        let mut agg = CriAggregator::new(1024);
        let msg = b"hello world";
        match agg.feed(msg, true) {
            AggregateResult::Complete(out) => {
                assert_eq!(out, b"hello world");
                // Verify zero-copy: output points to the same memory as input
                assert!(core::ptr::eq(out.as_ptr(), msg.as_ptr()));
            }
            AggregateResult::Pending => panic!("expected Complete"),
        }
        agg.reset();
    }

    #[test]
    fn p_then_f() {
        let mut agg = CriAggregator::new(1024);

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
            AggregateResult::Pending => panic!("expected Complete"),
        }
        agg.reset();
    }

    #[test]
    fn max_size_truncation() {
        let mut agg = CriAggregator::new(10);

        // P line with 8 bytes
        agg.feed(b"12345678", false);

        // F line with 8 bytes — only 2 should fit
        match agg.feed(b"abcdefgh", true) {
            AggregateResult::Complete(out) => {
                assert_eq!(out.len(), 10);
                assert_eq!(out, b"12345678ab");
            }
            AggregateResult::Pending => panic!("expected Complete"),
        }
        agg.reset();
    }

    #[test]
    fn max_size_f_only() {
        let mut agg = CriAggregator::new(5);
        match agg.feed(b"truncated", true) {
            AggregateResult::Complete(out) => {
                assert_eq!(out, b"trunc");
            }
            AggregateResult::Pending => panic!("expected Complete"),
        }
        agg.reset();
    }

    #[test]
    fn reset_preserves_capacity() {
        let mut agg = CriAggregator::new(1024);
        agg.feed(b"some data", false);
        let cap_before = agg.pending.capacity();
        agg.reset();
        assert_eq!(agg.pending.capacity(), cap_before);
        assert!(agg.pending.is_empty());
    }

    #[test]
    fn multiple_sequences() {
        let mut agg = CriAggregator::new(1024);

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

#[cfg(kani)]
mod verification {
    use super::*;

    /// Prove CriAggregator::feed respects max_message_size for F-only lines.
    #[kani::proof]
    fn verify_aggregator_f_only_max_size() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut agg = CriAggregator::new(max_size);
        let msg: [u8; 16] = kani::any();

        match agg.feed(&msg, true) {
            AggregateResult::Complete(out) => {
                assert!(out.len() <= max_size, "F-only output exceeds max");

                // Guard vacuity: verify constraint doesn't eliminate all paths
                kani::cover!(out.len() > 0, "non-empty output");
                kani::cover!(out.len() == max_size, "output at max size");
                kani::cover!(out.len() < max_size, "output under max size");
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove CriAggregator::feed respects max_message_size for P+F sequences.
    #[kani::proof]
    fn verify_aggregator_pf_max_size() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut agg = CriAggregator::new(max_size);

        let msg1: [u8; 8] = kani::any();
        let _ = agg.feed(&msg1, false);

        let msg2: [u8; 8] = kani::any();
        match agg.feed(&msg2, true) {
            AggregateResult::Complete(out) => {
                assert!(out.len() <= max_size, "P+F output exceeds max");

                // Guard vacuity: verify constraint allows meaningful paths
                kani::cover!(out.len() > 8, "P+F concatenation happened");
                kani::cover!(out.len() == max_size, "truncation at max size");
                kani::cover!(out.len() < max_size, "no truncation needed");
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove P lines never produce output.
    #[kani::proof]
    fn verify_aggregator_p_returns_pending() {
        let mut agg = CriAggregator::new(1024);
        let msg: [u8; 8] = kani::any();
        match agg.feed(&msg, false) {
            AggregateResult::Pending => {} // expected
            AggregateResult::Complete(_) => panic!("P line should not produce Complete"),
        }
    }

    /// Prove 3-step P+P+F respects max_message_size.
    #[kani::proof]
    fn verify_aggregator_ppf_max_size() {
        let max_size: usize = kani::any();
        kani::assume(max_size >= 1 && max_size <= 32);

        let mut agg = CriAggregator::new(max_size);

        let msg1: [u8; 8] = kani::any();
        let _ = agg.feed(&msg1, false);

        let msg2: [u8; 8] = kani::any();
        let _ = agg.feed(&msg2, false);

        let msg3: [u8; 8] = kani::any();
        match agg.feed(&msg3, true) {
            AggregateResult::Complete(out) => {
                assert!(out.len() <= max_size, "P+P+F output exceeds max");

                // Guard vacuity: verify 3-part aggregation works
                kani::cover!(out.len() > 16, "multi-partial concatenation");
                kani::cover!(out.len() == max_size, "truncation at limit");
                kani::cover!(out.len() < 24, "all parts fit");
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }

    /// Prove reset after Complete allows a clean new sequence.
    #[kani::proof]
    fn verify_aggregator_reset_clears_state() {
        let mut agg = CriAggregator::new(64);

        let msg1: [u8; 4] = kani::any();
        let _ = agg.feed(&msg1, false);
        assert!(agg.has_pending());

        agg.reset();
        assert!(!agg.has_pending());

        // After reset, a new F line works as zero-copy
        let msg2: [u8; 4] = kani::any();
        match agg.feed(&msg2, true) {
            AggregateResult::Complete(out) => {
                assert_eq!(out.len(), 4);
            }
            AggregateResult::Pending => panic!("F line should produce Complete"),
        }
    }
}
