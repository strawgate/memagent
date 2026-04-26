#![deny(clippy::indexing_slicing)]

//! Arrow integration layer for ffwd.
//!
//! Implements ffwd-core's `ScanBuilder` trait using Apache Arrow types.
//! Contains `StreamingBuilder` (zero-copy hot path) and the `Scanner`
//! wrapper type that produces `RecordBatch`.

/// Columnar batch builder for structured producers (OTLP, CSV).
pub mod columnar;
pub mod conflict_schema;
pub mod materialize;
pub mod scanner;
pub mod star_schema;
pub mod streaming_builder;

/// Check and set a per-row written bit for the given field index.
///
/// Returns `true` if the field has already been written in this row
/// (duplicate key — caller should skip the write), `false` otherwise.
///
/// # Limitation
/// Only the first 64 fields (indices 0–63) are tracked. For `idx >= 64` this
/// function always returns `false`, meaning **duplicate-key detection is
/// disabled** for those fields. The first value written wins by convention;
/// subsequent writes for the same key in the same row are silently accepted
/// rather than deduplicated. This matches RFC 8259's "undefined" semantics for
/// duplicate keys and keeps the hot path allocation-free.
#[inline(always)]
pub(crate) fn check_dup_bits(written_bits: &mut u64, idx: usize) -> bool {
    if idx >= u64::BITS as usize {
        // Duplicate-key detection is not tracked beyond the first 64 fields.
        // Writes for idx >= 64 are always allowed through.
        return false;
    }
    let bit = 1u64 << idx;
    if *written_bits & bit != 0 {
        return true;
    }
    *written_bits |= bit;
    false
}

// Re-export scanner types for convenience
pub use scanner::Scanner;
pub use streaming_builder::StreamingBuilder;

#[cfg(kani)]
mod verification {
    use super::*;

    /// Exhaustive proof: first call for a tracked index clears, second detects duplicate.
    /// Covers both the tracked (idx < 64) and untracked (idx >= 64) branches.
    #[kani::proof]
    fn verify_check_dup_bits_first_write_then_duplicate() {
        let idx: usize = kani::any();
        kani::assume(idx < 128);
        let mut bits: u64 = 0;

        let first = check_dup_bits(&mut bits, idx);
        let second = check_dup_bits(&mut bits, idx);

        if idx < 64 {
            // Oracle: independently compute expected return and state.
            let expected_bit = 1u64 << idx;
            assert!(!first, "first write must not be flagged as duplicate");
            assert!(second, "second write must be flagged as duplicate");
            assert_eq!(bits, expected_bit, "exactly one bit must be set");
        } else {
            assert!(!first, "idx >= 64 always returns false");
            assert!(!second, "idx >= 64 always returns false");
            assert_eq!(bits, 0, "no bits modified for idx >= 64");
        }

        kani::cover!(idx < 64, "tracked field");
        kani::cover!(idx >= 64, "untracked field");
        kani::cover!(idx == 0, "first index");
        kani::cover!(idx == 63, "last tracked index");
        kani::cover!(idx == 64, "first untracked index");
    }

    /// Two distinct tracked indices never interfere with each other.
    /// Oracle: verifies that exactly the two target bits are set afterwards.
    #[kani::proof]
    fn verify_check_dup_bits_independence() {
        let a: usize = kani::any();
        let b: usize = kani::any();
        kani::assume(a < 64 && b < 64 && a != b);

        let mut bits: u64 = 0;
        let first_a = check_dup_bits(&mut bits, a);
        let first_b = check_dup_bits(&mut bits, b);
        let dup_a = check_dup_bits(&mut bits, a);
        let dup_b = check_dup_bits(&mut bits, b);

        assert!(!first_a, "a first write is clean");
        assert!(!first_b, "b first write is clean");
        assert!(dup_a, "a second write is duplicate");
        assert!(dup_b, "b second write is duplicate");

        // Oracle: exactly two bits set, at positions a and b.
        let expected = (1u64 << a) | (1u64 << b);
        assert_eq!(bits, expected, "exactly two bits must be set");

        kani::cover!(a == 0 && b == 63, "boundary indices");
        kani::cover!(a < 32 && b >= 32, "cross-halfword pair");
    }

    /// Pre-existing bits are preserved — setting idx doesn't clear other bits.
    /// Oracle: the result is exactly `before | (1 << idx)`.
    #[kani::proof]
    fn verify_check_dup_bits_preserves_existing() {
        let idx: usize = kani::any();
        kani::assume(idx < 64);
        let mut bits: u64 = kani::any();
        let before = bits;

        let was_dup = check_dup_bits(&mut bits, idx);

        // Oracle: independently compute expected state and return value.
        let expected_bits = before | (1u64 << idx);
        let expected_dup = (before & (1u64 << idx)) != 0;
        assert_eq!(bits, expected_bits, "bits must be before | target");
        assert_eq!(was_dup, expected_dup, "return must reflect prior state");

        kani::cover!(before != 0, "non-zero existing bits");
        kani::cover!((before & (1u64 << idx)) != 0, "target bit already set");
        kani::cover!((before & (1u64 << idx)) == 0, "target bit initially clear");
    }

    /// Boundary proof: idx=63 is the last tracked index, idx=64 is the first
    /// untracked. Third call with an already-duplicate returns true again.
    #[kani::proof]
    fn verify_check_dup_bits_boundary_at_64() {
        let mut bits: u64 = 0;

        // idx=63 is tracked: first clean, second dup, third still dup.
        let first = check_dup_bits(&mut bits, 63);
        let second = check_dup_bits(&mut bits, 63);
        let third = check_dup_bits(&mut bits, 63);
        assert!(!first, "63 first write clean");
        assert!(second, "63 second write is dup");
        assert!(third, "63 third write still dup (idempotent)");

        // idx=64 is untracked: never modifies bits, never returns true.
        let bits_before = bits;
        let call_64 = check_dup_bits(&mut bits, 64);
        assert!(!call_64, "idx=64 always false");
        assert_eq!(bits, bits_before, "bits unchanged for idx=64");

        // Large index: same behaviour.
        let call_big = check_dup_bits(&mut bits, 10_000);
        assert!(!call_big, "idx=10000 always false");
        assert_eq!(bits, bits_before, "bits unchanged for large idx");
    }
}
