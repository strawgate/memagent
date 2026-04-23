//! Byte-level comparison helpers for Kani proofs.

/// Bounded byte-by-byte equality assertion suitable for Kani proofs.
///
/// Unlike `assert_eq!` on slices (which may use formatting that Kani cannot
/// handle), this function compares element-by-element with bounded unwind.
pub fn assert_bytes_eq(actual: &[u8], expected: &[u8]) {
    assert_eq!(actual.len(), expected.len());
    let mut i = 0;
    while i < expected.len() {
        assert_eq!(actual[i], expected[i]);
        i += 1;
    }
}
