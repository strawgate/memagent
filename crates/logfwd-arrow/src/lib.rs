//! Arrow integration layer for logfwd.
//!
//! Implements logfwd-core's `ScanBuilder` trait using Apache Arrow types.
//! Contains `StreamingBuilder` (zero-copy hot path) and `StorageBuilder`
//! (persistence path), plus scanner wrapper types that produce `RecordBatch`.

pub mod scanner;
pub mod storage_builder;
pub mod streaming_builder;

pub(crate) const MAX_INLINE_BITS: usize = u64::BITS as usize;

#[inline(always)]
pub(crate) fn check_dup_bits(
    written_bits: &mut u64,
    overflow_bits: &mut Vec<u64>,
    idx: usize,
) -> bool {
    if idx < MAX_INLINE_BITS {
        let bit = 1u64 << idx;
        if *written_bits & bit != 0 {
            return true;
        }
        *written_bits |= bit;
        return false;
    }

    let word = (idx / MAX_INLINE_BITS) - 1;
    if overflow_bits.len() <= word {
        overflow_bits.resize(word + 1, 0);
    }
    let bit = 1u64 << (idx % MAX_INLINE_BITS);
    if overflow_bits[word] & bit != 0 {
        return true;
    }
    overflow_bits[word] |= bit;
    false
}

// Re-export scanner types for convenience
pub use scanner::{SimdScanner, StreamingSimdScanner};
pub use storage_builder::StorageBuilder;
pub use streaming_builder::StreamingBuilder;
