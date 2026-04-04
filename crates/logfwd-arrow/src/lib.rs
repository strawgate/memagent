//! Arrow integration layer for logfwd.
//!
//! Implements logfwd-core's `ScanBuilder` trait using Apache Arrow types.
//! Contains `StreamingBuilder` (zero-copy hot path) and `StorageBuilder`
//! (persistence path), plus scanner wrapper types that produce `RecordBatch`.

pub mod conflict_schema;
pub mod scanner;
pub mod star_schema;
pub mod storage_builder;
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
pub use scanner::{SimdScanner, StreamingSimdScanner};
pub use storage_builder::StorageBuilder;
pub use streaming_builder::StreamingBuilder;
