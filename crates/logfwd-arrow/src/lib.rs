//! Arrow integration layer for logfwd.
//!
//! Implements logfwd-core's `ScanBuilder` trait using Apache Arrow types.
//! Contains `StreamingBuilder` (zero-copy hot path) and `StorageBuilder`
//! (persistence path), plus scanner wrapper types that produce `RecordBatch`.

pub mod scanner;
pub mod storage_builder;
pub mod streaming_builder;

// Re-export scanner types for convenience
pub use scanner::{SimdScanner, StreamingSimdScanner};
pub use storage_builder::StorageBuilder;
pub use streaming_builder::StreamingBuilder;
