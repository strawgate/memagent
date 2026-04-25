//! Shared deterministic data generators and helpers for ffwd benchmarks.
//!
//! All generators accept a `seed` parameter for reproducible output. Given the
//! same `(count, seed)` pair, every call returns byte-identical results.
#![allow(clippy::print_stdout, clippy::print_stderr)]
// Bench harnesses print reports to stdout/stderr.

use std::sync::Arc;

use ffwd_output::{BatchMetadata, Compression, OtlpProtocol, OtlpSink};
use ffwd_types::diagnostics::ComponentStats;

pub mod cardinality;
pub mod generators;

// ---------------------------------------------------------------------------
// Shared benchmark helpers
// ---------------------------------------------------------------------------

/// Null sink that discards all data — measures pure iteration overhead.
pub struct NullSink;

impl NullSink {
    /// Discard a batch. Signature matches the hot-path call sites in benchmarks.
    pub fn send_batch(
        &mut self,
        _batch: &arrow::record_batch::RecordBatch,
        _metadata: &BatchMetadata,
    ) -> std::io::Result<()> {
        Ok(())
    }

    /// No-op flush.
    pub fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    /// Sink name for diagnostics.
    pub fn name(&self) -> &'static str {
        "null"
    }
}

/// Create an `OtlpSink` for benchmarking (buffer-only, no HTTP).
pub fn make_otlp_sink(compression: Compression) -> OtlpSink {
    OtlpSink::new(
        "bench".into(),
        "http://localhost:1".into(), // unreachable — benchmarks only call encode_batch
        OtlpProtocol::Http,
        compression,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    )
    .expect("valid otlp sink for bench")
}
