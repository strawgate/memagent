//! Shared deterministic data generators and helpers for logfwd benchmarks.
//!
//! All generators accept a `seed` parameter for reproducible output. Given the
//! same `(count, seed)` pair, every call returns byte-identical results.

#![allow(deprecated)] // Benchmarks use sync OutputSink; migration tracked separately.

use std::sync::Arc;

use logfwd_output::{BatchMetadata, Compression, OtlpProtocol, OtlpSink, OutputSink};
use logfwd_types::diagnostics::ComponentStats;

pub mod generators;

// ---------------------------------------------------------------------------
// Shared benchmark helpers
// ---------------------------------------------------------------------------

/// Null sink that discards all data — measures pure iteration overhead.
pub struct NullSink;

impl OutputSink for NullSink {
    fn send_batch(
        &mut self,
        _batch: &arrow::record_batch::RecordBatch,
        _metadata: &BatchMetadata,
    ) -> std::io::Result<()> {
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
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
}
