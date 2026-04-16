//! Benchmark infrastructure helpers for logfwd.
//!
//! Provides `NullSink`, `make_otlp_sink`, and `make_metadata` for
//! criterion benchmarks and profiling binaries.

use std::sync::Arc;

use logfwd_output::{BatchMetadata, Compression, OtlpProtocol, OtlpSink};
use logfwd_types::diagnostics::ComponentStats;

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

/// Create benchmark-standard `BatchMetadata` with typical K8s resource attributes.
pub fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::new(vec![
            ("service.name".into(), "bench-service".into()),
            ("service.version".into(), "1.0.0".into()),
            ("host.name".into(), "bench-node-01".into()),
        ]),
        observed_time_ns: 1_705_312_200_000_000_000, // 2024-01-15T10:30:00Z
    }
}
