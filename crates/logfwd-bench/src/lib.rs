//! Shared deterministic data generators and helpers for logfwd benchmarks.
//!
//! Generators are deterministic and index-based (no seed parameter). Given the
//! same `count`, every call returns byte-identical results.
#![allow(clippy::print_stdout, clippy::print_stderr)]
// Bench harnesses print reports to stdout/stderr.

use std::io::Write;
use std::sync::Arc;

use logfwd_output::{BatchMetadata, Compression, OtlpProtocol, OtlpSink};
use logfwd_types::diagnostics::ComponentStats;

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

// ---------------------------------------------------------------------------
// Inline JSON-line generators (non-seeded, index-based)
// ---------------------------------------------------------------------------

/// Generate `n` JSON log lines with 6 fields (timestamp, level, message,
/// duration_ms, request_id, service).
pub fn generate_simple(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 180);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];
    for i in 0..n {
        write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{:016x}","service":"myapp"}}"#,
            i % 1000,
            levels[i % 4],
            paths[i % 5],
            10000 + (i * 7) % 90000,
            1 + (i * 13) % 500,
            (i as u64).wrapping_mul(0x517cc1b727220a95),
        ).unwrap();
        buf.push(b'\n');
    }
    buf
}

/// Generate `n` JSON log lines with 20 fields (wide schema).
pub fn generate_wide(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 600);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
    let namespaces = ["default", "kube-system", "monitoring", "logging"];
    for i in 0..n {
        write!(
            buf,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request {}","duration_ms":{},"service":"myapp","host":"node-{}","pod":"app-{:04}","namespace":"{}","method":"{}","status_code":{},"region":"{}","user_id":"user-{}","trace_id":"{:032x}","response_bytes":{},"latency_p99_ms":{},"error_count":{},"cache_hit":{},"db_query_ms":{},"upstream":"svc-{}","version":"v{}.{}"}}"#,
            i % 1000, levels[i % 4], i, 1 + (i * 13) % 500,
            i % 10, i % 100, namespaces[i % 4], methods[i % 5],
            [200, 201, 400, 404, 500][i % 5], regions[i % 4],
            i % 1000, (i as u64).wrapping_mul(0x517cc1b727220a95),
            100 + (i * 37) % 10000, 10 + (i * 11) % 1000,
            i32::from(i % 20 == 0),
            if i % 3 == 0 { "true" } else { "false" },
            (i * 7) % 200, i % 4, 1 + i % 5, i % 10,
        ).unwrap();
        buf.push(b'\n');
    }
    buf
}

/// Generate `n` JSON log lines with 3 fields (narrow schema: ts, lvl, msg).
pub fn generate_narrow(n: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(n * 60);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    for i in 0..n {
        write!(
            buf,
            r#"{{"ts":"{}","lvl":"{}","msg":"event {}"}}"#,
            i,
            levels[i % 4],
            i
        )
        .unwrap();
        buf.push(b'\n');
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::{generate_narrow, generate_simple, generate_wide};

    #[test]
    fn simple_is_deterministic() {
        assert_eq!(generate_simple(100), generate_simple(100));
    }

    #[test]
    fn simple_line_count_matches_n() {
        let out = generate_simple(50);
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 50);
    }

    #[test]
    fn wide_is_deterministic() {
        assert_eq!(generate_wide(100), generate_wide(100));
    }

    #[test]
    fn wide_line_count_matches_n() {
        let out = generate_wide(50);
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 50);
    }

    #[test]
    fn narrow_is_deterministic() {
        assert_eq!(generate_narrow(100), generate_narrow(100));
    }

    #[test]
    fn narrow_line_count_matches_n() {
        let out = generate_narrow(50);
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 50);
    }
}
