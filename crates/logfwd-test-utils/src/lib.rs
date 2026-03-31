//! Shared test utilities for the logfwd workspace.
//!
//! Provides reusable proptest strategies for JSON generation, test sink
//! implementations, and common helpers used across multiple test suites.

pub mod json;
pub mod sinks;

use std::path::Path;

/// Return a no-op OpenTelemetry Meter for tests.
pub fn test_meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("test")
}

/// Write `count` NDJSON lines to `path`, each with a monotonically increasing
/// `sequence_id` (starting from 0) and the given `source_id`.
pub fn generate_json_lines(path: &Path, count: usize, source_id: &str) {
    let mut data = String::with_capacity(count * 120);
    for i in 0..count {
        let level = if i % 2 == 0 { "INFO" } else { "ERROR" };
        data.push_str(&format!(
            r#"{{"sequence_id":{i},"source_id":"{source_id}","level":"{level}","message":"test line {i}","generated_at":"2024-01-01T00:00:00Z"}}"#,
        ));
        data.push('\n');
    }
    std::fs::write(path, data.as_bytes()).expect("failed to write test data");
}
