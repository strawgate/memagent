//! Shared test utilities for the ffwd workspace.
//!
//! Provides reusable proptest strategies for JSON generation, test sink
//! implementations, and common helpers used across multiple test suites.

pub mod arrow;
pub mod json;
pub mod sinks;

pub use sinks::CountingSink;

use std::io::Write as _;
use std::path::Path;

/// Return the number of proptest cases to run.
///
/// Reads from `PROPTEST_CASES` env var (for CI scaling).
/// Default: 256 (fast enough for per-PR CI, thorough enough to catch most bugs).
/// Nightly CI sets 10,000 for deeper exploration.
pub fn proptest_cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256)
}

/// Return the number of proptest cases for expensive state-machine tests.
///
/// These tests exercise real file IO, fsyncs, restarts, and replay logic, so
/// the generic `PROPTEST_CASES` default of 256 is too slow for normal PR runs.
///
/// Override with `FFWD_PROPTEST_STATE_MACHINE_CASES` when you want deeper coverage.
/// If that env var is unset, we cap the general proptest budget at 16 cases.
pub fn state_machine_proptest_cases() -> u32 {
    std::env::var("FFWD_PROPTEST_STATE_MACHINE_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| proptest_cases().min(16))
}

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
        use std::fmt::Write;
        write!(
            data,
            r#"{{"sequence_id":{i},"source_id":"{source_id}","level":"{level}","message":"test line {i}","generated_at":"2024-01-01T00:00:00Z"}}"#,
        )
        .unwrap();
        data.push('\n');
    }
    std::fs::write(path, data.as_bytes()).expect("failed to write test data");
}

/// Append `count` NDJSON lines to an existing file.
/// Sequence IDs continue from the current line count. Used by resume tests to simulate
/// new data appearing after the initial batch.
pub fn append_json_lines(path: &Path, count: usize, source_id: &str) {
    // Determine starting sequence from current line count in the file.
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let start_seq = existing.lines().count();

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("failed to open file for appending");

    for i in start_seq..start_seq + count {
        let level = if i % 2 == 0 { "INFO" } else { "ERROR" };
        writeln!(
            file,
            r#"{{"sequence_id":{i},"source_id":"{source_id}","level":"{level}","message":"test line {i}","generated_at":"2024-01-01T00:00:00Z"}}"#,
        )
        .expect("failed to append test data");
    }
}
