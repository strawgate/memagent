//! Fuzz the scanner → DataFusion `SqlTransform` integration.
//!
//! Feeds arbitrary bytes through `Scanner` and then executes a SQL query
//! on the resulting `RecordBatch` via `SqlTransform`.
//!
//! The goal is to verify that no combination of scanner output can cause a
//! panic inside DataFusion. Errors (returned as `Err(String)`) are expected
//! and acceptable — only panics are failures.
//!
//! Note: `SqlTransform::execute` creates a single-threaded tokio runtime
//! internally, making this target slower than the pure scanner targets.
//! Run it with a reduced corpus or a time limit, e.g.:
//!
//! ```
//! cargo +nightly fuzz run fuzz_scanner_transform -- -max_total_time=300
//! ```

#![no_main]
use libfuzzer_sys::fuzz_target;
use ffwd_core::scan_config::ScanConfig;
use ffwd_arrow::scanner::Scanner;
use ffwd_transform::SqlTransform;

fn run_transform(data: &[u8], validate_utf8: bool) {
    let mut scanner = Scanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8,
    });
    let Ok(batch) = scanner.scan_detached(bytes::Bytes::copy_from_slice(data)) else { return; };

    // SELECT * passes every column through DataFusion unchanged.
    if let Ok(mut transform) = SqlTransform::new("SELECT * FROM logs") {
        // Errors are fine; panics are not.
        let _ = transform.execute(batch);
    }
}

fuzz_target!(|data: &[u8]| {
    run_transform(data, false);
    run_transform(data, true);
});
