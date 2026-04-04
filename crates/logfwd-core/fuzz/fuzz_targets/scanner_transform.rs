//! Fuzz the scanner → DataFusion `SqlTransform` integration.
//!
//! Feeds arbitrary bytes through `CopyScanner` and then executes a SQL query
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
use logfwd_core::scan_config::ScanConfig;
use logfwd_arrow::scanner::CopyScanner;
use logfwd_transform::SqlTransform;

fuzz_target!(|data: &[u8]| {
    let mut scanner = CopyScanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: false,
        validate_utf8: false,
    });
    let Ok(batch) = scanner.scan(data) else { return; };

    // SELECT * passes every column through DataFusion unchanged.
    if let Ok(mut transform) = SqlTransform::new("SELECT * FROM logs") {
        // Errors are fine; panics are not.
        let _ = transform.execute(batch);
    }
});
