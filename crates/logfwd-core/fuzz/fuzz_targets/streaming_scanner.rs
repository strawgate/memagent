//! Fuzz the ZeroCopyScanner (StreamingBuilder / zero-copy path) with
//! arbitrary bytes.
//!
//! Covers:
//! - Extract-all mode (all keys extracted, no pushdown)
//! - Field pushdown mode (only requested fields extracted)
//!
//! Verifies that each produced `RecordBatch` is internally consistent: every
//! column has exactly `num_rows` entries and no panic occurs.

#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_core::scan_config::ScanConfig;
use logfwd_arrow::scanner::ZeroCopyScanner;

fn validate_batch(batch: &arrow::record_batch::RecordBatch, label: &str) {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    for col_idx in 0..batch.num_columns() {
        assert_eq!(
            batch.column(col_idx).len(),
            num_rows,
            "{label}: column '{}' length {} != num_rows {num_rows}",
            schema.field(col_idx).name(),
            batch.column(col_idx).len(),
        );
    }
}

fuzz_target!(|data: &[u8]| {
    // Extract-all mode.
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: false,
        validate_utf8: false,
    };
    let mut scanner = ZeroCopyScanner::new(config);
    let Ok(batch) = scanner.scan(bytes::Bytes::copy_from_slice(data)) else { return; };
    validate_batch(&batch, "streaming_extract_all");

    // Field pushdown mode.
    let config2 = ScanConfig {
        wanted_fields: vec![
            logfwd_core::scan_config::FieldSpec {
                name: "level".to_string(),
                aliases: vec![],
            },
            logfwd_core::scan_config::FieldSpec {
                name: "msg".to_string(),
                aliases: vec![],
            },
        ],
        extract_all: false,
        keep_raw: false,
        validate_utf8: false,
    };
    let mut scanner2 = ZeroCopyScanner::new(config2);
    let Ok(batch2) = scanner2.scan(bytes::Bytes::copy_from_slice(data)) else { return; };
    validate_batch(&batch2, "streaming_pushdown");
});
