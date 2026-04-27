//! Fuzz the Scanner (StreamingBuilder / zero-copy path) with
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
use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;

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
    // Extract-all mode with line capture enabled.
    let config_line = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: false,
        row_predicate: None,
    };
    let mut scanner_line = Scanner::new(config_line);
    if let Ok(batch_line) = scanner_line.scan(bytes::Bytes::copy_from_slice(data)) {
        validate_batch(&batch_line, "streaming_extract_all_line_capture");
        assert!(
            batch_line.num_rows() == 0 || batch_line.schema().column_with_name("body").is_some(),
            "streaming_extract_all_line_capture: missing 'body' column when line capture is enabled",
        );
    }

    // Extract-all mode.
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
        row_predicate: None,
    };
    let mut scanner = Scanner::new(config);
    let Ok(batch) = scanner.scan(bytes::Bytes::copy_from_slice(data)) else { return; };
    validate_batch(&batch, "streaming_extract_all");

    // Field pushdown mode.
    let config2 = ScanConfig {
        wanted_fields: vec![
            ffwd_core::scan_config::FieldSpec {
                name: "level".to_string(),
                aliases: vec![],
            },
            ffwd_core::scan_config::FieldSpec {
                name: "msg".to_string(),
                aliases: vec![],
            },
        ],
        extract_all: false,
        line_field_name: None,
        validate_utf8: false,
        row_predicate: None,
    };
    let mut scanner2 = Scanner::new(config2);
    let Ok(batch2) = scanner2.scan(bytes::Bytes::copy_from_slice(data)) else { return; };
    validate_batch(&batch2, "streaming_pushdown");

    // --- validate_utf8: true variants ---

    // Extract-all mode with UTF-8 validation + line capture.
    let config_line_v = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: true,
        row_predicate: None,
    };
    let mut scanner_line_v = Scanner::new(config_line_v);
    if let Ok(batch_line_v) = scanner_line_v.scan(bytes::Bytes::copy_from_slice(data)) {
        validate_batch(&batch_line_v, "streaming_extract_all_line_capture_validate_utf8");
    }

    // Extract-all mode with UTF-8 validation.
    let config_v = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: true,
        row_predicate: None,
    };
    let mut scanner_v = Scanner::new(config_v);
    if let Ok(batch_v) = scanner_v.scan(bytes::Bytes::copy_from_slice(data)) {
        validate_batch(&batch_v, "streaming_extract_all_validate_utf8");
    }

    // Field pushdown mode with UTF-8 validation.
    let config2_v = ScanConfig {
        wanted_fields: vec![
            ffwd_core::scan_config::FieldSpec {
                name: "level".to_string(),
                aliases: vec![],
            },
            ffwd_core::scan_config::FieldSpec {
                name: "msg".to_string(),
                aliases: vec![],
            },
        ],
        extract_all: false,
        line_field_name: None,
        validate_utf8: true,
        row_predicate: None,
    };
    let mut scanner2_v = Scanner::new(config2_v);
    if let Ok(batch2_v) = scanner2_v.scan(bytes::Bytes::copy_from_slice(data)) {
        validate_batch(&batch2_v, "streaming_pushdown_validate_utf8");
    }
});
