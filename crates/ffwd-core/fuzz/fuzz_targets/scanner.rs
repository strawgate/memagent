//! Fuzz the JSON scanner with arbitrary bytes.

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
    // Extract-all mode.
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: false,
    };
    let mut scanner = Scanner::new(config);
    let Ok(batch) = scanner.scan_detached(bytes::Bytes::copy_from_slice(data)) else {
        return;
    };
    validate_batch(&batch, "extract_all");

    // Extract-all mode with line capture name collision against a parsed field.
    let config_collision = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("level".to_string()),
        validate_utf8: false,
    };
    let mut scanner_collision = Scanner::new(config_collision);
    if let Ok(batch_collision) = scanner_collision.scan_detached(bytes::Bytes::copy_from_slice(data))
    {
        validate_batch(&batch_collision, "extract_all_line_collision");
    }

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
    };
    let mut scanner2 = Scanner::new(config2);
    let Ok(batch2) = scanner2.scan_detached(bytes::Bytes::copy_from_slice(data)) else {
        return;
    };
    validate_batch(&batch2, "pushdown");

    // --- validate_utf8: true variants ---
    // When the input is not valid UTF-8, the scanner returns Err immediately.
    // When the fuzzer generates valid UTF-8, the full pipeline runs with
    // validation enabled, exercising the UTF-8 check code path.

    // Extract-all mode with UTF-8 validation.
    let config_v = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: true,
    };
    let mut scanner_v = Scanner::new(config_v);
    if let Ok(batch_v) = scanner_v.scan_detached(bytes::Bytes::copy_from_slice(data)) {
        validate_batch(&batch_v, "extract_all_validate_utf8");
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
    };
    let mut scanner2_v = Scanner::new(config2_v);
    if let Ok(batch2_v) = scanner2_v.scan_detached(bytes::Bytes::copy_from_slice(data)) {
        validate_batch(&batch2_v, "pushdown_validate_utf8");
    }
});
