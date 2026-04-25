//! Cross-mode consistency fuzz target.
//!
//! Runs the same input through both `scan_detached` (StringArray) and
//! `scan` (StringViewArray) and asserts that:
//! - Row counts match.
//! - The same set of column names is produced.
//! - For every column, the null pattern matches.
//! - For string columns, non-null values are identical (scan_detached copies
//!   into `StringArray`; scan stores zero-copy `StringViewArray`
//!   views — the string content must be the same).

#![no_main]
use libfuzzer_sys::fuzz_target;
use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use std::collections::BTreeSet;

fn check_consistency(data: &[u8], validate_utf8: bool) {
    let config = ScanConfig {
        validate_utf8,
        ..ScanConfig::default()
    };
    let buf = bytes::Bytes::copy_from_slice(data);

    let mut detached_scanner = Scanner::new(config);
    let Ok(detached_batch) = detached_scanner.scan_detached(buf.clone()) else { return; };

    let mut streaming_scanner = Scanner::new(ScanConfig {
        validate_utf8,
        ..ScanConfig::default()
    });
    let Ok(streaming_batch) = streaming_scanner.scan(buf) else { return; };

    // Row counts must match.
    assert_eq!(
        detached_batch.num_rows(),
        streaming_batch.num_rows(),
        "row count mismatch"
    );

    let num_rows = detached_batch.num_rows();

    // Column name sets must match.
    let detached_schema = detached_batch.schema();
    let streaming_schema = streaming_batch.schema();
    let detached_names: BTreeSet<&str> = detached_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let streaming_names: BTreeSet<&str> = streaming_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(detached_names, streaming_names, "column name sets differ");

    // For each column, null patterns and values must match.
    for name in &detached_names {
        let sc = detached_batch.column_by_name(name).unwrap();
        let st = streaming_batch.column_by_name(name).unwrap();

        for row in 0..num_rows {
            assert_eq!(
                sc.is_null(row),
                st.is_null(row),
                "null mismatch at row {row} col {name}"
            );
        }

        // Compare string values: scan_detached -> Utf8, scan -> Utf8View.
        let sc_type = sc.data_type();
        let st_type = st.data_type();
        match (sc_type, st_type) {
            (DataType::Utf8, DataType::Utf8View) => {
                let sa = sc.as_string::<i32>();
                let sv = st.as_string_view();
                for row in 0..num_rows {
                    if !sa.is_null(row) {
                        assert_eq!(
                            sa.value(row),
                            sv.value(row),
                            "string value mismatch at row {row} col {name}"
                        );
                    }
                }
            }
            // Int and float columns have the same Arrow type in both modes.
            (DataType::Int64, DataType::Int64) => {
                let sa = sc.as_primitive::<arrow::datatypes::Int64Type>();
                let sv = st.as_primitive::<arrow::datatypes::Int64Type>();
                for row in 0..num_rows {
                    if !sa.is_null(row) {
                        assert_eq!(
                            sa.value(row),
                            sv.value(row),
                            "int value mismatch at row {row} col {name}"
                        );
                    }
                }
            }
            (DataType::Float64, DataType::Float64) => {
                let sa = sc.as_primitive::<arrow::datatypes::Float64Type>();
                let sv = st.as_primitive::<arrow::datatypes::Float64Type>();
                for row in 0..num_rows {
                    if !sa.is_null(row) {
                        // NaN != NaN, but both modes should produce the same
                        // bit pattern for any given input so this is safe.
                        assert_eq!(
                            sa.value(row).to_bits(),
                            sv.value(row).to_bits(),
                            "float value mismatch at row {row} col {name}"
                        );
                    }
                }
            }
            _ => {
                // Unexpected type combination — just verify nullability already
                // checked above.
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    check_consistency(data, false);
    check_consistency(data, true);
});
