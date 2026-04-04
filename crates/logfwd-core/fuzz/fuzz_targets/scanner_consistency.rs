//! Cross-builder consistency fuzz target.
//!
//! Runs the same input through both `CopyScanner` (StorageBuilder) and
//! `ZeroCopyScanner` (StreamingBuilder) and asserts that:
//! - Row counts match.
//! - The same set of column names is produced.
//! - For every column, the null pattern matches.
//! - For string columns, non-null values are identical (StorageBuilder copies
//!   into `StringArray`; StreamingBuilder stores zero-copy `StringViewArray`
//!   views — the string content must be the same).

#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_core::scan_config::ScanConfig;
use logfwd_arrow::scanner::{CopyScanner, ZeroCopyScanner};

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use std::collections::BTreeSet;

fuzz_target!(|data: &[u8]| {
    let mut storage_scanner = CopyScanner::new(ScanConfig::default());
    let Ok(storage_batch) = storage_scanner.scan(data) else { return; };

    let mut streaming_scanner = ZeroCopyScanner::new(ScanConfig::default());
    let Ok(streaming_batch) = streaming_scanner.scan(bytes::Bytes::copy_from_slice(data)) else { return; };

    // Row counts must match.
    assert_eq!(
        storage_batch.num_rows(),
        streaming_batch.num_rows(),
        "row count mismatch"
    );

    let num_rows = storage_batch.num_rows();

    // Column name sets must match.
    let storage_schema = storage_batch.schema();
    let streaming_schema = streaming_batch.schema();
    let storage_names: BTreeSet<&str> = storage_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let streaming_names: BTreeSet<&str> = streaming_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(storage_names, streaming_names, "column name sets differ");

    // For each column, null patterns and values must match.
    for name in &storage_names {
        let sc = storage_batch.column_by_name(name).unwrap();
        let st = streaming_batch.column_by_name(name).unwrap();

        for row in 0..num_rows {
            assert_eq!(
                sc.is_null(row),
                st.is_null(row),
                "null mismatch at row {row} col {name}"
            );
        }

        // Compare string values: StorageBuilder → Utf8, StreamingBuilder → Utf8View.
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
            // Int and float columns have the same Arrow type in both builders.
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
                        // NaN != NaN, but both builders should produce the same
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
});
