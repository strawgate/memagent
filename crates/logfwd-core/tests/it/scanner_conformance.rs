#![allow(clippy::collapsible_if)]
//! Scanner conformance test suite.
//!
//! Proves the scanner produces identical Arrow output to the scalar scanner
//! across thousands of generated inputs plus curated edge cases.
//!
//! Run:
//!   cargo test -p logfwd-core --test scanner_conformance
//!
//! Scale up (10K+ cases):
//!   PROPTEST_CASES=10000 cargo test -p logfwd-core --test scanner_conformance
//!
//! Find minimal failing case:
//!   cargo test -p logfwd-core --test scanner_conformance -- --nocapture

use arrow::array::{Array, Float64Array, Int64Array, StringArray, StructArray};
use bytes::Bytes;
use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_test_utils::json::{arb_flat_object, arb_json_string, arb_ndjson_buffer};
use proptest::prelude::*;

// ===========================================================================
// Core: ground-truth oracle (sonic-rs)
// ===========================================================================

/// Return a child column from a conflict StructArray column.
/// `field` is the bare column name (e.g. `"status"`), `child` is `"int"`, `"str"`, or `"float"`.
fn get_struct_child<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    field: &str,
    child: &str,
) -> Option<&'a dyn Array> {
    let col = batch.column_by_name(field)?;
    let sa = col.as_any().downcast_ref::<StructArray>()?;
    let idx = sa.fields().iter().position(|f| f.name() == child)?;
    Some(sa.column(idx).as_ref())
}

/// Verify the scanner produces correct values by comparing against
/// sonic-rs (a known-correct JSON parser) as the ground-truth oracle.
fn assert_values_correct(input: &[u8]) {
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};

    let mut simd = Scanner::new(ScanConfig::default());
    let batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should succeed");

    // Parse each line with sonic-rs and verify the Arrow output matches
    let mut row = 0;
    for line in input.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let s = std::str::from_utf8(line).unwrap_or("");

        // Skip lines with duplicate keys — our scanner does first-writer-wins,
        // sonic-rs does last-writer-wins. Both are valid per RFC 8259.
        // Skip lines with duplicate keys — our scanner does first-writer-wins,
        // sonic-rs does last-writer-wins. Both are valid per RFC 8259.
        // Use to_object_iter which yields ALL keys including duplicates.
        {
            let mut seen = std::collections::HashSet::new();
            let mut has_dup = false;
            let iter = unsafe { sonic_rs::to_object_iter_unchecked(s) };
            for (key, _) in iter.flatten() {
                if !seen.insert(key.to_string()) {
                    has_dup = true;
                    break;
                }
            }
            if has_dup {
                row += 1;
                continue;
            }
        }

        let parsed: Result<sonic_rs::Value, _> = sonic_rs::from_str(s);
        let obj = match parsed {
            Ok(ref v) => match v.as_object() {
                Some(o) => o,
                None => {
                    row += 1;
                    continue;
                }
            },
            Err(_) => {
                row += 1;
                continue;
            }
        };

        assert!(
            row < batch.num_rows(),
            "Row {row} exceeds batch rows {}.\nInput: {:?}",
            batch.num_rows(),
            String::from_utf8_lossy(input)
        );

        // Check each field from the sonic-rs parse
        let mut seen_keys = std::collections::HashSet::new();
        for (key_str, val) in obj {
            // Skip duplicate keys (first-writer-wins in our scanner)
            if !seen_keys.insert(key_str.to_string()) {
                continue;
            }

            if val.is_str() {
                let expected = val.as_str().unwrap();
                // With struct-on-conflict: single-type fields use bare name,
                // conflict fields use a StructArray with a "str" child.
                // Try the struct child first, then fall back to the bare column.
                let col_owned;
                let col: &dyn Array = if let Some(child) = get_struct_child(&batch, key_str, "str")
                {
                    child
                } else {
                    col_owned = batch
                        .column_by_name(key_str)
                        .unwrap_or_else(|| {
                            panic!(
                                "Scanner produced no column for string key '{key_str}' at row {row}.\nInput: {:?}",
                                String::from_utf8_lossy(line)
                            )
                        });
                    col_owned.as_ref()
                };
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap_or_else(|| {
                    panic!(
                        "Expected StringArray for key '{key_str}' at row {row} but got {:?}.\nInput: {:?}",
                        col.data_type(),
                        String::from_utf8_lossy(line)
                    )
                });
                if !arr.is_null(row) {
                    let actual = arr.value(row);
                    // After #410: scanner decodes JSON escape sequences,
                    // so values match sonic-rs output for all strings.
                    assert_eq!(
                        actual,
                        expected,
                        "String value mismatch at '{key_str}'[{row}].\nExpected: {expected:?}\nActual: {actual:?}\nInput: {:?}",
                        String::from_utf8_lossy(line)
                    );
                }
            } else if val.is_i64() {
                let expected = val.as_i64().unwrap();
                let col_owned;
                let col: &dyn Array = if let Some(child) = get_struct_child(&batch, key_str, "int")
                {
                    child
                } else {
                    col_owned = batch
                        .column_by_name(key_str)
                        .unwrap_or_else(|| {
                            panic!(
                                "Scanner produced no column for int key '{key_str}' at row {row}.\nInput: {:?}",
                                String::from_utf8_lossy(line)
                            )
                        });
                    col_owned.as_ref()
                };
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap_or_else(|| {
                    panic!(
                        "Expected Int64Array for key '{key_str}' at row {row} but got {:?}.\nInput: {:?}",
                        col.data_type(),
                        String::from_utf8_lossy(line)
                    )
                });
                if !arr.is_null(row) {
                    assert_eq!(
                        arr.value(row),
                        expected,
                        "Int value mismatch at '{key_str}'[{row}].\nInput: {:?}",
                        String::from_utf8_lossy(line)
                    );
                }
            } else if val.is_f64() {
                let expected = val.as_f64().unwrap();
                let col_owned;
                let col: &dyn Array = if let Some(child) =
                    get_struct_child(&batch, key_str, "float")
                {
                    child
                } else {
                    col_owned = batch
                        .column_by_name(key_str)
                        .unwrap_or_else(|| {
                            panic!(
                                "Scanner produced no column for float key '{key_str}' at row {row}.\nInput: {:?}",
                                String::from_utf8_lossy(line)
                            )
                        });
                    col_owned.as_ref()
                };
                if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    if !arr.is_null(row) {
                        let actual = arr.value(row);
                        assert!(
                            actual == expected
                                || (actual - expected).abs() < 1e-6
                                || (actual.is_nan() && expected.is_nan()),
                            "Float value mismatch at '{key_str}'[{row}]: expected={expected}, actual={actual}.\nInput: {:?}",
                            String::from_utf8_lossy(line)
                        );
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    // Scanner classified this as Int64 (e.g. -0 has no decimal/exponent).
                    // The cast to f64 is exact for semantically integral values, so require
                    // exact equality rather than a loose tolerance that can hide real bugs.
                    if !arr.is_null(row) {
                        let actual = arr.value(row) as f64;
                        assert!(
                            actual == expected,
                            "Float(as int) value mismatch at '{key_str}'[{row}]: expected={expected}, actual={actual}.\nInput: {:?}",
                            String::from_utf8_lossy(line)
                        );
                    }
                } else {
                    panic!(
                        "Expected Float64Array or Int64Array for key '{key_str}' at row {row} but got {:?}.\nInput: {:?}",
                        col.data_type(),
                        String::from_utf8_lossy(line)
                    );
                }
            }
            // booleans stored as strings, nulls are null — skip for now
        }

        row += 1;
    }
}

// ===========================================================================
// proptest generators — shared from logfwd-test-utils
// ===========================================================================

// ===========================================================================
// proptest: ground-truth oracle (values verified against sonic-rs)
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(logfwd_test_utils::proptest_cases()))]

    #[test]
    fn oracle_single_line(obj in arb_flat_object(1..20)) {
        let input = format!("{obj}\n");
        assert_values_correct(input.as_bytes());
    }

    #[test]
    fn oracle_multi_line(buf in arb_ndjson_buffer()) {
        assert_values_correct(&buf);
    }

    #[test]
    fn oracle_wide_objects(obj in arb_flat_object(20..50)) {
        let input = format!("{obj}\n");
        assert_values_correct(input.as_bytes());
    }

    #[test]
    fn oracle_numbers(
        n in prop_oneof![
            Just(0i64),
            Just(-1i64),
            Just(i64::MAX),
            Just(i64::MIN),
            (-1_000_000i64..1_000_000),
        ],
    ) {
        let input = format!("{{\"n\":{n}}}\n");
        assert_values_correct(input.as_bytes());
    }

    #[test]
    fn oracle_floats(
        f in (-1.0e6f64..1.0e6).prop_filter("finite", |f| f.is_finite()),
    ) {
        let input = format!("{{\"v\":{f}}}\n");
        assert_values_correct(input.as_bytes());
    }

    #[test]
    fn oracle_strings_no_escapes(
        key in "[a-z]{1,8}",
        val in "[a-zA-Z0-9 ]{0,50}",
    ) {
        let input = format!("{{\"{key}\":\"{val}\"}}\n");
        assert_values_correct(input.as_bytes());
    }

    #[test]
    fn oracle_strings_with_escapes(
        key in "[a-z]{1,8}",
        val in arb_json_string(),
    ) {
        let input = format!("{{\"{key}\":\"{val}\"}}\n");
        assert_values_correct(input.as_bytes());
    }

    #[test]
    fn oracle_long_strings(
        key in "[a-z]{1,5}",
        val in "[a-zA-Z0-9 ]{60,200}",
    ) {
        let input = format!("{{\"{key}\":\"{val}\"}}\n");
        assert_values_correct(input.as_bytes());
    }
}

// ===========================================================================
// Cross-mode consistency: scan (Utf8View) vs scan_detached (Utf8)
// ===========================================================================

/// Verify scan and scan_detached produce the same row count and the same
/// non-null values for the same input. Accounts for StringArray vs StringViewArray.
fn assert_builders_consistent(input: &[u8]) {
    let mut detached = Scanner::new(ScanConfig::default());
    let mut streaming = Scanner::new(ScanConfig::default());

    let sb = detached
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan_detached should succeed");
    let stb = streaming
        .scan(Bytes::from(input.to_vec()))
        .expect("scan should succeed");

    assert_eq!(
        sb.num_rows(),
        stb.num_rows(),
        "Row count mismatch between scan modes"
    );

    // Compare column names (both should produce the same set)
    let mut s_names: Vec<_> = sb
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut st_names: Vec<_> = stb
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    s_names.sort();
    st_names.sort();
    assert_eq!(
        s_names, st_names,
        "Column names differ between scan modes.\nDetached: {s_names:?}\nStreaming: {st_names:?}"
    );

    // Compare values row-by-row for int and float columns
    for col_name in &s_names {
        let s_col = sb.column_by_name(col_name).unwrap();
        let st_col = stb.column_by_name(col_name).unwrap();
        for row in 0..sb.num_rows() {
            assert_eq!(
                s_col.is_null(row),
                st_col.is_null(row),
                "Null mismatch at {col_name}[{row}] between scan modes"
            );
            if !s_col.is_null(row) {
                if let Some(a) = s_col.as_any().downcast_ref::<Int64Array>() {
                    let b = st_col.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(
                        a.value(row),
                        b.value(row),
                        "Int mismatch at {col_name}[{row}]"
                    );
                }
                if let Some(a) = s_col.as_any().downcast_ref::<Float64Array>() {
                    let b = st_col.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert!(
                        (a.value(row) - b.value(row)).abs() < 1e-10,
                        "Float mismatch at {col_name}[{row}]"
                    );
                }
                // String comparison: scan_detached uses Utf8, scan uses Utf8View.
                let s_is_str = matches!(
                    s_col.data_type(),
                    arrow::datatypes::DataType::Utf8
                        | arrow::datatypes::DataType::Utf8View
                        | arrow::datatypes::DataType::LargeUtf8
                );
                if s_is_str {
                    assert!(
                        matches!(
                            st_col.data_type(),
                            arrow::datatypes::DataType::Utf8
                                | arrow::datatypes::DataType::Utf8View
                                | arrow::datatypes::DataType::LargeUtf8
                        ),
                        "Type mismatch at {col_name}[{row}]: detached has {:?} but streaming has {:?}",
                        s_col.data_type(),
                        st_col.data_type()
                    );
                    let s_val =
                        arrow::compute::cast(s_col, &arrow::datatypes::DataType::Utf8).unwrap();
                    let st_val =
                        arrow::compute::cast(st_col, &arrow::datatypes::DataType::Utf8).unwrap();
                    let sa = s_val.as_any().downcast_ref::<StringArray>().unwrap();
                    let sta = st_val.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(
                        sa.value(row),
                        sta.value(row),
                        "String mismatch at {col_name}[{row}]"
                    );
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(logfwd_test_utils::proptest_cases()))]

    #[test]
    fn consistency_single_line(obj in arb_flat_object(1..15)) {
        let input = format!("{obj}\n");
        assert_builders_consistent(input.as_bytes());
    }

    #[test]
    fn consistency_multi_line(buf in arb_ndjson_buffer()) {
        assert_builders_consistent(&buf);
    }
}

// ===========================================================================
// BytesMut accumulation consistency: pipeline pattern
// ===========================================================================

/// Verify that accumulating NDJSON chunks into BytesMut, freezing, and
/// scanning produces the same RecordBatch as scanning the concatenated input
/// directly. This exercises the exact data path used by pipeline.rs:
///   multiple Bytes → extend_from_slice into BytesMut → split().freeze() → ZeroCopyScanner
fn assert_accumulation_consistent(chunks: &[&[u8]]) {
    use bytes::{Bytes, BytesMut};
    use logfwd_arrow::scanner::Scanner;

    // Path A: concatenate then scan (baseline)
    let mut full = Vec::new();
    for chunk in chunks {
        full.extend_from_slice(chunk);
    }
    let mut scanner_a = Scanner::new(ScanConfig::default());
    let batch_a = scanner_a
        .scan(Bytes::from(full.clone()))
        .expect("baseline scan should succeed");

    // Path B: accumulate via BytesMut (pipeline pattern)
    let mut buf = BytesMut::new();
    for chunk in chunks {
        buf.extend_from_slice(chunk);
    }
    let frozen = buf.split().freeze();
    let mut scanner_b = Scanner::new(ScanConfig::default());
    let batch_b = scanner_b
        .scan(frozen)
        .expect("accumulated scan should succeed");

    assert_eq!(
        batch_a.num_rows(),
        batch_b.num_rows(),
        "Row count differs: direct={} vs accumulated={}",
        batch_a.num_rows(),
        batch_b.num_rows()
    );

    // Schema must match (name + type + nullability)
    let mut fields_a: Vec<_> = batch_a
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect();
    let mut fields_b: Vec<_> = batch_b
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect();
    fields_a.sort_by(|l, r| l.0.cmp(&r.0));
    fields_b.sort_by(|l, r| l.0.cmp(&r.0));
    assert_eq!(
        fields_a, fields_b,
        "Schema fields differ between direct and accumulated"
    );

    // Compare every column's values — catches corrupted StringView offsets
    for (i, field) in batch_a.schema().fields().iter().enumerate() {
        let col_a = batch_a.column(i);
        let col_b = batch_b.column_by_name(field.name()).unwrap_or_else(|| {
            panic!("Column '{}' missing in accumulated batch", field.name());
        });
        assert_eq!(
            col_a.len(),
            col_b.len(),
            "Column '{}' length differs",
            field.name()
        );
        for row in 0..col_a.len() {
            assert_eq!(
                col_a.is_null(row),
                col_b.is_null(row),
                "Null mismatch at {}[{row}]",
                field.name(),
            );
            if !col_a.is_null(row) {
                assert_eq!(
                    format!("{:?}", col_a.as_ref().slice(row, 1)),
                    format!("{:?}", col_b.as_ref().slice(row, 1)),
                    "Value mismatch at {}[{row}]",
                    field.name(),
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(logfwd_test_utils::proptest_cases()))]

    /// Split NDJSON at a proptest-chosen newline boundary and accumulate via BytesMut.
    #[test]
    fn accumulation_matches_direct(
        buf in arb_ndjson_buffer(),
        split_pct in 0usize..100,
    ) {
        // Find all newline positions
        let newlines: Vec<usize> = buf.iter()
            .enumerate()
            .filter(|&(_, b)| *b == b'\n')
            .map(|(i, _)| i + 1)
            .collect();

        if newlines.is_empty() {
            return Ok(());
        }

        // Pick a newline index based on proptest-generated percentage
        let idx = split_pct % newlines.len();
        let mid = newlines[idx];
        let chunk1 = &buf[..mid];
        let chunk2 = &buf[mid..];

        assert_accumulation_consistent(&[chunk1, chunk2]);
    }

    /// Single chunk (regression: must match direct scan exactly)
    #[test]
    fn accumulation_single_chunk(buf in arb_ndjson_buffer()) {
        assert_accumulation_consistent(&[&buf]);
    }

    /// Many small chunks (stress the accumulation pattern)
    #[test]
    fn accumulation_per_line(buf in arb_ndjson_buffer()) {
        let mut chunks: Vec<&[u8]> = Vec::new();
        let mut start = 0;
        for (i, &b) in buf.iter().enumerate() {
            if b == b'\n' {
                chunks.push(&buf[start..=i]);
                start = i + 1;
            }
        }
        if start < buf.len() {
            chunks.push(&buf[start..]);
        }
        if !chunks.is_empty() {
            assert_accumulation_consistent(&chunks);
        }
    }
}

// ===========================================================================
// Curated edge cases
// ===========================================================================

macro_rules! edge_case {
    ($name:ident, $input:expr) => {
        #[test]
        fn $name() {
            assert_values_correct($input);
        }
    };
}

// --- Basic types ---
edge_case!(edge_empty_object, b"{}\n");
edge_case!(edge_single_string, br#"{"a":"b"}"#);
edge_case!(edge_single_int, br#"{"a":42}"#);
edge_case!(edge_single_float, br#"{"a":3.14}"#);
edge_case!(edge_single_bool_true, br#"{"a":true}"#);
edge_case!(edge_single_bool_false, br#"{"a":false}"#);
edge_case!(edge_single_null, br#"{"a":null}"#);
edge_case!(edge_empty_string_value, br#"{"a":""}"#);

// --- Escapes ---
edge_case!(edge_escaped_quote, br#"{"a":"hello \"world\""}"#);
edge_case!(edge_escaped_backslash, b"{\"a\":\"test\\\\\"}\n");
edge_case!(
    edge_escaped_backslash_quote,
    b"{\"a\":\"test\\\\\\\"end\"}\n"
);
edge_case!(edge_escaped_newline, br#"{"a":"line1\nline2"}"#);
edge_case!(edge_escaped_tab, br#"{"a":"col1\tcol2"}"#);
edge_case!(edge_multiple_escapes, br#"{"a":"a\nb\tc\r\\"}"#);
edge_case!(edge_unicode_escape, br#"{"a":"\u0048ello"}"#);

// --- Nested structures ---
edge_case!(edge_nested_object, br#"{"user":{"name":"alice","id":1}}"#);
edge_case!(edge_nested_array, br#"{"tags":["a","b","c"]}"#);
edge_case!(edge_deep_nesting, br#"{"a":{"b":{"c":{"d":"deep"}}}}"#);
edge_case!(edge_array_mixed_types, br#"{"a":[1,"two",null,true,3.14]}"#);
edge_case!(
    edge_braces_in_nested_string,
    br#"{"data":{"msg":"has } and { inside"},"ok":true}"#
);
edge_case!(edge_brackets_in_string, br#"{"a":"[not an array]"}"#);

// --- Numbers ---
edge_case!(edge_negative_int, br#"{"v":-42}"#);
edge_case!(edge_negative_zero, br#"{"v":-0}"#);
edge_case!(edge_float_exponent, br#"{"v":1e2}"#);
edge_case!(edge_float_neg_exponent, br#"{"v":1.5e-3}"#);
edge_case!(edge_zero, br#"{"v":0}"#);
edge_case!(edge_large_int, br#"{"v":9223372036854775807}"#); // i64::MAX

// --- Multi-row ---
edge_case!(
    edge_type_conflict,
    br#"{"s":200}
{"s":"OK"}
"#
);
edge_case!(
    edge_missing_fields,
    br#"{"a":"hello"}
{"b":"world"}
"#
);
edge_case!(
    edge_varying_field_count,
    br#"{"a":1}
{"a":1,"b":2,"c":3}
{"a":1}
"#
);

// --- Whitespace ---
edge_case!(edge_trailing_whitespace, b"{\"a\":1}   \n");
edge_case!(edge_leading_whitespace, b"   {\"a\":1}\n");
edge_case!(edge_whitespace_around_colon, br#"{"a" : 1}"#);
edge_case!(
    edge_whitespace_everywhere,
    b"  {  \"a\"  :  1  ,  \"b\"  :  2  }  \n"
);

// --- Special characters in strings ---
edge_case!(edge_colon_in_string, br#"{"url":"http://x.com:8080/path"}"#);
edge_case!(edge_comma_in_string, br#"{"csv":"a,b,c"}"#);
edge_case!(edge_brace_in_string, br#"{"tmpl":"{{hello}}"}"#);

// --- Boundary conditions ---
edge_case!(edge_no_trailing_newline, br#"{"a":1}"#);
edge_case!(edge_multiple_newlines, b"{\"a\":1}\n\n\n{\"b\":2}\n");
edge_case!(edge_empty_input, b"");
edge_case!(edge_newline_only, b"\n\n\n");
edge_case!(
    edge_single_field_many_rows,
    b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n{\"x\":4}\n{\"x\":5}\n"
);

// --- SIMD boundary tests (16-byte and 64-byte aligned) ---

#[test]
fn edge_string_crossing_16byte_boundary() {
    // Key + value that puts the closing quote right at byte 16, 32, etc.
    for target_len in [14, 15, 16, 17, 30, 31, 32, 33, 62, 63, 64, 65] {
        let val: String = "x".repeat(target_len);
        let input = format!("{{\"k\":\"{val}\"}}\n");
        assert_values_correct(input.as_bytes());
    }
}

#[test]
fn edge_line_crossing_64byte_boundary() {
    // Lines of various lengths around 64-byte block boundaries
    for padding in [50, 60, 62, 63, 64, 65, 66, 70, 126, 127, 128, 129] {
        let val: String = "a".repeat(padding);
        let input = format!("{{\"msg\":\"{val}\",\"n\":42}}\n");
        assert_values_correct(input.as_bytes());
    }
}

#[test]
fn edge_escape_at_simd_boundaries() {
    // Place backslash-quote at positions 14, 15, 16, 31, 32, 63, 64
    for boundary in [14, 15, 16, 31, 32, 63, 64] {
        if boundary < 3 {
            continue;
        }
        let prefix: String = "x".repeat(boundary - 1);
        let input = format!("{{\"k\":\"{prefix}\\\"tail\"}}\n");
        assert_values_correct(input.as_bytes());
    }
}

#[test]
fn edge_many_fields() {
    let mut fields = Vec::new();
    for i in 0..100 {
        fields.push(format!("\"f{i}\":{i}"));
    }
    let input = format!("{{{}}}\n", fields.join(","));
    assert_values_correct(input.as_bytes());
}

#[test]
fn edge_large_batch() {
    let mut buf = Vec::new();
    for i in 0..5000 {
        let line = format!(
            r#"{{"level":"INFO","msg":"request {}","status":{},"duration":{:.2}}}"#,
            i,
            if i % 10 == 0 { 500 } else { 200 },
            0.5 + (i as f64) * 0.01
        );
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    assert_values_correct(&buf);
}

// --- Duplicate keys (both scanners handle gracefully via first-writer-wins) ---

#[test]
fn edge_duplicate_keys() {
    let input = br#"{"a":1,"a":2}
"#;
    // Both scanners handle duplicates via first-writer-wins in the builder
    let mut simd = Scanner::new(ScanConfig::default());
    let batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should succeed");
    assert_eq!(batch.num_rows(), 1);
    // First-writer-wins
    let col = batch
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 1);
}

#[test]
fn edge_duplicate_keys_different_types() {
    let input = br#"{"a":1,"a":"hello"}
"#;
    let mut simd = Scanner::new(ScanConfig::default());
    let batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should succeed");
    assert_eq!(batch.num_rows(), 1);
}

// --- No-panic on malformed input ---

#[test]
fn no_panic_truncated_string() {
    let input = b"{\"a\":\"unterminated\n";
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should not fail on malformed JSON");
}

#[test]
fn no_panic_truncated_object() {
    let input = b"{\"a\":1,\"b\"\n";
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should not fail on malformed JSON");
}

#[test]
fn no_panic_garbage() {
    let input = b"not json at all\n";
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should not fail on malformed JSON");
}

#[test]
fn no_panic_random_bytes() {
    // The scanner contract requires valid UTF-8 input. Use a mix of valid
    // UTF-8 byte sequences (including multi-byte characters) rather than
    // arbitrary bytes that violate the precondition.
    let input = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789{}\n\u{00e9}\u{4e2d}\u{1f600}\n"
        .as_bytes()
        .to_vec();
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input))
        .expect("scan should not fail on valid UTF-8");
}

#[test]
fn no_panic_only_quotes() {
    let input = b"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"";
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should not fail on malformed JSON");
}

#[test]
fn no_panic_only_backslashes() {
    let input = b"\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\";
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input.to_vec()))
        .expect("scan should not fail on malformed JSON");
}

#[test]
fn no_panic_deeply_nested() {
    let mut input = String::new();
    input.push_str("{\"a\":");
    for _ in 0..100 {
        input.push_str("{\"x\":");
    }
    input.push('1');
    for _ in 0..100 {
        input.push('}');
    }
    input.push_str("}\n");
    let mut simd = Scanner::new(ScanConfig::default());
    let _batch = simd
        .scan_detached(Bytes::from(input.into_bytes()))
        .expect("scan should not fail on valid UTF-8");
}
