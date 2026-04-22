//! Tier 5 — Data integrity compliance tests.
//!
//! Exercises edge cases in input data that could cause crashes, corruption,
//! or silent data loss in the scanner and builder layers.
//!
//! Run:
//!   cargo test -p logfwd-core --test compliance_data

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, StructArray};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use logfwd_arrow::scanner::Scanner;
use logfwd_core::cri::parse_cri_line;
use logfwd_core::reassembler::{AggregateResult, CriReassembler};
use logfwd_core::scan_config::ScanConfig;

// ===========================================================================
// Helpers
// ===========================================================================

/// Scan with Scanner (detached mode) using default config.
fn scan_storage(input: &[u8]) -> RecordBatch {
    let mut s = Scanner::new(ScanConfig::default());
    s.scan_detached(bytes::Bytes::from(input.to_vec()))
        .expect("scan_detached failed")
}

/// Scan with Scanner (streaming mode) using default config.
fn scan_streaming(input: &[u8]) -> RecordBatch {
    let mut s = Scanner::new(ScanConfig::default());
    s.scan(bytes::Bytes::from(input.to_vec()))
        .expect("scan failed")
}

/// Scan with Scanner (detached mode) with line capture enabled.
fn scan_storage_with_line_capture(input: &[u8]) -> RecordBatch {
    let config = ScanConfig {
        line_field_name: Some("body".to_string()),
        ..ScanConfig::default()
    };
    let mut s = Scanner::new(config);
    s.scan_detached(bytes::Bytes::from(input.to_vec()))
        .expect("scan_detached (line_capture) failed")
}

/// Extract a string column value from a RecordBatch.
/// Works with both Utf8 (StringArray) and Utf8View (StringViewArray).
fn get_str(batch: &RecordBatch, col_name: &str, row: usize) -> Option<String> {
    let col = batch.column_by_name(col_name)?;
    if col.is_null(row) {
        return None;
    }
    // Cast to Utf8 to handle both StringArray and StringViewArray.
    let cast = compute::cast(col, &DataType::Utf8).ok()?;
    let arr = cast.as_any().downcast_ref::<StringArray>()?;
    Some(arr.value(row).to_string())
}

/// Extract an i64 column value from a RecordBatch.
fn get_int(batch: &RecordBatch, col_name: &str, row: usize) -> Option<i64> {
    let col = batch.column_by_name(col_name)?;
    if col.is_null(row) {
        return None;
    }
    let arr = col.as_any().downcast_ref::<Int64Array>()?;
    Some(arr.value(row))
}

/// Extract an f64 column value from a RecordBatch.
fn get_float(batch: &RecordBatch, col_name: &str, row: usize) -> Option<f64> {
    let col = batch.column_by_name(col_name)?;
    if col.is_null(row) {
        return None;
    }
    let arr = col.as_any().downcast_ref::<Float64Array>()?;
    Some(arr.value(row))
}

/// Extract a bool column value from a RecordBatch.
fn get_bool(batch: &RecordBatch, col_name: &str, row: usize) -> Option<bool> {
    let col = batch.column_by_name(col_name)?;
    if col.is_null(row) {
        return None;
    }
    let arr = col.as_any().downcast_ref::<BooleanArray>()?;
    Some(arr.value(row))
}

/// Check that a column exists and the given row IS null.
fn assert_null(batch: &RecordBatch, col_name: &str, row: usize) {
    if let Some(col) = batch.column_by_name(col_name) {
        assert!(
            col.is_null(row),
            "{col_name}[{row}] expected null, got non-null"
        );
    }
    // Column not existing is also acceptable (field never written).
}

/// Check that a column exists and the given row is NOT null.
fn assert_not_null(batch: &RecordBatch, col_name: &str, row: usize) {
    let col = batch
        .column_by_name(col_name)
        .unwrap_or_else(|| panic!("column {col_name} missing"));
    assert!(
        !col.is_null(row),
        "{col_name}[{row}] expected non-null, got null"
    );
}

/// Extract an i64 value from the `"int"` child of a conflict StructArray column.
fn get_struct_int(batch: &RecordBatch, field: &str, row: usize) -> Option<i64> {
    let col = batch.column_by_name(field)?;
    let sa = col.as_any().downcast_ref::<StructArray>()?;
    let child_idx = sa.fields().iter().position(|f| f.name() == "int")?;
    let int_col = sa.column(child_idx);
    if int_col.is_null(row) {
        return None;
    }
    Some(int_col.as_any().downcast_ref::<Int64Array>()?.value(row))
}

/// Extract a String value from the `"str"` child of a conflict StructArray column.
/// Supports both Utf8 and Utf8View child arrays.
fn get_struct_str(batch: &RecordBatch, field: &str, row: usize) -> Option<String> {
    let col = batch.column_by_name(field)?;
    let sa = col.as_any().downcast_ref::<StructArray>()?;
    let child_idx = sa.fields().iter().position(|f| f.name() == "str")?;
    let str_col = sa.column(child_idx);
    if str_col.is_null(row) {
        return None;
    }
    let cast = compute::cast(str_col.as_ref(), &DataType::Utf8).ok()?;
    let arr = cast.as_any().downcast_ref::<StringArray>()?;
    Some(arr.value(row).to_string())
}

/// Extract an f64 value from the `"float"` child of a conflict StructArray column.
fn get_struct_float(batch: &RecordBatch, field: &str, row: usize) -> Option<f64> {
    let col = batch.column_by_name(field)?;
    let sa = col.as_any().downcast_ref::<StructArray>()?;
    let child_idx = sa.fields().iter().position(|f| f.name() == "float")?;
    let float_col = sa.column(child_idx);
    if float_col.is_null(row) {
        return None;
    }
    Some(
        float_col
            .as_any()
            .downcast_ref::<Float64Array>()?
            .value(row),
    )
}

/// Extract a bool value from the `"bool"` child of a conflict StructArray column.
fn get_struct_bool(batch: &RecordBatch, field: &str, row: usize) -> Option<bool> {
    let col = batch.column_by_name(field)?;
    let sa = col.as_any().downcast_ref::<StructArray>()?;
    let child_idx = sa.fields().iter().position(|f| f.name() == "bool")?;
    let bool_col = sa.column(child_idx);
    if bool_col.is_null(row) {
        return None;
    }
    Some(bool_col.as_any().downcast_ref::<BooleanArray>()?.value(row))
}

/// Assert that the named child field of a conflict StructArray column is null at `row`.
fn assert_struct_child_null(batch: &RecordBatch, field: &str, child: &str, row: usize) {
    if let Some(col) = batch.column_by_name(field)
        && let Some(sa) = col.as_any().downcast_ref::<StructArray>()
        && let Some(child_idx) = sa.fields().iter().position(|f| f.name() == child)
    {
        let child_col = sa.column(child_idx);
        assert!(
            child_col.is_null(row),
            "{field}.{child}[{row}] expected null, got non-null"
        );
    }
    // Child not present is also acceptable.
    // Column not existing is also acceptable.
}

/// Run a test function against both scan_detached and scan (streaming),
/// verifying they produce the same row count and column names.
fn assert_both_scanners<F>(input: &[u8], check: F)
where
    F: Fn(&RecordBatch),
{
    let sb = scan_storage(input);
    let stb = scan_streaming(input);

    assert_eq!(
        sb.num_rows(),
        stb.num_rows(),
        "Row count mismatch: storage={} streaming={}",
        sb.num_rows(),
        stb.num_rows()
    );

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
    assert_eq!(s_names, st_names, "Column name mismatch between builders");

    check(&sb);
    check(&stb);
}

// ===========================================================================
// Tests
// ===========================================================================

#[test]
fn compliance_empty_lines() {
    // Blank lines interspersed — scanner should skip them.
    let input = b"{\"seq\":0}\n\n{\"seq\":1}\n\n\n{\"seq\":2}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(get_int(batch, "seq", 0), Some(0));
        assert_eq!(get_int(batch, "seq", 1), Some(1));
        assert_eq!(get_int(batch, "seq", 2), Some(2));
    });
}

#[test]
fn compliance_huge_line() {
    // One JSON line with a 1MB message field.
    let big_msg: String = "x".repeat(1_000_000);
    let input = format!("{{\"msg\":\"{big_msg}\"}}\n");
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "msg", 0).expect("msg should exist");
        assert_eq!(val.len(), 1_000_000);
    });
}

#[test]
fn compliance_special_chars_in_values() {
    // JSON strings with escape sequences for newlines, tabs, quotes, backslashes, unicode.
    // After #410: scanner decodes escape sequences to their actual characters.
    let input = br#"{"msg":"line1\nline2\ttab\"quote\\back"}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "msg", 0).expect("msg should exist");
        // Scanner decodes escape sequences: \n → newline, \t → tab, \\ → backslash
        assert!(val.contains('\n'), "missing decoded newline in: {val:?}");
        assert!(val.contains('\t'), "missing decoded tab in: {val:?}");
        assert!(val.contains('\\'), "missing decoded backslash in: {val:?}");
    });
}

#[test]
fn compliance_nested_json_value() {
    let input = br#"{"data":{"inner":"value","num":42}}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "data", 0).expect("data should exist");
        assert!(
            val.contains("inner") && val.contains("value") && val.contains("42"),
            "nested JSON not preserved: {val}"
        );
        // Should start with { and end with }
        assert!(val.starts_with('{'), "should start with {{: {val}");
        assert!(val.ends_with('}'), "should end with }}: {val}");
    });
}

#[test]
fn compliance_array_value() {
    let input = b"{\"tags\":[\"a\",\"b\",\"c\"]}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "tags", 0).expect("tags should exist");
        assert_eq!(val, r#"["a","b","c"]"#);
    });
}

#[test]
fn compliance_duplicate_keys() {
    // First-writer-wins per scanner contract.
    let input = b"{\"a\":\"first\",\"b\":\"x\",\"a\":\"second\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "a", 0).expect("a should exist");
        assert_eq!(val, "first", "first-writer-wins violated");
    });
}

#[test]
fn compliance_type_conflict() {
    // Line 1: status as int. Line 2: status as string.
    // Both _int and _str columns should exist, with nulls where type doesn't match.
    let input = b"{\"status\":200}\n{\"status\":\"OK\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 2);

        // Row 0: status.int=200, status.str=null
        assert_eq!(get_struct_int(batch, "status", 0), Some(200));
        assert_struct_child_null(batch, "status", "str", 0);

        // Row 1: status.int=null, status.str="OK"
        assert_struct_child_null(batch, "status", "int", 1);
        assert_eq!(get_struct_str(batch, "status", 1), Some("OK".to_string()));
    });
}

#[test]
fn compliance_integer_boundaries() {
    let input = format!(
        "{{\"n\":{}}}\n{{\"n\":{}}}\n{{\"n\":{}}}\n",
        i64::MAX,              // 9223372036854775807
        i64::MIN,              // -9223372036854775808
        "9223372036854775808"  // i64::MAX + 1 -> overflow to float
    );
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 3);

        // Row 0: i64::MAX as int
        assert_eq!(get_struct_int(batch, "n", 0), Some(i64::MAX));

        // Row 1: i64::MIN as int
        assert_eq!(get_struct_int(batch, "n", 1), Some(i64::MIN));

        // Row 2: overflow -> float child
        assert_struct_child_null(batch, "n", "int", 2);
        let f = get_struct_float(batch, "n", 2).expect("n.float should exist for overflow");
        assert!(f > 9.2e18, "overflow should be a large float: {f}");
    });
}

#[test]
fn compliance_float_precision() {
    let input = b"{\"f\":3.141592653589793}\n{\"f\":1e308}\n{\"f\":-1e308}\n{\"f\":5e-324}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 4);

        let v0 = get_float(batch, "f", 0).expect("pi");
        assert!(
            (v0 - std::f64::consts::PI).abs() < 1e-15,
            "pi mismatch: {v0}"
        );

        let v1 = get_float(batch, "f", 1).expect("1e308");
        assert!((v1 - 1e308).abs() < 1e292, "1e308 mismatch: {v1}");

        let v2 = get_float(batch, "f", 2).expect("-1e308");
        assert!((v2 - (-1e308)).abs() < 1e292, "-1e308 mismatch: {v2}");

        let v3 = get_float(batch, "f", 3).expect("5e-324");
        assert!(v3 > 0.0 && v3 < 1e-300, "smallest positive: {v3}");
    });
}

#[test]
fn compliance_boolean_typed() {
    let input = b"{\"flag\":true}\n{\"flag\":false}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(get_bool(batch, "flag", 0), Some(true));
        assert_eq!(get_bool(batch, "flag", 1), Some(false));
    });
}

#[test]
fn compliance_null_value() {
    let input = b"{\"a\":\"x\",\"b\":null,\"c\":\"y\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_not_null(batch, "a", 0);
        assert_eq!(get_str(batch, "a", 0), Some("x".to_string()));
        // b is null — it may not have a column at all, or the struct children are null.
        assert_struct_child_null(batch, "b", "str", 0);
        assert_struct_child_null(batch, "b", "int", 0);
        assert_struct_child_null(batch, "b", "float", 0);
        assert_not_null(batch, "c", 0);
        assert_eq!(get_str(batch, "c", 0), Some("y".to_string()));
    });
}

#[test]
fn compliance_unicode_keys_and_values() {
    // UTF-8 keys and values with CJK and emoji.
    let input = "{\"名前\":\"太郎\",\"emoji\":\"🎉\"}\n";
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 1);
        // The scanner stores raw bytes between quotes, so CJK/emoji should round-trip.
        let name = get_str(batch, "名前", 0).expect("CJK key should exist");
        assert_eq!(name, "太郎");
        let emoji = get_str(batch, "emoji", 0).expect("emoji key should exist");
        assert_eq!(emoji, "🎉");
    });
}

#[test]
fn compliance_escaped_quotes_in_strings() {
    let input = br#"{"msg":"he said \"hello\" and left"}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "msg", 0).expect("msg should exist");
        // After #410: scanner decodes \" to literal quote characters.
        assert!(
            val.contains("\"hello\""),
            "decoded quotes not found: {val:?}"
        );
    });
}

#[test]
fn compliance_deeply_nested_json() {
    let input = br#"{"a":{"b":{"c":{"d":{"e":"deep"}}}}}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "a", 0).expect("a should exist");
        assert!(val.contains("deep"), "nested value not preserved: {val}");
        assert!(val.starts_with('{'));
        assert!(val.ends_with('}'));
    });
}

#[test]
fn compliance_many_fields() {
    // JSON object with 100 fields. Tests field lookup and the 63-field
    // duplicate detection bitmask boundary.
    let mut fields = Vec::new();
    for i in 0..100 {
        fields.push(format!("\"f{i}\":{i}"));
    }
    let input = format!("{{{}}}\n", fields.join(","));
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 1);
        // All 100 fields should be present as columns.
        for i in 0..100 {
            let col_name = format!("f{i}");
            assert_eq!(
                get_int(batch, &col_name, 0),
                Some(i),
                "field f{i} missing or wrong"
            );
        }
    });
}

#[test]
fn compliance_empty_object() {
    let input = b"{}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        // No columns with data (all would be null if they existed).
        // The batch may have 0 columns or columns from schema — row count is key.
    });
}

#[test]
fn compliance_empty_string_value() {
    let input = b"{\"key\":\"\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        // Empty string is NOT null.
        assert_not_null(batch, "key", 0);
        let val = get_str(batch, "key", 0).expect("key should exist");
        assert_eq!(val, "", "empty string should be empty, not null");
    });
}

#[test]
fn compliance_whitespace_variations() {
    // Compact and spaced JSON should produce identical results.
    let compact = b"{\"a\":\"b\",\"c\":1}\n";
    let spaced = b"{ \"a\" : \"b\" , \"c\" : 1 }\n";

    let batch_compact = scan_storage(compact);
    let batch_spaced = scan_storage(spaced);

    assert_eq!(batch_compact.num_rows(), 1);
    assert_eq!(batch_spaced.num_rows(), 1);
    assert_eq!(
        get_str(&batch_compact, "a", 0),
        get_str(&batch_spaced, "a", 0),
    );
    assert_eq!(
        get_int(&batch_compact, "c", 0),
        get_int(&batch_spaced, "c", 0),
    );

    // Also verify streaming produces same results.
    let batch_compact_s = scan_streaming(compact);
    let batch_spaced_s = scan_streaming(spaced);
    assert_eq!(
        get_str(&batch_compact_s, "a", 0),
        get_str(&batch_spaced_s, "a", 0),
    );
}

#[test]
fn compliance_tab_and_cr_json_boundaries() {
    let input = b"\t{\t\"a\"\t:\t\"b\"\r,\t\"c\"\t:\t1\r}\r\n";

    assert_both_scanners(input, |batch| {
        // JSON whitespace includes tab and CR. The scanner should skip them
        // around object/key/value boundaries while still normalizing CRLF
        // line endings.
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_str(batch, "a", 0), Some("b".to_string()));
        assert_eq!(get_int(batch, "c", 0), Some(1));
    });
}

#[test]
fn compliance_non_object_lines() {
    // Valid JSON but not objects — scanner emits a row but all columns are null.
    let input = b"\"just a string\"\n42\n[1,2,3]\ntrue\nnull\n{\"valid\":\"object\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 6);
        // Only the last row should have a non-null value.
        assert_eq!(get_str(batch, "valid", 5), Some("object".to_string()));
        // Rows 0-4 should have null for the valid_str column.
        for row in 0..5 {
            assert_null(batch, "valid", row);
        }
    });
}

#[test]
fn compliance_trailing_no_newline() {
    // Per the scanner code, lines without trailing newline ARE processed
    // (scan_streaming includes the final non-empty range when no \n is found).
    let input = b"{\"a\":\"complete\"}\n{\"b\":\"no newline at end\"}";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(get_str(batch, "a", 0), Some("complete".to_string()));
        assert_eq!(
            get_str(batch, "b", 1),
            Some("no newline at end".to_string())
        );
    });
}

#[test]
fn compliance_cri_format() {
    // CRI log format: parse_cri_line extracts message, scanner parses JSON.
    let cri_input = b"2024-01-15T10:30:00.000000000Z stdout F {\"msg\":\"hello\"}";
    let cri = parse_cri_line(cri_input).expect("valid CRI line");
    assert!(cri.is_full);

    let mut json_out = Vec::new();
    json_out.extend_from_slice(cri.message);
    json_out.push(b'\n');

    let batch = scan_storage(&json_out);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(get_str(&batch, "msg", 0), Some("hello".to_string()));
}

#[test]
fn compliance_cri_partial_lines() {
    // CRI partial (P) + full (F) — CriReassembler handles reassembly.
    let mut agg = CriReassembler::new(2 * 1024 * 1024);

    let p_line = b"2024-01-15T10:30:00.000000000Z stdout P {\"msg\":\"hel";
    let f_line = b"2024-01-15T10:30:00.000000000Z stdout F lo\"}";

    let cri_p = parse_cri_line(p_line).unwrap();
    assert!(!cri_p.is_full);
    assert!(matches!(
        agg.feed(cri_p.message, cri_p.is_full),
        AggregateResult::Pending
    ));

    let cri_f = parse_cri_line(f_line).unwrap();
    assert!(cri_f.is_full);
    match agg.feed(cri_f.message, cri_f.is_full) {
        AggregateResult::Complete(msg) | AggregateResult::Truncated(msg) => {
            let output = String::from_utf8_lossy(msg);
            assert!(
                output.contains("hel") && output.contains("lo"),
                "partial reassembly failed: {output}"
            );
        }
        AggregateResult::Pending => panic!("expected Complete after F line"),
    }
    agg.reset();
}

#[test]
fn compliance_raw_format() {
    // Raw format: non-JSON lines captured via line_capture.
    let raw_input = b"This is a plain text log line\n";

    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: Some("body".to_string()),
        validate_utf8: false,
        row_predicate: None,
    };
    let mut scanner = Scanner::new(config);
    let batch = scanner
        .scan_detached(bytes::Bytes::from(raw_input.to_vec()))
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(
        batch.schema().field_with_name("body").is_ok(),
        "raw format should produce body column"
    );
}

// ===========================================================================
// Additional edge cases
// ===========================================================================

#[test]
fn compliance_duplicate_keys_across_dup_detection_boundary() {
    // The scanner uses a 64-bit bitmask for duplicate detection on the first
    // 64 fields (indices 0..63). Fields at index >= 64 bypass dup detection.
    // Build a line with 65 unique fields, then duplicate the 65th field.
    let mut pairs = Vec::new();
    for i in 0..65 {
        pairs.push(format!("\"field{i}\":{i}"));
    }
    // Duplicate field64 (index 64, beyond the 64-bit bitmask).
    pairs.push("\"field64\":999".to_string());
    let input = format!("{{{}}}\n", pairs.join(","));
    let batch = scan_storage(input.as_bytes());
    assert_eq!(batch.num_rows(), 1);

    // field64_int should be the first value (0-indexed field64=64), but
    // since idx >= 64 bypasses dup detection, the second write (999) overwrites.
    // This is the known limitation — document it.
    let val = get_int(&batch, "field64", 0);
    assert!(
        val.is_some(),
        "field64 should exist regardless of dup detection"
    );
}

#[test]
fn compliance_empty_input() {
    assert_both_scanners(b"", |batch| {
        assert_eq!(batch.num_rows(), 0);
    });
}

#[test]
fn compliance_only_newlines() {
    assert_both_scanners(b"\n\n\n", |batch| {
        assert_eq!(batch.num_rows(), 0);
    });
}

#[test]
fn compliance_line_capture_preserves_line() {
    let input = b"{\"a\":1,\"b\":\"hello\"}\n";
    let batch = scan_storage_with_line_capture(input);
    assert_eq!(batch.num_rows(), 1);
    let raw = batch
        .column_by_name("body")
        .expect("body column should exist with line_capture=true");
    let arr = raw.as_any().downcast_ref::<StringArray>().unwrap();
    let val = arr.value(0);
    assert_eq!(val, "{\"a\":1,\"b\":\"hello\"}");
}

#[test]
fn compliance_very_long_key() {
    // Key with 500 characters.
    let key: String = "k".repeat(500);
    let input = format!("{{\"{key}\":\"val\"}}\n");
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 1);
        let col_name = key.clone();
        assert_eq!(get_str(batch, &col_name, 0), Some("val".to_string()));
    });
}

#[test]
fn compliance_numeric_string_not_coerced() {
    // A string value that looks like a number should remain a string.
    let input = b"{\"port\":\"8080\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_str(batch, "port", 0), Some("8080".to_string()));
        // Should NOT appear as an int column.
        assert!(
            batch.column_by_name("port_int").is_none(),
            "string '8080' should not create an int column"
        );
    });
}

#[test]
fn compliance_mixed_type_across_rows() {
    // Same field as int, float, string, bool, null across multiple rows.
    let input = b"{\"v\":42}\n{\"v\":3.14}\n{\"v\":\"text\"}\n{\"v\":true}\n{\"v\":null}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 5);

        // Row 0: int
        assert_eq!(get_struct_int(batch, "v", 0), Some(42));

        // Row 1: float
        let f = get_struct_float(batch, "v", 1).expect("v.float should exist for row 1");
        #[allow(clippy::approx_constant)]
        let expected = 3.14;
        assert!((f - expected).abs() < 1e-10);

        // Row 2: string "text"
        assert_eq!(get_struct_str(batch, "v", 2), Some("text".to_string()));

        // Row 3: bool
        assert_eq!(get_struct_bool(batch, "v", 3), Some(true));

        // Row 4: null — all typed children should be null for this row.
        assert_struct_child_null(batch, "v", "int", 4);
        assert_struct_child_null(batch, "v", "float", 4);
        assert_struct_child_null(batch, "v", "str", 4);
        assert_struct_child_null(batch, "v", "bool", 4);
    });
}

#[test]
fn compliance_negative_zero_integer() {
    // -0 as an integer: parse_int_fast returns Some(0).
    let input = b"{\"v\":-0}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_int(batch, "v", 0), Some(0));
    });
}

#[test]
fn compliance_batch_reuse_isolation() {
    // Scanning two different inputs with the same scanner should not leak
    // data between batches.
    let mut s = Scanner::new(ScanConfig::default());
    let b1 = s
        .scan_detached(bytes::Bytes::from(b"{\"a\":\"first\"}\n".to_vec()))
        .unwrap();
    let b2 = s
        .scan_detached(bytes::Bytes::from(b"{\"b\":\"second\"}\n".to_vec()))
        .unwrap();

    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b2.num_rows(), 1);

    // b2 should NOT contain field "a" from b1.
    // (StreamingBuilder clears collectors on begin_batch, but field_index persists
    // for schema stability. The column exists but the value should be null.)
    if b2.column_by_name("a").is_some() {
        assert_null(&b2, "a", 0);
    }
    assert_eq!(get_str(&b2, "b", 0), Some("second".to_string()));
}

#[test]
fn compliance_streaming_batch_reuse_isolation() {
    let mut s = Scanner::new(ScanConfig::default());
    let b1 = s
        .scan(bytes::Bytes::from_static(b"{\"a\":\"first\"}\n"))
        .unwrap();
    let b2 = s
        .scan(bytes::Bytes::from_static(b"{\"b\":\"second\"}\n"))
        .unwrap();

    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b2.num_rows(), 1);

    if b2.column_by_name("a").is_some() {
        assert_null(&b2, "a", 0);
    }
}

#[test]
fn compliance_json_with_carriage_return() {
    // Lines with \r\n (Windows-style) endings.
    let input = b"{\"a\":1}\r\n{\"b\":2}\r\n";
    assert_both_scanners(input, |batch| {
        // \r is treated as whitespace by skip_ws, so the scanner should handle this.
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(get_int(batch, "a", 0), Some(1));
        assert_eq!(get_int(batch, "b", 1), Some(2));
    });
}

#[test]
fn compliance_nested_array_of_objects() {
    let input = br#"{"items":[{"id":1},{"id":2}]}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "items", 0).expect("items column should exist");
        assert!(val.starts_with('['));
        assert!(val.ends_with(']'));
        assert!(val.contains("\"id\":1"));
        assert!(val.contains("\"id\":2"));
    });
}

#[test]
fn compliance_empty_nested_structures() {
    let input = b"{\"obj\":{},\"arr\":[]}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_str(batch, "obj", 0), Some("{}".to_string()));
        assert_eq!(get_str(batch, "arr", 0), Some("[]".to_string()));
    });
}

#[test]
fn compliance_scientific_notation_integers() {
    // 1e2 looks like it could be 100 but contains 'e', so it's a float.
    let input = b"{\"v\":1e2}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        // The 'e' makes it a float, not an int.
        let f = get_float(batch, "v", 0).expect("v column should be float for 1e2");
        assert!((f - 100.0).abs() < 1e-10);
    });
}

#[test]
fn compliance_zero_as_all_types() {
    // 0 as int, 0.0 as float, "0" as string.
    let input = b"{\"a\":0}\n{\"a\":0.0}\n{\"a\":\"0\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(get_struct_int(batch, "a", 0), Some(0));
        assert_eq!(get_struct_float(batch, "a", 1), Some(0.0));
        assert_eq!(get_struct_str(batch, "a", 2), Some("0".to_string()));
    });
}

#[test]
fn compliance_field_ordering_preserved() {
    // Fields should appear in the order they are first encountered.
    let input = b"{\"z\":1,\"a\":2,\"m\":3}\n";
    let batch = scan_storage(input);
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    // The first field encountered is "z", then "a", then "m".
    assert_eq!(names[0], "z");
    assert_eq!(names[1], "a");
    assert_eq!(names[2], "m");
}

#[test]
fn compliance_braces_inside_string_values() {
    // Braces inside string values should not confuse the nested object scanner.
    let input = br#"{"tmpl":"{{user}}","count":1}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_int(batch, "count", 0), Some(1));
    });
}

#[test]
fn compliance_large_batch_row_count() {
    // 10K rows — verify no off-by-one or overflow.
    let mut buf = Vec::new();
    for i in 0..10_000 {
        buf.extend_from_slice(format!("{{\"i\":{i}}}\n").as_bytes());
    }
    assert_both_scanners(&buf, |batch| {
        assert_eq!(batch.num_rows(), 10_000);
        assert_eq!(get_int(batch, "i", 0), Some(0));
        assert_eq!(get_int(batch, "i", 9_999), Some(9_999));
    });
}
