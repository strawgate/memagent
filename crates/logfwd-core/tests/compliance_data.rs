//! Tier 5 — Data integrity compliance tests.
//!
//! Exercises edge cases in input data that could cause crashes, corruption,
//! or silent data loss in the scanner and builder layers.
//!
//! Run:
//!   cargo test -p logfwd-core --test compliance_data

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use logfwd_arrow::scanner::{SimdScanner, StreamingSimdScanner};
use logfwd_core::aggregator::{AggregateResult, CriAggregator};
use logfwd_core::cri::parse_cri_line;
use logfwd_core::scan_config::ScanConfig;

// ===========================================================================
// Helpers
// ===========================================================================

/// Scan with SimdScanner (StorageBuilder) using default config.
fn scan_storage(input: &[u8]) -> RecordBatch {
    let mut s = SimdScanner::new(ScanConfig::default());
    s.scan(input).expect("StorageBuilder scan failed")
}

/// Scan with StreamingSimdScanner (StreamingBuilder) using default config.
fn scan_streaming(input: &[u8]) -> RecordBatch {
    let mut s = StreamingSimdScanner::new(ScanConfig::default());
    s.scan(bytes::Bytes::from(input.to_vec()))
        .expect("StreamingBuilder scan failed")
}

/// Scan with SimdScanner using keep_raw=true.
fn scan_storage_raw(input: &[u8]) -> RecordBatch {
    let config = ScanConfig {
        keep_raw: true,
        ..ScanConfig::default()
    };
    let mut s = SimdScanner::new(config);
    s.scan(input)
        .expect("StorageBuilder scan (keep_raw) failed")
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

/// Run a test function against both SimdScanner and StreamingSimdScanner,
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
        assert_eq!(get_int(batch, "seq_int", 0), Some(0));
        assert_eq!(get_int(batch, "seq_int", 1), Some(1));
        assert_eq!(get_int(batch, "seq_int", 2), Some(2));
    });
}

#[test]
fn compliance_huge_line() {
    // One JSON line with a 1MB message field.
    let big_msg: String = "x".repeat(1_000_000);
    let input = format!("{{\"msg\":\"{big_msg}\"}}\n");
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "msg_str", 0).expect("msg_str should exist");
        assert_eq!(val.len(), 1_000_000);
    });
}

#[test]
fn compliance_special_chars_in_values() {
    // JSON strings with escape sequences for newlines, tabs, quotes, backslashes, unicode.
    // The scanner stores raw byte content between quotes (including escape sequences).
    let input = br#"{"msg":"line1\nline2\ttab\"quote\\back"}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "msg_str", 0).expect("msg_str should exist");
        // Scanner stores raw escape sequences, so the value should contain
        // the literal backslash-n, backslash-t etc.
        assert!(val.contains(r#"\n"#), "missing \\n in: {val}");
        assert!(val.contains(r#"\t"#), "missing \\t in: {val}");
        assert!(val.contains(r#"\\"#), "missing \\\\ in: {val}");
    });
}

#[test]
fn compliance_nested_json_value() {
    let input = br#"{"data":{"inner":"value","num":42}}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "data_str", 0).expect("data_str should exist");
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
        let val = get_str(batch, "tags_str", 0).expect("tags_str should exist");
        assert_eq!(val, r#"["a","b","c"]"#);
    });
}

#[test]
fn compliance_duplicate_keys() {
    // First-writer-wins per scanner contract.
    let input = b"{\"a\":\"first\",\"b\":\"x\",\"a\":\"second\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "a_str", 0).expect("a_str should exist");
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

        // Row 0: status_int=200, status_str=null
        assert_eq!(get_int(batch, "status_int", 0), Some(200));
        assert_null(batch, "status_str", 0);

        // Row 1: status_int=null, status_str="OK"
        assert_null(batch, "status_int", 1);
        assert_eq!(get_str(batch, "status_str", 1), Some("OK".to_string()));
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
        assert_eq!(get_int(batch, "n_int", 0), Some(i64::MAX));

        // Row 1: i64::MIN as int
        assert_eq!(get_int(batch, "n_int", 1), Some(i64::MIN));

        // Row 2: overflow -> float column
        assert_null(batch, "n_int", 2);
        let f = get_float(batch, "n_float", 2).expect("n_float should exist for overflow");
        assert!(f > 9.2e18, "overflow should be a large float: {f}");
    });
}

#[test]
fn compliance_float_precision() {
    let input = b"{\"f\":3.141592653589793}\n{\"f\":1e308}\n{\"f\":-1e308}\n{\"f\":5e-324}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 4);

        let v0 = get_float(batch, "f_float", 0).expect("pi");
        assert!(
            (v0 - std::f64::consts::PI).abs() < 1e-15,
            "pi mismatch: {v0}"
        );

        let v1 = get_float(batch, "f_float", 1).expect("1e308");
        assert!((v1 - 1e308).abs() < 1e292, "1e308 mismatch: {v1}");

        let v2 = get_float(batch, "f_float", 2).expect("-1e308");
        assert!((v2 - (-1e308)).abs() < 1e292, "-1e308 mismatch: {v2}");

        let v3 = get_float(batch, "f_float", 3).expect("5e-324");
        assert!(v3 > 0.0 && v3 < 1e-300, "smallest positive: {v3}");
    });
}

#[test]
fn compliance_boolean_as_string() {
    let input = b"{\"flag\":true}\n{\"flag\":false}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(get_str(batch, "flag_str", 0), Some("true".to_string()));
        assert_eq!(get_str(batch, "flag_str", 1), Some("false".to_string()));
    });
}

#[test]
fn compliance_null_value() {
    let input = b"{\"a\":\"x\",\"b\":null,\"c\":\"y\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_not_null(batch, "a_str", 0);
        assert_eq!(get_str(batch, "a_str", 0), Some("x".to_string()));
        // b is null — it may not have a column at all, or the column is null.
        assert_null(batch, "b_str", 0);
        assert_null(batch, "b_int", 0);
        assert_null(batch, "b_float", 0);
        assert_not_null(batch, "c_str", 0);
        assert_eq!(get_str(batch, "c_str", 0), Some("y".to_string()));
    });
}

#[test]
fn compliance_unicode_keys_and_values() {
    // UTF-8 keys and values with CJK and emoji.
    let input = "{\"名前\":\"太郎\",\"emoji\":\"🎉\"}\n";
    assert_both_scanners(input.as_bytes(), |batch| {
        assert_eq!(batch.num_rows(), 1);
        // The scanner stores raw bytes between quotes, so CJK/emoji should round-trip.
        let name = get_str(batch, "名前_str", 0).expect("CJK key should exist");
        assert_eq!(name, "太郎");
        let emoji = get_str(batch, "emoji_str", 0).expect("emoji key should exist");
        assert_eq!(emoji, "🎉");
    });
}

#[test]
fn compliance_escaped_quotes_in_strings() {
    let input = br#"{"msg":"he said \"hello\" and left"}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "msg_str", 0).expect("msg_str should exist");
        // Scanner stores raw content between outer quotes, preserving escape sequences.
        assert!(
            val.contains("\\\"hello\\\""),
            "escaped quotes not preserved: {val}"
        );
    });
}

#[test]
fn compliance_deeply_nested_json() {
    let input = br#"{"a":{"b":{"c":{"d":{"e":"deep"}}}}}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "a_str", 0).expect("a_str should exist");
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
            let col_name = format!("f{i}_int");
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
        assert_not_null(batch, "key_str", 0);
        let val = get_str(batch, "key_str", 0).expect("key_str should exist");
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
        get_str(&batch_compact, "a_str", 0),
        get_str(&batch_spaced, "a_str", 0),
    );
    assert_eq!(
        get_int(&batch_compact, "c_int", 0),
        get_int(&batch_spaced, "c_int", 0),
    );

    // Also verify streaming produces same results.
    let batch_compact_s = scan_streaming(compact);
    let batch_spaced_s = scan_streaming(spaced);
    assert_eq!(
        get_str(&batch_compact_s, "a_str", 0),
        get_str(&batch_spaced_s, "a_str", 0),
    );
}

#[test]
fn compliance_non_object_lines() {
    // Valid JSON but not objects — scanner emits a row but all columns are null.
    let input = b"\"just a string\"\n42\n[1,2,3]\ntrue\nnull\n{\"valid\":\"object\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 6);
        // Only the last row should have a non-null value.
        assert_eq!(get_str(batch, "valid_str", 5), Some("object".to_string()));
        // Rows 0-4 should have null for the valid_str column.
        for row in 0..5 {
            assert_null(batch, "valid_str", row);
        }
    });
}

#[test]
fn compliance_trailing_no_newline() {
    // Per the scanner code, lines without trailing newline ARE processed
    // (scan_into uses eol=len when no \n is found and processes if pos < eol).
    let input = b"{\"a\":\"complete\"}\n{\"b\":\"no newline at end\"}";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(get_str(batch, "a_str", 0), Some("complete".to_string()));
        assert_eq!(
            get_str(batch, "b_str", 1),
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
    assert_eq!(get_str(&batch, "msg_str", 0), Some("hello".to_string()));
}

#[test]
fn compliance_cri_partial_lines() {
    // CRI partial (P) + full (F) — CriAggregator handles reassembly.
    let mut agg = CriAggregator::new(2 * 1024 * 1024);

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
        AggregateResult::Complete(msg) => {
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
    // Raw format: non-JSON lines captured via keep_raw.
    let raw_input = b"This is a plain text log line\n";

    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: true,
        validate_utf8: false,
    };
    let mut scanner = logfwd_arrow::scanner::SimdScanner::new(config);
    let batch = scanner.scan(raw_input).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(
        batch.schema().field_with_name("_raw").is_ok(),
        "raw format should produce _raw column"
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
    let val = get_int(&batch, "field64_int", 0);
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
fn compliance_keep_raw_preserves_line() {
    let input = b"{\"a\":1,\"b\":\"hello\"}\n";
    let batch = scan_storage_raw(input);
    assert_eq!(batch.num_rows(), 1);
    let raw = batch
        .column_by_name("_raw")
        .expect("_raw column should exist with keep_raw=true");
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
        let col_name = format!("{key}_str");
        assert_eq!(get_str(batch, &col_name, 0), Some("val".to_string()));
    });
}

#[test]
fn compliance_numeric_string_not_coerced() {
    // A string value that looks like a number should remain a string.
    let input = b"{\"port\":\"8080\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_str(batch, "port_str", 0), Some("8080".to_string()));
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
        assert_eq!(get_int(batch, "v_int", 0), Some(42));

        // Row 1: float
        let f = get_float(batch, "v_float", 1).expect("v_float row 1");
        #[allow(clippy::approx_constant)]
        let expected = 3.14;
        assert!((f - expected).abs() < 1e-10);

        // Row 2: string "text"
        assert_eq!(get_str(batch, "v_str", 2), Some("text".to_string()));

        // Row 3: bool stored as string
        assert_eq!(get_str(batch, "v_str", 3), Some("true".to_string()));

        // Row 4: null — all typed columns should be null for this row.
        assert_null(batch, "v_int", 4);
        assert_null(batch, "v_float", 4);
        assert_null(batch, "v_str", 4);
    });
}

#[test]
fn compliance_negative_zero_integer() {
    // -0 as an integer: parse_int_fast returns Some(0).
    let input = b"{\"v\":-0}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_int(batch, "v_int", 0), Some(0));
    });
}

#[test]
fn compliance_batch_reuse_isolation() {
    // Scanning two different inputs with the same scanner should not leak
    // data between batches.
    let mut s = SimdScanner::new(ScanConfig::default());
    let b1 = s.scan(b"{\"a\":\"first\"}\n").unwrap();
    let b2 = s.scan(b"{\"b\":\"second\"}\n").unwrap();

    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b2.num_rows(), 1);

    // b2 should NOT contain field "a" from b1.
    // (StorageBuilder clears collectors on begin_batch, but field_index persists
    // for schema stability. The column exists but the value should be null.)
    if b2.column_by_name("a_str").is_some() {
        assert_null(&b2, "a_str", 0);
    }
    assert_eq!(get_str(&b2, "b_str", 0), Some("second".to_string()));
}

#[test]
fn compliance_streaming_batch_reuse_isolation() {
    let mut s = StreamingSimdScanner::new(ScanConfig::default());
    let b1 = s
        .scan(bytes::Bytes::from_static(b"{\"a\":\"first\"}\n"))
        .unwrap();
    let b2 = s
        .scan(bytes::Bytes::from_static(b"{\"b\":\"second\"}\n"))
        .unwrap();

    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b2.num_rows(), 1);

    if b2.column_by_name("a_str").is_some() {
        assert_null(&b2, "a_str", 0);
    }
}

#[test]
fn compliance_json_with_carriage_return() {
    // Lines with \r\n (Windows-style) endings.
    let input = b"{\"a\":1}\r\n{\"b\":2}\r\n";
    assert_both_scanners(input, |batch| {
        // \r is treated as whitespace by skip_ws, so the scanner should handle this.
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(get_int(batch, "a_int", 0), Some(1));
        assert_eq!(get_int(batch, "b_int", 1), Some(2));
    });
}

#[test]
fn compliance_nested_array_of_objects() {
    let input = br#"{"items":[{"id":1},{"id":2}]}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        let val = get_str(batch, "items_str", 0).expect("items_str");
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
        assert_eq!(get_str(batch, "obj_str", 0), Some("{}".to_string()));
        assert_eq!(get_str(batch, "arr_str", 0), Some("[]".to_string()));
    });
}

#[test]
fn compliance_scientific_notation_integers() {
    // 1e2 looks like it could be 100 but contains 'e', so it's a float.
    let input = b"{\"v\":1e2}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 1);
        // The 'e' makes it a float, not an int.
        let f = get_float(batch, "v_float", 0).expect("v_float");
        assert!((f - 100.0).abs() < 1e-10);
    });
}

#[test]
fn compliance_zero_as_all_types() {
    // 0 as int, 0.0 as float, "0" as string.
    let input = b"{\"a\":0}\n{\"a\":0.0}\n{\"a\":\"0\"}\n";
    assert_both_scanners(input, |batch| {
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(get_int(batch, "a_int", 0), Some(0));
        assert_eq!(get_float(batch, "a_float", 1), Some(0.0));
        assert_eq!(get_str(batch, "a_str", 2), Some("0".to_string()));
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
    assert_eq!(names[0], "z_int");
    assert_eq!(names[1], "a_int");
    assert_eq!(names[2], "m_int");
}

#[test]
fn compliance_braces_inside_string_values() {
    // Braces inside string values should not confuse the nested object scanner.
    let input = br#"{"tmpl":"{{user}}","count":1}"#;
    let input_nl = [input.as_slice(), b"\n"].concat();
    assert_both_scanners(&input_nl, |batch| {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(get_int(batch, "count_int", 0), Some(1));
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
        assert_eq!(get_int(batch, "i_int", 0), Some(0));
        assert_eq!(get_int(batch, "i_int", 9_999), Some(9_999));
    });
}
