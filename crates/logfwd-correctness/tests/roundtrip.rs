//! Round-trip correctness tests.
//!
//! These tests verify that data survives the full format-parser → scanner →
//! serializer cycle without corruption.  The key invariant: after parsing and
//! scanning, every field present in the original input is still present and
//! still has the correct value in the output.
//!
//! Fixture files used:
//!   tests/fixtures/synthetic/json-lines.log   — newline-delimited JSON
//!   tests/fixtures/synthetic/cri-lines.log    — CRI container-log format

use arrow::array::AsArray;
use logfwd_correctness::{batch_to_json_lines, batch_to_sorted_json_lines, run_pipeline};
use logfwd_core::format::{CriParser, JsonParser, RawParser};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Deserialize a single JSON object from a string (returns serde_json::Value).
/// Panics cleanly on malformed input so tests get readable failures.
fn parse_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or_else(|e| panic!("invalid JSON: {e}\nInput: {s}"))
}

// ---------------------------------------------------------------------------
// Raw format round-trip
// ---------------------------------------------------------------------------

#[test]
fn raw_each_line_produces_one_row() {
    // Every non-empty input line should become exactly one row.
    let input = b"first line\nsecond line with spaces\nthird line: alphanumeric only\n";

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, input);

    assert_eq!(result.batch.num_rows(), 3, "should have one row per input line");
}

#[test]
fn raw_lines_without_special_chars_round_trip_verbatim() {
    // For lines with no JSON-special characters (`"`, `\`, `\r`, `\t`),
    // the `_raw` field in the JSON output equals the original line exactly.
    let input = b"192.168.1.1 - - [01/Jan/2024:00:00:00 +0000] GET /path 200 512\n\
                  10.0.0.1 - - [01/Jan/2024:00:00:01 +0000] POST /api 201 128\n";

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, input);

    assert_eq!(result.batch.num_rows(), 2);

    let json_out = batch_to_json_lines(&result.batch);
    let rows: Vec<serde_json::Value> = json_out
        .lines()
        .filter(|l| !l.is_empty())
        .map(parse_json)
        .collect();

    assert_eq!(
        rows[0]["_raw"],
        "192.168.1.1 - - [01/Jan/2024:00:00:00 +0000] GET /path 200 512"
    );
    assert_eq!(
        rows[1]["_raw"],
        "10.0.0.1 - - [01/Jan/2024:00:00:01 +0000] POST /api 201 128"
    );
}

#[test]
fn raw_special_chars_stored_as_json_escaped() {
    // NOTE ON SCANNER DESIGN: The scanner stores JSON string values WITHOUT
    // decoding escape sequences (a deliberate zero-copy design choice).
    // Consequently, for a line containing `"` or `\`, the value stored in the
    // Arrow `_raw_str` column is the JSON-escaped form, not the original bytes.
    //
    // Example:
    //   input line:       line with "quotes"
    //   _raw_str in Arrow: line with \"quotes\"
    //
    // This is recoverable — apply the inverse escape to restore the original.
    let original = r#"line with "double quotes" and \backslash\"#;
    let input = format!("{original}\n");

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, input.as_bytes());

    assert_eq!(result.batch.num_rows(), 1);

    // Read _raw_str directly from Arrow — no serialization/deserialization.
    let col = result.batch.column_by_name("_raw_str").unwrap();
    let arr = col.as_string::<i32>();
    let raw_str_value = arr.value(0);

    // The stored value is the JSON-escaped form of the original line.
    let expected_escaped = original
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    assert_eq!(raw_str_value, expected_escaped);
}

#[test]
fn raw_empty_lines_are_skipped() {
    let input = b"line one\n\nline two\n\n\nline three\n";

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, input);

    // Empty lines must not produce rows.
    assert_eq!(result.batch.num_rows(), 3);
}

// ---------------------------------------------------------------------------
// JSON format round-trip
// ---------------------------------------------------------------------------

#[test]
fn json_fields_survive_parse_scan_cycle() {
    let fixture = include_bytes!("fixtures/synthetic/json-lines.log");
    let mut parser = JsonParser::new();
    let result = run_pipeline(&mut parser, fixture);

    let json_out = batch_to_json_lines(&result.batch);

    for (row_idx, line) in json_out.lines().filter(|l| !l.is_empty()).enumerate() {
        let row = parse_json(line);
        // Every row in the synthetic fixture has these fields.
        assert!(
            row.get("timestamp").is_some(),
            "row {row_idx} missing 'timestamp'"
        );
        assert!(
            row.get("level").is_some(),
            "row {row_idx} missing 'level'"
        );
        assert!(
            row.get("service").is_some(),
            "row {row_idx} missing 'service'"
        );
        assert!(row.get("msg").is_some(), "row {row_idx} missing 'msg'");
    }
}

#[test]
fn json_numeric_fields_are_integers_in_output() {
    // Integer fields in JSON should be extracted as integers (not strings).
    let input = b"{\"status\":200,\"duration_ms\":42,\"msg\":\"ok\"}\n";

    let mut parser = JsonParser::new();
    let result = run_pipeline(&mut parser, input);

    assert_eq!(result.batch.num_rows(), 1);

    let json_out = batch_to_json_lines(&result.batch);
    let row = parse_json(json_out.lines().next().unwrap());

    // Should deserialize as numbers, not quoted strings.
    assert_eq!(row["status"], 200);
    assert_eq!(row["duration_ms"], 42);
    assert_eq!(row["msg"], "ok");
}

#[test]
fn json_float_fields_survive() {
    let input = b"{\"latency\":1.5,\"score\":0.999,\"name\":\"test\"}\n";

    let mut parser = JsonParser::new();
    let result = run_pipeline(&mut parser, input);

    let json_out = batch_to_json_lines(&result.batch);
    let row = parse_json(json_out.lines().next().unwrap());

    let latency = row["latency"].as_f64().expect("latency should be a float");
    assert!((latency - 1.5).abs() < f64::EPSILON);

    let score = row["score"].as_f64().expect("score should be a float");
    assert!((score - 0.999).abs() < 1e-9);
}

#[test]
fn json_full_synthetic_fixture_row_count() {
    // Each line of the fixture should produce exactly one row.
    let fixture = include_bytes!("fixtures/synthetic/json-lines.log");
    let expected_rows = fixture.split(|&b| b == b'\n').filter(|l| !l.is_empty()).count();

    let mut parser = JsonParser::new();
    let result = run_pipeline(&mut parser, fixture);

    assert_eq!(result.batch.num_rows(), expected_rows);
}

// ---------------------------------------------------------------------------
// CRI format round-trip
// ---------------------------------------------------------------------------

#[test]
fn cri_full_line_message_extracted() {
    // A single full CRI line should emit its message content.
    let input = b"2024-01-15T10:30:00.123Z stdout F {\"level\":\"info\",\"msg\":\"hello\"}\n";

    let mut parser = CriParser::new(1024 * 1024);
    let result = run_pipeline(&mut parser, input);

    assert_eq!(result.batch.num_rows(), 1, "one full line → one row");

    let json_out = batch_to_json_lines(&result.batch);
    let row = parse_json(json_out.lines().next().unwrap());

    assert_eq!(row["level"], "info");
    assert_eq!(row["msg"], "hello");
}

#[test]
fn cri_partial_lines_reassembled() {
    // A P+F pair should be reassembled into a single message.
    // P chunk message: `{"msg":"split-`  +  F chunk message: `line"}`
    // Combined message: `{"msg":"split-line"}`
    let p = b"2024-01-15T10:30:00.000Z stdout P {\"msg\":\"split-\n";
    let f = b"2024-01-15T10:30:00.001Z stdout F line\"}\n";
    let input = [p.as_slice(), f.as_slice()].concat();

    let mut parser = CriParser::new(1024 * 1024);
    let result = run_pipeline(&mut parser, &input);

    assert_eq!(result.batch.num_rows(), 1, "P+F pair → single row");

    let json_out = batch_to_json_lines(&result.batch);
    let row = parse_json(json_out.lines().next().unwrap());
    assert_eq!(row["msg"], "split-line");
}

#[test]
fn cri_fixture_produces_expected_row_count() {
    // The synthetic fixture has 8 CRI input lines, but one pair is P+F,
    // so we expect 7 output rows (one per complete message).
    let fixture = include_bytes!("fixtures/synthetic/cri-lines.log");

    let mut parser = CriParser::new(1024 * 1024);
    let result = run_pipeline(&mut parser, fixture);

    // 8 input CRI lines: 6 full lines + 1 P/F pair = 7 complete messages.
    assert_eq!(result.batch.num_rows(), 7);
}

#[test]
fn cri_only_full_lines_and_reassembled_pairs_produce_rows() {
    // Lone "P" lines must NOT appear in output until their matching "F" arrives.
    let lines: &[&[u8]] = &[
        b"2024-01-15T10:30:00Z stdout F {\"n\":1}\n",
        b"2024-01-15T10:30:01Z stdout P {\"n\":\n",   // partial — buffered
        b"2024-01-15T10:30:01Z stdout F 2}\n",         // completes the partial
        b"2024-01-15T10:30:02Z stdout F {\"n\":3}\n",
    ];
    let input: Vec<u8> = lines.iter().flat_map(|l| l.iter().copied()).collect();

    let mut parser = CriParser::new(1024 * 1024);
    let result = run_pipeline(&mut parser, &input);

    assert_eq!(result.batch.num_rows(), 3);

    let json_out = batch_to_json_lines(&result.batch);
    let rows: Vec<serde_json::Value> = json_out
        .lines()
        .filter(|l| !l.is_empty())
        .map(parse_json)
        .collect();

    assert_eq!(rows[0]["n"], 1);
    assert_eq!(rows[1]["n"], 2);
    assert_eq!(rows[2]["n"], 3);
}

// ---------------------------------------------------------------------------
// Cross-format: identical content through different parsers
// ---------------------------------------------------------------------------

#[test]
fn same_json_content_same_batch_regardless_of_input_format() {
    // A JSON log line fed through JsonParser vs. wrapped in CRI format via
    // CriParser should both produce a batch with the same logical content.
    let json_line = br#"{"level":"info","code":42}"#;
    let cri_line =
        b"2024-01-15T10:30:00Z stdout F {\"level\":\"info\",\"code\":42}\n";

    let mut json_parser = JsonParser::new();
    let json_input = [json_line.as_slice(), b"\n"].concat();
    let json_result = run_pipeline(&mut json_parser, &json_input);

    let mut cri_parser = CriParser::new(1024 * 1024);
    let cri_result = run_pipeline(&mut cri_parser, cri_line);

    assert_eq!(json_result.batch.num_rows(), 1);
    assert_eq!(cri_result.batch.num_rows(), 1);

    // Both should have the same sorted JSON output.
    let json_sorted = batch_to_sorted_json_lines(&json_result.batch);
    let cri_sorted = batch_to_sorted_json_lines(&cri_result.batch);
    assert_eq!(json_sorted, cri_sorted);
}
