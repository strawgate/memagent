//! Pipeline invariant tests.
//!
//! These tests verify properties that must hold for *all* inputs:
//!
//! 1. **Determinism** — the same bytes always produce the same output.
//! 2. **No data loss** — every non-empty input line becomes exactly one row.
//! 3. **`_raw` preservation** — the raw parser stores each line verbatim in
//!    the `_raw` field with no mutation (only JSON-safe escaping, reversed on
//!    read-back).
//! 4. **Type consistency** — field types are stable across repeated scans of
//!    the same data.
//! 5. **Reset / multi-call correctness** — calling `process()` in multiple
//!    chunks produces the same result as one big call.

use arrow::array::{Array, AsArray};
use logfwd_correctness::{batch_to_json_lines, batch_to_sorted_json_lines, run_pipeline};
use logfwd_core::format::{CriParser, FormatParser, JsonParser, RawParser};
use logfwd_core::scanner::{ScanConfig, Scanner};

// ---------------------------------------------------------------------------
// 1. Determinism
// ---------------------------------------------------------------------------

fn check_determinism_raw(label: &str, input: &[u8]) {
    let outputs: Vec<String> = (0..3)
        .map(|_| {
            let mut parser = RawParser::new();
            let result = run_pipeline(&mut parser, input);
            batch_to_sorted_json_lines(&result.batch)
        })
        .collect();

    for (i, output) in outputs.iter().enumerate().skip(1) {
        assert_eq!(
            &outputs[0], output,
            "{label}: run 0 vs run {i} produced different output"
        );
    }
}

fn check_determinism_json(label: &str, input: &[u8]) {
    let outputs: Vec<String> = (0..3)
        .map(|_| {
            let mut parser = JsonParser::new();
            let result = run_pipeline(&mut parser, input);
            batch_to_sorted_json_lines(&result.batch)
        })
        .collect();

    for (i, output) in outputs.iter().enumerate().skip(1) {
        assert_eq!(
            &outputs[0], output,
            "{label}: run 0 vs run {i} produced different output"
        );
    }
}

#[test]
fn determinism_raw_nginx_access() {
    check_determinism_raw(
        "nginx-access",
        include_bytes!("fixtures/elastic-integrations/nginx-access.log"),
    );
}

#[test]
fn determinism_raw_apache_access() {
    check_determinism_raw(
        "apache-access",
        include_bytes!("fixtures/elastic-integrations/apache-access.log"),
    );
}

#[test]
fn determinism_raw_syslog_darwin() {
    check_determinism_raw(
        "syslog-darwin",
        include_bytes!("fixtures/elastic-integrations/syslog-darwin.log"),
    );
}

#[test]
fn determinism_json_synthetic() {
    check_determinism_json(
        "synthetic-json-lines",
        include_bytes!("fixtures/synthetic/json-lines.log"),
    );
}

// ---------------------------------------------------------------------------
// 2. No data loss
// ---------------------------------------------------------------------------

/// Verify that the scanner never drops a row — every non-empty parsed line
/// must appear in the output batch.
fn check_no_data_loss_raw(label: &str, input: &[u8]) {
    let expected_rows = input
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .count();

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, input);

    assert_eq!(
        result.batch.num_rows(),
        expected_rows,
        "{label}: expected {expected_rows} rows, got {}",
        result.batch.num_rows()
    );
    assert_eq!(
        result.line_count,
        expected_rows,
        "{label}: parser line_count mismatch"
    );
}

#[test]
fn no_data_loss_nginx() {
    check_no_data_loss_raw(
        "nginx-access",
        include_bytes!("fixtures/elastic-integrations/nginx-access.log"),
    );
}

#[test]
fn no_data_loss_apache() {
    check_no_data_loss_raw(
        "apache-access",
        include_bytes!("fixtures/elastic-integrations/apache-access.log"),
    );
}

#[test]
fn no_data_loss_syslog() {
    check_no_data_loss_raw(
        "syslog-darwin",
        include_bytes!("fixtures/elastic-integrations/syslog-darwin.log"),
    );
}

#[test]
fn no_data_loss_json_synthetic() {
    let input = include_bytes!("fixtures/synthetic/json-lines.log");
    let expected_rows = input
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .count();

    let mut parser = JsonParser::new();
    let result = run_pipeline(&mut parser, input);

    assert_eq!(result.batch.num_rows(), expected_rows);
    assert_eq!(result.line_count, expected_rows);
}

// ---------------------------------------------------------------------------
// 3. _raw preservation
// ---------------------------------------------------------------------------
//
// The scanner's `scan_string` function returns raw bytes between JSON quotes
// WITHOUT decoding escape sequences (intentional, for performance).  This
// means the `_raw` Arrow column will contain JSON-encoded content, not the
// decoded original bytes.  For example, a line containing literal `"` will
// be stored as `\"` in the `_raw` column.
//
// The invariant we test here is therefore:
//   - The `_raw` Arrow column value equals the JSON-escaped form of the
//     original line (i.e., what `RawParser` put between the outer quotes).
//   - For lines with no characters needing JSON escaping (such as syslog
//     lines without embedded quotes), the Arrow value equals the original.

/// Read the `_raw_str` column from a batch (the parsed `_raw` field value,
/// stored with JSON escapes NOT decoded) and return a vec of String values.
///
/// The scanner stores two `_raw`-related columns:
/// - `_raw`: the entire input JSON line (e.g. `{"_raw":"original content"}`)
/// - `_raw_str`: the parsed value of the `_raw` JSON field, with escape
///   sequences preserved literally (e.g. `original content` but with `\"`
///   for any `"` in the original)
fn extract_raw_str_column(batch: &arrow::record_batch::RecordBatch) -> Vec<String> {
    let col = batch
        .column_by_name("_raw_str")
        .expect("batch should have _raw_str column after RawParser");
    let arr = col.as_string::<i32>();
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                String::new()
            } else {
                arr.value(i).to_string()
            }
        })
        .collect()
}

/// JSON-escape a string the same way `RawParser` does, producing the bytes
/// that will be stored in the `_raw` Arrow column.
fn json_escape(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            _ => out.push(b as char),
        }
    }
    out
}

/// For all log lines, the `_raw_str` Arrow column stores `json_escape(original)`.
/// This is the key recoverability invariant: no data is lost.
fn assert_raw_str_invariant(label: &str, fixture: &[u8]) {
    let original_lines: Vec<&str> = std::str::from_utf8(fixture)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .collect();

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);

    let raw_values = extract_raw_str_column(&result.batch);
    assert_eq!(raw_values.len(), original_lines.len(), "{label}: row count mismatch");

    for (i, (raw, orig)) in raw_values.iter().zip(original_lines.iter()).enumerate() {
        let expected = json_escape(orig);
        assert_eq!(
            raw, &expected,
            "{label} row {i}: _raw_str should equal json_escape(original line)\n  original: {orig}"
        );
    }
}

#[test]
fn raw_field_recoverable_nginx() {
    assert_raw_str_invariant(
        "nginx-access",
        include_bytes!("fixtures/elastic-integrations/nginx-access.log"),
    );
}

#[test]
fn raw_field_recoverable_apache() {
    assert_raw_str_invariant(
        "apache-access",
        include_bytes!("fixtures/elastic-integrations/apache-access.log"),
    );
}

#[test]
fn raw_field_recoverable_syslog() {
    assert_raw_str_invariant(
        "syslog-darwin",
        include_bytes!("fixtures/elastic-integrations/syslog-darwin.log"),
    );
}

// ---------------------------------------------------------------------------
// 4. Type consistency across repeated scans
// ---------------------------------------------------------------------------

#[test]
fn type_consistency_repeated_scans() {
    // Scanning the same data multiple times must yield the same schema.
    let input = include_bytes!("fixtures/synthetic/json-lines.log");
    let mut parser = JsonParser::new();
    let mut parsed = Vec::new();
    parser.process(input, &mut parsed);

    let schema_first = {
        let mut scanner = Scanner::new(ScanConfig::default(), 16);
        let batch = scanner.scan(&parsed);
        batch.schema()
    };
    let schema_second = {
        let mut scanner = Scanner::new(ScanConfig::default(), 16);
        let batch = scanner.scan(&parsed);
        batch.schema()
    };

    assert_eq!(
        schema_first, schema_second,
        "scanning identical data twice must produce the same schema"
    );
}

// ---------------------------------------------------------------------------
// 5. Reset / multi-chunk correctness
// ---------------------------------------------------------------------------

#[test]
fn multi_chunk_equals_single_call_raw() {
    // Splitting the input into pieces and feeding them sequentially must
    // produce the same logical output as one big process() call.
    let input = include_bytes!("fixtures/elastic-integrations/nginx-access.log");

    // Single call baseline.
    let single = {
        let mut parser = RawParser::new();
        let result = run_pipeline(&mut parser, input);
        batch_to_sorted_json_lines(&result.batch)
    };

    // Multi-chunk (split at every 64 bytes).
    let chunked = {
        let mut parser = RawParser::new();
        let mut all_parsed: Vec<u8> = Vec::new();
        let mut total_lines = 0usize;
        for chunk in input.chunks(64) {
            total_lines += parser.process(chunk, &mut all_parsed);
        }
        let mut scanner = Scanner::new(ScanConfig::default(), total_lines.max(1));
        let batch = scanner.scan(&all_parsed);
        batch_to_sorted_json_lines(&batch)
    };

    assert_eq!(single, chunked, "chunked feeding must equal single-call feeding");
}

#[test]
fn multi_chunk_equals_single_call_json() {
    let input = include_bytes!("fixtures/synthetic/json-lines.log");

    let single = {
        let mut parser = JsonParser::new();
        let result = run_pipeline(&mut parser, input);
        batch_to_sorted_json_lines(&result.batch)
    };

    let chunked = {
        let mut parser = JsonParser::new();
        let mut all_parsed: Vec<u8> = Vec::new();
        let mut total_lines = 0usize;
        for chunk in input.chunks(64) {
            total_lines += parser.process(chunk, &mut all_parsed);
        }
        let mut scanner = Scanner::new(ScanConfig::default(), total_lines.max(1));
        let batch = scanner.scan(&all_parsed);
        batch_to_sorted_json_lines(&batch)
    };

    assert_eq!(single, chunked);
}

#[test]
fn reset_clears_partial_state_raw() {
    // After reset(), a partially-fed line must not leak into the next batch.
    let mut parser = RawParser::new();
    let mut out = Vec::new();

    // Feed an incomplete line (no trailing newline).
    parser.process(b"incomplete line", &mut out);
    // Reset should discard the buffered partial.
    parser.reset();

    // Now feed a complete line — it must appear alone in the output.
    let n = parser.process(b"complete line\n", &mut out);
    assert_eq!(n, 1, "only one line should be emitted after reset");

    let mut scanner = Scanner::new(ScanConfig::default(), 1);
    let batch = scanner.scan(&out);
    assert_eq!(batch.num_rows(), 1);

    let json_out = batch_to_json_lines(&batch);
    let row: serde_json::Value = serde_json::from_str(json_out.lines().next().unwrap()).unwrap();
    assert_eq!(row["_raw"], "complete line");
}

#[test]
fn reset_clears_partial_state_json() {
    let mut parser = JsonParser::new();
    let mut out = Vec::new();

    // Partial JSON — no trailing newline.
    parser.process(b"{\"x\":1}", &mut out);
    parser.reset();

    // New clean line after reset.
    let n = parser.process(b"{\"y\":2}\n", &mut out);
    assert_eq!(n, 1);

    let mut scanner = Scanner::new(ScanConfig::default(), 1);
    let batch = scanner.scan(&out);
    assert_eq!(batch.num_rows(), 1);

    let json_out = batch_to_json_lines(&batch);
    let row: serde_json::Value = serde_json::from_str(json_out.lines().next().unwrap()).unwrap();
    assert_eq!(row["y"], 2);
    assert!(row.get("x").is_none(), "x should not appear after reset");
}

// ---------------------------------------------------------------------------
// 6. CRI-specific invariants
// ---------------------------------------------------------------------------

#[test]
fn cri_determinism_synthetic_fixture() {
    let input = include_bytes!("fixtures/synthetic/cri-lines.log");
    let outputs: Vec<String> = (0..3)
        .map(|_| {
            let mut parser = CriParser::new(1024 * 1024);
            let result = run_pipeline(&mut parser, input);
            batch_to_sorted_json_lines(&result.batch)
        })
        .collect();

    for (i, output) in outputs.iter().enumerate().skip(1) {
        assert_eq!(
            &outputs[0], output,
            "cri synthetic: run 0 vs run {i} differs"
        );
    }
}

#[test]
fn cri_invalid_lines_are_skipped_not_panicked() {
    // Lines that don't match CRI format should be silently skipped.
    let lines: &[&[u8]] = &[
        b"not a cri line at all\n",
        b"2024-01-15T10:30:00Z stdout F {\"valid\":true}\n",
        b"also not cri\n",
        b"incomplete\n",
    ];
    let input: Vec<u8> = lines.iter().flat_map(|l| l.iter().copied()).collect();

    let mut parser = CriParser::new(1024 * 1024);
    let result = run_pipeline(&mut parser, &input);

    // Only the valid CRI line should produce a row — no panic.
    assert_eq!(result.batch.num_rows(), 1);
}

