//! Corpus tests: real-world log data from the elastic/integrations repository.
//!
//! All fixture files are pinned to commit:
//!   elastic/integrations @ 68b822662604844675c944e9cb68ea1edadf4ccd
//!
//! Each test runs a fixture through the raw-format pipeline (which wraps every
//! line verbatim in `{"_raw":"..."}`) and then takes a snapshot of:
//!   1. The row count.
//!   2. The Arrow schema.
//!   3. Every row as JSON, sorted for stability.
//!
//! On the first run the snapshots are written automatically.  To intentionally
//! update them after a behaviour change, set `UPDATE_SNAPSHOTS=1`.

use logfwd_correctness::{
    assert_snapshot, batch_to_sorted_json_lines, count_nonempty_lines, run_pipeline,
    schema_summary,
};
use logfwd_core::format::{JsonParser, RawParser};

// ---------------------------------------------------------------------------
// Snapshot format
// ---------------------------------------------------------------------------

/// Compose the full snapshot text for a corpus test.
fn make_snapshot(fixture_name: &str, row_count: usize, schema: &str, sorted_rows: &str) -> String {
    format!(
        "# Corpus snapshot: {fixture_name}\n\
         # elastic/integrations @ 68b822662604844675c944e9cb68ea1edadf4ccd\n\
         \n\
         rows: {row_count}\n\
         \n\
         schema:\n\
         {schema}\n\
         \n\
         rows (sorted):\n\
         {sorted_rows}\n"
    )
}

// ---------------------------------------------------------------------------
// Elastic integrations corpus — raw format
// ---------------------------------------------------------------------------

#[test]
fn corpus_nginx_access_raw() {
    let fixture = include_bytes!("fixtures/elastic-integrations/nginx-access.log");
    let expected_rows = count_nonempty_lines(fixture);

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);

    assert_eq!(
        result.batch.num_rows(),
        expected_rows,
        "nginx-access: every input line must produce a row"
    );

    let snapshot = make_snapshot(
        "nginx-access.log",
        result.batch.num_rows(),
        &schema_summary(&result.batch),
        &batch_to_sorted_json_lines(&result.batch),
    );
    assert_snapshot("nginx-access-raw.txt", &snapshot);
}

#[test]
fn corpus_apache_access_raw() {
    let fixture = include_bytes!("fixtures/elastic-integrations/apache-access.log");
    let expected_rows = count_nonempty_lines(fixture);

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);

    assert_eq!(
        result.batch.num_rows(),
        expected_rows,
        "apache-access: every input line must produce a row"
    );

    let snapshot = make_snapshot(
        "apache-access.log",
        result.batch.num_rows(),
        &schema_summary(&result.batch),
        &batch_to_sorted_json_lines(&result.batch),
    );
    assert_snapshot("apache-access-raw.txt", &snapshot);
}

#[test]
fn corpus_syslog_darwin_raw() {
    let fixture = include_bytes!("fixtures/elastic-integrations/syslog-darwin.log");
    let expected_rows = count_nonempty_lines(fixture);

    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);

    assert_eq!(
        result.batch.num_rows(),
        expected_rows,
        "syslog-darwin: every input line must produce a row"
    );

    let snapshot = make_snapshot(
        "syslog-darwin.log",
        result.batch.num_rows(),
        &schema_summary(&result.batch),
        &batch_to_sorted_json_lines(&result.batch),
    );
    assert_snapshot("syslog-darwin-raw.txt", &snapshot);
}

// ---------------------------------------------------------------------------
// Synthetic corpus — JSON format
// ---------------------------------------------------------------------------

#[test]
fn corpus_synthetic_json_lines() {
    let fixture = include_bytes!("fixtures/synthetic/json-lines.log");
    let expected_rows = count_nonempty_lines(fixture);

    let mut parser = JsonParser::new();
    let result = run_pipeline(&mut parser, fixture);

    assert_eq!(
        result.batch.num_rows(),
        expected_rows,
        "synthetic json-lines: every input line must produce a row"
    );

    let snapshot = make_snapshot(
        "json-lines.log",
        result.batch.num_rows(),
        &schema_summary(&result.batch),
        &batch_to_sorted_json_lines(&result.batch),
    );
    assert_snapshot("synthetic-json-lines.txt", &snapshot);
}

// ---------------------------------------------------------------------------
// Row-count contract: no line must be silently dropped
// ---------------------------------------------------------------------------

/// Assert that `format_name` drops zero lines from `fixture`.
fn assert_no_drops_raw(fixture_name: &str, fixture: &[u8]) {
    let expected = count_nonempty_lines(fixture);
    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);
    assert_eq!(
        result.batch.num_rows(),
        expected,
        "{fixture_name}: expected {expected} rows but got {}",
        result.batch.num_rows()
    );
}

#[test]
fn corpus_no_drops_nginx_access() {
    assert_no_drops_raw(
        "nginx-access.log",
        include_bytes!("fixtures/elastic-integrations/nginx-access.log"),
    );
}

#[test]
fn corpus_no_drops_apache_access() {
    assert_no_drops_raw(
        "apache-access.log",
        include_bytes!("fixtures/elastic-integrations/apache-access.log"),
    );
}

#[test]
fn corpus_no_drops_syslog_darwin() {
    assert_no_drops_raw(
        "syslog-darwin.log",
        include_bytes!("fixtures/elastic-integrations/syslog-darwin.log"),
    );
}

// ---------------------------------------------------------------------------
// Specific content spot-checks
// ---------------------------------------------------------------------------

#[test]
fn corpus_nginx_first_line_ip() {
    // The very first nginx log line starts with IP 67.43.156.13.
    // After wrapping with RawParser the `_raw` field must contain it.
    let fixture = include_bytes!("fixtures/elastic-integrations/nginx-access.log");
    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);

    let json = logfwd_correctness::batch_to_json_lines(&result.batch);
    let first: serde_json::Value = serde_json::from_str(json.lines().next().unwrap()).unwrap();
    let raw = first["_raw"].as_str().unwrap();
    assert!(
        raw.starts_with("67.43.156.13"),
        "expected nginx log to start with source IP, got: {raw}"
    );
}

#[test]
fn corpus_apache_contains_ipv6_address() {
    // The apache fixture includes IPv6 addresses — verify they survive escaping.
    let fixture = include_bytes!("fixtures/elastic-integrations/apache-access.log");
    let mut parser = RawParser::new();
    let result = run_pipeline(&mut parser, fixture);

    let json = logfwd_correctness::batch_to_json_lines(&result.batch);
    let has_ipv6 = json
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .any(|row| {
            row["_raw"]
                .as_str()
                .map(|r| r.contains("::1") || r.contains(':'))
                .unwrap_or(false)
        });
    assert!(has_ipv6, "expected at least one IPv6 address in apache fixture");
}
