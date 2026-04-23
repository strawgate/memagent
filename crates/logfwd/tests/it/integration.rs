//! End-to-end integration tests for the logfwd pipeline.
//!
//! Tests exercise the full config → file tailing → parsing → transform →
//! output path using temporary files and in-process sinks.  Each test follows
//! the same pattern:
//!   1. Write test data to a tempfile.
//!   2. Build a `Pipeline` via `Pipeline::from_config`.
//!   3. Run it in the calling thread (or a background thread when data must
//!      be written mid-run) until a `CancellationToken` fires.
//!   4. Assert on pipeline metrics (lines_in / lines_out) and, where
//!      applicable, on captured output.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_arrow::scanner::Scanner;
use logfwd_config::Config;
use logfwd_core::scan_config::ScanConfig;
use logfwd_test_utils::test_meter;
use logfwd_transform::SqlTransform;
use logfwd_transform::enrichment::CsvFileTable;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Run `pipeline` until `expected_lines` have passed through the transform
/// stage (checked via `transform_in.lines_total`), then cancel and return.
///
/// A safety deadline prevents the test from hanging if the pipeline stalls.
/// Polls every 20 ms so tests finish as soon as data is processed rather than
/// waiting a fixed timeout.
fn run_until_lines(mut pipeline: Pipeline, expected_lines: u64) -> Pipeline {
    let metrics = Arc::clone(pipeline.metrics());
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= expected_lines {
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sd.cancel();
    });
    pipeline.run(&shutdown).expect("pipeline.run failed");
    pipeline
}

/// Run `pipeline` until the first output sink reports `expected_lines`
/// forwarded rows, then cancel and return.
fn run_until_output_lines(mut pipeline: Pipeline, expected_lines: u64) -> Pipeline {
    let metrics = Arc::clone(pipeline.metrics());
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let lines_out = metrics
                .outputs
                .first()
                .map(|(_, _, stats)| stats.lines_total.load(Ordering::Relaxed))
                .unwrap_or(0);
            if lines_out >= expected_lines {
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sd.cancel();
    });
    pipeline.run(&shutdown).expect("pipeline.run failed");
    pipeline
}

// ---------------------------------------------------------------------------
// 1. Happy path: JSON log file → stdout output → verify lines processed
// ---------------------------------------------------------------------------

/// Write ten JSON log lines, run the pipeline, and assert that the transform
/// stage received exactly ten rows.
#[test]
fn test_happy_path_json_output() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("app.log");

    let mut data = String::new();
    for i in 0..10 {
        data.push_str(&format!(
            r#"{{"level":"INFO","message":"request {i}","status":{}}}"#,
            200 + i,
        ));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
        log_path.display()
    );
    let config = Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    let pipeline = run_until_output_lines(pipeline, 10);

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 10,
        "expected 10 lines through transform, got {lines_in}"
    );
}

// ---------------------------------------------------------------------------
// 2. CRI format: CRI log file → cri parser → verify fields extracted
// ---------------------------------------------------------------------------

/// Write CRI-formatted log lines (all "F" — full, no partial reassembly needed),
/// run the pipeline in CRI mode, and assert that the parser forwarded the
/// expected number of complete log lines.
#[test]
fn test_cri_format_parsing() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("container.log");

    // Three full CRI lines.  Each yields one log record after parsing.
    let cri_data = concat!(
        r#"2024-01-15T10:30:00.123456789Z stdout F {"level":"INFO","msg":"started"}"#,
        "\n",
        r#"2024-01-15T10:30:01.234567890Z stdout F {"level":"WARN","msg":"slow query"}"#,
        "\n",
        r#"2024-01-15T10:30:02.345678901Z stdout F {"level":"ERROR","msg":"timeout"}"#,
        "\n",
    );
    std::fs::write(&log_path, cri_data.as_bytes()).unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: cri
output:
  type: stdout
  format: json
",
        log_path.display()
    );
    let config = Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    let pipeline = run_until_lines(pipeline, 3);

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 3,
        "expected 3 CRI lines through transform, got {lines_in}"
    );
}

// ---------------------------------------------------------------------------
// 3. SQL transform: WHERE filter reduces row count
// ---------------------------------------------------------------------------

/// Write ten JSON lines (five ERROR, five INFO), run a pipeline whose SQL
/// transform filters to ERROR rows only, and assert that the output stats
/// reflect the reduction.
#[test]
fn test_sql_transform_filters_rows() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("mixed.log");

    let mut data = String::new();
    for i in 0..10 {
        let level = if i % 2 == 0 { "ERROR" } else { "INFO" };
        data.push_str(&format!(r#"{{"level":"{level}","message":"event {i}"}}"#,));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level = 'ERROR'"
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );
    let config = Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    let pipeline = run_until_output_lines(pipeline, 5);

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    // lines_out is now derived from the output sink's stats (single increment
    // path: each sink calls inc_lines once on success).
    let lines_out = pipeline
        .metrics()
        .outputs
        .first()
        .map(|(_, _, s)| s.lines_total.load(Ordering::Relaxed))
        .unwrap_or(0);

    assert_eq!(
        lines_in, 5,
        "expected 5 rows into transform after scanner-side pushdown, got {lines_in}"
    );
    assert_eq!(
        lines_out, 5,
        "expected 5 rows out of transform (ERROR only), got {lines_out}"
    );
}

// ---------------------------------------------------------------------------
// 4. HTTP output: supported config validates at load time
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 5. File rotation: write → rotate → write more → verify no data loss
// ---------------------------------------------------------------------------

/// Simulate a logrotate-style rotation: write initial data, let the pipeline
/// start reading it, rename the file (rotation), then create a fresh file and
/// write more data.  Verify that the total number of processed rows equals the
/// combined pre- and post-rotation line counts.
#[test]
fn test_file_rotation_no_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("rotate.log");
    let rotated_path = dir.path().join("rotate.log.1");

    // Pre-rotation data (5 lines).
    let pre_data: String = (0..5)
        .map(|i| format!(r#"{{"batch":"pre","i":{i}}}"#) + "\n")
        .collect();
    std::fs::write(&log_path, pre_data.as_bytes()).unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
        log_path.display()
    );
    let config = Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    let metrics = Arc::clone(pipeline.metrics());
    let shutdown = CancellationToken::new();
    let sd_run = shutdown.clone();
    let sd_write = shutdown.clone();

    // Write post-rotation data in a background thread.
    std::thread::spawn(move || {
        // Poll until the pre-rotation data (5 lines) is ingested before rotating.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 5
                || std::time::Instant::now() >= deadline
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // Rename the active file to simulate rotation.
        std::fs::rename(&log_path, &rotated_path).unwrap();

        // Create the new active file.
        let post_data: String = (0..5)
            .map(|i| format!(r#"{{"batch":"post","i":{i}}}"#) + "\n")
            .collect();
        std::fs::write(&log_path, post_data.as_bytes()).unwrap();

        // Poll until all 10 lines (pre + post) are processed, then cancel.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 10
                || std::time::Instant::now() >= deadline
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sd_run.cancel();
    });

    pipeline.run(&sd_write).expect("pipeline.run failed");

    // Verify that all 10 lines (5 pre-rotation + 5 post-rotation) were processed.
    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 10,
        "expected 10 lines (5 pre-rotation + 5 post-rotation), got {lines_in}"
    );
}

// ---------------------------------------------------------------------------
// 6. Config validation: invalid YAML → clean error message
// ---------------------------------------------------------------------------

/// Verify that malformed YAML returns an error rather than panicking, and that
/// missing required config fields (e.g. `path` for a file input) are caught
/// before any I/O begins.
#[test]
fn test_config_validation_errors() {
    // Malformed YAML.
    let bad_yaml = "input: {type: file, path: [unclosed";
    let result = Config::load_str(bad_yaml);
    assert!(
        result.is_err(),
        "expected parse error for malformed YAML, got Ok"
    );
    let msg = result.unwrap_err().to_string();
    assert!(!msg.is_empty(), "error message should not be empty");

    // Missing required 'path' for a file input.
    let missing_path = r"
input:
  type: file
output:
  type: stdout
";
    let result = Config::load_str(missing_path);
    assert!(
        result.is_err(),
        "expected validation error for missing input path"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("path"),
        "error message should mention 'path', got: {msg}"
    );

    // Missing required 'endpoint' for an OTLP output.
    let dir = tempfile::tempdir().unwrap();
    let dummy = dir.path().join("x.log");
    std::fs::write(&dummy, b"").unwrap();
    let missing_endpoint = format!(
        r"
input:
  type: file
  path: {}
output:
  type: otlp
",
        dummy.display()
    );
    let result = Config::load_str(&missing_endpoint);
    assert!(
        result.is_err(),
        "expected validation error for missing OTLP endpoint"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("endpoint"),
        "error message should mention 'endpoint', got: {msg}"
    );

    // Valid minimal config should load without error.
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("app.log");
    std::fs::write(&log, b"").unwrap();
    let valid = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
        log.display()
    );
    assert!(Config::load_str(&valid).is_ok(), "valid config should load");
}

// ---------------------------------------------------------------------------
// 7. Enrichment: scan JSON → SqlTransform + CsvFileTable JOIN → enriched cols
// ---------------------------------------------------------------------------

/// Verify the enrichment path: scan a set of JSON log lines into an Arrow
/// `RecordBatch`, register a `CsvFileTable` with the `SqlTransform`, execute a
/// SQL JOIN that brings in new columns from the CSV, and confirm the output
/// batch contains the expected enriched column values.
#[test]
fn test_enrichment_join() {
    // Build a RecordBatch from raw JSON using the same scanner the pipeline uses.
    let json_lines = concat!(
        r#"{"service":"auth","message":"login ok"}"#,
        "\n",
        r#"{"service":"orders","message":"order placed"}"#,
        "\n",
        r#"{"service":"auth","message":"login failed"}"#,
        "\n",
    );
    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner
        .scan_detached(bytes::Bytes::from(json_lines.as_bytes().to_vec()))
        .expect("scan failed");
    assert_eq!(batch.num_rows(), 3);

    // Write a CSV enrichment file: service → owning team.
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("teams.csv");
    std::fs::write(&csv_path, b"service,team\nauth,platform\norders,commerce\n").unwrap();

    let csv_table = Arc::new(CsvFileTable::new("teams", &csv_path));
    csv_table.reload().expect("failed to load enrichment CSV");

    // SQL joins the log batch with the enrichment table on the `service` field.
    // Both scanner and CSV columns are addressed via bare names for this
    // single-type dataset.  The alias brings the enriched column into the output.
    let sql = "SELECT l.service, l.message, t.team AS team \
               FROM logs l \
               JOIN teams t ON l.service = t.service";
    let mut transform = SqlTransform::new(sql).expect("SQL parse failed");
    transform
        .add_enrichment_table(csv_table)
        .expect("failed to register enrichment table");

    let result = transform
        .execute_blocking(batch)
        .expect("transform execution failed");

    // All 3 log rows have a matching service in the CSV.
    assert_eq!(result.num_rows(), 3, "expected 3 enriched rows");

    // The output must contain the `team` column from the CSV.
    let schema = result.schema();
    assert!(
        schema.field_with_name("team").is_ok(),
        "expected 'team' column in enriched output; schema: {schema:?}"
    );

    // Spot-check values: both "auth" rows should map to "platform".
    let team_col = result.column_by_name("team").expect("team column missing");
    use arrow::array::StringViewArray;
    // CSV enrichment tables store string columns as Utf8View.
    let team_arr = team_col
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("team column should be DataType::Utf8View");
    let teams: Vec<&str> = team_arr.iter().map(|v| v.unwrap_or("")).collect();
    assert!(
        teams.contains(&"platform"),
        "expected 'platform' in team column; got {teams:?}"
    );
    assert!(
        teams.contains(&"commerce"),
        "expected 'commerce' in team column; got {teams:?}"
    );
}
