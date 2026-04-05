//! Compliance test framework for logfwd pipeline correctness.
//!
//! Tests generate NDJSON with monotonically increasing `sequence_id` fields,
//! run them through the full pipeline (file tailing → scanner → SQL transform
//! → output), and verify that no lines are lost, duplicated, or reordered.
//!
//! The framework consists of:
//!   - **Generator**: writes NDJSON test data with sequence IDs
//!   - **CaptureSink**: a [`Sink`] that captures RecordBatches in memory
//!   - **Verifier**: extracts sequence IDs from captured batches and checks
//!     for gaps, duplicates, and ordering violations
//!   - **Pipeline helper**: builds a pipeline from YAML config with an
//!     injected CaptureSink

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, AsArray, Int64Array};
use arrow::record_batch::RecordBatch;

use logfwd::pipeline::Pipeline;
use logfwd_config::Config;
use logfwd_output::BatchMetadata;
use logfwd_output::sink::{SendResult, Sink};
use logfwd_test_utils::{generate_json_lines, test_meter};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// CaptureSink (thread-safe, usable from integration tests)
// ---------------------------------------------------------------------------

/// A [`Sink`] that stores all received `RecordBatch`es in a shared
/// `Arc<Mutex<Vec<RecordBatch>>>` for post-run verification.
struct CaptureSink {
    name: String,
    batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl CaptureSink {
    fn new(name: &str) -> (Self, Arc<Mutex<Vec<RecordBatch>>>) {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let sink = CaptureSink {
            name: name.to_string(),
            batches: Arc::clone(&batches),
        };
        (sink, batches)
    }
}

impl Sink for CaptureSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        self.batches
            .lock()
            .expect("CaptureSink lock poisoned")
            .push(batch.clone());
        Box::pin(async { SendResult::Ok })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// ComplianceReport + Verifier
// ---------------------------------------------------------------------------

/// Summary of compliance verification results.
#[derive(Debug)]
struct ComplianceReport {
    lines_generated: u64,
    lines_received: u64,
    gaps: Vec<u64>,
    duplicates: Vec<u64>,
    out_of_order: u64,
    malformed: u64,
}

impl ComplianceReport {
    fn is_perfect(&self) -> bool {
        self.gaps.is_empty()
            && self.duplicates.is_empty()
            && self.out_of_order == 0
            && self.malformed == 0
            && self.lines_received == self.lines_generated
    }
}

impl std::fmt::Display for ComplianceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "generated={}, received={}, gaps={}, duplicates={}, out_of_order={}, malformed={}",
            self.lines_generated,
            self.lines_received,
            self.gaps.len(),
            self.duplicates.len(),
            self.out_of_order,
            self.malformed,
        )?;
        if !self.gaps.is_empty() {
            let show: Vec<String> = self.gaps.iter().take(20).map(ToString::to_string).collect();
            write!(f, "\n  first gaps: [{}]", show.join(", "))?;
        }
        if !self.duplicates.is_empty() {
            let show: Vec<String> = self
                .duplicates
                .iter()
                .take(20)
                .map(ToString::to_string)
                .collect();
            write!(f, "\n  first duplicates: [{}]", show.join(", "))?;
        }
        Ok(())
    }
}

/// Extract sequence IDs from captured batches and check correctness.
///
/// Looks for the `sequence_id_int` column (the scanner's typed naming
/// convention for integer fields). Optionally filters by `source_id` when
/// `source_id_str` is present in the schema and `source_id` is not empty.
fn verify_batches(
    batches: &[RecordBatch],
    expected_count: usize,
    source_id: &str,
) -> ComplianceReport {
    let mut all_ids: Vec<i64> = Vec::new();

    for batch in batches {
        // Find the sequence_id column (scanner produces sequence_id_int for integers).
        let seq_col = batch.column_by_name("sequence_id");
        if seq_col.is_none() {
            // Batch has no sequence_id_int column; all rows are malformed
            // from the compliance perspective.
            continue;
        }
        let seq_col = seq_col.unwrap();
        let seq_arr = seq_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sequence_id should be Int64");

        // Optional source_id filter.
        let source_filter: Option<Vec<bool>> = if source_id.is_empty() {
            None
        } else {
            batch.column_by_name("source_id").map(|col| {
                (0..batch.num_rows())
                    .map(|row| {
                        if col.is_null(row) {
                            return false;
                        }
                        let val = match col.data_type() {
                            arrow::datatypes::DataType::Utf8 => col.as_string::<i32>().value(row),
                            arrow::datatypes::DataType::Utf8View => col.as_string_view().value(row),
                            _ => "",
                        };
                        val == source_id
                    })
                    .collect()
            })
        };

        for row in 0..batch.num_rows() {
            // Apply source filter if present.
            if let Some(ref filter) = source_filter
                && !filter[row]
            {
                continue;
            }

            if seq_arr.is_null(row) {
                continue;
            }
            all_ids.push(seq_arr.value(row));
        }
    }

    let lines_received = all_ids.len() as u64;

    // Check for out-of-order: count non-monotonic transitions.
    let mut out_of_order: u64 = 0;
    for window in all_ids.windows(2) {
        if window[1] < window[0] {
            out_of_order += 1;
        }
    }

    // Check for duplicates.
    let mut seen = HashSet::with_capacity(all_ids.len());
    let mut duplicates = Vec::new();
    for &id in &all_ids {
        if !seen.insert(id) {
            duplicates.push(id as u64);
        }
    }
    duplicates.sort_unstable();
    duplicates.dedup();

    // Check for gaps: which IDs from 0..expected_count are missing?
    let expected_set: HashSet<i64> = (0..expected_count as i64).collect();
    let received_set: HashSet<i64> = all_ids.iter().copied().collect();
    let mut gaps: Vec<u64> = expected_set
        .difference(&received_set)
        .map(|&id| id as u64)
        .collect();
    gaps.sort_unstable();

    ComplianceReport {
        lines_generated: expected_count as u64,
        lines_received,
        gaps,
        duplicates,
        out_of_order,
        malformed: 0,
    }
}

// ---------------------------------------------------------------------------
// Pipeline helper
// ---------------------------------------------------------------------------

/// Build a pipeline from YAML config, inject a CaptureSink, run for the
/// specified duration, and return the captured RecordBatches.
fn run_compliance_pipeline(yaml: &str, timeout: Duration) -> Vec<RecordBatch> {
    let config = Config::load_str(yaml).expect("failed to parse YAML config");
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
        .expect("failed to build pipeline");

    let (sink, captured) = CaptureSink::new("compliance-capture");
    let mut pipeline = pipeline.with_sink(Box::new(sink));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(timeout);
        sd.cancel();
    });

    pipeline.run(&shutdown).expect("pipeline.run failed");

    let batches = captured
        .lock()
        .expect("CaptureSink lock poisoned after run");
    batches.clone()
}

// ---------------------------------------------------------------------------
// Tier 1 tests
// ---------------------------------------------------------------------------

/// Happy path: generate 10,000 lines, run through SELECT *, verify zero loss.
#[test]
fn compliance_happy_path() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("app.log");
    let count = 10_000;

    generate_json_lines(&log_path, count, "src1");

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

    let batches = run_compliance_pipeline(&yaml, Duration::from_millis(2000));
    let report = verify_batches(&batches, count, "");

    assert!(
        report.is_perfect(),
        "happy path compliance failed: {report}"
    );
}

/// Multi-source: 3 files with 5,000 lines each, different source_ids.
/// Verify per-source completeness.
#[test]
fn compliance_multi_source() {
    let dir = tempfile::tempdir().unwrap();
    let sources = ["src_a", "src_b", "src_c"];
    let per_source = 5_000;

    let mut input_yaml_parts = Vec::new();
    for source in &sources {
        let log_path = dir.path().join(format!("{source}.log"));
        generate_json_lines(&log_path, per_source, source);
        input_yaml_parts.push(format!(
            "      - type: file\n        path: {}\n        format: json",
            log_path.display()
        ));
    }

    let yaml = format!(
        r"
pipelines:
  default:
    inputs:
{}
    outputs:
      - type: stdout
        format: json
",
        input_yaml_parts.join("\n")
    );

    let batches = run_compliance_pipeline(&yaml, Duration::from_millis(3000));

    for source in &sources {
        let report = verify_batches(&batches, per_source, source);
        assert!(
            report.gaps.is_empty(),
            "multi-source compliance failed for {source}: {report}"
        );
        assert!(
            report.duplicates.is_empty(),
            "multi-source has duplicates for {source}: {report}"
        );
    }
}

/// Transform filter: generate 10,000 lines alternating INFO/ERROR, filter
/// to ERROR only, verify the ERROR subsequence has zero gaps.
#[test]
fn compliance_transform_filter() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("mixed.log");
    let count = 10_000;

    generate_json_lines(&log_path, count, "src1");

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

    let batches = run_compliance_pipeline(&yaml, Duration::from_millis(2000));

    // ERROR lines are the odd-numbered sequence_ids (1, 3, 5, ...).
    let mut all_ids: Vec<i64> = Vec::new();
    for batch in &batches {
        if let Some(seq_col) = batch.column_by_name("sequence_id") {
            let seq_arr = seq_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sequence_id should be Int64");
            for row in 0..batch.num_rows() {
                if !seq_arr.is_null(row) {
                    all_ids.push(seq_arr.value(row));
                }
            }
        }
    }

    // Expected: all odd sequence_ids from 0..count.
    let expected_odds: HashSet<i64> = (0..count as i64).filter(|i| i % 2 != 0).collect();
    let received: HashSet<i64> = all_ids.iter().copied().collect();

    let missing: Vec<i64> = expected_odds.difference(&received).copied().collect();
    let unexpected: Vec<i64> = received.difference(&expected_odds).copied().collect();

    assert!(
        missing.is_empty(),
        "filter compliance: missing {} ERROR sequence_ids (first 10: {:?})",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
    assert!(
        unexpected.is_empty(),
        "filter compliance: received {} unexpected (non-ERROR) sequence_ids (first 10: {:?})",
        unexpected.len(),
        &unexpected[..unexpected.len().min(10)]
    );
    assert_eq!(
        all_ids.len(),
        count / 2,
        "expected {} ERROR rows, got {}",
        count / 2,
        all_ids.len()
    );
}

/// Transform select: generate 10,000 lines, project only sequence_id_int
/// and message_str. Verify correct columns and zero gaps.
#[test]
fn compliance_transform_select() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("select.log");
    let count = 10_000;

    generate_json_lines(&log_path, count, "src1");

    let yaml = format!(
        r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT sequence_id, message FROM logs"
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );

    let batches = run_compliance_pipeline(&yaml, Duration::from_millis(2000));

    // Verify schema: only the projected columns should be present.
    for batch in &batches {
        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            col_names.contains(&"sequence_id"),
            "expected sequence_id column, got: {col_names:?}"
        );
        assert!(
            col_names.contains(&"message"),
            "expected message column, got: {col_names:?}"
        );
        assert_eq!(
            col_names.len(),
            2,
            "expected exactly 2 columns, got: {col_names:?}"
        );
    }

    let report = verify_batches(&batches, count, "");
    assert!(report.gaps.is_empty(), "select compliance: {report}");
    assert!(
        report.duplicates.is_empty(),
        "select compliance has duplicates: {report}"
    );
}

/// Large batch: 100,000 lines in a single burst. Verify zero gaps.
#[test]
fn compliance_large_batch() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("large.log");
    let count = 100_000;

    generate_json_lines(&log_path, count, "src1");

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

    let batches = run_compliance_pipeline(&yaml, Duration::from_millis(5000));
    let report = verify_batches(&batches, count, "");

    assert!(
        report.gaps.is_empty(),
        "large batch compliance: {} gaps out of {count} lines. {report}",
        report.gaps.len()
    );
    assert!(
        report.duplicates.is_empty(),
        "large batch compliance has duplicates: {report}"
    );
    assert_eq!(
        report.lines_received, count as u64,
        "large batch: expected {count} lines, received {}. {report}",
        report.lines_received
    );
}
