//! Shared helpers for logfwd correctness / integration tests.
//!
//! Each test module under `tests/` imports from this crate to:
//!
//! * Run the format-parser → scanner → serializer pipeline on raw bytes.
//! * Produce a stable, deterministic text snapshot of an Arrow RecordBatch.
//! * Compare that snapshot against an on-disk expected file, or write it on
//!   the first run / when `UPDATE_SNAPSHOTS=1` is set.

use std::io::Write as _;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use logfwd_core::format::FormatParser;
use logfwd_core::scanner::{ScanConfig, Scanner};
use logfwd_output::{BatchMetadata, StdoutFormat, StdoutSink};

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

/// Result of running one input through the format → scan pipeline.
pub struct PipelineResult {
    /// Newline-delimited JSON lines produced by the format parser.
    pub parsed_lines: Vec<u8>,
    /// Number of JSON lines the format parser emitted.
    pub line_count: usize,
    /// The Arrow RecordBatch produced by the scanner.
    pub batch: RecordBatch,
}

/// Run `input` bytes through `parser` then through a `Scanner` with the
/// default config (`SELECT *`, keep `_raw`).
pub fn run_pipeline(parser: &mut dyn FormatParser, input: &[u8]) -> PipelineResult {
    let mut parsed_lines: Vec<u8> = Vec::new();
    let line_count = parser.process(input, &mut parsed_lines);

    let mut scanner = Scanner::new(ScanConfig::default(), line_count.max(1));
    let batch = scanner.scan(&parsed_lines);

    PipelineResult {
        parsed_lines,
        line_count,
        batch,
    }
}

// ---------------------------------------------------------------------------
// Batch serialization
// ---------------------------------------------------------------------------

fn dummy_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: vec![],
        observed_time_ns: 0,
    }
}

/// Serialize every row of `batch` to newline-delimited JSON, then sort the
/// lines so the output is stable regardless of insertion order.
pub fn batch_to_sorted_json_lines(batch: &RecordBatch) -> String {
    let json = batch_to_json_lines(batch);
    let mut rows: Vec<&str> = json.lines().collect();
    rows.sort();
    rows.join("\n")
}

/// Serialize `batch` in row order to newline-delimited JSON.
pub fn batch_to_json_lines(batch: &RecordBatch) -> String {
    let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
    let mut buf: Vec<u8> = Vec::new();
    sink.write_batch_to(batch, &dummy_metadata(), &mut buf)
        .expect("write_batch_to should not fail");
    String::from_utf8(buf).expect("valid UTF-8 from StdoutSink")
}

// ---------------------------------------------------------------------------
// Schema description
// ---------------------------------------------------------------------------

/// Return a stable, human-readable description of the batch schema:
/// one `field_name: type` per line, alphabetically sorted.
pub fn schema_summary(batch: &RecordBatch) -> String {
    let mut fields: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
        .collect();
    fields.sort();
    fields.join("\n")
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

/// Directory that holds the expected snapshot files checked into the repo.
pub fn snapshots_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
}

/// Assert that `actual` matches the content of `snapshots_dir()/<name>`.
///
/// If the snapshot file does not exist yet, it is created automatically
/// (first-run seeding).  Set the environment variable `UPDATE_SNAPSHOTS=1`
/// to force-overwrite an existing snapshot (useful after intentional changes).
pub fn assert_snapshot(name: &str, actual: &str) {
    let path = snapshots_dir().join(name);
    let update = std::env::var("UPDATE_SNAPSHOTS").as_deref() == Ok("1");

    if !path.exists() || update {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(actual.as_bytes()).unwrap();
        // Don't fail on first-run seeding — just wrote the baseline.
        return;
    }

    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read snapshot {}: {e}", path.display()));

    if actual != expected {
        eprintln!("\n=== SNAPSHOT MISMATCH: {} ===", name);
        eprintln!("--- expected (first 40 lines) ---");
        for line in expected.lines().take(40) {
            eprintln!("  {line}");
        }
        eprintln!("--- actual (first 40 lines) ---");
        for line in actual.lines().take(40) {
            eprintln!("  {line}");
        }
        eprintln!("(run with UPDATE_SNAPSHOTS=1 to accept new output)");
        panic!("snapshot mismatch for {name}");
    }
}

// ---------------------------------------------------------------------------
// Line counting helpers
// ---------------------------------------------------------------------------

/// Count non-empty lines in a byte slice (matching how RawParser and
/// JsonParser count lines — blank lines are skipped).
pub fn count_nonempty_lines(bytes: &[u8]) -> usize {
    bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .count()
}
