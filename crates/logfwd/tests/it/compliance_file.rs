//! Tier 2 compliance tests: file handling scenarios.
//!
//! These tests exercise file rotation, truncation, and discovery through the
//! full pipeline (file tailer -> parser -> scanner -> transform -> output).
//! Each test writes sequenced JSON lines, manipulates files while the pipeline
//! runs on a background thread, then verifies the pipeline processed the
//! expected number of lines via metrics.

use std::fs;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use logfwd::pipeline::Pipeline;
use logfwd_config::Config;
use logfwd_diagnostics::diagnostics::PipelineMetrics;
use logfwd_test_utils::test_meter;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate `count` JSON log lines starting at sequence number `start`.
/// Each line: {"seq":<n>,"msg":"line <n>"}
fn generate_lines(start: usize, count: usize) -> String {
    let mut buf = String::with_capacity(count * 40);
    for i in start..start + count {
        buf.push_str(&format!(r#"{{"seq":{},"msg":"line {}"}}"#, i, i));
        buf.push('\n');
    }
    buf
}

/// Build a pipeline from a YAML config string.
fn build_pipeline(yaml: &str) -> Pipeline {
    let config = Config::load_str(yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap()
}

/// Build a simple pipeline config YAML for a single file input.
fn file_pipeline_yaml(log_path: &std::path::Path) -> String {
    format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: "null"
",
        log_path.display()
    )
}

/// Keep a generous wait budget for file-compliance tests.
/// Coverage and busy CI hosts can introduce significant scheduling jitter.
fn wait_timeout() -> Duration {
    Duration::from_secs(90)
}

#[derive(Clone, Copy, Debug)]
struct LineWaitSnapshot {
    expected: usize,
    observed: u64,
    elapsed: Duration,
    timeout: Duration,
    polls: u64,
}

fn observed_lines(metrics: &PipelineMetrics) -> u64 {
    metrics.transform_in.lines_total.load(Ordering::Relaxed)
}

fn wait_for_line_count(
    metrics: &PipelineMetrics,
    expected: usize,
    timeout: Duration,
) -> Result<LineWaitSnapshot, LineWaitSnapshot> {
    let started = Instant::now();
    let mut polls = 0;
    loop {
        polls += 1;
        let observed = observed_lines(metrics);
        let snapshot = LineWaitSnapshot {
            expected,
            observed,
            elapsed: started.elapsed(),
            timeout,
            polls,
        };
        if observed >= expected as u64 {
            return Ok(snapshot);
        }
        if snapshot.elapsed >= timeout {
            return Err(snapshot);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn panic_line_wait_timeout(phase: &str, snapshot: LineWaitSnapshot) -> ! {
    panic!(
        "{phase}: timed out waiting for {} lines; observed={} elapsed_ms={} timeout_ms={} polls={}",
        snapshot.expected,
        snapshot.observed,
        snapshot.elapsed.as_millis(),
        snapshot.timeout.as_millis(),
        snapshot.polls
    );
}

fn wait_for_ready_lines(
    shutdown: &CancellationToken,
    metrics: &PipelineMetrics,
    expected: usize,
    timeout: Duration,
    phase: &str,
) -> LineWaitSnapshot {
    match wait_for_line_count(metrics, expected, timeout) {
        Ok(snapshot) => snapshot,
        Err(snapshot) => {
            shutdown.cancel();
            panic_line_wait_timeout(phase, snapshot);
        }
    }
}

/// Build a pipeline config YAML for a glob input pattern.
///
/// Uses a short `glob_rescan_interval_ms` so tests don't wait 5 seconds for
/// the default rescan timer to fire.
fn glob_pipeline_yaml(pattern: &str) -> String {
    format!(
        r#"
input:
  type: file
  path: "{}"
  format: json
  glob_rescan_interval_ms: 50
output:
  type: "null"
"#,
        pattern
    )
}

/// Run a pipeline on a background thread.
///
/// Returns the shutdown token, a metrics handle for polling mid-run, and the
/// join handle. The metrics arc is cloned before the pipeline is moved into
/// the thread so callers can poll counters without waiting for `join()`.
fn run_pipeline_background(
    mut pipeline: Pipeline,
) -> (
    CancellationToken,
    Arc<PipelineMetrics>,
    std::thread::JoinHandle<Pipeline>,
) {
    let metrics = Arc::clone(pipeline.metrics());
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || {
        pipeline.run(&sd).expect("pipeline.run failed");
        pipeline
    });
    (shutdown, metrics, handle)
}

/// Poll `metrics.transform_in.lines_total` until it reaches `expected` or the
/// timeout passes, then cancel the pipeline.
fn wait_for_lines_and_cancel(
    shutdown: &CancellationToken,
    metrics: &PipelineMetrics,
    expected: usize,
    timeout: Duration,
    phase: &str,
) {
    let result = wait_for_line_count(metrics, expected, timeout);
    shutdown.cancel();
    if let Err(snapshot) = result {
        panic_line_wait_timeout(phase, snapshot);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Simulate logrotate "create" style rotation:
/// 1. Write 5000 lines to test.log
/// 2. Start pipeline
/// 3. Rename test.log -> test.log.1
/// 4. Create new test.log with 5000 more lines
/// 5. Verify all 10000 lines received
#[test]
fn compliance_file_rotate_create() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    let rotated_path = dir.path().join("test.log.1");

    // Write initial 5000 lines.
    fs::write(&log_path, generate_lines(0, 5000)).unwrap();

    let yaml = file_pipeline_yaml(&log_path);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Wait for initial 5000 lines to be ingested before rotating.
    wait_for_ready_lines(
        &shutdown,
        &metrics,
        5000,
        wait_timeout(),
        "initial ingest before create-style rotation",
    );

    // Simulate logrotate "create" style.
    fs::rename(&log_path, &rotated_path).unwrap();
    {
        let mut f = fs::File::create(&log_path).unwrap();
        f.write_all(generate_lines(5000, 5000).as_bytes()).unwrap();
        f.flush().unwrap();
    }

    // Poll until all 10000 lines are processed or 5s safety deadline.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        10000,
        wait_timeout(),
        "all lines after create-style rotation",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 10000,
        "expected 10000 lines through transform after create-style rotation, got {lines_in}"
    );
}

/// Simulate logrotate "copytruncate" style rotation:
/// 1. Write 5000 lines to test.log
/// 2. Start pipeline
/// 3. Copy test.log -> test.log.1, truncate test.log to 0
/// 4. Write 5000 more lines to test.log
/// 5. Verify all 10000 lines received
///
/// NOTE: copytruncate has a known race condition where data written between
/// the copy and the truncate can be lost. This test may fail in CI.
#[test]
#[ignore] // Known issue: copytruncate can lose data between copy and truncate
fn compliance_file_rotate_copytruncate() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    let backup_path = dir.path().join("test.log.1");

    // Write initial 5000 lines.
    fs::write(&log_path, generate_lines(0, 5000)).unwrap();

    let yaml = file_pipeline_yaml(&log_path);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Wait for initial 5000 lines to be ingested before rotating.
    wait_for_ready_lines(
        &shutdown,
        &metrics,
        5000,
        wait_timeout(),
        "initial ingest before copytruncate rotation",
    );

    // Simulate logrotate "copytruncate" style.
    fs::copy(&log_path, &backup_path).unwrap();
    // Truncate the original file.
    fs::File::create(&log_path).unwrap();
    // Brief pause to let the tailer detect the truncation before writing new data.
    std::thread::sleep(Duration::from_millis(200));

    // Write new data (sequence continues from 5000).
    {
        let mut f = fs::OpenOptions::new().write(true).open(&log_path).unwrap();
        f.write_all(generate_lines(5000, 5000).as_bytes()).unwrap();
        f.flush().unwrap();
    }

    // Poll until all 10000 lines are processed or 5s safety deadline.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        10000,
        wait_timeout(),
        "all lines after copytruncate rotation",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 10000,
        "expected 10000 lines through transform after copytruncate rotation, got {lines_in}"
    );
}

/// Truncate a file mid-stream and write new data:
/// 1. Write 500 lines
/// 2. Start pipeline, wait for processing
/// 3. Truncate file to 0 bytes (using set_len, preserving inode)
/// 4. Write 500 new lines (seq starts at 500)
/// 5. Verify no data is lost (>= 1000 lines)
///
/// When the file is truncated and rewritten, the tailer may detect BOTH
/// a fingerprint change (triggering the rotation path which drains the old
/// fd) AND see the new content via the re-opened file. This can cause the
/// post-truncation data to be read twice. This duplication is acceptable:
/// the tailer's contract is "no data loss", not "exactly once delivery".
/// Deduplication is the downstream consumer's responsibility.
#[test]
fn compliance_file_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("trunc.log");

    // Write initial 500 lines.
    fs::write(&log_path, generate_lines(0, 500)).unwrap();

    let yaml = file_pipeline_yaml(&log_path);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Wait for initial 500 lines to be ingested before truncating.
    wait_for_ready_lines(
        &shutdown,
        &metrics,
        500,
        wait_timeout(),
        "initial ingest before truncation",
    );

    // Truncate the file in-place (same inode) and write new data.
    {
        let f = fs::OpenOptions::new().write(true).open(&log_path).unwrap();
        f.set_len(0).unwrap();
    }
    // Brief pause to let the tailer detect the truncation before writing new data.
    std::thread::sleep(Duration::from_millis(100));

    {
        let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(generate_lines(500, 500).as_bytes()).unwrap();
        f.flush().unwrap();
    }

    // Poll until >= 1000 lines processed or 5s safety deadline.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        1000,
        wait_timeout(),
        "all lines after truncation",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    // All original 500 + all new 500 lines must be received. The tailer may
    // deliver post-truncation lines twice (rotation drain + new fd), so the
    // count can exceed 1000.
    assert!(
        lines_in >= 1000,
        "expected at least 1000 lines through transform after truncation, got {lines_in}"
    );
}

/// Delete a file and recreate it:
/// 1. Write 1000 lines
/// 2. Start pipeline, wait for processing
/// 3. Delete the file
/// 4. Recreate with 1000 new lines
/// 5. Verify lines from recreated file received
#[test]
fn compliance_file_delete_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("recreate.log");

    // Write initial 1000 lines.
    fs::write(&log_path, generate_lines(0, 1000)).unwrap();

    let yaml = file_pipeline_yaml(&log_path);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Wait for initial 1000 lines to be ingested before deleting.
    wait_for_ready_lines(
        &shutdown,
        &metrics,
        1000,
        wait_timeout(),
        "initial ingest before delete/recreate",
    );

    // Delete the file.
    fs::remove_file(&log_path).unwrap();
    // Brief pause to let the tailer detect the deletion before recreating.
    std::thread::sleep(Duration::from_millis(100));

    // Recreate the file with new data.
    {
        let mut f = fs::File::create(&log_path).unwrap();
        f.write_all(generate_lines(1000, 1000).as_bytes()).unwrap();
        f.flush().unwrap();
    }

    // Poll until >= 2000 lines processed or 5s safety deadline.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        2000,
        wait_timeout(),
        "all lines after delete/recreate",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    // Both the original 1000 lines and the recreated file's 1000 lines
    // should be read. On some platforms (macOS FSEvents) the tailer may
    // re-read the original data after inode change, producing >2000.
    // What matters: no data loss (>= 2000), and bounded (no runaway).
    assert!(
        lines_in >= 2000,
        "expected at least 2000 lines (original + recreated) through transform, got {lines_in}"
    );
    assert!(
        lines_in <= 4000,
        "too many lines — possible runaway re-read: {lines_in}"
    );
}

/// File grows continuously while pipeline is running:
/// 1. Write 100 initial lines
/// 2. Start pipeline
/// 3. Append 100 lines every 50ms for 2 seconds (40 batches = 4000 lines)
/// 4. Verify all 4100 lines received
#[test]
fn compliance_file_grows_while_running() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("growing.log");

    // Write initial 100 lines.
    fs::write(&log_path, generate_lines(0, 100)).unwrap();

    let yaml = file_pipeline_yaml(&log_path);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Wait for the pipeline to start tailing before appending.
    wait_for_ready_lines(
        &shutdown,
        &metrics,
        100,
        wait_timeout(),
        "initial ingest before continuous appends",
    );

    // Append 100 lines every 50ms for 2 seconds (40 iterations).
    let log_path_clone = log_path.clone();
    let writer_handle = std::thread::spawn(move || {
        for batch in 0..40 {
            let start = 100 + batch * 100;
            let data = generate_lines(start, 100);
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(&log_path_clone)
                .unwrap();
            f.write_all(data.as_bytes()).unwrap();
            f.flush().unwrap();
            std::thread::sleep(Duration::from_millis(50));
        }
    });

    writer_handle.join().expect("writer thread panicked");

    // Poll until all 4100 lines are processed or 5s safety deadline.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        4100,
        wait_timeout(),
        "all lines after continuous appends",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 4100,
        "expected at least 4100 lines through transform for continuous growth, got {lines_in}"
    );
}

/// Glob discovers new files created after the pipeline starts:
/// 1. Create test1.log with 1000 lines
/// 2. Start pipeline with glob "*.log"
/// 3. Wait for test1.log to be processed
/// 4. Create test2.log with 1000 lines
/// 5. Wait for glob rescan + processing
/// 6. Verify all 2000 lines received
#[test]
fn compliance_glob_new_files() {
    let dir = tempfile::tempdir().unwrap();

    let log1_path = dir.path().join("test1.log");
    fs::write(&log1_path, generate_lines(0, 1000)).unwrap();

    let pattern = format!("{}/*.log", dir.path().display());
    let yaml = glob_pipeline_yaml(&pattern);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Wait for test1.log to be ingested before creating test2.log.
    wait_for_ready_lines(
        &shutdown,
        &metrics,
        1000,
        wait_timeout(),
        "first glob-discovered file before creating second file",
    );

    // Create a second log file.
    let log2_path = dir.path().join("test2.log");
    {
        let mut f = fs::File::create(&log2_path).unwrap();
        f.write_all(generate_lines(1000, 1000).as_bytes()).unwrap();
        f.flush().unwrap();
    }

    // Poll until both files are processed or 3s safety deadline.
    // glob_rescan_interval_ms is set to 50ms in glob_pipeline_yaml(), so the
    // new file is discovered quickly rather than waiting the default 5s.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        2000,
        wait_timeout(),
        "all lines after glob-discovered second file",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 2000,
        "expected 2000 lines through transform (1000 from each file), got {lines_in}"
    );
}

/// A file that ends without a trailing newline must not drop its last record.
///
/// Regression test for: last line silently dropped when input file has no
/// trailing newline.
#[test]
fn compliance_file_no_trailing_newline() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("no-newline.log");

    // Write 3 complete lines followed by a record with NO trailing newline.
    let mut content = generate_lines(0, 3);
    content.push_str(r#"{"seq":3,"msg":"no newline"}"#); // deliberately no '\n'
    fs::write(&log_path, &content).unwrap();

    let yaml = file_pipeline_yaml(&log_path);
    let pipeline = build_pipeline(&yaml);
    let (shutdown, metrics, handle) = run_pipeline_background(pipeline);

    // Poll until all 4 lines are processed or 3s safety deadline.
    // The EndOfFile event flushes the partial line without a trailing newline.
    wait_for_lines_and_cancel(
        &shutdown,
        &metrics,
        4,
        wait_timeout(),
        "file without trailing newline",
    );
    let pipeline = handle.join().expect("pipeline thread panicked");

    let lines_in = pipeline
        .metrics()
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 4,
        "expected 4 lines through transform (3 complete + 1 without trailing newline), got {lines_in}"
    );
}
