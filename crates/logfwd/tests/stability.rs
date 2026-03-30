//! Long-running stability tests for the async pipeline.
//!
//! These tests are marked `#[ignore]` and only run in nightly CI or
//! when explicitly requested:
//!
//!   cargo test -p logfwd --test stability -- --ignored
//!
//! Each test runs the pipeline for an extended duration under various
//! stress conditions and asserts on resource usage (memory, fd count)
//! and data integrity (no lost lines, monotonic metrics).

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use logfwd::pipeline::Pipeline;
use logfwd_config::Config;
use logfwd_output::{BatchMetadata, OutputSink};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("test")
}

/// Generate `count` NDJSON lines starting at sequence `start`.
fn generate_lines(start: usize, count: usize) -> String {
    let mut buf = String::with_capacity(count * 80);
    for i in start..start + count {
        buf.push_str(&format!(
            r#"{{"seq":{},"level":"INFO","msg":"stability test line","ts":"2026-01-01T00:00:00Z"}}"#,
            i
        ));
        buf.push('\n');
    }
    buf
}

/// Get the current process RSS in bytes.
/// Linux: reads /proc/self/statm (current RSS, not peak).
/// macOS: uses rusage (reports peak RSS, not current — noted in tests).
fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let fields: Vec<&str> = statm.split_whitespace().collect();
            if let Some(rss_pages) = fields.get(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 };
                    return Some(pages * page_size);
                }
            }
        }
        None
    }
    #[cfg(target_os = "macos")]
    {
        unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
                Some(usage.ru_maxrss as u64)
            } else {
                None
            }
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

/// Sample RSS `count` times at `interval` while a future runs.
/// Returns the samples for trend analysis.
async fn sample_rss_during<F: std::future::Future>(
    fut: F,
    interval: Duration,
) -> (F::Output, Vec<u64>) {
    let samples = Arc::new(std::sync::Mutex::new(Vec::new()));
    let samples_clone = samples.clone();

    let sampler = tokio::spawn(async move {
        loop {
            if let Some(rss) = process_rss_bytes() {
                samples_clone.lock().unwrap().push(rss);
            }
            tokio::time::sleep(interval).await;
        }
    });

    let result = fut.await;
    sampler.abort();
    let _ = sampler.await; // wait for abort to complete

    let samples = samples.lock().unwrap().clone();
    (result, samples)
}

/// Count open file descriptors for the current process.
#[cfg(unix)]
fn open_fd_count() -> usize {
    let fd_dir = format!("/proc/{}/fd", std::process::id());
    if let Ok(entries) = std::fs::read_dir(&fd_dir) {
        entries.count()
    } else {
        #[cfg(target_os = "macos")]
        {
            let output = std::process::Command::new("lsof")
                .arg("-p")
                .arg(std::process::id().to_string())
                .output();
            match output {
                Ok(o) => {
                    let s = String::from_utf8_lossy(&o.stdout);
                    s.lines().count().saturating_sub(1)
                }
                Err(_) => 0,
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            0
        }
    }
}

/// Compute the slope of a linear regression on the samples.
/// Positive slope = growing, negative = shrinking.
/// Returns bytes-per-sample growth rate.
fn rss_growth_rate(samples: &[u64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let n = samples.len() as f64;
    let sum_x: f64 = (0..samples.len()).map(|i| i as f64).sum();
    let sum_y: f64 = samples.iter().map(|&s| s as f64).sum();
    let sum_xy: f64 = samples
        .iter()
        .enumerate()
        .map(|(i, &s)| i as f64 * s as f64)
        .sum();
    let sum_x2: f64 = (0..samples.len()).map(|i| (i as f64).powi(2)).sum();

    (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
}

fn build_pipeline(log_path: &std::path::Path) -> Pipeline {
    let yaml = format!(
        r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );
    let config = Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap()
}

// ---------------------------------------------------------------------------
// Test sinks
// ---------------------------------------------------------------------------

/// Tracks received sequence numbers for gap/duplicate/content verification.
#[derive(Clone)]
struct ReceivedData {
    /// All seq values received (may contain duplicates if bug exists).
    seqs: Arc<std::sync::Mutex<Vec<i64>>>,
    rows: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
}

impl ReceivedData {
    fn new() -> Self {
        Self {
            seqs: Arc::new(std::sync::Mutex::new(Vec::new())),
            rows: Arc::new(AtomicU64::new(0)),
            batches: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Verify no gaps, no duplicates, all seqs in expected_start..expected_start+expected_count.
    fn verify_complete(&self, expected_count: u64) {
        let seqs = self.seqs.lock().unwrap();
        let mut sorted: Vec<i64> = seqs.clone();
        sorted.sort();
        sorted.dedup();

        // Find the actual range of seqs received.
        let min_seq = sorted.first().copied().unwrap_or(0);
        let max_seq = sorted.last().copied().unwrap_or(0);

        let total = seqs.len() as u64;
        let unique = sorted.len() as u64;
        let duplicates = total - unique;

        assert_eq!(
            duplicates, 0,
            "found {duplicates} duplicate seq values out of {total} total"
        );

        // Check: we got exactly expected_count unique values and no gaps
        // within the received range.
        assert_eq!(
            unique, expected_count,
            "expected {expected_count} unique seqs, got {unique} (range {min_seq}..={max_seq})"
        );

        // Verify contiguous: max - min + 1 should equal count.
        if unique > 0 {
            let span = (max_seq - min_seq + 1) as u64;
            assert_eq!(
                span, unique,
                "seqs are not contiguous: range {min_seq}..={max_seq} (span {span}) but only {unique} unique values"
            );
        }
    }

    /// Verify no duplicates, and return how many unique seqs we got.
    /// Less strict than verify_complete — doesn't require all seqs present.
    fn verify_no_duplicates(&self) -> u64 {
        let seqs = self.seqs.lock().unwrap();
        let mut sorted: Vec<i64> = seqs.clone();
        sorted.sort();

        let total = seqs.len() as u64;
        sorted.dedup();
        let unique = sorted.len() as u64;
        let duplicates = total - unique;

        assert_eq!(
            duplicates, 0,
            "found {duplicates} duplicate seq values out of {total} total"
        );

        unique
    }
}

/// Sink that extracts seq_int values from each batch for verification.
struct VerifyingSink {
    data: ReceivedData,
}

impl VerifyingSink {
    fn new(data: ReceivedData) -> Self {
        Self { data }
    }
}

impl OutputSink for VerifyingSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.data
            .rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.data.batches.fetch_add(1, Ordering::Relaxed);

        // Extract seq_int column values.
        if let Some(idx) = batch.schema().index_of("seq_int").ok() {
            let col = batch.column(idx);
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                let mut seqs = self.data.seqs.lock().unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        seqs.push(arr.value(i));
                    }
                }
            }
        }

        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &str {
        "verifying"
    }
}

/// Sink that applies backpressure by sleeping on every Nth batch.
/// Also captures seq values for content verification.
struct BackpressureSink {
    data: ReceivedData,
    slow_hits: Arc<AtomicU64>,
    calls: u64,
    slow_every_n: u64,
    slow_duration: Duration,
}

impl BackpressureSink {
    fn new(
        data: ReceivedData,
        slow_every_n: u64,
        slow_duration: Duration,
    ) -> (Self, Arc<AtomicU64>) {
        let slow_hits = Arc::new(AtomicU64::new(0));
        (
            Self {
                data,
                slow_hits: slow_hits.clone(),
                calls: 0,
                slow_every_n,
                slow_duration,
            },
            slow_hits,
        )
    }
}

impl OutputSink for BackpressureSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.calls += 1;
        if self.calls % self.slow_every_n == 0 {
            self.slow_hits.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(self.slow_duration);
        }
        self.data
            .rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.data.batches.fetch_add(1, Ordering::Relaxed);

        // Extract seq values.
        if let Some(idx) = batch.schema().index_of("seq_int").ok() {
            let col = batch.column(idx);
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                let mut seqs = self.data.seqs.lock().unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        seqs.push(arr.value(i));
                    }
                }
            }
        }

        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &str {
        "backpressure"
    }
}

// ---------------------------------------------------------------------------
// Stability tests
// ---------------------------------------------------------------------------

/// Run the pipeline under sustained load for 30 seconds.
/// Verify: RSS growth rate is not unbounded (leak detection via linear
/// regression), no fd leaks, most written lines are processed.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn stability_sustained_load_30s() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("sustained.log");

    std::fs::write(&log_path, generate_lines(0, 1000)).unwrap();

    let mut pipeline = build_pipeline(&log_path);
    let received = ReceivedData::new();
    pipeline = pipeline.with_output(Box::new(VerifyingSink::new(received.clone())));
    pipeline.set_batch_timeout(Duration::from_millis(50));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let lp = log_path.clone();

    #[cfg(unix)]
    let initial_fds = open_fd_count();

    let total_written = Arc::new(AtomicU64::new(1000));
    let tw = total_written.clone();

    // Background writer: append 500 lines every 100ms for 30s.
    // Total: 1000 initial + ~150,000 appended = ~151,000 lines.
    tokio::spawn(async move {
        let mut seq = 1000usize;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(30) {
            let lines = generate_lines(seq, 500);
            seq += 500;
            tw.store(seq as u64, Ordering::Relaxed);
            {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&lp)
                    .unwrap();
                f.write_all(lines.as_bytes()).unwrap();
                f.flush().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        sd.cancel();
    });

    // Run pipeline while sampling RSS every second.
    let (result, rss_samples) = sample_rss_during(
        tokio::time::timeout(Duration::from_secs(60), pipeline.run_async(&shutdown)),
        Duration::from_secs(1),
    )
    .await;

    assert!(result.is_ok(), "pipeline must not hang during 30s stability test");
    assert!(result.unwrap().is_ok(), "pipeline must not error");

    let written = total_written.load(Ordering::Relaxed);
    let total_rows = received.rows.load(Ordering::Relaxed);
    let total_batches = received.batches.load(Ordering::Relaxed);

    eprintln!(
        "stability_sustained_load_30s: wrote {written} lines, output received {total_rows} rows in {total_batches} batches"
    );

    // Correctness: every line written must be received, no duplicates.
    // File input is durable — data sits on disk. The pipeline must
    // process all of it before shutdown completes.
    assert_eq!(
        total_rows, written,
        "data loss: wrote {written} lines but only received {total_rows}"
    );
    received.verify_no_duplicates();

    // RSS growth rate check: slope of RSS samples over time.
    // A memory leak would show a positive, sustained growth rate.
    // Allow up to 1MB/sample (1MB/sec) growth for JIT/cache warmup.
    if rss_samples.len() >= 5 {
        let growth = rss_growth_rate(&rss_samples);
        let growth_mb = growth / 1_048_576.0;
        let first_mb = rss_samples.first().unwrap_or(&0) / 1_048_576;
        let last_mb = rss_samples.last().unwrap_or(&0) / 1_048_576;
        eprintln!(
            "  RSS: first={first_mb}MB, last={last_mb}MB, growth_rate={growth_mb:.2}MB/sample ({} samples)",
            rss_samples.len()
        );

        // Only check on Linux where RSS is current (not peak).
        #[cfg(target_os = "linux")]
        {
            // After initial warmup (first 5 samples), growth should stabilize.
            if rss_samples.len() > 10 {
                let late_growth = rss_growth_rate(&rss_samples[5..]);
                let late_growth_mb = late_growth / 1_048_576.0;
                eprintln!("  RSS late growth rate (post-warmup): {late_growth_mb:.2}MB/sample");
                assert!(
                    late_growth_mb < 2.0,
                    "RSS growing at {late_growth_mb:.2}MB/sec after warmup — possible memory leak"
                );
            }
        }
    }

    // FD leak check.
    #[cfg(unix)]
    {
        let final_fds = open_fd_count();
        let fd_growth = final_fds as i64 - initial_fds as i64;
        eprintln!("  FDs: initial={initial_fds}, final={final_fds}, growth={fd_growth}");
        assert!(
            fd_growth < 20,
            "fd count grew by {fd_growth} — possible fd leak"
        );
    }
}

/// Sustained backpressure: slow output creates backpressure for 15s.
/// Verify: the slow path was actually hit, no deadlock, data still flows.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn stability_backpressure_15s() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("backpressure.log");

    std::fs::write(&log_path, generate_lines(0, 500)).unwrap();

    let mut pipeline = build_pipeline(&log_path);

    // Slow output: every 3rd batch takes 200ms.
    let bp_received = ReceivedData::new();
    let (sink, slow_hits) = BackpressureSink::new(bp_received.clone(), 3, Duration::from_millis(200));
    pipeline = pipeline.with_output(Box::new(sink));
    pipeline.set_batch_timeout(Duration::from_millis(30));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let lp = log_path.clone();
    let total_written = Arc::new(AtomicU64::new(500));
    let tw = total_written.clone();

    // Background writer: sustained input for 15s.
    tokio::spawn(async move {
        let mut seq = 500usize;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            let lines = generate_lines(seq, 200);
            seq += 200;
            tw.store(seq as u64, Ordering::Relaxed);
            {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&lp)
                    .unwrap();
                f.write_all(lines.as_bytes()).unwrap();
                f.flush().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        sd.cancel();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(45),
        pipeline.run_async(&shutdown),
    )
    .await;

    assert!(result.is_ok(), "pipeline must not deadlock under backpressure");
    assert!(result.unwrap().is_ok());

    let total_rows = bp_received.rows.load(Ordering::Relaxed);
    let hits = slow_hits.load(Ordering::Relaxed);
    let written = total_written.load(Ordering::Relaxed);

    eprintln!(
        "stability_backpressure_15s: wrote {written}, output {total_rows} rows, slow path hit {hits} times"
    );

    // The slow path MUST have been hit — otherwise we didn't actually
    // test backpressure.
    assert!(
        hits >= 3,
        "backpressure slow path was only hit {hits} times — test didn't exercise backpressure"
    );

    // Correctness: even under backpressure, every line must be delivered.
    // Slow output doesn't mean lost output — the pipeline must drain
    // everything before shutdown completes.
    let written = total_written.load(Ordering::Relaxed);
    assert_eq!(
        total_rows, written,
        "data loss under backpressure: wrote {written} but only received {total_rows}"
    );
    bp_received.verify_no_duplicates();
}

/// Metrics must be monotonically increasing and internally consistent.
/// Polls 100 times over 10s: transform_out <= transform_in, no decreases.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn stability_metrics_monotonic_10s() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("metrics.log");

    std::fs::write(&log_path, generate_lines(0, 100)).unwrap();

    let mut pipeline = build_pipeline(&log_path);
    let metrics_received = ReceivedData::new();
    pipeline = pipeline.with_output(Box::new(VerifyingSink::new(metrics_received.clone())));
    pipeline.set_batch_timeout(Duration::from_millis(30));

    let metrics = pipeline.metrics().clone();
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let lp = log_path.clone();

    // Background writer.
    tokio::spawn(async move {
        let mut seq = 100usize;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            let lines = generate_lines(seq, 100);
            seq += 100;
            {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&lp)
                    .unwrap();
                f.write_all(lines.as_bytes()).unwrap();
                f.flush().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        sd.cancel();
    });

    // Poll metrics periodically while pipeline runs.
    let metrics_for_check = metrics.clone();
    let check_handle = tokio::spawn(async move {
        let mut prev_in = 0u64;
        let mut prev_out = 0u64;
        let mut violations = Vec::new();
        let mut samples = 0u32;

        for tick in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let curr_in = metrics_for_check
                .transform_in
                .lines_total
                .load(Ordering::Relaxed);
            let curr_out = metrics_for_check
                .transform_out
                .lines_total
                .load(Ordering::Relaxed);

            if curr_in > 0 || curr_out > 0 {
                samples += 1;
            }

            if curr_in < prev_in {
                violations.push(format!(
                    "tick {tick}: transform_in decreased {prev_in} -> {curr_in}"
                ));
            }
            if curr_out < prev_out {
                violations.push(format!(
                    "tick {tick}: transform_out decreased {prev_out} -> {curr_out}"
                ));
            }
            if curr_out > curr_in {
                violations.push(format!(
                    "tick {tick}: transform_out ({curr_out}) > transform_in ({curr_in})"
                ));
            }

            prev_in = curr_in;
            prev_out = curr_out;
        }

        (violations, samples)
    });

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        pipeline.run_async(&shutdown),
    )
    .await;

    assert!(result.is_ok(), "pipeline must complete within 30s");
    assert!(result.unwrap().is_ok());

    let (violations, samples) = check_handle.await.unwrap();

    eprintln!("stability_metrics_monotonic_10s: {samples} non-zero samples");

    // We must have actually observed data flowing.
    assert!(
        samples >= 10,
        "only saw {samples} non-zero metric samples — test didn't observe enough data flow"
    );

    assert!(
        violations.is_empty(),
        "metrics violated monotonicity:\n{}",
        violations.join("\n")
    );
}

/// Rapid file rotation during processing must not leak fds or lose data.
/// Verify we read data from multiple rotation epochs.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn stability_file_rotation_stress_10s() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("rotate.log");

    std::fs::write(&log_path, generate_lines(0, 50)).unwrap();

    let mut pipeline = build_pipeline(&log_path);
    let received = ReceivedData::new();
    pipeline = pipeline.with_output(Box::new(VerifyingSink::new(received.clone())));
    pipeline.set_batch_timeout(Duration::from_millis(30));

    #[cfg(unix)]
    let initial_fds = open_fd_count();

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let lp = log_path.clone();
    let total_written = Arc::new(AtomicU64::new(50));
    let tw = total_written.clone();

    // Background: rotate the file every 500ms for 10s.
    tokio::spawn(async move {
        let mut seq = 50usize;
        let start = Instant::now();
        let mut rotation = 0;

        while start.elapsed() < Duration::from_secs(10) {
            // Write data.
            let lines = generate_lines(seq, 100);
            seq += 100;
            tw.store(seq as u64, Ordering::Relaxed);
            {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&lp)
                    .unwrap();
                f.write_all(lines.as_bytes()).unwrap();
                f.flush().unwrap();
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Simulate log rotation: rename current file, create new one.
            rotation += 1;
            let rotated = lp.with_extension(format!("log.{rotation}"));
            let _ = std::fs::rename(&lp, &rotated);
            std::fs::write(&lp, b"").unwrap();
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        sd.cancel();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        pipeline.run_async(&shutdown),
    )
    .await;

    assert!(result.is_ok(), "pipeline must not hang during rotation stress");
    assert!(result.unwrap().is_ok());

    let total_rows = received.rows.load(Ordering::Relaxed);
    let written = total_written.load(Ordering::Relaxed);

    eprintln!(
        "stability_file_rotation_stress_10s: wrote {written}, output {total_rows} rows"
    );

    // Correctness: every line written across all rotations must be
    // delivered. File rotation should not cause data loss.
    assert_eq!(
        total_rows, written,
        "data loss during rotation: wrote {written} but only received {total_rows}"
    );
    received.verify_no_duplicates();

    // FD leak check.
    #[cfg(unix)]
    {
        let final_fds = open_fd_count();
        let fd_growth = final_fds as i64 - initial_fds as i64;
        eprintln!("  FDs: initial={initial_fds}, final={final_fds}, growth={fd_growth}");
        assert!(
            fd_growth < 10,
            "fd count grew by {fd_growth} during rotation — possible fd leak"
        );
    }
}

/// 20 start/stop cycles must not leak fds or grow memory.
/// Each cycle creates a fresh pipeline, processes data, shuts down.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn stability_repeated_start_stop_20_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let num_cycles = 20;

    #[cfg(unix)]
    let initial_fds = open_fd_count();
    let initial_rss = process_rss_bytes().unwrap_or(0);

    let mut rss_per_cycle = Vec::new();

    for cycle in 0..num_cycles {
        let log_path = dir.path().join(format!("cycle_{cycle}.log"));

        // Use seq values that are unique across cycles.
        let base_seq = cycle * 100;
        std::fs::write(&log_path, generate_lines(base_seq, 100)).unwrap();

        let mut pipeline = build_pipeline(&log_path);
        let cycle_received = ReceivedData::new();
        pipeline = pipeline.with_output(Box::new(VerifyingSink::new(cycle_received.clone())));
        pipeline.set_batch_timeout(Duration::from_millis(20));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            pipeline.run_async(&shutdown),
        )
        .await;

        assert!(result.is_ok(), "cycle {cycle}: pipeline must not hang");
        assert!(
            result.unwrap().is_ok(),
            "cycle {cycle}: pipeline must not error"
        );

        let rows = cycle_received.rows.load(Ordering::Relaxed);
        assert!(
            rows >= 100,
            "cycle {cycle}: expected >= 100 rows, got {rows}"
        );

        // Every line in this cycle should be present with no duplicates.
        cycle_received.verify_complete(100);

        if let Some(rss) = process_rss_bytes() {
            rss_per_cycle.push(rss);
        }
    }

    // RSS trend across cycles: should not grow steadily.
    if rss_per_cycle.len() >= 10 {
        let late_growth = rss_growth_rate(&rss_per_cycle[5..]);
        let late_growth_mb = late_growth / 1_048_576.0;
        let first_mb = rss_per_cycle.first().unwrap_or(&0) / 1_048_576;
        let last_mb = rss_per_cycle.last().unwrap_or(&0) / 1_048_576;
        eprintln!(
            "stability_repeated_start_stop: RSS first={first_mb}MB, last={last_mb}MB, late_growth={late_growth_mb:.2}MB/cycle"
        );

        #[cfg(target_os = "linux")]
        assert!(
            late_growth_mb < 1.0,
            "RSS growing at {late_growth_mb:.2}MB/cycle after warmup — possible leak"
        );
    }

    // FD leak check across all cycles.
    #[cfg(unix)]
    {
        let final_fds = open_fd_count();
        let fd_growth = final_fds as i64 - initial_fds as i64;
        eprintln!(
            "  FDs: initial={initial_fds}, final={final_fds}, growth={fd_growth} over {num_cycles} cycles"
        );
        assert!(
            fd_growth < 20,
            "fd count grew by {fd_growth} over {num_cycles} cycles — possible fd leak"
        );
    }

    let final_rss = process_rss_bytes().unwrap_or(0);
    eprintln!(
        "  RSS: initial={}MB, final={}MB",
        initial_rss / 1_048_576,
        final_rss / 1_048_576,
    );
}

/// Crash-restart resilience: write a payload, start the pipeline,
/// kill it after 1 second, restart, repeat. After all cycles, verify
/// that every line was delivered at least once across all restarts.
///
/// Without checkpointing (#44), each restart re-reads from the
/// beginning of the file, so we expect massive duplicates. That's
/// at-least-once semantics. But there must be ZERO gaps — every
/// seq value from the file must appear in at least one cycle's output.
///
/// When checkpointing is implemented, duplicates should drop to near
/// zero (only the in-flight window at crash time gets re-delivered).
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn stability_crash_restart_no_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("crash_restart.log");

    let total_lines = 5000;
    std::fs::write(&log_path, generate_lines(0, total_lines)).unwrap();

    let num_cycles = 10;
    let kill_after = Duration::from_secs(1);

    // Collect all seq values across all cycles.
    let all_seqs = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
    let mut total_rows_all_cycles = 0u64;

    for cycle in 0..num_cycles {
        let mut pipeline = build_pipeline(&log_path);
        let cycle_received = ReceivedData::new();
        pipeline = pipeline.with_output(Box::new(VerifyingSink::new(cycle_received.clone())));
        pipeline.set_batch_timeout(Duration::from_millis(20));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Kill after 1 second — simulates crash.
        tokio::spawn(async move {
            tokio::time::sleep(kill_after).await;
            sd.cancel();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            pipeline.run_async(&shutdown),
        )
        .await;

        assert!(result.is_ok(), "cycle {cycle}: pipeline must not hang");
        assert!(
            result.unwrap().is_ok(),
            "cycle {cycle}: pipeline must not error"
        );

        let cycle_rows = cycle_received.rows.load(Ordering::Relaxed);
        total_rows_all_cycles += cycle_rows;

        // Merge this cycle's seqs into the global set.
        let cycle_seqs = cycle_received.seqs.lock().unwrap();
        all_seqs.lock().unwrap().extend(cycle_seqs.iter());

        eprintln!(
            "  cycle {cycle}: received {cycle_rows} rows (total across cycles: {total_rows_all_cycles})"
        );
    }

    // Verify: every seq from 0..total_lines must appear at least once.
    let all = all_seqs.lock().unwrap();
    let mut sorted: Vec<i64> = all.clone();
    sorted.sort();
    let total_with_dupes = sorted.len();
    sorted.dedup();
    let unique = sorted.len();
    let duplicates = total_with_dupes - unique;

    eprintln!(
        "stability_crash_restart: {total_lines} lines in file, {unique} unique seqs received, {duplicates} duplicates across {num_cycles} cycles"
    );

    // Check for gaps.
    let mut missing = Vec::new();
    for expected in 0..total_lines as i64 {
        if sorted.binary_search(&expected).is_err() {
            missing.push(expected);
            if missing.len() > 20 {
                break;
            }
        }
    }

    assert!(
        missing.is_empty(),
        "DATA LOSS: {} seq values never delivered across {num_cycles} restart cycles (first 20: {:?})",
        total_lines as usize - unique,
        missing
    );

    // Duplicates are expected without checkpointing.
    // Log the ratio for tracking — when checkpointing lands, this
    // should drop dramatically.
    let dupe_ratio = duplicates as f64 / total_with_dupes as f64;
    eprintln!(
        "  Duplicate ratio: {:.1}% ({duplicates}/{total_with_dupes}) — expected without checkpointing (#44)",
        dupe_ratio * 100.0
    );
}
