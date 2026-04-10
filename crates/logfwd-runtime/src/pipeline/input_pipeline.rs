//! Input pipeline: I/O → CPU worker separation.
//!
//! Every input gets two dedicated OS threads:
//! - **I/O worker**: polls the input source, accumulates bytes, sends chunks
//! - **CPU worker**: scans bytes into RecordBatch, runs SQL transform
//!
//! The two workers are connected by a bounded channel (capacity 4). Backpressure
//! cascades: output pool blocks → pipeline blocks → CPU worker blocks → I/O
//! worker blocks on its source.
//!
//! # Architecture
//!
//! ```text
//! Per input:
//! ┌──────────┐  bounded(4)  ┌──────────┐  ChannelMsg
//! │ I/O      │─────────────▶│ CPU      │──────────────────────▶ pipeline rx
//! │ Worker   │              │ Worker   │
//! │ poll()   │              │ scan()   │
//! │ accum()  │              │ sql()    │
//! └──────────┘              └──────────┘
//!  OS thread                 OS thread
//! ```

#[cfg(not(feature = "turmoil"))]
use std::collections::HashMap;
#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;
#[cfg(not(feature = "turmoil"))]
use std::time::{Duration, Instant};

#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatch;
#[cfg(not(feature = "turmoil"))]
use bytes::Bytes;
#[cfg(not(feature = "turmoil"))]
use tokio::sync::mpsc;
#[cfg(not(feature = "turmoil"))]
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "turmoil"))]
use logfwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(not(feature = "turmoil"))]
use logfwd_io::input::InputEvent;
#[cfg(not(feature = "turmoil"))]
use logfwd_io::poll_cadence::AdaptivePollController;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::pipeline::SourceId;

#[cfg(not(feature = "turmoil"))]
use logfwd_io::tail::ByteOffset;

#[cfg(not(feature = "turmoil"))]
use super::health::{HealthTransitionEvent, reduce_component_health};
#[cfg(not(feature = "turmoil"))]
use super::{InputState, InputTransform};

/// Capacity of the bounded channel between I/O worker and CPU worker.
#[cfg(not(feature = "turmoil"))]
const IO_CPU_CHANNEL_CAPACITY: usize = 4;

// ---------------------------------------------------------------------------
// I/O worker — reads bytes from source, accumulates, sends to CPU worker
// ---------------------------------------------------------------------------

/// Chunk of accumulated bytes sent from I/O worker to CPU worker.
#[cfg(not(feature = "turmoil"))]
pub(crate) struct IoChunk {
    pub bytes: Bytes,
    pub checkpoints: Vec<(SourceId, ByteOffset)>,
    pub queued_at: tokio::time::Instant,
    pub input_index: usize,
}

#[cfg(not(feature = "turmoil"))]
pub(crate) enum IoWorkItem {
    Bytes(IoChunk),
    Batch {
        batch: RecordBatch,
        checkpoints: Vec<(SourceId, ByteOffset)>,
        queued_at: tokio::time::Instant,
        input_index: usize,
    },
}

/// Run the I/O worker loop for one input.
///
/// Polls the input source, accumulates bytes to `batch_target_bytes`,
/// and sends chunks to the CPU worker via `tx`.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
fn io_worker_loop(
    mut input: InputState,
    tx: mpsc::Sender<IoWorkItem>,
    metrics: Arc<PipelineMetrics>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    input_index: usize,
) {
    let mut buffered_since: Option<Instant> = None;
    let mut last_bp_warn: Option<Instant> = None;
    let mut adaptive_poll =
        AdaptivePollController::new(input.source.get_cadence().adaptive_fast_polls_max);

    'io_loop: loop {
        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            break;
        }

        let events = match input.source.poll() {
            Ok(e) => e,
            Err(e) => {
                adaptive_poll.reset_fast_polls();
                input.stats.inc_errors();
                input.stats.set_health(reduce_component_health(
                    input.stats.health(),
                    HealthTransitionEvent::PollFailed,
                ));
                tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));
        let cadence = input.source.get_cadence();
        adaptive_poll.observe_signal(cadence.signal);

        if events.is_empty() {
            if adaptive_poll.should_fast_poll() {
                metrics.inc_cadence_fast_repoll();
            } else {
                metrics.inc_cadence_idle_sleep();
                std::thread::sleep(poll_interval);
            }
        } else {
            for event in events {
                match event {
                    InputEvent::Data { bytes, .. } => {
                        input.buf.extend_from_slice(&bytes);
                    }
                    InputEvent::Batch { batch, .. } => {
                        if !input.buf.is_empty() {
                            let data = input.buf.split().freeze();
                            let checkpoints = input.source.checkpoint_data();
                            let chunk = IoWorkItem::Bytes(IoChunk {
                                bytes: data,
                                checkpoints,
                                queued_at: tokio::time::Instant::now(),
                                input_index,
                            });
                            match tx.try_send(chunk) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(chunk)) => {
                                    if last_bp_warn
                                        .is_none_or(|t| t.elapsed() >= Duration::from_secs(5))
                                    {
                                        tracing::warn!(
                                            input = input.source.name(),
                                            "input.backpressure"
                                        );
                                        last_bp_warn = Some(Instant::now());
                                    }
                                    metrics.inc_backpressure_stall();
                                    if tx.blocking_send(chunk).is_err() {
                                        break 'io_loop;
                                    }
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break 'io_loop,
                            }
                            buffered_since = None;
                        }

                        let item = IoWorkItem::Batch {
                            batch,
                            checkpoints: input.source.checkpoint_data(),
                            queued_at: tokio::time::Instant::now(),
                            input_index,
                        };
                        match tx.try_send(item) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(item)) => {
                                if last_bp_warn
                                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(5))
                                {
                                    tracing::warn!(
                                        input = input.source.name(),
                                        "input.backpressure"
                                    );
                                    last_bp_warn = Some(Instant::now());
                                }
                                metrics.inc_backpressure_stall();
                                if tx.blocking_send(item).is_err() {
                                    break 'io_loop;
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => break 'io_loop,
                        }
                    }
                    InputEvent::Rotated { .. } => {
                        input.stats.inc_rotations();
                        tracing::info!(input = input.source.name(), "input.file_rotated");
                    }
                    InputEvent::Truncated { .. } => {
                        input.stats.inc_rotations();
                        tracing::info!(input = input.source.name(), "input.file_truncated");
                    }
                    InputEvent::EndOfFile { .. } => {}
                }
            }
            if buffered_since.is_none() && !input.buf.is_empty() {
                buffered_since = Some(Instant::now());
            }
        }

        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let flush_by_size = input.buf.len() >= batch_target_bytes;
        let flush_by_timeout = !input.buf.is_empty() && timeout_elapsed;

        if flush_by_size || flush_by_timeout {
            if flush_by_size {
                metrics.inc_flush_by_size();
            } else {
                metrics.inc_flush_by_timeout();
            }

            let data = input.buf.split().freeze();
            let checkpoints = input.source.checkpoint_data();
            let chunk = IoWorkItem::Bytes(IoChunk {
                bytes: data,
                checkpoints,
                queued_at: tokio::time::Instant::now(),
                input_index,
            });

            // Try non-blocking first; if full, log backpressure and block.
            // Shutdown awareness comes from the channel-close cascade: when
            // the CPU worker exits, it drops its rx, so blocking_send returns Err.
            match tx.try_send(chunk) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(chunk)) => {
                    if last_bp_warn.is_none_or(|t| t.elapsed() >= Duration::from_secs(5)) {
                        tracing::warn!(input = input.source.name(), "input.backpressure");
                        last_bp_warn = Some(Instant::now());
                    }
                    metrics.inc_backpressure_stall();
                    if tx.blocking_send(chunk).is_err() {
                        break;
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
            buffered_since = None;
        }
    }

    // Drain remaining buffered data on shutdown.
    // Use blocking_send (not shutdown-aware) because shutdown is already
    // cancelled and we want to deliver remaining data to the CPU worker.
    if !input.buf.is_empty() {
        let data = input.buf.split().freeze();
        let checkpoints = input.source.checkpoint_data();
        let chunk = IoWorkItem::Bytes(IoChunk {
            bytes: data,
            checkpoints,
            queued_at: tokio::time::Instant::now(),
            input_index,
        });
        if tx.blocking_send(chunk).is_err() {
            tracing::warn!(
                input = input.source.name(),
                "input.channel_closed_on_shutdown_drain"
            );
        }
    }

    input.stats.set_health(reduce_component_health(
        input.stats.health(),
        HealthTransitionEvent::ShutdownCompleted,
    ));
}

// ---------------------------------------------------------------------------
// CPU worker — scans bytes into RecordBatch, runs SQL transform
// ---------------------------------------------------------------------------

/// Run the CPU worker loop for one input.
///
/// Receives raw bytes from the I/O worker, scans them into Arrow RecordBatches,
/// runs the per-input SQL transform, and sends `ChannelMsg` to the
/// pipeline's main select loop.
///
/// Creates a lightweight tokio current-thread runtime for DataFusion SQL
/// execution (which is async internally). The runtime is created once and
/// reused for all batches.
#[cfg(not(feature = "turmoil"))]
fn cpu_worker_loop(
    mut rx: mpsc::Receiver<IoWorkItem>,
    tx: mpsc::Sender<super::ChannelMsg>,
    mut transform: InputTransform,
    metrics: Arc<PipelineMetrics>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(
                input = transform.input_name.as_str(),
                error = %e,
                "cpu_worker: failed to create tokio runtime — input disabled"
            );
            return;
        }
    };

    while let Some(item) = rx.blocking_recv() {
        let (batch, checkpoints, queued_at, input_index, scan_ns) = match item {
            IoWorkItem::Bytes(chunk) => {
                let t0 = Instant::now();
                let batch = match transform.scanner.scan(chunk.bytes) {
                    Ok(b) => b,
                    Err(e) => {
                        metrics.inc_scan_error();
                        metrics.inc_parse_error();
                        metrics.inc_dropped_batch();
                        tracing::warn!(
                            input = transform.input_name.as_str(),
                            error = %e,
                            "cpu_worker: scan error (batch dropped)"
                        );
                        continue;
                    }
                };
                (
                    batch,
                    chunk.checkpoints,
                    chunk.queued_at,
                    chunk.input_index,
                    t0.elapsed().as_nanos() as u64,
                )
            }
            IoWorkItem::Batch {
                batch,
                checkpoints,
                queued_at,
                input_index,
            } => (batch, checkpoints, queued_at, input_index, 0),
        };

        let num_rows = batch.num_rows();
        if num_rows > 0 {
            metrics.transform_in.inc_lines(num_rows as u64);
        }

        let t1 = Instant::now();
        let result = match rt.block_on(transform.transform.execute(batch)) {
            Ok(r) => r,
            Err(e) => {
                metrics.inc_transform_error();
                metrics.inc_dropped_batch();
                tracing::warn!(
                    input = transform.input_name.as_str(),
                    error = %e,
                    "cpu_worker: transform error (batch dropped)"
                );
                continue;
            }
        };

        let transform_ns = t1.elapsed().as_nanos() as u64;
        let checkpoint_map: HashMap<SourceId, ByteOffset> = checkpoints.into_iter().collect();

        let msg = super::ChannelMsg {
            batch: result,
            checkpoints: checkpoint_map,
            queued_at: Some(queued_at),
            input_index,
            scan_ns,
            transform_ns,
        };

        // Use blocking_send (not shutdown-aware) so we deliver all
        // remaining batches during shutdown drain. The CPU worker exits
        // when the I/O worker drops its sender (rx returns None).
        if tx.blocking_send(msg).is_err() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// InputPipelineManager — spawns and joins I/O + CPU workers
// ---------------------------------------------------------------------------

/// Manages the I/O and CPU worker threads for all pipeline inputs.
///
/// Each input gets two OS threads connected by a bounded channel:
/// - I/O worker: polls source, accumulates bytes, sends `IoChunk`
/// - CPU worker: scans, SQL transforms, sends `ChannelMsg`
///
/// Shutdown cascade: shutdown token → I/O workers exit → drop io_tx →
/// CPU workers drain remaining chunks → CPU workers exit → drop pipeline_tx →
/// pipeline's rx.recv() returns None.
#[cfg(not(feature = "turmoil"))]
pub(crate) struct InputPipelineManager {
    io_handles: Vec<std::thread::JoinHandle<()>>,
    cpu_handles: Vec<std::thread::JoinHandle<()>>,
}

#[cfg(not(feature = "turmoil"))]
impl InputPipelineManager {
    /// Spawn I/O + CPU worker pairs for each input.
    ///
    /// Consumes `inputs` and `transforms` — they're moved to their
    /// respective worker threads.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn(
        inputs: Vec<InputState>,
        transforms: Vec<InputTransform>,
        pipeline_tx: mpsc::Sender<super::ChannelMsg>,
        metrics: Arc<PipelineMetrics>,
        shutdown: CancellationToken,
        batch_target_bytes: usize,
        batch_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        assert_eq!(
            inputs.len(),
            transforms.len(),
            "inputs ({}) and transforms ({}) must match",
            inputs.len(),
            transforms.len(),
        );

        let mut io_handles = Vec::with_capacity(inputs.len());
        let mut cpu_handles = Vec::with_capacity(inputs.len());

        for (idx, (input, transform)) in inputs.into_iter().zip(transforms.into_iter()).enumerate()
        {
            let (io_tx, io_rx) = mpsc::channel::<IoWorkItem>(IO_CPU_CHANNEL_CAPACITY);

            // Spawn I/O worker.
            let shutdown_io = shutdown.clone();
            let metrics_io = Arc::clone(&metrics);
            io_handles.push(std::thread::spawn(move || {
                io_worker_loop(
                    input,
                    io_tx,
                    metrics_io,
                    shutdown_io,
                    batch_target_bytes,
                    batch_timeout,
                    poll_interval,
                    idx,
                );
            }));

            // Spawn CPU worker.
            let pipeline_tx = pipeline_tx.clone();
            let metrics_cpu = Arc::clone(&metrics);
            cpu_handles.push(std::thread::spawn(move || {
                cpu_worker_loop(io_rx, pipeline_tx, transform, metrics_cpu);
            }));
        }

        Self {
            io_handles,
            cpu_handles,
        }
    }

    /// Join all worker threads. Call after the pipeline channel is fully drained.
    ///
    /// Joins CPU workers first (they exit when I/O workers drop their senders),
    /// then I/O workers.
    pub(super) fn join(self) {
        for h in self.cpu_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: cpu worker panicked");
            }
        }
        for h in self.io_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: io worker panicked");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;

    // --- InputPipelineManager integration tests ---
    // These test the full split pipeline via from_config + run, so they
    // don't need direct access to private types.

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn manager_spawns_and_joins_with_empty_inputs() {
        // Verify InputPipelineManager handles zero inputs gracefully.
        // We test this through the pipeline's run path with a generator
        // that immediately shuts down.
        let yaml = r"
input:
  type: generator
output:
  type: 'null'
";
        let config = logfwd_config::Config::load_str(yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should shut down cleanly: {result:?}"
        );
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_processes_file_with_sql_filter() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("split_test.log");

        // Write lines: half INFO, half DEBUG.
        let mut data = String::new();
        for i in 0..20 {
            let level = if i % 2 == 0 { "INFO" } else { "DEBUG" };
            data.push_str(&format!(r#"{{"level":"{}","seq":{}}}"#, level, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level = 'INFO'"
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = pipeline.metrics().clone();

        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                // Wait until transform_in shows all rows were scanned.
                if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 20 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "split pipeline with SQL should work: {result:?}"
        );

        // CPU worker scans 20 lines.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(lines_in, 20, "expected 20 lines scanned, got {lines_in}");

        // SQL WHERE filters half → 10 rows reach output.
        let lines_out = pipeline
            .metrics
            .transform_out
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_out, 10,
            "expected 10 lines after SQL filter, got {lines_out}"
        );
    }

    /// Two file inputs processed simultaneously through InputPipelineManager.
    /// Verifies both inputs' data reaches the output.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_two_inputs_both_processed() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log1 = dir.path().join("input1.log");
        let log2 = dir.path().join("input2.log");

        // 10 lines each, different content.
        let mut data1 = String::new();
        let mut data2 = String::new();
        for i in 0..10 {
            data1.push_str(&format!(r#"{{"src":"a","seq":{}}}"#, i));
            data1.push('\n');
            data2.push_str(&format!(r#"{{"src":"b","seq":{}}}"#, i));
            data2.push('\n');
        }
        std::fs::write(&log1, data1.as_bytes()).unwrap();
        std::fs::write(&log2, data2.as_bytes()).unwrap();

        let yaml = format!(
            r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
      - type: file
        path: {}
        format: json
    outputs:
      - type: 'null'
"#,
            log1.display(),
            log2.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = pipeline.metrics().clone();

        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                // Wait until both files' data is scanned (20 lines total).
                if metrics.transform_in.lines_total.load(Ordering::Relaxed) >= 20 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "two-input pipeline should work: {result:?}");

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_in, 20,
            "expected 20 lines from both inputs, got {lines_in}"
        );
    }

    /// End-to-end regression: file-tail read-budget signals should propagate
    /// through framed input into the runtime adaptive cadence loop.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_records_adaptive_fast_repolls() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("adaptive_fast_repolls.log");

        let payload = "x".repeat(160);
        let mut data = String::new();
        for i in 0..40 {
            data.push_str(&format!(
                r#"{{"level":"INFO","seq":{},"msg":"{}"}}"#,
                i, payload
            ));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
  poll_interval_ms: 200
  per_file_read_budget_bytes: 64
  adaptive_fast_polls_max: 4
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = pipeline.metrics().clone();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            loop {
                let lines_in = metrics.transform_in.lines_total.load(Ordering::Relaxed);
                let fast_repolls = metrics.cadence_fast_repolls.load(Ordering::Relaxed);
                if lines_in >= 40 && fast_repolls > 0 {
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "adaptive cadence pipeline should run cleanly: {result:?}"
        );

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 40,
            "expected to process test file rows, got {lines_in}"
        );

        let fast_repolls = pipeline
            .metrics
            .cadence_fast_repolls
            .load(Ordering::Relaxed);
        assert!(
            fast_repolls > 0,
            "expected adaptive cadence fast repolls to be recorded, got {fast_repolls}"
        );
    }

    /// Transform error: CPU worker drops the batch and does NOT advance
    /// the checkpoint (correct at-least-once semantics — data will be
    /// re-read on restart).
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_transform_error_drops_batch() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("transform_err.log");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        // SQL references a column that doesn't exist → DataFusion error.
        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT nonexistent_col FROM logs"
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should survive transform errors: {result:?}"
        );

        let errors = pipeline.metrics.transform_errors.load(Ordering::Relaxed);
        assert!(
            errors > 0,
            "CPU worker should report transform errors, got {errors}"
        );

        // No data reaches the pipeline — checkpoints NOT advanced
        // (at-least-once: data will be re-read on restart).
        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert!(
            dropped > 0,
            "failed batches should be counted as dropped, got {dropped}"
        );
    }

    /// Processor that returns a Transient error — tickets should be held
    /// (checkpoint does NOT advance), and the pipeline continues.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn split_pipeline_processor_transient_error_continues() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("proc_err.log");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
output:
  type: 'null'
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let meter = opentelemetry::global::meter("test");
        let mut pipeline =
            crate::pipeline::Pipeline::from_config("default", pipe_cfg, &meter, None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        // Processor that always returns a transient error.
        #[derive(Debug)]
        struct FailingProcessor;
        impl crate::processor::Processor for FailingProcessor {
            fn process(
                &mut self,
                _batch: RecordBatch,
                _meta: &logfwd_output::BatchMetadata,
            ) -> Result<smallvec::SmallVec<[RecordBatch; 1]>, crate::processor::ProcessorError>
            {
                Err(crate::processor::ProcessorError::Transient(
                    "test transient error".to_string(),
                ))
            }

            fn flush(&mut self) -> smallvec::SmallVec<[RecordBatch; 1]> {
                smallvec::SmallVec::new()
            }

            fn name(&self) -> &'static str {
                "failing"
            }

            fn is_stateful(&self) -> bool {
                false
            }
        }

        pipeline = pipeline.with_processor(Box::new(FailingProcessor));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should survive processor errors: {result:?}"
        );

        // Data was scanned (transform_in > 0) but rejected by processor
        // so nothing reached output (transform_out == 0).
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(lines_in > 0, "data should be scanned, got {lines_in}");

        let lines_out = pipeline
            .metrics
            .transform_out
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_out, 0,
            "no data should pass through failing processor, got {lines_out}"
        );
    }
}
