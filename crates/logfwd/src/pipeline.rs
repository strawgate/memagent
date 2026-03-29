//! Pipeline: YAML config → inputs → Scanner → SQL transform → output sinks.
//!
//! Three execution modes are supported:
//!
//! | Mode | Threads | Notes |
//! |------|---------|-------|
//! | `memory` | 1 | Default. Collect → transform → output on one core. |
//! | `disk` | 2 | Collector writes compressed Arrow IPC to a disk queue; processor reads, transforms, and outputs on a second core. Survives restarts. |
//! | `memory_multi` | 2 | Collector passes Arrow batches through a bounded in-memory channel; processor transforms and outputs on a second core. |
//!
//! In all modes [`Pipeline::run`] blocks until the `shutdown` flag is set.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use crossbeam_channel::{RecvTimeoutError, Sender, bounded};
use opentelemetry::metrics::Meter;

use logfwd_config::{Format, InputConfig, InputType, PipelineConfig, PipelineMode};
use logfwd_core::diagnostics::{ComponentStats, PipelineMetrics};
use logfwd_core::disk_queue::{DiskQueueReader, DiskQueueWriter};
use logfwd_core::format::{CriParser, FormatParser, JsonParser, RawParser};
use logfwd_core::input::{FileInput, InputEvent, InputSource};
use logfwd_core::scanner::Scanner;
use logfwd_core::tail::TailConfig;
use logfwd_output::{BatchMetadata, FanOut, OutputSink, build_output_sink};
use logfwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// Per-input state
// ---------------------------------------------------------------------------

struct InputState {
    #[expect(dead_code, reason = "reserved for future per-input logging")]
    name: String,
    source: Box<dyn InputSource>,
    parser: Box<dyn FormatParser>,
    /// Accumulates newline-delimited JSON for the scanner.
    json_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

// ---------------------------------------------------------------------------
// Run mode (internal)
// ---------------------------------------------------------------------------

enum RunMode {
    /// Single thread: collect → scan → transform → output.
    Memory,
    /// Two threads: collector writes to disk queue; processor reads from it.
    Disk { dir: PathBuf, max_bytes: u64 },
    /// Two threads: collector sends through a bounded in-memory channel.
    MemoryMulti { channel_capacity: usize },
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// A single pipeline: inputs → Scanner → SQL transform → output sinks.
pub struct Pipeline {
    #[expect(dead_code, reason = "reserved for future per-pipeline logging")]
    name: String,
    mode: RunMode,
    inputs: Vec<InputState>,
    scanner: Scanner,
    transform: SqlTransform,
    output: Box<dyn OutputSink>,
    metrics: Arc<PipelineMetrics>,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
}

impl Pipeline {
    /// Construct a pipeline from parsed YAML config.
    pub fn from_config(name: &str, config: &PipelineConfig, meter: &Meter) -> Result<Self, String> {
        let transform_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");
        let transform = SqlTransform::new(transform_sql)?;
        let scan_config = transform.scan_config();
        let scanner = Scanner::new(scan_config, 8192);

        let mut metrics = PipelineMetrics::new(name, transform_sql, meter);

        // Build inputs (file only for now).
        let mut inputs = Vec::new();
        for (i, input_cfg) in config.inputs.iter().enumerate() {
            let input_name = input_cfg
                .name
                .clone()
                .unwrap_or_else(|| format!("input_{i}"));
            let input_type_str = format!("{:?}", input_cfg.input_type).to_lowercase();
            let input_stats = metrics.add_input(&input_name, &input_type_str);
            inputs.push(build_input_state(&input_name, input_cfg, input_stats)?);
        }

        // Build outputs.
        let mut sinks: Vec<Box<dyn OutputSink>> = Vec::new();
        for (i, output_cfg) in config.outputs.iter().enumerate() {
            let output_name = output_cfg
                .name
                .clone()
                .unwrap_or_else(|| format!("output_{i}"));
            let output_type_str = format!("{:?}", output_cfg.output_type).to_lowercase();
            let _output_stats = metrics.add_output(&output_name, &output_type_str);
            sinks.push(build_output_sink(&output_name, output_cfg)?);
        }

        let output: Box<dyn OutputSink> = if sinks.len() == 1 {
            sinks.into_iter().next().unwrap()
        } else {
            Box::new(FanOut::new(sinks))
        };

        let mode = match &config.mode {
            PipelineMode::Memory => RunMode::Memory,
            PipelineMode::Disk => {
                let dq = config.disk_queue.as_ref().ok_or_else(|| {
                    format!("pipeline '{name}': mode=disk requires a disk_queue section")
                })?;
                RunMode::Disk {
                    dir: PathBuf::from(&dq.dir),
                    max_bytes: dq.max_bytes,
                }
            }
            PipelineMode::MemoryMulti => RunMode::MemoryMulti {
                channel_capacity: config.channel_capacity,
            },
        };

        Ok(Pipeline {
            name: name.to_string(),
            mode,
            inputs,
            scanner,
            transform,
            output,
            metrics: Arc::new(metrics),
            batch_target_bytes: 4 * 1024 * 1024,
            batch_timeout: Duration::from_millis(100),
            poll_interval: Duration::from_millis(10),
        })
    }

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    /// Run the pipeline until `shutdown` is signaled. Consumes the pipeline.
    ///
    /// In `memory` mode this blocks on the calling thread.
    /// In `disk` and `memory_multi` modes a collector thread is spawned;
    /// the calling thread becomes the processor.
    pub fn run(self, shutdown: &AtomicBool) -> io::Result<()> {
        // Destructure so we can move fields into different threads.
        let Pipeline {
            name: _,
            mode,
            inputs,
            scanner,
            transform,
            output,
            metrics,
            batch_target_bytes,
            batch_timeout,
            poll_interval,
        } = self;

        match mode {
            RunMode::Memory => run_memory(
                inputs,
                scanner,
                transform,
                output,
                metrics,
                batch_target_bytes,
                batch_timeout,
                poll_interval,
                shutdown,
            ),
            RunMode::Disk { dir, max_bytes } => run_disk(
                inputs,
                scanner,
                transform,
                output,
                metrics,
                batch_target_bytes,
                batch_timeout,
                poll_interval,
                shutdown,
                dir,
                max_bytes,
            ),
            RunMode::MemoryMulti { channel_capacity } => run_memory_multi(
                inputs,
                scanner,
                transform,
                output,
                metrics,
                batch_target_bytes,
                batch_timeout,
                poll_interval,
                shutdown,
                channel_capacity,
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Memory mode — single thread
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn run_memory(
    mut inputs: Vec<InputState>,
    mut scanner: Scanner,
    mut transform: SqlTransform,
    mut output: Box<dyn OutputSink>,
    metrics: Arc<PipelineMetrics>,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    let mut last_flush = Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let had_data = collect_inputs(&mut inputs);

        let size_ready = inputs.iter().any(|i| i.json_buf.len() >= batch_target_bytes);
        // Time-based flush fires when there is buffered data AND enough time
        // has elapsed since the last flush — regardless of whether new data
        // arrived this particular iteration.  The `had_data &&` guard that was
        // originally here was incorrect: data arrives before the timeout fires
        // and would never be flushed if no further data came in later.
        let time_ready = !inputs.iter().all(|i| i.json_buf.is_empty())
            && last_flush.elapsed() >= batch_timeout;

        if size_ready || time_ready {
            if size_ready {
                metrics.inc_flush_by_size();
            } else {
                metrics.inc_flush_by_timeout();
            }

            let combined = drain_inputs(&mut inputs);
            if !combined.is_empty() {
                let t0 = Instant::now();
                let batch = scanner.scan(&combined);
                let scan_elapsed = t0.elapsed();

                if batch.num_rows() > 0 {
                    let num_rows = batch.num_rows() as u64;
                    metrics.transform_in.inc_lines(num_rows);

                    let t1 = Instant::now();
                    let result = transform.execute(batch).map_err(|e| {
                        metrics.inc_transform_error();
                        io::Error::other(format!("transform error: {e}"))
                    })?;
                    let transform_elapsed = t1.elapsed();

                    metrics.transform_out.inc_lines(result.num_rows() as u64);

                    let t2 = Instant::now();
                    let metadata = BatchMetadata {
                        resource_attrs: vec![],
                        observed_time_ns: now_nanos(),
                    };
                    if let Err(e) = output.send_batch(&result, &metadata) {
                        metrics.output_error();
                        return Err(e);
                    }
                    let output_elapsed = t2.elapsed();

                    metrics.record_batch(
                        num_rows,
                        scan_elapsed.as_nanos() as u64,
                        transform_elapsed.as_nanos() as u64,
                        output_elapsed.as_nanos() as u64,
                    );
                }
            }

            last_flush = Instant::now();
        }

        if !had_data {
            std::thread::sleep(poll_interval);
        }
    }

    output.flush()
}

// ---------------------------------------------------------------------------
// Disk mode — two threads with disk queue
// ---------------------------------------------------------------------------

/// Collector loop for disk mode: scan JSON → Arrow, write to disk queue.
///
/// Retries on `WouldBlock` (queue full) with a short sleep so the processor
/// can catch up.  Exits when `shutdown` is set.
#[allow(clippy::too_many_arguments)]
fn collector_to_disk(
    mut inputs: Vec<InputState>,
    mut scanner: Scanner,
    metrics: Arc<PipelineMetrics>,
    mut queue_writer: DiskQueueWriter,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    let mut last_flush = Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let had_data = collect_inputs(&mut inputs);

        let size_ready = inputs.iter().any(|i| i.json_buf.len() >= batch_target_bytes);
        // Time-based flush fires when there is buffered data AND enough time
        // has elapsed since the last flush — regardless of whether new data
        // arrived this particular iteration.  The `had_data &&` guard that was
        // originally here was incorrect: data arrives before the timeout fires
        // and would never be flushed if no further data came in later.
        let time_ready = !inputs.iter().all(|i| i.json_buf.is_empty())
            && last_flush.elapsed() >= batch_timeout;

        if size_ready || time_ready {
            if size_ready {
                metrics.inc_flush_by_size();
            } else {
                metrics.inc_flush_by_timeout();
            }

            let combined = drain_inputs(&mut inputs);
            if !combined.is_empty() {
                let batch = scanner.scan(&combined);

                if batch.num_rows() > 0 {
                    // If queue is full, back off until the processor catches up.
                    loop {
                        match queue_writer.push(&batch) {
                            Ok(()) => break,
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                metrics.inc_backpressure_stall();
                                if shutdown.load(Ordering::Relaxed) {
                                    return Ok(());
                                }
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }

            last_flush = Instant::now();
        }

        if !had_data {
            std::thread::sleep(poll_interval);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_disk(
    inputs: Vec<InputState>,
    scanner: Scanner,
    mut transform: SqlTransform,
    mut output: Box<dyn OutputSink>,
    metrics: Arc<PipelineMetrics>,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    shutdown: &AtomicBool,
    dir: PathBuf,
    max_bytes: u64,
) -> io::Result<()> {
    let queue_writer = DiskQueueWriter::new(&dir, max_bytes)
        .map_err(|e| io::Error::other(format!("disk queue init: {e}")))?;
    let queue_reader = DiskQueueReader::new(&dir)
        .map_err(|e| io::Error::other(format!("disk queue init: {e}")))?;

    // Signal sent from processor → collector when the processor wants to stop.
    let stop_collector = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop_collector);
    let metrics_clone = Arc::clone(&metrics);

    let collector_handle = std::thread::Builder::new()
        .name("logfwd-collector".into())
        .spawn(move || {
            collector_to_disk(
                inputs,
                scanner,
                metrics_clone,
                queue_writer,
                batch_target_bytes,
                batch_timeout,
                poll_interval,
                &stop_clone,
            )
        })
        .map_err(|e| io::Error::other(format!("spawn collector thread: {e}")))?;

    // Processor loop on the calling thread.
    loop {
        // Propagate external shutdown to the collector.
        if shutdown.load(Ordering::Relaxed) {
            stop_collector.store(true, Ordering::Relaxed);
        }

        match queue_reader.pop() {
            Ok(Some(batch)) => {
                process_batch(batch, &mut transform, &mut *output, &metrics)?;
            }
            Ok(None) => {
                // Queue empty: if collector is also done we can exit.
                if collector_handle.is_finished() {
                    break;
                }
                std::thread::sleep(poll_interval);
            }
            Err(e) => {
                stop_collector.store(true, Ordering::Relaxed);
                let _ = collector_handle.join();
                return Err(e);
            }
        }
    }

    let _ = collector_handle.join();
    output.flush()
}

// ---------------------------------------------------------------------------
// MemoryMulti mode — two threads, bounded in-memory channel
// ---------------------------------------------------------------------------

/// Collector loop for memory-multi mode: scan JSON → Arrow, send to channel.
#[allow(clippy::too_many_arguments)]
fn collector_to_channel(
    mut inputs: Vec<InputState>,
    mut scanner: Scanner,
    metrics: Arc<PipelineMetrics>,
    tx: Sender<RecordBatch>,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    let mut last_flush = Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let had_data = collect_inputs(&mut inputs);

        let size_ready = inputs.iter().any(|i| i.json_buf.len() >= batch_target_bytes);
        // Time-based flush fires when there is buffered data AND enough time
        // has elapsed since the last flush — regardless of whether new data
        // arrived this particular iteration.  The `had_data &&` guard that was
        // originally here was incorrect: data arrives before the timeout fires
        // and would never be flushed if no further data came in later.
        let time_ready = !inputs.iter().all(|i| i.json_buf.is_empty())
            && last_flush.elapsed() >= batch_timeout;

        if size_ready || time_ready {
            if size_ready {
                metrics.inc_flush_by_size();
            } else {
                metrics.inc_flush_by_timeout();
            }

            let combined = drain_inputs(&mut inputs);
            if !combined.is_empty() {
                let batch = scanner.scan(&combined);

                if batch.num_rows() > 0 {
                    // Bounded send: blocks when the channel is full (back-pressure).
                    if tx.send(batch).is_err() {
                        // Receiver dropped (processor exited) — stop collecting.
                        break;
                    }
                }
            }

            last_flush = Instant::now();
        }

        if !had_data {
            std::thread::sleep(poll_interval);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_memory_multi(
    inputs: Vec<InputState>,
    scanner: Scanner,
    mut transform: SqlTransform,
    mut output: Box<dyn OutputSink>,
    metrics: Arc<PipelineMetrics>,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    shutdown: &AtomicBool,
    channel_capacity: usize,
) -> io::Result<()> {
    let (tx, rx) = bounded::<RecordBatch>(channel_capacity);

    let stop_collector = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop_collector);
    let metrics_clone = Arc::clone(&metrics);

    let collector_handle = std::thread::Builder::new()
        .name("logfwd-collector".into())
        .spawn(move || {
            collector_to_channel(
                inputs,
                scanner,
                metrics_clone,
                tx,
                batch_target_bytes,
                batch_timeout,
                poll_interval,
                &stop_clone,
            )
        })
        .map_err(|e| io::Error::other(format!("spawn collector thread: {e}")))?;

    // Processor loop on the calling thread.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            stop_collector.store(true, Ordering::Relaxed);
        }

        match rx.recv_timeout(poll_interval) {
            Ok(batch) => {
                process_batch(batch, &mut transform, &mut *output, &metrics)?;
            }
            Err(RecvTimeoutError::Timeout) => {
                if collector_handle.is_finished() {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    let _ = collector_handle.join();
    output.flush()
}

// ---------------------------------------------------------------------------
// Shared processor helper
// ---------------------------------------------------------------------------

/// Transform and output a single RecordBatch, recording metrics.
fn process_batch(
    batch: RecordBatch,
    transform: &mut SqlTransform,
    output: &mut dyn OutputSink,
    metrics: &PipelineMetrics,
) -> io::Result<()> {
    let num_rows = batch.num_rows() as u64;
    metrics.transform_in.inc_lines(num_rows);

    let t1 = Instant::now();
    let result = transform.execute(batch).map_err(|e| {
        metrics.inc_transform_error();
        io::Error::other(format!("transform error: {e}"))
    })?;
    let transform_elapsed = t1.elapsed();

    metrics.transform_out.inc_lines(result.num_rows() as u64);

    let t2 = Instant::now();
    let metadata = BatchMetadata {
        resource_attrs: vec![],
        observed_time_ns: now_nanos(),
    };
    if let Err(e) = output.send_batch(&result, &metadata) {
        metrics.output_error();
        return Err(e);
    }
    let output_elapsed = t2.elapsed();

    metrics.record_batch(
        num_rows,
        0, // scan time tracked by collector in multi-core modes
        transform_elapsed.as_nanos() as u64,
        output_elapsed.as_nanos() as u64,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared input helpers
// ---------------------------------------------------------------------------

/// Poll all inputs; accumulate JSON into per-input buffers.
/// Returns `true` if any input produced data.
fn collect_inputs(inputs: &mut [InputState]) -> bool {
    let mut had_data = false;
    for input in inputs.iter_mut() {
        let events = match input.source.poll() {
            Ok(e) => e,
            Err(_) => continue,
        };
        if events.is_empty() {
            continue;
        }
        had_data = true;
        for event in events {
            match event {
                InputEvent::Data { bytes, .. } => {
                    input.stats.inc_bytes(bytes.len() as u64);
                    let n = input.parser.process(&bytes, &mut input.json_buf);
                    input.stats.inc_lines(n as u64);
                }
                InputEvent::Rotated | InputEvent::Truncated => {
                    input.parser.reset();
                }
            }
        }
    }
    had_data
}

/// Drain all non-empty input JSON buffers into a single combined buffer.
fn drain_inputs(inputs: &mut [InputState]) -> Vec<u8> {
    let mut combined = Vec::new();
    for input in inputs.iter_mut() {
        if !input.json_buf.is_empty() {
            combined.append(&mut input.json_buf);
        }
    }
    combined
}

// ---------------------------------------------------------------------------
// Input construction
// ---------------------------------------------------------------------------

fn build_format_parser(format: &Format) -> Box<dyn FormatParser> {
    match format {
        Format::Cri => Box::new(CriParser::new(2 * 1024 * 1024)),
        Format::Raw => Box::new(RawParser::new()),
        _ => Box::new(JsonParser::new()),
    }
}

fn build_input_state(
    name: &str,
    cfg: &InputConfig,
    stats: Arc<ComponentStats>,
) -> Result<InputState, String> {
    match cfg.input_type {
        InputType::File => {
            let path = cfg
                .path
                .as_ref()
                .ok_or_else(|| format!("input '{name}': file input requires 'path'"))?;
            let paths = vec![PathBuf::from(path)];
            let format = cfg.format.clone().unwrap_or(Format::Auto);
            let tail_config = TailConfig {
                start_from_end: false,
                poll_interval_ms: 50,
                read_buf_size: 256 * 1024,
                ..Default::default()
            };
            let source = FileInput::new(name.to_string(), &paths, tail_config)
                .map_err(|e| format!("input '{name}': failed to create tailer: {e}"))?;

            Ok(InputState {
                name: name.to_string(),
                source: Box::new(source),
                parser: build_format_parser(&format),
                json_buf: Vec::with_capacity(4 * 1024 * 1024),
                stats,
            })
        }
        _ => Err(format!(
            "input '{name}': type {:?} not yet supported",
            cfg.input_type
        )),
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use logfwd_config::{Format, OutputConfig, OutputType};

    fn test_meter() -> Meter {
        opentelemetry::global::meter("test")
    }

    #[test]
    fn test_build_output_sink_stdout() {
        let cfg = OutputConfig {
            name: Some("test".to_string()),
            output_type: OutputType::Stdout,
            endpoint: None,
            protocol: None,
            compression: None,
            format: Some(Format::Json),
            path: None,
        };
        let sink = build_output_sink("test", &cfg).unwrap();
        assert_eq!(sink.name(), "test");
    }

    #[test]
    fn test_build_output_sink_otlp() {
        let cfg = OutputConfig {
            name: Some("otel".to_string()),
            output_type: OutputType::Otlp,
            endpoint: Some("http://localhost:4318".to_string()),
            protocol: Some("http".to_string()),
            compression: Some("zstd".to_string()),
            format: None,
            path: None,
        };
        let sink = build_output_sink("otel", &cfg).unwrap();
        assert_eq!(sink.name(), "otel");
    }

    #[test]
    fn test_build_output_sink_http() {
        let cfg = OutputConfig {
            name: Some("es".to_string()),
            output_type: OutputType::Http,
            endpoint: Some("http://localhost:9200".to_string()),
            protocol: None,
            compression: None,
            format: None,
            path: None,
        };
        let sink = build_output_sink("es", &cfg).unwrap();
        assert_eq!(sink.name(), "es");
    }

    #[test]
    fn test_build_output_sink_missing_endpoint() {
        let cfg = OutputConfig {
            name: Some("bad".to_string()),
            output_type: OutputType::Otlp,
            endpoint: None,
            protocol: None,
            compression: None,
            format: None,
            path: None,
        };
        let result = build_output_sink("bad", &cfg);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.contains("endpoint"), "got: {err}");
    }

    #[test]
    fn test_pipeline_from_config() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter());
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn test_pipeline_from_config_with_transform() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level_str = 'ERROR'"
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter());
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn test_pipeline_from_config_disk_mode() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let queue_dir = dir.path().join("queue");
        std::fs::write(&log_path, b"").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {log}
  format: json
mode: disk
disk_queue:
  dir: {queue}
output:
  type: stdout
  format: json
"#,
            log = log_path.display(),
            queue = queue_dir.display(),
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter());
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn test_pipeline_from_config_memory_multi_mode() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
mode: memory_multi
channel_capacity: 32
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter());
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn test_pipeline_run_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(
                r#"{{"level":"INFO","msg":"hello {}","status":{}}}"#,
                i,
                200 + i
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
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        // Save metrics Arc before run() consumes the pipeline.
        let metrics = Arc::clone(pipeline.metrics());

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd_clone = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd_clone.store(true, Ordering::Relaxed);
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        let lines_in = metrics.transform_in.lines_total.load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }

    #[test]
    fn test_pipeline_run_disk_mode() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let queue_dir = dir.path().join("queue");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(r#"{{"level":"INFO","n":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {log}
  format: json
mode: disk
disk_queue:
  dir: {queue}
output:
  type: stdout
  format: json
"#,
            log = log_path.display(),
            queue = queue_dir.display(),
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        let metrics = Arc::clone(pipeline.metrics());

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd_clone = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(800));
            sd_clone.store(true, Ordering::Relaxed);
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        let lines_in = metrics.transform_in.lines_total.load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }

    #[test]
    fn test_pipeline_run_memory_multi_mode() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(r#"{{"level":"INFO","n":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
mode: memory_multi
channel_capacity: 16
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        let metrics = Arc::clone(pipeline.metrics());

        let shutdown = Arc::new(AtomicBool::new(false));
        let sd_clone = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            sd_clone.store(true, Ordering::Relaxed);
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        let lines_in = metrics.transform_in.lines_total.load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }
}
