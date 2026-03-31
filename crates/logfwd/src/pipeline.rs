//! Pipeline: YAML config → inputs → Scanner → SQL transform → output sinks.
//!
//! Single thread per pipeline. All components are already built and tested;
//! this module wires them together.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::metrics::Meter;

use logfwd_arrow::scanner::StreamingSimdScanner as Scanner;
use logfwd_config::{
    EnrichmentConfig, Format, GeoDatabaseFormat, InputConfig, InputType, PipelineConfig,
};
use logfwd_core::diagnostics::{ComponentStats, PipelineMetrics};
use logfwd_core::format::{CriParser, FormatParser, JsonParser, RawParser};
use logfwd_core::input::{FileInput, InputEvent, InputSource};
use logfwd_core::tail::TailConfig;
use logfwd_output::{BatchMetadata, FanOut, OutputSink, build_output_sink};
use logfwd_transform::SqlTransform;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Per-input state
// ---------------------------------------------------------------------------

struct InputState {
    #[expect(dead_code, reason = "reserved for future per-input logging")]
    name: String,
    source: Box<dyn InputSource>,
    parser: Box<dyn FormatParser>,
    /// Buffer accumulating newline-delimited JSON for the scanner.
    json_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// A single pipeline: inputs → Scanner → SQL transform → output sinks.
pub struct Pipeline {
    #[expect(dead_code, reason = "reserved for future per-pipeline logging")]
    name: String,
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
        let mut transform = SqlTransform::new(transform_sql)?;

        // Wire up enrichment sources.
        for enrichment in &config.enrichment {
            match enrichment {
                EnrichmentConfig::GeoDatabase(geo_cfg) => {
                    let db: Arc<dyn logfwd_core::enrichment::GeoDatabase> = match geo_cfg.format {
                        GeoDatabaseFormat::Mmdb => {
                            let mmdb = logfwd_transform::udf::geo_lookup::MmdbDatabase::open(
                                &geo_cfg.path,
                            )
                            .map_err(|e| {
                                format!("failed to open geo database '{}': {e}", geo_cfg.path)
                            })?;
                            Arc::new(mmdb)
                        }
                    };
                    if geo_cfg.refresh_interval.is_some() {
                        eprintln!(
                            "warn: geo_database refresh_interval is not yet implemented, database will not auto-reload"
                        );
                    }
                    transform.set_geo_database(db);
                }
            }
        }

        let scan_config = transform.scan_config();
        let scanner = Scanner::new(scan_config);

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

        Ok(Pipeline {
            name: name.to_string(),
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

    /// Replace the output sink. Useful for injecting a test sink.
    pub fn with_output(mut self, output: Box<dyn OutputSink>) -> Self {
        self.output = output;
        self
    }

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    /// Override the batch flush timeout (for testing).
    pub fn set_batch_timeout(&mut self, timeout: Duration) {
        self.batch_timeout = timeout;
    }

    /// Run the pipeline until `shutdown` is cancelled. Blocks the calling thread.
    pub fn run(&mut self, shutdown: &CancellationToken) -> io::Result<()> {
        let mut last_flush = Instant::now();

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let mut had_data = false;

            for input in &mut self.inputs {
                let events = match input.source.poll() {
                    Ok(e) => e,
                    Err(e) => {
                        eprintln!("pipeline: input poll error (skipping this cycle): {e}");
                        continue;
                    }
                };
                if events.is_empty() {
                    continue;
                }
                had_data = true;

                for event in events {
                    match event {
                        InputEvent::Data { bytes, .. } => {
                            input.stats.inc_bytes(bytes.len() as u64);
                            let (n, parse_err) = input.parser.process(&bytes, &mut input.json_buf);
                            input.stats.inc_lines(n as u64);
                            if parse_err > 0 {
                                input.stats.inc_parse_errors(parse_err as u64);
                            }
                        }
                        InputEvent::Rotated | InputEvent::Truncated => {
                            input.parser.reset();
                        }
                    }
                }
            }

            // Flush when any input buffer exceeds threshold or timeout elapsed.
            let size_ready = self
                .inputs
                .iter()
                .any(|i| i.json_buf.len() >= self.batch_target_bytes);
            let has_buffered = !self.inputs.iter().all(|i| i.json_buf.is_empty());
            let time_ready = has_buffered && last_flush.elapsed() >= self.batch_timeout;

            if size_ready || time_ready {
                // Track flush reason.
                if size_ready {
                    self.metrics.inc_flush_by_size();
                } else {
                    self.metrics.inc_flush_by_timeout();
                }

                let mut combined = Vec::new();
                for input in &mut self.inputs {
                    if !input.json_buf.is_empty() {
                        combined.append(&mut input.json_buf);
                    }
                }

                if !combined.is_empty() {
                    // Scan stage.
                    let t0 = Instant::now();
                    let batch = match self.scanner.scan(combined.into()) {
                        Ok(b) => b,
                        Err(e) => {
                            self.metrics.inc_scan_error();
                            self.metrics.inc_dropped_batch();
                            eprintln!("pipeline: scan error (batch dropped): {e}");
                            last_flush = Instant::now();
                            continue;
                        }
                    };
                    let scan_elapsed = t0.elapsed();

                    if batch.num_rows() > 0 {
                        let num_rows = batch.num_rows() as u64;
                        self.metrics.transform_in.inc_lines(num_rows);

                        // Transform stage.
                        let t1 = Instant::now();
                        let result = match self.transform.execute_blocking(batch) {
                            Ok(r) => r,
                            Err(e) => {
                                self.metrics.inc_transform_error();
                                self.metrics.inc_dropped_batch();
                                eprintln!("pipeline: transform error (batch dropped): {e}");
                                last_flush = Instant::now();
                                continue;
                            }
                        };
                        let transform_elapsed = t1.elapsed();

                        self.metrics
                            .transform_out
                            .inc_lines(result.num_rows() as u64);

                        // Output stage.
                        let t2 = Instant::now();
                        let metadata = BatchMetadata {
                            resource_attrs: vec![],
                            observed_time_ns: now_nanos(),
                        };
                        if let Err(e) = self.output.send_batch(&result, &metadata) {
                            self.metrics.output_error();
                            self.metrics.inc_dropped_batch();
                            eprintln!("pipeline: output error (batch dropped): {e}");
                            last_flush = Instant::now();
                            continue;
                        }
                        let output_elapsed = t2.elapsed();

                        // Record batch-level metrics.
                        self.metrics.record_batch(
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
                std::thread::sleep(self.poll_interval);
            }
        }

        // Drain any data that accumulated in json_buf since the last periodic
        // flush so that nothing is silently discarded on shutdown.
        let mut combined = Vec::new();
        for input in &mut self.inputs {
            if !input.json_buf.is_empty() {
                combined.append(&mut input.json_buf);
            }
        }

        if !combined.is_empty() {
            let t0 = Instant::now();
            let batch = match self.scanner.scan(combined.into()) {
                Ok(b) => b,
                Err(e) => {
                    self.metrics.inc_scan_error();
                    self.metrics.inc_dropped_batch();
                    eprintln!("pipeline: scan error on shutdown flush (batch dropped): {e}");
                    return Ok(());
                }
            };
            let scan_elapsed = t0.elapsed();

            if batch.num_rows() > 0 {
                let num_rows = batch.num_rows() as u64;
                self.metrics.transform_in.inc_lines(num_rows);

                let t1 = Instant::now();
                match self.transform.execute_blocking(batch) {
                    Ok(result) => {
                        let transform_elapsed = t1.elapsed();
                        self.metrics
                            .transform_out
                            .inc_lines(result.num_rows() as u64);

                        let t2 = Instant::now();
                        let metadata = BatchMetadata {
                            resource_attrs: vec![],
                            observed_time_ns: now_nanos(),
                        };
                        if let Err(e) = self.output.send_batch(&result, &metadata) {
                            self.metrics.output_error();
                            self.metrics.inc_dropped_batch();
                            eprintln!("pipeline: output error on shutdown flush: {e}");
                            return Ok(());
                        }
                        let output_elapsed = t2.elapsed();

                        self.metrics.record_batch(
                            num_rows,
                            scan_elapsed.as_nanos() as u64,
                            transform_elapsed.as_nanos() as u64,
                            output_elapsed.as_nanos() as u64,
                        );
                    }
                    Err(e) => {
                        self.metrics.inc_transform_error();
                        self.metrics.inc_dropped_batch();
                        eprintln!("pipeline: transform error (batch dropped): {e}");
                    }
                }
            }
        }

        self.output.flush()?;
        Ok(())
    }

    /// Async pipeline loop. Input threads stay on OS threads; scanning,
    /// transform, and output run in the async context with `select!`.
    ///
    /// Output uses `block_in_place` to avoid blocking the tokio runtime
    /// while ureq sends HTTP requests. This overlaps input reading with
    /// output sending across different batches.
    ///
    /// Known limitations (acceptable for migration period):
    /// - Scanner and output use block_in_place, which tells tokio to
    ///   temporarily move other tasks off this worker thread. During slow
    ///   HTTP sends, flush_interval can't fire on this worker. Goes away
    ///   when ureq is replaced with an async HTTP client.
    /// - self.inputs.drain(..) makes this method non-reentrant.
    pub async fn run_async(&mut self, shutdown: &CancellationToken) -> io::Result<()> {
        // Spawn input threads. Each polls its source, parses format, and
        // sends accumulated JSON lines through a bounded channel.
        // Backpressure: when the channel is full, the input thread blocks.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(16);

        let batch_target = self.batch_target_bytes;
        let batch_timeout = self.batch_timeout;
        let poll_interval = self.poll_interval;
        let mut input_handles = Vec::new();
        for input in self.inputs.drain(..) {
            let tx = tx.clone();
            let sd = shutdown.clone();
            input_handles.push(std::thread::spawn(move || {
                input_poll_loop(input, tx, sd, batch_target, batch_timeout, poll_interval);
            }));
        }
        drop(tx); // Drop our copy so rx.recv() returns None when all inputs exit.

        let mut json_buf = Vec::with_capacity(self.batch_target_bytes);
        let mut flush_interval = tokio::time::interval(self.batch_timeout);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }

                msg = rx.recv() => {
                    match msg {
                        Some(data) => {
                            json_buf.extend_from_slice(&data);
                            // Flush if buffer is large enough.
                            if json_buf.len() >= self.batch_target_bytes {
                                self.metrics.inc_flush_by_size();
                                self.flush_batch(&mut json_buf).await;
                                flush_interval.reset();
                            }
                        }
                        None => {
                            // All input threads exited.
                            break;
                        }
                    }
                }

                _ = flush_interval.tick() => {
                    if !json_buf.is_empty() {
                        self.metrics.inc_flush_by_timeout();
                        self.flush_batch(&mut json_buf).await;
                    }
                }
            }
        }

        // Drain channel messages before joining input threads.
        // This prevents deadlock during shutdown if a producer is blocked in
        // `blocking_send` while the bounded channel is full.
        // Flush in batch_target_bytes increments to avoid unbounded memory
        // growth when the channel has many buffered messages.
        while let Some(data) = rx.recv().await {
            json_buf.extend_from_slice(&data);
            if json_buf.len() >= self.batch_target_bytes {
                self.flush_batch(&mut json_buf).await;
            }
        }

        // All sender clones have now been dropped, so input threads can be
        // joined without risking a channel backpressure deadlock.
        for h in input_handles {
            if let Err(e) = h.join() {
                eprintln!("pipeline: input thread panicked: {e:?}");
            }
        }

        // Flush any remaining buffered data.
        if !json_buf.is_empty() {
            self.flush_batch(&mut json_buf).await;
        }

        tokio::task::block_in_place(|| self.output.flush())?;
        Ok(())
    }

    /// Scan + transform + output a batch of accumulated JSON lines.
    async fn flush_batch(&mut self, json_buf: &mut Vec<u8>) {
        if json_buf.is_empty() {
            return;
        }
        // Swap in a pre-allocated buffer to avoid losing the capacity
        // of the original json_buf (which was created with
        // Vec::with_capacity(batch_target_bytes)).
        let mut combined = Vec::with_capacity(self.batch_target_bytes);
        std::mem::swap(json_buf, &mut combined);

        // Scan (CPU-bound, ~1-5ms per 4MB batch). block_in_place tells
        // tokio to move other tasks off this worker while scanning.
        let t0 = Instant::now();
        let batch = match tokio::task::block_in_place(|| self.scanner.scan(combined.into())) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("pipeline: scan error (batch dropped): {e}");
                return;
            }
        };
        let scan_elapsed = t0.elapsed();

        if batch.num_rows() == 0 {
            return;
        }

        let num_rows = batch.num_rows() as u64;
        self.metrics.transform_in.inc_lines(num_rows);

        // Transform (already async).
        let t1 = Instant::now();
        let result = match self.transform.execute(batch).await {
            Ok(r) => r,
            Err(e) => {
                self.metrics.inc_transform_error();
                eprintln!("pipeline: transform error (batch dropped): {e}");
                return;
            }
        };
        let transform_elapsed = t1.elapsed();
        self.metrics
            .transform_out
            .inc_lines(result.num_rows() as u64);

        // Output (block_in_place wraps the sync HTTP send).
        let t2 = Instant::now();
        let metadata = BatchMetadata {
            resource_attrs: vec![],
            observed_time_ns: now_nanos(),
        };
        if let Err(e) = tokio::task::block_in_place(|| self.output.send_batch(&result, &metadata)) {
            self.metrics.output_error();
            eprintln!("pipeline: output error (batch dropped): {e}");
            return;
        }
        let output_elapsed = t2.elapsed();

        self.metrics.record_batch(
            num_rows,
            scan_elapsed.as_nanos() as u64,
            transform_elapsed.as_nanos() as u64,
            output_elapsed.as_nanos() as u64,
        );
    }
}

/// Input polling loop for a dedicated OS thread. Reads from the source,
/// parses format, and sends accumulated JSON lines through the channel.
///
/// Accumulates data in the input's `json_buf` across multiple poll cycles
/// before sending. Sends when the buffer reaches `batch_target_bytes` or
/// after `batch_timeout` of accumulated data, whichever comes first.
/// This avoids flooding the channel with tiny fragments.
fn input_poll_loop(
    mut input: InputState,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
) {
    // Track when the buffer first became non-empty, not when we last
    // sent. This ensures batch_timeout measures "time since first data
    // arrived in this batch", preventing tiny flushes after idle periods.
    let mut buffered_since: Option<Instant> = None;

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let events = match input.source.poll() {
            Ok(e) => e,
            Err(e) => {
                eprintln!("pipeline input: poll error: {e}");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        if events.is_empty() {
            std::thread::sleep(poll_interval);
        } else {
            // Track when this batch started accumulating.
            if !input.json_buf.is_empty() && buffered_since.is_none() {
                buffered_since = Some(Instant::now());
            }
            for event in events {
                match event {
                    InputEvent::Data { bytes, .. } => {
                        input.stats.inc_bytes(bytes.len() as u64);
                        let (n, parse_err) = input.parser.process(&bytes, &mut input.json_buf);
                        input.stats.inc_lines(n as u64);
                        if parse_err > 0 {
                            input.stats.inc_parse_errors(parse_err as u64);
                        }
                    }
                    InputEvent::Rotated | InputEvent::Truncated => {
                        input.parser.reset();
                    }
                }
            }
            // Set buffered_since after processing if buffer was empty before.
            if buffered_since.is_none() && !input.json_buf.is_empty() {
                buffered_since = Some(Instant::now());
            }
        }

        // Send when buffer reaches target size OR timeout since first
        // data in this batch has elapsed.
        let timeout_elapsed = buffered_since
            .map(|t| t.elapsed() >= batch_timeout)
            .unwrap_or(false);
        let should_send = input.json_buf.len() >= batch_target_bytes
            || (!input.json_buf.is_empty() && timeout_elapsed);
        if should_send {
            // Swap in a pre-allocated buffer to avoid reallocation churn.
            // The old buffer is sent to the consumer; the new one reuses
            // the capacity from a fresh allocation (same size as the
            // original pre-allocation in build_input_state).
            let mut data = Vec::with_capacity(batch_target_bytes);
            std::mem::swap(&mut input.json_buf, &mut data);
            if tx.blocking_send(data).is_err() {
                break;
            }
            buffered_since = None;
        }
    }

    // Drain any remaining buffered data on shutdown.
    if !input.json_buf.is_empty() {
        let data = std::mem::take(&mut input.json_buf);
        let _ = tx.blocking_send(data);
    }
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
            let format = cfg.format.clone().unwrap_or(Format::Auto);
            let tail_config = TailConfig {
                start_from_end: false,
                poll_interval_ms: 50,
                read_buf_size: 256 * 1024,
                max_open_files: cfg.max_open_files.unwrap_or(1024),
                ..Default::default()
            };
            let is_glob = path.contains('*') || path.contains('?') || path.contains('[');
            let source = if is_glob {
                FileInput::new_with_globs(name.to_string(), &[path.as_str()], tail_config)
            } else {
                FileInput::new(name.to_string(), &[PathBuf::from(path)], tail_config)
            }
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
    use logfwd_test_utils::sinks::{DevNullSink, FailingSink, FrozenSink, SlowSink};
    use logfwd_test_utils::test_meter;

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
            auth: None,
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
            auth: None,
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
            auth: None,
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
            auth: None,
        };
        let result = build_output_sink("bad", &cfg);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.contains("endpoint"), "got: {err}");
    }

    #[test]
    fn test_pipeline_from_config() {
        // Write a temp JSON file so the tailer has something to open.
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
    fn test_pipeline_run_one_batch() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Write JSON lines.
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

        // Use a very short batch timeout so the test flushes quickly.
        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd_clone.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        // Verify metrics show data was processed.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }

    /// Data buffered in `json_buf` at the moment the shutdown token fires must
    /// not be silently discarded.  The final flush-on-shutdown should process
    /// any pending data before the pipeline exits.
    #[test]
    fn test_pipeline_flush_on_shutdown() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let mut data = String::new();
        for i in 0..5 {
            data.push_str(&format!(r#"{{"level":"INFO","msg":"hello {}"}}"#, i));
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

        // Set batch_timeout large enough that the normal time-based flush will
        // never fire during the test — data stays in json_buf until shutdown.
        pipeline.batch_timeout = Duration::from_secs(60);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();

        // Cancel after enough time for the file to be polled and data to land
        // in json_buf, but well before the 60-second batch_timeout would fire.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            sd_clone.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        // The shutdown flush must have forwarded the buffered lines.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }

    /// A SQL that references a column that does not exist in the batch should
    /// cause the transform to return an error.  The pipeline must log the error,
    /// drop that batch, and continue running — it must NOT return `Err`.
    #[test]
    fn test_pipeline_transform_error_skips_batch_continues() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Write JSON lines that do NOT contain `nonexistent_col`.
        let mut data = String::new();
        for i in 0..5 {
            data.push_str(&format!(r#"{{"level":"INFO","msg":"hello {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        // SQL references a column that will never be present → DataFusion
        // returns an error at execution time, not at parse / new() time.
        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT nonexistent_col FROM logs"
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

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd_clone.cancel();
        });

        // Pipeline must not return Err even though every batch will fail the
        // transform step.
        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should survive transform errors but got: {:?}",
            result.err()
        );

        // At least one transform error must have been counted.
        let errors = pipeline.metrics.transform_errors.load(Ordering::Relaxed);
        assert!(errors > 0, "expected transform_errors > 0, got {errors}");

        // Every failed transform must also increment the dropped-batches counter.
        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert!(
            dropped > 0,
            "expected dropped_batches_total > 0, got {dropped}"
        );
    }

    // -----------------------------------------------------------------------
    // Async pipeline tests
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_pipeline_end_to_end() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("async_test.log");
        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(
                r#"{{"level":"INFO","msg":"async event {}","val":{}}}"#,
                i, i
            ));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();

        // Use devnull output to avoid stdout noise in test.
        pipeline = pipeline.with_output(Box::new(DevNullSink));

        pipeline.batch_timeout = Duration::from_millis(50);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(
            result.is_ok(),
            "async pipeline should succeed: {:?}",
            result.err()
        );

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 100,
            "expected at least 100 lines through transform, got {lines_in}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_pipeline_shutdown_drains_data() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("drain_test.log");

        // Write data
        let mut data = String::new();
        for i in 0..50 {
            data.push_str(&format!(r#"{{"msg":"drain {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Give enough time for data to be read and processed, then shutdown.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        // After shutdown, all data should have been drained.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 50,
            "shutdown should drain all data, got {lines_in} lines (expected >= 50)"
        );
    }

    // -------------------------------------------------------------------
    // Shutdown stress tests
    // -------------------------------------------------------------------

    /// Shutdown must not deadlock when the bounded channel is full and
    /// input threads are blocked in blocking_send. This was a real bug:
    /// if the pipeline joins threads before draining the channel, the
    /// threads can't make progress and the join hangs forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_does_not_deadlock_on_full_channel() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("full_channel.log");

        // Write enough data to fill the channel (16 slots). Use a tiny
        // batch target so each line becomes its own channel message.
        let mut data = String::new();
        for i in 0..200 {
            data.push_str(&format!(r#"{{"i":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();

        // SlowSink blocks the consumer, causing the channel to fill up.
        pipeline = pipeline.with_output(Box::new(SlowSink {
            delay: Duration::from_millis(50),
        }));
        pipeline.batch_target_bytes = 64; // tiny batches → many channel sends
        pipeline.batch_timeout = Duration::from_millis(10);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Cancel quickly — while input threads are likely blocked on a full channel.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            sd.cancel();
        });

        // This must complete without deadlock. If the drain/join order is
        // wrong, this will hang and the test will time out.
        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "pipeline should not deadlock on shutdown with full channel"
        );
        assert!(
            result.unwrap().is_ok(),
            "pipeline should complete successfully"
        );
    }

    /// Shutdown with a slow output must still drain all buffered data.
    /// The output takes 50ms per batch, but shutdown should wait for
    /// the drain to complete rather than dropping data.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_drains_with_slow_output() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("slow_output.log");

        let mut data = String::new();
        for i in 0..50 {
            data.push_str(&format!(r#"{{"msg":"slow {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(SlowSink {
            delay: Duration::from_millis(20),
        }));
        pipeline.batch_timeout = Duration::from_millis(30);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 50,
            "slow output: all data should drain on shutdown, got {lines_in} (expected >= 50)"
        );
    }

    /// After all file data is read, shutdown must still drain everything.
    /// The file tailer keeps polling after EOF (it's a tailer, not a
    /// reader), so the pipeline only exits via shutdown signal.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_pipeline_processes_all_data_before_shutdown() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("eof_test.log");

        let mut data = String::new();
        for i in 0..30 {
            data.push_str(&format!(r#"{{"eof":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Wait for data to be processed, then shut down.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 30,
            "all file data should be processed before shutdown, got {lines_in} (expected >= 30)"
        );
    }

    /// Shutdown with zero data should complete instantly without errors.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_empty_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("empty.log");
        std::fs::write(&log_path, b"").unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            sd.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "empty pipeline should shut down cleanly");
        assert!(result.unwrap().is_ok());
    }

    /// Immediate shutdown (cancel before run_async) must not panic or hang.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_immediate_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("immediate.log");
        std::fs::write(&log_path, b"{\"a\":1}\n").unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));

        let shutdown = CancellationToken::new();
        // Cancel immediately, before even starting.
        shutdown.cancel();

        let result =
            tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "immediate shutdown must not hang");
        assert!(result.unwrap().is_ok(), "immediate shutdown must not error");
    }

    /// Output errors must not crash the pipeline — batches are dropped
    /// and processing continues.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_output_errors_do_not_crash() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("output_err.log");

        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(r#"{{"err":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        // FailingSink fails the first N batches, then succeeds.
        pipeline = pipeline.with_output(Box::new(FailingSink {
            fail_count: 3,
            calls: 0,
        }));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(result.is_ok(), "output errors should not crash pipeline");

        // Data should still flow through transform even though output
        // failed on some batches.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in > 0,
            "data should flow through transform despite output errors"
        );
    }

    /// Rapid repeated shutdown signals must not cause panics.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_repeated_shutdown_signals() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("repeat_shutdown.log");

        let mut data = String::new();
        for i in 0..50 {
            data.push_str(&format!(r#"{{"rep":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Cancel many times — CancellationToken should handle this gracefully.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            sd.cancel();
            sd.cancel();
            sd.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "repeated shutdown must not hang");
        assert!(result.unwrap().is_ok());
    }

    /// Large batch that exceeds batch_target_bytes on a single line
    /// must still be processed and not cause channel issues.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_oversized_single_line() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("big_line.log");

        // Single line with a large value (~10KB).
        let big_value = "x".repeat(10_000);
        let data = format!(r#"{{"big":"{}"}}"#, big_value);
        std::fs::write(&log_path, format!("{data}\n")).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));
        pipeline.batch_target_bytes = 1024; // Much smaller than the line
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 1,
            "oversized line should still be processed, got {lines_in}"
        );
    }

    /// A frozen output (blocks indefinitely on send_batch) must not
    /// prevent shutdown. The pipeline should still exit because shutdown
    /// cancellation breaks the select! loop, and the drain completes
    /// when input threads exit.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_with_frozen_output() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("frozen.log");

        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!(r#"{{"frozen":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();

        // FrozenSink: first call blocks until the token is cancelled.
        // This simulates an output that hangs (network timeout, deadlock).
        let freeze_token = CancellationToken::new();
        pipeline = pipeline.with_output(Box::new(FrozenSink {
            release: freeze_token.clone(),
        }));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let ft = freeze_token.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
            // Release the frozen sink shortly after shutdown so the
            // block_in_place call can return and the pipeline can exit.
            tokio::time::sleep(Duration::from_millis(100)).await;
            ft.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "frozen output must not prevent shutdown (5s timeout)"
        );
        assert!(result.unwrap().is_ok());
    }

    /// Concurrent shutdown and data arrival: data written to the file
    /// after shutdown is signalled should not cause panics or hangs.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_concurrent_write_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("concurrent.log");

        // Start with some data.
        let mut initial = String::new();
        for i in 0..10 {
            initial.push_str(&format!(r#"{{"phase":"initial","i":{i}}}"#));
            initial.push('\n');
        }
        std::fs::write(&log_path, initial.as_bytes()).unwrap();

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

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter()).unwrap();
        pipeline = pipeline.with_output(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let lp = log_path.clone();

        // Concurrently: write more data AND shutdown at roughly the same time.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;

            // Write more data to the file (the tailer should pick it up).
            let mut more = String::new();
            for i in 10..30 {
                more.push_str(&format!(r#"{{"phase":"late","i":{i}}}"#));
                more.push('\n');
            }
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&lp).unwrap();
            f.write_all(more.as_bytes()).unwrap();
            f.flush().unwrap();

            // Shutdown shortly after the write.
            tokio::time::sleep(Duration::from_millis(50)).await;
            sd.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "concurrent write + shutdown must not hang");
        assert!(result.unwrap().is_ok());
    }
}
