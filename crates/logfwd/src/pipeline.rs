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
use logfwd_io::diagnostics::{ComponentStats, PipelineMetrics};
use logfwd_io::format::FormatProcessor;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{FileInput, InputEvent, InputSource};
use logfwd_io::tail::TailConfig;
use logfwd_output::{BatchMetadata, FanOut, FanOutError, OutputSink, build_output_sink};
use logfwd_transform::SqlTransform;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Per-input state
// ---------------------------------------------------------------------------

struct InputState {
    /// The input source, wrapped in FramedInput for line framing + format processing.
    /// The pipeline receives scanner-ready bytes — it doesn't know about formats.
    source: Box<dyn InputSource>,
    /// Buffer accumulating scanner-ready bytes for batching.
    buf: Vec<u8>,
    /// Input metrics (used for parse/rotation/truncation observability).
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
    /// Static OTLP resource attributes (e.g. `service.name`) emitted with every batch.
    resource_attrs: Vec<(String, String)>,
}

impl Pipeline {
    /// Construct a pipeline from parsed YAML config.
    pub fn from_config(
        name: &str,
        config: &PipelineConfig,
        meter: &Meter,
        base_path: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        let transform_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");
        let mut transform = SqlTransform::new(transform_sql)?;

        // Wire up enrichment sources.
        for enrichment in &config.enrichment {
            match enrichment {
                EnrichmentConfig::GeoDatabase(geo_cfg) => {
                    let mut path = std::path::PathBuf::from(&geo_cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }

                    let db: Arc<dyn logfwd_io::enrichment::GeoDatabase> = match geo_cfg.format {
                        GeoDatabaseFormat::Mmdb => {
                            let mmdb = logfwd_transform::udf::geo_lookup::MmdbDatabase::open(&path)
                                .map_err(|e| {
                                    format!("failed to open geo database '{}': {e}", path.display())
                                })?;
                            Arc::new(mmdb)
                        }
                        _ => {
                            return Err(format!(
                                "unsupported geo database format: {:?}",
                                geo_cfg.format
                            ));
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

        let mut scan_config = transform.scan_config();
        // Raw format sends non-JSON lines to the scanner. Without keep_raw,
        // these produce empty rows. Force keep_raw so every line is captured.
        if config
            .inputs
            .iter()
            .any(|i| matches!(i.format, Some(Format::Raw)))
        {
            scan_config.keep_raw = true;
        }
        let scanner = Scanner::new(scan_config);

        let mut metrics = PipelineMetrics::new(name, transform_sql, meter);

        // Build inputs (file only for now).
        let mut inputs = Vec::new();
        for (i, input_cfg) in config.inputs.iter().enumerate() {
            let mut resolved_cfg = input_cfg.clone();
            if let Some(path_str) = &input_cfg.path {
                let mut path = std::path::PathBuf::from(path_str);
                if path.is_relative()
                    && let Some(base) = base_path
                {
                    path = base.join(path);
                }
                // Try to canonicalize for better error reporting/stability, but don't fail if it doesn't exist yet (glob)
                if let Ok(abs_path) = std::fs::canonicalize(&path) {
                    resolved_cfg.path = Some(abs_path.to_string_lossy().into_owned());
                } else {
                    resolved_cfg.path = Some(path.to_string_lossy().into_owned());
                }
            }

            let input_name = input_cfg
                .name
                .clone()
                .unwrap_or_else(|| format!("input_{i}"));
            let input_type_str = format!("{:?}", input_cfg.input_type).to_lowercase();
            let input_stats = metrics.add_input(&input_name, &input_type_str);
            inputs.push(build_input_state(&input_name, &resolved_cfg, input_stats)?);
        }

        // Build outputs.
        let mut sinks: Vec<Box<dyn OutputSink>> = Vec::new();
        for (i, output_cfg) in config.outputs.iter().enumerate() {
            let output_name = output_cfg
                .name
                .clone()
                .unwrap_or_else(|| format!("output_{i}"));
            let output_type_str = format!("{:?}", output_cfg.output_type).to_lowercase();
            let output_stats = metrics.add_output(&output_name, &output_type_str);
            sinks.push(build_output_sink(&output_name, output_cfg, output_stats)?);
        }

        let output: Box<dyn OutputSink> = if sinks.len() == 1 {
            sinks.into_iter().next().unwrap()
        } else {
            Box::new(FanOut::new(sinks))
        };

        // Convert resource_attrs HashMap to a sorted Vec for deterministic output.
        let mut resource_attrs: Vec<(String, String)> = config
            .resource_attrs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        resource_attrs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

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
            resource_attrs,
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
    /// Run the pipeline until `shutdown` is cancelled. Blocks the calling thread.
    ///
    /// Delegates to `run_async` on a tokio runtime. The sync interface exists
    /// for test convenience; production uses `run_async` directly.
    pub fn run(&mut self, shutdown: &CancellationToken) -> io::Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime")
            .block_on(self.run_async(shutdown))
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
            let metrics = Arc::clone(&self.metrics);
            input_handles.push(std::thread::spawn(move || {
                input_poll_loop(
                    input,
                    tx,
                    metrics,
                    sd,
                    batch_target,
                    batch_timeout,
                    poll_interval,
                );
            }));
        }
        drop(tx); // Drop our copy so rx.recv() returns None when all inputs exit.

        let mut scan_buf = Vec::with_capacity(self.batch_target_bytes);
        let mut flush_interval = tokio::time::interval(self.batch_timeout);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    break;
                }

                msg = rx.recv() => {
                    match msg {
                        Some(data) => {
                            scan_buf.extend_from_slice(&data);
                            // Flush if buffer is large enough.
                            if scan_buf.len() >= self.batch_target_bytes {
                                self.metrics.inc_flush_by_size();
                                self.flush_batch(&mut scan_buf).await;
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
                    if !scan_buf.is_empty() {
                        self.metrics.inc_flush_by_timeout();
                        self.flush_batch(&mut scan_buf).await;
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
            scan_buf.extend_from_slice(&data);
            if scan_buf.len() >= self.batch_target_bytes {
                self.flush_batch(&mut scan_buf).await;
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
        if !scan_buf.is_empty() {
            self.flush_batch(&mut scan_buf).await;
        }

        tokio::task::block_in_place(|| self.output.flush())?;
        Ok(())
    }

    /// Scan + transform + output a batch of accumulated JSON lines.
    async fn flush_batch(&mut self, scan_buf: &mut Vec<u8>) {
        if scan_buf.is_empty() {
            return;
        }
        // Swap in a pre-allocated buffer to avoid losing the capacity
        // of the original scan_buf (which was created with
        // Vec::with_capacity(batch_target_bytes)).
        let mut combined = Vec::with_capacity(self.batch_target_bytes);
        std::mem::swap(scan_buf, &mut combined);

        // Scan (CPU-bound, ~1-5ms per 4MB batch). block_in_place tells
        // tokio to move other tasks off this worker while scanning.
        let t0 = Instant::now();
        let batch = match tokio::task::block_in_place(|| self.scanner.scan(combined.into())) {
            Ok(b) => b,
            Err(e) => {
                self.metrics.inc_scan_error();
                self.metrics.inc_dropped_batch();
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
                self.metrics.inc_dropped_batch();
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
            resource_attrs: self.resource_attrs.clone(),
            observed_time_ns: now_nanos(),
        };
        if let Err(e) = tokio::task::block_in_place(|| self.output.send_batch(&result, &metadata)) {
            if let Some(fanout_error) = e
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<FanOutError>())
            {
                for failed_sink in fanout_error.failed_sinks() {
                    self.metrics.output_error(failed_sink);
                }
            } else {
                self.metrics.output_error(self.output.name());
            }
            self.metrics.inc_dropped_batch();
            eprintln!("pipeline: output error (batch dropped): {e}");
            return;
        }
        let output_elapsed = t2.elapsed();
        self.metrics.inc_output_success(num_rows);

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
/// Accumulates data in the input's `buf` across multiple poll cycles
/// before sending. Sends when the buffer reaches `batch_target_bytes` or
/// after `batch_timeout` of accumulated data, whichever comes first.
/// This avoids flooding the channel with tiny fragments.
fn input_poll_loop(
    mut input: InputState,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    metrics: Arc<PipelineMetrics>,
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

        // FramedInput handles newline framing, remainder management, and
        // format processing (CRI/Auto/passthrough). Events arriving here
        // contain scanner-ready bytes.
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
            if !input.buf.is_empty() && buffered_since.is_none() {
                buffered_since = Some(Instant::now());
            }
            for event in events {
                match event {
                    InputEvent::Data { bytes } => {
                        input.buf.extend_from_slice(&bytes);
                    }
                    InputEvent::Rotated | InputEvent::Truncated => {
                        // FramedInput already resets its internal state on these events.
                        // Track these expected lifecycle events separately from errors.
                        input.stats.inc_rotations();
                    }
                    _ => {}
                }
            }
            if buffered_since.is_none() && !input.buf.is_empty() {
                buffered_since = Some(Instant::now());
            }
        }

        // Send when buffer reaches target size OR timeout since first
        // data in this batch has elapsed.
        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let should_send =
            input.buf.len() >= batch_target_bytes || (!input.buf.is_empty() && timeout_elapsed);
        if should_send {
            let mut data = Vec::with_capacity(batch_target_bytes);
            std::mem::swap(&mut input.buf, &mut data);
            if blocking_send_with_backpressure_metric(&tx, &metrics, data).is_err() {
                break;
            }
            buffered_since = None;
        }
    }

    // Drain any remaining buffered data on shutdown.
    if !input.buf.is_empty() {
        let data = std::mem::take(&mut input.buf);
        let _ = blocking_send_with_backpressure_metric(&tx, &metrics, data);
    }
}

fn blocking_send_with_backpressure_metric(
    tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
    metrics: &PipelineMetrics,
    data: Vec<u8>,
) -> Result<(), tokio::sync::mpsc::error::SendError<Vec<u8>>> {
    match tx.try_send(data) {
        Ok(()) => Ok(()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(data)) => {
            metrics.inc_backpressure_stall();
            tx.blocking_send(data)
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(data)) => {
            Err(tokio::sync::mpsc::error::SendError(data))
        }
    }
}

// ---------------------------------------------------------------------------
// Input construction
// ---------------------------------------------------------------------------

/// Build a format processor from the config format.
fn make_format(
    name: &str,
    input_type: InputType,
    format: &Format,
    stats: &Arc<ComponentStats>,
) -> Result<FormatProcessor, String> {
    const CRI_MAX_MESSAGE: usize = 2 * 1024 * 1024;
    let proc = match format {
        Format::Cri => FormatProcessor::cri(CRI_MAX_MESSAGE, Arc::clone(stats)),
        Format::Auto => FormatProcessor::auto(CRI_MAX_MESSAGE, Arc::clone(stats)),
        Format::Json | Format::Raw => FormatProcessor::Passthrough,
        unsupported => {
            return Err(format!(
                "input '{name}': format {:?} is not supported for {:?} inputs",
                unsupported, input_type
            ));
        }
    };
    Ok(proc)
}

fn validate_input_format(name: &str, input_type: InputType, format: &Format) -> Result<(), String> {
    match input_type {
        InputType::Generator | InputType::Otlp => {
            if !matches!(format, Format::Json) {
                return Err(format!(
                    "input '{name}': format {:?} is not supported for {:?} inputs (expected json)",
                    format, input_type
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn build_input_state(
    name: &str,
    cfg: &InputConfig,
    stats: Arc<ComponentStats>,
) -> Result<InputState, String> {
    let (raw_source, format, buf_cap): (Box<dyn InputSource>, Format, usize) = match cfg.input_type
    {
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
            validate_input_format(name, InputType::File, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Generator => {
            use logfwd_io::generator::{GeneratorConfig, GeneratorInput};
            let events_per_sec = match cfg.listen.as_deref() {
                    Some(s) => s.parse().map_err(|_| {
                        format!(
                            "input '{name}': generator 'listen' must be a valid integer (events/sec), got '{s}'"
                        )
                    })?,
                    None => 0,
                };
            let config = GeneratorConfig {
                events_per_sec,
                batch_size: 1000,
                total_events: 0,
                ..Default::default()
            };
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Generator, &format)?;
            let source = GeneratorInput::new(name, config);
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Otlp => {
            let addr = cfg
                .listen
                .as_ref()
                .ok_or_else(|| format!("input '{name}': otlp input requires 'listen'"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Otlp, &format)?;
            let source = logfwd_io::otlp_receiver::OtlpReceiverInput::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to start OTLP receiver: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Udp => {
            let addr = cfg
                .listen
                .as_ref()
                .ok_or_else(|| format!("input '{name}': udp input requires 'listen'"))?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for UDP inputs (CRI is a file-based container log format)"
                ));
            }
            let source = logfwd_io::udp_input::UdpInput::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to bind UDP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Udp, &format)?;
            (Box::new(source), format, 1024 * 1024)
        }
        InputType::Tcp => {
            let addr = cfg
                .listen
                .as_ref()
                .ok_or_else(|| format!("input '{name}': tcp input requires 'listen'"))?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for TCP inputs (CRI is a file-based container log format)"
                ));
            }
            let source = logfwd_io::tcp_input::TcpInput::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to bind TCP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Tcp, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        _ => {
            return Err(format!(
                "input '{name}': type {:?} not yet supported",
                cfg.input_type
            ));
        }
    };

    // Wrap the raw transport with framing + format processing.
    let format_proc = make_format(name, cfg.input_type.clone(), &format, &stats)?;
    let framed = FramedInput::new(raw_source, format_proc, Arc::clone(&stats));

    Ok(InputState {
        source: Box::new(framed),
        buf: Vec::with_capacity(buf_cap),
        stats,
    })
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
    use std::io;
    use std::sync::atomic::Ordering;

    use logfwd_config::{Format, OutputConfig, OutputType};
    use logfwd_io::diagnostics::ComponentStats;
    use logfwd_test_utils::sinks::{DevNullSink, FailingSink, FrozenSink, SlowSink};
    use logfwd_test_utils::test_meter;

    struct NamedSink<T> {
        name: &'static str,
        inner: T,
    }

    impl<T> NamedSink<T> {
        fn new(name: &'static str, inner: T) -> Self {
            Self { name, inner }
        }
    }

    impl<T: OutputSink> OutputSink for NamedSink<T> {
        fn send_batch(
            &mut self,
            batch: &arrow::record_batch::RecordBatch,
            metadata: &BatchMetadata,
        ) -> io::Result<()> {
            self.inner.send_batch(batch, metadata)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }

        fn name(&self) -> &str {
            self.name
        }
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
            auth: None,
        };
        let sink = build_output_sink("test", &cfg, Arc::new(ComponentStats::new())).unwrap();
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
        let sink = build_output_sink("otel", &cfg, Arc::new(ComponentStats::new())).unwrap();
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
        let sink = build_output_sink("es", &cfg, Arc::new(ComponentStats::new())).unwrap();
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
        let result = build_output_sink("bad", &cfg, Arc::new(ComponentStats::new()));
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
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
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
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

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

    /// Data buffered in `buf` at the moment the shutdown token fires must
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Set batch_timeout large enough that the normal time-based flush will
        // never fire during the test — data stays in buf until shutdown.
        pipeline.batch_timeout = Duration::from_secs(60);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();

        // Cancel after enough time for the file to be polled and data to land
        // in buf, but well before the 60-second batch_timeout would fire.
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

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

    #[tokio::test]
    async fn test_blocking_send_records_backpressure_stall() {
        use std::sync::atomic::Ordering;

        let meter = test_meter();
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);

        tx.try_send(vec![1]).unwrap();

        let tx2 = tx.clone();
        let metrics2 = Arc::clone(&metrics);
        let handle = std::thread::spawn(move || {
            blocking_send_with_backpressure_metric(&tx2, &metrics2, vec![2])
        });

        for _ in 0..50 {
            if metrics.backpressure_stalls.load(Ordering::Relaxed) > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(
            metrics.backpressure_stalls.load(Ordering::Relaxed),
            1,
            "full channel should increment backpressure_stalls before blocking"
        );

        assert_eq!(rx.recv().await, Some(vec![1]));
        assert_eq!(rx.recv().await, Some(vec![2]));
        assert!(
            handle.join().unwrap().is_ok(),
            "blocking send should succeed"
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_fanout_output_errors_only_increment_failing_output() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("fanout_output_err.log");

        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(r#"{{"err":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
pipelines:
  default:
    inputs:
      - type: file
        path: "{}"
        format: json
    outputs:
      - type: stdout
        format: json
        name: healthy-a
      - type: stdout
        format: json
        name: failing
      - type: stdout
        format: json
        name: healthy-b
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_output(Box::new(FanOut::new(vec![
            Box::new(NamedSink::new("healthy-a", DevNullSink)),
            Box::new(NamedSink::new("failing", FailingSink::new(3))),
            Box::new(NamedSink::new("healthy-b", DevNullSink)),
        ])));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(
            result.is_ok(),
            "fanout output errors should not crash pipeline"
        );

        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert!(
            dropped > 0,
            "expected dropped_batches_total > 0, got {dropped}"
        );

        let output_errors: Vec<_> = pipeline
            .metrics
            .outputs
            .iter()
            .map(|(name, _, stats)| (name.as_str(), stats.errors_total.load(Ordering::Relaxed)))
            .collect();

        assert_eq!(
            output_errors,
            vec![("healthy-a", 0), ("failing", dropped), ("healthy-b", 0)]
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

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

    /// SlowSink: verify that batch timeout flushes fire when the output
    /// is slow, and that the pipeline continues processing all data.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_slow_output_flush_by_timeout() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("slow_timeout.log");

        // Write enough data that multiple batches will be produced.
        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(r#"{{"msg":"timeout {}"}}"#, i));
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Slow output with a small delay. Combined with a short batch_timeout
        // and large batch_target_bytes, this forces flushes by timeout rather
        // than by size.
        pipeline = pipeline.with_output(Box::new(SlowSink {
            delay: Duration::from_millis(10),
        }));
        pipeline.batch_timeout = Duration::from_millis(30);
        pipeline.batch_target_bytes = 1024 * 1024; // 1 MB — far larger than our data

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 100,
            "slow output: pipeline should process all data, got {lines_in} (expected >= 100)"
        );

        let batches = pipeline.metrics.batches_total.load(Ordering::Relaxed);
        assert!(
            batches > 0,
            "slow output: at least one batch should succeed, got {batches}"
        );

        let by_timeout = pipeline.metrics.flush_by_timeout.load(Ordering::Relaxed);
        assert!(
            by_timeout > 0,
            "slow output: at least one flush should be by timeout, got {by_timeout}"
        );
    }

    /// FrozenSink: verify that a frozen output causes dropped batches
    /// and output errors to be tracked in metrics while the pipeline
    /// still exits cleanly when released.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_frozen_output_tracks_metrics() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("frozen_metrics.log");

        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!(r#"{{"msg":"frozen {}"}}"#, i));
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

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
            // Release the frozen sink so the block_in_place call returns.
            tokio::time::sleep(Duration::from_millis(100)).await;
            ft.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "frozen output: pipeline must exit within timeout"
        );
        assert!(result.unwrap().is_ok());

        // With a frozen output, the input side should still have ingested
        // data (the input thread reads independently of the output).
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        let output_nanos = pipeline.metrics.output_nanos_total.load(Ordering::Relaxed);

        // The frozen sink blocks in send_batch, so output_nanos should
        // reflect time spent waiting. At least some data should have
        // reached the transform stage.
        assert!(
            lines_in > 0,
            "frozen output: some lines should reach transform, got {lines_in}"
        );
        assert!(
            output_nanos > 0,
            "frozen output: output stage should record nonzero time, got {output_nanos}"
        );
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
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
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

#[cfg(test)]
mod format_integration_tests {
    use super::*;
    use logfwd_arrow::scanner::SimdScanner;
    use logfwd_core::scan_config::ScanConfig;

    /// JSON format: raw bytes pass directly through to scanner.
    #[test]
    fn json_format_direct_to_scanner() {
        let input =
            b"{\"level\":\"INFO\",\"msg\":\"hello\"}\n{\"level\":\"WARN\",\"msg\":\"world\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
        };
        let mut scanner = SimdScanner::new(config);
        let batch = scanner.scan(input).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field_with_name("level_str").is_ok());
        assert!(batch.schema().field_with_name("msg_str").is_ok());
    }

    /// Raw format: lines captured as _raw when keep_raw is true.
    #[test]
    fn raw_format_captures_lines() {
        let input = b"plain text line 1\nplain text line 2\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: true,
            validate_utf8: false,
        };
        let mut scanner = SimdScanner::new(config);
        let batch = scanner.scan(input).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field_with_name("_raw").is_ok());
    }

    /// CRI format: message extracted, timestamp/stream/flag stripped.
    #[test]
    fn cri_extraction_produces_json() {
        let cri_input = b"2024-01-15T10:30:00Z stdout F {\"level\":\"INFO\",\"msg\":\"hello\"}\n";
        let mut out = Vec::new();
        let stats = Arc::new(ComponentStats::new());
        let mut fmt = FormatProcessor::cri(1024, Arc::clone(&stats));
        fmt.process_lines(cri_input, &mut out);

        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
        };
        let mut scanner = SimdScanner::new(config);
        let batch = scanner.scan(&out).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.schema().field_with_name("level_str").is_ok());
    }

    /// Mixed: CRI P+F → scanner produces correct fields.
    #[test]
    fn cri_pf_merge_then_scan() {
        let input = b"2024-01-15T10:30:00Z stdout P {\"level\":\"ER\n2024-01-15T10:30:00Z stdout F ROR\",\"msg\":\"boom\"}\n";
        let mut out = Vec::new();
        let stats = Arc::new(ComponentStats::new());
        let mut fmt = FormatProcessor::cri(1024, Arc::clone(&stats));
        fmt.process_lines(input, &mut out);

        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
        };
        let mut scanner = SimdScanner::new(config);
        let batch = scanner.scan(&out).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}

#[cfg(test)]
mod proptest_pipeline {
    use super::*;
    use logfwd_arrow::scanner::SimdScanner;
    use logfwd_core::scan_config::ScanConfig;
    use proptest::prelude::*;

    /// Generate random NDJSON: N lines of {"k":"v"} with random key/value lengths.
    fn arb_ndjson(max_lines: usize) -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(("[a-z]{1,8}", "[a-zA-Z0-9 ]{0,50}"), 1..=max_lines).prop_map(
            |fields| {
                let mut buf = Vec::new();
                for (key, val) in fields {
                    buf.extend_from_slice(b"{\"");
                    buf.extend_from_slice(key.as_bytes());
                    buf.extend_from_slice(b"\":\"");
                    buf.extend_from_slice(val.as_bytes());
                    buf.extend_from_slice(b"\"}\n");
                }
                buf
            },
        )
    }

    proptest! {
        /// Split arbitrary NDJSON at an arbitrary point. Both halves
        /// processed with remainder handling should produce the same
        /// RecordBatch row count as processing the whole buffer at once.
        #[test]
        #[allow(unused_assignments, unused_variables)]
        fn split_anywhere_same_row_count(
            ndjson in arb_ndjson(20),
            split_pct in 1u8..99u8,
        ) {
            let split_at = (ndjson.len() as u64 * split_pct as u64 / 100) as usize;
            let split_at = split_at.max(1).min(ndjson.len() - 1);

            // Whole buffer at once
            let config = ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                keep_raw: false,
                validate_utf8: false,
            };
            let mut scanner_whole = SimdScanner::new(ScanConfig { wanted_fields: vec![], extract_all: true, keep_raw: false, validate_utf8: false });
            let batch_whole = scanner_whole.scan(&ndjson).unwrap();

            // Split into two chunks with remainder handling
            let chunk1 = &ndjson[..split_at];
            let chunk2 = &ndjson[split_at..];

            let mut buf = Vec::new();
            let mut remainder: Vec<u8> = Vec::new();

            // Process chunk1
            let mut combined = remainder;
            combined.extend_from_slice(chunk1);
            if let Some(pos) = memchr::memrchr(b'\n', &combined) {
                if pos + 1 < combined.len() {
                    remainder = combined[pos + 1..].to_vec();
                    combined.truncate(pos + 1);
                } else {
                    remainder = Vec::new();
                }
                buf.extend_from_slice(&combined);
            } else {
                remainder = combined;
            }

            // Process chunk2
            let mut combined = remainder;
            combined.extend_from_slice(chunk2);
            if let Some(pos) = memchr::memrchr(b'\n', &combined) {
                if pos + 1 < combined.len() {
                    combined.truncate(pos + 1);
                }
                buf.extend_from_slice(&combined);
            }

            let config2 = ScanConfig { wanted_fields: vec![], extract_all: true, keep_raw: false, validate_utf8: false }; let mut scanner_split = SimdScanner::new(config2);
            let batch_split = scanner_split.scan(&buf).unwrap();

            prop_assert_eq!(
                batch_whole.num_rows(),
                batch_split.num_rows(),
                "split at {} of {}: row count mismatch",
                split_at,
                ndjson.len()
            );
        }

        /// CRI P/F sequences with random message content should always
        /// produce the same number of complete messages.
        #[test]
        fn cri_pf_random_sequences(
            messages in proptest::collection::vec("[a-zA-Z0-9]{1,30}", 1..=10),
            partials in proptest::collection::vec(0u8..3u8, 1..=10),
        ) {
            let mut input = Vec::new();
            let mut expected_count = 0usize;

            for (i, msg) in messages.iter().enumerate() {
                let num_partials = if i < partials.len() { partials[i] as usize } else { 0 };
                let msg_bytes = msg.as_bytes();

                if num_partials == 0 || msg_bytes.len() < 2 {
                    // Single F line
                    input.extend_from_slice(
                        format!("2024-01-15T10:30:{:02}Z stdout F {}\n", i % 60, msg).as_bytes(),
                    );
                    expected_count += 1;
                } else {
                    // Split into P chunks + final F
                    let chunk_size = (msg_bytes.len() / (num_partials + 1)).max(1);
                    let mut offset = 0;
                    for _ in 0..num_partials {
                        let end = (offset + chunk_size).min(msg_bytes.len());
                        let chunk = &msg[offset..end];
                        input.extend_from_slice(
                            format!("2024-01-15T10:30:{:02}Z stdout P {}\n", i % 60, chunk)
                                .as_bytes(),
                        );
                        offset = end;
                    }
                    let remaining = &msg[offset..];
                    input.extend_from_slice(
                        format!("2024-01-15T10:30:{:02}Z stdout F {}\n", i % 60, remaining)
                            .as_bytes(),
                    );
                    expected_count += 1;
                }
            }

            let mut out = Vec::new();
            let stats = Arc::new(ComponentStats::new());
            let mut fmt = FormatProcessor::cri(1024 * 1024, Arc::clone(&stats));
            fmt.process_lines(&input, &mut out);

            let line_count = out.iter().filter(|&&b| b == b'\n').count();
            prop_assert_eq!(
                line_count,
                expected_count,
                "expected {} complete messages, got {}",
                expected_count,
                line_count
            );
        }
    }
}
