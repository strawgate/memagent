//! Pipeline: YAML config → inputs → Scanner → SQL transform → output sinks.
//!
//! Single thread per pipeline. All components are already built and tested;
//! this module wires them together.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::metrics::Meter;

use logfwd_config::{Format, InputConfig, InputType, PipelineConfig};
use logfwd_core::diagnostics::{ComponentStats, PipelineMetrics};
use logfwd_core::format::{CriParser, FormatParser, JsonParser, RawParser};
use logfwd_core::input::{FileInput, InputEvent, InputSource};
use logfwd_core::scanner::SimdScanner as Scanner;
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
        let transform = SqlTransform::new(transform_sql)?;
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

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
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
                let events = input.source.poll()?;
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
                    let batch = self.scanner.scan(&combined);
                    let scan_elapsed = t0.elapsed();

                    if batch.num_rows() > 0 {
                        let num_rows = batch.num_rows() as u64;
                        self.metrics.transform_in.inc_lines(num_rows);

                        // Transform stage.
                        let t1 = Instant::now();
                        let result = match self.transform.execute(batch) {
                            Ok(r) => r,
                            Err(e) => {
                                self.metrics.inc_transform_error();
                                return Err(io::Error::other(format!("transform error: {e}")));
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
                            return Err(e);
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

        self.output.flush()?;
        Ok(())
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
}
