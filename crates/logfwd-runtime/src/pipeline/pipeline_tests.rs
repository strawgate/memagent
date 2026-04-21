//! Tests for pipeline construction, run, shutdown, checkpoint, and machine integration.

use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
// std::time::Instant is cfg-gated in the parent module for turmoil compatibility,
// but tests need it for timeout deadlines regardless of the turmoil feature flag.
use serial_test::serial;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use logfwd_config::{
    CompressionFormat, Format, OtlpOutputConfig, OtlpProtocol, OutputConfigV2, StdoutOutputConfig,
};
use logfwd_core::scan_config::ScanConfig;
use logfwd_diagnostics::diagnostics::ComponentStats;
use logfwd_output::{
    BatchMetadata,
    sink::{SendResult, Sink},
};
use logfwd_test_utils::sinks::{DevNullSink, FailingSink, FrozenSink, SlowSink};
use logfwd_test_utils::test_meter;

struct PanicSink;

impl Sink for PanicSink {
    fn send_batch<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            panic!("injected panic for held-ticket shutdown test");
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        "panic"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn test_build_sink_factory_stdout() {
    let cfg = OutputConfigV2::Stdout(StdoutOutputConfig {
        name: Some("test".to_string()),
        format: Some(Format::Json),
    });
    let factory = build_sink_factory("test", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
    assert_eq!(factory.name(), "test");
    let sink = factory.create().expect("create should succeed");
    assert_eq!(sink.name(), "test");
}

#[test]
fn test_build_sink_factory_otlp() {
    let cfg = OutputConfigV2::Otlp(OtlpOutputConfig {
        name: Some("otel".to_string()),
        endpoint: Some("http://localhost:4318".to_string()),
        protocol: Some(OtlpProtocol::Http),
        compression: Some(CompressionFormat::Zstd),
        ..Default::default()
    });
    let factory = build_sink_factory("otel", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
    assert_eq!(factory.name(), "otel");
}

#[test]
fn test_build_sink_factory_missing_endpoint() {
    let cfg = OutputConfigV2::Otlp(OtlpOutputConfig {
        name: Some("bad".to_string()),
        ..Default::default()
    });
    let result = build_sink_factory("bad", &cfg, None, Arc::new(ComponentStats::new()));
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("endpoint"), "got: {err}");
}

#[test]
fn test_pipeline_from_config() {
    // Write a temp JSON file so the tailer has something to open.
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

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
transform: "SELECT * FROM logs WHERE level = 'ERROR'"
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
fn test_pipeline_from_config_per_input_sql() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
        sql: "SELECT level FROM logs WHERE level = 'ERROR'"
    outputs:
      - type: stdout
        format: json
"#,
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
    assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());

    let pipeline = pipeline.unwrap();
    assert_eq!(pipeline.input_transforms.len(), 1);
}

#[test]
fn test_pipeline_from_config_per_input_sql_overrides_transform() {
    let dir = tempfile::tempdir().unwrap();
    let log1 = dir.path().join("app.log");
    let log2 = dir.path().join("sys.log");
    std::fs::write(&log1, b"{\"level\":\"INFO\"}\n").unwrap();
    std::fs::write(&log2, b"{\"level\":\"WARN\"}\n").unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
        sql: "SELECT level FROM logs WHERE level = 'ERROR'"
      - type: file
        path: {}
        format: json
    transform: "SELECT * FROM logs WHERE level != 'DEBUG'"
    outputs:
      - type: stdout
        format: json
"#,
        log1.display(),
        log2.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
    assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());

    let pipeline = pipeline.unwrap();
    // Two inputs → two transforms
    assert_eq!(pipeline.input_transforms.len(), 2);
}

#[test]
fn test_pipeline_from_config_propagates_batch_tuning() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

    let yaml = format!(
        r"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
    outputs:
      - type: stdout
        format: json
    batch_target_bytes: 8192
    batch_timeout_ms: 250
    poll_interval_ms: 42
",
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    assert_eq!(pipeline.batch_target_bytes, 8192);
    assert_eq!(pipeline.batch_timeout, Duration::from_millis(250));
    assert_eq!(pipeline.poll_interval, Duration::from_millis(42));
}

#[test]
fn run_rejects_zero_batch_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

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
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    pipeline.batch_timeout = Duration::ZERO;
    let shutdown = CancellationToken::new();
    let err = pipeline
        .run(&shutdown)
        .expect_err("zero batch_timeout should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(
        err.to_string().contains("batch_timeout"),
        "unexpected error: {err}"
    );
}

#[test]
fn run_rejects_zero_batch_target_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

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
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    pipeline.batch_target_bytes = 0;
    let shutdown = CancellationToken::new();
    let err = pipeline
        .run(&shutdown)
        .expect_err("zero batch_target_bytes should be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(
        err.to_string().contains("batch_target_bytes"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_pipeline_from_config_generator_record_profile() {
    let yaml = r#"
input:
  type: generator
  generator:
    events_per_sec: 25000
    batch_size: 1024
    profile: record
    attributes:
      benchmark_id: run-123
      pod_name: emitter-0
      stream_id: emitter-0
    sequence:
      field: seq
    event_created_unix_nano_field: event_created_unix_nano
output:
  type: "null"
"#;
    let config = logfwd_config::Config::load_str(yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
        .unwrap_or_else(|err| panic!("unexpected pipeline build error: {err}"));
    let events = pipeline.inputs[0].source.poll().unwrap();
    let mut bytes = Vec::new();
    for event in &events {
        match event {
            InputEvent::Data {
                bytes: event_bytes, ..
            } => bytes.extend_from_slice(event_bytes),
            _ => panic!("expected generator data event"),
        }
    }
    let mut scanner = Scanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: None,
        validate_utf8: false,
    });
    let batch = scanner.scan_detached(Bytes::from(bytes)).unwrap();
    let benchmark_id = batch
        .column_by_name("benchmark_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let pod_name = batch
        .column_by_name("pod_name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let stream_id = batch
        .column_by_name("stream_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let seq = batch
        .column_by_name("seq")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(benchmark_id.value(0), "run-123");
    assert_eq!(pod_name.value(0), "emitter-0");
    assert_eq!(stream_id.value(0), "emitter-0");
    assert_eq!(seq.value(0), 1);
    assert!(batch.column_by_name("event_created_unix_nano").is_some());
}

#[test]
fn test_pipeline_from_config_file_output() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    let output_path = dir.path().join("capture.ndjson");
    std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: file
  path: {}
  format: json
",
        log_path.display(),
        output_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
    assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
}

#[test]
fn otlp_structured_ingress_is_disabled_when_line_capture_is_required() {
    let scan_config = ScanConfig {
        line_field_name: Some(logfwd_types::field_names::BODY.to_string()),
        ..ScanConfig::default()
    };
    assert!(!input_build::otlp_uses_structured_ingress(&scan_config));
}

#[test]
fn otlp_structured_ingress_is_enabled_when_line_capture_is_not_required() {
    let scan_config = ScanConfig {
        line_field_name: None,
        ..ScanConfig::default()
    };
    assert!(input_build::otlp_uses_structured_ingress(&scan_config));
}

#[test]
fn test_pipeline_with_processor() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    // Write 4 lines.
    let mut data = String::new();
    for i in 0..4 {
        data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
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
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // Custom processor that drops rows where seq is odd.
    #[derive(Debug)]
    struct DropOddProcessor;
    impl Processor for DropOddProcessor {
        fn process(
            &mut self,
            batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<smallvec::SmallVec<[RecordBatch; 1]>, crate::processor::ProcessorError>
        {
            // Drop rows where seq is odd
            let seq_array = batch
                .column_by_name("seq")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let mut indices = vec![];
            for i in 0..seq_array.len() {
                if seq_array.value(i) % 2 == 0 {
                    indices.push(i as u32);
                }
            }
            let indices_array = arrow::array::UInt32Array::from(indices);
            let filtered = arrow::compute::take_record_batch(&batch, &indices_array).unwrap();
            Ok(smallvec::smallvec![filtered])
        }

        fn flush(&mut self) -> smallvec::SmallVec<[RecordBatch; 1]> {
            smallvec::SmallVec::new()
        }

        fn name(&self) -> &'static str {
            "drop_odd"
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    pipeline = pipeline.with_processor(Box::new(DropOddProcessor));

    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.poll_interval = Duration::from_millis(5);

    let shutdown = CancellationToken::new();
    let sd_clone = shutdown.clone();
    let metrics = Arc::clone(&pipeline.metrics);

    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let errors = metrics.transform_errors.load(Ordering::Relaxed);
            let dropped = metrics.dropped_batches_total.load(Ordering::Relaxed);
            if errors > 0 || dropped > 0 || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sd_clone.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "got: {:?}", result.err());

    // Input reads 4 lines, but transform output drops 2 lines via processor.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 4,
        "expected transform_in to be 4, got {}",
        lines_in
    );

    let lines_out = pipeline
        .metrics
        .transform_out
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_out, 2,
        "expected transform_out to be 2, got {}",
        lines_out
    );
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
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // Use a very short batch timeout so the test flushes quickly.
    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.poll_interval = Duration::from_millis(5);

    let shutdown = CancellationToken::new();
    let sd_clone = shutdown.clone();
    let metrics = Arc::clone(&pipeline.metrics);

    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let errors = metrics.transform_errors.load(Ordering::Relaxed);
            let dropped = metrics.dropped_batches_total.load(Ordering::Relaxed);
            if errors > 0 || dropped > 0 || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
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

    // SQL references a column that will never be present. Use generator
    // input so this test only exercises pipeline transform behavior and is
    // not sensitive to file-tail timing under heavy coverage.
    let yaml = r#"
input:
  type: generator
  generator:
    events_per_sec: 10000
    batch_size: 64
    profile: record
    attributes:
      benchmark_id: run-123
      pod_name: emitter-0
      stream_id: emitter-0
transform: "SELECT nonexistent_col FROM logs"
output:
  type: "null"
"#;
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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));

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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
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
    pipeline = pipeline.with_sink(Box::new(SlowSink {
        delay: Duration::from_millis(50),
    }));
    pipeline.batch_target_bytes = 64; // tiny batches -> many channel sends
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
    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "pipeline should not deadlock on shutdown with full channel"
    );
    assert!(
        result.unwrap().is_ok(),
        "pipeline should complete successfully"
    );
}

/// Terminal held-ticket shutdown must close the input channel before
/// joining producers. Otherwise producers blocked on a full bounded channel
/// can keep shutdown stuck forever.
#[tokio::test(flavor = "multi_thread")]
async fn held_ticket_shutdown_does_not_deadlock_on_full_channel() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("held_ticket_full_channel.log");

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
    // A panic in the sink is converted into InternalFailure by the worker,
    // which maps to Hold and triggers terminal shutdown.
    pipeline = pipeline.with_sink(Box::new(PanicSink));
    pipeline.batch_target_bytes = 64;
    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.set_pool_drain_timeout(Duration::from_secs(1));

    let shutdown = CancellationToken::new();
    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "held-ticket shutdown should close the channel and not deadlock"
    );
    assert!(
        result.unwrap().is_ok(),
        "pipeline should complete terminal held-ticket shutdown cleanly"
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
    pipeline = pipeline.with_sink(Box::new(SlowSink {
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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd.cancel();
    });

    let result = tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));

    let shutdown = CancellationToken::new();
    // Cancel immediately, before even starting.
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

    assert!(result.is_ok(), "immediate shutdown must not hang");
    assert!(result.unwrap().is_ok(), "immediate shutdown must not error");
}

/// Output errors must not crash the pipeline. Retry/control-plane
/// failures are surfaced as output errors without advancing checkpoints.
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
    pipeline = pipeline.with_sink(Box::new(FailingSink {
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
async fn test_async_fanout_output_errors_do_not_count_as_permanent_drops() {
    // This test verifies that a persistently-failing output does not
    // crash the pipeline and is not misclassified as a permanent drop.
    //
    // With the async worker pool, retries are built into the pool. An
    // always-failing sink causes the pool to exhaust retries and return a
    // non-delivered AckItem outcome that should HOLD the checkpoint seam.
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
        name: always-failing
"#,
        log_path.display()
    );

    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    // always-failing: fails every call — workers retry indefinitely and the
    // pipeline must hold checkpoint progress instead of rejecting it.
    pipeline = pipeline.with_sink(Box::new(FailingSink::new(u32::MAX)));
    pipeline.batch_timeout = Duration::from_millis(20);
    // Short drain timeout so pool cancels workers promptly on shutdown.
    pipeline.set_pool_drain_timeout(Duration::from_secs(1));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        // Give workers time to start retrying.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        sd.cancel();
    });

    let result = pipeline.run_async(&shutdown).await;
    assert!(
        result.is_ok(),
        "fanout output errors should not crash pipeline"
    );

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in > 0,
        "data should still reach transform despite output failure"
    );

    let dropped = pipeline
        .metrics
        .dropped_batches_total
        .load(Ordering::Relaxed);
    assert_eq!(
        dropped, 0,
        "retry/control-plane failures should not count as permanent drops"
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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
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

    let result = tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
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
    pipeline = pipeline.with_sink(Box::new(FrozenSink {
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

    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

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
    pipeline = pipeline.with_sink(Box::new(SlowSink {
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

    // In the split architecture, timeout-based flushing happens in the
    // I/O worker (not tracked by pipeline metrics). Verify data flowed
    // through the pipeline by checking batches_total above.
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
    pipeline = pipeline.with_sink(Box::new(FrozenSink {
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

    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
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

    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(result.is_ok(), "concurrent write + shutdown must not hang");
    assert!(result.unwrap().is_ok());
}

// -----------------------------------------------------------------------
// PipelineMachine integration tests (#586, #587)
// -----------------------------------------------------------------------

/// Helper: build a pipeline from a log file path with a custom async sink.
fn pipeline_with_sink(log_path: &std::path::Path, sink: Box<dyn Sink>) -> Pipeline {
    let yaml = format!(
        r#"
input:
  type: file
  path: {}
  format: json
output:
  type: "null"
"#,
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
        .unwrap()
        .with_sink(sink)
}

#[test]
fn test_machine_initialized_on_construction() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, "").unwrap();

    let pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    assert!(
        pipeline.machine.is_some(),
        "PipelineMachine must be initialized in from_config"
    );
}

#[test]
fn test_machine_clean_shutdown_no_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    logfwd_test_utils::generate_json_lines(&log_path, 100, "machine-test");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.batch_rows_total.load(Ordering::Relaxed) >= 100 {
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
    assert!(result.is_ok());
    assert!(
        pipeline.machine.is_none(),
        "machine should be consumed by shutdown drain"
    );
}

#[test]
fn test_flush_batch_with_zero_rows_still_completes() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("empty.log");
    std::fs::write(&log_path, "").unwrap();

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        sd.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "empty file should not crash pipeline");
}

#[test]
fn test_flush_batch_output_error_machine_still_drains() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    logfwd_test_utils::generate_json_lines(&log_path, 50, "fail-test");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(FailingSink::new(u32::MAX)));
    pipeline.set_batch_timeout(Duration::from_millis(10));
    // Short drain timeout so pool cancels indefinitely-retrying workers promptly.
    pipeline.set_pool_drain_timeout(Duration::from_secs(1));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.outputs[0].2.errors() > 0 {
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
    assert!(result.is_ok());
    assert!(
        pipeline.machine.is_none(),
        "machine should be consumed even with output errors"
    );
}

#[test]
fn test_channel_msg_carries_checkpoints() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(4);

    let empty = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
    let mut checkpoints = HashMap::new();
    checkpoints.insert(SourceId(42), ByteOffset(1000));
    tx.try_send(ChannelMsg {
        batch: empty,
        checkpoints,
        queued_at: Some(tokio::time::Instant::now()),
        input_index: 0,
        scan_ns: 0,
        transform_ns: 0,
    })
    .unwrap();

    let msg = rx.try_recv().unwrap();
    assert_eq!(msg.checkpoints.len(), 1);
    assert_eq!(msg.checkpoints[&SourceId(42)], ByteOffset(1000));
}

#[test]
fn hold_disposition_retains_failed_ticket_without_advancing_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("hold.log");
    std::fs::write(&log_path, b"").unwrap();

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    let machine = pipeline.machine.as_mut().unwrap();
    let source = SourceId(42);
    let ticket = machine.create_batch(source, 1000);
    let ticket = machine.begin_send(ticket);

    let (has_held, _advances) = pipeline.ack_all_tickets(vec![ticket], TicketDisposition::Hold);
    assert!(
        has_held,
        "hold disposition must request terminal shutdown to bound held-ticket growth"
    );

    let machine = pipeline.machine.as_ref().unwrap();
    assert_eq!(
        machine.in_flight_count(),
        1,
        "held tickets must keep the source checkpoint unadvanced"
    );
    assert_eq!(machine.committed_checkpoint(source), None);
    assert_eq!(
        pipeline.held_tickets.len(),
        1,
        "hold must retain a failed queued ticket instead of dropping the sending ticket"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_apply_pool_ack_does_not_underflow_inflight_counter() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("underflow.log");
    std::fs::write(&log_path, "").unwrap();
    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));

    pipeline
        .metrics
        .inflight_batches
        .store(0, Ordering::Relaxed);
    pipeline
        .apply_pool_ack(AckItem {
            tickets: vec![],
            outcome: crate::worker_pool::DeliveryOutcome::InternalFailure,
            num_rows: 0,
            submitted_at: tokio::time::Instant::now(),
            scan_ns: 0,
            transform_ns: 0,
            output_ns: 0,
            queue_wait_ns: 0,
            send_latency_ns: 0,
            batch_id: 0,
            output_name: "test".to_string(),
        })
        .await;

    assert_eq!(
        pipeline.metrics.inflight_batches.load(Ordering::Relaxed),
        0,
        "inflight counter must not underflow on stray ack"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn held_pool_ack_requests_terminal_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("held-pool-ack.log");
    std::fs::write(&log_path, "").unwrap();
    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    let machine = pipeline.machine.as_mut().unwrap();
    let ticket = machine.create_batch(SourceId(42), 1000);
    let ticket = machine.begin_send(ticket);

    let should_stop = pipeline
        .apply_pool_ack(AckItem {
            tickets: vec![ticket],
            outcome: crate::worker_pool::DeliveryOutcome::InternalFailure,
            num_rows: 0,
            submitted_at: tokio::time::Instant::now(),
            scan_ns: 0,
            transform_ns: 0,
            output_ns: 0,
            queue_wait_ns: 0,
            send_latency_ns: 0,
            batch_id: 0,
            output_name: "test".to_string(),
        })
        .await;

    assert!(
        should_stop,
        "held worker outcomes must terminalize ingestion instead of accumulating tickets"
    );
    assert_eq!(pipeline.held_tickets.len(), 1);
    assert_eq!(
        pipeline
            .machine
            .as_ref()
            .unwrap()
            .committed_checkpoint(SourceId(42)),
        None
    );
}

#[cfg(feature = "internal-failpoints")]
#[test]
#[serial]
fn test_failpoint_submit_before_pool_triggers_hold_and_shutdown() {
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "runtime::pipeline::submit::before_pool_submit",
        "1*return->off",
    )
    .expect("configure failpoint");

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("submit-failpoint.log");
    logfwd_test_utils::generate_json_lines(&log_path, 20, "submit-failpoint");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "submit failpoint should trigger graceful shutdown without panic"
    );
    assert!(
        pipeline.machine.is_none(),
        "pipeline machine should still reach drained terminal state"
    );

    fail::remove("runtime::pipeline::submit::before_pool_submit");
    scenario.teardown();
}

#[cfg(feature = "internal-failpoints")]
#[test]
#[serial]
fn test_failpoint_shutdown_skip_channel_drain_remains_safe() {
    let scenario = fail::FailScenario::setup();
    fail::cfg("runtime::pipeline::run_async::skip_channel_drain", "return")
        .expect("configure failpoint");

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-skip-drain.log");
    logfwd_test_utils::generate_json_lines(&log_path, 100, "skip-drain");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
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
        "skip-drain failpoint must not deadlock or crash shutdown"
    );

    fail::remove("runtime::pipeline::run_async::skip_channel_drain");
    scenario.teardown();
}

#[test]
fn test_transform_filter_all_rows_does_not_crash() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    // Write data that will be filtered by the transform
    logfwd_test_utils::generate_json_lines(&log_path, 50, "filter-test");

    let yaml = format!(
        r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level = 'NONEXISTENT'"
output:
  type: "null"
"#,
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.batches_total.load(Ordering::Relaxed) > 0 {
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
        "pipeline with filtering transform must not crash"
    );
    assert!(
        pipeline.machine.is_none(),
        "machine should drain cleanly even when transform filters all rows"
    );

    // Bug #700 fix: batch_rows_total must be 0 when all rows are filtered
    // by the SQL WHERE clause. batches_total must still be incremented.
    let batches = pipeline.metrics.batches_total.load(Ordering::Relaxed);
    assert!(
        batches > 0,
        "batches_total must be incremented even when transform filters all rows, got {batches}"
    );
    let batch_rows = pipeline.metrics.batch_rows_total.load(Ordering::Relaxed);
    assert_eq!(
        batch_rows, 0,
        "batch_rows_total must be 0 when transform filters all rows, got {batch_rows}"
    );
}

/// Write N lines, run the pipeline, shut down cleanly. Verify that
/// checkpoints.json exists and contains a non-zero offset.
#[test]
#[serial]
fn test_checkpoint_persisted_after_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    logfwd_test_utils::generate_json_lines(&log_path, 50, "cp-test");

    // Override data dir so checkpoints land in our temp dir.
    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::set_var("LOGFWD_DATA_DIR", dir.path());
    }

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.batch_rows_total.load(Ordering::Relaxed) >= 50 {
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

    pipeline.run(&shutdown).unwrap();

    // checkpoints.json must exist under the pipeline-scoped subdirectory.
    let cp_dir = dir.path().join("default");
    assert!(
        cp_dir.join("checkpoints.json").exists(),
        "checkpoints.json must exist after clean shutdown"
    );
    // Re-open the store to verify the checkpoints are readable.
    let store = FileCheckpointStore::open(&cp_dir).unwrap();
    let cps = store.load_all();
    assert!(!cps.is_empty(), "at least one checkpoint must be written");
    assert!(cps[0].offset > 0, "checkpoint offset must be non-zero");

    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::remove_var("LOGFWD_DATA_DIR");
    }
}

/// Write N lines, run pipeline, shut down, then write M more lines, run
/// again. The second run must process only the M new lines.
#[cfg(not(feature = "turmoil"))]
#[test]
#[serial]
fn test_pipeline_resumes_from_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("resume.log");
    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::set_var("LOGFWD_DATA_DIR", dir.path());
    }

    // First run: write 20 lines, process them.
    logfwd_test_utils::generate_json_lines(&log_path, 20, "resume-first");
    {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);
        let sink = logfwd_test_utils::CountingSink::new(counter_clone);

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.batch_rows_total.load(Ordering::Relaxed) >= 20 {
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
        pipeline.run(&shutdown).unwrap();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            20,
            "first run must process 20 rows"
        );
    }

    // Append 10 more lines.
    logfwd_test_utils::append_json_lines(&log_path, 10, "resume-second");

    // Second run: must process only the 10 new lines.
    {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);
        let sink = logfwd_test_utils::CountingSink::new(counter_clone);

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.batch_rows_total.load(Ordering::Relaxed) >= 10 {
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
        pipeline.run(&shutdown).unwrap();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            10,
            "second run must process only the 10 new lines, not re-read the first 20"
        );
    }

    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::remove_var("LOGFWD_DATA_DIR");
    }
}
