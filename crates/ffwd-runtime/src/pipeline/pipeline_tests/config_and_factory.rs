// Tests for pipeline construction, run, shutdown, checkpoint, and machine integration.

use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
// std::time::Instant is cfg-gated in the parent module for turmoil compatibility,
// but tests need it for timeout deadlines regardless of the turmoil feature flag.
use serial_test::serial;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use ffwd_config::{
    CompressionFormat, Format, OtlpOutputConfig, OtlpProtocol, OutputConfigV2, StdoutOutputConfig,
};
use ffwd_core::scan_config::ScanConfig;
use ffwd_diagnostics::diagnostics::ComponentStats;
use ffwd_output::{
    BatchMetadata,
    sink::{SendResult, Sink},
};
use ffwd_test_utils::sinks::{DevNullSink, FailingSink, FrozenSink, SlowSink};
use ffwd_test_utils::test_meter;

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

    let yaml = single_pipeline_yaml(
        &format!("type: file\npath: {}\nformat: json", log_path.display()),
        "type: stdout\nformat: json",
    );
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
    assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
}

#[test]
fn test_pipeline_from_config_with_transform() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, b"").unwrap();

    let yaml = single_pipeline_yaml_with_transform(
        &format!("type: file\npath: {}\nformat: json", log_path.display()),
        Some("\"SELECT * FROM logs WHERE level = 'ERROR'\""),
        "type: stdout\nformat: json",
    );
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
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
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
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
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
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
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
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

    let yaml = single_pipeline_yaml(
        &format!("type: file\npath: {}\nformat: json", log_path.display()),
        "type: stdout\nformat: json",
    );
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
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

    let yaml = single_pipeline_yaml(
        &format!("type: file\npath: {}\nformat: json", log_path.display()),
        "type: stdout\nformat: json",
    );
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
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
    let yaml = single_pipeline_yaml(
        r#"
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
"#,
        r#"type: "null""#,
    );
    let config = ffwd_config::Config::load_str(yaml).unwrap();
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
        row_predicate: None,
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

    let yaml = single_pipeline_yaml(
        &format!("type: file\npath: {}\nformat: json", log_path.display()),
        &format!("type: file\npath: {}\nformat: json", output_path.display()),
    );
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
    assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
}

#[test]
fn otlp_structured_ingress_is_disabled_when_line_capture_is_required() {
    let scan_config = ScanConfig {
        line_field_name: Some(ffwd_types::field_names::BODY.to_string()),
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
