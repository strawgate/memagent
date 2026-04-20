use super::*;
use crate::factory::build_sink_factory;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_config::OutputType;
use logfwd_config::{CompressionFormat, Format, OtlpProtocol, OutputConfig};
use logfwd_types::diagnostics::ComponentStats;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);

fn make_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
    ]));
    let level = StringArray::from(vec![Some("ERROR"), Some("INFO")]);
    let status = Int64Array::from(vec![Some(500), Some(200)]);
    RecordBatch::try_new(schema, vec![Arc::new(level), Arc::new(status)]).unwrap()
}

fn make_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: 1_700_000_000_000_000_000,
    }
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let unique = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "logfwd-output-tests-{name}-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_stdout_json() {
    let batch = make_test_batch();
    let meta = make_metadata();
    let mut sink = StdoutSink::new(
        "test".to_string(),
        StdoutFormat::Json,
        Arc::new(ComponentStats::new()),
    );
    let mut out: Vec<u8> = Vec::new();
    sink.write_batch_to(&batch, &meta, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 2);
    // First row: level=ERROR, status=500
    assert!(
        lines[0].contains("\"level\":\"ERROR\""),
        "got: {}",
        lines[0]
    );
    assert!(lines[0].contains("\"status\":500"), "got: {}", lines[0]);
    // Second row: level=INFO, status=200
    assert!(lines[1].contains("\"level\":\"INFO\""), "got: {}", lines[1]);
    assert!(lines[1].contains("\"status\":200"), "got: {}", lines[1]);
}

#[test]
fn test_stdout_write_batch_identical_output() {
    // Two StdoutSinks writing the same batch produce identical output.
    let batch = make_test_batch();
    let meta = make_metadata();

    let mut sink1 = StdoutSink::new(
        "s1".to_string(),
        StdoutFormat::Json,
        Arc::new(ComponentStats::new()),
    );
    let mut sink2 = StdoutSink::new(
        "s2".to_string(),
        StdoutFormat::Json,
        Arc::new(ComponentStats::new()),
    );

    let mut out1: Vec<u8> = Vec::new();
    let mut out2: Vec<u8> = Vec::new();

    sink1.write_batch_to(&batch, &meta, &mut out1).unwrap();
    sink2.write_batch_to(&batch, &meta, &mut out2).unwrap();

    // Both should have identical output.
    assert_eq!(out1, out2);
    assert!(!out1.is_empty());
}

#[test]
fn test_otlp_encoding() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
    ]));
    let ts = StringArray::from(vec![Some("2024-01-15T10:30:00Z")]);
    let level = StringArray::from(vec![Some("ERROR")]);
    let msg = StringArray::from(vec![Some("something broke")]);
    let status = Int64Array::from(vec![Some(500)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts),
            Arc::new(level),
            Arc::new(msg),
            Arc::new(status),
        ],
    )
    .unwrap();

    let meta = BatchMetadata {
        resource_attrs: Arc::new(vec![("k8s.pod.name".to_string(), "myapp-abc".to_string())]),
        observed_time_ns: 1_700_000_000_000_000_000,
    };

    let mut sink = OtlpSink::new(
        "test-otlp".to_string(),
        "http://localhost:4318".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();
    sink.encode_batch(&batch, &meta);

    // Should produce non-empty protobuf bytes.
    assert!(!sink.encoder_buf.is_empty());
    // First byte should be tag for field 1 (ResourceLogs), wire type 2 = 0x0A
    assert_eq!(sink.encoder_buf[0], 0x0A);
}

#[tokio::test]
async fn test_otlp_gzip_send_batch_sets_content_encoding() {
    let batch = make_test_batch();
    let meta = make_metadata();
    let server = tiny_http::Server::http("127.0.0.1:0").expect("server");
    let addr = match server.server_addr() {
        tiny_http::ListenAddr::IP(addr) => addr,
        tiny_http::ListenAddr::Unix(_) => panic!("expected tcp listener"),
    };
    let handle = std::thread::spawn(move || {
        let request = server.recv().expect("request");
        let encoding = request
            .headers()
            .iter()
            .find(|h| h.field.equiv("Content-Encoding"))
            .map(|h| h.value.as_str().to_string())
            .unwrap_or_default();
        assert_eq!(encoding, "gzip");
        request
            .respond(tiny_http::Response::from_string("{}").with_status_code(200))
            .expect("response");
    });
    let mut sink = OtlpSink::new(
        "test-otlp".to_string(),
        format!("http://{addr}"),
        OtlpProtocol::Http,
        Compression::Gzip,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();

    use crate::Sink;
    use crate::sink::SendResult;
    match sink.send_batch(&batch, &meta).await {
        SendResult::Ok => {}
        other => panic!("gzip compression should succeed, got: {other:?}"),
    }
    handle.join().expect("server thread");
}

#[test]
fn test_json_lines_serializes_body_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let raw = StringArray::from(vec![
        Some(r#"{"ts":"2024-01-15","msg":"hello"}"#),
        Some(r#"{"ts":"2024-01-15","msg":"world"}"#),
    ]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(raw)]).unwrap();

    let mut sink = JsonLinesSink::new(
        "test-jsonl".to_string(),
        "http://localhost:9200".to_string(),
        reqwest::header::HeaderMap::new(),
        Compression::None,
        Arc::new(reqwest::Client::new()),
        Arc::new(ComponentStats::new()),
    );
    sink.serialize_batch(&batch).unwrap();

    let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(
        lines[0],
        r#"{"body":"{\"ts\":\"2024-01-15\",\"msg\":\"hello\"}"}"#
    );
    assert_eq!(
        lines[1],
        r#"{"body":"{\"ts\":\"2024-01-15\",\"msg\":\"world\"}"}"#
    );
}

#[test]
fn test_stdout_text_format() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
    ]));
    let raw = StringArray::from(vec![Some("original log line")]);
    let level = StringArray::from(vec![Some("INFO")]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(raw), Arc::new(level)]).unwrap();
    let meta = make_metadata();

    let mut sink = StdoutSink::new(
        "test".to_string(),
        StdoutFormat::Text,
        Arc::new(ComponentStats::new()),
    );
    let mut out: Vec<u8> = Vec::new();
    sink.write_batch_to(&batch, &meta, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert_eq!(output.trim(), "original log line");
}

#[test]
fn test_distinct_column_names_both_emitted() {
    // Columns with different names are separate fields, even if they share
    // a base-name prefix.  The old suffix-stripping merged these; now each
    // column name is preserved verbatim.
    let schema = Arc::new(Schema::new(vec![
        Field::new("status_str", DataType::Utf8, true),
        Field::new("status_int", DataType::Int64, true),
    ]));
    let status_s = StringArray::from(vec![Some("500")]);
    let status_i = Int64Array::from(vec![Some(500)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(status_s), Arc::new(status_i)]).unwrap();
    let meta = make_metadata();

    let mut sink = StdoutSink::new(
        "test".to_string(),
        StdoutFormat::Json,
        Arc::new(ComponentStats::new()),
    );
    let mut out: Vec<u8> = Vec::new();
    sink.write_batch_to(&batch, &meta, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    let v: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
    // Both fields present under their original column names.
    assert_eq!(v["status_str"], "500", "got: {output}");
    assert_eq!(v["status_int"], 500, "got: {output}");
}

#[test]
fn test_float_column_json() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "duration_ms",
        DataType::Float64,
        true,
    )]));
    let dur = Float64Array::from(vec![Some(3.25)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dur)]).unwrap();
    let meta = make_metadata();

    let mut sink = StdoutSink::new(
        "test".to_string(),
        StdoutFormat::Json,
        Arc::new(ComponentStats::new()),
    );
    let mut out: Vec<u8> = Vec::new();
    sink.write_batch_to(&batch, &meta, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("\"duration_ms\":3.25"), "got: {}", output);
}

#[test]
fn test_build_sink_factory_stdout() {
    let cfg = OutputConfig {
        name: Some("test".to_string()),
        output_type: OutputType::Stdout,
        format: Some(Format::Json),
        ..Default::default()
    };
    // StdoutSink uses the async pipeline — must use build_sink_factory.
    let factory = build_sink_factory("test", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
    assert_eq!(factory.name(), "test");
}

#[test]
fn test_build_sink_factory_stdout_rejects_unsupported_format() {
    let cfg = OutputConfig {
        name: Some("test".to_string()),
        output_type: OutputType::Stdout,
        format: Some(Format::Cri),
        ..Default::default()
    };
    match build_sink_factory("test", &cfg, None, Arc::new(ComponentStats::new())) {
        Ok(_) => panic!("expected unsupported stdout format to be rejected"),
        Err(err) => assert!(
            err.to_string().contains("stdout does not support"),
            "got: {err}"
        ),
    }
}

#[test]
fn test_build_sink_factory_file_resolves_relative_path_against_base_path() {
    let base_dir = unique_temp_dir("base");
    let cwd_dir = unique_temp_dir("cwd");
    let filename = format!(
        "capture-{}.ndjson",
        NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed)
    );

    let cfg = OutputConfig {
        name: Some("capture".to_string()),
        output_type: OutputType::File,
        path: Some(filename.clone()),
        format: Some(Format::Json),
        ..Default::default()
    };

    let factory = build_sink_factory(
        "capture",
        &cfg,
        Some(&base_dir),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();

    assert_eq!(factory.name(), "capture");
    assert!(base_dir.join(&filename).exists());
    assert!(!cwd_dir.join(&filename).exists());

    let _ = std::fs::remove_file(base_dir.join(&filename));
    let _ = std::fs::remove_dir(&base_dir);
    let _ = std::fs::remove_dir(&cwd_dir);
}

#[test]
fn test_build_sink_factory_file_rejects_compression() {
    let cfg = OutputConfig {
        name: Some("capture".to_string()),
        output_type: OutputType::File,
        path: Some("/tmp/capture.ndjson".to_string()),
        compression: Some(CompressionFormat::Zstd),
        ..Default::default()
    };

    let err = match build_sink_factory("capture", &cfg, None, Arc::new(ComponentStats::new())) {
        Ok(_) => panic!("expected file compression to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("does not support 'zstd' compression")
    );
}

#[test]
fn test_build_sink_factory_otlp_accepts_gzip() {
    let cfg = OutputConfig {
        name: Some("otel".to_string()),
        output_type: OutputType::Otlp,
        endpoint: Some("http://localhost:4318".to_string()),
        protocol: Some(OtlpProtocol::Http),
        compression: Some(CompressionFormat::Gzip),
        ..Default::default()
    };
    build_sink_factory("otel", &cfg, None, Arc::new(ComponentStats::new()))
        .expect("gzip OTLP compression should be accepted");
}

#[test]
fn test_build_sink_factory_http_supported() {
    let cfg = OutputConfig {
        name: Some("http-ok".to_string()),
        output_type: OutputType::Http,
        endpoint: Some("http://localhost:9200".to_string()),
        compression: Some(CompressionFormat::Gzip),
        ..Default::default()
    };
    let result = build_sink_factory("http-ok", &cfg, None, Arc::new(ComponentStats::new()));
    assert!(result.is_ok(), "Http type should be supported");
}

#[test]
fn test_build_sink_factory_elasticsearch_rejects_unknown_compression() {
    let cfg = OutputConfig {
        name: Some("es".to_string()),
        output_type: OutputType::Elasticsearch,
        endpoint: Some("http://localhost:9200".to_string()),
        compression: Some(CompressionFormat::Zstd),
        ..Default::default()
    };
    let err = match build_sink_factory("es", &cfg, None, Arc::new(ComponentStats::new())) {
        Ok(_) => panic!("elasticsearch must reject unsupported compression"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("elasticsearch does not support 'zstd' compression")
    );
}

#[test]
fn test_build_sink_factory_missing_endpoint() {
    let cfg = OutputConfig {
        name: Some("bad".to_string()),
        output_type: OutputType::Otlp,
        ..Default::default()
    };
    // OTLP now uses the async pipeline via build_sink_factory.
    let result = build_sink_factory("bad", &cfg, None, Arc::new(ComponentStats::new()));
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("endpoint"), "got: {err}");
}

#[test]
fn test_build_auth_headers_none() {
    let headers = build_auth_headers(None);
    assert!(headers.is_empty());
}

#[test]
fn test_build_auth_headers_bearer() {
    use logfwd_config::AuthConfig;
    let auth = AuthConfig {
        bearer_token: Some("tok123".to_string()),
        headers: std::collections::HashMap::new(),
    };
    let headers = build_auth_headers(Some(&auth));
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].0, "Authorization");
    assert_eq!(headers[0].1, "Bearer tok123");
}

#[test]
fn test_build_auth_headers_custom() {
    use logfwd_config::AuthConfig;
    let mut h = std::collections::HashMap::new();
    h.insert("X-API-Key".to_string(), "secret".to_string());
    let auth = AuthConfig {
        bearer_token: None,
        headers: h,
    };
    let headers = build_auth_headers(Some(&auth));
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].0, "X-API-Key");
    assert_eq!(headers[0].1, "secret");
}

#[test]
fn test_build_auth_headers_bearer_and_custom() {
    use logfwd_config::AuthConfig;
    let mut h = std::collections::HashMap::new();
    h.insert("X-Tenant".to_string(), "acme".to_string());
    let auth = AuthConfig {
        bearer_token: Some("mytoken".to_string()),
        headers: h,
    };
    let headers = build_auth_headers(Some(&auth));
    // Bearer first, then custom
    assert_eq!(headers.len(), 2);
    assert!(
        headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer mytoken")
    );
    assert!(headers.iter().any(|(k, v)| k == "X-Tenant" && v == "acme"));
}

#[test]
fn test_build_sink_factory_elasticsearch_with_bearer_auth() {
    use logfwd_config::AuthConfig;
    let cfg = OutputConfig {
        name: Some("auth-sink".to_string()),
        output_type: OutputType::Elasticsearch,
        endpoint: Some("http://localhost:9200".to_string()),
        auth: Some(AuthConfig {
            bearer_token: Some("mytoken".to_string()),
            headers: std::collections::HashMap::new(),
        }),
        ..Default::default()
    };
    // Verify the factory builds successfully with auth.
    let factory =
        build_sink_factory("auth-sink", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
    assert_eq!(factory.name(), "auth-sink");
}

// -----------------------------------------------------------------------
// Tests for issue #285/#316: OTLP type dispatch uses actual DataType
// -----------------------------------------------------------------------

#[test]
fn test_otlp_type_mismatch_no_panic() {
    // Column named "count_int" but actually contains strings.
    // Before the fix this would panic in as_primitive::<Int64Type>().
    // After the fix it falls through to AttrArray::Str.
    let schema = Arc::new(Schema::new(vec![
        Field::new("count_int", DataType::Utf8, true),
        Field::new("message_str", DataType::Utf8, true),
    ]));
    let count = StringArray::from(vec![Some("high")]);
    let msg = StringArray::from(vec![Some("something happened")]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(count), Arc::new(msg)]).unwrap();
    let meta = BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: 1_700_000_000_000_000_000,
    };
    let mut sink = OtlpSink::new(
        "test-otlp".to_string(),
        "http://localhost:4318".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();
    // Must not panic.
    sink.encode_batch(&batch, &meta);
    assert!(!sink.encoder_buf.is_empty());
}

#[test]
fn test_otlp_real_int_column_encoded() {
    // Column named "status_str" but actually Int64: should be encoded as int attr.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status_str",
        DataType::Int64,
        true,
    )]));
    let status = Int64Array::from(vec![Some(200i64)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(status)]).unwrap();
    let meta = BatchMetadata {
        resource_attrs: Arc::default(),
        observed_time_ns: 1_700_000_000_000_000_000,
    };
    let mut sink = OtlpSink::new(
        "test-otlp".to_string(),
        "http://localhost:4318".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        Arc::new(ComponentStats::new()),
    )
    .unwrap();
    // Must not panic, and should produce non-empty output.
    sink.encode_batch(&batch, &meta);
    assert!(!sink.encoder_buf.is_empty());
}

// -----------------------------------------------------------------------
// Tests for issue #317: JSON Lines schema lookup panic paths removed
// -----------------------------------------------------------------------

#[test]
fn test_json_lines_body_only_no_panic() {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let raw = StringArray::from(vec![Some(r#"{"x":1}"#)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(raw)]).unwrap();
    let mut sink = JsonLinesSink::new(
        "test-jsonl".to_string(),
        "http://localhost:9200".to_string(),
        reqwest::header::HeaderMap::new(),
        Compression::None,
        Arc::new(reqwest::Client::new()),
        Arc::new(ComponentStats::new()),
    );
    // Must not panic.
    sink.serialize_batch(&batch).unwrap();
    let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
    assert_eq!(output.trim(), r#"{"body":"{\"x\":1}"}"#);
}

// -----------------------------------------------------------------------
// Tests for issue #318: is_transient_error classification
// -----------------------------------------------------------------------

#[test]
fn test_is_transient_error_5xx() {
    assert!(is_transient_error(&ureq::Error::StatusCode(500)));
    assert!(is_transient_error(&ureq::Error::StatusCode(502)));
    assert!(is_transient_error(&ureq::Error::StatusCode(503)));
    assert!(is_transient_error(&ureq::Error::StatusCode(599)));
}

#[test]
fn test_is_transient_error_429() {
    assert!(is_transient_error(&ureq::Error::StatusCode(429)));
}

#[test]
fn test_is_transient_error_4xx_not_transient() {
    assert!(!is_transient_error(&ureq::Error::StatusCode(400)));
    assert!(!is_transient_error(&ureq::Error::StatusCode(401)));
    assert!(!is_transient_error(&ureq::Error::StatusCode(403)));
    assert!(!is_transient_error(&ureq::Error::StatusCode(404)));
}

#[test]
fn test_is_transient_error_network() {
    assert!(is_transient_error(&ureq::Error::HostNotFound));
    assert!(is_transient_error(&ureq::Error::ConnectionFailed));
}

#[test]
fn test_is_transient_error_non_retryable() {
    assert!(!is_transient_error(&ureq::Error::BadUri("bad".to_string())));
}

// ---------------------------------------------------------------------------
// Regression test for #1620: console format must show struct column values
// ---------------------------------------------------------------------------

/// `write_console()` must serialise Struct columns (e.g. from `grok()`) as
/// inline JSON rather than silently dropping them as empty strings.
#[test]
fn test_console_format_struct_column_rendered_as_json() {
    use arrow::array::{ArrayRef, StructArray};
    use arrow::datatypes::Fields;

    let method_field = Arc::new(Field::new("method", DataType::Utf8, true));
    let status_field = Arc::new(Field::new("status", DataType::Utf8, true));

    let method_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("GET"), Some("POST")]));
    let status_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("200"), Some("500")]));

    let struct_arr = StructArray::new(
        Fields::from(vec![method_field, status_field]),
        vec![method_arr, status_arr],
        None,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new(
            "parsed",
            DataType::Struct(Fields::from(vec![
                Field::new("method", DataType::Utf8, true),
                Field::new("status", DataType::Utf8, true),
            ])),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("ERROR"), Some("INFO")])),
            Arc::new(StringArray::from(vec![Some("request failed"), Some("ok")])),
            Arc::new(struct_arr),
        ],
    )
    .unwrap();

    let meta = make_metadata();
    let mut sink = StdoutSink::new(
        "test".to_string(),
        StdoutFormat::Console,
        Arc::new(ComponentStats::new()),
    );
    let mut out: Vec<u8> = Vec::new();
    sink.write_batch_to(&batch, &meta, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 2, "expected 2 output lines, got: {output:?}");

    assert!(
        lines[0].contains("parsed="),
        "expected 'parsed=' in: {}",
        lines[0]
    );
    assert!(
        lines[0].contains("\"method\""),
        "console format must render struct as JSON, got: {}",
        lines[0]
    );
    assert!(
        lines[0].contains("\"GET\""),
        "struct field value must be visible, got: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("\"POST\""),
        "row 1 struct field value must be visible, got: {}",
        lines[1]
    );
}

/// Console output currently skips null values for all field types.
/// This regression test asserts that behavior remains consistent for Struct:
/// null struct row is omitted, while non-null row renders as JSON.
#[test]
fn test_console_format_null_struct_cell() {
    use arrow::array::{ArrayRef, StructArray};
    use arrow::buffer::{BooleanBuffer, NullBuffer};
    use arrow::datatypes::Fields;

    let method_field = Arc::new(Field::new("method", DataType::Utf8, true));
    let method_arr: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, Some("POST")]));

    let nulls = NullBuffer::new(BooleanBuffer::collect_bool(2, |i| i == 1));
    let struct_arr = StructArray::new(
        Fields::from(vec![method_field]),
        vec![method_arr],
        Some(nulls),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, true),
        Field::new(
            "parsed",
            DataType::Struct(Fields::from(vec![Field::new(
                "method",
                DataType::Utf8,
                true,
            )])),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("WARN"), Some("INFO")])),
            Arc::new(struct_arr),
        ],
    )
    .unwrap();

    let meta = make_metadata();
    let mut sink = StdoutSink::new(
        "test".to_string(),
        StdoutFormat::Console,
        Arc::new(ComponentStats::new()),
    );
    let mut out: Vec<u8> = Vec::new();
    sink.write_batch_to(&batch, &meta, &mut out).unwrap();

    let output = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 2, "expected 2 output lines: {output:?}");

    assert!(
        !lines[0].contains("parsed="),
        "row 0 null struct should be omitted, got: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("\"POST\""),
        "row 1 non-null struct must render, got: {}",
        lines[1]
    );
}
