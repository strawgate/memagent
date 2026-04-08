//! Output sink trait and implementations for serializing Arrow RecordBatches
//! to various formats: stdout JSON/text, JSON lines over HTTP, OTLP protobuf.

mod arrow_ipc_sink;
mod file_sink;
mod json_lines;
mod null;
mod otap_sink;
mod otlp_sink;
pub mod sink;
mod stdout;
mod tcp_sink;
mod udp_sink;

pub mod error;

pub(crate) mod http_classify;

mod conflict_columns;
mod elasticsearch;
mod factory;
mod metadata;
mod row_json;

mod loki;

pub use arrow_ipc_sink::{ArrowIpcSinkFactory, deserialize_ipc, serialize_ipc};
pub use elasticsearch::{ElasticsearchRequestMode, ElasticsearchSink, ElasticsearchSinkFactory};
pub use error::OutputError;
pub use factory::build_sink_factory;
pub use file_sink::{FileSink, FileSinkFactory};
pub use json_lines::{JsonLinesSink, JsonLinesSinkFactory};
pub use loki::{LokiSink, LokiSinkFactory};
pub use metadata::{BatchMetadata, Compression};
pub use null::{NullSink, NullSinkFactory};
pub use otap_sink::{
    ArrowPayloadType, BatchStatus, DecodedPayload, OtapSinkFactory, StatusCode,
    decode_batch_arrow_records, decode_batch_arrow_records_generated_fast, decode_batch_status,
    decode_batch_status_generated_fast, encode_batch_arrow_records,
    encode_batch_arrow_records_generated_fast,
};
pub use otlp_sink::{OtlpProtocol, OtlpSink, OtlpSinkFactory};
pub use sink::{
    AsyncFanoutFactory, AsyncFanoutSink, OnceAsyncFactory, SendResult, Sink, SinkFactory,
};
pub use stdout::StdoutSinkFactory;
#[cfg(test)]
use stdout::*;
pub use tcp_sink::{TcpSink, TcpSinkFactory};
pub use udp_sink::{UdpSink, UdpSinkFactory};

pub use conflict_columns::{ColInfo, ColVariant, build_col_infos};
pub(crate) use conflict_columns::{get_array, is_null};
#[cfg(any(test, kani))]
#[allow(unused_imports)]
pub(crate) use conflict_columns::{is_conflict_struct, json_priority, str_priority, variant_dt};
pub(crate) use metadata::build_auth_headers;
pub use row_json::write_row_json;
pub(crate) use row_json::{coalesce_as_str, str_value};

#[cfg(test)]
use arrow::array::Array;
#[cfg(any(test, kani))]
use arrow::datatypes::DataType;
#[cfg(test)]
use arrow::record_batch::RecordBatch;
#[cfg(test)]
use logfwd_config::{Format, OutputConfig};

// ---------------------------------------------------------------------------
// HTTP retry helper
// ---------------------------------------------------------------------------

/// Returns `true` if the ureq error is transient and worth retrying.
///
/// Transient errors are: HTTP 429 Too Many Requests, 5xx server errors, and
/// network/transport failures (I/O, host not found, connection failed, timeout).
#[cfg(test)]
pub(crate) fn is_transient_error(e: &ureq::Error) -> bool {
    match e {
        ureq::Error::StatusCode(status) => *status == 429 || *status >= 500,
        ureq::Error::Io(_)
        | ureq::Error::HostNotFound
        | ureq::Error::ConnectionFailed
        | ureq::Error::Timeout(_) => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use logfwd_config::OutputType;
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
    fn test_raw_passthrough() {
        // Build a batch with only _raw column (simulating no transforms).
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
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
            Arc::new(ComponentStats::new()),
        );
        sink.serialize_batch(&batch).unwrap();

        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2);
        // Should be the original JSON, not re-serialized.
        assert_eq!(lines[0], r#"{"ts":"2024-01-15","msg":"hello"}"#);
        assert_eq!(lines[1], r#"{"ts":"2024-01-15","msg":"world"}"#);
    }

    #[test]
    fn test_stdout_text_format() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
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
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(status_s), Arc::new(status_i)]).unwrap();
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
        let factory =
            build_sink_factory("test", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
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
        let original_cwd = std::env::current_dir().unwrap();

        let cfg = OutputConfig {
            name: Some("capture".to_string()),
            output_type: OutputType::File,
            path: Some("capture.ndjson".to_string()),
            format: Some(Format::Json),
            ..Default::default()
        };

        std::env::set_current_dir(&cwd_dir).unwrap();
        let factory = build_sink_factory(
            "capture",
            &cfg,
            Some(&base_dir),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();
        std::env::set_current_dir(&original_cwd).unwrap();

        assert_eq!(factory.name(), "capture");
        assert!(base_dir.join("capture.ndjson").exists());
        assert!(!cwd_dir.join("capture.ndjson").exists());

        let _ = std::fs::remove_file(base_dir.join("capture.ndjson"));
        let _ = std::fs::remove_dir(&base_dir);
        let _ = std::fs::remove_dir(&cwd_dir);
    }

    #[test]
    fn test_build_sink_factory_file_rejects_compression() {
        let cfg = OutputConfig {
            name: Some("capture".to_string()),
            output_type: OutputType::File,
            path: Some("/tmp/capture.ndjson".to_string()),
            compression: Some("zstd".to_string()),
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
            protocol: Some("http".to_string()),
            compression: Some("gzip".to_string()),
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
            compression: Some("gzip".to_string()),
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
            compression: Some("zstd".to_string()),
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
    fn test_json_lines_raw_passthrough_no_panic() {
        // A batch that satisfies is_raw_passthrough but has no other null columns.
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let raw = StringArray::from(vec![Some(r#"{"x":1}"#)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(raw)]).unwrap();
        let mut sink = JsonLinesSink::new(
            "test-jsonl".to_string(),
            "http://localhost:9200".to_string(),
            reqwest::header::HeaderMap::new(),
            Compression::None,
            Arc::new(ComponentStats::new()),
        );
        // Must not panic.
        sink.serialize_batch(&batch).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        assert_eq!(output.trim(), r#"{"x":1}"#);
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
}

#[cfg(test)]
mod write_row_json_tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_batch(fields: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let schema = Schema::new(
            fields
                .iter()
                .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let arrays: Vec<Arc<dyn Array>> = fields.into_iter().map(|(_, a)| a).collect();
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn render(batch: &RecordBatch, row: usize) -> String {
        let cols = build_col_infos(batch);
        let mut out = Vec::new();
        write_row_json(batch, row, &cols, &mut out).expect("write_row_json failed");
        String::from_utf8(out).expect("output must be valid UTF-8")
    }

    #[test]
    fn basic_string_field() {
        let batch = make_batch(vec![("msg", Arc::new(StringArray::from(vec!["hello"])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["msg"], "hello");
    }

    #[test]
    fn integer_field() {
        let batch = make_batch(vec![("status", Arc::new(Int64Array::from(vec![200])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["status"], 200);
    }

    #[test]
    fn integer_null_emits_null_literal() {
        let batch = make_batch(vec![(
            "status",
            Arc::new(Int64Array::from(vec![Some(200), None])),
        )]);
        let json = render(&batch, 1);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert!(v["status"].is_null(), "null integer must serialize as null");
    }

    #[test]
    fn float_field() {
        let expected = std::f64::consts::PI;
        let batch = make_batch(vec![(
            "duration",
            Arc::new(Float64Array::from(vec![expected])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert!((v["duration"].as_f64().unwrap() - expected).abs() < 0.001);
    }

    #[test]
    fn null_values_preserved() {
        let batch = make_batch(vec![(
            "msg",
            Arc::new(StringArray::from(vec![Some("hello"), None])),
        )]);
        let json0 = render(&batch, 0);
        let json1 = render(&batch, 1);
        assert!(json0.contains("msg"));
        // Null field must be emitted as "field":null, not omitted entirely.
        let v1: serde_json::Value = serde_json::from_str(&json1).expect("row 1 must be valid JSON");
        assert!(
            v1["msg"].is_null(),
            "null field should be emitted as null, got {json1}"
        );
    }

    #[test]
    fn string_escaping_quotes_and_backslash() {
        let batch = make_batch(vec![(
            "msg",
            Arc::new(StringArray::from(vec![r#"say "hello" and \ more"#])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["msg"], r#"say "hello" and \ more"#);
    }

    #[test]
    fn string_escaping_control_chars() {
        // Null byte and other control chars must be \uXXXX escaped
        let input = "before\x00after\x01\x1f";
        let batch = make_batch(vec![("msg", Arc::new(StringArray::from(vec![input])))]);
        let json = render(&batch, 0);
        // Must be valid JSON
        let v: serde_json::Value =
            serde_json::from_str(&json).expect("control chars must be escaped");
        assert_eq!(v["msg"], "before\x00after\x01\x1f");
    }

    #[test]
    fn string_escaping_newline_tab_cr() {
        let batch = make_batch(vec![(
            "msg",
            Arc::new(StringArray::from(vec!["line1\nline2\ttab\rreturn"])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["msg"], "line1\nline2\ttab\rreturn");
    }

    /// Regression: field names containing `"`, `\`, and control characters must
    /// be JSON-escaped in the key position, not written raw.  A raw `"` in the
    /// key would produce `{"a"b": …}` — structurally invalid JSON.
    #[test]
    fn field_name_special_chars_are_escaped() {
        // Column name `a"b` (with a literal double-quote) must appear in the
        // output as the JSON key `"a\"b"`, not as raw `"a"b"`.
        let batch = make_batch(vec![(r#"a"b"#, Arc::new(StringArray::from(vec!["val"])))]);
        let json = render(&batch, 0);
        // The output must parse as valid JSON.
        let v: serde_json::Value =
            serde_json::from_str(&json).expect("field name with quote must produce valid JSON");
        // The unescaped key round-trips correctly.
        assert_eq!(v[r#"a"b"#], "val");
    }

    #[test]
    fn field_name_backslash_escaped() {
        let batch = make_batch(vec![(r"a\b", Arc::new(StringArray::from(vec!["val"])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value =
            serde_json::from_str(&json).expect("field name with backslash must produce valid JSON");
        assert_eq!(v[r"a\b"], "val");
    }

    #[test]
    fn field_name_control_char_escaped() {
        // A field name containing a newline must be \n-escaped in the key.
        let batch = make_batch(vec![("a\nb", Arc::new(StringArray::from(vec!["val"])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value =
            serde_json::from_str(&json).expect("field name with newline must produce valid JSON");
        assert_eq!(v["a\nb"], "val");
    }

    #[test]
    fn float_infinity_nan_emit_null() {
        let batch = make_batch(vec![(
            "val",
            Arc::new(Float64Array::from(vec![
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
                42.0,
            ])),
        )]);
        for row in 0..3 {
            let json = render(&batch, row);
            let v: serde_json::Value = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("row {row}: invalid JSON: {json} — {e}"));
            assert!(
                v["val"].is_null(),
                "row {row}: inf/nan should be null, got {}",
                v["val"]
            );
        }
        // Row 3 (42.0) should be a number
        let json = render(&batch, 3);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["val"], 42.0);
    }

    #[test]
    fn multiple_fields_valid_json() {
        let batch = make_batch(vec![
            ("level", Arc::new(StringArray::from(vec!["INFO"]))),
            ("status", Arc::new(Int64Array::from(vec![200]))),
            ("duration", Arc::new(Float64Array::from(vec![1.5]))),
        ]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["level"], "INFO");
        assert_eq!(v["status"], 200);
        assert_eq!(v["duration"], 1.5);
    }

    /// When a field has both int and str variants, the struct conflict column
    /// pattern preserves both.  The first non-null variant wins per row.
    #[test]
    fn mixed_type_struct_conflict_no_data_loss() {
        // Row 0: status.int=200, status.str=null → integer wins
        // Row 1: status.int=null, status.str="ok" → string wins
        let batch = make_int_str_struct_batch(vec![Some(200), None], vec![None, Some("ok")]);
        let json0 = render(&batch, 0);
        let v0: serde_json::Value = serde_json::from_str(&json0).unwrap();
        assert_eq!(v0["status"], 200, "row 0 should be integer 200");

        let json1 = render(&batch, 1);
        let v1: serde_json::Value = serde_json::from_str(&json1).unwrap();
        assert_eq!(v1["status"], "ok", "row 1 should be string 'ok'");
    }

    /// After SQL transforms, columns may have DataTypes that don't match
    /// their name suffix. Dispatch on DataType ensures correctness.
    #[test]
    fn sql_computed_column_dispatches_on_datatype() {
        // COUNT(*) produces Int64 column named "cnt" (no suffix).
        // Old code treated it as string (default arm). New code checks DataType.
        let batch = make_batch(vec![("cnt", Arc::new(Int64Array::from(vec![42])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            v["cnt"], 42,
            "computed int column should serialize as number, not string"
        );
    }

    /// Float values like 1.0 should stay as numbers, not become strings.
    #[test]
    fn float_roundtrip_preserves_type() {
        let batch = make_batch(vec![("score", Arc::new(Float64Array::from(vec![1.0])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            v["score"].is_number(),
            "1.0 should roundtrip as number, got {}",
            v["score"]
        );
    }

    /// Boolean-like values are stored as strings by the scanner. After a SQL
    /// CAST to boolean (which DataFusion might represent as UInt8 or Boolean),
    /// ensure they don't panic. Non-int/float types fall through to string.
    #[test]
    fn unknown_datatype_falls_through_to_string() {
        use arrow::array::BooleanArray;
        let batch = make_batch(vec![(
            "active",
            Arc::new(BooleanArray::from(vec![true])) as Arc<dyn Array>,
        )]);
        let json = render(&batch, 0);
        // Boolean columns go through str_value fallback, which returns ""
        // for non-Utf8 types. This is existing behavior — the point is no panic.
        let _: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
    }

    #[test]
    fn boolean_and_large_utf8_serialization() {
        use arrow::array::{BooleanArray, LargeStringArray};
        let batch = make_batch(vec![
            (
                "active",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            ),
            (
                "note",
                Arc::new(LargeStringArray::from(vec![
                    Some("large"),
                    Some("text"),
                    None,
                ])),
            ),
        ]);

        // Row 0: true, "large"
        let json0 = render(&batch, 0);
        let v0: serde_json::Value = serde_json::from_str(&json0).unwrap();
        assert_eq!(v0["active"], true);
        assert_eq!(v0["note"], "large");

        // Row 1: false, "text"
        let json1 = render(&batch, 1);
        let v1: serde_json::Value = serde_json::from_str(&json1).unwrap();
        assert_eq!(v1["active"], false);
        assert_eq!(v1["note"], "text");
    }

    /// Regression: `SELECT NULL AS empty_val` produces a DataType::Null column.
    /// Must serialize as JSON null, not empty string.
    #[test]
    fn null_literal_type_serializes_as_null() {
        use arrow::array::NullArray;
        let batch = make_batch(vec![(
            "empty_val",
            Arc::new(NullArray::new(1)) as Arc<dyn Array>,
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert!(
            v["empty_val"].is_null(),
            "DataType::Null should serialize as JSON null, got {json}"
        );
    }

    /// Regression: `ROW_NUMBER() OVER ()` and `COUNT(*)` produce UInt64 columns.
    /// Must serialize as JSON number, not empty string.
    #[test]
    fn uint64_serializes_as_number() {
        use arrow::array::UInt64Array;
        let batch = make_batch(vec![("row_num", Arc::new(UInt64Array::from(vec![1_u64])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(
            v["row_num"], 1,
            "UInt64 should serialize as JSON number, got {json}"
        );
    }

    /// Regression: `CAST(status AS INT)` produces an Int32 column.
    /// Must serialize as JSON number, not empty string.
    #[test]
    fn int32_serializes_as_number() {
        use arrow::array::Int32Array;
        let batch = make_batch(vec![("s", Arc::new(Int32Array::from(vec![42_i32])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(
            v["s"], 42,
            "Int32 should serialize as JSON number, got {json}"
        );
    }

    /// Regression: Float32 columns must serialize as JSON number, not empty string.
    #[test]
    fn float32_serializes_as_number() {
        use arrow::array::Float32Array;
        let batch = make_batch(vec![("val", Arc::new(Float32Array::from(vec![3.14_f32])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert!(
            v["val"].is_number(),
            "Float32 should serialize as JSON number, got {json}"
        );
        let diff = (v["val"].as_f64().unwrap() - 3.14_f64).abs();
        assert!(
            diff < 0.001,
            "Float32 value should be ~3.14, got {}",
            v["val"]
        );
    }

    /// Float32 infinity and NaN must emit JSON null (matches Float64 behavior).
    #[test]
    fn float32_nonfinite_emits_null() {
        use arrow::array::Float32Array;
        let batch = make_batch(vec![(
            "val",
            Arc::new(Float32Array::from(vec![f32::INFINITY, f32::NAN])),
        )]);
        for row in 0..2 {
            let json = render(&batch, row);
            let v: serde_json::Value = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("row {row}: invalid JSON: {json} — {e}"));
            assert!(
                v["val"].is_null(),
                "row {row}: Float32 inf/nan should be null, got {}",
                v["val"]
            );
        }
    }

    /// All small integer types (Int8, Int16, UInt8, UInt16, UInt32) must
    /// serialize as JSON numbers.
    #[test]
    fn small_integer_types_serialize_as_numbers() {
        use arrow::array::{Int8Array, Int16Array, UInt8Array, UInt16Array, UInt32Array};
        let batch = make_batch(vec![
            (
                "i8",
                Arc::new(Int8Array::from(vec![-1_i8])) as Arc<dyn Array>,
            ),
            ("i16", Arc::new(Int16Array::from(vec![1000_i16]))),
            ("u8", Arc::new(UInt8Array::from(vec![255_u8]))),
            ("u16", Arc::new(UInt16Array::from(vec![65535_u16]))),
            ("u32", Arc::new(UInt32Array::from(vec![1_000_000_u32]))),
        ]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["i8"], -1, "Int8 should be number");
        assert_eq!(v["i16"], 1000, "Int16 should be number");
        assert_eq!(v["u8"], 255, "UInt8 should be number");
        assert_eq!(v["u16"], 65535, "UInt16 should be number");
        assert_eq!(v["u32"], 1_000_000, "UInt32 should be number");
    }

    // -----------------------------------------------------------------------
    // Struct conflict column tests (production)
    // -----------------------------------------------------------------------

    /// Build a `status: Struct { int: Int64, str: Utf8 }` batch.
    fn make_int_str_struct_batch(
        int_vals: Vec<Option<i64>>,
        str_vals: Vec<Option<&str>>,
    ) -> RecordBatch {
        use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field, Fields, Schema};

        let int_field = Arc::new(Field::new("int", DataType::Int64, true));
        let str_field = Arc::new(Field::new("str", DataType::Utf8, true));
        let int_arr: ArrayRef = Arc::new(Int64Array::from(int_vals));
        let str_arr: ArrayRef = Arc::new(StringArray::from(str_vals));

        let n = int_arr.len();
        let nulls = NullBuffer::new(arrow::buffer::BooleanBuffer::collect_bool(n, |i| {
            !int_arr.is_null(i) || !str_arr.is_null(i)
        }));

        let struct_field = Field::new(
            "status",
            DataType::Struct(Fields::from(vec![
                int_field.as_ref().clone(),
                str_field.as_ref().clone(),
            ])),
            true,
        );
        let struct_arr: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![int_field, str_field]),
            vec![int_arr, str_arr],
            Some(nulls),
        ));

        RecordBatch::try_new(Arc::new(Schema::new(vec![struct_field])), vec![struct_arr]).unwrap()
    }

    fn make_duplicate_status_batch(struct_first: bool) -> RecordBatch {
        use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field, Fields, Schema};

        let int_field = Arc::new(Field::new("int", DataType::Int64, true));
        let str_field = Arc::new(Field::new("str", DataType::Utf8, true));
        let int_arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(200_i64)]));
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("OK")]));

        let nulls = NullBuffer::new(arrow::buffer::BooleanBuffer::collect_bool(1, |i| {
            !int_arr.is_null(i) || !str_arr.is_null(i)
        }));
        let struct_field = Field::new(
            "status",
            DataType::Struct(Fields::from(vec![
                int_field.as_ref().clone(),
                str_field.as_ref().clone(),
            ])),
            true,
        );
        let struct_arr: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![int_field, str_field]),
            vec![int_arr, str_arr],
            Some(nulls),
        ));
        let flat_field = Field::new("status", DataType::Utf8, true);
        let flat_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("flat-status")]));

        let (fields, arrays) = if struct_first {
            (vec![struct_field, flat_field], vec![struct_arr, flat_arr])
        } else {
            (vec![flat_field, struct_field], vec![flat_arr, struct_arr])
        };

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("duplicate-name test batch must be valid")
    }

    #[test]
    fn build_col_infos_struct_conflict_json_order() {
        // Struct with int+str → json_variants should have Int64 first.
        let batch = make_int_str_struct_batch(vec![Some(200)], vec![Some("OK")]);
        let cols = build_col_infos(&batch);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].field_name, "status");
        assert!(
            matches!(
                cols[0].json_variants[0],
                ColVariant::StructField {
                    dt: DataType::Int64,
                    ..
                }
            ),
            "json_variants[0] must be Int64"
        );
    }

    #[test]
    fn build_col_infos_struct_conflict_str_order() {
        // str_variants should have Utf8 first.
        let batch = make_int_str_struct_batch(vec![Some(200)], vec![Some("OK")]);
        let cols = build_col_infos(&batch);
        assert_eq!(cols.len(), 1);
        assert!(
            matches!(
                cols[0].str_variants[0],
                ColVariant::StructField {
                    dt: DataType::Utf8,
                    ..
                }
            ),
            "str_variants[0] must be Utf8"
        );
    }

    #[test]
    fn build_col_infos_merges_duplicate_name_when_struct_precedes_flat() {
        let batch = make_duplicate_status_batch(true);
        let cols = build_col_infos(&batch);
        assert_eq!(
            cols.len(),
            1,
            "duplicate field name should merge into one ColInfo"
        );
    }

    #[test]
    fn build_col_infos_merges_duplicate_name_when_flat_precedes_struct() {
        let batch = make_duplicate_status_batch(false);
        let cols = build_col_infos(&batch);
        assert_eq!(
            cols.len(),
            1,
            "duplicate field name should merge into one ColInfo"
        );
    }

    #[test]
    fn is_conflict_struct_rejects_mismatched_child_types() {
        use arrow::datatypes::Fields;
        let fields = Fields::from(vec![
            Field::new("int", DataType::Utf8, true),
            Field::new("str", DataType::Int64, true),
        ]);
        assert!(
            !is_conflict_struct(&fields),
            "name-only matching should not classify arbitrary structs as conflict structs"
        );
    }

    #[test]
    fn write_row_json_struct_int_emits_number() {
        // status: Struct{int=200, str=null} → {"status":200}
        let batch = make_int_str_struct_batch(vec![Some(200), None], vec![None, Some("OK")]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            v["status"].is_number(),
            "expected number, got {}",
            v["status"]
        );
        assert_eq!(v["status"].as_i64(), Some(200));
    }

    #[test]
    fn write_row_json_struct_str_emits_string() {
        // status: Struct{int=null, str="OK"} → {"status":"OK"}
        let batch = make_int_str_struct_batch(vec![Some(200), None], vec![None, Some("OK")]);
        let json = render(&batch, 1);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            v["status"].is_string(),
            "expected string, got {}",
            v["status"]
        );
        assert_eq!(v["status"].as_str(), Some("OK"));
    }

    #[test]
    fn write_row_json_flat_column_unchanged() {
        // Plain Utf8 column works as before (no struct involved).
        let batch = make_batch(vec![("level", Arc::new(StringArray::from(vec!["INFO"])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["level"], "INFO");
    }

    /// Regression for #705: user-defined SQL aliases ending in `_int`, `_str`,
    /// or `_float` must appear verbatim in JSON output, not be stripped.
    #[test]
    fn user_defined_alias_preserved_verbatim() {
        let batch = make_batch(vec![
            ("dur_int", Arc::new(Int64Array::from(vec![42]))),
            ("label_str", Arc::new(StringArray::from(vec!["hello"]))),
            ("score_float", Arc::new(Float64Array::from(vec![3.14]))),
        ]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        // Aliases must NOT be mangled — "dur_int" stays "dur_int", not "dur".
        assert_eq!(
            v["dur_int"], 42,
            "alias 'dur_int' must be preserved, got: {json}"
        );
        assert_eq!(
            v["label_str"], "hello",
            "alias 'label_str' must be preserved, got: {json}"
        );
        assert!(
            (v["score_float"].as_f64().unwrap() - 3.14).abs() < 0.01,
            "alias 'score_float' must be preserved, got: {json}"
        );
    }
}

#[cfg(test)]
mod write_row_json_proptests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn render_row(batch: &RecordBatch, row: usize) -> String {
        let cols = build_col_infos(batch);
        let mut out = Vec::new();
        write_row_json(batch, row, &cols, &mut out).expect("write_row_json failed");
        String::from_utf8(out).expect("output must be valid UTF-8")
    }

    proptest! {
        /// Every output of write_row_json must be valid JSON.
        #[test]
        fn prop_write_row_json_always_valid_json(
            values in prop::collection::vec(any::<Option<i64>>(), 1..20usize),
        ) {
            let schema = Arc::new(Schema::new(vec![
                Field::new("count", DataType::Int64, true),
            ]));
            let arr: Int64Array = values.iter().copied().collect();
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(arr)],
            ).unwrap();

            for row in 0..batch.num_rows() {
                let json_str = render_row(&batch, row);
                let parsed: serde_json::Value = serde_json::from_str(&json_str)
                    .unwrap_or_else(|_| panic!("row {row} must produce valid JSON, got: {json_str}"));
                prop_assert!(parsed.is_object(), "output must be a JSON object");
            }
        }

        /// Column names are preserved verbatim — no suffix stripping.
        #[test]
        fn prop_int_field_name_preserved_verbatim(
            raw_values in prop::collection::vec(any::<i64>(), 1..5usize),
            // Only lowercase ASCII + underscore, no leading digit
            field_base in "[a-z][a-z0-9_]{0,15}",
        ) {
            let field_name = format!("{field_base}_int");
            let schema = Arc::new(Schema::new(vec![
                Field::new(&field_name, DataType::Int64, false),
            ]));
            let arr = Int64Array::from(raw_values.clone());
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

            for (row, &expected_val) in raw_values.iter().enumerate() {
                let json_str = render_row(&batch, row);
                let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
                let obj = parsed.as_object().unwrap();
                // Column name must appear verbatim (no suffix stripping)
                prop_assert!(
                    obj.contains_key(field_name.as_str()),
                    "field '{field_name}' must be present verbatim; got keys: {:?}",
                    obj.keys().collect::<Vec<_>>()
                );
                // Value must round-trip
                prop_assert_eq!(
                    obj[field_name.as_str()].as_i64(),
                    Some(expected_val),
                    "integer value must round-trip"
                );
            }
        }

        /// Column names are preserved verbatim — no suffix stripping for _str.
        #[test]
        fn prop_str_field_name_preserved_verbatim(
            raw_values in prop::collection::vec(
                proptest::option::of("[^\n\r]{0,50}"),
                1..5usize,
            ),
            field_base in "[a-z][a-z0-9_]{0,15}",
        ) {
            let field_name = format!("{field_base}_str");
            let schema = Arc::new(Schema::new(vec![
                Field::new(&field_name, DataType::Utf8, true),
            ]));
            let arr: StringArray = raw_values.iter().map(|v| v.as_deref()).collect();
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

            for (row, expected_val) in raw_values.iter().enumerate() {
                let json_str = render_row(&batch, row);
                let parsed: serde_json::Value = serde_json::from_str(&json_str)
                    .unwrap_or_else(|_| panic!("row {row}: invalid JSON: {json_str}"));
                let obj = parsed.as_object().unwrap();
                prop_assert!(
                    obj.contains_key(field_name.as_str()),
                    "field '{field_name}' must be present verbatim"
                );
                match expected_val {
                    Some(s) => prop_assert_eq!(
                        obj[field_name.as_str()].as_str(),
                        Some(s.as_str()),
                        "string value must round-trip"
                    ),
                    None => prop_assert!(
                        obj[field_name.as_str()].is_null(),
                        "null must serialize as JSON null"
                    ),
                }
            }
        }

        /// float field round-trip: column name preserved verbatim, value preserved.
        #[test]
        fn prop_float_field_roundtrip(
            raw_values in prop::collection::vec(
                // Exclude NaN and inf which don't round-trip through JSON
                (-1e15f64..1e15f64).prop_filter("finite", |v| v.is_finite()),
                1..5usize,
            ),
            field_base in "[a-z][a-z0-9_]{0,15}",
        ) {
            let field_name = format!("{field_base}_float");
            let schema = Arc::new(Schema::new(vec![
                Field::new(&field_name, DataType::Float64, false),
            ]));
            let arr = Float64Array::from(raw_values.clone());
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

            for (row, &expected_val) in raw_values.iter().enumerate() {
                let json_str = render_row(&batch, row);
                let parsed: serde_json::Value = serde_json::from_str(&json_str)
                    .unwrap_or_else(|_| panic!("row {row}: invalid JSON: {json_str}"));
                let obj = parsed.as_object().unwrap();
                prop_assert!(obj.contains_key(field_name.as_str()));
                let got = obj[field_name.as_str()].as_f64().unwrap();
                // Allow small floating-point round-trip epsilon
                prop_assert!(
                    (got - expected_val).abs() <= expected_val.abs() * 1e-10 + 1e-10,
                    "float value diverged: expected {expected_val}, got {got}"
                );
            }
        }

        /// No internal newlines in JSON document output (bulk format requires single-line docs).
        #[test]
        fn prop_no_internal_newlines(
            // Include values with embedded newlines to ensure they're escaped
            values in prop::collection::vec(
                proptest::option::of(".*"),
                1..10usize,
            ),
        ) {
            let schema = Arc::new(Schema::new(vec![
                Field::new("msg", DataType::Utf8, true),
            ]));
            let arr: StringArray = values.iter().map(|v| v.as_deref()).collect();
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
            let cols = build_col_infos(&batch);
            let mut out = Vec::new();
            for row in 0..batch.num_rows() {
                out.clear();
                write_row_json(&batch, row, &cols, &mut out).unwrap();
                let s = String::from_utf8(out.clone()).unwrap();
                prop_assert!(
                    !s.contains('\n') && !s.contains('\r'),
                    "row {row}: JSON output must not contain unescaped newlines, got: {s:?}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod kani_proofs {
    use super::*;
    use arrow::datatypes::Fields;

    /// Empty struct is not a conflict struct — the guard requires at least one child.
    #[kani::proof]
    fn verify_is_conflict_struct_empty_returns_false() {
        let fields = Fields::empty();
        assert!(!is_conflict_struct(&fields));
        kani::cover!(true, "empty fields path exercised");
    }

    /// JSON priority ordering: Int64 wins over Float64 wins over Utf8.
    /// This is the core contract that drives per-row type-preserving serialization.
    #[kani::proof]
    fn verify_json_priority_total_order() {
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Float64));
        assert!(json_priority(&DataType::Float64) > json_priority(&DataType::Utf8));
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Utf8));
        kani::cover!(
            json_priority(&DataType::Int64) > json_priority(&DataType::Utf8),
            "int beats utf8"
        );
    }

    /// String-coalesce priority: Utf8/Utf8View wins over numeric types.
    /// Loki labels and other string consumers depend on this contract.
    #[kani::proof]
    fn verify_str_priority_string_beats_numerics() {
        assert!(str_priority(&DataType::Utf8) > str_priority(&DataType::Int64));
        assert!(str_priority(&DataType::Utf8View) > str_priority(&DataType::Float64));
        assert!(str_priority(&DataType::Utf8) == str_priority(&DataType::Utf8View));
        kani::cover!(
            str_priority(&DataType::Utf8) > str_priority(&DataType::Int64),
            "utf8 beats int64"
        );
    }

    /// json_priority and str_priority assign different orderings — they are not equal.
    /// This guards against accidentally returning the same function for both callers.
    #[kani::proof]
    fn verify_json_and_str_priority_differ_for_int_vs_utf8() {
        // In JSON mode, Int64 wins; in string mode, Utf8 wins.
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Utf8));
        assert!(str_priority(&DataType::Utf8) > str_priority(&DataType::Int64));
        kani::cover!(
            true,
            "ordering inversion between json and str priority verified"
        );
    }

    /// ColVariant::Flat preserves its col_idx and DataType exactly.
    #[kani::proof]
    fn verify_col_variant_flat_preserves_idx_and_type() {
        let col_idx: usize = kani::any();
        let v = ColVariant::Flat {
            col_idx,
            dt: DataType::Int64,
        };
        if let ColVariant::Flat { col_idx: idx, dt } = v {
            assert_eq!(idx, col_idx);
            assert_eq!(dt, DataType::Int64);
        }
        kani::cover!(col_idx > 0, "non-zero col_idx");
        kani::cover!(col_idx == 0, "zero col_idx");
    }

    /// ColVariant::StructField preserves struct_col_idx, field_idx, and DataType.
    #[kani::proof]
    fn verify_col_variant_struct_field_preserves_indices() {
        let struct_col_idx: usize = kani::any();
        let field_idx: usize = kani::any();
        let v = ColVariant::StructField {
            struct_col_idx,
            field_idx,
            dt: DataType::Utf8,
        };
        if let ColVariant::StructField {
            struct_col_idx: sci,
            field_idx: fi,
            dt,
        } = v
        {
            assert_eq!(sci, struct_col_idx);
            assert_eq!(fi, field_idx);
            assert_eq!(dt, DataType::Utf8);
        }
        kani::cover!(struct_col_idx > 0, "non-zero struct_col_idx");
        kani::cover!(field_idx > 0, "non-zero field_idx");
    }

    /// variant_dt extracts the DataType from a Flat variant correctly.
    #[kani::proof]
    fn verify_variant_dt_flat() {
        let v = ColVariant::Flat {
            col_idx: 0,
            dt: DataType::Int64,
        };
        assert_eq!(variant_dt(&v), &DataType::Int64);
        kani::cover!(true, "variant_dt(Flat) returns correct type");
    }

    /// variant_dt extracts the DataType from a StructField variant correctly.
    #[kani::proof]
    fn verify_variant_dt_struct_field() {
        let v = ColVariant::StructField {
            struct_col_idx: 0,
            field_idx: 1,
            dt: DataType::Float64,
        };
        assert_eq!(variant_dt(&v), &DataType::Float64);
        kani::cover!(true, "variant_dt(StructField) returns correct type");
    }
}
