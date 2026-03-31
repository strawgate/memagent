//! Output sink trait and implementations for serializing Arrow RecordBatches
//! to various formats: stdout JSON/text, JSON lines over HTTP, OTLP protobuf.

mod fanout;
mod json_lines;
mod otlp_sink;
pub mod sink;
mod stdout;

// Placeholder sinks — not yet wired into build_output_sink.
#[allow(dead_code)]
mod elasticsearch;
#[allow(dead_code)]
mod loki;
#[allow(dead_code)]
mod parquet;

pub use fanout::FanOut;
pub use json_lines::JsonLinesSink;
pub use otlp_sink::{OtlpProtocol, OtlpSink};
use stdout::*;

use std::io::{self, Write};

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use logfwd_config::{AuthConfig, Format, OutputConfig, OutputType};

// ---------------------------------------------------------------------------
// HTTP retry helper
// ---------------------------------------------------------------------------

/// Maximum number of retry attempts for transient HTTP failures.
pub(crate) const HTTP_MAX_RETRIES: u32 = 3;
/// Initial retry delay in milliseconds; doubles on each subsequent attempt.
pub(crate) const HTTP_RETRY_INITIAL_DELAY_MS: u64 = 100;

/// Returns `true` if the ureq error is transient and worth retrying.
///
/// Transient errors are: HTTP 429 Too Many Requests, 5xx server errors, and
/// network/transport failures (I/O, host not found, connection failed, timeout).
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
// Trait + metadata
// ---------------------------------------------------------------------------

/// Metadata about the batch for output serialization.
pub struct BatchMetadata {
    /// Resource attributes (k8s pod name, namespace, etc.)
    pub resource_attrs: Vec<(String, String)>,
    /// Observed timestamp in nanoseconds.
    pub observed_time_ns: u64,
}

/// Every output implements this trait.
pub trait OutputSink: Send {
    /// Serialize and send a batch.
    fn send_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()>;
    /// Flush any buffered data.
    fn flush(&mut self) -> io::Result<()>;
    /// Output name (from config).
    fn name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Column naming helpers
// ---------------------------------------------------------------------------

/// Parse a typed column name into (field_name, type_suffix).
///
/// "duration_ms$int" -> ("duration_ms", "int")
/// "level$str"       -> ("level", "str")
/// "_raw"            -> ("_raw", "")
pub fn parse_column_name(col_name: &str) -> (&str, &str) {
    if let Some(pos) = col_name.rfind('$') {
        let suffix = &col_name[pos + 1..];
        if suffix == "str" || suffix == "int" || suffix == "float" {
            return (&col_name[..pos], suffix);
        }
    }
    (col_name, "")
}

// ---------------------------------------------------------------------------
// JSON serialization helpers (shared by StdoutSink and JsonLinesSink)
// ---------------------------------------------------------------------------

/// Describes one output logical field: its name, and the list of Arrow column
/// indices and their type suffixes that contribute to this field.
pub(crate) struct ColInfo {
    field_name: String,
    variants: Vec<(usize, String)>,
    /// Minimum original column index among all variants, used for stable sorting.
    min_idx: usize,
}

/// Build a grouped list of columns for JSON output.
/// When the same field_name appears multiple times (e.g. status$int and
/// status$str), they are grouped under the same logical field.
pub(crate) fn build_col_infos(batch: &RecordBatch) -> Vec<ColInfo> {
    use std::collections::BTreeMap;
    let schema = batch.schema();
    // Use BTreeMap to group by field_name while maintaining some order.
    let mut groups: BTreeMap<String, Vec<(usize, String)>> = BTreeMap::new();
    let mut min_indices: BTreeMap<String, usize> = BTreeMap::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().as_str();
        let (field_name, type_suffix) = parse_column_name(name);
        groups
            .entry(field_name.to_string())
            .or_default()
            .push((idx, type_suffix.to_string()));
        min_indices
            .entry(field_name.to_string())
            .and_modify(|m| *m = (*m).min(idx))
            .or_insert(idx);
    }

    // Priority: int > float > str > untyped
    fn type_priority(suffix: &str) -> u8 {
        match suffix {
            "int" => 3,
            "float" => 2,
            "str" => 1,
            _ => 0,
        }
    }

    let mut infos: Vec<ColInfo> = groups
        .into_iter()
        .map(|(field_name, mut variants)| {
            // Sort variants by priority within each field.
            variants.sort_by(|a, b| type_priority(&b.1).cmp(&type_priority(&a.1)));
            let min_idx = min_indices[&field_name];
            ColInfo {
                field_name,
                variants,
                min_idx,
            }
        })
        .collect();

    // Re-sort by original minimum column index to maintain stable output order.
    infos.sort_by_key(|c| c.min_idx);
    infos
}

/// Read a string value from a column at the given row.
///
/// Supports both `Utf8` (StringArray) and `Utf8View` (StringViewArray) columns,
/// allowing output sinks to work transparently with either scanner.
pub(crate) fn str_value(col: &dyn Array, row: usize) -> &str {
    match col.data_type() {
        DataType::Utf8 => col.as_string::<i32>().value(row),
        DataType::Utf8View => col.as_string_view().value(row),
        _ => "",
    }
}

/// Write a single row as a JSON object into `out`.
pub(crate) fn write_row_json(batch: &RecordBatch, row: usize, cols: &[ColInfo], out: &mut Vec<u8>) {
    out.push(b'{');
    let mut first = true;
    for col in cols {
        // Find the first variant (by priority) that is not null for this row.
        let mut best_variant = None;
        for (idx, suffix) in &col.variants {
            if !batch.column(*idx).is_null(row) {
                best_variant = Some((*idx, suffix.as_str()));
                break;
            }
        }

        let Some((idx, suffix)) = best_variant else {
            continue;
        };

        if !first {
            out.push(b',');
        }
        first = false;
        // Key
        out.push(b'"');
        out.extend_from_slice(col.field_name.as_bytes());
        out.push(b'"');
        out.push(b':');
        // Value — type-aware
        let arr = batch.column(idx);
        match suffix {
            "int" => {
                let arr = arr.as_primitive::<arrow::datatypes::Int64Type>();
                let v = arr.value(row);
                // Write integer directly; no quotes in JSON.
                let _ = Write::write_fmt(out, format_args!("{}", v));
            }
            "float" => {
                let arr = arr.as_primitive::<arrow::datatypes::Float64Type>();
                let v = arr.value(row);
                // RFC 8259: JSON numbers cannot be inf, -inf, or NaN.
                // Emit as null instead of producing invalid JSON.
                if v.is_finite() {
                    let _ = Write::write_fmt(out, format_args!("{}", v));
                } else {
                    out.extend_from_slice(b"null");
                }
            }
            _ => {
                // str or untyped — treat as string (Utf8 or Utf8View)
                let v = str_value(arr, row);
                out.push(b'"');
                // JSON string escape per RFC 8259
                for &b in v.as_bytes() {
                    match b {
                        b'"' => out.extend_from_slice(b"\\\""),
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        b'\t' => out.extend_from_slice(b"\\t"),
                        b if b < 0x20 => {
                            // Escape control characters per RFC 8259
                            let _ = Write::write_fmt(out, format_args!("\\u{:04x}", b));
                        }
                        _ => out.push(b),
                    }
                }
                out.push(b'"');
            }
        }
    }
    out.push(b'}');
}

/// Compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Zstd,
    Gzip,
    None,
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Build a flat list of HTTP headers from an [`AuthConfig`].
///
/// - `bearer_token` produces `Authorization: Bearer <token>`.
/// - `headers` entries are appended as-is.
fn build_auth_headers(auth: Option<&AuthConfig>) -> Vec<(String, String)> {
    let Some(auth) = auth else {
        return Vec::new();
    };
    let mut headers: Vec<(String, String)> = Vec::new();
    if let Some(token) = &auth.bearer_token {
        headers.push(("Authorization".to_string(), format!("Bearer {token}")));
    }
    for (k, v) in &auth.headers {
        headers.push((k.clone(), v.clone()));
    }
    headers
}

// ---------------------------------------------------------------------------
// Output construction (factory)
// ---------------------------------------------------------------------------

/// Build an output sink from configuration.
pub fn build_output_sink(name: &str, cfg: &OutputConfig) -> Result<Box<dyn OutputSink>, String> {
    let auth_headers = build_auth_headers(cfg.auth.as_ref());
    match cfg.output_type {
        OutputType::Stdout => {
            let fmt = match cfg.format.as_ref() {
                Some(Format::Json) => StdoutFormat::Json,
                Some(Format::Console) => StdoutFormat::Console,
                _ => StdoutFormat::Text,
            };
            Ok(Box::new(StdoutSink::new(name.to_string(), fmt)))
        }
        OutputType::Otlp => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': OTLP requires 'endpoint'"))?;
            let protocol = match cfg.protocol.as_deref() {
                Some("grpc") => OtlpProtocol::Grpc,
                _ => OtlpProtocol::Http,
            };
            let compression = match cfg.compression.as_deref() {
                Some("zstd") => Compression::Zstd,
                Some("gzip") => Compression::Gzip,
                _ => Compression::None,
            };
            Ok(Box::new(OtlpSink::new(
                name.to_string(),
                endpoint.clone(),
                protocol,
                compression,
                auth_headers,
            )))
        }
        OutputType::Http => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': HTTP requires 'endpoint'"))?;
            Ok(Box::new(JsonLinesSink::new(
                name.to_string(),
                endpoint.clone(),
                auth_headers,
            )))
        }
        _ => Err(format!(
            "output '{name}': type {:?} not yet supported",
            cfg.output_type
        )),
    }
}

// ---------------------------------------------------------------------------
// Test helper
// ---------------------------------------------------------------------------

/// Test helper: captures all sent batches for assertion.
#[cfg(test)]
pub struct CaptureSink {
    name: String,
    pub batches: Vec<RecordBatch>,
}

#[cfg(test)]
impl CaptureSink {
    pub fn new(name: &str) -> Self {
        CaptureSink {
            name: name.to_string(),
            batches: Vec::new(),
        }
    }
}

#[cfg(test)]
impl OutputSink for CaptureSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.batches.push(batch.clone());
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level$str", DataType::Utf8, true),
            Field::new("status$int", DataType::Int64, true),
        ]));
        let level = StringArray::from(vec![Some("ERROR"), Some("INFO")]);
        let status = Int64Array::from(vec![Some(500), Some(200)]);
        RecordBatch::try_new(schema, vec![Arc::new(level), Arc::new(status)]).unwrap()
    }

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: vec![],
            observed_time_ns: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn test_parse_column_name() {
        assert_eq!(parse_column_name("status$int"), ("status", "int"));
        assert_eq!(parse_column_name("level$str"), ("level", "str"));
        assert_eq!(
            parse_column_name("duration_ms$float"),
            ("duration_ms", "float")
        );
        assert_eq!(parse_column_name("_raw"), ("_raw", ""));
        assert_eq!(parse_column_name("plain"), ("plain", ""));
    }

    #[test]
    fn test_stdout_json() {
        let batch = make_test_batch();
        let meta = make_metadata();
        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();

        let output = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();
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
    fn test_fanout() {
        // FanOut to two sinks that write to Vec<u8>.
        // We use StdoutSink with write_batch_to to capture output.
        let batch = make_test_batch();
        let meta = make_metadata();

        let mut sink1 = StdoutSink::new("s1".to_string(), StdoutFormat::Json);
        let mut sink2 = StdoutSink::new("s2".to_string(), StdoutFormat::Json);

        let mut out1: Vec<u8> = Vec::new();
        let mut out2: Vec<u8> = Vec::new();

        sink1.write_batch_to(&batch, &meta, &mut out1).unwrap();
        sink2.write_batch_to(&batch, &meta, &mut out2).unwrap();

        // Both should have identical output.
        assert_eq!(out1, out2);
        assert!(!out1.is_empty());

        // Also test FanOut trait dispatch works.
        let fanout_s1 = StdoutSink::new("f1".to_string(), StdoutFormat::Json);
        let fanout_s2 = StdoutSink::new("f2".to_string(), StdoutFormat::Json);
        let mut fanout = FanOut::new(vec![Box::new(fanout_s1), Box::new(fanout_s2)]);
        // send_batch writes to real stdout, but should not error.
        let result = fanout.send_batch(&batch, &meta);
        assert!(result.is_ok());
    }

    #[test]
    fn test_otlp_encoding() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp$str", DataType::Utf8, true),
            Field::new("level$str", DataType::Utf8, true),
            Field::new("message$str", DataType::Utf8, true),
            Field::new("status$int", DataType::Int64, true),
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
            resource_attrs: vec![("k8s.pod.name".to_string(), "myapp-abc".to_string())],
            observed_time_ns: 1_700_000_000_000_000_000,
        };

        let mut sink = OtlpSink::new(
            "test-otlp".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
        );
        sink.encode_batch(&batch, &meta);

        // Should produce non-empty protobuf bytes.
        assert!(!sink.encoder_buf.is_empty());
        // First byte should be tag for field 1 (ResourceLogs), wire type 2 = 0x0A
        assert_eq!(sink.encoder_buf[0], 0x0A);
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
            vec![],
        );
        sink.serialize_batch(&batch).unwrap();

        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        // Should be the original JSON, not re-serialized.
        assert_eq!(lines[0], r#"{"ts":"2024-01-15","msg":"hello"}"#);
        assert_eq!(lines[1], r#"{"ts":"2024-01-15","msg":"world"}"#);
    }

    #[test]
    fn test_stdout_text_format() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
            Field::new("level$str", DataType::Utf8, true),
        ]));
        let raw = StringArray::from(vec![Some("original log line")]);
        let level = StringArray::from(vec![Some("INFO")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(raw), Arc::new(level)]).unwrap();
        let meta = make_metadata();

        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Text);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();
        let output = String::from_utf8(out).unwrap();
        assert_eq!(output.trim(), "original log line");
    }

    #[test]
    fn test_type_preference_dedup() {
        // When both status$int and status$str exist, int should win.
        let schema = Arc::new(Schema::new(vec![
            Field::new("status$str", DataType::Utf8, true),
            Field::new("status$int", DataType::Int64, true),
        ]));
        let status_s = StringArray::from(vec![Some("500")]);
        let status_i = Int64Array::from(vec![Some(500)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(status_s), Arc::new(status_i)]).unwrap();
        let meta = make_metadata();

        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();
        let output = String::from_utf8(out).unwrap();
        // Should have integer 500, not string "500"
        assert!(output.contains("\"status\":500"), "got: {}", output);
        // Should NOT have the string version
        assert!(!output.contains("\"status\":\"500\""), "got: {}", output);
    }

    #[test]
    fn test_float_column_json() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration_ms$float",
            DataType::Float64,
            true,
        )]));
        let dur = Float64Array::from(vec![Some(3.25)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dur)]).unwrap();
        let meta = make_metadata();

        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("\"duration_ms\":3.25"), "got: {}", output);
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
    fn test_build_output_sink_http_with_bearer_auth() {
        use logfwd_config::AuthConfig;
        let cfg = OutputConfig {
            name: Some("auth-sink".to_string()),
            output_type: OutputType::Http,
            endpoint: Some("http://localhost:9200".to_string()),
            protocol: None,
            compression: None,
            format: None,
            path: None,
            auth: Some(AuthConfig {
                bearer_token: Some("mytoken".to_string()),
                headers: std::collections::HashMap::new(),
            }),
        };
        let sink = build_output_sink("auth-sink", &cfg).unwrap();
        assert_eq!(sink.name(), "auth-sink");
    }

    // -----------------------------------------------------------------------
    // Tests for issue #285/#316: OTLP type dispatch uses actual DataType
    // -----------------------------------------------------------------------

    #[test]
    fn test_otlp_type_mismatch_no_panic() {
        // Column named "count$int" but actually contains strings.
        // Before the fix this would panic in as_primitive::<Int64Type>().
        // After the fix it falls through to AttrArray::Str.
        let schema = Arc::new(Schema::new(vec![
            Field::new("count$int", DataType::Utf8, true),
            Field::new("message$str", DataType::Utf8, true),
        ]));
        let count = StringArray::from(vec![Some("high")]);
        let msg = StringArray::from(vec![Some("something happened")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(count), Arc::new(msg)]).unwrap();
        let meta = BatchMetadata {
            resource_attrs: vec![],
            observed_time_ns: 1_700_000_000_000_000_000,
        };
        let mut sink = OtlpSink::new(
            "test-otlp".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
        );
        // Must not panic.
        sink.encode_batch(&batch, &meta);
        assert!(!sink.encoder_buf.is_empty());
    }

    #[test]
    fn test_otlp_real_int_column_encoded() {
        // Column named "status$str" but actually Int64: should be encoded as int attr.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status$str",
            DataType::Int64,
            true,
        )]));
        let status = Int64Array::from(vec![Some(200i64)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(status)]).unwrap();
        let meta = BatchMetadata {
            resource_attrs: vec![],
            observed_time_ns: 1_700_000_000_000_000_000,
        };
        let mut sink = OtlpSink::new(
            "test-otlp".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
        );
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
            vec![],
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
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_batch(fields: Vec<(&str, Arc<dyn arrow::array::Array>)>) -> RecordBatch {
        let schema = Schema::new(
            fields
                .iter()
                .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let arrays: Vec<Arc<dyn arrow::array::Array>> =
            fields.into_iter().map(|(_, a)| a).collect();
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn render(batch: &RecordBatch, row: usize) -> String {
        let cols = build_col_infos(batch);
        let mut out = Vec::new();
        write_row_json(batch, row, &cols, &mut out);
        String::from_utf8(out).expect("output must be valid UTF-8")
    }

    #[test]
    fn basic_string_field() {
        let batch = make_batch(vec![(
            "msg_str",
            Arc::new(StringArray::from(vec!["hello"])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["msg"], "hello");
    }

    #[test]
    fn integer_field() {
        let batch = make_batch(vec![("status_int", Arc::new(Int64Array::from(vec![200])))]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["status"], 200);
    }

    #[test]
    fn float_field() {
        let batch = make_batch(vec![(
            "duration_float",
            Arc::new(Float64Array::from(vec![3.14])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert!((v["duration"].as_f64().unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn null_values_skipped() {
        let batch = make_batch(vec![(
            "msg_str",
            Arc::new(StringArray::from(vec![Some("hello"), None])),
        )]);
        let json0 = render(&batch, 0);
        let json1 = render(&batch, 1);
        assert!(json0.contains("msg"));
        assert_eq!(json1, "{}"); // null skipped entirely
    }

    #[test]
    fn string_escaping_quotes_and_backslash() {
        let batch = make_batch(vec![(
            "msg_str",
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
        let batch = make_batch(vec![("msg_str", Arc::new(StringArray::from(vec![input])))]);
        let json = render(&batch, 0);
        // Must be valid JSON
        let v: serde_json::Value =
            serde_json::from_str(&json).expect("control chars must be escaped");
        assert_eq!(v["msg"], "before\x00after\x01\x1f");
    }

    #[test]
    fn string_escaping_newline_tab_cr() {
        let batch = make_batch(vec![(
            "msg_str",
            Arc::new(StringArray::from(vec!["line1\nline2\ttab\rreturn"])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["msg"], "line1\nline2\ttab\rreturn");
    }

    #[test]
    fn float_infinity_nan_emit_null() {
        let batch = make_batch(vec![(
            "val_float",
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
            ("level_str", Arc::new(StringArray::from(vec!["INFO"]))),
            ("status_int", Arc::new(Int64Array::from(vec![200]))),
            ("duration_float", Arc::new(Float64Array::from(vec![1.5]))),
        ]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert_eq!(v["level"], "INFO");
        assert_eq!(v["status"], 200);
        assert_eq!(v["duration"], 1.5);
    }
}
