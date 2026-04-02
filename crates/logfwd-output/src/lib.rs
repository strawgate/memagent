//! Output sink trait and implementations for serializing Arrow RecordBatches
//! to various formats: stdout JSON/text, JSON lines over HTTP, OTLP protobuf.

mod fanout;
mod json_lines;
mod null;
mod otlp_sink;
pub mod sink;
mod stdout;
mod tcp_sink;
mod udp_sink;

mod elasticsearch;

mod loki;
#[allow(dead_code)]
mod parquet;

pub use elasticsearch::{ElasticsearchAsyncSink, ElasticsearchSink, ElasticsearchSinkFactory};
pub use fanout::{FanOut, FanOutError};
pub use json_lines::JsonLinesSink;
pub use loki::{LokiAsyncSink, LokiSinkFactory};
pub use null::NullSink;
pub use otlp_sink::{OtlpProtocol, OtlpSink};
pub use sink::{SendResult, Sink, SinkFactory, SyncSinkAdapter};
use stdout::*;
pub use tcp_sink::TcpSink;
pub use udp_sink::UdpSink;

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use logfwd_config::{AuthConfig, Format, OutputConfig, OutputType};
use logfwd_io::diagnostics::ComponentStats;

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
    /// Resource attributes (k8s pod name, namespace, etc.) — Arc so cloning is cheap.
    pub resource_attrs: Arc<Vec<(String, String)>>,
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
/// "duration_ms_int" -> ("duration_ms", "int")
/// "level_str"       -> ("level", "str")
/// "_raw"            -> ("_raw", "")
pub fn parse_column_name(col_name: &str) -> (&str, &str) {
    if let Some(pos) = col_name.rfind('_') {
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

/// Describes one output field, potentially backed by multiple Arrow columns.
///
/// A JSON field like "status" that had both integer and string values across
/// rows produces `status_int` (Int64) and `status_str` (Utf8) columns. This
/// struct groups them so the output picks the first non-null variant per row,
/// preserving type fidelity (integers stay unquoted, strings stay quoted).
pub struct ColInfo {
    /// Base field name with type suffix stripped (e.g. "status" from "status_int").
    pub field_name: String,
    /// All column variants for this field, ordered by type priority (Int64 first).
    /// Each entry is (column_index, data_type).
    pub variants: Vec<(usize, DataType)>,
}

/// Priority for Arrow DataTypes when multiple columns exist for the same field.
/// Higher priority types are checked first per row — the first non-null wins.
fn datatype_priority(dt: &DataType) -> u8 {
    match dt {
        DataType::Int64 => 3,
        DataType::Float64 => 2,
        _ => 1, // Utf8, Utf8View, or anything else
    }
}

/// Build a grouped, ordered list of output fields from a RecordBatch schema.
///
/// Columns sharing the same base field name (e.g. `status_int` and `status_str`)
/// are grouped into a single `ColInfo` with multiple variants. During output,
/// the first non-null variant per row is used — no data is silently dropped.
pub fn build_col_infos(batch: &RecordBatch) -> Vec<ColInfo> {
    let schema = batch.schema();

    // Collect (field_name, column_index, data_type) for every column.
    let mut entries: Vec<(String, usize, DataType)> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let (field_name, _) = parse_column_name(field.name().as_str());
        entries.push((field_name.to_string(), idx, field.data_type().clone()));
    }

    // Group by field_name, preserving first-seen order.
    let mut groups: Vec<ColInfo> = Vec::new();
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (field_name, idx, dt) in entries {
        if let Some(&group_idx) = seen.get(&field_name) {
            groups[group_idx].variants.push((idx, dt));
        } else {
            seen.insert(field_name.clone(), groups.len());
            groups.push(ColInfo {
                field_name,
                variants: vec![(idx, dt)],
            });
        }
    }

    // Sort variants within each group by priority (Int64 first).
    for g in &mut groups {
        g.variants
            .sort_by(|a, b| datatype_priority(&b.1).cmp(&datatype_priority(&a.1)));
    }

    groups
}

/// Read a string value from a column at the given row.
///
/// Supports both `Utf8` (StringArray) and `Utf8View` (StringViewArray) columns,
/// allowing output sinks to work transparently with either scanner.
pub(crate) fn str_value(col: &dyn Array, row: usize) -> &str {
    match col.data_type() {
        DataType::Utf8 => col.as_string::<i32>().value(row),
        DataType::Utf8View => col.as_string_view().value(row),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row),
        _ => "",
    }
}

/// Write a JSON string value with RFC 8259 escaping.
fn write_json_string(out: &mut Vec<u8>, v: &str) -> io::Result<()> {
    out.push(b'"');
    for &b in v.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b if b < 0x20 => {
                Write::write_fmt(out, format_args!("\\u{:04x}", b))?;
            }
            _ => out.push(b),
        }
    }
    out.push(b'"');
    Ok(())
}

/// Write a single Arrow value as JSON, dispatching on the actual Arrow DataType.
///
/// Int64 → unquoted integer, Float64 → unquoted number (null for non-finite),
/// everything else → quoted string. This preserves JSON type fidelity on
/// roundtrip without relying on column name suffixes.
fn write_json_value(arr: &dyn Array, row: usize, out: &mut Vec<u8>) -> io::Result<()> {
    match arr.data_type() {
        DataType::Int64 => {
            let v = arr.as_primitive::<arrow::datatypes::Int64Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Float64 => {
            let v = arr
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row);
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        DataType::Boolean => {
            if arr.is_null(row) {
                out.extend_from_slice(b"null");
            } else {
                let v = arr.as_boolean().value(row);
                out.extend_from_slice(if v { b"true" } else { b"false" });
            }
        }
        _ => {
            write_json_string(out, str_value(arr, row))?;
        }
    }
    Ok(())
}

/// Write a single row as a JSON object into `out`.
///
/// For fields backed by multiple typed columns (e.g. `status_int` + `status_str`),
/// the first non-null variant is used. If all variants are null the field is emitted
/// as `"field":null` to preserve JSON field presence. Type dispatch uses the Arrow
/// DataType, not the column name suffix.
pub fn write_row_json(
    batch: &RecordBatch,
    row: usize,
    cols: &[ColInfo],
    out: &mut Vec<u8>,
) -> io::Result<()> {
    out.push(b'{');
    let mut first = true;
    for col in cols {
        // Find the first non-null variant for this field.
        let variant = col
            .variants
            .iter()
            .find(|(idx, _)| !batch.column(*idx).is_null(row));

        if !first {
            out.push(b',');
        }
        first = false;

        // Key
        out.push(b'"');
        out.extend_from_slice(col.field_name.as_bytes());
        out.push(b'"');
        out.push(b':');

        let Some((arr_idx, _)) = variant else {
            // All variants null for this row — emit JSON null to preserve field presence.
            out.extend_from_slice(b"null");
            continue;
        };
        let arr = batch.column(*arr_idx);

        // Value — dispatch on Arrow DataType, not column name suffix
        write_json_value(arr, row, out)?;
    }
    out.push(b'}');
    Ok(())
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
pub fn build_output_sink(
    name: &str,
    cfg: &OutputConfig,
    stats: Arc<ComponentStats>,
) -> Result<Box<dyn OutputSink>, String> {
    let auth_headers = build_auth_headers(cfg.auth.as_ref());
    match cfg.output_type {
        OutputType::Stdout => {
            let fmt = match cfg.format.as_ref() {
                Some(Format::Json) => StdoutFormat::Json,
                Some(Format::Console) => StdoutFormat::Console,
                _ => StdoutFormat::Text,
            };
            Ok(Box::new(StdoutSink::new(name.to_string(), fmt, stats)))
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
                Some("gzip") => {
                    return Err(format!(
                        "output '{name}': OTLP does not support 'gzip' compression yet"
                    ));
                }
                _ => Compression::None,
            };
            Ok(Box::new(OtlpSink::new(
                name.to_string(),
                endpoint.clone(),
                protocol,
                compression,
                auth_headers,
                stats,
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
                stats,
            )))
        }
        OutputType::Null => Ok(Box::new(NullSink::new(name.to_string(), stats))),
        OutputType::TcpOut => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': tcp_out requires 'endpoint'"))?;
            Ok(Box::new(TcpSink::new(
                name.to_string(),
                endpoint.clone(),
                stats,
            )))
        }
        OutputType::UdpOut => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': udp_out requires 'endpoint'"))?;
            UdpSink::new(name.to_string(), endpoint.clone(), stats)
                .map(|s| Box::new(s) as Box<dyn OutputSink>)
                .map_err(|e| format!("output '{name}': udp_out bind failed: {e}"))
        }
        OutputType::Elasticsearch => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': elasticsearch requires 'endpoint'"))?;
            // Index name can come from config.index or config.path, defaulting to "logs"
            let index = cfg
                .index
                .as_ref()
                .or(cfg.path.as_ref())
                .map_or("logs", String::as_str)
                .to_string();
            Ok(Box::new(ElasticsearchSink::new(
                name.to_string(),
                endpoint.clone(),
                index,
                auth_headers,
                stats,
            )))
        }
        _ => Err(format!(
            "output '{name}': type {:?} not yet supported",
            cfg.output_type
        )),
    }
}

// ---------------------------------------------------------------------------
// OnceFactory — wraps a pre-built OutputSink for sync/legacy sinks
// ---------------------------------------------------------------------------

/// `SinkFactory` implementation for synchronous [`OutputSink`] types.
///
/// Wraps one pre-built sink in a `Mutex`. On the first `create()` call it
/// transfers ownership into a [`SyncSinkAdapter`]; subsequent calls return an
/// error. This naturally enforces `max_workers = 1` for sync sinks since the
/// pool will stop spawning workers once the factory starts returning errors.
///
/// For most sync sinks (Stdout, TCP, UDP) a single worker is correct —
/// there is only one connection or file handle anyway.
pub struct OnceFactory {
    name: String,
    inner: Mutex<Option<Box<dyn OutputSink>>>,
}

impl OnceFactory {
    pub fn new(name: String, sink: Box<dyn OutputSink>) -> Self {
        OnceFactory {
            name,
            inner: Mutex::new(Some(sink)),
        }
    }
}

impl sink::SinkFactory for OnceFactory {
    fn create(&self) -> io::Result<Box<dyn sink::Sink>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("OnceFactory mutex poisoned"))?;
        match guard.take() {
            Some(s) => Ok(Box::new(SyncSinkAdapter::new(s))),
            None => Err(io::Error::other(
                "OnceFactory: sync sink already consumed (max_workers must be 1)",
            )),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_use(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// build_sink_factory — produce Arc<dyn SinkFactory> from config
// ---------------------------------------------------------------------------

/// Build an `Arc<dyn SinkFactory>` from an output configuration.
///
/// For async-native sinks (Elasticsearch, Loki) this returns a factory that
/// creates a fresh reqwest-based sink per worker. For all other sinks it
/// builds the sink synchronously and wraps it in a [`OnceFactory`] — those
/// sinks are limited to one worker.
pub fn build_sink_factory(
    name: &str,
    cfg: &logfwd_config::OutputConfig,
    stats: Arc<ComponentStats>,
) -> Result<Arc<dyn sink::SinkFactory>, String> {
    use logfwd_config::OutputType;

    let auth_headers = build_auth_headers(cfg.auth.as_ref());

    match cfg.output_type {
        OutputType::Elasticsearch => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': elasticsearch requires 'endpoint'"))?;
            let index = cfg
                .index
                .as_ref()
                .or(cfg.path.as_ref())
                .map_or("logs", String::as_str)
                .to_string();
            let compress = cfg.compression.as_deref() == Some("gzip");
            let factory = ElasticsearchSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                index,
                auth_headers,
                compress,
                stats,
            )
            .map_err(|e| format!("output '{name}': elasticsearch factory: {e}"))?;
            Ok(Arc::new(factory))
        }
        OutputType::Loki => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': loki requires 'endpoint'"))?;
            let factory = LokiSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                None, // tenant_id: not yet in OutputConfig
                Vec::new(),
                Vec::new(),
                auth_headers,
                stats,
            )
            .map_err(|e| format!("output '{name}': loki factory: {e}"))?;
            Ok(Arc::new(factory))
        }
        _ => {
            // Sync sink — build it once, wrap in OnceFactory (max_workers=1).
            let sink = build_output_sink(name, cfg, stats)?;
            Ok(Arc::new(OnceFactory::new(name.to_string(), sink)))
        }
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
    use arrow::datatypes::{Field, Schema};
    use logfwd_io::diagnostics::ComponentStats;
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
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

    #[test]
    fn test_parse_column_name() {
        assert_eq!(parse_column_name("status_int"), ("status", "int"));
        assert_eq!(parse_column_name("level_str"), ("level", "str"));
        assert_eq!(
            parse_column_name("duration_ms_float"),
            ("duration_ms", "float")
        );
        assert_eq!(parse_column_name("_raw"), ("_raw", ""));
        assert_eq!(parse_column_name("plain"), ("plain", ""));
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
    fn test_fanout() {
        // FanOut to two sinks that write to Vec<u8>.
        // We use StdoutSink with write_batch_to to capture output.
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

        // Also test FanOut trait dispatch works.
        let fanout_s1 = StdoutSink::new(
            "f1".to_string(),
            StdoutFormat::Json,
            Arc::new(ComponentStats::new()),
        );
        let fanout_s2 = StdoutSink::new(
            "f2".to_string(),
            StdoutFormat::Json,
            Arc::new(ComponentStats::new()),
        );
        let mut fanout = FanOut::new(vec![Box::new(fanout_s1), Box::new(fanout_s2)]);
        // send_batch writes to real stdout, but should not error.
        let result = fanout.send_batch(&batch, &meta);
        assert!(result.is_ok());
    }

    struct AlwaysFailSink {
        name: &'static str,
    }

    impl OutputSink for AlwaysFailSink {
        fn send_batch(
            &mut self,
            _batch: &RecordBatch,
            _metadata: &BatchMetadata,
        ) -> io::Result<()> {
            Err(io::Error::other(format!("{} failed", self.name)))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    #[test]
    fn test_fanout_error_reports_failed_sink_names() {
        let batch = make_test_batch();
        let meta = make_metadata();
        let mut fanout = FanOut::new(vec![
            Box::new(AlwaysFailSink { name: "sink-a" }),
            Box::new(AlwaysFailSink { name: "sink-b" }),
        ]);

        let err = fanout
            .send_batch(&batch, &meta)
            .expect_err("fanout should fail");
        let fanout_err = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<FanOutError>())
            .expect("fanout should wrap failures in FanOutError");

        assert_eq!(fanout_err.failed_sinks(), ["sink-a", "sink-b"]);
    }

    #[test]
    fn test_otlp_encoding() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_str", DataType::Utf8, true),
            Field::new("level_str", DataType::Utf8, true),
            Field::new("message_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
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
            Arc::new(ComponentStats::new()),
        );
        sink.encode_batch(&batch, &meta);

        // Should produce non-empty protobuf bytes.
        assert!(!sink.encoder_buf.is_empty());
        // First byte should be tag for field 1 (ResourceLogs), wire type 2 = 0x0A
        assert_eq!(sink.encoder_buf[0], 0x0A);
    }

    #[test]
    fn test_otlp_gzip_send_batch_returns_error() {
        let batch = make_test_batch();
        let meta = make_metadata();
        let mut sink = OtlpSink::new(
            "test-otlp".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::Gzip,
            vec![],
            Arc::new(ComponentStats::new()),
        );

        let err = sink.send_batch(&batch, &meta).unwrap_err();
        assert!(err.to_string().contains("gzip"), "got: {err}");
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
            Field::new("level_str", DataType::Utf8, true),
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
    fn test_type_preference_dedup() {
        // When both status_int and status_str exist, int should win.
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
        // Should have integer 500, not string "500"
        assert!(output.contains("\"status\":500"), "got: {}", output);
        // Should NOT have the string version
        assert!(!output.contains("\"status\":\"500\""), "got: {}", output);
    }

    #[test]
    fn test_float_column_json() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration_ms_float",
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
    fn test_build_output_sink_stdout() {
        let cfg = OutputConfig {
            name: Some("test".to_string()),
            output_type: OutputType::Stdout,
            endpoint: None,
            protocol: None,
            compression: None,
            format: Some(Format::Json),
            path: None,
            index: None,
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
            index: None,
            auth: None,
        };
        let sink = build_output_sink("otel", &cfg, Arc::new(ComponentStats::new())).unwrap();
        assert_eq!(sink.name(), "otel");
    }

    #[test]
    fn test_build_output_sink_otlp_rejects_gzip() {
        let cfg = OutputConfig {
            name: Some("otel".to_string()),
            output_type: OutputType::Otlp,
            endpoint: Some("http://localhost:4318".to_string()),
            protocol: Some("http".to_string()),
            compression: Some("gzip".to_string()),
            format: None,
            path: None,
            index: None,
            auth: None,
        };
        let err = match build_output_sink("otel", &cfg, Arc::new(ComponentStats::new())) {
            Ok(_) => panic!("expected gzip OTLP compression to be rejected"),
            Err(err) => err,
        };
        assert!(err.contains("gzip"), "got: {err}");
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
            index: None,
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
            index: None,
            auth: None,
        };
        let result = build_output_sink("bad", &cfg, Arc::new(ComponentStats::new()));
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
            index: None,
            auth: Some(AuthConfig {
                bearer_token: Some("mytoken".to_string()),
                headers: std::collections::HashMap::new(),
            }),
        };
        let sink = build_output_sink("auth-sink", &cfg, Arc::new(ComponentStats::new())).unwrap();
        assert_eq!(sink.name(), "auth-sink");
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
            Arc::new(ComponentStats::new()),
        );
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
            Arc::new(ComponentStats::new()),
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
        write_row_json(batch, row, &cols, &mut out).expect("write_row_json failed");
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
        let expected = std::f64::consts::PI;
        let batch = make_batch(vec![(
            "duration_float",
            Arc::new(Float64Array::from(vec![expected])),
        )]);
        let json = render(&batch, 0);
        let v: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        assert!((v["duration"].as_f64().unwrap() - expected).abs() < 0.001);
    }

    #[test]
    fn null_values_preserved() {
        let batch = make_batch(vec![(
            "msg_str",
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

    /// Regression: when a field has both int and str variants across rows,
    /// the old dedup picked one column and silently dropped the other.
    /// Now both variants are kept, and the first non-null wins per row.
    #[test]
    fn mixed_type_field_no_data_loss() {
        // Row 0: status is integer 200 (int column populated, str null)
        // Row 1: status is string "ok" (str column populated, int null)
        let batch = make_batch(vec![
            (
                "status_int",
                Arc::new(Int64Array::from(vec![Some(200), None])),
            ),
            (
                "status_str",
                Arc::new(StringArray::from(vec![None, Some("ok")])),
            ),
        ]);
        // Row 0: should get the integer value
        let json0 = render(&batch, 0);
        let v0: serde_json::Value = serde_json::from_str(&json0).unwrap();
        assert_eq!(v0["status"], 200, "row 0 should be integer 200");

        // Row 1: should get the string value — NOT be missing
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
        let batch = make_batch(vec![(
            "score_float",
            Arc::new(Float64Array::from(vec![1.0])),
        )]);
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
            Arc::new(BooleanArray::from(vec![true])) as Arc<dyn arrow::array::Array>,
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
}
