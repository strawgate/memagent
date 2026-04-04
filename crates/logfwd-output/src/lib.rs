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

pub use elasticsearch::{ElasticsearchAsyncSink, ElasticsearchSinkFactory};
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

use arrow::array::{Array, AsArray, StructArray};
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
///
/// Deprecated: the flat-suffix scheme is being replaced by struct conflict
/// columns.  This function is kept for backward compatibility with
/// `otlp_sink.rs` column-role detection.
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
// Struct conflict column helpers
// ---------------------------------------------------------------------------

/// Returns `true` if a Struct column's child fields are all type-name fields
/// ("int", "float", "str", "bool") — i.e. it is a conflict struct column.
///
/// Note: the current builders only emit "int", "float", and "str" children.
/// "bool" is included for forward compatibility — if a future builder adds a
/// bool child, this detection and the priority functions will handle it
/// automatically.
fn is_conflict_struct(fields: &arrow::datatypes::Fields) -> bool {
    !fields.is_empty()
        && fields
            .iter()
            .all(|f| matches!(f.name().as_str(), "int" | "float" | "str" | "bool"))
}

/// JSON output priority: higher wins per row.  Int64 > Float64 > Boolean > Utf8.
fn json_priority(dt: &DataType) -> u8 {
    match dt {
        DataType::Int64 => 4,
        DataType::Float64 => 3,
        DataType::Boolean => 2,
        _ => 1,
    }
}

/// String-coalesce priority: Utf8 wins (for Loki labels etc.), then Bool, Int, Float.
fn str_priority(dt: &DataType) -> u8 {
    match dt {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => 4,
        DataType::Boolean => 3,
        DataType::Int64 => 2,
        DataType::Float64 => 1,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// ColVariant / ColInfo
// ---------------------------------------------------------------------------

/// Where to find one typed variant of a conflict field.
pub enum ColVariant {
    /// A top-level flat Arrow column.
    Flat { col_idx: usize, dt: DataType },
    /// One child field inside a StructArray conflict column.
    StructField {
        struct_col_idx: usize,
        field_idx: usize,
        dt: DataType,
    },
}

/// Describes one output JSON field, potentially backed by a struct conflict
/// column or multiple flat typed columns.
pub struct ColInfo {
    /// Logical field name (e.g. "status", "_raw").
    pub field_name: String,
    /// Variants ordered for JSON output: Int64 > Float64 > Boolean > Utf8.
    pub json_variants: Vec<ColVariant>,
    /// Variants ordered for the virtual coalesced Utf8 column: Utf8 first,
    /// then Boolean, Int64, Float64.  Used by Loki label extraction.
    pub str_variants: Vec<ColVariant>,
}

// ---------------------------------------------------------------------------
// JSON serialization helpers (shared by StdoutSink and JsonLinesSink)
// ---------------------------------------------------------------------------

/// Extract the `DataType` from any `ColVariant`.
fn variant_dt(v: &ColVariant) -> &DataType {
    match v {
        ColVariant::Flat { dt, .. } => dt,
        ColVariant::StructField { dt, .. } => dt,
    }
}

/// Returns `true` if the given `ColVariant` is null at `row` in `batch`.
pub(crate) fn is_null(batch: &RecordBatch, variant: &ColVariant, row: usize) -> bool {
    match variant {
        ColVariant::Flat { col_idx, .. } => batch.column(*col_idx).is_null(row),
        ColVariant::StructField {
            struct_col_idx,
            field_idx,
            ..
        } => {
            let sa = batch
                .column(*struct_col_idx)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("conflict struct column must be StructArray");
            sa.is_null(row) || sa.column(*field_idx).is_null(row)
        }
    }
}

/// Return a reference to the underlying Arrow array for a `ColVariant`.
pub(crate) fn get_array<'b>(batch: &'b RecordBatch, variant: &ColVariant) -> &'b dyn Array {
    match variant {
        ColVariant::Flat { col_idx, .. } => batch.column(*col_idx).as_ref(),
        ColVariant::StructField {
            struct_col_idx,
            field_idx,
            ..
        } => {
            let sa = batch
                .column(*struct_col_idx)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("conflict struct column must be StructArray");
            sa.column(*field_idx).as_ref()
        }
    }
}

/// Coalesce a conflict field to a `String` using `str_variants` ordering
/// (Utf8 wins, then Boolean, Int64, Float64).  Returns `None` if all variants
/// are null.
///
/// Used by Loki label extraction to always produce a string value.
pub(crate) fn coalesce_as_str(batch: &RecordBatch, row: usize, col: &ColInfo) -> Option<String> {
    let variant = col.str_variants.iter().find(|v| !is_null(batch, v, row))?;
    let arr = get_array(batch, variant);
    let s = match arr.data_type() {
        DataType::Int64 => {
            let v = arr.as_primitive::<arrow::datatypes::Int64Type>().value(row);
            itoa::Buffer::new().format(v).to_string()
        }
        DataType::Float64 => {
            let v = arr
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row);
            if v.is_finite() {
                ryu::Buffer::new().format_finite(v).to_string()
            } else {
                return None;
            }
        }
        DataType::Boolean => {
            if arr.as_boolean().value(row) {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        _ => str_value(arr, row).to_string(),
    };
    Some(s)
}

/// Build a grouped, ordered list of output fields from a RecordBatch schema.
///
/// Handles both struct conflict columns (`status: Struct { int, str }`) and
/// legacy flat typed columns (`status_int`, `status_str`).  The returned
/// `ColInfo` items contain two independently ordered variant lists for the
/// two coalesce strategies.
pub fn build_col_infos(batch: &RecordBatch) -> Vec<ColInfo> {
    let schema = batch.schema();
    let mut infos: Vec<ColInfo> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Struct(child_fields) if is_conflict_struct(child_fields) => {
                // Struct conflict column: one ColInfo, variants = child fields.
                let mut json_variants: Vec<ColVariant> = child_fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, f)| ColVariant::StructField {
                        struct_col_idx: col_idx,
                        field_idx,
                        dt: f.data_type().clone(),
                    })
                    .collect();
                let mut str_variants: Vec<ColVariant> = child_fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, f)| ColVariant::StructField {
                        struct_col_idx: col_idx,
                        field_idx,
                        dt: f.data_type().clone(),
                    })
                    .collect();

                json_variants.sort_by_key(|v| std::cmp::Reverse(json_priority(variant_dt(v))));
                str_variants.sort_by_key(|v| std::cmp::Reverse(str_priority(variant_dt(v))));

                infos.push(ColInfo {
                    field_name: field.name().clone(),
                    json_variants,
                    str_variants,
                });
            }
            dt => {
                // Plain flat column (may be a legacy `_int`/`_str` suffixed column
                // OR a post-SQL Utf8 coalesced column — both handled identically).
                let (field_name, _) = parse_column_name(field.name().as_str());
                // Check if there's already a ColInfo for this logical field name
                // (for legacy flat `status_int` + `status_str` pairs).
                if let Some(existing) = infos.iter_mut().find(|c| c.field_name == field_name) {
                    existing.json_variants.push(ColVariant::Flat {
                        col_idx,
                        dt: dt.clone(),
                    });
                    existing.str_variants.push(ColVariant::Flat {
                        col_idx,
                        dt: dt.clone(),
                    });
                    // Re-sort both lists.
                    existing
                        .json_variants
                        .sort_by_key(|v| std::cmp::Reverse(json_priority(variant_dt(v))));
                    existing
                        .str_variants
                        .sort_by_key(|v| std::cmp::Reverse(str_priority(variant_dt(v))));
                } else {
                    infos.push(ColInfo {
                        field_name: field_name.to_string(),
                        json_variants: vec![ColVariant::Flat {
                            col_idx,
                            dt: dt.clone(),
                        }],
                        str_variants: vec![ColVariant::Flat {
                            col_idx,
                            dt: dt.clone(),
                        }],
                    });
                }
            }
        }
    }

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
/// Integer types → unquoted integer, float types → unquoted number (null for
/// non-finite), Null → JSON null, Boolean → true/false/null, everything else →
/// quoted string. This preserves JSON type fidelity on roundtrip without
/// relying on column name suffixes.
fn write_json_value(arr: &dyn Array, row: usize, out: &mut Vec<u8>) -> io::Result<()> {
    match arr.data_type() {
        DataType::Null => {
            out.extend_from_slice(b"null");
        }
        DataType::Int8 => {
            let v = arr.as_primitive::<arrow::datatypes::Int8Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int16 => {
            let v = arr.as_primitive::<arrow::datatypes::Int16Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int32 => {
            let v = arr.as_primitive::<arrow::datatypes::Int32Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int64 => {
            let v = arr.as_primitive::<arrow::datatypes::Int64Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt8 => {
            let v = arr.as_primitive::<arrow::datatypes::UInt8Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt16 => {
            let v = arr
                .as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt32 => {
            let v = arr
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt64 => {
            let v = arr
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Float32 => {
            let v = arr
                .as_primitive::<arrow::datatypes::Float32Type>()
                .value(row);
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
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
/// For fields backed by struct conflict columns or multiple flat typed columns,
/// the first non-null variant (by `json_variants` ordering) is used. If all
/// variants are null the field is emitted as `"field":null` to preserve JSON
/// field presence.  Type dispatch uses the Arrow DataType, not the column name
/// suffix.
pub fn write_row_json(
    batch: &RecordBatch,
    row: usize,
    cols: &[ColInfo],
    out: &mut Vec<u8>,
) -> io::Result<()> {
    out.push(b'{');
    let mut first = true;
    for col in cols {
        // Find the first non-null variant for this field (json ordering).
        let variant = col.json_variants.iter().find(|v| !is_null(batch, v, row));

        if !first {
            out.push(b',');
        }
        first = false;

        // Key — escape to produce valid JSON if field_name contains special chars.
        write_json_string(out, &col.field_name)?;
        out.push(b':');

        let Some(v) = variant else {
            // All variants null for this row — emit JSON null to preserve field presence.
            out.extend_from_slice(b"null");
            continue;
        };
        let arr = get_array(batch, v);

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
            let compression = match cfg.compression.as_deref() {
                Some("gzip") => Compression::Gzip,
                Some(other) => {
                    return Err(format!(
                        "output '{name}': json_lines HTTP does not support '{other}' compression (use 'gzip')"
                    ));
                }
                None => Compression::None,
            };
            Ok(Box::new(JsonLinesSink::new(
                name.to_string(),
                endpoint.clone(),
                auth_headers,
                compression,
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
        OutputType::Elasticsearch => Err(format!(
            "output '{name}': elasticsearch requires the async pipeline — use build_sink_factory() instead"
        )),
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

impl SinkFactory for OnceFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
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
    cfg: &OutputConfig,
    stats: Arc<ComponentStats>,
) -> Result<Arc<dyn SinkFactory>, String> {
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
    fn test_build_output_sink_http_with_gzip_compression() {
        let cfg = OutputConfig {
            name: Some("http-gz".to_string()),
            output_type: OutputType::Http,
            endpoint: Some("http://localhost:9200".to_string()),
            protocol: None,
            compression: Some("gzip".to_string()),
            format: None,
            path: None,
            index: None,
            auth: None,
        };
        let sink = build_output_sink("http-gz", &cfg, Arc::new(ComponentStats::new())).unwrap();
        assert_eq!(sink.name(), "http-gz");
    }

    #[test]
    fn test_build_output_sink_http_rejects_unknown_compression() {
        let cfg = OutputConfig {
            name: Some("http-bad".to_string()),
            output_type: OutputType::Http,
            endpoint: Some("http://localhost:9200".to_string()),
            protocol: None,
            compression: Some("zstd".to_string()),
            format: None,
            path: None,
            index: None,
            auth: None,
        };
        let result = build_output_sink("http-bad", &cfg, Arc::new(ComponentStats::new()));
        assert!(
            result.is_err(),
            "unsupported compression should be rejected"
        );
        let err = result.err().unwrap();
        assert!(
            err.contains("zstd"),
            "error should name the unsupported algorithm, got: {err}"
        );
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
                Field::new("count_int", DataType::Int64, true),
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

        /// Type-suffix stripping: _int suffix → field name without suffix.
        #[test]
        fn prop_int_field_name_strips_suffix(
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
                // Normalized name must exist, raw name must NOT exist
                prop_assert!(
                    obj.contains_key(field_base.as_str()),
                    "normalized field '{field_base}' must be present; got keys: {:?}",
                    obj.keys().collect::<Vec<_>>()
                );
                prop_assert!(
                    !obj.contains_key(field_name.as_str()),
                    "raw field '{field_name}' must NOT appear in output"
                );
                // Value must round-trip
                prop_assert_eq!(
                    obj[field_base.as_str()].as_i64(),
                    Some(expected_val),
                    "integer value must round-trip"
                );
            }
        }

        /// Type-suffix stripping: _str suffix → field name without suffix.
        #[test]
        fn prop_str_field_name_strips_suffix(
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
                    obj.contains_key(field_base.as_str()),
                    "normalized field '{field_base}' must be present"
                );
                match expected_val {
                    Some(s) => prop_assert_eq!(
                        obj[field_base.as_str()].as_str(),
                        Some(s.as_str()),
                        "string value must round-trip"
                    ),
                    None => prop_assert!(
                        obj[field_base.as_str()].is_null(),
                        "null must serialize as JSON null"
                    ),
                }
            }
        }

        /// float field round-trip: _float suffix stripped, value preserved.
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
                prop_assert!(obj.contains_key(field_base.as_str()));
                let got = obj[field_base.as_str()].as_f64().unwrap();
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
                Field::new("msg_str", DataType::Utf8, true),
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
    fn proof_is_conflict_struct_empty_returns_false() {
        let fields = Fields::empty();
        assert!(!is_conflict_struct(&fields));
        kani::cover!(true, "empty fields path exercised");
    }

    /// JSON priority ordering: Int64 wins over Float64 wins over Utf8.
    /// This is the core contract that drives per-row type-preserving serialization.
    #[kani::proof]
    fn proof_json_priority_total_order() {
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
    fn proof_str_priority_string_beats_numerics() {
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
    fn proof_json_and_str_priority_differ_for_int_vs_utf8() {
        // In JSON mode, Int64 wins; in string mode, Utf8 wins.
        assert!(json_priority(&DataType::Int64) > json_priority(&DataType::Utf8));
        assert!(str_priority(&DataType::Utf8) > str_priority(&DataType::Int64));
        kani::cover!(
            true,
            "ordering inversion between json and str priority verified"
        );
    }
}
