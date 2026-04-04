use std::io;
use std::sync::Arc;

use arrow::array::{Array, AsArray, PrimitiveArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;

use logfwd_arrow::conflict_schema::normalize_conflict_columns;
use logfwd_core::otlp::{
    self, Severity, bytes_field_size, encode_bytes_field, encode_fixed32, encode_fixed64,
    encode_tag, encode_varint, encode_varint_field, hex_decode, parse_severity,
    parse_timestamp_nanos, varint_len,
};
use logfwd_io::diagnostics::ComponentStats;
use zstd::bulk::Compressor as ZstdCompressor;

#[allow(deprecated)]
use super::{
    BatchMetadata, Compression, HTTP_MAX_RETRIES, HTTP_RETRY_INITIAL_DELAY_MS, OutputSink,
    is_transient_error, str_value,
};

// ---------------------------------------------------------------------------
// InstrumentationScope constants
// ---------------------------------------------------------------------------

/// Name emitted in the OTLP `InstrumentationScope.name` field of every `ScopeLogs`.
const SCOPE_NAME: &[u8] = b"logfwd";
/// Version emitted in the OTLP `InstrumentationScope.version` field (from Cargo.toml).
const SCOPE_VERSION: &[u8] = env!("CARGO_PKG_VERSION").as_bytes();

// ---------------------------------------------------------------------------
// OtlpSink
// ---------------------------------------------------------------------------

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OtlpProtocol {
    Grpc,
    Http,
}

/// Sends OTLP protobuf LogRecords over gRPC or HTTP.
pub struct OtlpSink {
    name: String,
    endpoint: String,
    protocol: OtlpProtocol,
    compression: Compression,
    headers: Vec<(String, String)>,
    pub(crate) encoder_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    grpc_buf: Vec<u8>,
    compressor: Option<ZstdCompressor<'static>>,
    http_agent: ureq::Agent,
    stats: Arc<ComponentStats>,
}

impl OtlpSink {
    pub fn new(
        name: String,
        endpoint: String,
        protocol: OtlpProtocol,
        compression: Compression,
        headers: Vec<(String, String)>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let compressor = match compression {
            Compression::Zstd => {
                Some(ZstdCompressor::new(1).expect("zstd level 1 is always valid"))
            }
            _ => None,
        };
        let http_agent = ureq::config::Config::builder()
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .build()
            .new_agent();
        OtlpSink {
            name,
            endpoint,
            protocol,
            compression,
            headers,
            encoder_buf: Vec::with_capacity(64 * 1024),
            compress_buf: Vec::with_capacity(64 * 1024),
            grpc_buf: Vec::with_capacity(64 * 1024),
            compressor,
            http_agent,
            stats,
        }
    }

    /// Encode a full ExportLogsServiceRequest from a RecordBatch.
    /// Returns the raw protobuf bytes in `self.encoder_buf`.
    pub fn encode_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) {
        self.encoder_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        // Normalize any conflict struct columns (e.g. `status: Struct { int, str }`)
        // to flat Utf8 columns before encoding. Without this, struct columns would be
        // silently dropped, causing data loss when no SQL transform is applied upstream.
        let normalized;
        let batch = if batch
            .schema()
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Struct(_)))
        {
            normalized = normalize_conflict_columns(batch.clone());
            &normalized
        } else {
            batch
        };

        // Resolve column roles and downcast arrays once for the whole batch.
        let columns = resolve_batch_columns(batch);

        // Phase 1: encode all LogRecords into a temp buffer.
        let mut records_buf: Vec<u8> = Vec::with_capacity(num_rows * 128);
        let mut record_ranges: Vec<(usize, usize)> = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let start = records_buf.len();
            encode_row_as_log_record(&columns, row, metadata, &mut records_buf);
            record_ranges.push((start, records_buf.len()));
        }

        // Phase 2: compute sizes bottom-up.
        // ScopeLogs inner = scope (InstrumentationScope) + repeated log_records (LogRecord)
        let instrumentation_scope_inner_size =
            bytes_field_size(otlp::INSTRUMENTATION_SCOPE_NAME, SCOPE_NAME.len())
                + bytes_field_size(otlp::INSTRUMENTATION_SCOPE_VERSION, SCOPE_VERSION.len());

        let mut scope_logs_inner_size =
            bytes_field_size(otlp::SCOPE_LOGS_SCOPE, instrumentation_scope_inner_size);
        for &(start, end) in &record_ranges {
            let record_len = end - start;
            scope_logs_inner_size += bytes_field_size(otlp::SCOPE_LOGS_LOG_RECORDS, record_len);
        }

        // ResourceLogs inner = resource (field 1) + scope_logs (field 2)
        let mut resource_inner_size =
            bytes_field_size(otlp::RESOURCE_LOGS_SCOPE_LOGS, scope_logs_inner_size);

        // Encode resource attributes as Resource message
        let mut resource_msg: Vec<u8> = Vec::new();
        if !metadata.resource_attrs.is_empty() {
            for (k, v) in metadata.resource_attrs.as_ref() {
                encode_key_value_string(
                    &mut resource_msg,
                    otlp::RESOURCE_ATTRIBUTES,
                    k.as_bytes(),
                    v.as_bytes(),
                );
            }
        }
        if !resource_msg.is_empty() {
            resource_inner_size +=
                bytes_field_size(otlp::RESOURCE_LOGS_RESOURCE, resource_msg.len());
        }

        let request_size =
            bytes_field_size(otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS, resource_inner_size);

        // Phase 3: write the final protobuf.
        self.encoder_buf.reserve(request_size + 16);

        // ExportLogsServiceRequest.resource_logs
        encode_tag(
            &mut self.encoder_buf,
            otlp::EXPORT_LOGS_REQUEST_RESOURCE_LOGS,
            otlp::WIRE_TYPE_LEN,
        );
        encode_varint(&mut self.encoder_buf, resource_inner_size as u64);

        // Resource (field 1 of ResourceLogs)
        if !resource_msg.is_empty() {
            encode_bytes_field(
                &mut self.encoder_buf,
                otlp::RESOURCE_LOGS_RESOURCE,
                &resource_msg,
            );
        }

        // ScopeLogs (field 2 of ResourceLogs)
        encode_tag(
            &mut self.encoder_buf,
            otlp::RESOURCE_LOGS_SCOPE_LOGS,
            otlp::WIRE_TYPE_LEN,
        );
        encode_varint(&mut self.encoder_buf, scope_logs_inner_size as u64);

        // InstrumentationScope (field 1 of ScopeLogs)
        encode_tag(
            &mut self.encoder_buf,
            otlp::SCOPE_LOGS_SCOPE,
            otlp::WIRE_TYPE_LEN,
        );
        encode_varint(
            &mut self.encoder_buf,
            instrumentation_scope_inner_size as u64,
        );
        encode_bytes_field(
            &mut self.encoder_buf,
            otlp::INSTRUMENTATION_SCOPE_NAME,
            SCOPE_NAME,
        );
        encode_bytes_field(
            &mut self.encoder_buf,
            otlp::INSTRUMENTATION_SCOPE_VERSION,
            SCOPE_VERSION,
        );

        // LogRecords (repeated, field 2 of ScopeLogs)
        for &(start, end) in &record_ranges {
            encode_bytes_field(
                &mut self.encoder_buf,
                otlp::SCOPE_LOGS_LOG_RECORDS,
                &records_buf[start..end],
            );
        }
    }
}

#[allow(deprecated)]
impl OutputSink for OtlpSink {
    fn send_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()> {
        self.encode_batch(batch, metadata);
        if self.encoder_buf.is_empty() {
            return Ok(());
        }

        let payload: &[u8] = match self.compression {
            Compression::Zstd => {
                if let Some(ref mut compressor) = self.compressor {
                    // Produce raw zstd frames — no logfwd-internal header.
                    // OTLP receivers expect standard zstd per HTTP Content-Encoding.
                    // Reuse compress_buf to avoid per-batch allocation.
                    let bound = zstd::zstd_safe::compress_bound(self.encoder_buf.len());
                    self.compress_buf.clear();
                    self.compress_buf.reserve(bound);
                    let compressed_len = compressor
                        .compress_to_buffer(&self.encoder_buf, &mut self.compress_buf)
                        .map_err(io::Error::other)?;
                    self.compress_buf.truncate(compressed_len);
                    &self.compress_buf
                } else {
                    &self.encoder_buf
                }
            }
            Compression::Gzip => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "OTLP gzip compression is not yet implemented",
                ));
            }
            Compression::None => &self.encoder_buf,
        };

        let content_type = match self.protocol {
            OtlpProtocol::Grpc => "application/grpc",
            OtlpProtocol::Http => "application/x-protobuf",
        };

        // For gRPC, prepend the 5-byte length-prefixed frame header required by the
        // gRPC wire protocol. Note: ureq uses HTTP/1.1; a true gRPC endpoint requires
        // HTTP/2. Use a reverse proxy (e.g. Envoy) or `protocol: http` when the
        // collector does not accept HTTP/1.1 upgrades.
        //
        // The compressed flag reflects whether this specific payload is compressed
        // (i.e. Zstd was configured AND the compressor is present). If the compressor
        // was not initialized for some reason, the payload falls back to uncompressed
        // and the flag must be 0x00.
        let payload_is_compressed =
            self.compression == Compression::Zstd && self.compressor.is_some();
        let payload: &[u8] = if self.protocol == OtlpProtocol::Grpc {
            write_grpc_frame(&mut self.grpc_buf, payload, payload_is_compressed)?;
            &self.grpc_buf
        } else {
            payload
        };

        // Retry with exponential backoff for transient failures.
        // 1 initial attempt + up to HTTP_MAX_RETRIES retries; delays: 100ms → 200ms → 400ms.
        // Note: the full encoded payload is retransmitted on each attempt.
        // For large batches this multiplies bandwidth; this is acceptable as a
        // temporary measure until SinkDriver (#319) handles retries externally.
        let build_req = || {
            let mut req = self.http_agent.post(&self.endpoint);
            for (k, v) in &self.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req = req.header("Content-Type", content_type);
            if payload_is_compressed {
                req = req.header("Content-Encoding", "zstd");
            }
            req
        };
        let mut delay_ms: u64 = HTTP_RETRY_INITIAL_DELAY_MS;
        let mut attempt: u32 = 0;
        loop {
            match build_req().send(payload) {
                Ok(_) => {
                    self.stats.inc_lines(batch.num_rows() as u64);
                    self.stats.inc_bytes(self.encoder_buf.len() as u64);
                    return Ok(());
                }
                Err(e) if attempt < HTTP_MAX_RETRIES && is_transient_error(&e) => {
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    delay_ms *= 2;
                    attempt += 1;
                }
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Pre-downcast array variant for an attribute column.
enum AttrArray<'a> {
    Str(&'a dyn Array),
    Int(&'a PrimitiveArray<Int64Type>),
    Float(&'a PrimitiveArray<Float64Type>),
    Bool(&'a arrow::array::BooleanArray),
}

/// Pre-resolved column roles and downcast arrays for one RecordBatch.
///
/// Built once in [`encode_batch`] before the per-row loop to avoid
/// re-scanning the schema and re-downcasting arrays on every row.
struct BatchColumns<'a> {
    /// Downcast array for the timestamp column (e.g. "2024-01-15T10:30:00Z").
    timestamp_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the level/severity column (e.g. "ERROR").
    level_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the primary message/body column.
    body_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the `_raw` column, used as a per-row body
    /// fallback when `body_col` is null for that row.
    raw_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the `trace_id` column (32 hex chars → 16-byte OTLP field 9).
    trace_id_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the `span_id` column (16 hex chars → 8-byte OTLP field 10).
    span_id_col: Option<(usize, &'a dyn Array)>,
    /// Downcast array for the `flags` / `trace_flags` column (uint32, OTLP field 8).
    flags_col: Option<(usize, &'a PrimitiveArray<Int64Type>)>,
    /// Non-special attribute columns: (field_name, pre-downcast array).
    attribute_cols: Vec<(String, AttrArray<'a>)>,
}

/// Scan the batch schema once and resolve column roles and downcast arrays.
fn resolve_batch_columns(batch: &RecordBatch) -> BatchColumns<'_> {
    let schema = batch.schema();
    let mut timestamp_col: Option<(usize, &dyn Array)> = None;
    let mut level_col: Option<(usize, &dyn Array)> = None;
    let mut body_col: Option<(usize, &dyn Array)> = None;
    let mut raw_col: Option<(usize, &dyn Array)> = None;
    let mut trace_id_col: Option<(usize, &dyn Array)> = None;
    let mut span_id_col: Option<(usize, &dyn Array)> = None;
    let mut flags_col: Option<(usize, &PrimitiveArray<Int64Type>)> = None;
    // Indices of columns to exclude from attributes.
    let mut excluded: Vec<usize> = Vec::with_capacity(4);

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().as_str();
        let field_name = col_name;
        match field_name {
            "timestamp" | "time" | "ts" => {
                if timestamp_col.is_none()
                    && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View)
                {
                    timestamp_col = Some((idx, batch.column(idx).as_ref()));
                    excluded.push(idx);
                }
            }
            "level" | "severity" | "log_level" | "loglevel" | "lvl" => {
                if level_col.is_none()
                    && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View)
                {
                    level_col = Some((idx, batch.column(idx).as_ref()));
                    excluded.push(idx);
                }
            }
            "message" | "msg" | "_msg" | "body" => {
                if body_col.is_none()
                    && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View)
                {
                    body_col = Some((idx, batch.column(idx).as_ref()));
                    excluded.push(idx);
                }
            }
            "_raw" => {
                // Always excluded from attributes; used as per-row body fallback.
                excluded.push(idx);
                if raw_col.is_none() {
                    raw_col = Some((idx, batch.column(idx).as_ref()));
                }
            }
            "trace_id" => {
                if trace_id_col.is_none()
                    && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View)
                {
                    trace_id_col = Some((idx, batch.column(idx).as_ref()));
                    excluded.push(idx);
                }
            }
            "span_id" => {
                if span_id_col.is_none()
                    && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View)
                {
                    span_id_col = Some((idx, batch.column(idx).as_ref()));
                    excluded.push(idx);
                }
            }
            "flags" | "trace_flags" => {
                if flags_col.is_none() && matches!(field.data_type(), DataType::Int64) {
                    flags_col = Some((idx, batch.column(idx).as_primitive::<Int64Type>()));
                    excluded.push(idx);
                }
            }
            _ => {}
        }
    }

    let mut attribute_cols: Vec<(String, AttrArray<'_>)> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if excluded.contains(&idx) {
            continue;
        }
        let field_name = field.name().as_str();
        // Dispatch on the actual Arrow DataType, not the column name suffix.
        // A SQL transform may produce a column whose name suffix disagrees with
        // its real type (e.g. `SELECT level_str AS count_int`); using
        // `field.data_type()` avoids an `as_primitive` panic in that case.
        let attr = match field.data_type() {
            DataType::Int64 => AttrArray::Int(batch.column(idx).as_primitive::<Int64Type>()),
            DataType::Float64 => AttrArray::Float(batch.column(idx).as_primitive::<Float64Type>()),
            DataType::Boolean => AttrArray::Bool(batch.column(idx).as_boolean()),
            // Non-conflict struct columns (e.g. nested objects not produced by the
            // type-conflict builder) cannot be encoded as a single typed OTLP attribute.
            // Conflict structs (Struct { int, str, float, bool }) are already normalized
            // to flat Utf8 by `encode_batch` before this function is called.
            DataType::Struct(_) => continue,
            _ => AttrArray::Str(batch.column(idx).as_ref()),
        };
        attribute_cols.push((field_name.to_string(), attr));
    }

    BatchColumns {
        timestamp_col,
        level_col,
        body_col,
        raw_col,
        trace_id_col,
        span_id_col,
        flags_col,
        attribute_cols,
    }
}

/// Encode a single RecordBatch row as an OTLP LogRecord using pre-resolved columns.
fn encode_row_as_log_record(
    columns: &BatchColumns<'_>,
    row: usize,
    metadata: &BatchMetadata,
    buf: &mut Vec<u8>,
) {
    // --- Read per-row values from pre-resolved columns ---

    let timestamp_ns: u64 = columns
        .timestamp_col
        .and_then(|(_, arr)| {
            if arr.is_null(row) {
                None
            } else {
                parse_timestamp_nanos(str_value(arr, row).as_bytes())
            }
        })
        .unwrap_or(0);

    let (severity_num, severity_text): (Severity, &[u8]) = columns
        .level_col
        .and_then(|(_, arr)| {
            if arr.is_null(row) {
                None
            } else {
                Some(parse_severity(str_value(arr, row).as_bytes()))
            }
        })
        .unwrap_or((Severity::Unspecified, b""));

    let body: &str = columns
        .body_col
        .and_then(|(_, arr)| {
            if arr.is_null(row) {
                None
            } else {
                Some(str_value(arr, row))
            }
        })
        .or_else(|| {
            columns.raw_col.and_then(|(_, arr)| {
                if arr.is_null(row) {
                    None
                } else {
                    Some(str_value(arr, row))
                }
            })
        })
        .unwrap_or("");
    let body_bytes = body.as_bytes();

    // --- Write protobuf fields ---

    // LogRecord.time_unix_nano (fixed64)
    if timestamp_ns > 0 {
        encode_fixed64(buf, otlp::LOG_RECORD_TIME_UNIX_NANO, timestamp_ns);
    }

    // LogRecord.severity_number (varint)
    if severity_num as u8 > 0 {
        encode_varint_field(buf, otlp::LOG_RECORD_SEVERITY_NUMBER, severity_num as u64);
    }

    // LogRecord.severity_text (string)
    if !severity_text.is_empty() {
        encode_bytes_field(buf, otlp::LOG_RECORD_SEVERITY_TEXT, severity_text);
    }

    // LogRecord.body (AnyValue { string_value })
    if !body_bytes.is_empty() {
        let anyvalue_inner_size = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, body_bytes.len());
        encode_tag(buf, otlp::LOG_RECORD_BODY, otlp::WIRE_TYPE_LEN);
        encode_varint(buf, anyvalue_inner_size as u64);
        encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, body_bytes);
    }

    // LogRecord.attributes — pre-resolved attribute columns
    for (field_name, attr) in &columns.attribute_cols {
        match attr {
            AttrArray::Int(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_int(
                        buf,
                        otlp::LOG_RECORD_ATTRIBUTES,
                        field_name.as_bytes(),
                        arr.value(row),
                    );
                }
            }
            AttrArray::Float(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_double(
                        buf,
                        otlp::LOG_RECORD_ATTRIBUTES,
                        field_name.as_bytes(),
                        arr.value(row),
                    );
                }
            }
            AttrArray::Bool(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_bool(
                        buf,
                        otlp::LOG_RECORD_ATTRIBUTES,
                        field_name.as_bytes(),
                        arr.value(row),
                    );
                }
            }
            AttrArray::Str(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_string(
                        buf,
                        otlp::LOG_RECORD_ATTRIBUTES,
                        field_name.as_bytes(),
                        str_value(*arr, row).as_bytes(),
                    );
                }
            }
        }
    }

    // LogRecord.flags (fixed32) — W3C trace flags
    if let Some((_, arr)) = columns.flags_col {
        if !arr.is_null(row) {
            encode_fixed32(buf, otlp::LOG_RECORD_FLAGS, arr.value(row) as u32);
        }
    }

    // LogRecord.trace_id (bytes, 16 bytes) — hex-decoded from 32-char string column
    if let Some((_, arr)) = columns.trace_id_col {
        if !arr.is_null(row) {
            let hex = str_value(arr, row);
            let mut decoded = [0u8; 16];
            if hex_decode(hex.as_bytes(), &mut decoded) {
                encode_bytes_field(buf, otlp::LOG_RECORD_TRACE_ID, &decoded);
            }
        }
    }

    // LogRecord.span_id (bytes, 8 bytes) — hex-decoded from 16-char string column
    if let Some((_, arr)) = columns.span_id_col {
        if !arr.is_null(row) {
            let hex = str_value(arr, row);
            let mut decoded = [0u8; 8];
            if hex_decode(hex.as_bytes(), &mut decoded) {
                encode_bytes_field(buf, otlp::LOG_RECORD_SPAN_ID, &decoded);
            }
        }
    }

    // LogRecord.observed_time_unix_nano (fixed64)
    encode_fixed64(
        buf,
        otlp::LOG_RECORD_OBSERVED_TIME_UNIX_NANO,
        metadata.observed_time_ns,
    );
}

/// Encode a KeyValue with string AnyValue as an attribute.
/// `field_number` is the protobuf field tag of the parent message's `attributes` repeated field
/// (e.g. `LOG_RECORD_ATTRIBUTES` for LogRecord, `RESOURCE_ATTRIBUTES` for Resource).
/// KeyValue: { key (string), value (AnyValue { string_value }) }
fn encode_key_value_string(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: &[u8]) {
    let anyvalue_inner = bytes_field_size(otlp::ANY_VALUE_STRING_VALUE, value.len());
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_bytes_field(buf, otlp::ANY_VALUE_STRING_VALUE, value);
}

/// Encode a KeyValue with int AnyValue (`AnyValue.int_value`).
fn encode_key_value_int(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: i64) {
    let anyvalue_inner = 1 + varint_len(value as u64); // tag(1 byte) + varint
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_varint_field(buf, otlp::ANY_VALUE_INT_VALUE, value as u64);
}

/// Encode a KeyValue with double AnyValue (`AnyValue.double_value`).
fn encode_key_value_double(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: f64) {
    let anyvalue_inner = 1 + 8; // tag(1 byte) + fixed64
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_fixed64(buf, otlp::ANY_VALUE_DOUBLE_VALUE, value.to_bits());
}

/// Encode a KeyValue with boolean AnyValue (`AnyValue.bool_value`).
fn encode_key_value_bool(buf: &mut Vec<u8>, field_number: u32, key: &[u8], value: bool) {
    let anyvalue_inner = 1 + 1; // tag(1 byte) + varint(1 byte)
    let kv_inner = bytes_field_size(otlp::KEY_VALUE_KEY, key.len())
        + bytes_field_size(otlp::KEY_VALUE_VALUE, anyvalue_inner);
    encode_tag(buf, field_number, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, otlp::KEY_VALUE_KEY, key);
    encode_tag(buf, otlp::KEY_VALUE_VALUE, otlp::WIRE_TYPE_LEN);
    encode_varint(buf, anyvalue_inner as u64);
    encode_varint_field(buf, otlp::ANY_VALUE_BOOL_VALUE, u64::from(value));
}

/// Write a gRPC length-prefixed message frame into `buf`.
///
/// gRPC wire format (per the [gRPC over HTTP/2 specification](https://grpc.io/docs/what-is-grpc/core-concepts/)):
/// ```text
/// [1 byte: compressed flag (0 = not compressed, 1 = compressed)]
/// [4 bytes: big-endian message length]
/// [N bytes: protobuf message]
/// ```
fn write_grpc_frame(buf: &mut Vec<u8>, payload: &[u8], compressed: bool) -> io::Result<()> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "gRPC message payload must be < 4 GiB",
        )
    })?;
    buf.clear();
    buf.push(u8::from(compressed));
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    /// Struct conflict columns (status: Struct { int, str }) must be normalized
    /// to flat Utf8 before OTLP encoding so values are not silently dropped.
    #[test]
    fn struct_conflict_column_is_normalized_not_dropped() {
        use arrow::array::{Int64Array as I64A, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field as F, Fields};

        let int_arr: Arc<dyn Array> = Arc::new(I64A::from(vec![Some(200i64), None]));
        let str_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![None::<&str>, Some("OK")]));
        let child_fields = Fields::from(vec![
            Arc::new(F::new("int", DataType::Int64, true)),
            Arc::new(F::new("str", DataType::Utf8, true)),
        ]);
        let validity = NullBuffer::from(vec![true, true]);
        let struct_arr: Arc<dyn Array> = Arc::new(StructArray::new(
            child_fields.clone(),
            vec![Arc::clone(&int_arr), Arc::clone(&str_arr)],
            Some(validity),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Struct(child_fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![struct_arr]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // After normalization the "status" key must appear in the encoded output
        // with its coalesced value ("200" from the int child, "OK" from str child).
        assert!(
            contains_bytes(&sink.encoder_buf, b"status"),
            "conflict struct column 'status' must be encoded as an OTLP attribute after normalization"
        );
        assert!(
            contains_bytes(&sink.encoder_buf, b"200"),
            "int value 200 must be encoded as the coalesced string '200'"
        );
        assert!(
            contains_bytes(&sink.encoder_buf, b"OK"),
            "str value 'OK' must be encoded as an OTLP attribute"
        );
    }

    fn make_sink() -> OtlpSink {
        OtlpSink::new(
            "test".into(),
            "http://localhost:4318".into(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            Arc::new(ComponentStats::new()),
        )
    }

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 1_000_000_000,
        }
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn encode_trace_id_as_field_9() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // field 9, wire type 2: tag = (9 << 3) | 2 = 0x4A; length = 16 = 0x10
        let mut expected = vec![0x4Au8, 0x10u8];
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "trace_id field 9 not found in encoded output"
        );
    }

    #[test]
    fn encode_span_id_as_field_10() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "span_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // field 10, wire type 2: tag = (10 << 3) | 2 = 0x52; length = 8 = 0x08
        let mut expected = vec![0x52u8, 0x08u8];
        expected.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "span_id field 10 not found in encoded output"
        );
    }

    #[test]
    fn encode_flags_as_field_8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flags",
            DataType::Int64,
            true,
        )]));
        let arr = Int64Array::from(vec![1i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // field 8, wire type 5: tag = (8 << 3) | 5 = 0x45; then 4 bytes LE
        let expected = [0x45u8, 0x01, 0x00, 0x00, 0x00];
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "flags field 8 not found in encoded output"
        );
    }

    #[test]
    fn trace_id_not_encoded_as_attribute() {
        // A trace_id column must NOT appear as a KeyValue attribute (field 6).
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // If trace_id were encoded as an attribute, its key bytes would appear.
        assert!(
            !contains_bytes(&sink.encoder_buf, b"trace_id"),
            "trace_id key must not appear as an attribute"
        );
    }

    #[test]
    fn span_id_not_encoded_as_attribute() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "span_id",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(vec!["0102030405060708"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        assert!(
            !contains_bytes(&sink.encoder_buf, b"span_id"),
            "span_id key must not appear as an attribute"
        );
    }

    #[test]
    fn invalid_trace_id_hex_is_silently_ignored() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        // Not a valid 32-char hex string.
        let arr = StringArray::from(vec!["not-a-valid-hex-string-here!!!!"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata()); // must not panic

        // Field 9 tag 0x4A must not appear.
        let mut probe = vec![0x4Au8, 0x10u8];
        probe.extend_from_slice(&[0u8; 16]);
        assert!(
            !contains_bytes(&sink.encoder_buf, &probe),
            "invalid trace_id should not produce field 9"
        );
    }

    #[test]
    fn grpc_frame_prepends_five_byte_header() {
        let proto_payload = [0x0a, 0x02, 0x08, 0x01];
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, false).unwrap();
        assert_eq!(framed.len(), 5 + proto_payload.len());
        assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(
            msg_len as usize,
            proto_payload.len(),
            "length field must match payload length"
        );
        assert_eq!(
            &framed[5..],
            &proto_payload,
            "payload bytes must follow header"
        );
    }

    #[test]
    fn grpc_frame_empty_payload() {
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &[], false).unwrap();
        assert_eq!(framed.len(), 5);
        assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(msg_len, 0, "length field must be zero for empty payload");
    }

    #[test]
    fn grpc_frame_compressed_flag() {
        let proto_payload = [0x0a, 0x02];
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, true).unwrap();
        assert_eq!(framed[0], 0x01, "compressed flag must be 0x01");
    }

    /// Verify that `encode_batch` + `write_grpc_frame` produce a valid 5-byte gRPC frame header
    /// followed by the exact protobuf payload. Tests the encode-and-frame path directly without
    /// making a real network call.
    #[test]
    fn encode_and_frame_payload() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let arr = StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        // Encode the batch the same way send_batch would, then frame it manually.
        let mut sink = OtlpSink::new(
            "test".into(),
            "http://localhost:4318".into(),
            OtlpProtocol::Grpc,
            Compression::None,
            vec![],
            Arc::new(ComponentStats::new()),
        );
        sink.encode_batch(&batch, &make_metadata());
        let proto_payload = sink.encoder_buf.clone();

        // Frame as send_batch would.
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, false).unwrap();

        // The frame header must be 5 bytes followed by the exact protobuf payload.
        assert_eq!(
            framed[0], 0x00,
            "compressed flag must be 0x00 for uncompressed gRPC"
        );
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(
            msg_len as usize,
            proto_payload.len(),
            "gRPC length field must match protobuf payload length"
        );
        assert_eq!(
            &framed[5..],
            proto_payload.as_slice(),
            "protobuf payload must follow the frame header"
        );
    }

    #[test]
    fn scope_logs_has_instrumentation_scope() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let arr = StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // The InstrumentationScope name "logfwd" must be present in the encoded bytes.
        assert!(
            contains_bytes(&sink.encoder_buf, b"logfwd"),
            "InstrumentationScope name 'logfwd' not found in encoded output"
        );

        // The InstrumentationScope version (from CARGO_PKG_VERSION) must also be present.
        let version = env!("CARGO_PKG_VERSION").as_bytes();
        assert!(
            contains_bytes(&sink.encoder_buf, version),
            "InstrumentationScope version not found in encoded output"
        );
    }

    #[test]
    fn encode_boolean_as_attribute() {
        use arrow::array::BooleanArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            true,
        )]));
        let arr = BooleanArray::from(vec![Some(true)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        // LogRecord field 6 tag: (6 << 3) | 2 = 0x32
        // KeyValue field 1 key tag: (1 << 3) | 2 = 0x0A, then "active"
        // KeyValue field 2 value AnyValue tag: (2 << 3) | 2 = 0x12
        // AnyValue field 2 bool_value tag: (2 << 3) | 0 = 0x10, then 0x01
        let expected = [0x10u8, 0x01];
        assert!(
            contains_bytes(&sink.encoder_buf, &expected),
            "boolean attribute not found in encoded output"
        );
        assert!(
            contains_bytes(&sink.encoder_buf, b"active"),
            "attribute key 'active' not found"
        );
    }

    /// Roundtrip oracle test: encode a RecordBatch with our hand-rolled encoder,
    /// decode with prost (the canonical protobuf library), and compare fields.
    ///
    /// This is the definitive test that our OTLP encoding is spec-compliant.
    /// If we encode a field incorrectly, prost::Message::decode will either
    /// fail or produce different values.
    #[test]
    fn roundtrip_encode_decode_via_prost() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        // Build a RecordBatch with all supported LogRecord field types.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("flags", DataType::Int64, true),
            Field::new("host", DataType::Utf8, true), // string attribute
            Field::new("count", DataType::Int64, true), // int attribute
            Field::new("latency", DataType::Float64, true), // double attribute
            Field::new("active", DataType::Boolean, true), // bool attribute
        ]));

        let ts_arr = StringArray::from(vec!["2024-01-15T10:30:00Z"]);
        let level_arr = StringArray::from(vec!["ERROR"]);
        let msg_arr = StringArray::from(vec!["disk full"]);
        let trace_arr = StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"]);
        let span_arr = StringArray::from(vec!["0102030405060708"]);
        let flags_arr = Int64Array::from(vec![1i64]);
        let host_arr = StringArray::from(vec!["web-01"]);
        let count_arr = Int64Array::from(vec![42i64]);
        let latency_arr = arrow::array::Float64Array::from(vec![1.5f64]);
        let active_arr = arrow::array::BooleanArray::from(vec![Some(true)]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts_arr),
                Arc::new(level_arr),
                Arc::new(msg_arr),
                Arc::new(trace_arr),
                Arc::new(span_arr),
                Arc::new(flags_arr),
                Arc::new(host_arr),
                Arc::new(count_arr),
                Arc::new(latency_arr),
                Arc::new(active_arr),
            ],
        )
        .expect("valid batch");

        let observed_ns: u64 = 1_700_000_000_000_000_000;
        let resource_attrs = Arc::new(vec![("k8s.pod.name".to_string(), "my-pod".to_string())]);
        let metadata = BatchMetadata {
            resource_attrs,
            observed_time_ns: observed_ns,
        };

        // Encode with our hand-rolled encoder.
        let mut sink = make_sink();
        sink.encode_batch(&batch, &metadata);
        assert!(
            !sink.encoder_buf.is_empty(),
            "encoder must produce non-empty output"
        );

        // Decode with prost — the canonical protobuf decoder.
        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode our encoding without error");

        // --- Verify structure ---
        assert_eq!(request.resource_logs.len(), 1, "exactly one ResourceLogs");
        let rl = &request.resource_logs[0];

        // Resource attributes
        let resource = rl.resource.as_ref().expect("Resource must be present");
        let pod_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "k8s.pod.name");
        assert!(pod_attr.is_some(), "resource attr k8s.pod.name must exist");
        let pod_val = pod_attr
            .unwrap()
            .value
            .as_ref()
            .and_then(|v| match &v.value {
                Some(Value::StringValue(s)) => Some(s.as_str()),
                _ => None,
            });
        assert_eq!(pod_val, Some("my-pod"), "resource attr value mismatch");

        // ScopeLogs
        assert_eq!(rl.scope_logs.len(), 1, "exactly one ScopeLogs");
        let sl = &rl.scope_logs[0];
        let scope = sl
            .scope
            .as_ref()
            .expect("InstrumentationScope must be present");
        assert_eq!(scope.name, "logfwd", "scope name must be 'logfwd'");
        assert_eq!(
            scope.version,
            env!("CARGO_PKG_VERSION"),
            "scope version must match CARGO_PKG_VERSION"
        );

        // LogRecord
        assert_eq!(sl.log_records.len(), 1, "exactly one LogRecord");
        let lr = &sl.log_records[0];

        // time_unix_nano: 2024-01-15T10:30:00Z = 1705314600 seconds
        assert_eq!(
            lr.time_unix_nano, 1_705_314_600_000_000_000,
            "time_unix_nano mismatch"
        );

        // observed_time_unix_nano
        assert_eq!(
            lr.observed_time_unix_nano, observed_ns,
            "observed_time_unix_nano mismatch"
        );

        // severity
        assert_eq!(lr.severity_number, 17, "ERROR severity_number must be 17");
        assert_eq!(lr.severity_text, "ERROR", "severity_text mismatch");

        // body
        let body_str = lr.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(body_str, Some("disk full"), "body mismatch");

        // trace_id (16 bytes, decoded from hex)
        assert_eq!(
            lr.trace_id,
            vec![
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10
            ],
            "trace_id mismatch"
        );

        // span_id (8 bytes, decoded from hex)
        assert_eq!(
            lr.span_id,
            vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            "span_id mismatch"
        );

        // flags
        assert_eq!(lr.flags, 1, "flags mismatch");

        // --- Verify attributes ---
        let find_attr = |name: &str| lr.attributes.iter().find(|kv| kv.key == name);

        // String attribute: host
        let host_kv = find_attr("host").expect("host attribute must exist");
        let host_val = host_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(host_val, Some("web-01"), "host attribute value mismatch");

        // Int attribute: count
        let count_kv = find_attr("count").expect("count attribute must exist");
        let count_val = count_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::IntValue(i)) => Some(*i),
            _ => None,
        });
        assert_eq!(count_val, Some(42), "count attribute value mismatch");

        // Double attribute: latency
        let latency_kv = find_attr("latency").expect("latency attribute must exist");
        let latency_val = latency_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::DoubleValue(d)) => Some(*d),
            _ => None,
        });
        assert!(
            (latency_val.unwrap() - 1.5).abs() < f64::EPSILON,
            "latency attribute value mismatch"
        );

        // Bool attribute: active
        let active_kv = find_attr("active").expect("active attribute must exist");
        let active_val = active_kv.value.as_ref().and_then(|v| match &v.value {
            Some(Value::BoolValue(b)) => Some(*b),
            _ => None,
        });
        assert_eq!(active_val, Some(true), "active attribute value mismatch");
    }

    /// Roundtrip with minimal fields: only body, no timestamp, no severity,
    /// no trace context. Ensures sparse records encode correctly.
    #[test]
    fn roundtrip_minimal_record() {
        use opentelemetry_proto::tonic::{
            collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let msg_arr = StringArray::from(vec!["hello world"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg_arr)]).expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode minimal record");

        let lr = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(lr.time_unix_nano, 0, "no timestamp column means 0");
        assert_eq!(lr.severity_number, 0, "no severity means unspecified");
        let body_str = lr.body.as_ref().and_then(|v| match &v.value {
            Some(Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        });
        assert_eq!(body_str, Some("hello world"), "body mismatch");
        assert!(lr.trace_id.is_empty(), "no trace_id column means empty");
        assert!(lr.span_id.is_empty(), "no span_id column means empty");
    }

    /// Roundtrip with multiple rows to verify repeated LogRecord encoding.
    #[test]
    fn roundtrip_multiple_rows() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let msg_arr = StringArray::from(vec!["first", "second", "third"]);
        let level_arr = StringArray::from(vec!["INFO", "WARN", "ERROR"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg_arr), Arc::new(level_arr)])
            .expect("valid batch");

        let mut sink = make_sink();
        sink.encode_batch(&batch, &make_metadata());

        let request = ExportLogsServiceRequest::decode(sink.encoder_buf.as_slice())
            .expect("prost must decode multi-row batch");

        let records = &request.resource_logs[0].scope_logs[0].log_records;
        assert_eq!(records.len(), 3, "must have 3 LogRecords");

        let bodies: Vec<&str> = records
            .iter()
            .filter_map(|lr| {
                lr.body.as_ref().and_then(|v| match &v.value {
                    Some(Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
            })
            .collect();
        assert_eq!(bodies, vec!["first", "second", "third"]);

        let severities: Vec<i32> = records.iter().map(|lr| lr.severity_number).collect();
        assert_eq!(severities, vec![9, 13, 17], "INFO=9, WARN=13, ERROR=17");
    }
}
