use std::io;
use std::sync::Arc;

use arrow::array::{Array, AsArray, PrimitiveArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;

use logfwd_core::otlp::{
    Severity, bytes_field_size, encode_bytes_field, encode_fixed32, encode_fixed64, encode_tag,
    encode_varint, encode_varint_field, hex_decode, parse_severity, parse_timestamp_nanos,
    varint_len,
};
use logfwd_io::compress::ChunkCompressor;
use logfwd_io::diagnostics::ComponentStats;

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
    compressor: Option<ChunkCompressor>,
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
                Some(ChunkCompressor::new(1).expect("zstd level 1 is always valid"))
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
        // ScopeLogs inner = field 1 (InstrumentationScope) + repeated field 2 (LogRecord) entries
        let instrumentation_scope_inner_size =
            bytes_field_size(1, SCOPE_NAME.len()) + bytes_field_size(2, SCOPE_VERSION.len());

        let mut scope_logs_inner_size = bytes_field_size(1, instrumentation_scope_inner_size);
        for &(start, end) in &record_ranges {
            let record_len = end - start;
            // tag for field 2 wire type 2 + varint length + payload
            scope_logs_inner_size +=
                varint_len(((2u64) << 3) | 2) + varint_len(record_len as u64) + record_len;
        }

        // ResourceLogs inner = resource attributes (field 1) + scope_logs (field 2)
        let mut resource_inner_size = bytes_field_size(2, scope_logs_inner_size);

        // Encode resource attributes as Resource message (field 1 of ResourceLogs)
        let mut resource_msg: Vec<u8> = Vec::new();
        if !metadata.resource_attrs.is_empty() {
            for (k, v) in metadata.resource_attrs.as_ref() {
                encode_key_value_string(&mut resource_msg, k.as_bytes(), v.as_bytes());
            }
        }
        if !resource_msg.is_empty() {
            // Resource message field 1 (attributes) — we wrote KeyValues directly.
            // Wrap in Resource message (field 1 of ResourceLogs).
            resource_inner_size += bytes_field_size(1, resource_msg.len());
        }

        let request_size = bytes_field_size(1, resource_inner_size);

        // Phase 3: write the final protobuf.
        self.encoder_buf.reserve(request_size + 16);

        // ExportLogsServiceRequest.resource_logs (field 1)
        encode_tag(&mut self.encoder_buf, 1, 2);
        encode_varint(&mut self.encoder_buf, resource_inner_size as u64);

        // Resource (field 1 of ResourceLogs)
        if !resource_msg.is_empty() {
            encode_bytes_field(&mut self.encoder_buf, 1, &resource_msg);
        }

        // ScopeLogs (field 2 of ResourceLogs)
        encode_tag(&mut self.encoder_buf, 2, 2);
        encode_varint(&mut self.encoder_buf, scope_logs_inner_size as u64);

        // InstrumentationScope (field 1 of ScopeLogs)
        encode_tag(&mut self.encoder_buf, 1, 2);
        encode_varint(
            &mut self.encoder_buf,
            instrumentation_scope_inner_size as u64,
        );
        encode_bytes_field(&mut self.encoder_buf, 1, SCOPE_NAME);
        encode_bytes_field(&mut self.encoder_buf, 2, SCOPE_VERSION);

        // LogRecords (field 2 of ScopeLogs, repeated)
        for &(start, end) in &record_ranges {
            encode_bytes_field(&mut self.encoder_buf, 2, &records_buf[start..end]);
        }
    }
}

impl OutputSink for OtlpSink {
    fn send_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()> {
        self.encode_batch(batch, metadata);
        if self.encoder_buf.is_empty() {
            return Ok(());
        }

        let payload: &[u8] = match self.compression {
            Compression::Zstd => {
                if let Some(ref mut compressor) = self.compressor {
                    let chunk = compressor.compress(&self.encoder_buf)?;
                    self.compress_buf.clear();
                    self.compress_buf.extend_from_slice(&chunk.data);
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
            write_grpc_frame(&mut self.grpc_buf, payload, payload_is_compressed);
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
                    // inc_lines is counted by the pipeline; only track bytes here.
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
            // Struct conflict columns (status: Struct { int, str }) cannot be encoded as
            // a single typed OTLP attribute without coalescing. Skip them here; a SQL
            // transform with normalize_conflict_columns() produces a flat Utf8 column
            // that encodes correctly via the fallback arm below.
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

    // field 1: time_unix_nano (fixed64)
    if timestamp_ns > 0 {
        encode_fixed64(buf, 1, timestamp_ns);
    }

    // field 2: severity_number (varint)
    if severity_num as u8 > 0 {
        encode_varint_field(buf, 2, severity_num as u64);
    }

    // field 3: severity_text (string)
    if !severity_text.is_empty() {
        encode_bytes_field(buf, 3, severity_text);
    }

    // field 5: body (AnyValue { string_value })
    if !body_bytes.is_empty() {
        let anyvalue_inner_size = bytes_field_size(1, body_bytes.len());
        encode_tag(buf, 5, 2);
        encode_varint(buf, anyvalue_inner_size as u64);
        encode_bytes_field(buf, 1, body_bytes);
    }

    // field 6: attributes — pre-resolved attribute columns
    for (field_name, attr) in &columns.attribute_cols {
        match attr {
            AttrArray::Int(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_int(buf, field_name.as_bytes(), arr.value(row));
                }
            }
            AttrArray::Float(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_double(buf, field_name.as_bytes(), arr.value(row));
                }
            }
            AttrArray::Bool(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_bool(buf, field_name.as_bytes(), arr.value(row));
                }
            }
            AttrArray::Str(arr) => {
                if !arr.is_null(row) {
                    encode_key_value_string(
                        buf,
                        field_name.as_bytes(),
                        str_value(*arr, row).as_bytes(),
                    );
                }
            }
        }
    }

    // field 8: flags (fixed32) — W3C trace flags
    if let Some((_, arr)) = columns.flags_col {
        if !arr.is_null(row) {
            encode_fixed32(buf, 8, arr.value(row) as u32);
        }
    }

    // field 9: trace_id (bytes, 16 bytes) — hex-decoded from 32-char string column
    if let Some((_, arr)) = columns.trace_id_col {
        if !arr.is_null(row) {
            let hex = str_value(arr, row);
            let mut decoded = [0u8; 16];
            if hex_decode(hex.as_bytes(), &mut decoded) {
                encode_bytes_field(buf, 9, &decoded);
            }
        }
    }

    // field 10: span_id (bytes, 8 bytes) — hex-decoded from 16-char string column
    if let Some((_, arr)) = columns.span_id_col {
        if !arr.is_null(row) {
            let hex = str_value(arr, row);
            let mut decoded = [0u8; 8];
            if hex_decode(hex.as_bytes(), &mut decoded) {
                encode_bytes_field(buf, 10, &decoded);
            }
        }
    }

    // field 11: observed_time_unix_nano (fixed64)
    encode_fixed64(buf, 11, metadata.observed_time_ns);
}

/// Encode a KeyValue with string AnyValue as an attribute (field 6 of LogRecord).
/// KeyValue: { key (field 1, string), value (field 2, AnyValue { string_value (field 1) }) }
fn encode_key_value_string(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    let anyvalue_inner = bytes_field_size(1, value.len()); // AnyValue.string_value
    let kv_inner = bytes_field_size(1, key.len()) + bytes_field_size(2, anyvalue_inner);
    // LogRecord field 6, wire type 2
    encode_tag(buf, 6, 2);
    encode_varint(buf, kv_inner as u64);
    // KeyValue.key = field 1
    encode_bytes_field(buf, 1, key);
    // KeyValue.value = field 2 (AnyValue)
    encode_tag(buf, 2, 2);
    encode_varint(buf, anyvalue_inner as u64);
    // AnyValue.string_value = field 1
    encode_bytes_field(buf, 1, value);
}

/// Encode a KeyValue with int AnyValue (field 3 of AnyValue = int_value).
fn encode_key_value_int(buf: &mut Vec<u8>, key: &[u8], value: i64) {
    let anyvalue_inner = 1 + varint_len(value as u64); // tag(1 byte) + varint
    let kv_inner = bytes_field_size(1, key.len()) + bytes_field_size(2, anyvalue_inner);
    encode_tag(buf, 6, 2);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, 1, key);
    encode_tag(buf, 2, 2);
    encode_varint(buf, anyvalue_inner as u64);
    // AnyValue.int_value = field 3, wire type 0 (varint)
    encode_varint_field(buf, 3, value as u64);
}

/// Encode a KeyValue with double AnyValue (field 4 of AnyValue = double_value).
fn encode_key_value_double(buf: &mut Vec<u8>, key: &[u8], value: f64) {
    let anyvalue_inner = 1 + 8; // tag(1 byte) + fixed64
    let kv_inner = bytes_field_size(1, key.len()) + bytes_field_size(2, anyvalue_inner);
    encode_tag(buf, 6, 2);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, 1, key);
    encode_tag(buf, 2, 2);
    encode_varint(buf, anyvalue_inner as u64);
    // AnyValue.double_value = field 4, wire type 1 (64-bit fixed)
    encode_fixed64(buf, 4, value.to_bits());
}

/// Encode a KeyValue with boolean AnyValue (field 2 of AnyValue = bool_value).
fn encode_key_value_bool(buf: &mut Vec<u8>, key: &[u8], value: bool) {
    let anyvalue_inner = 1 + 1; // tag(1 byte) + varint(1 byte)
    let kv_inner = bytes_field_size(1, key.len()) + bytes_field_size(2, anyvalue_inner);
    encode_tag(buf, 6, 2);
    encode_varint(buf, kv_inner as u64);
    encode_bytes_field(buf, 1, key);
    encode_tag(buf, 2, 2);
    encode_varint(buf, anyvalue_inner as u64);
    // AnyValue.bool_value = field 2, wire type 0 (varint)
    encode_varint_field(buf, 2, u64::from(value));
}

/// Write a gRPC length-prefixed message frame into `buf`.
///
/// gRPC wire format (per the [gRPC over HTTP/2 specification](https://grpc.io/docs/what-is-grpc/core-concepts/)):
/// ```text
/// [1 byte: compressed flag (0 = not compressed, 1 = compressed)]
/// [4 bytes: big-endian message length]
/// [N bytes: protobuf message]
/// ```
fn write_grpc_frame(buf: &mut Vec<u8>, payload: &[u8], compressed: bool) {
    buf.clear();
    buf.push(u8::from(compressed));
    buf.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("gRPC message payload must be < 4 GiB")
            .to_be_bytes(),
    );
    buf.extend_from_slice(payload);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    /// Struct conflict columns (status: Struct { int, str }) must be skipped
    /// by the OTLP attribute encoder rather than emitting an empty-string attribute.
    #[test]
    fn struct_conflict_column_is_skipped_not_emitted_as_empty_string() {
        use arrow::array::{Int64Array as I64A, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field as F, Fields};

        let int_arr: Arc<dyn arrow::array::Array> = Arc::new(I64A::from(vec![Some(200i64), None]));
        let str_arr: Arc<dyn arrow::array::Array> =
            Arc::new(StringArray::from(vec![None::<&str>, Some("OK")]));
        let child_fields = Fields::from(vec![
            Arc::new(F::new("int", DataType::Int64, true)),
            Arc::new(F::new("str", DataType::Utf8, true)),
        ]);
        let validity = NullBuffer::from(vec![true, true]);
        let struct_arr: Arc<dyn arrow::array::Array> = Arc::new(StructArray::new(
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

        // The struct column must NOT appear in the encoded output —
        // no "status" key and no empty-string value.
        assert!(
            !contains_bytes(&sink.encoder_buf, b"status"),
            "struct conflict column 'status' must not be encoded as an OTLP attribute"
        );
    }

    fn make_sink() -> OtlpSink {
        OtlpSink::new(
            "test".into(),
            "http://localhost:4318".into(),
            OtlpProtocol::Http,
            Compression::None,
            vec![],
            Arc::new(logfwd_io::diagnostics::ComponentStats::new()),
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
        write_grpc_frame(&mut framed, &proto_payload, false);
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
        write_grpc_frame(&mut framed, &[], false);
        assert_eq!(framed.len(), 5);
        assert_eq!(framed[0], 0x00, "compressed flag must be 0x00");
        let msg_len = u32::from_be_bytes(framed[1..5].try_into().unwrap());
        assert_eq!(msg_len, 0, "length field must be zero for empty payload");
    }

    #[test]
    fn grpc_frame_compressed_flag() {
        let proto_payload = [0x0a, 0x02];
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, true);
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
            Arc::new(logfwd_io::diagnostics::ComponentStats::new()),
        );
        sink.encode_batch(&batch, &make_metadata());
        let proto_payload = sink.encoder_buf.clone();

        // Frame as send_batch would.
        let mut framed = Vec::new();
        write_grpc_frame(&mut framed, &proto_payload, false);

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
}
