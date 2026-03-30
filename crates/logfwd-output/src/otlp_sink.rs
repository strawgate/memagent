use std::io;

use arrow::array::{Array, AsArray, PrimitiveArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;

use logfwd_core::compress::ChunkCompressor;
use logfwd_core::otlp::{
    Severity, bytes_field_size, encode_bytes_field, encode_fixed64, encode_tag, encode_varint,
    encode_varint_field, parse_severity, parse_timestamp_nanos, varint_len,
};

use super::{BatchMetadata, Compression, OutputSink, parse_column_name, str_value};

// ---------------------------------------------------------------------------
// OtlpSink
// ---------------------------------------------------------------------------

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub encoder_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    compressor: Option<ChunkCompressor>,
    http_agent: ureq::Agent,
}

impl OtlpSink {
    pub fn new(
        name: String,
        endpoint: String,
        protocol: OtlpProtocol,
        compression: Compression,
        headers: Vec<(String, String)>,
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
            compressor,
            http_agent,
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
        // ScopeLogs inner = repeated field 2 (LogRecord) entries
        let mut scope_logs_inner_size = 0usize;
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
            for (k, v) in &metadata.resource_attrs {
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
            Compression::Gzip | Compression::None => &self.encoder_buf,
        };

        let content_type = match self.protocol {
            OtlpProtocol::Grpc => "application/grpc",
            OtlpProtocol::Http => "application/x-protobuf",
        };

        let mut req = self.http_agent.post(&self.endpoint);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", content_type);
        if self.compression == Compression::Zstd {
            req = req.header("Content-Encoding", "zstd");
        }

        req.send(payload)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
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
    // Indices of columns to exclude from attributes.
    let mut excluded: Vec<usize> = Vec::with_capacity(4);

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().as_str();
        let (field_name, _) = parse_column_name(col_name);
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
            _ => {}
        }
    }

    let mut attribute_cols: Vec<(String, AttrArray<'_>)> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if excluded.contains(&idx) {
            continue;
        }
        let col_name = field.name().as_str();
        let (field_name, type_suffix) = parse_column_name(col_name);
        let attr = match type_suffix {
            "int" => AttrArray::Int(batch.column(idx).as_primitive::<Int64Type>()),
            "float" => AttrArray::Float(batch.column(idx).as_primitive::<Float64Type>()),
            _ => AttrArray::Str(batch.column(idx).as_ref()),
        };
        attribute_cols.push((field_name.to_string(), attr));
    }

    BatchColumns {
        timestamp_col,
        level_col,
        body_col,
        raw_col,
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
                Some(parse_timestamp_nanos(str_value(arr, row).as_bytes()))
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
