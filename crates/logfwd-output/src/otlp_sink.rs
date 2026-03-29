use std::io;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use logfwd_core::compress::ChunkCompressor;
use logfwd_core::otlp::{
    Severity, bytes_field_size, encode_bytes_field, encode_fixed64, encode_tag, encode_varint,
    encode_varint_field, parse_severity, parse_timestamp_nanos, varint_len,
};

use super::{BatchMetadata, Compression, OutputSink, parse_column_name};

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
    pub encoder_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    compressor: Option<ChunkCompressor>,
}

impl OtlpSink {
    pub fn new(
        name: String,
        endpoint: String,
        protocol: OtlpProtocol,
        compression: Compression,
    ) -> Self {
        let compressor = match compression {
            Compression::Zstd => Some(ChunkCompressor::new(1)),
            _ => None,
        };
        OtlpSink {
            name,
            endpoint,
            protocol,
            compression,
            encoder_buf: Vec::with_capacity(64 * 1024),
            compress_buf: Vec::with_capacity(64 * 1024),
            compressor,
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

        // Phase 1: encode all LogRecords into a temp buffer.
        let mut records_buf: Vec<u8> = Vec::with_capacity(num_rows * 128);
        let mut record_ranges: Vec<(usize, usize)> = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let start = records_buf.len();
            encode_row_as_log_record(batch, row, metadata, &mut records_buf);
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

        let mut req = ureq::post(&self.endpoint);
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

/// Encode a single RecordBatch row as an OTLP LogRecord.
pub fn encode_row_as_log_record(
    batch: &RecordBatch,
    row: usize,
    metadata: &BatchMetadata,
    buf: &mut Vec<u8>,
) {
    let schema = batch.schema();

    // --- Find special columns ---
    let mut timestamp_ns: u64 = 0;
    let mut severity_num = Severity::Unspecified;
    let mut severity_text: &[u8] = b"";
    let mut body: Option<&str> = None;
    let mut body_col_idx: Option<usize> = None;
    let mut timestamp_col_idx: Option<usize> = None;
    let mut level_col_idx: Option<usize> = None;

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_name = field.name().as_str();
        let (field_name, _type_suffix) = parse_column_name(col_name);

        match field_name {
            "timestamp" | "time" | "ts" => {
                if timestamp_col_idx.is_none() && !batch.column(idx).is_null(row) {
                    timestamp_col_idx = Some(idx);
                    if let DataType::Utf8 = field.data_type() {
                        let arr = batch.column(idx).as_string::<i32>();
                        let val = arr.value(row);
                        timestamp_ns = parse_timestamp_nanos(val.as_bytes());
                    }
                }
            }
            "level" | "severity" | "log_level" | "loglevel" | "lvl" => {
                if level_col_idx.is_none() && !batch.column(idx).is_null(row) {
                    level_col_idx = Some(idx);
                    if let DataType::Utf8 = field.data_type() {
                        let arr = batch.column(idx).as_string::<i32>();
                        let val = arr.value(row);
                        let (sev, text) = parse_severity(val.as_bytes());
                        severity_num = sev;
                        severity_text = text;
                    }
                }
            }
            "message" | "msg" | "_msg" | "body" => {
                if body_col_idx.is_none() && !batch.column(idx).is_null(row) {
                    body_col_idx = Some(idx);
                    if let DataType::Utf8 = field.data_type() {
                        let arr = batch.column(idx).as_string::<i32>();
                        body = Some(arr.value(row));
                    }
                }
            }
            "_raw" => {
                // Fallback body source
                if body_col_idx.is_none() && !batch.column(idx).is_null(row) {
                    body_col_idx = Some(idx);
                    let arr = batch.column(idx).as_string::<i32>();
                    body = Some(arr.value(row));
                }
            }
            _ => {}
        }
    }

    let body_bytes = body.unwrap_or("").as_bytes();

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

    // field 6: attributes — all remaining columns
    for (idx, field) in schema.fields().iter().enumerate() {
        if Some(idx) == timestamp_col_idx || Some(idx) == level_col_idx || Some(idx) == body_col_idx
        {
            continue;
        }
        let col_name = field.name().as_str();
        let (field_name, type_suffix) = parse_column_name(col_name);
        if field_name == "_raw" {
            continue;
        }
        if batch.column(idx).is_null(row) {
            continue;
        }

        match type_suffix {
            "int" => {
                let arr = batch
                    .column(idx)
                    .as_primitive::<arrow::datatypes::Int64Type>();
                let v = arr.value(row);
                encode_key_value_int(buf, field_name.as_bytes(), v);
            }
            "float" => {
                let arr = batch
                    .column(idx)
                    .as_primitive::<arrow::datatypes::Float64Type>();
                let v = arr.value(row);
                encode_key_value_double(buf, field_name.as_bytes(), v);
            }
            _ => {
                let arr = batch.column(idx).as_string::<i32>();
                let v = arr.value(row);
                encode_key_value_string(buf, field_name.as_bytes(), v.as_bytes());
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
