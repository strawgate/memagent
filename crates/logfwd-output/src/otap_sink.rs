//! OTAP Arrow exporter — converts flat RecordBatches to OTAP star schema,
//! serializes each table as Arrow IPC, wraps in protobuf `BatchArrowRecords`,
//! and POSTs over HTTP.
//!
//! The OTAP wire format uses `BatchArrowRecords` with four `ArrowPayload`
//! entries (LOGS, LOG_ATTRS, RESOURCE_ATTRS, SCOPE_ATTRS). Each payload
//! carries Arrow IPC bytes. The response is a `BatchStatus` protobuf.
//!
//! Protobuf encoding is hand-rolled using the same helpers as `otlp_sink.rs`
//! to avoid a build.rs codegen dependency.
//!
//! ## Wire format reference
//!
//! ```text
//! message BatchArrowRecords {
//!   int64  batch_id        = 1;
//!   repeated ArrowPayload arrow_payloads = 2;
//!   bytes  headers         = 3;
//! }
//! message ArrowPayload {
//!   string          schema_id = 1;
//!   ArrowPayloadType type     = 2;
//!   bytes           record    = 3;
//! }
//! message BatchStatus {
//!   int64      batch_id       = 1;
//!   StatusCode status_code    = 2;
//!   string     status_message = 3;
//! }
//! enum ArrowPayloadType {
//!   LOGS = 0; LOG_ATTRS = 30; RESOURCE_ATTRS = 40; SCOPE_ATTRS = 50;
//! }
//! enum StatusCode { OK = 0; UNAVAILABLE = 1; INVALID_ARGUMENT = 2; }
//! ```

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use logfwd_arrow::star_schema::flat_to_star;
use logfwd_core::otlp::{self, encode_bytes_field, encode_tag, encode_varint, encode_varint_field};
use logfwd_types::diagnostics::ComponentStats;

use super::arrow_ipc_sink::serialize_ipc;
use super::sink::{SendResult, Sink, SinkFactory};
use super::{BatchMetadata, Compression};

/// Content-Type for protobuf-encoded OTAP messages.
const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Default retry-after duration when the server returns 429 without a
/// Retry-After header.
const DEFAULT_RETRY_AFTER_SECS: u64 = 5;

// ---------------------------------------------------------------------------
// ArrowPayloadType enum values (from OTAP proto)
// ---------------------------------------------------------------------------

/// OTAP `ArrowPayloadType` enum values.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ArrowPayloadType {
    /// LOGS fact table.
    Logs = 0,
    /// LOG_ATTRS dimension table.
    LogAttrs = 30,
    /// RESOURCE_ATTRS dimension table.
    ResourceAttrs = 40,
    /// SCOPE_ATTRS dimension table.
    ScopeAttrs = 50,
}

// ---------------------------------------------------------------------------
// StatusCode enum values (from OTAP proto)
// ---------------------------------------------------------------------------

/// OTAP `StatusCode` enum values.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatusCode {
    /// Batch accepted.
    Ok = 0,
    /// Server temporarily unavailable.
    Unavailable = 1,
    /// Invalid request.
    InvalidArgument = 2,
}

impl StatusCode {
    fn from_u64(v: u64) -> Self {
        match v {
            0 => StatusCode::Ok,
            1 => StatusCode::Unavailable,
            2 => StatusCode::InvalidArgument,
            _ => StatusCode::InvalidArgument,
        }
    }
}

// ---------------------------------------------------------------------------
// BatchStatus (decoded response)
// ---------------------------------------------------------------------------

/// A decoded `ArrowPayload`: (schema_id, payload_type_value, ipc_bytes).
pub type DecodedPayload = (String, u32, Vec<u8>);

/// Decoded OTAP `BatchStatus` response.
#[derive(Debug, Clone)]
pub struct BatchStatus {
    /// The batch_id this status corresponds to.
    pub batch_id: i64,
    /// Status code from the collector.
    pub status_code: StatusCode,
    /// Optional human-readable status message.
    pub status_message: String,
}

// ---------------------------------------------------------------------------
// Protobuf encoding
// ---------------------------------------------------------------------------

/// Encode an `ArrowPayload` message into `buf`.
///
/// ```text
/// message ArrowPayload {
///   string           schema_id = 1;  // length-delimited
///   ArrowPayloadType type      = 2;  // varint
///   bytes            record    = 3;  // length-delimited
/// }
/// ```
fn encode_arrow_payload(
    buf: &mut Vec<u8>,
    schema_id: &str,
    ptype: ArrowPayloadType,
    ipc_bytes: &[u8],
) {
    // field 1: schema_id (string = length-delimited, wire type 2)
    if !schema_id.is_empty() {
        encode_bytes_field(buf, 1, schema_id.as_bytes());
    }

    // field 2: type (enum = varint, wire type 0)
    // Only encode if non-zero (protobuf default for enums is 0).
    let type_val = ptype as u64;
    if type_val != 0 {
        encode_varint_field(buf, 2, type_val);
    }

    // field 3: record (bytes = length-delimited, wire type 2)
    if !ipc_bytes.is_empty() {
        encode_bytes_field(buf, 3, ipc_bytes);
    }
}

/// Encode a `BatchArrowRecords` message.
///
/// ```text
/// message BatchArrowRecords {
///   int64            batch_id       = 1;  // varint (zigzag for signed)
///   repeated ArrowPayload arrow_payloads = 2;  // length-delimited
///   bytes            headers        = 3;  // length-delimited
/// }
/// ```
pub fn encode_batch_arrow_records(
    buf: &mut Vec<u8>,
    batch_id: i64,
    payloads: &[(String, ArrowPayloadType, Vec<u8>)],
    headers: &[u8],
) {
    // field 1: batch_id (int64 = varint, wire type 0)
    // protobuf int64 uses standard varint (not zigzag for `int64` type).
    // Negative values use 10-byte two's complement encoding.
    if batch_id != 0 {
        encode_varint_field(buf, 1, batch_id as u64);
    }

    // field 2: repeated ArrowPayload (each as length-delimited submessage)
    for (schema_id, ptype, ipc_bytes) in payloads {
        // Encode the submessage into a temporary buffer to get its length.
        let mut sub = Vec::with_capacity(ipc_bytes.len() + 64);
        encode_arrow_payload(&mut sub, schema_id, *ptype, ipc_bytes);

        // Write tag + length + submessage bytes.
        encode_tag(buf, 2, 2); // field 2, wire type 2 (length-delimited)
        encode_varint(buf, sub.len() as u64);
        buf.extend_from_slice(&sub);
    }

    // field 3: headers (bytes = length-delimited, wire type 2)
    if !headers.is_empty() {
        encode_bytes_field(buf, 3, headers);
    }
}

// ---------------------------------------------------------------------------
// Protobuf decoding — thin wrappers around logfwd_core::otlp
// ---------------------------------------------------------------------------

fn decode_varint(data: &[u8], pos: usize) -> io::Result<(u64, usize)> {
    otlp::decode_varint(data, pos).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn decode_tag(data: &[u8], pos: usize) -> io::Result<(u32, u8, usize)> {
    otlp::decode_tag(data, pos).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn skip_field(data: &[u8], wire_type: u8, pos: usize) -> io::Result<usize> {
    otlp::skip_field(data, wire_type, pos)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Decode a `BatchStatus` response from protobuf bytes.
///
/// ```text
/// message BatchStatus {
///   int64      batch_id       = 1;  // varint
///   StatusCode status_code    = 2;  // varint (enum)
///   string     status_message = 3;  // length-delimited
/// }
/// ```
pub fn decode_batch_status(data: &[u8]) -> io::Result<BatchStatus> {
    let mut batch_id: i64 = 0;
    let mut status_code = StatusCode::Ok;
    let mut status_message = String::new();

    let mut pos = 0;
    while pos < data.len() {
        let (field_number, wire_type, new_pos) = decode_tag(data, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            (1, 0) => {
                // batch_id: int64 varint
                let (val, new_pos) = decode_varint(data, pos)?;
                batch_id = val as i64;
                pos = new_pos;
            }
            (2, 0) => {
                // status_code: enum varint
                let (val, new_pos) = decode_varint(data, pos)?;
                status_code = StatusCode::from_u64(val);
                pos = new_pos;
            }
            (3, 2) => {
                // status_message: string (length-delimited)
                let (len, new_pos) = decode_varint(data, pos)?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated status_message",
                    ));
                }
                status_message = String::from_utf8_lossy(&data[new_pos..end]).into_owned();
                pos = end;
            }
            _ => {
                // Unknown field — skip it for forward compatibility.
                pos = skip_field(data, wire_type, pos)?;
            }
        }
    }

    Ok(BatchStatus {
        batch_id,
        status_code,
        status_message,
    })
}

/// Decode a `BatchArrowRecords` from protobuf bytes.
///
/// Returns the batch_id and a list of (schema_id, payload_type, ipc_bytes)
/// for each `ArrowPayload` in the message.
pub fn decode_batch_arrow_records(data: &[u8]) -> io::Result<(i64, Vec<DecodedPayload>, Vec<u8>)> {
    let mut batch_id: i64 = 0;
    let mut payloads: Vec<DecodedPayload> = Vec::new();
    let mut headers: Vec<u8> = Vec::new();

    let mut pos = 0;
    while pos < data.len() {
        let (field_number, wire_type, new_pos) = decode_tag(data, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            (1, 0) => {
                // batch_id
                let (val, new_pos) = decode_varint(data, pos)?;
                batch_id = val as i64;
                pos = new_pos;
            }
            (2, 2) => {
                // ArrowPayload submessage
                let (len, new_pos) = decode_varint(data, pos)?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated ArrowPayload",
                    ));
                }
                let payload = decode_arrow_payload(&data[new_pos..end])?;
                payloads.push(payload);
                pos = end;
            }
            (3, 2) => {
                // headers
                let (len, new_pos) = decode_varint(data, pos)?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated headers",
                    ));
                }
                headers = data[new_pos..end].to_vec();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos)?;
            }
        }
    }

    Ok((batch_id, payloads, headers))
}

/// Decode a single `ArrowPayload` submessage.
/// Returns (schema_id, payload_type_value, ipc_bytes).
fn decode_arrow_payload(data: &[u8]) -> io::Result<DecodedPayload> {
    let mut schema_id = String::new();
    let mut payload_type: u32 = 0;
    let mut record: Vec<u8> = Vec::new();

    let mut pos = 0;
    while pos < data.len() {
        let (field_number, wire_type, new_pos) = decode_tag(data, pos)?;
        pos = new_pos;

        match (field_number, wire_type) {
            (1, 2) => {
                // schema_id: string
                let (len, new_pos) = decode_varint(data, pos)?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated schema_id",
                    ));
                }
                schema_id = String::from_utf8_lossy(&data[new_pos..end]).into_owned();
                pos = end;
            }
            (2, 0) => {
                // type: enum varint
                let (val, new_pos) = decode_varint(data, pos)?;
                payload_type = val as u32;
                pos = new_pos;
            }
            (3, 2) => {
                // record: bytes
                let (len, new_pos) = decode_varint(data, pos)?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated record",
                    ));
                }
                record = data[new_pos..end].to_vec();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos)?;
            }
        }
    }

    Ok((schema_id, payload_type, record))
}

// ---------------------------------------------------------------------------
// OtapSink
// ---------------------------------------------------------------------------

/// Async sink that converts flat RecordBatches to OTAP star schema,
/// serializes as protobuf `BatchArrowRecords`, and POSTs over HTTP.
pub struct OtapSink {
    name: String,
    config: Arc<OtapSinkConfig>,
    client: reqwest::Client,
    /// Monotonically increasing batch ID for request/response correlation.
    batch_counter: Arc<AtomicI64>,
    /// Reusable buffer for protobuf encoding.
    proto_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

/// Configuration shared across all `OtapSink` instances from the same factory.
pub(crate) struct OtapSinkConfig {
    endpoint: String,
    compression: Compression,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

impl OtapSink {
    pub(crate) fn new(
        name: String,
        config: Arc<OtapSinkConfig>,
        client: reqwest::Client,
        batch_counter: Arc<AtomicI64>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        OtapSink {
            name,
            config,
            client,
            batch_counter,
            proto_buf: Vec::with_capacity(128 * 1024),
            stats,
        }
    }

    /// Convert a flat RecordBatch to OTAP protobuf bytes.
    ///
    /// 1. `flat_to_star()` converts to 4 Arrow tables
    /// 2. `serialize_ipc()` serializes each table to Arrow IPC bytes
    /// 3. `encode_batch_arrow_records()` wraps in protobuf
    fn encode_batch(&mut self, batch: &RecordBatch) -> io::Result<i64> {
        self.proto_buf.clear();

        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let star = flat_to_star(batch).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("flat_to_star failed: {e}"),
            )
        })?;

        // Serialize each star schema table to Arrow IPC.
        let logs_ipc = serialize_ipc(&star.logs)?;
        let log_attrs_ipc = serialize_ipc(&star.log_attrs)?;
        let resource_attrs_ipc = serialize_ipc(&star.resource_attrs)?;
        let scope_attrs_ipc = serialize_ipc(&star.scope_attrs)?;

        let batch_id = self.batch_counter.fetch_add(1, Ordering::Relaxed) + 1;

        let payloads = vec![
            ("logs".to_string(), ArrowPayloadType::Logs, logs_ipc),
            (
                "log_attrs".to_string(),
                ArrowPayloadType::LogAttrs,
                log_attrs_ipc,
            ),
            (
                "resource_attrs".to_string(),
                ArrowPayloadType::ResourceAttrs,
                resource_attrs_ipc,
            ),
            (
                "scope_attrs".to_string(),
                ArrowPayloadType::ScopeAttrs,
                scope_attrs_ipc,
            ),
        ];

        encode_batch_arrow_records(&mut self.proto_buf, batch_id, &payloads, &[]);

        Ok(batch_id)
    }

    /// Optionally compress the protobuf payload.
    fn maybe_compress(&self) -> io::Result<Vec<u8>> {
        match self.config.compression {
            Compression::Zstd => zstd::bulk::compress(&self.proto_buf, 1).map_err(io::Error::other),
            Compression::None | Compression::Gzip => Ok(self.proto_buf.clone()),
        }
    }

    /// POST the encoded payload and parse the `BatchStatus` response.
    async fn do_send(&self, payload: Vec<u8>, batch_id: i64) -> io::Result<SendResult> {
        let mut req = self
            .client
            .post(&self.config.endpoint)
            .header("Content-Type", CONTENT_TYPE_PROTOBUF);

        if self.config.compression == Compression::Zstd {
            req = req.header("Content-Encoding", "zstd");
        }

        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        let response = req.body(payload).send().await.map_err(io::Error::other)?;
        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(DEFAULT_RETRY_AFTER_SECS);
            return Ok(SendResult::RetryAfter(Duration::from_secs(retry_after)));
        }

        if status.is_server_error() {
            let _body = response.text().await.unwrap_or_default();
            return Ok(SendResult::RetryAfter(Duration::from_secs(
                DEFAULT_RETRY_AFTER_SECS,
            )));
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Ok(SendResult::Rejected(format!("HTTP {status}: {body}")));
        }

        // Parse BatchStatus from the response body.
        let body_bytes = response.bytes().await.map_err(io::Error::other)?;
        if !body_bytes.is_empty() {
            let batch_status = decode_batch_status(&body_bytes)?;
            match batch_status.status_code {
                StatusCode::Ok => {}
                StatusCode::Unavailable => {
                    return Ok(SendResult::RetryAfter(Duration::from_secs(
                        DEFAULT_RETRY_AFTER_SECS,
                    )));
                }
                StatusCode::InvalidArgument => {
                    return Ok(SendResult::Rejected(format!(
                        "OTAP rejected batch {batch_id}: {}",
                        batch_status.status_message
                    )));
                }
            }
        }

        Ok(SendResult::Ok)
    }
}

impl Sink for OtapSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            let batch_id = match self.encode_batch(batch) {
                Ok(id) => id,
                Err(e) => return SendResult::IoError(e),
            };
            if self.proto_buf.is_empty() {
                return SendResult::Ok;
            }

            let payload = match self.maybe_compress() {
                Ok(p) => p,
                Err(e) => return SendResult::IoError(e),
            };
            let payload_len = payload.len() as u64;
            let row_count = batch.num_rows() as u64;

            let result = match self.do_send(payload, batch_id).await {
                Ok(r) => r,
                Err(e) => return SendResult::IoError(e),
            };
            if matches!(result, SendResult::Ok) {
                self.stats.inc_lines(row_count);
                self.stats.inc_bytes(payload_len);
            }
            result
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// OtapSinkFactory
// ---------------------------------------------------------------------------

/// Creates `OtapSink` instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` (connection pool reuse) and
/// a shared atomic batch counter for unique batch IDs.
pub struct OtapSinkFactory {
    name: String,
    config: Arc<OtapSinkConfig>,
    client: reqwest::Client,
    batch_counter: Arc<AtomicI64>,
    stats: Arc<ComponentStats>,
}

impl OtapSinkFactory {
    /// Create a new factory.
    ///
    /// - `endpoint`: Target HTTP URL (e.g. `http://collector:4317`)
    /// - `compression`: Compression algorithm (Zstd or None)
    /// - `headers`: Authentication / custom headers as `(key, value)` pairs
    pub fn new(
        name: String,
        endpoint: String,
        compression: Compression,
        headers: Vec<(String, String)>,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let parsed_headers = headers
            .into_iter()
            .map(|(k, v)| {
                let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                let value = reqwest::header::HeaderValue::from_str(&v)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                Ok((name, value))
            })
            .collect::<io::Result<Vec<_>>>()?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(64)
            .build()
            .map_err(io::Error::other)?;

        Ok(OtapSinkFactory {
            name,
            config: Arc::new(OtapSinkConfig {
                endpoint,
                compression,
                headers: parsed_headers,
            }),
            client,
            batch_counter: Arc::new(AtomicI64::new(0)),
            stats,
        })
    }
}

impl SinkFactory for OtapSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(OtapSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            self.client.clone(),
            Arc::clone(&self.batch_counter),
            Arc::clone(&self.stats),
        )))
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
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use logfwd_arrow::star_schema::{flat_to_star, star_to_flat};

    use super::super::arrow_ipc_sink::deserialize_ipc;

    /// Build a flat RecordBatch matching logfwd's pipeline output format.
    fn make_flat_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("_resource_host", DataType::Utf8, true),
            Field::new("_resource_namespace", DataType::Utf8, true),
            Field::new("request_id", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));
        let message = StringArray::from(vec![
            Some("request started"),
            Some("request completed"),
            Some("shutting down"),
        ]);
        let level = StringArray::from(vec![Some("INFO"), Some("INFO"), Some("WARN")]);
        let timestamp = StringArray::from(vec![
            Some("2024-03-15T10:30:00.000000000Z"),
            Some("2024-03-15T10:30:01.000000000Z"),
            Some("2024-03-15T10:30:02.000000000Z"),
        ]);
        let resource_host = StringArray::from(vec![Some("host-a"), Some("host-a"), Some("host-b")]);
        let resource_ns = StringArray::from(vec![
            Some("production"),
            Some("production"),
            Some("staging"),
        ]);
        let request_id = StringArray::from(vec![Some("abc-123"), Some("abc-123"), None]);
        let status = Int64Array::from(vec![Some(200), Some(200), None]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(message),
                Arc::new(level),
                Arc::new(timestamp),
                Arc::new(resource_host),
                Arc::new(resource_ns),
                Arc::new(request_id),
                Arc::new(status),
            ],
        )
        .expect("test batch creation should succeed")
    }

    #[test]
    fn encode_decode_batch_arrow_records_roundtrip() {
        let payloads = vec![
            (
                "logs".to_string(),
                ArrowPayloadType::Logs,
                b"fake-ipc-logs".to_vec(),
            ),
            (
                "log_attrs".to_string(),
                ArrowPayloadType::LogAttrs,
                b"fake-ipc-attrs".to_vec(),
            ),
            (
                "resource_attrs".to_string(),
                ArrowPayloadType::ResourceAttrs,
                b"fake-ipc-resource".to_vec(),
            ),
            (
                "scope_attrs".to_string(),
                ArrowPayloadType::ScopeAttrs,
                b"fake-ipc-scope".to_vec(),
            ),
        ];

        let mut buf = Vec::new();
        encode_batch_arrow_records(&mut buf, 42, &payloads, &[]);

        let (batch_id, decoded_payloads, headers) =
            decode_batch_arrow_records(&buf).expect("decode should succeed");

        assert_eq!(batch_id, 42);
        assert!(headers.is_empty());
        assert_eq!(decoded_payloads.len(), 4);

        // Verify each payload.
        assert_eq!(decoded_payloads[0].0, "logs");
        assert_eq!(decoded_payloads[0].1, ArrowPayloadType::Logs as u32);
        assert_eq!(decoded_payloads[0].2, b"fake-ipc-logs");

        assert_eq!(decoded_payloads[1].0, "log_attrs");
        assert_eq!(decoded_payloads[1].1, ArrowPayloadType::LogAttrs as u32);
        assert_eq!(decoded_payloads[1].2, b"fake-ipc-attrs");

        assert_eq!(decoded_payloads[2].0, "resource_attrs");
        assert_eq!(
            decoded_payloads[2].1,
            ArrowPayloadType::ResourceAttrs as u32
        );
        assert_eq!(decoded_payloads[2].2, b"fake-ipc-resource");

        assert_eq!(decoded_payloads[3].0, "scope_attrs");
        assert_eq!(decoded_payloads[3].1, ArrowPayloadType::ScopeAttrs as u32);
        assert_eq!(decoded_payloads[3].2, b"fake-ipc-scope");
    }

    #[test]
    fn encode_decode_batch_arrow_records_with_headers() {
        let payloads = vec![("test".to_string(), ArrowPayloadType::Logs, b"data".to_vec())];

        let mut buf = Vec::new();
        encode_batch_arrow_records(&mut buf, 7, &payloads, b"custom-headers");

        let (batch_id, decoded_payloads, headers) =
            decode_batch_arrow_records(&buf).expect("decode should succeed");

        assert_eq!(batch_id, 7);
        assert_eq!(decoded_payloads.len(), 1);
        assert_eq!(headers, b"custom-headers");
    }

    #[test]
    fn encode_decode_batch_status_roundtrip() {
        // Encode a BatchStatus manually.
        let mut buf = Vec::new();
        // field 1: batch_id = 42
        encode_varint_field(&mut buf, 1, 42);
        // field 2: status_code = OK (0) — omit since default
        // field 3: status_message = "success"
        encode_bytes_field(&mut buf, 3, b"success");

        let status = decode_batch_status(&buf).expect("decode should succeed");
        assert_eq!(status.batch_id, 42);
        assert_eq!(status.status_code, StatusCode::Ok);
        assert_eq!(status.status_message, "success");
    }

    #[test]
    fn decode_batch_status_unavailable() {
        let mut buf = Vec::new();
        encode_varint_field(&mut buf, 1, 99);
        encode_varint_field(&mut buf, 2, StatusCode::Unavailable as u64);
        encode_bytes_field(&mut buf, 3, b"server overloaded");

        let status = decode_batch_status(&buf).expect("decode should succeed");
        assert_eq!(status.batch_id, 99);
        assert_eq!(status.status_code, StatusCode::Unavailable);
        assert_eq!(status.status_message, "server overloaded");
    }

    #[test]
    fn decode_batch_status_invalid_argument() {
        let mut buf = Vec::new();
        encode_varint_field(&mut buf, 1, 5);
        encode_varint_field(&mut buf, 2, StatusCode::InvalidArgument as u64);
        encode_bytes_field(&mut buf, 3, b"bad schema");

        let status = decode_batch_status(&buf).expect("decode should succeed");
        assert_eq!(status.batch_id, 5);
        assert_eq!(status.status_code, StatusCode::InvalidArgument);
        assert_eq!(status.status_message, "bad schema");
    }

    #[test]
    fn decode_batch_status_empty_message() {
        let status = decode_batch_status(&[]).expect("empty should decode to defaults");
        assert_eq!(status.batch_id, 0);
        assert_eq!(status.status_code, StatusCode::Ok);
        assert!(status.status_message.is_empty());
    }

    #[test]
    fn full_pipeline_flat_to_otap_roundtrip() {
        // This is the key integration test: flat → star → IPC → protobuf → decode → IPC → star → flat.
        let original = make_flat_batch();
        let num_rows = original.num_rows();

        // Step 1: flat_to_star
        let star = flat_to_star(&original).expect("flat_to_star should succeed");
        assert_eq!(star.logs.num_rows(), num_rows);

        // Step 2: Serialize each table to Arrow IPC
        let logs_ipc = serialize_ipc(&star.logs).expect("logs IPC");
        let log_attrs_ipc = serialize_ipc(&star.log_attrs).expect("log_attrs IPC");
        let resource_attrs_ipc = serialize_ipc(&star.resource_attrs).expect("resource_attrs IPC");
        let scope_attrs_ipc = serialize_ipc(&star.scope_attrs).expect("scope_attrs IPC");

        // Step 3: Encode as BatchArrowRecords protobuf
        let payloads = vec![
            ("logs".to_string(), ArrowPayloadType::Logs, logs_ipc),
            (
                "log_attrs".to_string(),
                ArrowPayloadType::LogAttrs,
                log_attrs_ipc,
            ),
            (
                "resource_attrs".to_string(),
                ArrowPayloadType::ResourceAttrs,
                resource_attrs_ipc,
            ),
            (
                "scope_attrs".to_string(),
                ArrowPayloadType::ScopeAttrs,
                scope_attrs_ipc,
            ),
        ];

        let mut proto_buf = Vec::new();
        encode_batch_arrow_records(&mut proto_buf, 1, &payloads, &[]);
        assert!(
            !proto_buf.is_empty(),
            "protobuf encoding should produce bytes"
        );

        // Step 4: Decode the protobuf
        let (batch_id, decoded_payloads, _headers) =
            decode_batch_arrow_records(&proto_buf).expect("decode should succeed");
        assert_eq!(batch_id, 1);
        assert_eq!(decoded_payloads.len(), 4);

        // Step 5: Deserialize IPC bytes back to RecordBatches
        let decoded_logs = deserialize_ipc(&decoded_payloads[0].2).expect("logs IPC decode");
        let decoded_log_attrs =
            deserialize_ipc(&decoded_payloads[1].2).expect("log_attrs IPC decode");
        let decoded_resource_attrs =
            deserialize_ipc(&decoded_payloads[2].2).expect("resource_attrs IPC decode");
        let decoded_scope_attrs =
            deserialize_ipc(&decoded_payloads[3].2).expect("scope_attrs IPC decode");

        assert_eq!(decoded_logs.len(), 1);
        assert_eq!(decoded_log_attrs.len(), 1);
        assert_eq!(decoded_resource_attrs.len(), 1);
        assert_eq!(decoded_scope_attrs.len(), 1);

        // Step 6: Reconstruct star schema and convert back to flat
        let reconstructed_star = logfwd_arrow::star_schema::StarSchema {
            logs: decoded_logs.into_iter().next().expect("logs batch"),
            log_attrs: decoded_log_attrs
                .into_iter()
                .next()
                .expect("log_attrs batch"),
            resource_attrs: decoded_resource_attrs
                .into_iter()
                .next()
                .expect("resource_attrs batch"),
            scope_attrs: decoded_scope_attrs
                .into_iter()
                .next()
                .expect("scope_attrs batch"),
        };

        let flat = star_to_flat(&reconstructed_star).expect("star_to_flat should succeed");
        assert_eq!(flat.num_rows(), num_rows);

        // Verify key columns survived the roundtrip.
        // Note: star_to_flat produces Utf8 columns for all values.
        let flat_schema = flat.schema();

        // Check "message" column.
        let msg_idx = flat_schema
            .index_of("message")
            .expect("should have message column");
        let msg_arr = flat
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("message should be StringArray");
        assert_eq!(msg_arr.value(0), "request started");
        assert_eq!(msg_arr.value(1), "request completed");
        assert_eq!(msg_arr.value(2), "shutting down");

        // Check "level" column.
        let level_idx = flat_schema
            .index_of("level")
            .expect("should have level column");
        let level_arr = flat
            .column(level_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("level should be StringArray");
        assert_eq!(level_arr.value(0), "INFO");
        assert_eq!(level_arr.value(1), "INFO");
        assert_eq!(level_arr.value(2), "WARN");

        // Check resource attributes are preserved.
        let host_idx = flat_schema
            .index_of("_resource_host")
            .expect("should have _resource_host column");
        let host_arr = flat
            .column(host_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("_resource_host should be StringArray");
        assert_eq!(host_arr.value(0), "host-a");
        assert_eq!(host_arr.value(1), "host-a");
        assert_eq!(host_arr.value(2), "host-b");
    }

    #[test]
    fn empty_batch_produces_empty_protobuf() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::new_empty(schema);

        let config = Arc::new(OtapSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::None,
            headers: Vec::new(),
        });
        let stats = Arc::new(ComponentStats::new());
        let client = reqwest::Client::new();
        let counter = Arc::new(AtomicI64::new(0));
        let mut sink = OtapSink::new("test".to_string(), config, client, counter, stats);

        let batch_id = sink.encode_batch(&batch).expect("encode empty batch");
        assert_eq!(batch_id, 0);
        assert!(
            sink.proto_buf.is_empty(),
            "empty batch should produce no protobuf bytes"
        );
    }

    #[test]
    fn encode_batch_assigns_incrementing_ids() {
        let batch = make_flat_batch();

        let config = Arc::new(OtapSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::None,
            headers: Vec::new(),
        });
        let stats = Arc::new(ComponentStats::new());
        let client = reqwest::Client::new();
        let counter = Arc::new(AtomicI64::new(0));
        let mut sink = OtapSink::new("test".to_string(), config, client, counter, stats);

        let id1 = sink.encode_batch(&batch).expect("first encode");
        assert_eq!(id1, 1);

        let id2 = sink.encode_batch(&batch).expect("second encode");
        assert_eq!(id2, 2);

        let id3 = sink.encode_batch(&batch).expect("third encode");
        assert_eq!(id3, 3);
    }

    #[test]
    fn protobuf_encoding_handles_large_batch_ids() {
        let payloads = vec![("logs".to_string(), ArrowPayloadType::Logs, b"data".to_vec())];

        let mut buf = Vec::new();
        encode_batch_arrow_records(&mut buf, i64::MAX, &payloads, &[]);

        let (batch_id, decoded_payloads, _) =
            decode_batch_arrow_records(&buf).expect("decode should succeed");

        assert_eq!(batch_id, i64::MAX);
        assert_eq!(decoded_payloads.len(), 1);
    }

    #[test]
    fn decode_batch_status_skips_unknown_fields() {
        // Encode a status with an extra unknown field (field 99, varint).
        let mut buf = Vec::new();
        encode_varint_field(&mut buf, 1, 10); // batch_id
        encode_varint_field(&mut buf, 99, 12345); // unknown field
        encode_varint_field(&mut buf, 2, StatusCode::Ok as u64);
        encode_bytes_field(&mut buf, 3, b"ok");

        let status = decode_batch_status(&buf).expect("should skip unknown fields");
        assert_eq!(status.batch_id, 10);
        assert_eq!(status.status_code, StatusCode::Ok);
        assert_eq!(status.status_message, "ok");
    }
}
