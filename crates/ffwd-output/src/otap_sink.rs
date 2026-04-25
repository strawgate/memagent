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
//!
//! **Wire-format note:** The old hand-rolled encoder used `BATCH_STATUS_OK = 1`,
//! which mapped to `UNAVAILABLE` in the proto enum. The prost-based encoder
//! correctly uses `OK = 0`. This is a wire-format fix but a breaking change
//! for mixed deployments where old senders talk to new receivers (or vice versa).
//! Old receivers that checked `status_code == 1` for success will misinterpret
//! the corrected `0` value.

use std::future::Future;
use std::io;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use ffwd_otap_proto::otap::{
    ArrowPayload as ProtoArrowPayload, ArrowPayloadType as ProtoArrowPayloadType,
    BatchArrowRecords as ProtoBatchArrowRecords, BatchStatus as ProtoBatchStatus,
    StatusCode as ProtoStatusCode,
};
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;
use prost::Message;

use ffwd_arrow::conflict_schema::{has_conflict_struct_columns, normalize_conflict_columns};
use ffwd_arrow::star_schema::flat_to_star;
use ffwd_types::diagnostics::ComponentStats;

use super::arrow_ipc_sink::serialize_ipc;
use super::sink::{SendResult, Sink, SinkFactory};
use super::{BatchMetadata, Compression};
use crate::http_classify::{self, DEFAULT_RETRY_AFTER_SECS};

mod generated_fast {
    include!("generated/otap_fast_v1.rs");
}

/// Content-Type for protobuf-encoded OTAP messages.
const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

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
    fn from_proto(status: ProtoStatusCode) -> Self {
        match status {
            ProtoStatusCode::Ok => Self::Ok,
            ProtoStatusCode::Unavailable => Self::Unavailable,
            ProtoStatusCode::InvalidArgument => Self::InvalidArgument,
        }
    }
}

impl ArrowPayloadType {
    fn to_proto(self) -> ProtoArrowPayloadType {
        match self {
            Self::Logs => ProtoArrowPayloadType::Logs,
            Self::LogAttrs => ProtoArrowPayloadType::LogAttrs,
            Self::ResourceAttrs => ProtoArrowPayloadType::ResourceAttrs,
            Self::ScopeAttrs => ProtoArrowPayloadType::ScopeAttrs,
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

/// Encode a `BatchArrowRecords` message.
///
/// ```text
/// message BatchArrowRecords {
///   int64            batch_id       = 1;  // standard varint (not zigzag; that's sint64)
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
    let message = ProtoBatchArrowRecords {
        batch_id,
        arrow_payloads: payloads
            .iter()
            // Clone is required because the proto struct takes ownership but we
            // borrow the slice. Changing the signature to consume a Vec would
            // avoid the clone but would break the public API and all callers.
            .map(|(schema_id, payload_type, record)| ProtoArrowPayload {
                schema_id: schema_id.clone(),
                r#type: payload_type.to_proto() as i32,
                record: record.clone(),
            })
            .collect(),
        headers: headers.to_vec(),
    };
    message.encode(buf).expect("vec-backed encode cannot fail");
}

/// Benchmark/reference path: encode OTAP with the checked-in fast generator.
pub fn encode_batch_arrow_records_generated_fast(
    buf: &mut Vec<u8>,
    batch_id: i64,
    payloads: &[(String, ArrowPayloadType, Vec<u8>)],
    headers: &[u8],
) {
    generated_fast::encode_batch_arrow_records_generated_fast(buf, batch_id, payloads, headers);
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
    if data.is_empty() {
        return Ok(BatchStatus {
            batch_id: 0,
            status_code: StatusCode::Ok,
            status_message: String::new(),
        });
    }
    let decoded = ProtoBatchStatus::decode(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let status_code = StatusCode::from_proto(
        // Defensive: unknown status codes are treated as errors, not success.
        ProtoStatusCode::try_from(decoded.status_code).unwrap_or(ProtoStatusCode::InvalidArgument),
    );

    Ok(BatchStatus {
        batch_id: decoded.batch_id,
        status_code,
        status_message: decoded.status_message,
    })
}

/// Benchmark/reference path: decode OTAP `BatchStatus` with the checked-in fast generator.
pub fn decode_batch_status_generated_fast(data: &[u8]) -> io::Result<BatchStatus> {
    generated_fast::decode_batch_status_generated_fast(data)
}

/// Decode a `BatchArrowRecords` from protobuf bytes.
///
/// Returns the batch_id and a list of (schema_id, payload_type, ipc_bytes)
/// for each `ArrowPayload` in the message.
pub fn decode_batch_arrow_records(data: &[u8]) -> io::Result<(i64, Vec<DecodedPayload>, Vec<u8>)> {
    let decoded = ProtoBatchArrowRecords::decode(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let payloads = decoded
        .arrow_payloads
        .into_iter()
        .map(|payload| (payload.schema_id, payload.r#type as u32, payload.record))
        .collect();
    Ok((decoded.batch_id, payloads, decoded.headers))
}

/// Benchmark/reference path: decode `BatchArrowRecords` with the checked-in fast generator.
pub fn decode_batch_arrow_records_generated_fast(
    data: &[u8],
) -> io::Result<(i64, Vec<DecodedPayload>, Vec<u8>)> {
    generated_fast::decode_batch_arrow_records_generated_fast(data)
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

        // Normalize conflict struct columns (e.g. `status: Struct { int, str }`)
        // to flat Utf8 before the star schema pivot. Without this, struct-valued
        // attributes can still produce star-schema rows, but their rendered value
        // is empty because str_value_at returns "" for Struct types.
        // The OTLP sink applies the same normalization before encoding.
        let normalized;
        let batch = if has_conflict_struct_columns(batch.schema().as_ref()) {
            normalized = normalize_conflict_columns(batch.clone());
            &normalized
        } else {
            batch
        };

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
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::fast());
                encoder.write_all(&self.proto_buf)?;
                encoder.finish()
            }
            Compression::None => Ok(self.proto_buf.clone()),
        }
    }

    /// POST the encoded payload and parse the `BatchStatus` response.
    async fn do_send(&self, payload: Vec<u8>, batch_id: i64) -> io::Result<SendResult> {
        let mut req = self
            .client
            .post(&self.config.endpoint)
            .header("Content-Type", CONTENT_TYPE_PROTOBUF);

        match self.config.compression {
            Compression::Zstd => req = req.header("Content-Encoding", "zstd"),
            Compression::Gzip => req = req.header("Content-Encoding", "gzip"),
            Compression::None => {}
        }

        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        let response = req.body(payload).send().await.map_err(io::Error::other)?;
        let status = response.status();

        if !status.is_success() {
            let retry_after = response.headers().get("Retry-After").cloned();
            let body = response.text().await.unwrap_or_default();
            if let Some(send_result) = http_classify::classify_http_status(
                status.as_u16(),
                retry_after.as_ref(),
                &format!("OTAP: {body}"),
            ) {
                return Ok(send_result);
            }
            return Err(io::Error::other(format!(
                "OTAP request failed with status {status}: {body}"
            )));
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

    #[test]
    fn integer_overflow_varint_len() {
        // Crafted protobuf with a varint field length that overflows usize.
        let data = vec![
            0x1a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ];
        let err = generated_fast::decode_batch_arrow_records_generated_fast(&data).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
    use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use ffwd_arrow::star_schema::{flat_to_star, star_to_flat};
    use ffwd_core::otlp::encode_varint_field;
    use reqwest::header::{CONTENT_ENCODING, RETRY_AFTER};

    use super::super::arrow_ipc_sink::deserialize_ipc;

    /// Build a flat RecordBatch matching ffwd's pipeline output format.
    fn make_flat_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("resource.attributes.host", DataType::Utf8, true),
            Field::new("resource.attributes.namespace", DataType::Utf8, true),
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
    fn generated_fast_batch_arrow_records_matches_generated_prost() {
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

        let mut generated = Vec::new();
        encode_batch_arrow_records(&mut generated, 42, &payloads, b"custom-headers");

        let mut fast = Vec::new();
        encode_batch_arrow_records_generated_fast(&mut fast, 42, &payloads, b"custom-headers");

        assert_eq!(
            fast, generated,
            "generated-fast OTAP encoding drifted from prost path"
        );
    }

    #[test]
    fn encode_decode_batch_status_roundtrip() {
        let mut buf = Vec::new();
        ProtoBatchStatus {
            batch_id: 42,
            status_code: ProtoStatusCode::Ok as i32,
            status_message: "success".to_string(),
        }
        .encode(&mut buf)
        .expect("vec-backed encode cannot fail");

        let status = decode_batch_status(&buf).expect("decode should succeed");
        assert_eq!(status.batch_id, 42);
        assert_eq!(status.status_code, StatusCode::Ok);
        assert_eq!(status.status_message, "success");
    }

    #[test]
    fn decode_batch_status_unavailable() {
        let mut buf = Vec::new();
        ProtoBatchStatus {
            batch_id: 99,
            status_code: ProtoStatusCode::Unavailable as i32,
            status_message: "server overloaded".to_string(),
        }
        .encode(&mut buf)
        .expect("vec-backed encode cannot fail");

        let status = decode_batch_status(&buf).expect("decode should succeed");
        assert_eq!(status.batch_id, 99);
        assert_eq!(status.status_code, StatusCode::Unavailable);
        assert_eq!(status.status_message, "server overloaded");
    }

    #[test]
    fn decode_batch_status_invalid_argument() {
        let mut buf = Vec::new();
        ProtoBatchStatus {
            batch_id: 5,
            status_code: ProtoStatusCode::InvalidArgument as i32,
            status_message: "bad schema".to_string(),
        }
        .encode(&mut buf)
        .expect("vec-backed encode cannot fail");

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
        let reconstructed_star = ffwd_arrow::star_schema::StarSchema {
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
            .index_of("resource.attributes.host")
            .expect("should have resource.attributes.host column");
        let host_arr = flat
            .column(host_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resource.attributes.host should be StringArray");
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
        let mut buf = Vec::new();
        ProtoBatchStatus {
            batch_id: 10,
            status_code: ProtoStatusCode::Ok as i32,
            status_message: "ok".to_string(),
        }
        .encode(&mut buf)
        .expect("vec-backed encode cannot fail");
        encode_varint_field(&mut buf, 99, 12345);

        let status = decode_batch_status(&buf).expect("should skip unknown fields");
        assert_eq!(status.batch_id, 10);
        assert_eq!(status.status_code, StatusCode::Ok);
        assert_eq!(status.status_message, "ok");
    }

    #[test]
    fn generated_fast_decoders_match_generated_prost() {
        let payloads = vec![
            ("logs".to_string(), ArrowPayloadType::Logs, b"data".to_vec()),
            (
                "resource_attrs".to_string(),
                ArrowPayloadType::ResourceAttrs,
                b"resource".to_vec(),
            ),
        ];

        let mut buf = Vec::new();
        encode_batch_arrow_records(&mut buf, 11, &payloads, b"headers");

        let generated = decode_batch_arrow_records(&buf).expect("generated decode");
        let fast = decode_batch_arrow_records_generated_fast(&buf).expect("generated fast decode");
        assert_eq!(fast, generated, "generated-fast OTAP batch decode drifted");

        let mut status_buf = Vec::new();
        ProtoBatchStatus {
            batch_id: 99,
            status_code: ProtoStatusCode::Unavailable as i32,
            status_message: "busy".to_string(),
        }
        .encode(&mut status_buf)
        .expect("vec-backed encode cannot fail");

        let generated_status = decode_batch_status(&status_buf).expect("generated status decode");
        let fast_status =
            decode_batch_status_generated_fast(&status_buf).expect("generated fast status decode");
        assert_eq!(fast_status.batch_id, generated_status.batch_id);
        assert_eq!(
            fast_status.status_code as u32,
            generated_status.status_code as u32
        );
        assert_eq!(fast_status.status_message, generated_status.status_message);
    }

    /// Regression test for #1656: conflict struct columns must be normalized
    /// before the flat→star pivot in encode_batch.
    ///
    /// Without the fix, a `status` column of type `Struct { int: Int64, str: Utf8 }`
    /// would still emit LOG_ATTRS rows for non-null values, but the attribute string
    /// value would not be populated correctly because `str_value_at` returns "" for
    /// Struct data types. With the fix, the column is coalesced to a Utf8 column first.
    #[test]
    fn conflict_struct_column_is_normalized_not_dropped() {
        use arrow::array::{Array, ArrayRef, Int64Array, StructArray};
        use arrow::datatypes::Fields;

        // Build a batch with a conflict struct column `status: Struct { int: Int64, str: Utf8 }`.
        // Row 0: int=200 (int child non-null, str child null)
        // Row 1: str="OK" (str child non-null, int child null)
        let conflict_fields: Fields = Fields::from(vec![
            Field::new("int", DataType::Int64, true),
            Field::new("str", DataType::Utf8, true),
        ]);
        let int_child = Arc::new(Int64Array::from(vec![Some(200_i64), None])) as ArrayRef;
        let str_child = Arc::new(StringArray::from(vec![None, Some("OK")])) as ArrayRef;
        let conflict_col =
            StructArray::new(conflict_fields.clone(), vec![int_child, str_child], None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("status", DataType::Struct(conflict_fields), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("req1"), Some("req2")])) as ArrayRef,
                Arc::new(conflict_col) as ArrayRef,
            ],
        )
        .unwrap();

        let config = Arc::new(OtapSinkConfig {
            endpoint: "http://localhost:4317".to_string(),
            compression: Compression::None,
            headers: vec![],
        });
        let client = reqwest::Client::new();
        let counter = Arc::new(AtomicI64::new(0));
        let stats = Arc::new(ComponentStats::new());
        let mut sink = OtapSink::new("test".to_string(), config, client, counter, stats);

        // Must not error (before the fix, struct column values would be rendered as empty strings).
        let batch_id = sink
            .encode_batch(&batch)
            .expect("encode_batch should succeed");
        assert_eq!(batch_id, 1);

        // Decode to verify the 'status' attribute appears in LOG_ATTRS.
        let (_, payloads, _) =
            decode_batch_arrow_records(&sink.proto_buf).expect("decode_batch_arrow_records");
        assert_eq!(payloads.len(), 4);

        let log_attrs_ipc = payloads
            .iter()
            .find(|(_, ptype, _)| *ptype == ArrowPayloadType::LogAttrs as u32)
            .map(|(_, _, ipc)| ipc)
            .expect("LOG_ATTRS payload must be present");
        let log_attrs_batches = deserialize_ipc(log_attrs_ipc).expect("deserialize log_attrs IPC");
        let log_attrs = log_attrs_batches
            .into_iter()
            .next()
            .expect("log_attrs batch");

        // The `status` column must appear as a LOG_ATTR key.
        let key_arr = log_attrs.column_by_name("key").expect("key column");
        let key_str = key_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key is StringArray");
        let num_attrs = key_arr.len();
        let keys: Vec<&str> = (0..num_attrs)
            .filter(|&i| !key_arr.is_null(i))
            .map(|i| key_str.value(i))
            .collect();
        assert!(
            keys.contains(&"status"),
            "expected 'status' in LOG_ATTRS keys, got: {keys:?}"
        );

        // Verify both coalesced values are present ("200" from int child and "OK" from str).
        let str_arr = log_attrs.column_by_name("str").expect("str column");
        let str_col = str_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str is StringArray");
        let status_vals: Vec<&str> = (0..num_attrs)
            .filter(|&i| !key_arr.is_null(i) && key_str.value(i) == "status")
            .filter(|&i| !str_arr.is_null(i))
            .map(|i| str_col.value(i))
            .collect();
        assert!(
            status_vals.contains(&"200") && status_vals.contains(&"OK"),
            "expected both coalesced status values (200 and OK), got: {status_vals:?}"
        );
    }

    #[test]
    fn internal_columns_are_not_encoded_as_log_attrs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("__source_id", DataType::UInt64, true),
            Field::new("__typename", DataType::Utf8, true),
            Field::new("file.path", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("request")])),
                Arc::new(UInt64Array::from(vec![Some(7)])),
                Arc::new(StringArray::from(vec![Some("LogEvent")])),
                Arc::new(StringArray::from(vec![Some("/var/log/app.log")])),
            ],
        )
        .expect("valid batch");
        let config = Arc::new(OtapSinkConfig {
            endpoint: "http://localhost:4317".to_string(),
            compression: Compression::None,
            headers: vec![],
        });
        let client = reqwest::Client::new();
        let counter = Arc::new(AtomicI64::new(0));
        let stats = Arc::new(ComponentStats::new());
        let mut sink = OtapSink::new("test".to_string(), config, client, counter, stats);

        sink.encode_batch(&batch).expect("encode_batch");

        let (_, payloads, _) =
            decode_batch_arrow_records(&sink.proto_buf).expect("decode_batch_arrow_records");
        let log_attrs_ipc = payloads
            .iter()
            .find(|(_, ptype, _)| *ptype == ArrowPayloadType::LogAttrs as u32)
            .map(|(_, _, ipc)| ipc)
            .expect("LOG_ATTRS payload");
        let log_attrs_batches = deserialize_ipc(log_attrs_ipc).expect("deserialize LOG_ATTRS");
        let log_attrs = log_attrs_batches
            .into_iter()
            .next()
            .expect("log_attrs batch");
        let key_arr = log_attrs.column_by_name("key").expect("key column");
        let key_str = key_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key is StringArray");
        let keys: Vec<&str> = (0..key_arr.len())
            .filter(|&i| !key_arr.is_null(i))
            .map(|i| key_str.value(i))
            .collect();

        assert!(
            !keys.contains(&"__source_id"),
            "internal source id must not be exported as LOG_ATTRS: {keys:?}"
        );
        assert!(
            keys.contains(&"file.path"),
            "public source metadata columns remain regular output columns: {keys:?}"
        );
        assert!(
            keys.contains(&"__typename"),
            "user double-underscore columns must remain regular output columns: {keys:?}"
        );
    }

    #[test]
    fn maybe_compress_gzip_produces_valid_gzip_stream() {
        use std::io::Read as _;

        let config = Arc::new(OtapSinkConfig {
            endpoint: "http://localhost:4318/v1/otap".to_string(),
            compression: Compression::Gzip,
            headers: vec![],
        });
        let mut sink = OtapSink::new(
            "test".to_string(),
            config,
            reqwest::Client::new(),
            Arc::new(AtomicI64::new(0)),
            Arc::new(ComponentStats::new()),
        );
        sink.proto_buf = b"sample-otap-payload".to_vec();

        let compressed = sink
            .maybe_compress()
            .expect("gzip compression should succeed");
        assert_ne!(compressed, sink.proto_buf, "payload should be compressed");

        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(compressed.as_slice())
            .read_to_end(&mut decoded)
            .expect("gzip payload should decompress");
        assert_eq!(
            decoded, sink.proto_buf,
            "gzip roundtrip should preserve protobuf bytes"
        );
    }

    #[tokio::test]
    async fn otap_send_gzip_sets_content_encoding_header() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/v1/otap")
            .match_header(CONTENT_ENCODING.as_str(), "gzip")
            .with_status(200)
            .create_async()
            .await;

        let config = Arc::new(OtapSinkConfig {
            endpoint: format!("{}/v1/otap", server.url()),
            compression: Compression::Gzip,
            headers: vec![],
        });
        let sink = OtapSink::new(
            "test".to_string(),
            config,
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("client"),
            Arc::new(AtomicI64::new(0)),
            Arc::new(ComponentStats::new()),
        );

        let payload = b"sample-otap-payload".to_vec();
        let send_result = sink
            .do_send(payload, 1)
            .await
            .expect("do_send should succeed");
        assert!(matches!(send_result, SendResult::Ok));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn otap_send_retry_after_http_date_is_honored() {
        let retry_after_http_date =
            httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(60));

        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/v1/otap")
            .with_status(429)
            .with_header(RETRY_AFTER.as_str(), &retry_after_http_date)
            .create_async()
            .await;

        let config = Arc::new(OtapSinkConfig {
            endpoint: format!("{}/v1/otap", server.url()),
            compression: Compression::None,
            headers: vec![],
        });
        let sink = OtapSink::new(
            "test".to_string(),
            config,
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("client"),
            Arc::new(AtomicI64::new(0)),
            Arc::new(ComponentStats::new()),
        );

        let result = sink
            .do_send(vec![0x01], 10)
            .await
            .expect("do_send should classify 429");

        match result {
            SendResult::RetryAfter(duration) => {
                assert!(
                    (58..=60).contains(&duration.as_secs()),
                    "HTTP-date Retry-After should parse to ~60s, got {}s",
                    duration.as_secs()
                );
            }
            other => panic!("expected RetryAfter, got {other:?}"),
        }
    }
}
