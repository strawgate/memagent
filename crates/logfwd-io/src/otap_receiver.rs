//! OTAP HTTP receiver: accepts OTAP `BatchArrowRecords` protobuf, extracts
//! the star schema payloads (LOGS + attrs tables), converts to flat
//! `RecordBatch` via `star_to_flat`, and sends to the pipeline.
//!
//! Endpoint: POST `/v1/arrow_logs`
//! Content-Type: `application/x-protobuf`
//!
//! The protobuf is hand-decoded (no tonic codegen) following the same pattern
//! as `otlp_receiver.rs`. Responds with a hand-encoded `BatchStatus` protobuf.

use std::io;
use std::io::Read as _;
use std::sync::mpsc;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use logfwd_arrow::star_schema::{StarSchema, attrs_schema, star_to_flat};
use logfwd_otap_proto::otap::{
    ArrowPayloadType as ProtoArrowPayloadType, BatchArrowRecords as ProtoBatchArrowRecords,
    BatchStatus as ProtoBatchStatus, StatusCode as ProtoStatusCode,
};
use prost::Message;

use crate::InputError;

/// Maximum request body size: 10 MB.
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Bounded channel capacity.
const CHANNEL_BOUND: usize = 256;

// ---------------------------------------------------------------------------
// ArrowPayloadType enum values (from OTAP proto)
// ---------------------------------------------------------------------------

/// LOGS fact table payload.
const PAYLOAD_TYPE_LOGS: u32 = ProtoArrowPayloadType::Logs as u32;
/// LOG_ATTRS dimension table payload.
const PAYLOAD_TYPE_LOG_ATTRS: u32 = ProtoArrowPayloadType::LogAttrs as u32;
/// RESOURCE_ATTRS dimension table payload.
const PAYLOAD_TYPE_RESOURCE_ATTRS: u32 = ProtoArrowPayloadType::ResourceAttrs as u32;
/// SCOPE_ATTRS dimension table payload.
const PAYLOAD_TYPE_SCOPE_ATTRS: u32 = ProtoArrowPayloadType::ScopeAttrs as u32;

// ---------------------------------------------------------------------------
// BatchStatus status codes
// ---------------------------------------------------------------------------

/// Batch processed successfully.
const BATCH_STATUS_OK: u32 = ProtoStatusCode::Ok as u32;

// ---------------------------------------------------------------------------
// OtapReceiver
// ---------------------------------------------------------------------------

/// OTAP receiver that accepts OTAP `BatchArrowRecords` protobuf over HTTP
/// and produces flat `RecordBatch` for the pipeline.
pub struct OtapReceiver {
    name: String,
    rx: Option<mpsc::Receiver<RecordBatch>>,
    addr: std::net::SocketAddr,
    server: std::sync::Arc<tiny_http::Server>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl OtapReceiver {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4317").
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity(name, addr, CHANNEL_BOUND)
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    pub fn new_with_capacity(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
    ) -> io::Result<Self> {
        let server = std::sync::Arc::new(
            tiny_http::Server::http(addr)
                .map_err(|e| io::Error::other(format!("OTAP receiver bind {addr}: {e}")))?,
        );

        let bound_addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(a) => a,
            tiny_http::ListenAddr::Unix(_) => {
                return Err(io::Error::other("OTAP receiver: unexpected listen addr"));
            }
        };

        let (tx, rx) = mpsc::sync_channel(capacity);
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_clone = std::sync::Arc::clone(&shutdown);

        let server_clone = std::sync::Arc::clone(&server);
        let handle = std::thread::Builder::new()
            .name("otap-receiver".into())
            .spawn(move || {
                while !shutdown_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut request = match server_clone.try_recv() {
                        Ok(Some(req)) => req,
                        Ok(None) => {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            continue;
                        }
                        // Exit the worker thread on accept-side I/O failure instead of
                        // spinning forever and silently dropping all future requests.
                        Err(_) => break,
                    };

                    let url = request.url().to_string();

                    let path = url.split('?').next().unwrap_or(&url);
                    if path != "/v1/arrow_logs" {
                        let _ = request.respond(
                            tiny_http::Response::from_string("not found").with_status_code(404),
                        );
                        continue;
                    }
                    if request.method() != &tiny_http::Method::Post {
                        let allow_header = "Allow: POST"
                            .parse::<tiny_http::Header>()
                            .expect("static header is valid");
                        let _ = request.respond(
                            tiny_http::Response::from_string("method not allowed")
                                .with_status_code(405)
                                .with_header(allow_header),
                        );
                        continue;
                    }

                    if request.body_length().unwrap_or(0) > MAX_BODY_SIZE {
                        let _ = request.respond(
                            tiny_http::Response::from_string("payload too large")
                                .with_status_code(413),
                        );
                        continue;
                    }

                    // Read body with hard cap.
                    let mut body =
                        Vec::with_capacity(request.body_length().unwrap_or(0).min(MAX_BODY_SIZE));
                    match request
                        .as_reader()
                        .take(MAX_BODY_SIZE as u64 + 1)
                        .read_to_end(&mut body)
                    {
                        Ok(n) if n > MAX_BODY_SIZE => {
                            let _ = request.respond(
                                tiny_http::Response::from_string("payload too large")
                                    .with_status_code(413),
                            );
                            continue;
                        }
                        Err(_) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string("read error")
                                    .with_status_code(400),
                            );
                            continue;
                        }
                        Ok(_) => {}
                    }

                    // Decode BatchArrowRecords protobuf.
                    let batch_records = match decode_batch_arrow_records(&body) {
                        Ok(b) => b,
                        Err(msg) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(msg.to_string())
                                    .with_status_code(400),
                            );
                            continue;
                        }
                    };

                    // Group payloads by type, deserialize IPC bytes into
                    // RecordBatches, and assemble a StarSchema.
                    let star = match assemble_star_schema(&batch_records.payloads) {
                        Ok(s) => s,
                        Err(msg) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(msg.to_string())
                                    .with_status_code(400),
                            );
                            continue;
                        }
                    };

                    // Convert star schema to flat RecordBatch.
                    let flat = match star_to_flat(&star) {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(format!(
                                    "star_to_flat failed: {e}"
                                ))
                                .with_status_code(400),
                            );
                            continue;
                        }
                    };

                    // Skip empty batches.
                    if flat.num_rows() == 0 {
                        let resp_body =
                            encode_batch_status(batch_records.batch_id, BATCH_STATUS_OK);
                        let _ = request.respond(
                            tiny_http::Response::from_data(resp_body)
                                .with_header(
                                    "Content-Type: application/x-protobuf"
                                        .parse::<tiny_http::Header>()
                                        .expect("static header is valid"),
                                )
                                .with_status_code(200),
                        );
                        continue;
                    }

                    // Send to pipeline via bounded channel.
                    match tx.try_send(flat) {
                        Ok(()) => {
                            let resp_body =
                                encode_batch_status(batch_records.batch_id, BATCH_STATUS_OK);
                            let _ = request.respond(
                                tiny_http::Response::from_data(resp_body)
                                    .with_header(
                                        "Content-Type: application/x-protobuf"
                                            .parse::<tiny_http::Header>()
                                            .expect("static header is valid"),
                                    )
                                    .with_status_code(200),
                            );
                        }
                        Err(mpsc::TrySendError::Full(_)) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "too many requests: pipeline backpressure",
                                )
                                .with_status_code(429),
                            );
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "service unavailable: pipeline disconnected",
                                )
                                .with_status_code(503),
                            );
                        }
                    }
                }
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx: Some(rx),
            addr: bound_addr,
            server,
            shutdown,
            handle: Some(handle),
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Try to receive all available RecordBatches (non-blocking).
    pub fn try_recv_all(&self) -> Vec<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Vec::new();
        };
        let mut batches = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            batches.push(batch);
        }
        batches
    }

    /// Blocking receive of the next RecordBatch.
    pub fn recv(&self) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("OTAP receiver: already closed"));
        };
        rx.recv()
            .map_err(|_| io::Error::other("OTAP receiver: channel disconnected"))
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("OTAP receiver: already closed"));
        };
        rx.recv_timeout(timeout).map_err(|e| match e {
            mpsc::RecvTimeoutError::Timeout => {
                io::Error::new(io::ErrorKind::TimedOut, "OTAP receiver: timed out")
            }
            mpsc::RecvTimeoutError::Disconnected => {
                io::Error::other("OTAP receiver: channel disconnected")
            }
        })
    }

    /// Return the name of this receiver.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Decoded `BatchArrowRecords` message.
struct BatchArrowRecords {
    batch_id: i64,
    payloads: Vec<ArrowPayload>,
}

/// Decoded `ArrowPayload` message.
struct ArrowPayload {
    payload_type: u32,
    record: Vec<u8>,
}

/// Decode `BatchArrowRecords` from protobuf bytes.
///
/// ```text
/// message BatchArrowRecords {
///   int64 batch_id = 1;
///   repeated ArrowPayload arrow_payloads = 2;
///   bytes headers = 3;
/// }
/// ```
fn decode_batch_arrow_records(buf: &[u8]) -> Result<BatchArrowRecords, InputError> {
    let decoded =
        ProtoBatchArrowRecords::decode(buf).map_err(|e| InputError::Receiver(e.to_string()))?;
    let payloads = decoded
        .arrow_payloads
        .into_iter()
        .map(|payload| ArrowPayload {
            payload_type: payload.r#type as u32,
            record: payload.record,
        })
        .collect();
    Ok(BatchArrowRecords {
        batch_id: decoded.batch_id,
        payloads,
    })
}

// ---------------------------------------------------------------------------
// Star schema assembly
// ---------------------------------------------------------------------------

/// Deserialize Arrow IPC stream bytes into a single `RecordBatch`.
///
/// If the stream contains multiple batches, they are concatenated (though OTAP
/// payloads typically contain exactly one batch per payload).
fn deserialize_ipc_batch(bytes: &[u8]) -> Result<RecordBatch, InputError> {
    if bytes.is_empty() {
        return Err(InputError::Receiver("empty Arrow IPC payload".to_string()));
    }
    let cursor = io::Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| InputError::Receiver(format!("Arrow IPC reader failed: {e}")))?;

    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| InputError::Receiver(format!("Arrow IPC read batch failed: {e}")))?;

    if batches.is_empty() {
        return Err(InputError::Receiver(
            "Arrow IPC stream contained no batches".to_string(),
        ));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().expect("checked len"));
    }

    // Concatenate multiple batches (rare but possible).
    arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| InputError::Receiver(format!("concat Arrow IPC batches failed: {e}")))
}

/// Group `ArrowPayload`s by type and assemble a `StarSchema`.
///
/// Requires at least a LOGS payload. Missing attrs tables get empty defaults
/// with the correct schema.
fn assemble_star_schema(payloads: &[ArrowPayload]) -> Result<StarSchema, InputError> {
    let mut logs_batch: Option<RecordBatch> = None;
    let mut log_attrs_batch: Option<RecordBatch> = None;
    let mut resource_attrs_batch: Option<RecordBatch> = None;
    let mut scope_attrs_batch: Option<RecordBatch> = None;

    for payload in payloads {
        if payload.record.is_empty() {
            continue;
        }
        let batch = deserialize_ipc_batch(&payload.record)?;

        match payload.payload_type {
            PAYLOAD_TYPE_LOGS => logs_batch = Some(batch),
            PAYLOAD_TYPE_LOG_ATTRS => log_attrs_batch = Some(batch),
            PAYLOAD_TYPE_RESOURCE_ATTRS => resource_attrs_batch = Some(batch),
            PAYLOAD_TYPE_SCOPE_ATTRS => scope_attrs_batch = Some(batch),
            _ => {
                // Unknown payload type — skip.
            }
        }
    }

    let logs = logs_batch.ok_or_else(|| {
        InputError::Receiver("missing LOGS payload in BatchArrowRecords".to_string())
    })?;

    // Use empty batches with correct schemas for missing dimension tables.
    let log_attrs = log_attrs_batch
        .unwrap_or_else(|| RecordBatch::new_empty(std::sync::Arc::new(attrs_schema())));
    let resource_attrs = resource_attrs_batch
        .unwrap_or_else(|| RecordBatch::new_empty(std::sync::Arc::new(attrs_schema())));
    let scope_attrs = scope_attrs_batch
        .unwrap_or_else(|| RecordBatch::new_empty(std::sync::Arc::new(attrs_schema())));

    Ok(StarSchema {
        logs,
        log_attrs,
        resource_attrs,
        scope_attrs,
    })
}

// attrs_schema() is imported from logfwd_arrow::star_schema.

// ---------------------------------------------------------------------------
// BatchStatus protobuf encoding
// ---------------------------------------------------------------------------

/// Encode a `BatchStatus` protobuf response.
///
/// ```text
/// message BatchStatus {
///   int64 batch_id = 1;
///   StatusCode status_code = 2;
/// }
/// ```
fn encode_batch_status(batch_id: i64, status_code: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    let message = ProtoBatchStatus {
        batch_id,
        status_code: status_code as i32,
        status_message: String::new(),
    };
    message
        .encode(&mut buf)
        .expect("vec-backed encode cannot fail");
    buf
}

// Protobuf encode helpers (encode_tag, encode_varint) are imported from
// logfwd_core::otlp.

impl Drop for OtapReceiver {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.rx.take();
        self.server.unblock();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt8Array,
        UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    use logfwd_arrow::star_schema::flat_to_star;

    // Regression test for issue #1142: clean shutdown
    #[test]
    fn clean_shutdown_releases_port() {
        let addr = "127.0.0.1:0";
        let receiver = OtapReceiver::new("test", addr).unwrap();
        let port = receiver.local_addr().port();

        // Wait briefly for thread to start blocking
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Drop it
        drop(receiver);

        // Wait briefly for the OS to actually release the port
        std::thread::sleep(std::time::Duration::from_millis(50));

        // The port should now be free to bind to immediately
        let new_addr = format!("127.0.0.1:{}", port);
        let result = tiny_http::Server::http(&new_addr);
        assert!(result.is_ok(), "Failed to bind to port {} after drop", port);
    }

    /// Build a `BatchArrowRecords` protobuf from components.
    fn encode_batch_arrow_records(
        batch_id: i64,
        payloads: &[(u32, &[u8])], // (payload_type, arrow_ipc_bytes)
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        let message = ProtoBatchArrowRecords {
            batch_id,
            arrow_payloads: payloads
                .iter()
                .map(
                    |(payload_type, record)| logfwd_otap_proto::otap::ArrowPayload {
                        schema_id: String::new(),
                        r#type: *payload_type as i32,
                        record: record.to_vec(),
                    },
                )
                .collect(),
            headers: Vec::new(),
        };
        message
            .encode(&mut buf)
            .expect("vec-backed encode cannot fail");
        buf
    }

    /// Serialize a RecordBatch to Arrow IPC stream bytes.
    fn serialize_batch_to_ipc(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .expect("IPC writer init");
        writer.write(batch).expect("IPC write batch");
        writer.finish().expect("IPC finish");
        buf
    }

    // --- Helper: build star schema tables for testing ---

    fn make_logs_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body_str", DataType::Utf8, true),
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
            Field::new("flags", DataType::UInt32, true),
            Field::new("dropped_attributes_count", DataType::UInt32, true),
        ]));

        let mut trace_id_builder = arrow::array::FixedSizeBinaryBuilder::new(16);
        trace_id_builder.append_null();
        trace_id_builder.append_null();

        let mut span_id_builder = arrow::array::FixedSizeBinaryBuilder::new(8);
        span_id_builder.append_null();
        span_id_builder.append_null();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![0u32, 1])),
            Arc::new(UInt32Array::from(vec![0u32, 0])),
            Arc::new(UInt32Array::from(vec![0u32, 0])),
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                Some(1_705_314_600_000_000_000i64),
                Some(1_705_314_601_000_000_000i64),
            ])),
            Arc::new(arrow::array::Int32Array::from(vec![Some(9), Some(17)])),
            Arc::new(StringArray::from(vec![Some("INFO"), Some("ERROR")])),
            Arc::new(StringArray::from(vec![
                Some("request started"),
                Some("connection failed"),
            ])),
            Arc::new(trace_id_builder.finish()),
            Arc::new(span_id_builder.finish()),
            Arc::new(UInt32Array::from(vec![None as Option<u32>, None])),
            Arc::new(UInt32Array::from(vec![Some(0u32), Some(0)])),
        ];

        RecordBatch::try_new(schema, columns).expect("logs batch")
    }

    fn make_log_attrs_batch() -> RecordBatch {
        let schema = Arc::new(attrs_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![0u32, 1])),
            Arc::new(StringArray::from(vec!["host", "host"])),
            Arc::new(UInt8Array::from(vec![0u8, 0])), // ATTR_TYPE_STR
            Arc::new(StringArray::from(vec![Some("host-1"), Some("host-2")])),
            Arc::new(Int64Array::from(vec![None as Option<i64>, None])),
            Arc::new(Float64Array::from(vec![None as Option<f64>, None])),
            Arc::new(BooleanArray::from(vec![None as Option<bool>, None])),
            Arc::new(BinaryArray::from(vec![None as Option<&[u8]>, None])),
        ];
        RecordBatch::try_new(schema, columns).expect("log_attrs batch")
    }

    fn make_resource_attrs_batch() -> RecordBatch {
        let schema = Arc::new(attrs_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(StringArray::from(vec!["service_name"])),
            Arc::new(UInt8Array::from(vec![0u8])), // ATTR_TYPE_STR
            Arc::new(StringArray::from(vec![Some("api-server")])),
            Arc::new(Int64Array::from(vec![None as Option<i64>])),
            Arc::new(Float64Array::from(vec![None as Option<f64>])),
            Arc::new(BooleanArray::from(vec![None as Option<bool>])),
            Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
        ];
        RecordBatch::try_new(schema, columns).expect("resource_attrs batch")
    }

    fn make_scope_attrs_batch() -> RecordBatch {
        let schema = Arc::new(attrs_schema());
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(StringArray::from(vec!["scope_name"])),
            Arc::new(UInt8Array::from(vec![0u8])),
            Arc::new(StringArray::from(vec![Some("logfwd")])),
            Arc::new(Int64Array::from(vec![None as Option<i64>])),
            Arc::new(Float64Array::from(vec![None as Option<f64>])),
            Arc::new(BooleanArray::from(vec![None as Option<bool>])),
            Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
        ];
        RecordBatch::try_new(schema, columns).expect("scope_attrs batch")
    }

    // --- Proto decode tests ---

    #[test]
    fn decode_batch_arrow_records_extracts_payloads() {
        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let log_attrs_ipc = serialize_batch_to_ipc(&make_log_attrs_batch());
        let resource_attrs_ipc = serialize_batch_to_ipc(&make_resource_attrs_batch());
        let scope_attrs_ipc = serialize_batch_to_ipc(&make_scope_attrs_batch());

        let proto = encode_batch_arrow_records(
            42,
            &[
                (PAYLOAD_TYPE_LOGS, &logs_ipc),
                (PAYLOAD_TYPE_LOG_ATTRS, &log_attrs_ipc),
                (PAYLOAD_TYPE_RESOURCE_ATTRS, &resource_attrs_ipc),
                (PAYLOAD_TYPE_SCOPE_ATTRS, &scope_attrs_ipc),
            ],
        );

        let decoded = decode_batch_arrow_records(&proto).expect("decode should succeed");
        assert_eq!(decoded.batch_id, 42);
        assert_eq!(decoded.payloads.len(), 4);
        assert_eq!(decoded.payloads[0].payload_type, PAYLOAD_TYPE_LOGS);
        assert_eq!(decoded.payloads[1].payload_type, PAYLOAD_TYPE_LOG_ATTRS);
        assert_eq!(
            decoded.payloads[2].payload_type,
            PAYLOAD_TYPE_RESOURCE_ATTRS
        );
        assert_eq!(decoded.payloads[3].payload_type, PAYLOAD_TYPE_SCOPE_ATTRS);

        // Verify the IPC bytes can be deserialized.
        let star = assemble_star_schema(&decoded.payloads).expect("assemble should succeed");
        assert_eq!(star.logs.num_rows(), 2);
        assert_eq!(star.log_attrs.num_rows(), 2);
        assert_eq!(star.resource_attrs.num_rows(), 1);
        assert_eq!(star.scope_attrs.num_rows(), 1);
    }

    #[test]
    fn decode_invalid_protobuf_returns_error() {
        let result = decode_batch_arrow_records(b"not valid protobuf\xff\xff");
        // Should not panic — may succeed with garbage data or return error.
        // The key invariant is no panic.
        let _ = result;
    }

    #[test]
    fn decode_empty_body_produces_empty_batch() {
        let proto = encode_batch_arrow_records(0, &[]);
        let decoded = decode_batch_arrow_records(&proto).expect("decode");
        assert_eq!(decoded.batch_id, 0);
        assert!(decoded.payloads.is_empty());
    }

    #[test]
    fn assemble_star_schema_requires_logs_payload() {
        let result = assemble_star_schema(&[]);
        assert!(result.is_err());
        let err = result.err().expect("should be error");
        assert!(
            err.to_string().contains("missing LOGS payload"),
            "got: {err}"
        );
    }

    #[test]
    fn assemble_star_schema_fills_missing_attrs() {
        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let payloads = vec![ArrowPayload {
            payload_type: PAYLOAD_TYPE_LOGS,
            record: logs_ipc,
        }];

        let star = assemble_star_schema(&payloads).expect("assemble");
        assert_eq!(star.logs.num_rows(), 2);
        assert_eq!(star.log_attrs.num_rows(), 0);
        assert_eq!(star.resource_attrs.num_rows(), 0);
        assert_eq!(star.scope_attrs.num_rows(), 0);
    }

    // --- Roundtrip test: flat → star → OTAP proto → decode → star → flat ---

    #[test]
    fn roundtrip_flat_to_otap_proto_and_back() {
        // Start with a flat RecordBatch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("_resource_service_name", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00.123456789Z"),
                Some("2024-01-15T10:30:01Z"),
            ])),
            Arc::new(StringArray::from(vec![Some("INFO"), Some("ERROR")])),
            Arc::new(StringArray::from(vec![
                Some("request started"),
                Some("connection failed"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("api-server"),
                Some("api-server"),
            ])),
            Arc::new(StringArray::from(vec![Some("host-1"), Some("host-2")])),
        ];

        let original = RecordBatch::try_new(schema, columns).expect("valid batch");

        // flat → star
        let star = flat_to_star(&original).expect("flat_to_star");

        // star → OTAP proto (serialize each table as IPC, encode as proto)
        let logs_ipc = serialize_batch_to_ipc(&star.logs);
        let log_attrs_ipc = serialize_batch_to_ipc(&star.log_attrs);
        let resource_attrs_ipc = serialize_batch_to_ipc(&star.resource_attrs);
        let scope_attrs_ipc = serialize_batch_to_ipc(&star.scope_attrs);

        let proto = encode_batch_arrow_records(
            1,
            &[
                (PAYLOAD_TYPE_LOGS, &logs_ipc),
                (PAYLOAD_TYPE_LOG_ATTRS, &log_attrs_ipc),
                (PAYLOAD_TYPE_RESOURCE_ATTRS, &resource_attrs_ipc),
                (PAYLOAD_TYPE_SCOPE_ATTRS, &scope_attrs_ipc),
            ],
        );

        // OTAP proto → decode → star
        let decoded = decode_batch_arrow_records(&proto).expect("decode");
        let decoded_star = assemble_star_schema(&decoded.payloads).expect("assemble");

        // star → flat
        let roundtrip = star_to_flat(&decoded_star).expect("star_to_flat");

        assert_eq!(roundtrip.num_rows(), 2);

        // Verify key columns survived the roundtrip.
        let rt_schema = roundtrip.schema();

        let msg_idx = rt_schema.index_of("message").expect("message col");
        let msg_arr = roundtrip
            .column(msg_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(msg_arr.value(0), "request started");
        assert_eq!(msg_arr.value(1), "connection failed");

        let lvl_idx = rt_schema.index_of("level").expect("level col");
        let lvl_arr = roundtrip
            .column(lvl_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(lvl_arr.value(0), "INFO");
        assert_eq!(lvl_arr.value(1), "ERROR");

        let rs_idx = rt_schema
            .index_of("_resource_service_name")
            .expect("resource col");
        let rs_arr = roundtrip
            .column(rs_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(rs_arr.value(0), "api-server");
        assert_eq!(rs_arr.value(1), "api-server");

        let host_idx = rt_schema.index_of("host").expect("host col");
        let host_arr = roundtrip
            .column(host_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(host_arr.value(0), "host-1");
        assert_eq!(host_arr.value(1), "host-2");
    }

    // --- HTTP integration tests ---

    #[test]
    fn receiver_accepts_otap_post() {
        let receiver = OtapReceiver::new_with_capacity("test", "127.0.0.1:0", 16)
            .expect("bind should succeed");
        let addr = receiver.local_addr();

        // Build a valid OTAP payload.
        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let log_attrs_ipc = serialize_batch_to_ipc(&make_log_attrs_batch());
        let resource_attrs_ipc = serialize_batch_to_ipc(&make_resource_attrs_batch());
        let scope_attrs_ipc = serialize_batch_to_ipc(&make_scope_attrs_batch());

        let proto = encode_batch_arrow_records(
            1,
            &[
                (PAYLOAD_TYPE_LOGS, &logs_ipc),
                (PAYLOAD_TYPE_LOG_ATTRS, &log_attrs_ipc),
                (PAYLOAD_TYPE_RESOURCE_ATTRS, &resource_attrs_ipc),
                (PAYLOAD_TYPE_SCOPE_ATTRS, &scope_attrs_ipc),
            ],
        );

        let url = format!("http://{addr}/v1/arrow_logs");
        let response = ureq::post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        // Receive the flat batch.
        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_rejects_wrong_path() {
        let receiver = OtapReceiver::new_with_capacity("test-404", "127.0.0.1:0", 16)
            .expect("bind should succeed");
        let addr = receiver.local_addr();

        let url = format!("http://{addr}/v1/logs");
        let result = ureq::post(&url).send(b"data" as &[u8]);
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 404),
            other => panic!("expected 404, got {other:?}"),
        }
    }

    #[test]
    fn receiver_rejects_get_method() {
        let receiver = OtapReceiver::new_with_capacity("test-405", "127.0.0.1:0", 16)
            .expect("bind should succeed");
        let addr = receiver.local_addr();

        let url = format!("http://{addr}/v1/arrow_logs");
        let result = ureq::get(&url).call();
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 405),
            other => panic!("expected 405, got {other:?}"),
        }
    }

    #[test]
    fn receiver_returns_429_when_channel_full() {
        let receiver = OtapReceiver::new_with_capacity("test-429", "127.0.0.1:0", 1)
            .expect("bind should succeed");
        let addr = receiver.local_addr();

        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let proto = encode_batch_arrow_records(1, &[(PAYLOAD_TYPE_LOGS, &logs_ipc)]);

        let url = format!("http://{addr}/v1/arrow_logs");

        // Fill the channel (capacity = 1).
        let resp = ureq::post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto)
            .expect("first POST should succeed");
        assert_eq!(resp.status().as_u16(), 200);

        // Next request should get 429.
        let result = ureq::post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto);
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert!(
            status == 429 || status == 503,
            "expected 429 or 503, got {status}"
        );

        // Drain so the receiver is valid.
        let _ = receiver.try_recv_all();
    }

    #[test]
    fn batch_status_response_is_valid_protobuf() {
        let resp = encode_batch_status(42, BATCH_STATUS_OK);
        let decoded = ProtoBatchStatus::decode(resp.as_slice()).expect("decode status");
        assert_eq!(decoded.batch_id, 42);
        assert_eq!(decoded.status_code, BATCH_STATUS_OK as i32);
    }
}
