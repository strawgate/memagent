//! OTAP HTTP receiver: accepts OTAP `BatchArrowRecords` protobuf, extracts
//! the star schema payloads (LOGS + attrs tables), converts to flat
//! `RecordBatch` via `star_to_flat`, and sends to the pipeline.
//!
//! Endpoint: POST `/v1/arrow_logs`
//! Content-Type: `application/x-protobuf`
//!
//! The protobuf is hand-decoded (no tonic codegen) following the same pattern
//! as `otlp_receiver.rs`. Responds with a hand-encoded `BatchStatus` protobuf.

#![allow(clippy::indexing_slicing)]

use std::io;
use std::sync::mpsc;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use ffwd_arrow::star_schema::{StarSchema, attrs_schema, star_to_flat};
use ffwd_otap_proto::otap::{
    ArrowPayloadType as ProtoArrowPayloadType, BatchArrowRecords as ProtoBatchArrowRecords,
    BatchStatus as ProtoBatchStatus, StatusCode as ProtoStatusCode,
};
use ffwd_types::diagnostics::{ComponentHealth, ComponentStats};
use prost::Message;
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::receiver_health::{ReceiverHealthEvent, reduce_receiver_health};
use crate::receiver_http::{
    MAX_REQUEST_BODY_SIZE, parse_content_length, parse_content_type, read_limited_body,
};

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
    background_task: BackgroundHttpTask,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
}

#[derive(Clone)]
struct OtapServerState {
    tx: mpsc::SyncSender<RecordBatch>,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    stats: Option<Arc<ComponentStats>>,
}

impl OtapReceiver {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4317").
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity_and_stats(name, addr, CHANNEL_BOUND, None)
    }

    /// Like [`Self::new`] but wires receiver diagnostics counters.
    pub fn new_with_stats(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_and_stats(name, addr, CHANNEL_BOUND, Some(stats))
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    pub fn new_with_capacity(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
    ) -> io::Result<Self> {
        Self::new_with_capacity_and_stats(name, addr, capacity, None)
    }

    /// Like [`Self::new_with_capacity`] but wires receiver diagnostics counters.
    pub fn new_with_capacity_and_stats(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        stats: Option<Arc<ComponentStats>>,
    ) -> io::Result<Self> {
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("OTAP receiver bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("OTAP receiver set_nonblocking {bound_addr}: {e}"))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let state = Arc::new(OtapServerState {
            tx,
            shutdown: Arc::clone(&shutdown),
            health: Arc::clone(&health),
            stats,
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_for_server = Arc::clone(&shutdown);
        let health_for_server = Arc::clone(&health);
        let state_for_server = Arc::clone(&state);

        let handle = std::thread::Builder::new()
            .name("otap-receiver".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                    return;
                };

                runtime.block_on(async move {
                    let Ok(listener) = tokio::net::TcpListener::from_std(std_listener) else {
                        store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                        return;
                    };

                    let app = axum::Router::new()
                        .route("/v1/arrow_logs", post(handle_otap_request))
                        .with_state(state_for_server);

                    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });

                    if server.await.is_err() && !shutdown_for_server.load(Ordering::Relaxed) {
                        store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                    }
                });
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx: Some(rx),
            addr: bound_addr,
            background_task: BackgroundHttpTask::new_axum(shutdown_tx, handle),
            shutdown,
            health,
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
            .map_err(|_e| io::Error::other("OTAP receiver: channel disconnected"))
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

    /// Coarse runtime health for readiness and diagnostics integration.
    pub fn health(&self) -> ComponentHealth {
        let stored = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        if self.background_task.is_finished() && !self.shutdown.load(Ordering::Relaxed) {
            ComponentHealth::Failed
        } else {
            stored
        }
    }
}

fn record_error(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_errors();
    }
}

fn record_parse_error(stats: Option<&Arc<ComponentStats>>) {
    if let Some(stats) = stats {
        stats.inc_parse_errors(1);
    }
}

fn store_health_event(health: &AtomicU8, event: ReceiverHealthEvent) {
    let mut current = health.load(Ordering::Relaxed);
    loop {
        let current_health = ComponentHealth::from_repr(current);
        let next = reduce_receiver_health(current_health, event).as_repr();
        match health.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

async fn handle_otap_request(
    State(state): State<Arc<OtapServerState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let content_encoding = match parse_content_encoding(&headers) {
        Ok(content_encoding) => content_encoding,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-encoding header").into_response();
        }
    };

    match parse_content_type(&headers) {
        Ok(Some(content_type)) => {
            if content_type != "application/x-protobuf" {
                record_error(state.stats.as_ref());
                return (
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported content-type",
                )
                    .into_response();
            }
        }
        Ok(None) => {
            record_error(state.stats.as_ref());
            return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "missing content-type").into_response();
        }
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-type header").into_response();
        }
    }

    let content_length = parse_content_length(&headers);
    if content_length.is_some_and(|body_len| body_len > MAX_REQUEST_BODY_SIZE as u64) {
        record_error(state.stats.as_ref());
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let body = match read_limited_body(body, MAX_REQUEST_BODY_SIZE, content_length).await {
        Ok(body) => body,
        Err(status) => {
            record_error(state.stats.as_ref());
            let message = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload too large"
            } else {
                "read error"
            };
            return (status, message).into_response();
        }
    };

    let body = match content_encoding.as_deref() {
        Some("gzip") => match decompress_gzip(&body, MAX_REQUEST_BODY_SIZE) {
            Ok(decompressed) => decompressed,
            Err(InputError::Io(_)) => {
                record_parse_error(state.stats.as_ref());
                return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
            }
            Err(_) => {
                record_parse_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, "gzip decompression failed").into_response();
            }
        },
        Some(other) => {
            record_error(state.stats.as_ref());
            return (
                StatusCode::BAD_REQUEST,
                format!("unsupported content-encoding: {other}"),
            )
                .into_response();
        }
        None => body,
    };

    let batch_records = match decode_batch_arrow_records(&body) {
        Ok(records) => records,
        Err(msg) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
    };

    let star = match assemble_star_schema(&batch_records.payloads) {
        Ok(star) => star,
        Err(msg) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
    };

    let flat = match star_to_flat(&star) {
        Ok(flat) => flat,
        Err(e) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, format!("star_to_flat failed: {e}")).into_response();
        }
    };

    if flat.num_rows() == 0 {
        store_health_event(&state.health, ReceiverHealthEvent::DeliveryNoop);
        let resp_body = encode_batch_status(batch_records.batch_id, BATCH_STATUS_OK);
        return (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/x-protobuf")],
            resp_body,
        )
            .into_response();
    }

    match state.tx.try_send(flat) {
        Ok(()) => {
            store_health_event(&state.health, ReceiverHealthEvent::DeliveryAccepted);
            let resp_body = encode_batch_status(batch_records.batch_id, BATCH_STATUS_OK);
            (
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-protobuf")],
                resp_body,
            )
                .into_response()
        }
        Err(mpsc::TrySendError::Full(_)) => {
            record_error(state.stats.as_ref());
            store_health_event(&state.health, ReceiverHealthEvent::Backpressure);
            (
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests: pipeline backpressure",
            )
                .into_response()
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            record_error(state.stats.as_ref());
            if !state.shutdown.load(Ordering::Relaxed) {
                store_health_event(&state.health, ReceiverHealthEvent::FatalFailure);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable: pipeline disconnected",
            )
                .into_response()
        }
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

#[cfg(fuzzing)]
pub fn fuzz_decode_batch_arrow_records(buf: &[u8]) -> Result<(i64, usize), InputError> {
    let decoded = decode_batch_arrow_records(buf)?;
    Ok((decoded.batch_id, decoded.payloads.len()))
}

fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_e| StatusCode::BAD_REQUEST)?;
    let encoding = parsed.trim();
    if encoding.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if encoding.eq_ignore_ascii_case("identity") {
        return Ok(None);
    }
    Ok(Some(encoding.to_ascii_lowercase()))
}

fn decompress_gzip(body: &[u8], max_request_body_size: usize) -> Result<Vec<u8>, InputError> {
    let decoder = flate2::read::GzDecoder::new(body);
    read_decompressed_body(
        decoder,
        body.len(),
        max_request_body_size,
        "gzip decompression failed",
    )
}

fn read_decompressed_body(
    mut reader: impl io::Read,
    compressed_len: usize,
    max_request_body_size: usize,
    error_label: &str,
) -> Result<Vec<u8>, InputError> {
    use std::io::Read;
    let mut decompressed = Vec::with_capacity(compressed_len.min(max_request_body_size));
    match reader
        .by_ref()
        .take(max_request_body_size as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > max_request_body_size => Err(InputError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "payload too large",
        ))),
        Ok(_) => Ok(decompressed),
        Err(_) => Err(InputError::Receiver(error_label.to_string())),
    }
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
    let log_attrs =
        log_attrs_batch.unwrap_or_else(|| RecordBatch::new_empty(Arc::new(attrs_schema())));
    let resource_attrs =
        resource_attrs_batch.unwrap_or_else(|| RecordBatch::new_empty(Arc::new(attrs_schema())));
    let scope_attrs =
        scope_attrs_batch.unwrap_or_else(|| RecordBatch::new_empty(Arc::new(attrs_schema())));

    Ok(StarSchema {
        logs,
        log_attrs,
        resource_attrs,
        scope_attrs,
    })
}

// attrs_schema() is imported from ffwd_arrow::star_schema.

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
// ffwd_core::otlp.

impl Drop for OtapReceiver {
    fn drop(&mut self) {
        let current = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        self.health.store(
            reduce_receiver_health(current, ReceiverHealthEvent::ShutdownRequested).as_repr(),
            Ordering::Relaxed,
        );
        self.shutdown.store(true, Ordering::Relaxed);
        self.rx.take();
        let current = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        self.health.store(
            reduce_receiver_health(current, ReceiverHealthEvent::ShutdownCompleted).as_repr(),
            Ordering::Relaxed,
        );
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
    use ffwd_types::diagnostics::ComponentStats;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Seek as _;
    use std::io::Write as _;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ffwd_arrow::star_schema::flat_to_star;

    fn loopback_http_client() -> ureq::Agent {
        ureq::Agent::config_builder()
            .proxy(None)
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .into()
    }

    fn wait_until<F>(timeout: Duration, mut predicate: F, failure_message: &str)
    where
        F: FnMut() -> bool,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(predicate(), "{failure_message}");
    }

    fn wait_for_server(addr: std::net::SocketAddr) {
        wait_until(
            Duration::from_secs(2),
            || std::net::TcpStream::connect(addr).is_ok(),
            "OTAP receiver did not accept connections",
        );
    }

    fn tempfile_payload(bytes: &[u8]) -> std::fs::File {
        let mut file = tempfile::tempfile().expect("create tempfile payload");
        file.write_all(bytes).expect("write tempfile payload");
        file.flush().expect("flush tempfile payload");
        file.rewind().expect("rewind tempfile payload");
        file
    }

    // Regression test for issue #1142: clean shutdown
    #[test]
    fn clean_shutdown_releases_port() {
        let addr = "127.0.0.1:0";
        let receiver = OtapReceiver::new("test", addr).unwrap();
        let port = receiver.local_addr().port();

        // Drop it
        drop(receiver);

        // The port should now be free to bind to immediately
        let new_addr = format!("127.0.0.1:{}", port);
        wait_until(
            Duration::from_secs(1),
            || tiny_http::Server::http(&new_addr).is_ok(),
            &format!("failed to bind to port {port} after drop"),
        );
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
                    |(payload_type, record)| ffwd_otap_proto::otap::ArrowPayload {
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
            Arc::new(StringArray::from(vec![Some("ffwd")])),
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
            Field::new("resource.attributes.service_name", DataType::Utf8, true),
            Field::new("resource.attributes.service.name", DataType::Utf8, true),
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
            Arc::new(StringArray::from(vec![Some("orders"), Some("orders")])),
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
            .index_of("resource.attributes.service_name")
            .expect("resource col");
        let rs_arr = roundtrip
            .column(rs_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(rs_arr.value(0), "api-server");
        assert_eq!(rs_arr.value(1), "api-server");

        let rs_dot_idx = rt_schema
            .index_of("resource.attributes.service.name")
            .expect("dotted resource col");
        let rs_dot_arr = roundtrip
            .column(rs_dot_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("str");
        assert_eq!(rs_dot_arr.value(0), "orders");
        assert_eq!(rs_dot_arr.value(1), "orders");

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
        assert_eq!(receiver.health(), ComponentHealth::Healthy);

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
        let response = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        // Receive the flat batch.
        let received = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn receiver_rejects_wrong_path() {
        let receiver = OtapReceiver::new_with_capacity("test-404", "127.0.0.1:0", 16)
            .expect("bind should succeed");
        let addr = receiver.local_addr();

        let url = format!("http://{addr}/v1/logs");
        let result = loopback_http_client().post(&url).send(b"data" as &[u8]);
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
        let result = loopback_http_client().get(&url).call();
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 405),
            other => panic!("expected 405, got {other:?}"),
        }
    }

    #[test]
    fn receiver_rejects_unsupported_content_type() {
        let stats = Arc::new(ComponentStats::new());
        let receiver = OtapReceiver::new_with_capacity_and_stats(
            "test-415",
            "127.0.0.1:0",
            16,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);

        let url = format!("http://{addr}/v1/arrow_logs");
        let payload = tempfile_payload(b"{}");
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/json")
            .send(payload);
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 415),
            other => panic!("expected 415, got {other:?}"),
        }
        assert_eq!(stats.errors(), 1);
    }

    #[test]
    fn receiver_rejects_truncated_gzip_body() {
        let receiver = OtapReceiver::new_with_capacity("test-gzip-truncated", "127.0.0.1:0", 16)
            .expect("bind should succeed");
        let addr = receiver.local_addr();

        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let proto = encode_batch_arrow_records(9, &[(PAYLOAD_TYPE_LOGS, &logs_ipc)]);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&proto)
            .expect("should write OTAP payload to gzip stream");
        let mut gzip = encoder.finish().expect("should finish gzip stream");
        gzip.truncate(gzip.len().saturating_sub(4));

        let url = format!("http://{addr}/v1/arrow_logs");
        let payload = tempfile_payload(&gzip);
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "gzip")
            .send(payload);

        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(err) => panic!("unexpected transport error: {err}"),
        };
        assert_eq!(status, 400, "truncated gzip body must be rejected");
    }

    #[test]
    fn receiver_returns_429_when_channel_full() {
        let stats = Arc::new(ComponentStats::new());
        let receiver = OtapReceiver::new_with_capacity_and_stats(
            "test-429",
            "127.0.0.1:0",
            1,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);

        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let proto = encode_batch_arrow_records(1, &[(PAYLOAD_TYPE_LOGS, &logs_ipc)]);

        let url = format!("http://{addr}/v1/arrow_logs");

        // Fill the channel (capacity = 1).
        let resp = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto)
            .expect("first POST should succeed");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(receiver.health(), ComponentHealth::Healthy);

        // Next request should get 429.
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto);
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 429, "expected 429, got {status}");
        assert_eq!(receiver.health(), ComponentHealth::Degraded);
        assert_eq!(stats.errors(), 1);

        // Drain so the receiver is valid.
        let _ = receiver.try_recv_all();

        let resp = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto)
            .expect("recovery POST should succeed");
        assert_eq!(resp.status().as_u16(), 200);
        let _ = receiver.recv_timeout(Duration::from_secs(2));
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn batch_status_response_is_valid_protobuf() {
        let resp = encode_batch_status(42, BATCH_STATUS_OK);
        let decoded = ProtoBatchStatus::decode(resp.as_slice()).expect("decode status");
        assert_eq!(decoded.batch_id, 42);
        assert_eq!(decoded.status_code, BATCH_STATUS_OK as i32);
    }

    #[test]
    fn invalid_otap_payload_increments_parse_errors_when_stats_hooked() {
        let stats = Arc::new(ComponentStats::new());
        let receiver = OtapReceiver::new_with_capacity_and_stats(
            "test-stats-parse",
            "127.0.0.1:0",
            16,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);

        let url = format!("http://{addr}/v1/arrow_logs");
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(b"invalid protobuf payload" as &[u8]);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
        assert_eq!(stats.parse_errors(), 1);
        assert_eq!(stats.errors(), 0);
    }

    #[test]
    fn disconnected_pipeline_increments_errors_when_stats_hooked() {
        let stats = Arc::new(ComponentStats::new());
        let mut receiver = OtapReceiver::new_with_capacity_and_stats(
            "test-stats-errors",
            "127.0.0.1:0",
            16,
            Some(Arc::clone(&stats)),
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        wait_for_server(addr);
        receiver.rx.take();

        let logs_ipc = serialize_batch_to_ipc(&make_logs_batch());
        let proto = encode_batch_arrow_records(1, &[(PAYLOAD_TYPE_LOGS, &logs_ipc)]);
        let url = format!("http://{addr}/v1/arrow_logs");
        let result = loopback_http_client()
            .post(&url)
            .header("Content-Type", "application/x-protobuf")
            .send(&proto);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 503);
        assert_eq!(stats.errors(), 1);
    }
}
