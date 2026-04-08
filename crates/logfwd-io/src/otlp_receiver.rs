//! OTLP HTTP receiver input source.
//!
//! Listens for OTLP ExportLogsServiceRequest via HTTP POST, decodes the
//! protobuf, and produces JSON lines that the scanner can process.
//!
//! Endpoint: POST /v1/logs (protobuf or JSON)
//!
//! This replaces the hand-rolled `--blackhole` with a proper pipeline input.

use std::io;
use std::io::Read as _;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};

use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use base64::Engine as _;
use bytes::Bytes;
use flate2::read::GzDecoder;
use http_body_util::BodyExt as _;
use logfwd_arrow::{Scanner, StreamingBuilder};
use logfwd_core::scan_config::ScanConfig;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message;
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::diagnostics::ComponentStats;
use crate::input::{InputEvent, InputSource};
use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::field_names;

/// Maximum request body size: 10 MB.
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Bounded channel capacity — limits memory when the pipeline falls behind.
const CHANNEL_BOUND: usize = 4096;

/// OTLP receiver that listens for log exports via HTTP.
pub struct OtlpReceiverInput {
    name: String,
    rx: Option<mpsc::Receiver<ReceiverPayload>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    background_task: BackgroundHttpTask,
    /// Shutdown mechanism for the background thread.
    is_running: Arc<AtomicBool>,
    /// Source-owned health snapshot for readiness and diagnostics.
    health: Arc<AtomicU8>,
}

#[derive(Clone, Copy)]
enum ReceiverMode {
    JsonLines,
    StructuredBatch,
}

enum ReceiverPayload {
    JsonLines {
        lines: Vec<u8>,
        /// Bytes charged to input diagnostics for this accepted request.
        /// This is the HTTP request-body size as received on the wire
        /// (possibly compressed), not the decoded/decompressed size.
        accounted_bytes: u64,
    },
    Batch {
        batch: RecordBatch,
        /// Bytes charged to input diagnostics for this accepted request.
        /// This is the HTTP request-body size as received on the wire
        /// (possibly compressed), not the decoded/decompressed size.
        accounted_bytes: u64,
    },
}

struct OtlpServerState {
    tx: mpsc::SyncSender<ReceiverPayload>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    mode: ReceiverMode,
    stats: Option<Arc<ComponentStats>>,
}

impl OtlpReceiverInput {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4318").
    /// Spawns a background thread to handle requests.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::JsonLines,
            None,
        )
    }

    /// Like [`Self::new`], but wires input diagnostics into receiver-side
    /// transport and decode failures.
    pub fn new_with_stats(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::JsonLines,
            Some(stats),
        )
    }

    /// Like [`Self::new`], but emits structured batches directly instead of
    /// JSON lines.
    pub fn new_structured(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::StructuredBatch,
            None,
        )
    }

    /// Like [`Self::new_structured`], but wires input diagnostics into
    /// receiver-side transport and decode failures.
    pub fn new_structured_with_stats(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            CHANNEL_BOUND,
            ReceiverMode::StructuredBatch,
            Some(stats),
        )
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    #[cfg(test)]
    fn new_with_capacity(name: impl Into<String>, addr: &str, capacity: usize) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(name, addr, capacity, ReceiverMode::JsonLines, None)
    }

    #[cfg(test)]
    fn new_with_capacity_and_stats(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        Self::new_with_capacity_mode_and_stats(
            name,
            addr,
            capacity,
            ReceiverMode::JsonLines,
            Some(stats),
        )
    }

    fn new_with_capacity_mode_and_stats(
        name: impl Into<String>,
        addr: &str,
        capacity: usize,
        mode: ReceiverMode,
        stats: Option<Arc<ComponentStats>>,
    ) -> io::Result<Self> {
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("OTLP receiver bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("OTLP receiver set_nonblocking {bound_addr}: {e}"))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);
        let is_running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let state = Arc::new(OtlpServerState {
            tx,
            is_running: Arc::clone(&is_running),
            health: Arc::clone(&health),
            mode,
            stats,
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let state_for_server = Arc::clone(&state);
        let is_running_for_server = Arc::clone(&is_running);
        let health_for_server = Arc::clone(&health);

        let handle = std::thread::Builder::new()
            .name("otlp-receiver".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        record_error(state_for_server.stats.as_ref());
                        health_for_server
                            .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                        return;
                    }
                };

                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(listener) => listener,
                        Err(_) => {
                            record_error(state_for_server.stats.as_ref());
                            health_for_server
                                .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                            return;
                        }
                    };

                    let app = axum::Router::new()
                        .route("/v1/logs", post(handle_otlp_request))
                        .with_state(state_for_server);

                    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });

                    if server.await.is_err() && is_running_for_server.load(Ordering::Relaxed) {
                        record_error(state.stats.as_ref());
                        health_for_server
                            .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
                    }
                });
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name: name.into(),
            rx: Some(rx),
            addr: bound_addr,
            background_task: BackgroundHttpTask::new_axum(shutdown_tx, handle),
            is_running,
            health,
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
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

async fn handle_otlp_request(
    State(state): State<Arc<OtlpServerState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if declared_content_length(&headers).is_some_and(|body_len| body_len > MAX_BODY_SIZE as u64) {
        record_error(state.stats.as_ref());
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let content_encoding = match parse_content_encoding(&headers) {
        Ok(content_encoding) => content_encoding,
        Err(status) => {
            record_error(state.stats.as_ref());
            return (status, "invalid content-encoding header").into_response();
        }
    };

    let mut body = match read_limited_body(body).await {
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

    let accounted_bytes = body.len() as u64;
    body = match content_encoding.as_deref() {
        Some("zstd") => match decompress_zstd(&body) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(InputError::Io(e)) if e.kind() == io::ErrorKind::InvalidData => {
                record_error(state.stats.as_ref());
                return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response();
            }
            Err(_) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, "zstd decompression failed").into_response();
            }
        },
        Some("gzip") => match decompress_gzip(&body) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(InputError::Io(e)) if e.kind() == io::ErrorKind::InvalidData => {
                record_error(state.stats.as_ref());
                return (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response();
            }
            Err(_) => {
                record_error(state.stats.as_ref());
                return (StatusCode::BAD_REQUEST, "gzip decompression failed").into_response();
            }
        },
        None | Some("identity") => body,
        Some(other) => {
            record_error(state.stats.as_ref());
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format!("unsupported content-encoding: {other}"),
            )
                .into_response();
        }
    };

    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/x-protobuf");
    let is_json = content_type
        .to_ascii_lowercase()
        .contains("application/json");

    let payload = if is_json {
        decode_otlp_logs_with_mode_json(&body, state.mode, accounted_bytes)
    } else {
        decode_otlp_logs_with_mode(&body, state.mode, accounted_bytes)
    };
    let payload = match payload {
        Ok(payload) => payload,
        Err(msg) => {
            record_parse_error(state.stats.as_ref());
            return (StatusCode::BAD_REQUEST, msg.to_string()).into_response();
        }
    };

    let send_result = if payload.is_empty() {
        Ok(())
    } else {
        state.tx.try_send(payload)
    };

    match send_result {
        Ok(()) => {
            state
                .health
                .store(ComponentHealth::Healthy.as_repr(), Ordering::Relaxed);
            (StatusCode::OK, [(CONTENT_TYPE, "application/json")], "{}").into_response()
        }
        Err(mpsc::TrySendError::Full(_)) => {
            state
                .health
                .store(ComponentHealth::Degraded.as_repr(), Ordering::Relaxed);
            (
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests: pipeline backpressure",
            )
                .into_response()
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            record_error(state.stats.as_ref());
            if state.is_running.load(Ordering::Relaxed) {
                state
                    .health
                    .store(ComponentHealth::Failed.as_repr(), Ordering::Relaxed);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable: pipeline disconnected",
            )
                .into_response()
        }
    }
}

fn declared_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Some(parsed.to_ascii_lowercase()))
}

async fn read_limited_body(body: Body) -> Result<Vec<u8>, StatusCode> {
    let mut body = body;
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| StatusCode::BAD_REQUEST)?;
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if out.len().saturating_add(chunk.len()) > MAX_BODY_SIZE {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

impl Drop for OtlpReceiverInput {
    fn drop(&mut self) {
        self.health
            .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);
        self.is_running.store(false, Ordering::Relaxed);
        self.rx.take();
        self.health
            .store(ComponentHealth::Stopped.as_repr(), Ordering::Relaxed);
    }
}

impl InputSource for OtlpReceiverInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let Some(rx) = self.rx.as_ref() else {
            return Ok(vec![]);
        };
        let mut all = Vec::new();
        let mut all_accounted_bytes = 0_u64;
        let mut batches = Vec::new();

        // Drain all available decoded payloads.
        while let Ok(data) = rx.try_recv() {
            match data {
                ReceiverPayload::JsonLines {
                    lines,
                    accounted_bytes,
                } => {
                    all.extend_from_slice(&lines);
                    all_accounted_bytes = all_accounted_bytes.saturating_add(accounted_bytes);
                }
                ReceiverPayload::Batch {
                    batch,
                    accounted_bytes,
                } => batches.push((batch, accounted_bytes)),
            }
        }

        let mut events = Vec::with_capacity((!all.is_empty()) as usize + batches.len());
        if !all.is_empty() {
            events.push(InputEvent::Data {
                bytes: all,
                source_id: None,
                accounted_bytes: all_accounted_bytes,
            });
        }
        events.extend(
            batches
                .into_iter()
                .map(|(batch, accounted_bytes)| InputEvent::Batch {
                    batch,
                    source_id: None,
                    accounted_bytes,
                }),
        );
        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        let stored = ComponentHealth::from_repr(self.health.load(Ordering::Relaxed));
        if self.background_task.is_finished() && self.is_running.load(Ordering::Relaxed) {
            ComponentHealth::Failed
        } else {
            stored
        }
    }
}

impl ReceiverPayload {
    fn is_empty(&self) -> bool {
        match self {
            ReceiverPayload::JsonLines { lines, .. } => lines.is_empty(),
            ReceiverPayload::Batch { batch, .. } => batch.num_rows() == 0,
        }
    }
}

fn decompress_zstd(body: &[u8]) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body)
        .map_err(|_| InputError::Receiver("zstd decompression failed".to_string()))?;
    read_decompressed_body(decoder, body.len(), "zstd decompression failed")
}

fn decompress_gzip(body: &[u8]) -> Result<Vec<u8>, InputError> {
    let decoder = GzDecoder::new(body);
    read_decompressed_body(decoder, body.len(), "gzip decompression failed")
}

fn read_decompressed_body(
    reader: impl io::Read,
    compressed_len: usize,
    error_label: &str,
) -> Result<Vec<u8>, InputError> {
    let mut decompressed = Vec::with_capacity(compressed_len.min(MAX_BODY_SIZE));
    match reader
        .take(MAX_BODY_SIZE as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > MAX_BODY_SIZE => Err(InputError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "payload too large",
        ))),
        Ok(_) => Ok(decompressed),
        Err(_) => Err(InputError::Receiver(error_label.to_string())),
    }
}

/// Decode an ExportLogsServiceRequest from JSON body and produce
/// newline-delimited JSON lines. Parses the OTLP JSON structure directly
/// since the protobuf types don't derive serde traits.
fn decode_otlp_logs_json(body: &[u8]) -> Result<Vec<u8>, InputError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let root: serde_json::Value = sonic_rs::from_slice(body)
        .map_err(|e| InputError::Receiver(format!("invalid JSON: {e}")))?;

    let resource_logs = match root.get("resourceLogs").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => {
            // An object that parses as valid JSON but lacks `resourceLogs` is
            // not a valid ExportLogsServiceRequest. Return 400 so the client
            // knows its payload was rejected rather than silently discarding it.
            return Err(InputError::Receiver(
                "missing required field 'resourceLogs' in OTLP JSON payload".to_string(),
            ));
        }
    };

    let mut out = Vec::new();

    for rl in resource_logs {
        // Collect resource attributes.
        let mut resource_attrs: Vec<(&str, String)> = Vec::new();
        if let Some(attrs) = rl
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|a| a.as_array())
        {
            for kv in attrs {
                let Some(key) = kv.get("key").and_then(|k| k.as_str()) else {
                    continue;
                };
                let Some(value) = kv.get("value") else {
                    continue;
                };
                if let Some(value) = json_any_value_to_string(value)? {
                    resource_attrs.push((key, value));
                }
            }
        }

        let scope_logs = match rl.get("scopeLogs").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => continue,
        };

        for sl in scope_logs {
            let records = match sl.get("logRecords").and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => continue,
            };

            for record in records {
                out.push(b'{');

                if let Some(ts) = record.get("timeUnixNano") {
                    let parsed = parse_protojson_u64(ts).ok_or_else(|| {
                        InputError::Receiver(
                            "invalid OTLP JSON timeUnixNano: not a valid uint64".into(),
                        )
                    })?;
                    if parsed > 0 {
                        write_json_key(&mut out, field_names::TIMESTAMP);
                        write_u64_to_buf(&mut out, parsed);
                        out.push(b',');
                    }
                }

                if let Some(sev) = record.get("severityText").and_then(|v| v.as_str()) {
                    if !sev.is_empty() {
                        write_json_string_field(&mut out, field_names::SEVERITY, sev);
                        out.push(b',');
                    }
                }

                if let Some(body_val) = record.get("body")
                    && let Some(body_str) = json_any_value_to_string(body_val)?
                {
                    write_json_string_field(&mut out, field_names::BODY, &body_str);
                    out.push(b',');
                }

                for (key, value) in &resource_attrs {
                    write_json_string_field(&mut out, key, value);
                    out.push(b',');
                }

                if let Some(attrs) = record.get("attributes").and_then(|v| v.as_array()) {
                    for kv in attrs {
                        if let (Some(key), Some(val)) =
                            (kv.get("key").and_then(|k| k.as_str()), kv.get("value"))
                            && write_json_any_value_field_from_json(&mut out, key, val)?
                        {
                            out.push(b',');
                        }
                    }
                }

                if let Some(tid) = record.get("traceId").and_then(|v| v.as_str()) {
                    if !tid.is_empty() {
                        write_json_string_field(&mut out, field_names::TRACE_ID, tid);
                        out.push(b',');
                    }
                }
                if let Some(sid) = record.get("spanId").and_then(|v| v.as_str()) {
                    if !sid.is_empty() {
                        write_json_string_field(&mut out, field_names::SPAN_ID, sid);
                        out.push(b',');
                    }
                }

                if out.last() == Some(&b',') {
                    out.pop();
                }
                out.extend_from_slice(b"}\n");
            }
        }
    }

    Ok(out)
}

fn decode_otlp_logs_with_mode_json(
    body: &[u8],
    mode: ReceiverMode,
    accounted_bytes: u64,
) -> Result<ReceiverPayload, InputError> {
    match mode {
        ReceiverMode::JsonLines => {
            decode_otlp_logs_json(body).map(|lines| ReceiverPayload::JsonLines {
                lines,
                accounted_bytes,
            })
        }
        ReceiverMode::StructuredBatch => {
            decode_otlp_logs_json_to_batch(body).map(|batch| ReceiverPayload::Batch {
                batch,
                accounted_bytes,
            })
        }
    }
}

/// Extract a scalar OTLP JSON AnyValue as an owned string.
/// Returns `Ok(None)` when the value is not a supported scalar string representation.
fn json_any_value_to_string(v: &serde_json::Value) -> Result<Option<String>, InputError> {
    if let Some(s) = v.get("stringValue").and_then(|v| v.as_str()) {
        return Ok(Some(s.to_string()));
    }
    if let Some(i) = v.get("intValue") {
        let parsed = parse_protojson_i64(i)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON intValue".into()))?;
        return Ok(Some(parsed.to_string()));
    }
    if let Some(dv) = v.get("doubleValue") {
        let parsed = parse_protojson_f64(dv)
            .ok_or_else(|| InputError::Receiver("invalid OTLP JSON doubleValue".into()))?;
        return Ok(Some(parsed.to_string()));
    }
    if let Some(b) = v.get("boolValue").and_then(serde_json::Value::as_bool) {
        return Ok(Some(if b { "true" } else { "false" }.to_string()));
    }
    if let Some(bytes) = v.get("bytesValue").and_then(|v| v.as_str()) {
        let decoded = decode_protojson_bytes(bytes)
            .map_err(|e| InputError::Receiver(format!("invalid OTLP JSON bytesValue: {e}")))?;
        return Ok(Some(hex::encode(&decoded)));
    }
    Ok(None)
}

/// Write an OTLP JSON AnyValue as a JSON field preserving primitive types.
fn write_json_any_value_field_from_json(
    out: &mut Vec<u8>,
    key: &str,
    value: &serde_json::Value,
) -> Result<bool, InputError> {
    if let Some(i) = value.get("intValue") {
        write_json_key(out, key);
        let parsed = parse_protojson_i64(i).ok_or_else(|| {
            InputError::Receiver(format!(
                "invalid OTLP JSON intValue for key {key}: not a valid integer"
            ))
        })?;
        write_i64_to_buf(out, parsed);
        return Ok(true);
    }

    if let Some(dv) = value.get("doubleValue") {
        let parsed = parse_protojson_f64(dv).ok_or_else(|| {
            InputError::Receiver(format!(
                "invalid OTLP JSON doubleValue for key {key}: not a valid float"
            ))
        })?;
        write_json_key(out, key);
        write_f64_to_buf(out, parsed);
        return Ok(true);
    }

    if let Some(b) = value.get("boolValue").and_then(serde_json::Value::as_bool) {
        write_json_key(out, key);
        if b {
            out.extend_from_slice(b"true");
        } else {
            out.extend_from_slice(b"false");
        }
        return Ok(true);
    }

    if let Some(s) = json_any_value_to_string(value)? {
        write_json_string_field(out, key, &s);
        return Ok(true);
    }

    Ok(false)
}

/// Decode an ExportLogsServiceRequest protobuf and produce newline-delimited
/// JSON. Each LogRecord becomes one JSON line with fields that the scanner
/// can extract into Arrow columns.
fn decode_otlp_logs(body: &[u8]) -> Result<Vec<u8>, InputError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let request = ExportLogsServiceRequest::decode(body)
        .map_err(|e| InputError::Receiver(format!("invalid protobuf: {e}")))?;

    Ok(convert_request_to_json_lines(&request))
}

fn decode_otlp_logs_with_mode(
    body: &[u8],
    mode: ReceiverMode,
    accounted_bytes: u64,
) -> Result<ReceiverPayload, InputError> {
    match mode {
        ReceiverMode::JsonLines => decode_otlp_logs(body).map(|lines| ReceiverPayload::JsonLines {
            lines,
            accounted_bytes,
        }),
        ReceiverMode::StructuredBatch => {
            decode_otlp_logs_to_batch(body).map(|batch| ReceiverPayload::Batch {
                batch,
                accounted_bytes,
            })
        }
    }
}

fn decode_otlp_logs_json_to_batch(body: &[u8]) -> Result<RecordBatch, InputError> {
    let json_lines = decode_otlp_logs_json(body)?;
    if json_lines.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }
    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan(Bytes::from(json_lines))
        .map_err(|e| InputError::Receiver(format!("structured OTLP JSON scan error: {e}")))
}

fn decode_otlp_logs_to_batch(body: &[u8]) -> Result<RecordBatch, InputError> {
    if body.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }

    let request = ExportLogsServiceRequest::decode(body)
        .map_err(|e| InputError::Receiver(format!("invalid protobuf: {e}")))?;

    convert_request_to_batch(&request)
}

/// Shared conversion: ExportLogsServiceRequest -> newline-delimited JSON bytes.
fn convert_request_to_json_lines(request: &ExportLogsServiceRequest) -> Vec<u8> {
    let mut out = Vec::new();

    for resource_logs in &request.resource_logs {
        // Extract resource attributes (e.g., service.name).
        let mut resource_attrs: Vec<(&str, String)> = Vec::new();
        if let Some(ref resource) = resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(ref value) = attr.value {
                    if let Some(value) = any_value_to_string(value) {
                        resource_attrs.push((&attr.key, value));
                    }
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                out.push(b'{');

                // timestamp (write directly without allocation)
                if record.time_unix_nano > 0 {
                    out.push(b'"');
                    out.extend_from_slice(field_names::TIMESTAMP.as_bytes());
                    out.extend_from_slice(b"\":");
                    write_u64_to_buf(&mut out, record.time_unix_nano);
                    out.push(b',');
                }

                // severity
                if !record.severity_text.is_empty() {
                    write_json_string_field(&mut out, field_names::SEVERITY, &record.severity_text);
                    out.push(b',');
                }

                // body
                if let Some(ref body_val) = record.body {
                    if let Some(body_str) = any_value_to_string(body_val) {
                        write_json_string_field(&mut out, field_names::BODY, &body_str);
                        out.push(b',');
                    }
                }

                // resource attributes
                for (key, value) in &resource_attrs {
                    write_json_string_field(&mut out, key, value);
                    out.push(b',');
                }

                // log record attributes
                for attr in &record.attributes {
                    if let Some(ref value) = attr.value {
                        if write_json_any_value(&mut out, &attr.key, value) {
                            out.push(b',');
                        }
                    }
                }

                // trace context (write hex directly to avoid allocation)
                if !record.trace_id.is_empty() {
                    out.push(b'"');
                    out.extend_from_slice(field_names::TRACE_ID.as_bytes());
                    out.extend_from_slice(b"\":\"");
                    write_hex_to_buf(&mut out, &record.trace_id);
                    out.extend_from_slice(b"\",");
                }
                if !record.span_id.is_empty() {
                    out.push(b'"');
                    out.extend_from_slice(field_names::SPAN_ID.as_bytes());
                    out.extend_from_slice(b"\":\"");
                    write_hex_to_buf(&mut out, &record.span_id);
                    out.extend_from_slice(b"\",");
                }

                // Remove trailing comma.
                if out.last() == Some(&b',') {
                    out.pop();
                }

                out.extend_from_slice(b"}\n");
            }
        }
    }

    out
}

fn convert_request_to_batch(request: &ExportLogsServiceRequest) -> Result<RecordBatch, InputError> {
    let mut builder = StreamingBuilder::new(false);
    builder.begin_batch(Bytes::new());

    let timestamp_idx = builder.resolve_field(field_names::TIMESTAMP.as_bytes());
    let severity_idx = builder.resolve_field(field_names::SEVERITY.as_bytes());
    let body_idx = builder.resolve_field(field_names::BODY.as_bytes());
    let trace_id_idx = builder.resolve_field(field_names::TRACE_ID.as_bytes());
    let span_id_idx = builder.resolve_field(field_names::SPAN_ID.as_bytes());
    let mut hex_buf = Vec::with_capacity(64);

    for resource_logs in &request.resource_logs {
        let mut resource_attrs = Vec::new();
        if let Some(ref resource) = resource_logs.resource {
            for attr in &resource.attributes {
                if let Some(ref value) = attr.value {
                    resource_attrs.push((&attr.key, value));
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                builder.begin_row();

                if record.time_unix_nano > 0
                    && let Ok(ts) = i64::try_from(record.time_unix_nano)
                {
                    builder.append_i64_value_by_idx(timestamp_idx, ts);
                }

                if !record.severity_text.is_empty() {
                    builder
                        .append_decoded_str_by_idx(severity_idx, record.severity_text.as_bytes());
                }

                if let Some(ref body_val) = record.body {
                    append_any_value_as_string(&mut builder, body_idx, body_val, &mut hex_buf);
                }

                for attr in &record.attributes {
                    if let Some(ref value) = attr.value {
                        append_attribute_value(&mut builder, &attr.key, value, &mut hex_buf);
                    }
                }

                for (key, value) in &resource_attrs {
                    append_attribute_value(&mut builder, key, value, &mut hex_buf);
                }

                if !record.trace_id.is_empty() {
                    append_hex_field(&mut builder, trace_id_idx, &record.trace_id, &mut hex_buf);
                }
                if !record.span_id.is_empty() {
                    append_hex_field(&mut builder, span_id_idx, &record.span_id, &mut hex_buf);
                }

                builder.end_row();
            }
        }
    }

    builder
        .finish_batch_detached()
        .map_err(|e| InputError::Receiver(format!("structured OTLP batch build error: {e}")))
}

fn append_attribute_value(
    builder: &mut StreamingBuilder,
    key: &str,
    value: &AnyValue,
    hex_buf: &mut Vec<u8>,
) {
    let idx = builder.resolve_field(key.as_bytes());
    match &value.value {
        Some(Value::IntValue(v)) => builder.append_i64_value_by_idx(idx, *v),
        Some(Value::DoubleValue(v)) => builder.append_f64_value_by_idx(idx, *v),
        Some(Value::BoolValue(v)) => builder.append_bool_by_idx(idx, *v),
        Some(Value::StringValue(v)) => builder.append_decoded_str_by_idx(idx, v.as_bytes()),
        Some(Value::BytesValue(v)) => append_hex_field(builder, idx, v, hex_buf),
        _ => {}
    }
}

fn append_any_value_as_string(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: &AnyValue,
    hex_buf: &mut Vec<u8>,
) {
    match &value.value {
        Some(Value::StringValue(v)) => builder.append_decoded_str_by_idx(idx, v.as_bytes()),
        Some(Value::IntValue(v)) => {
            let value = v.to_string();
            builder.append_decoded_str_by_idx(idx, value.as_bytes());
        }
        Some(Value::DoubleValue(v)) => {
            let value = v.to_string();
            builder.append_decoded_str_by_idx(idx, value.as_bytes());
        }
        Some(Value::BoolValue(v)) => {
            builder.append_decoded_str_by_idx(idx, if *v { b"true" } else { b"false" });
        }
        Some(Value::BytesValue(v)) => append_hex_field(builder, idx, v, hex_buf),
        _ => {}
    }
}

fn append_hex_field(
    builder: &mut StreamingBuilder,
    idx: usize,
    value: &[u8],
    hex_buf: &mut Vec<u8>,
) {
    hex_buf.clear();
    hex_buf.reserve(value.len() * 2);
    write_hex_to_buf(hex_buf, value);
    builder.append_decoded_str_by_idx(idx, hex_buf);
}

fn any_value_to_string(v: &AnyValue) -> Option<String> {
    match &v.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::BytesValue(b)) => Some(hex::encode(b)),
        _ => None,
    }
}

fn parse_protojson_i64(value: &serde_json::Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(n) = value.as_u64() {
        return i64::try_from(n).ok();
    }
    if let Some(n) = value.as_number() {
        return parse_protojson_i64_str(&n.to_string());
    }
    if let Some(s) = value.as_str() {
        return parse_protojson_i64_str(s);
    }
    None
}

fn parse_protojson_u64(value: &serde_json::Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    if let Some(s) = value.as_str() {
        return parse_protojson_u64_str(s);
    }
    if let Some(n) = value.as_number() {
        return parse_protojson_u64_str(&n.to_string());
    }
    None
}

fn parse_protojson_f64(value: &serde_json::Value) -> Option<f64> {
    if let Some(n) = value.as_f64() {
        return Some(n);
    }
    if let Some(s) = value.as_str() {
        return match s {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => s.parse::<f64>().ok(),
        };
    }
    None
}

fn parse_protojson_i64_str(s: &str) -> Option<i64> {
    let (negative, digits) = normalize_protojson_integral_digits(s)?;
    let magnitude = digits.parse::<u64>().ok()?;
    if negative {
        let signed = i128::from(magnitude).checked_neg()?;
        i64::try_from(signed).ok()
    } else {
        i64::try_from(magnitude).ok()
    }
}

fn parse_protojson_u64_str(s: &str) -> Option<u64> {
    let (negative, digits) = normalize_protojson_integral_digits(s)?;
    if negative {
        return None;
    }
    digits.parse::<u64>().ok()
}

fn normalize_protojson_integral_digits(s: &str) -> Option<(bool, String)> {
    const MAX_INTEGER_DECIMAL_DIGITS: usize = 20;

    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (negative, unsigned) = match s.as_bytes()[0] {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if unsigned.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(idx) => (&unsigned[..idx], unsigned[idx + 1..].parse::<i32>().ok()?),
        None => (unsigned, 0),
    };

    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    if digits.is_empty() {
        return None;
    }
    if digits.bytes().all(|b| b == b'0') {
        return Some((false, "0".to_string()));
    }

    let fractional_digits = i32::try_from(frac_part.len()).ok()?;
    let effective_exponent = exponent.checked_sub(fractional_digits)?;

    if effective_exponent >= 0 {
        let zeros = usize::try_from(effective_exponent).ok()?;
        let total_len = digits.len().checked_add(zeros)?;
        if total_len > MAX_INTEGER_DECIMAL_DIGITS {
            return None;
        }
        digits.extend(std::iter::repeat_n('0', zeros));
    } else {
        let trim = usize::try_from(effective_exponent.checked_neg()?).ok()?;
        if trim > digits.len() {
            return None;
        }
        if !digits[digits.len() - trim..].bytes().all(|b| b == b'0') {
            return None;
        }
        digits.truncate(digits.len() - trim);
    }

    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some((false, "0".to_string()));
    }

    Some((negative, digits.to_string()))
}

fn decode_protojson_bytes(value: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};

    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .or_else(|_| URL_SAFE.decode(value))
        .or_else(|_| URL_SAFE_NO_PAD.decode(value))
}

fn write_json_any_value(out: &mut Vec<u8>, key: &str, v: &AnyValue) -> bool {
    match &v.value {
        Some(Value::IntValue(i)) => {
            write_json_key(out, key);
            write_i64_to_buf(out, *i);
            true
        }
        Some(Value::DoubleValue(d)) => {
            write_json_key(out, key);
            write_f64_to_buf(out, *d);
            true
        }
        Some(Value::BoolValue(b)) => {
            write_json_key(out, key);
            out.extend_from_slice(if *b { b"true" } else { b"false" });
            true
        }
        Some(Value::StringValue(s)) => {
            write_json_string_field(out, key, s);
            true
        }
        Some(Value::BytesValue(bytes)) => {
            write_json_key(out, key);
            out.push(b'"');
            write_hex_to_buf(out, bytes);
            out.push(b'"');
            true
        }
        _ => false,
    }
}

/// Write i64 to buffer without allocation using itoa algorithm.
#[inline]
fn write_i64_to_buf(out: &mut Vec<u8>, mut n: i64) {
    if n == 0 {
        out.push(b'0');
        return;
    }

    if n < 0 {
        out.push(b'-');
        // Handle i64::MIN specially to avoid overflow
        if n == i64::MIN {
            out.extend_from_slice(b"9223372036854775808");
            return;
        }
        n = -n;
    }

    // Count digits
    let mut temp = n;
    let mut digits = 0;
    while temp > 0 {
        temp /= 10;
        digits += 1;
    }

    // Reserve space and write digits in reverse
    let start = out.len();
    out.resize(start + digits, 0);
    let mut pos = start + digits - 1;
    while n > 0 {
        out[pos] = b'0' + (n % 10) as u8;
        n /= 10;
        pos = pos.wrapping_sub(1);
    }
}

/// Write u64 to buffer without allocation using itoa algorithm.
#[inline]
fn write_u64_to_buf(out: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        out.push(b'0');
        return;
    }

    // Count digits
    let mut temp = n;
    let mut digits = 0;
    while temp > 0 {
        temp /= 10;
        digits += 1;
    }

    // Reserve space and write digits in reverse
    let start = out.len();
    out.resize(start + digits, 0);
    let mut pos = start + digits - 1;
    while n > 0 {
        out[pos] = b'0' + (n % 10) as u8;
        n /= 10;
        pos = pos.wrapping_sub(1);
    }
}

/// Write f64 to buffer without allocation using ryu algorithm.
#[inline]
fn write_f64_to_buf(out: &mut Vec<u8>, d: f64) {
    use std::io::Write;
    if !d.is_finite() {
        out.extend_from_slice(b"null");
        return;
    }
    // Use ryu for optimal float formatting (available in std)
    let _ = write!(out, "{}", d);
}

fn write_json_string_field(out: &mut Vec<u8>, key: &str, value: &str) {
    write_json_key(out, key);
    write_json_quoted_string(out, value);
}

const HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";

fn write_json_key(out: &mut Vec<u8>, key: &str) {
    write_json_quoted_string(out, key);
    out.push(b':');
}

fn write_json_quoted_string(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    write_json_escaped_string_contents(out, value);
    out.push(b'"');
}

fn write_json_escaped_string_contents(out: &mut Vec<u8>, value: &str) {
    // JSON escape per RFC 8259: all control chars (0x00-0x1f) must be escaped.
    for &b in value.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x00..=0x1f => {
                // Escape remaining control chars as \u00XX.
                out.extend_from_slice(b"\\u00");
                out.push(HEX_DIGITS[(b >> 4) as usize]);
                out.push(HEX_DIGITS[(b & 0x0f) as usize]);
            }
            _ => out.push(b),
        }
    }
}

/// Write hex-encoded bytes directly to output buffer (zero allocation).
fn write_hex_to_buf(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(HEX_TABLE[(b >> 4) as usize]);
        out.push(HEX_TABLE[(b & 0xf) as usize]);
    }
}

#[cfg(test)]
fn write_i64_to_buf_simple(out: &mut Vec<u8>, n: i64) {
    out.extend_from_slice(n.to_string().as_bytes());
}

#[cfg(test)]
fn write_u64_to_buf_simple(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(n.to_string().as_bytes());
}

#[cfg(test)]
fn write_f64_to_buf_simple(out: &mut Vec<u8>, d: f64) {
    if !d.is_finite() {
        out.extend_from_slice(b"null");
    } else {
        out.extend_from_slice(d.to_string().as_bytes());
    }
}

#[cfg(test)]
fn write_hex_to_buf_simple(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(hex::encode(bytes).as_bytes());
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_write_hex_to_buf_lower_hex_pairs() {
        let bytes: [u8; 2] = kani::any();
        let mut out = Vec::new();
        write_hex_to_buf(&mut out, &bytes);

        assert_eq!(out.len(), bytes.len() * 2);
        assert_eq!(
            out.as_slice(),
            &[
                HEX_DIGITS[(bytes[0] >> 4) as usize],
                HEX_DIGITS[(bytes[0] & 0x0f) as usize],
                HEX_DIGITS[(bytes[1] >> 4) as usize],
                HEX_DIGITS[(bytes[1] & 0x0f) as usize],
            ]
        );
        assert!(
            out.iter()
                .all(|&b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        kani::cover!(bytes[0] == 0, "hex encoding covers low nibble zeros");
        kani::cover!(bytes[1] == u8::MAX, "hex encoding covers ff");
    }
}

/// Minimal hex encoding (avoid adding the `hex` crate).
/// Only used in any_value_to_string for BytesValue (rare case).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_TABLE[(b >> 4) as usize] as char);
            s.push(HEX_TABLE[(b & 0xf) as usize] as char);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use logfwd_arrow::Scanner;
    use logfwd_core::scan_config::ScanConfig;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use proptest::prelude::*;
    use prost::Message;

    fn make_test_request() -> Vec<u8> {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: 1_705_314_600_000_000_000,
                            severity_text: "INFO".into(),
                            body: Some(AnyValue {
                                value: Some(Value::StringValue("hello world".into())),
                            }),
                            attributes: vec![
                                KeyValue {
                                    key: "service".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("myapp".into())),
                                    }),
                                },
                                KeyValue {
                                    key: "payload".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::BytesValue(vec![
                                            0x01, 0x02, 0x03, 0x04,
                                        ])),
                                    }),
                                },
                            ],
                            ..Default::default()
                        },
                        LogRecord {
                            severity_text: "ERROR".into(),
                            body: Some(AnyValue {
                                value: Some(Value::StringValue("something broke".into())),
                            }),
                            attributes: vec![KeyValue {
                                key: "status".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::IntValue(500)),
                                }),
                            }],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        request.encode_to_vec()
    }

    fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(any::<char>(), 0..=max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn non_empty_unicode_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(any::<char>(), 1..=max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn any_value_to_string_simple(v: &AnyValue) -> Option<String> {
        match &v.value {
            Some(Value::StringValue(s)) => Some(s.clone()),
            Some(Value::IntValue(i)) => Some(i.to_string()),
            Some(Value::DoubleValue(d)) => Some(d.to_string()),
            Some(Value::BoolValue(b)) => Some(b.to_string()),
            Some(Value::BytesValue(b)) => Some(hex::encode(b)),
            _ => None,
        }
    }

    fn any_value_to_json_simple(value: &AnyValue) -> Option<serde_json::Value> {
        match &value.value {
            Some(Value::IntValue(v)) => Some(serde_json::Value::from(*v)),
            Some(Value::DoubleValue(v)) => {
                let mut buf = Vec::new();
                write_f64_to_buf_simple(&mut buf, *v);
                serde_json::from_slice(&buf).ok()
            }
            Some(Value::BoolValue(v)) => Some(serde_json::Value::from(*v)),
            Some(Value::StringValue(v)) => Some(serde_json::Value::String(v.clone())),
            Some(Value::BytesValue(v)) => Some(serde_json::Value::String(hex::encode(v))),
            _ => None,
        }
    }

    fn convert_request_to_json_lines_simple(request: &ExportLogsServiceRequest) -> Vec<u8> {
        let mut out = Vec::new();

        for resource_logs in &request.resource_logs {
            let mut resource_attrs: Vec<(String, String)> = Vec::new();
            if let Some(resource) = &resource_logs.resource {
                for attr in &resource.attributes {
                    if let Some(value) = &attr.value
                        && let Some(stringified) = any_value_to_string_simple(value)
                    {
                        resource_attrs.push((attr.key.clone(), stringified));
                    }
                }
            }

            for scope_logs in &resource_logs.scope_logs {
                for record in &scope_logs.log_records {
                    let mut obj = serde_json::Map::new();

                    if record.time_unix_nano > 0 {
                        obj.insert(
                            field_names::TIMESTAMP.to_string(),
                            serde_json::Value::from(record.time_unix_nano),
                        );
                    }

                    if !record.severity_text.is_empty() {
                        obj.insert(
                            field_names::SEVERITY.to_string(),
                            serde_json::Value::String(record.severity_text.clone()),
                        );
                    }

                    if let Some(body) = &record.body
                        && let Some(body_str) = any_value_to_string_simple(body)
                    {
                        obj.insert(
                            field_names::BODY.to_string(),
                            serde_json::Value::String(body_str),
                        );
                    }

                    for (key, value) in &resource_attrs {
                        obj.insert(key.clone(), serde_json::Value::String(value.clone()));
                    }

                    for attr in &record.attributes {
                        if let Some(value) = &attr.value
                            && let Some(json_value) = any_value_to_json_simple(value)
                        {
                            obj.insert(attr.key.clone(), json_value);
                        }
                    }

                    if !record.trace_id.is_empty() {
                        obj.insert(
                            field_names::TRACE_ID.to_string(),
                            serde_json::Value::String(hex::encode(&record.trace_id)),
                        );
                    }
                    if !record.span_id.is_empty() {
                        obj.insert(
                            field_names::SPAN_ID.to_string(),
                            serde_json::Value::String(hex::encode(&record.span_id)),
                        );
                    }

                    serde_json::to_writer(&mut out, &serde_json::Value::Object(obj))
                        .expect("json serialization should succeed");
                    out.push(b'\n');
                }
            }
        }

        out
    }

    fn parse_json_lines_values(bytes: &[u8]) -> Vec<serde_json::Value> {
        if bytes.is_empty() {
            return Vec::new();
        }
        std::str::from_utf8(bytes)
            .expect("json lines must be valid UTF-8")
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid json line"))
            .collect()
    }

    fn any_value_strategy() -> impl Strategy<Value = AnyValue> {
        prop_oneof![
            unicode_string(20).prop_map(|s| AnyValue {
                value: Some(Value::StringValue(s)),
            }),
            any::<i64>().prop_map(|v| AnyValue {
                value: Some(Value::IntValue(v)),
            }),
            prop_oneof![
                (-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 100.0),
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
            ]
            .prop_map(|v| AnyValue {
                value: Some(Value::DoubleValue(v)),
            }),
            any::<bool>().prop_map(|b| AnyValue {
                value: Some(Value::BoolValue(b)),
            }),
            proptest::collection::vec(any::<u8>(), 0..16).prop_map(|b| AnyValue {
                value: Some(Value::BytesValue(b)),
            }),
            Just(AnyValue { value: None }),
        ]
    }

    fn key_value_strategy() -> impl Strategy<Value = KeyValue> {
        (
            non_empty_unicode_string(16),
            prop::option::of(any_value_strategy()),
        )
            .prop_map(|(key, value)| KeyValue { key, value })
    }

    fn log_record_strategy() -> impl Strategy<Value = LogRecord> {
        (
            any::<u64>(),
            unicode_string(8),
            prop::option::of(any_value_strategy()),
            proptest::collection::vec(key_value_strategy(), 0..6),
            proptest::collection::vec(any::<u8>(), 0..20),
            proptest::collection::vec(any::<u8>(), 0..12),
        )
            .prop_map(
                |(time_unix_nano, severity_text, body, attributes, trace_id, span_id)| LogRecord {
                    time_unix_nano,
                    severity_text,
                    body,
                    attributes,
                    trace_id,
                    span_id,
                    ..Default::default()
                },
            )
    }

    fn scope_logs_strategy() -> impl Strategy<Value = ScopeLogs> {
        proptest::collection::vec(log_record_strategy(), 0..6).prop_map(|log_records| ScopeLogs {
            log_records,
            ..Default::default()
        })
    }

    fn resource_logs_strategy() -> impl Strategy<Value = ResourceLogs> {
        (
            proptest::collection::vec(key_value_strategy(), 0..6),
            proptest::collection::vec(scope_logs_strategy(), 0..3),
        )
            .prop_map(|(resource_attributes, scope_logs)| ResourceLogs {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    ..Default::default()
                }),
                scope_logs,
                ..Default::default()
            })
    }

    fn request_strategy() -> impl Strategy<Value = ExportLogsServiceRequest> {
        proptest::collection::vec(resource_logs_strategy(), 0..4)
            .prop_map(|resource_logs| ExportLogsServiceRequest { resource_logs })
    }

    #[test]
    fn structured_batch_matches_legacy_scanned_batch() {
        let body = make_test_request();
        let json_lines = decode_otlp_logs(&body).expect("legacy decode succeeds");
        let structured = decode_otlp_logs_to_batch(&body).expect("structured decode succeeds");

        let mut scanner = Scanner::new(ScanConfig::default());
        let legacy = scanner
            .scan(Bytes::from(json_lines))
            .expect("legacy JSON lines scan");

        assert_eq!(legacy.num_columns(), structured.num_columns());
        for idx in 0..legacy.num_columns() {
            assert_eq!(
                legacy.schema().field(idx).name(),
                structured.schema().field(idx).name()
            );
            assert_eq!(
                column_values(legacy.column(idx).as_ref()),
                column_values(structured.column(idx).as_ref())
            );
        }
    }

    proptest! {
        #[test]
        fn proptest_convert_request_to_json_lines_fast_matches_simple(
            request in request_strategy()
        ) {
            let fast = convert_request_to_json_lines(&request);
            let simple = convert_request_to_json_lines_simple(&request);

            prop_assert_eq!(
                parse_json_lines_values(&fast),
                parse_json_lines_values(&simple),
                "convert_request_to_json_lines fast path drifted from simple reference"
            );
        }
    }

    fn column_values(array: &dyn Array) -> Vec<String> {
        if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
            return (0..array.len())
                .map(|idx| {
                    if array.is_null(idx) {
                        "NULL".to_string()
                    } else {
                        array.value(idx).to_string()
                    }
                })
                .collect();
        }
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            return (0..array.len())
                .map(|idx| {
                    if array.is_null(idx) {
                        "NULL".to_string()
                    } else {
                        array.value(idx).to_string()
                    }
                })
                .collect();
        }
        if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
            return (0..array.len())
                .map(|idx| {
                    if array.is_null(idx) {
                        "NULL".to_string()
                    } else {
                        array.value(idx).to_string()
                    }
                })
                .collect();
        }
        if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
            return (0..array.len())
                .map(|idx| {
                    if array.is_null(idx) {
                        "NULL".to_string()
                    } else {
                        array.value(idx).to_string()
                    }
                })
                .collect();
        }
        panic!("unsupported test array type: {:?}", array.data_type());
    }

    #[test]
    fn structured_batch_preserves_boolean_type_and_dotted_attributes() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("checkout-api".into())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        attributes: vec![KeyValue {
                            key: "sampled".into(),
                            value: Some(AnyValue {
                                value: Some(Value::BoolValue(true)),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = convert_request_to_batch(&request).expect("structured decode succeeds");

        let sampled = batch
            .column_by_name("sampled")
            .expect("sampled column must exist");
        assert_eq!(sampled.data_type(), &DataType::Boolean);
        let sampled = sampled
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("sampled must be BooleanArray");
        assert!(sampled.value(0), "sampled=true must be preserved as bool");

        let service_name = batch
            .column_by_name("service.name")
            .expect("dotted attribute must keep original column name");
        let service_name = service_name
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("service.name should remain a flat string column");
        assert_eq!(service_name.value(0), "checkout-api");
        assert!(
            batch.column_by_name("service_name").is_none(),
            "dotted attributes must not require sanitized internal-name coupling"
        );
    }

    #[test]
    fn structured_batch_preserves_resource_boolean_type() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "resource.sampled".into(),
                        value: Some(AnyValue {
                            value: Some(Value::BoolValue(true)),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = convert_request_to_batch(&request).expect("structured decode succeeds");
        let sampled = batch
            .column_by_name("resource.sampled")
            .expect("resource.sampled must exist");
        assert_eq!(sampled.data_type(), &DataType::Boolean);
        let sampled = sampled
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("resource.sampled must be BooleanArray");
        assert!(sampled.value(0), "resource bool attribute must stay typed");
    }

    #[test]
    fn decodes_otlp_to_json_lines() {
        let body = make_test_request();
        let json = decode_otlp_logs(&body).unwrap();
        let text = String::from_utf8(json).unwrap();
        let lines: Vec<&str> = text.lines().collect();

        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"level\":\"INFO\""), "got: {}", lines[0]);
        assert!(
            lines[0].contains("\"message\":\"hello world\""),
            "got: {}",
            lines[0]
        );
        assert!(
            lines[0].contains("\"service\":\"myapp\""),
            "got: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("\"level\":\"ERROR\""),
            "got: {}",
            lines[1]
        );
        assert!(lines[1].contains("\"status\":500"), "got: {}", lines[1]);
    }

    /// Contract test: protobuf and JSON OTLP inputs that represent the same
    /// records must produce the same semantic JSON-line output.
    #[test]
    fn protobuf_and_json_inputs_match_semantics() {
        let protobuf_lines = decode_otlp_logs(&make_test_request()).unwrap();

        let json_body = r#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [
                        {
                            "timeUnixNano": "1705314600000000000",
                            "severityText": "INFO",
                            "body": {"stringValue": "hello world"},
                            "attributes": [
                                {
                                    "key": "service",
                                    "value": {"stringValue": "myapp"}
                                },
                                {
                                    "key": "payload",
                                    "value": {"bytesValue": "AQIDBA=="}
                                }
                            ]
                        },
                        {
                            "severityText": "ERROR",
                            "body": {"stringValue": "something broke"},
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": "500"}
                            }]
                        }
                    ]
                }]
            }]
        }"#;
        let json_lines = decode_otlp_logs_json(json_body.as_bytes()).unwrap();

        let parse = |lines: &[u8]| -> Vec<serde_json::Value> {
            String::from_utf8(lines.to_vec())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect()
        };

        let left = parse(&protobuf_lines);
        let right = parse(&json_lines);
        assert_eq!(left.len(), 2, "expected 2 protobuf-decoded rows");
        assert_eq!(right.len(), 2, "expected 2 json-decoded rows");

        for (lhs, rhs) in left.iter().zip(right.iter()) {
            assert_eq!(lhs.get("level"), rhs.get("level"));
            assert_eq!(lhs.get("message"), rhs.get("message"));
            assert_eq!(lhs.get("service"), rhs.get("service"));
            assert_eq!(lhs.get("status"), rhs.get("status"));
            assert_eq!(lhs.get("payload"), rhs.get("payload"));
        }
    }

    #[test]
    fn record_attributes_override_resource_attributes_in_protobuf_paths() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("resource".into())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        attributes: vec![KeyValue {
                            key: "service.name".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("record".into())),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let json_lines = decode_otlp_logs(&request.encode_to_vec()).expect("protobuf decode");
        let rows = parse_json_lines_values(&json_lines);
        assert_eq!(rows.len(), 1, "expected one decoded row");
        assert_eq!(
            rows[0]
                .get("service.name")
                .and_then(serde_json::Value::as_str),
            Some("record"),
            "record attribute must override same-key resource attribute in JSON lines"
        );

        let batch = convert_request_to_batch(&request).expect("structured batch decode");
        let service_name = batch
            .column_by_name("service.name")
            .expect("service.name column should exist");
        let service_name = service_name
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("service.name must be a string column");
        assert_eq!(
            service_name.value(0),
            "record",
            "record attribute must override same-key resource attribute in structured batch"
        );
    }

    #[test]
    fn record_attributes_override_resource_attributes_in_json_input_path() {
        let json_body = serde_json::json!({
            "resourceLogs": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "resource"}
                    }]
                },
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [{
                            "key": "service.name",
                            "value": {"stringValue": "record"}
                        }]
                    }]
                }]
            }]
        });

        let json_lines =
            decode_otlp_logs_json(json_body.to_string().as_bytes()).expect("json decode");
        let rows = parse_json_lines_values(&json_lines);
        assert_eq!(rows.len(), 1, "expected one decoded row");
        assert_eq!(
            rows[0]
                .get("service.name")
                .and_then(serde_json::Value::as_str),
            Some("record"),
            "record attribute must override same-key resource attribute for OTLP JSON input"
        );
    }

    #[test]
    fn json_bytes_value_matches_protobuf_semantics() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::BytesValue(vec![0x01, 0x02, 0x03, 0x04])),
                        }),
                        attributes: vec![KeyValue {
                            key: "payload".into(),
                            value: Some(AnyValue {
                                value: Some(Value::BytesValue(vec![0x0a, 0x0b, 0x0c])),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let protobuf_lines = decode_otlp_logs(&request.encode_to_vec()).unwrap();
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "body": {"bytesValue": "AQIDBA=="},
                            "attributes": [{
                                "key": "payload",
                                "value": {"bytesValue": "CgsM"}
                            }]
                        }]
                    }]
                }]
            }"#,
        )
        .unwrap();

        let parse_first = |lines: &[u8]| -> serde_json::Value {
            let line = String::from_utf8(lines.to_vec())
                .unwrap()
                .lines()
                .next()
                .expect("one decoded row")
                .to_string();
            serde_json::from_str(&line).unwrap()
        };

        let left = parse_first(&protobuf_lines);
        let right = parse_first(&json_lines);

        assert_eq!(
            left.get("message").and_then(serde_json::Value::as_str),
            Some("01020304")
        );
        assert_eq!(left.get("message"), right.get("message"));
        assert_eq!(left.get("payload"), right.get("payload"));
    }

    #[test]
    fn invalid_json_bytes_value_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "payload",
                                "value": {"bytesValue": "***not-base64***"}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(result.is_err(), "invalid base64 bytesValue must fail");
    }

    #[test]
    fn json_bytes_value_accepts_urlsafe_and_unpadded_base64() {
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "payload",
                                "value": {"bytesValue": "-_8"}
                            }]
                        }]
                    }]
                }]
            }"#,
        )
        .expect("urlsafe unpadded base64 should decode");

        let line = String::from_utf8(json_lines).expect("utf8");
        let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
        assert_eq!(
            row.get("payload").and_then(serde_json::Value::as_str),
            Some("fbff")
        );
    }

    #[test]
    fn invalid_json_int_value_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": true}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(result.is_err(), "invalid intValue must fail");
    }

    #[test]
    fn invalid_json_double_value_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "latency_ratio",
                                "value": {"doubleValue": "not-a-double"}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(result.is_err(), "invalid doubleValue must fail");
    }

    #[test]
    fn non_numeric_json_int_string_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": "notanumber"}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(
            result.is_err(),
            "non-numeric intValue string must fail instead of emitting malformed JSON"
        );
    }

    #[test]
    fn exponent_form_json_int_value_is_accepted() {
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": "5e2"}
                            }]
                        }]
                    }]
                }]
            }"#,
        )
        .expect("ProtoJSON exponent intValue should decode");

        let line = String::from_utf8(json_lines).expect("utf8");
        let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
        assert_eq!(
            row.get("status").and_then(serde_json::Value::as_i64),
            Some(500)
        );
    }

    #[test]
    fn bare_exponent_json_int_value_is_accepted() {
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": 1e2}
                            }]
                        }]
                    }]
                }]
            }"#,
        )
        .expect("bare exponent intValue should decode");

        let line = String::from_utf8(json_lines).expect("utf8");
        let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
        assert_eq!(
            row.get("status").and_then(serde_json::Value::as_i64),
            Some(100)
        );
    }

    #[test]
    fn huge_positive_exponent_json_int_value_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": "1e2147483647"}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(result.is_err(), "huge exponent intValue must fail");
    }

    #[test]
    fn huge_negative_exponent_json_int_value_returns_error_without_panicking() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": "1e-2147483648"}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(
            result.is_err(),
            "extreme negative exponent intValue must fail without panicking"
        );
    }

    #[test]
    fn protojson_integral_normalization_accepts_integral_decimal_forms() {
        assert_eq!(
            normalize_protojson_integral_digits(" +001.2300e+2 "),
            Some((false, "123".to_string()))
        );
        assert_eq!(
            normalize_protojson_integral_digits("-9223372036854775808"),
            Some((true, "9223372036854775808".to_string()))
        );
        assert_eq!(
            normalize_protojson_integral_digits("0.000e+999999"),
            Some((false, "0".to_string()))
        );
    }

    #[test]
    fn protojson_integral_normalization_rejects_non_integral_or_oversized_forms() {
        assert_eq!(normalize_protojson_integral_digits("1.5"), None);
        assert_eq!(normalize_protojson_integral_digits("1e-1"), None);
        assert_eq!(normalize_protojson_integral_digits("1e20"), None);
        assert_eq!(normalize_protojson_integral_digits("1e2147483647"), None);
    }

    #[test]
    fn out_of_range_json_int_value_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{
                                "key": "status",
                                "value": {"intValue": "9223372036854775808"}
                            }]
                        }]
                    }]
                }]
            }"#,
        );

        assert!(result.is_err(), "out-of-range intValue must fail");
    }

    #[test]
    fn invalid_json_time_unix_nano_returns_error() {
        let result = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "timeUnixNano": "not-a-number"
                        }]
                    }]
                }]
            }"#,
        );

        assert!(result.is_err(), "invalid timeUnixNano must fail");
    }

    #[test]
    fn zero_json_time_unix_nano_is_accepted_and_omitted() {
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "timeUnixNano": "0",
                            "body": {"stringValue": "hello"}
                        }]
                    }]
                }]
            }"#,
        )
        .expect("zero timeUnixNano should be accepted");

        let line = String::from_utf8(json_lines).expect("utf8");
        let row: serde_json::Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
        assert_eq!(
            row.get(field_names::BODY)
                .and_then(serde_json::Value::as_str),
            Some("hello")
        );
        assert!(
            row.get(field_names::TIMESTAMP).is_none(),
            "unknown timestamp should be omitted, not emitted as 0"
        );
    }

    #[test]
    fn special_float_strings_match_protobuf_semantics() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::DoubleValue(f64::NAN)),
                        }),
                        attributes: vec![
                            KeyValue {
                                key: "nan_attr".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::DoubleValue(f64::NAN)),
                                }),
                            },
                            KeyValue {
                                key: "pos_inf".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::DoubleValue(f64::INFINITY)),
                                }),
                            },
                            KeyValue {
                                key: "neg_inf".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::DoubleValue(f64::NEG_INFINITY)),
                                }),
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let protobuf_lines = decode_otlp_logs(&request.encode_to_vec()).unwrap();
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "scopeLogs": [{
                        "logRecords": [{
                            "body": {"doubleValue": "NaN"},
                            "attributes": [
                                {"key": "nan_attr", "value": {"doubleValue": "NaN"}},
                                {"key": "pos_inf", "value": {"doubleValue": "Infinity"}},
                                {"key": "neg_inf", "value": {"doubleValue": "-Infinity"}}
                            ]
                        }]
                    }]
                }]
            }"#,
        )
        .expect("special float string tokens should decode");

        let parse_first = |lines: &[u8]| -> serde_json::Value {
            let line = String::from_utf8(lines.to_vec())
                .unwrap()
                .lines()
                .next()
                .expect("one decoded row")
                .to_string();
            serde_json::from_str(&line).unwrap()
        };

        let protobuf_row = parse_first(&protobuf_lines);
        let json_row = parse_first(&json_lines);
        assert_eq!(protobuf_row, json_row);
        assert_eq!(
            json_row.get("message").and_then(serde_json::Value::as_str),
            Some("NaN")
        );
        assert!(
            json_row
                .get("nan_attr")
                .is_some_and(serde_json::Value::is_null)
        );
        assert!(
            json_row
                .get("pos_inf")
                .is_some_and(serde_json::Value::is_null)
        );
        assert!(
            json_row
                .get("neg_inf")
                .is_some_and(serde_json::Value::is_null)
        );
    }

    #[test]
    fn empty_string_body_and_unsupported_values_preserve_wire_equivalence() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "empty_resource".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue(String::new())),
                            }),
                        },
                        KeyValue {
                            key: "unsupported_resource".into(),
                            value: Some(AnyValue {
                                value: Some(Value::ArrayValue(Default::default())),
                            }),
                        },
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::StringValue(String::new())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let protobuf_lines = decode_otlp_logs(&request.encode_to_vec()).unwrap();
        let json_lines = decode_otlp_logs_json(
            br#"{
                "resourceLogs": [{
                    "resource": {
                        "attributes": [
                            {"key": "empty_resource", "value": {"stringValue": ""}},
                            {"key": "unsupported_resource", "value": {"arrayValue": {"values": []}}}
                        ]
                    },
                    "scopeLogs": [{
                        "logRecords": [{
                            "body": {"stringValue": ""}
                        }]
                    }]
                }]
            }"#,
        )
        .unwrap();

        let parse_first = |lines: &[u8]| -> serde_json::Value {
            let line = String::from_utf8(lines.to_vec())
                .unwrap()
                .lines()
                .next()
                .expect("one decoded row")
                .to_string();
            serde_json::from_str(&line).unwrap()
        };

        let protobuf_row = parse_first(&protobuf_lines);
        let json_row = parse_first(&json_lines);
        assert_eq!(protobuf_row, json_row);
        assert_eq!(
            protobuf_row
                .get("empty_resource")
                .and_then(serde_json::Value::as_str),
            Some("")
        );
        assert!(protobuf_row.get("unsupported_resource").is_none());
        assert_eq!(
            protobuf_row
                .get("message")
                .and_then(serde_json::Value::as_str),
            Some("")
        );
    }

    #[test]
    fn handles_invalid_protobuf() {
        let result = decode_otlp_logs(b"not valid protobuf");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_protobuf_increments_parse_errors_when_stats_hooked() {
        let stats = Arc::new(ComponentStats::new());
        let receiver = OtlpReceiverInput::new_with_capacity_and_stats(
            "test",
            "127.0.0.1:0",
            16,
            Arc::clone(&stats),
        )
        .unwrap();
        let url = format!("http://{}/v1/logs", receiver.local_addr());

        let status = match ureq::post(&url)
            .header("content-type", "application/x-protobuf")
            .send(b"not valid protobuf".as_slice())
        {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected transport error: {e}"),
        };

        assert_eq!(status, 400);
        assert_eq!(stats.parse_errors(), 1);
        assert_eq!(stats.errors(), 0);
    }

    #[test]
    fn handles_empty_body() {
        let json = decode_otlp_logs(b"").unwrap();
        assert!(json.is_empty());
    }

    #[test]
    fn handles_request_with_no_log_records() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let body = request.encode_to_vec();
        let json = decode_otlp_logs(&body).unwrap();
        assert!(json.is_empty());
    }

    #[test]
    fn handles_record_with_no_body() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        severity_text: "WARN".into(),
                        body: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let body = request.encode_to_vec();
        let json = decode_otlp_logs(&body).unwrap();
        let text = String::from_utf8(json).unwrap();
        assert!(text.contains("\"level\":\"WARN\""));
        assert!(!text.contains("\"message\""));
    }

    #[test]
    fn hex_encode_empty() {
        assert_eq!(hex::encode(&[]), "");
    }

    #[test]
    fn write_hex_to_buf_uses_lowercase_pairs() {
        let mut out = Vec::new();
        write_hex_to_buf(&mut out, &[0x00, 0xab, 0xff]);
        assert_eq!(String::from_utf8(out).unwrap(), "00abff");
    }

    #[test]
    fn integer_writers_emit_canonical_decimal_strings() {
        let mut out = Vec::new();

        write_u64_to_buf(&mut out, 0);
        assert_eq!(String::from_utf8(out.clone()).unwrap(), "0");

        out.clear();
        write_u64_to_buf(&mut out, 42);
        assert_eq!(String::from_utf8(out.clone()).unwrap(), "42");

        out.clear();
        write_u64_to_buf(&mut out, u64::MAX);
        assert_eq!(
            String::from_utf8(out.clone()).unwrap(),
            u64::MAX.to_string()
        );

        out.clear();
        write_i64_to_buf(&mut out, -17);
        assert_eq!(String::from_utf8(out.clone()).unwrap(), "-17");

        out.clear();
        write_i64_to_buf(&mut out, i64::MIN);
        assert_eq!(String::from_utf8(out).unwrap(), i64::MIN.to_string());
    }

    /// Local microbenchmark for OTLP request -> NDJSON conversion.
    ///
    /// Run with:
    /// `cargo test -p logfwd-io otlp_receiver::tests::bench_convert_request_to_json_lines_fast_vs_simple --release -- --ignored --nocapture`
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_convert_request_to_json_lines_fast_vs_simple() {
        use std::hint::black_box;
        use std::time::Instant;

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("bench".into())),
                            }),
                        },
                        KeyValue {
                            key: "service.instance".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("instance-1".into())),
                            }),
                        },
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..2_000)
                        .map(|i| LogRecord {
                            time_unix_nano: 1_710_000_000_000_000_000 + i as u64,
                            severity_text: match i % 4 {
                                0 => "INFO".into(),
                                1 => "WARN".into(),
                                2 => "ERROR".into(),
                                _ => "DEBUG".into(),
                            },
                            body: Some(AnyValue {
                                value: Some(Value::StringValue(format!("message-{i}"))),
                            }),
                            attributes: vec![
                                KeyValue {
                                    key: "status".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::IntValue(200 + (i % 5) as i64)),
                                    }),
                                },
                                KeyValue {
                                    key: "latency".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::DoubleValue((i % 1000) as f64 / 10.0)),
                                    }),
                                },
                                KeyValue {
                                    key: "active".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::BoolValue(i % 2 == 0)),
                                    }),
                                },
                            ],
                            trace_id: if i % 3 == 0 {
                                Vec::new()
                            } else {
                                vec![
                                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
                                    0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
                                ]
                            },
                            span_id: if i % 5 == 0 {
                                Vec::new()
                            } else {
                                vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
                            },
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let fast_once = convert_request_to_json_lines(&request);
        let simple_once = convert_request_to_json_lines_simple(&request);
        assert_eq!(
            parse_json_lines_values(&fast_once),
            parse_json_lines_values(&simple_once),
            "fast and simple converters must remain semantically equivalent"
        );

        const ITERS: usize = 80;
        let t0 = Instant::now();
        for _ in 0..ITERS {
            let out = convert_request_to_json_lines(&request);
            black_box(out.len());
        }
        let fast = t0.elapsed();

        let t0 = Instant::now();
        for _ in 0..ITERS {
            let out = convert_request_to_json_lines_simple(&request);
            black_box(out.len());
        }
        let simple = t0.elapsed();

        eprintln!(
            "convert_request_to_json_lines bench records={} iters={ITERS}",
            request.resource_logs[0].scope_logs[0].log_records.len()
        );
        eprintln!("  fast={:?}", fast);
        eprintln!("  simple={:?}", simple);
    }

    proptest! {
        #[test]
        fn proptest_write_i64_fast_matches_simple(n in any::<i64>(), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
            let mut fast = prefix.clone();
            let mut simple = prefix;
            write_i64_to_buf(&mut fast, n);
            write_i64_to_buf_simple(&mut simple, n);
            prop_assert_eq!(fast, simple);
        }

        #[test]
        fn proptest_write_u64_fast_matches_simple(n in any::<u64>(), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
            let mut fast = prefix.clone();
            let mut simple = prefix;
            write_u64_to_buf(&mut fast, n);
            write_u64_to_buf_simple(&mut simple, n);
            prop_assert_eq!(fast, simple);
        }

        #[test]
        fn proptest_write_f64_fast_matches_simple(bits in any::<u64>(), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
            let d = f64::from_bits(bits);
            let mut fast = prefix.clone();
            let mut simple = prefix;
            write_f64_to_buf(&mut fast, d);
            write_f64_to_buf_simple(&mut simple, d);
            prop_assert_eq!(fast, simple);
        }

        #[test]
        fn proptest_write_hex_fast_matches_simple(bytes in proptest::collection::vec(any::<u8>(), 0..256), prefix in proptest::collection::vec(any::<u8>(), 0..32)) {
            let mut fast = prefix.clone();
            let mut simple = prefix;
            write_hex_to_buf(&mut fast, &bytes);
            write_hex_to_buf_simple(&mut simple, &bytes);
            prop_assert_eq!(fast, simple);
        }
    }

    /// Local microbenchmark for writer helpers.
    ///
    /// Run with:
    /// `cargo test -p logfwd-io otlp_receiver::tests::bench_writer_helpers_fast_vs_simple --release -- --ignored --nocapture`
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_writer_helpers_fast_vs_simple() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 200_000;
        let i64_inputs: Vec<i64> = (0..N)
            .map(|i| ((i as i64).wrapping_mul(1_048_573)).wrapping_sub(73_421))
            .collect();
        let u64_inputs: Vec<u64> = (0..N)
            .map(|i| (i as u64).wrapping_mul(2_654_435_761))
            .collect();
        let f64_inputs: Vec<f64> = (0..N)
            .map(|i| {
                let r = (i as f64) * 0.125 - 17_333.75;
                if i % 97 == 0 {
                    f64::NAN
                } else if i % 89 == 0 {
                    f64::INFINITY
                } else if i % 83 == 0 {
                    f64::NEG_INFINITY
                } else {
                    r
                }
            })
            .collect();
        let hex_inputs: Vec<Vec<u8>> = (0..N)
            .map(|i| {
                let x = (i as u64).wrapping_mul(11_400_714_819_323_198_485);
                x.to_le_bytes().to_vec()
            })
            .collect();

        let mut buf = Vec::with_capacity(256 * 1024);

        let t0 = Instant::now();
        for &n in &i64_inputs {
            buf.clear();
            write_i64_to_buf(&mut buf, n);
            black_box(buf.len());
        }
        let i64_fast = t0.elapsed();

        let t0 = Instant::now();
        for &n in &i64_inputs {
            buf.clear();
            write_i64_to_buf_simple(&mut buf, n);
            black_box(buf.len());
        }
        let i64_simple = t0.elapsed();

        let t0 = Instant::now();
        for &n in &u64_inputs {
            buf.clear();
            write_u64_to_buf(&mut buf, n);
            black_box(buf.len());
        }
        let u64_fast = t0.elapsed();

        let t0 = Instant::now();
        for &n in &u64_inputs {
            buf.clear();
            write_u64_to_buf_simple(&mut buf, n);
            black_box(buf.len());
        }
        let u64_simple = t0.elapsed();

        let t0 = Instant::now();
        for &d in &f64_inputs {
            buf.clear();
            write_f64_to_buf(&mut buf, d);
            black_box(buf.len());
        }
        let f64_fast = t0.elapsed();

        let t0 = Instant::now();
        for &d in &f64_inputs {
            buf.clear();
            write_f64_to_buf_simple(&mut buf, d);
            black_box(buf.len());
        }
        let f64_simple = t0.elapsed();

        let t0 = Instant::now();
        for v in &hex_inputs {
            buf.clear();
            write_hex_to_buf(&mut buf, v);
            black_box(buf.len());
        }
        let hex_fast = t0.elapsed();

        let t0 = Instant::now();
        for v in &hex_inputs {
            buf.clear();
            write_hex_to_buf_simple(&mut buf, v);
            black_box(buf.len());
        }
        let hex_simple = t0.elapsed();

        eprintln!("writer bench N={N}");
        eprintln!("  i64  fast={:?} simple={:?}", i64_fast, i64_simple);
        eprintln!("  u64  fast={:?} simple={:?}", u64_fast, u64_simple);
        eprintln!("  f64  fast={:?} simple={:?}", f64_fast, f64_simple);
        eprintln!("  hex  fast={:?} simple={:?}", hex_fast, hex_simple);
    }

    #[test]
    fn json_escaping_control_chars() {
        // Build a string with all control chars that are valid single-byte UTF-8 (0x00-0x1f all are).
        let ctrl: String = (0u8..=0x1f).map(|b| b as char).collect();
        let mut out = Vec::new();
        write_json_string_field(&mut out, "k", &ctrl);
        let text = String::from_utf8(out).unwrap();

        // No raw control bytes should appear in the output.
        for b in text.as_bytes() {
            // The only bytes < 0x20 allowed are the literal `"` delimiters… but `"` is 0x22.
            // So nothing < 0x20 should appear at all.
            assert!(
                *b >= 0x20,
                "raw control byte 0x{:02x} found in output: {text}",
                b
            );
        }

        // Spot-check specific escapes.
        assert!(text.contains(r"\u0000"), "NUL not escaped: {text}");
        assert!(text.contains(r"\u0001"), "SOH not escaped: {text}");
        assert!(text.contains(r"\u0008"), "BS not escaped: {text}");
        assert!(text.contains(r"\t"), "TAB not escaped: {text}");
        assert!(text.contains(r"\n"), "LF not escaped: {text}");
        assert!(text.contains(r"\r"), "CR not escaped: {text}");
        assert!(text.contains(r"\u000c"), "FF not escaped: {text}");
    }

    #[test]
    fn json_escaping_unicode() {
        // Multi-byte UTF-8 should pass through unchanged.
        let input = "hello \u{00e9}\u{1f600} world \u{4e16}\u{754c}";
        let mut out = Vec::new();
        write_json_string_field(&mut out, "k", input);
        let text = String::from_utf8(out).unwrap();

        // The multi-byte chars should appear literally (not \u-escaped).
        assert!(text.contains('\u{00e9}'), "e-acute missing: {text}");
        assert!(text.contains('\u{1f600}'), "emoji missing: {text}");
        assert!(text.contains('\u{4e16}'), "CJK char missing: {text}");

        // Verify the whole thing is valid JSON.
        let json_str = format!("{{{text}}}");
        serde_json::from_str::<serde_json::Value>(&json_str)
            .unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
    }

    #[test]
    fn json_escaping_key_chars() {
        let mut out = Vec::new();
        write_json_string_field(&mut out, "my\"key\\path", "value");
        let text = String::from_utf8(out).unwrap();
        let json_str = format!("{{{text}}}");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json_str}"));
        assert_eq!(parsed["my\"key\\path"], "value");
    }

    /// Regression test: when the pipeline channel is full the receiver must
    /// return 429 rather than silently dropping the payload and returning 200.
    #[test]
    fn returns_429_when_channel_full_not_200() {
        // Use a tiny channel so it fills up after 2 sends.
        let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 2).unwrap();
        let addr = receiver.local_addr();
        let url = format!("http://{addr}/v1/logs");

        let body = serde_json::json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{"body": {"stringValue": "x"}}]
                }]
            }]
        })
        .to_string();

        // Fill the channel (capacity = 2 so two sends succeed).
        for i in 0..2 {
            let resp = ureq::post(&url)
                .header("content-type", "application/json")
                .send(body.as_bytes())
                .unwrap_or_else(|e| panic!("request {i} failed: {e}"));
            assert_eq!(
                resp.status(),
                200,
                "expected 200 while channel has capacity (request {i})"
            );
        }

        // The channel is now full; the next request must not return 200.
        let result = ureq::post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes());

        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_ne!(
            status, 200,
            "channel-full request must not return 200 (got {status})"
        );
        assert!(
            status == 429 || status == 503,
            "expected 429 or 503 for backpressure, got {status}"
        );
        assert_eq!(receiver.health(), ComponentHealth::Degraded);

        // Drain the two buffered entries so the receiver is valid.
        let _ = receiver.poll().unwrap();

        let resp = ureq::post(&url)
            .header("content-type", "application/json")
            .send(body.as_bytes())
            .expect("request after drain failed");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    // Bug #686: /v1/logsFOO and /v1/logs/extra should return 404, not 200.
    #[test]
    fn path_prefix_variants_return_404() {
        let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
        let port = receiver.local_addr().port();

        for bad_path in &["/v1/logsFOO", "/v1/logs/extra", "/v1/logs2", "/v1/log"] {
            let url = format!("http://127.0.0.1:{port}{bad_path}");
            let status = match ureq::get(&url).call() {
                Ok(r) => r.status().as_u16(),
                Err(ureq::Error::StatusCode(c)) => c,
                Err(e) => panic!("unexpected error for {bad_path}: {e}"),
            };
            assert_eq!(status, 404, "{bad_path} should return 404, got {status}");
        }
    }

    // Bug #687: Content-Type: Application/JSON (capital A) should be treated as JSON.
    #[test]
    fn content_type_matching_is_case_insensitive() {
        let mut receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
        let port = receiver.local_addr().port();
        let url = format!("http://127.0.0.1:{port}/v1/logs");

        let body = serde_json::json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{"body": {"stringValue": "hello"}}]
                }]
            }]
        })
        .to_string();

        let resp = ureq::post(&url)
            .header("content-type", "Application/JSON")
            .send(body.as_bytes())
            .expect("request failed");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "Application/JSON should be decoded as JSON and return 200"
        );

        std::thread::sleep(std::time::Duration::from_millis(50));
        let data = receiver.poll().unwrap();
        assert!(
            !data.is_empty(),
            "expected data from JSON body with mixed-case Content-Type"
        );
    }

    // Bug #723: wrong HTTP method should return 405, not 404.
    #[test]
    fn wrong_http_method_returns_405() {
        let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
        let port = receiver.local_addr().port();
        let url = format!("http://127.0.0.1:{port}/v1/logs");

        for (method, result) in [
            ("GET", ureq::get(&url).call()),
            ("DELETE", ureq::delete(&url).call()),
        ] {
            let status: u16 = match result {
                Ok(resp) => resp.status().as_u16(),
                Err(ureq::Error::StatusCode(code)) => code,
                Err(e) => panic!("unexpected error for {method}: {e}"),
            };
            assert_eq!(
                status, 405,
                "{method} /v1/logs should return 405 Method Not Allowed, got {status}"
            );
        }
    }

    // Bug #722: JSON body missing resourceLogs should return 400, not 200.
    #[test]
    fn missing_resource_logs_returns_400() {
        let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
        let port = receiver.local_addr().port();
        let url = format!("http://127.0.0.1:{port}/v1/logs");

        let bad_bodies = [r"{}", r#"{"foo":"bar"}"#, r#"{"resourceLogs":null}"#];
        for body in &bad_bodies {
            let result = ureq::post(&url)
                .header("content-type", "application/json")
                .send(body.as_bytes());
            let status: u16 = match result {
                Ok(resp) => resp.status().as_u16(),
                Err(ureq::Error::StatusCode(code)) => code,
                Err(e) => panic!("unexpected error for body {body}: {e}"),
            };
            assert_eq!(status, 400, "body {body:?} should return 400, got {status}");
        }
    }

    #[test]
    fn receiver_shuts_down_cleanly_on_drop() {
        // Create an input receiver binding to port 0 (OS assigns port).
        let receiver =
            OtlpReceiverInput::new("test-drop", "127.0.0.1:0").expect("should bind successfully");
        let local_addr = receiver.local_addr();

        // Drop the receiver. This should trigger `Drop`, set `is_running` to false,
        // and join the thread, closing the HTTP server and releasing the port.
        drop(receiver);

        // Wait a tiny bit for the OS to actually release the port if needed,
        // though join() in Drop should make it synchronous.
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Try to bind to the exact same port again. If the previous receiver
        // thread didn't shut down properly and leaked the server, this will fail
        // with "Address already in use".
        let _new_receiver = OtlpReceiverInput::new("test-drop-2", &local_addr.to_string())
            .expect("should bind successfully to the exact same port");
    }

    #[test]
    fn receiver_health_is_healthy_while_running() {
        let receiver =
            OtlpReceiverInput::new("test-health", "127.0.0.1:0").expect("should bind successfully");

        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn receiver_health_reports_stopping_when_shutdown_requested() {
        let receiver = OtlpReceiverInput::new("test-health-stop", "127.0.0.1:0")
            .expect("should bind successfully");
        receiver.is_running.store(false, Ordering::Relaxed);
        receiver
            .health
            .store(ComponentHealth::Stopping.as_repr(), Ordering::Relaxed);

        assert_eq!(receiver.health(), ComponentHealth::Stopping);
    }

    #[test]
    fn receiver_health_reports_failed_when_server_thread_exits() {
        let mut receiver = OtlpReceiverInput::new("test-health-failed", "127.0.0.1:0")
            .expect("should bind successfully");
        // Ensure the real worker exits before replacing the ownership task in-test.
        receiver.is_running.store(false, Ordering::Relaxed);
        let (shutdown_tx, _shutdown_rx) = oneshot::channel();
        receiver.background_task =
            BackgroundHttpTask::new_axum(shutdown_tx, std::thread::spawn(|| {}));
        receiver.is_running.store(true, Ordering::Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(10));

        assert_eq!(receiver.health(), ComponentHealth::Failed);
    }

    #[test]
    fn receiver_health_reports_failed_when_pipeline_disconnects() {
        let mut receiver =
            OtlpReceiverInput::new_with_capacity("test-disconnect", "127.0.0.1:0", 16)
                .expect("should bind successfully");
        let port = receiver.local_addr().port();
        let url = format!("http://127.0.0.1:{port}/v1/logs");
        receiver.rx.take();

        let status = match ureq::post(&url)
            .header("content-type", "application/json")
            .send(
                br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}"#,
            ) {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };

        assert_eq!(status, 503);
        assert_eq!(receiver.health(), ComponentHealth::Failed);
    }

    // Valid OTLP JSON should still return 200 after the 400 fix.
    #[test]
    fn valid_otlp_json_returns_200() {
        let receiver = OtlpReceiverInput::new_with_capacity("test", "127.0.0.1:0", 16).unwrap();
        let port = receiver.local_addr().port();
        let url = format!("http://127.0.0.1:{port}/v1/logs");

        let valid_body = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"severityText":"INFO","body":{"stringValue":"hello"}}]}]}]}"#;
        let result = ureq::post(&url)
            .header("content-type", "application/json")
            .send(valid_body.as_bytes());
        let status: u16 = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(
            status, 200,
            "valid OTLP JSON should return 200, got {status}"
        );
    }
}
