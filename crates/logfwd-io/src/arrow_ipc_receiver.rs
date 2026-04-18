//! Arrow IPC stream HTTP receiver.
//!
//! Accepts POST requests containing Arrow IPC stream bytes, deserializes them
//! into RecordBatches, and sends them through a channel. This bypasses the
//! scanner entirely — Arrow data enters the pipeline in native form.
//!
//! Endpoint: POST /v1/arrow
//!
//! Content-Type: `application/vnd.apache.arrow.stream` (uncompressed),
//! `application/vnd.apache.arrow.stream+zstd` (zstd compressed), or
//! `application/vnd.apache.arrow.stream+lz4` (lz4 compressed).
//! Also supports `Content-Encoding: zstd` and `Content-Encoding: lz4` headers.

use std::io;
use std::io::Read as _;
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
use logfwd_types::diagnostics::ComponentHealth;
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::input::{InputEvent, InputSource};
use crate::receiver_health::{ReceiverHealthEvent, reduce_receiver_health};
use crate::receiver_http::{MAX_REQUEST_BODY_SIZE, parse_content_length, read_limited_body};

/// Bounded channel capacity — limits memory when the pipeline falls behind.
const CHANNEL_BOUND: usize = 256;

#[derive(Debug, Clone)]
pub struct ArrowIpcReceiverOptions {
    /// Maximum concurrent connections processing requests. Defaults to `1024`.
    pub max_connections: usize,
    /// Maximum payload size in bytes. Defaults to `MAX_REQUEST_BODY_SIZE` (10 MiB).
    pub max_message_size_bytes: usize,
}

impl Default for ArrowIpcReceiverOptions {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            max_message_size_bytes: MAX_REQUEST_BODY_SIZE,
        }
    }
}

/// Arrow IPC receiver that listens for Arrow stream data via HTTP POST.
///
/// Produces `RecordBatch` directly, bypassing the JSON scanner. Each POST
/// can contain a single IPC stream with one or more batches.
pub struct ArrowIpcReceiver {
    name: String,
    rx: Option<mpsc::Receiver<DecodedBatch>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    background_task: BackgroundHttpTask,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
}

#[derive(Clone)]
struct ArrowIpcServerState {
    tx: mpsc::SyncSender<DecodedBatch>,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
    max_connections: usize,
    max_message_size_bytes: usize,
}

struct ConnectionGuard {
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct DecodedBatch {
    batch: RecordBatch,
    accounted_bytes: u64,
}

impl ArrowIpcReceiver {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4319").
    /// Spawns a background thread to handle requests.
    pub fn new(
        name: impl Into<String>,
        addr: &str,
        options: ArrowIpcReceiverOptions,
    ) -> io::Result<Self> {
        Self::new_with_capacity(name, addr, options, CHANNEL_BOUND)
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    pub fn new_with_capacity(
        name: impl Into<String>,
        addr: &str,
        options: ArrowIpcReceiverOptions,
        capacity: usize,
    ) -> io::Result<Self> {
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| io::Error::other(format!("Arrow IPC receiver bind {addr}: {e}")))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!(
                "Arrow IPC receiver set_nonblocking {bound_addr}: {e}"
            ))
        })?;

        let (tx, rx) = mpsc::sync_channel(capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let state = Arc::new(ArrowIpcServerState {
            tx,
            shutdown: Arc::clone(&shutdown),
            health: Arc::clone(&health),
            active_connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_connections: options.max_connections,
            max_message_size_bytes: options.max_message_size_bytes,
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_for_server = Arc::clone(&shutdown);
        let health_for_server = Arc::clone(&health);
        let state_for_server = Arc::clone(&state);

        let handle = std::thread::Builder::new()
            .name("arrow-ipc-receiver".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        store_health_event(&health_for_server, ReceiverHealthEvent::FatalFailure);
                        return;
                    }
                };

                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(listener) => listener,
                        Err(_) => {
                            store_health_event(
                                &health_for_server,
                                ReceiverHealthEvent::FatalFailure,
                            );
                            return;
                        }
                    };

                    let app = axum::Router::new()
                        .route("/v1/arrow", post(handle_arrow_ipc_request))
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
        while let Ok(decoded) = rx.try_recv() {
            batches.push(decoded.batch);
        }
        batches
    }

    /// Blocking receive of the next RecordBatch.
    pub fn recv(&self) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("Arrow IPC receiver: already closed"));
        };
        rx.recv()
            .map(|decoded| decoded.batch)
            .map_err(|_| io::Error::other("Arrow IPC receiver: channel disconnected"))
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("Arrow IPC receiver: already closed"));
        };
        rx.recv_timeout(timeout)
            .map_err(|e| match e {
                mpsc::RecvTimeoutError::Timeout => {
                    io::Error::new(io::ErrorKind::TimedOut, "Arrow IPC receiver: timed out")
                }
                mpsc::RecvTimeoutError::Disconnected => {
                    io::Error::other("Arrow IPC receiver: channel disconnected")
                }
            })
            .map(|decoded| decoded.batch)
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

/// Decompress zstd body with size limit.
fn decompress_zstd(body: &[u8], max_message_size_bytes: usize) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body).map_err(|_| {
        InputError::Receiver("zstd decompression failed: invalid header".to_string())
    })?;
    let mut decompressed = Vec::with_capacity(body.len().min(max_message_size_bytes));
    match decoder
        .take(max_message_size_bytes as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > max_message_size_bytes => Err(InputError::Receiver(
            "decompressed payload too large".to_string(),
        )),
        Ok(_) => Ok(decompressed),
        Err(e) => Err(InputError::Receiver(format!(
            "zstd decompression failed: {e}"
        ))),
    }
}

/// Decompress lz4 (size-prepended block format) body with size limit.
///
/// `lz4_flex::decompress_size_prepended` reads the first 4 bytes as a LE u32
/// declared size and pre-allocates that much memory. We validate the declared
/// size against `max_message_size_bytes` *before* calling it to prevent a
/// forged prefix from triggering an unbounded allocation (DoS vector).
fn decompress_lz4(body: &[u8], max_message_size_bytes: usize) -> Result<Vec<u8>, InputError> {
    if body.len() < 4 {
        return Err(InputError::Receiver(
            "lz4 decompression failed: missing size prefix".to_string(),
        ));
    }
    let declared_len = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    if declared_len > max_message_size_bytes {
        return Err(InputError::Receiver(
            "decompressed payload too large".to_string(),
        ));
    }
    lz4_flex::decompress_size_prepended(body)
        .map_err(|e| InputError::Receiver(format!("lz4 decompression failed: {e}")))
}

/// Decode an Arrow IPC stream from bytes into RecordBatches.
fn decode_ipc_stream(body: &[u8]) -> Result<Vec<RecordBatch>, InputError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = io::Cursor::new(body);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| InputError::Receiver(format!("invalid Arrow IPC stream: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| InputError::Receiver(format!("failed to read Arrow IPC batch: {e}")))
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

async fn handle_arrow_ipc_request(
    State(state): State<Arc<ArrowIpcServerState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if state.active_connections.fetch_add(1, Ordering::Relaxed) >= state.max_connections {
        state.active_connections.fetch_sub(1, Ordering::Relaxed);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent connections",
        )
            .into_response();
    }
    let _guard = ConnectionGuard {
        active_connections: Arc::clone(&state.active_connections),
    };

    let content_length = parse_content_length(&headers);
    if content_length.is_some_and(|body_len| body_len > state.max_message_size_bytes as u64) {
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let content_encodings = match parse_content_encoding(&headers) {
        Ok(content_encoding) => content_encoding,
        Err(status) => return (status, "invalid content-encoding header").into_response(),
    };
    let content_type = match parse_content_type(&headers) {
        Ok(content_type) => content_type,
        Err(status) => return (status, "invalid content-type header").into_response(),
    };

    let body = match read_limited_body(body, state.max_message_size_bytes, content_length).await {
        Ok(body) => body,
        Err(status) => {
            let message = if status == StatusCode::PAYLOAD_TOO_LARGE {
                "payload too large"
            } else {
                "read error"
            };
            return (status, message).into_response();
        }
    };
    let raw_body_len = body.len() as u64;

    let has_zstd_content_encoding = content_encodings
        .as_ref()
        .is_some_and(|encoding| encoding.has_zstd);
    let has_lz4_content_encoding = content_encodings
        .as_ref()
        .is_some_and(|encoding| encoding.has_lz4);
    let is_zstd = has_zstd_content_encoding
        || content_type.as_ref().is_some_and(|media_type| {
            media_type.eq_ignore_ascii_case("application/vnd.apache.arrow.stream+zstd")
        });
    let is_lz4 = has_lz4_content_encoding
        || content_type.as_ref().is_some_and(|media_type| {
            media_type.eq_ignore_ascii_case("application/vnd.apache.arrow.stream+lz4")
        });

    let body = if is_zstd {
        match decompress_zstd(&body, state.max_message_size_bytes) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "zstd decompression failed: invalid header",
                )
                    .into_response();
            }
        }
    } else if is_lz4 {
        match decompress_lz4(&body, state.max_message_size_bytes) {
            Ok(body) => body,
            Err(InputError::Receiver(msg)) => {
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "lz4 decompression failed").into_response();
            }
        }
    } else {
        body
    };

    let batches = match decode_ipc_stream(&body) {
        Ok(batches) => batches,
        Err(InputError::Receiver(msg)) => return (StatusCode::BAD_REQUEST, msg).into_response(),
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid Arrow IPC stream").into_response(),
    };

    let mut send_error: Option<StatusCode> = None;
    let mut sent_rows = false;
    let total_batch_count = batches.iter().filter(|batch| batch.num_rows() > 0).count() as u64;
    let per_batch_accounted_bytes = raw_body_len.checked_div(total_batch_count).unwrap_or(0);
    let mut emitted_count = 0_u64;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        sent_rows = true;
        emitted_count = emitted_count.saturating_add(1);
        let accounted_bytes = if emitted_count == total_batch_count {
            raw_body_len.saturating_sub(
                per_batch_accounted_bytes.saturating_mul(total_batch_count.saturating_sub(1)),
            )
        } else {
            per_batch_accounted_bytes
        };
        let payload = DecodedBatch {
            batch,
            accounted_bytes,
        };
        match state.tx.try_send(payload) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                send_error = Some(StatusCode::TOO_MANY_REQUESTS);
                break;
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                send_error = Some(StatusCode::SERVICE_UNAVAILABLE);
                break;
            }
        }
    }

    match send_error {
        Some(StatusCode::TOO_MANY_REQUESTS) => {
            store_health_event(&state.health, ReceiverHealthEvent::Backpressure);
            (
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests: pipeline backpressure",
            )
                .into_response()
        }
        Some(StatusCode::SERVICE_UNAVAILABLE) => {
            if !state.shutdown.load(Ordering::Relaxed) {
                store_health_event(&state.health, ReceiverHealthEvent::FatalFailure);
            }
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable: pipeline disconnected",
            )
                .into_response()
        }
        Some(_) => unreachable!(),
        None => {
            store_health_event(
                &state.health,
                if sent_rows {
                    ReceiverHealthEvent::DeliveryAccepted
                } else {
                    ReceiverHealthEvent::DeliveryNoop
                },
            );
            StatusCode::OK.into_response()
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct ContentEncodingFlags {
    has_zstd: bool,
    has_lz4: bool,
}

fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<ContentEncodingFlags>, StatusCode> {
    let Some(value) = headers.get(CONTENT_ENCODING) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut flags = ContentEncodingFlags::default();
    let mut has_token = false;
    for token in parsed.split(',').map(str::trim) {
        if token.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        if token.eq_ignore_ascii_case("zstd") {
            flags.has_zstd = true;
        } else if token.eq_ignore_ascii_case("lz4") {
            flags.has_lz4 = true;
        } else if !token.eq_ignore_ascii_case("identity") {
            return Err(StatusCode::BAD_REQUEST);
        }
        has_token = true;
    }
    if !has_token {
        return Err(StatusCode::BAD_REQUEST);
    }
    if flags.has_zstd && flags.has_lz4 {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(flags))
}

fn parse_content_type(headers: &HeaderMap) -> Result<Option<&str>, StatusCode> {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return Ok(None);
    };
    let parsed = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    let media_type = parsed.split(';').next().unwrap_or(parsed).trim();
    if media_type.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(media_type))
}

impl Drop for ArrowIpcReceiver {
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

impl InputSource for ArrowIpcReceiver {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let Some(rx) = self.rx.as_ref() else {
            return Ok(vec![]);
        };
        let mut events = Vec::new();
        while let Ok(decoded) = rx.try_recv() {
            events.push(InputEvent::Batch {
                batch: decoded.batch,
                source_id: None,
                accounted_bytes: decoded.accounted_bytes,
            });
        }
        Ok(events)
    }

    fn name(&self) -> &str {
        ArrowIpcReceiver::name(self)
    }

    fn health(&self) -> ComponentHealth {
        ArrowIpcReceiver::health(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Regression test for issue #1142: clean shutdown
    #[test]
    fn clean_shutdown_releases_port() {
        let addr = "127.0.0.1:0";
        let receiver =
            ArrowIpcReceiver::new("test", addr, ArrowIpcReceiverOptions::default()).unwrap();
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
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]))
    }

    fn make_test_batch() -> RecordBatch {
        let schema = test_schema();
        let msg = StringArray::from(vec![Some("alpha"), Some("beta")]);
        let code = Int64Array::from(vec![Some(1), Some(2)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(code)])
            .expect("test batch creation should succeed")
    }

    /// A second test batch with different content so tests can distinguish it
    /// from the batch produced by `make_test_batch`.
    fn make_test_batch_b() -> RecordBatch {
        let schema = test_schema();
        let msg = StringArray::from(vec![Some("gamma"), Some("delta"), Some("epsilon")]);
        let code = Int64Array::from(vec![Some(10), Some(20), Some(30)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(code)])
            .expect("test batch B creation should succeed")
    }

    fn serialize_batch(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .expect("writer init");
        writer.write(batch).expect("write batch");
        writer.finish().expect("finish");
        buf
    }

    fn serialize_batches(batches: &[RecordBatch]) -> Vec<u8> {
        let Some(first) = batches.first() else {
            return Vec::new();
        };
        let mut buf = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &first.schema())
            .expect("writer init");
        for batch in batches {
            writer.write(batch).expect("write batch");
        }
        writer.finish().expect("finish");
        buf
    }

    #[test]
    fn test_arrow_ipc_custom_options() {
        let mut options = ArrowIpcReceiverOptions::default();
        options.max_connections = 5;
        options.max_message_size_bytes = 10;

        let addr = "127.0.0.1:0";
        let receiver = ArrowIpcReceiver::new("test_custom", addr, options).unwrap();
        let local_addr = receiver.local_addr();

        // Send a request exceeding max_message_size_bytes
        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        assert!(ipc_bytes.len() > 10);

        let url = format!("http://{local_addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(ipc_bytes.as_slice());
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(
            status, 413,
            "expected PAYLOAD_TOO_LARGE due to custom max_message_size_bytes"
        );
    }

    #[test]
    fn receiver_accepts_arrow_ipc_post() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        assert_eq!(receiver.health(), ComponentHealth::Healthy);

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        // POST Arrow IPC data.
        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        // Receive the batch.
        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
        assert_eq!(received.num_columns(), 2);
        assert_eq!(received.schema(), batch.schema());
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn poll_emits_batch_event_with_accounted_bytes() {
        let mut receiver = ArrowIpcReceiver::new_with_capacity(
            "test-poll",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let events = receiver.poll().expect("poll should succeed");
        assert_eq!(events.len(), 1, "poll should emit one batch event");
        match &events[0] {
            InputEvent::Batch {
                batch,
                source_id,
                accounted_bytes,
            } => {
                assert_eq!(batch.num_rows(), 2);
                assert_eq!(*source_id, None);
                assert_eq!(*accounted_bytes, ipc_bytes.len() as u64);
            }
            _ => panic!("expected InputEvent::Batch"),
        }
    }

    #[test]
    fn receiver_accepts_zstd_compressed_arrow_ipc() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-zstd",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream+zstd")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_accepts_zstd_content_type_with_parameters() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-zstd-params",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header(
                "Content-Type",
                "application/vnd.apache.arrow.stream+zstd; charset=binary",
            )
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_accepts_tokenized_content_encoding_with_zstd() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-zstd-tokenized",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "identity, zstd")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_rejects_unsupported_content_encoding() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-unsupported-encoding",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "gzip")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
    }

    #[test]
    fn receiver_rejects_empty_content_encoding_token() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-empty-encoding-token",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "identity,,zstd")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
    }

    #[test]
    fn receiver_rejects_wrong_path() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-404",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
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
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-405",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::get(&url).call();
        match result {
            Err(ureq::Error::StatusCode(code)) => assert_eq!(code, 405),
            other => panic!("expected 405, got {other:?}"),
        }
    }

    #[test]
    fn receiver_reports_degraded_on_backpressure_and_recovers() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-429",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            1,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");

        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("first POST should succeed");
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(receiver.health(), ComponentHealth::Healthy);

        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 429);
        assert_eq!(receiver.health(), ComponentHealth::Degraded);

        let _ = receiver.try_recv_all();

        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
            .expect("recovery POST should succeed");
        assert_eq!(response.status().as_u16(), 200);
        let _ = receiver.recv_timeout(std::time::Duration::from_secs(2));
        assert_eq!(receiver.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn receiver_returns_429_on_backpressure_to_force_retry() {
        // When the channel fills up mid-request (partial accept), we return 429 so the
        // client retries the full request. This avoids silent data loss from unretried
        // remainder batches that were never accepted.
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-partial",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            1,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let url = format!("http://{addr}/v1/arrow");

        // Use two distinct batches so we can verify which one was accepted.
        // batch_a has 2 rows ("alpha","beta"), batch_b has 3 rows ("gamma","delta","epsilon").
        let batch_a = make_test_batch();
        let batch_b = make_test_batch_b();
        let batches = vec![batch_a, batch_b];
        let ipc_bytes = serialize_batches(&batches);

        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(
            status, 429,
            "POST should return 429 when channel fills up to force client retry"
        );

        let received = receiver.try_recv_all();
        assert_eq!(
            received.len(),
            1,
            "exactly one batch should have been accepted before backpressure"
        );
        // The accepted batch must be the first (prefix) batch — batch_a with 2 rows.
        assert_eq!(
            received[0].num_rows(),
            2,
            "the prefix batch (batch_a) should be the one accepted"
        );
        let msg_col = received[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 should be StringArray");
        assert_eq!(msg_col.value(0), "alpha");
        assert_eq!(msg_col.value(1), "beta");
        assert_eq!(receiver.health(), ComponentHealth::Degraded);
    }

    #[test]
    fn partial_accept_then_retry_can_duplicate_prefix_batches() {
        // This regression makes duplicate-risk semantics explicit:
        // 1) first POST partially succeeds then returns 429
        // 2) retry of the same payload also partially accepts then returns 429
        // 3) downstream sees duplicated prefix rows from both partial accepts
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-dup-risk",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            1,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();
        let url = format!("http://{addr}/v1/arrow");

        // Use two distinct batches so we can verify which one was re-delivered.
        // batch_a has 2 rows ("alpha","beta"), batch_b has 3 rows ("gamma","delta","epsilon").
        let batch_a = make_test_batch();
        let batch_b = make_test_batch_b();
        let batches = vec![batch_a, batch_b];
        let ipc_bytes = serialize_batches(&batches);

        let first_status = match ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
        {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(first_status, 429);

        let accepted_prefix = receiver.try_recv_all();
        assert_eq!(
            accepted_prefix.len(),
            1,
            "first request should have partially accepted one prefix batch before 429"
        );
        // Verify the accepted prefix is batch_a (2 rows, "alpha"/"beta").
        assert_eq!(accepted_prefix[0].num_rows(), 2);
        let msg_col = accepted_prefix[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 should be StringArray");
        assert_eq!(msg_col.value(0), "alpha");
        assert_eq!(msg_col.value(1), "beta");

        let retry_status = match ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes)
        {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(
            retry_status, 429,
            "retry also partially accepts then returns 429"
        );

        let duplicate_prefix = receiver.try_recv_all();
        assert_eq!(
            duplicate_prefix.len(),
            1,
            "retry should re-deliver the same first prefix batch"
        );
        // Verify the duplicate is specifically batch_a again, not batch_b.
        assert_eq!(
            duplicate_prefix[0].num_rows(),
            2,
            "duplicate prefix batch should be batch_a (2 rows), not batch_b (3 rows)"
        );
        let dup_msg_col = duplicate_prefix[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 should be StringArray");
        assert_eq!(dup_msg_col.value(0), "alpha");
        assert_eq!(dup_msg_col.value(1), "beta");
    }

    #[test]
    fn decode_ipc_stream_empty_body() {
        let batches = decode_ipc_stream(b"").expect("empty body should succeed");
        assert!(batches.is_empty());
    }

    #[test]
    fn empty_request_does_not_clear_failed_health() {
        let mut receiver = ArrowIpcReceiver::new_with_capacity(
            "test-empty",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        // Drop the consumer side so the receiver reports pipeline disconnection.
        receiver.rx.take();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let url = format!("http://{addr}/v1/arrow");

        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 503);
        assert_eq!(receiver.health(), ComponentHealth::Failed);

        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .send(b"" as &[u8])
            .expect("empty request should still succeed");
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(receiver.health(), ComponentHealth::Failed);
    }

    #[test]
    fn decode_ipc_stream_invalid_body() {
        let result = decode_ipc_stream(b"not arrow data");
        assert!(result.is_err());
    }

    #[test]
    fn receiver_accepts_lz4_compressed_arrow_ipc() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-lz4",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = lz4_flex::compress_prepend_size(&ipc_bytes);

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "lz4")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn receiver_accepts_lz4_content_type() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-lz4-ct",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);
        let compressed = lz4_flex::compress_prepend_size(&ipc_bytes);

        let url = format!("http://{addr}/v1/arrow");
        let response = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream+lz4")
            .send(&compressed)
            .expect("POST should succeed");
        assert_eq!(response.status().as_u16(), 200);

        let received = receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("should receive a batch");
        assert_eq!(received.num_rows(), 2);
    }

    #[test]
    fn decompress_lz4_rejects_oversized_declared_length() {
        // Forge a 4-byte prefix claiming 1 GB, followed by minimal data.
        let mut forged = Vec::new();
        forged.extend_from_slice(&(1_073_741_824_u32).to_le_bytes());
        forged.extend_from_slice(b"tiny");

        let result = decompress_lz4(&forged, 10 * 1024 * 1024);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("too large"),
            "expected 'too large' error, got: {err_msg}"
        );
    }

    #[test]
    fn decompress_lz4_rejects_missing_prefix() {
        let result = decompress_lz4(b"abc", 10 * 1024 * 1024);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("missing size prefix"),
            "expected 'missing size prefix' error, got: {err_msg}"
        );
    }

    #[test]
    fn receiver_rejects_conflicting_zstd_and_lz4_content_encoding() {
        let receiver = ArrowIpcReceiver::new_with_capacity(
            "test-conflict-encoding",
            "127.0.0.1:0",
            ArrowIpcReceiverOptions::default(),
            16,
        )
        .expect("bind should succeed");
        let addr = receiver.local_addr();

        let batch = make_test_batch();
        let ipc_bytes = serialize_batch(&batch);

        let url = format!("http://{addr}/v1/arrow");
        let result = ureq::post(&url)
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header("Content-Encoding", "zstd, lz4")
            .send(&ipc_bytes);
        let status = match result {
            Ok(resp) => resp.status().as_u16(),
            Err(ureq::Error::StatusCode(code)) => code,
            Err(e) => panic!("unexpected error: {e}"),
        };
        assert_eq!(status, 400);
    }
}
