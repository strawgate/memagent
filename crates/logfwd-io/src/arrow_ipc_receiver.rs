//! Arrow IPC stream HTTP receiver.
//!
//! Accepts POST requests containing Arrow IPC stream bytes, deserializes them
//! into RecordBatches, and sends them through a channel. This bypasses the
//! scanner entirely — Arrow data enters the pipeline in native form.
//!
//! Endpoint: POST /v1/arrow
//!
//! Content-Type: `application/vnd.apache.arrow.stream` (uncompressed)
//! or `application/vnd.apache.arrow.stream+zstd` (zstd compressed).
//! Also supports `Content-Encoding: zstd` header.

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
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use http_body_util::BodyExt as _;
use logfwd_types::diagnostics::ComponentHealth;
use tokio::sync::oneshot;

use crate::InputError;
use crate::background_http_task::BackgroundHttpTask;
use crate::receiver_health::{ReceiverHealthEvent, reduce_receiver_health};

/// Maximum request body size: 10 MB.
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Bounded channel capacity — limits memory when the pipeline falls behind.
const CHANNEL_BOUND: usize = 256;

/// Arrow IPC receiver that listens for Arrow stream data via HTTP POST.
///
/// Produces `RecordBatch` directly, bypassing the JSON scanner. Each POST
/// can contain a single IPC stream with one or more batches.
pub struct ArrowIpcReceiver {
    name: String,
    rx: Option<mpsc::Receiver<RecordBatch>>,
    /// The address the HTTP server is bound to.
    addr: std::net::SocketAddr,
    background_task: BackgroundHttpTask,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
}

#[derive(Clone)]
struct ArrowIpcServerState {
    tx: mpsc::SyncSender<RecordBatch>,
    shutdown: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
}

impl ArrowIpcReceiver {
    /// Bind an HTTP server on `addr` (e.g. "0.0.0.0:4319").
    /// Spawns a background thread to handle requests.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::new_with_capacity(name, addr, CHANNEL_BOUND)
    }

    /// Like [`Self::new`] but with an explicit channel capacity. Useful for tests.
    pub fn new_with_capacity(
        name: impl Into<String>,
        addr: &str,
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
        while let Ok(batch) = rx.try_recv() {
            batches.push(batch);
        }
        batches
    }

    /// Blocking receive of the next RecordBatch.
    pub fn recv(&self) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("Arrow IPC receiver: already closed"));
        };
        rx.recv()
            .map_err(|_| io::Error::other("Arrow IPC receiver: channel disconnected"))
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> io::Result<RecordBatch> {
        let Some(rx) = self.rx.as_ref() else {
            return Err(io::Error::other("Arrow IPC receiver: already closed"));
        };
        rx.recv_timeout(timeout).map_err(|e| match e {
            mpsc::RecvTimeoutError::Timeout => {
                io::Error::new(io::ErrorKind::TimedOut, "Arrow IPC receiver: timed out")
            }
            mpsc::RecvTimeoutError::Disconnected => {
                io::Error::other("Arrow IPC receiver: channel disconnected")
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

/// Decompress zstd body with size limit.
fn decompress_zstd(body: &[u8]) -> Result<Vec<u8>, InputError> {
    let decoder = zstd::Decoder::new(body).map_err(|_| {
        InputError::Receiver("zstd decompression failed: invalid header".to_string())
    })?;
    let mut decompressed = Vec::with_capacity(body.len().min(MAX_BODY_SIZE));
    match decoder
        .take(MAX_BODY_SIZE as u64 + 1)
        .read_to_end(&mut decompressed)
    {
        Ok(n) if n > MAX_BODY_SIZE => Err(InputError::Receiver(
            "decompressed payload too large".to_string(),
        )),
        Ok(_) => Ok(decompressed),
        Err(e) => Err(InputError::Receiver(format!(
            "zstd decompression failed: {e}"
        ))),
    }
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
    let current = ComponentHealth::from_repr(health.load(Ordering::Relaxed));
    health.store(
        reduce_receiver_health(current, event).as_repr(),
        Ordering::Relaxed,
    );
}

async fn handle_arrow_ipc_request(
    State(state): State<Arc<ArrowIpcServerState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if declared_content_length(&headers).is_some_and(|body_len| body_len > MAX_BODY_SIZE as u64) {
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large").into_response();
    }

    let content_encoding = match parse_content_encoding(&headers) {
        Ok(content_encoding) => content_encoding,
        Err(status) => return (status, "invalid content-encoding header").into_response(),
    };
    let content_type = match parse_content_type(&headers) {
        Ok(content_type) => content_type,
        Err(status) => return (status, "invalid content-type header").into_response(),
    };

    let body = match read_limited_body(body).await {
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

    let is_zstd = content_encoding.as_deref() == Some("zstd")
        || content_type.as_deref() == Some("application/vnd.apache.arrow.stream+zstd");

    let body = if is_zstd {
        match decompress_zstd(&body) {
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
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        sent_rows = true;
        match state.tx.try_send(batch) {
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

fn parse_content_type(headers: &HeaderMap) -> Result<Option<String>, StatusCode> {
    let Some(value) = headers.get(CONTENT_TYPE) else {
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
        let receiver = ArrowIpcReceiver::new("test", addr).unwrap();
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
    fn receiver_accepts_arrow_ipc_post() {
        let receiver = ArrowIpcReceiver::new_with_capacity("test", "127.0.0.1:0", 16)
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
    fn receiver_accepts_zstd_compressed_arrow_ipc() {
        let receiver = ArrowIpcReceiver::new_with_capacity("test-zstd", "127.0.0.1:0", 16)
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
    fn receiver_rejects_wrong_path() {
        let receiver = ArrowIpcReceiver::new_with_capacity("test-404", "127.0.0.1:0", 16)
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
        let receiver = ArrowIpcReceiver::new_with_capacity("test-405", "127.0.0.1:0", 16)
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
        let receiver = ArrowIpcReceiver::new_with_capacity("test-429", "127.0.0.1:0", 1)
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
        let receiver = ArrowIpcReceiver::new_with_capacity("test-partial", "127.0.0.1:0", 1)
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
        let receiver = ArrowIpcReceiver::new_with_capacity("test-dup-risk", "127.0.0.1:0", 1)
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
        let mut receiver = ArrowIpcReceiver::new_with_capacity("test-empty", "127.0.0.1:0", 16)
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
}
