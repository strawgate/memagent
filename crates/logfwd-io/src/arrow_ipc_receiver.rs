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
use logfwd_types::diagnostics::ComponentHealth;

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
        let server = Arc::new(
            tiny_http::Server::http(addr)
                .map_err(|e| io::Error::other(format!("Arrow IPC receiver bind {addr}: {e}")))?,
        );

        let bound_addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(a) => a,
            tiny_http::ListenAddr::Unix(_) => {
                return Err(io::Error::other(
                    "Arrow IPC receiver: unexpected listen addr",
                ));
            }
        };

        let (tx, rx) = mpsc::sync_channel(capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);
        let health = Arc::new(AtomicU8::new(ComponentHealth::Healthy.as_repr()));
        let health_clone = Arc::clone(&health);

        let server_clone = Arc::clone(&server);
        let handle = std::thread::Builder::new()
            .name("arrow-ipc-receiver".into())
            .spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    let store_event = |health: &AtomicU8, event| {
                        let current = ComponentHealth::from_repr(health.load(Ordering::Relaxed));
                        health.store(
                            reduce_receiver_health(current, event).as_repr(),
                            Ordering::Relaxed,
                        );
                    };
                    let mut request = match server_clone.try_recv() {
                        Ok(Some(req)) => req,
                        Ok(None) => {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            continue;
                        }
                        // Exit the worker thread on accept-side I/O failure instead of
                        // spinning forever and silently dropping all future requests.
                        Err(_) => {
                            store_event(&health_clone, ReceiverHealthEvent::FatalFailure);
                            break;
                        }
                    };

                    let url = request.url().to_string();

                    // Only accept the Arrow IPC endpoint path.
                    let path = url.split('?').next().unwrap_or(&url);
                    if path != "/v1/arrow" {
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

                    // Reject oversized bodies.
                    if request.body_length().unwrap_or(0) > MAX_BODY_SIZE {
                        let _ = request.respond(
                            tiny_http::Response::from_string("payload too large")
                                .with_status_code(413),
                        );
                        continue;
                    }

                    // Read body with a hard cap.
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

                    // Check for zstd compression via Content-Encoding or Content-Type.
                    let content_encoding = request
                        .headers()
                        .iter()
                        .find(|h| h.field.equiv("Content-Encoding"))
                        .map(|h| h.value.as_str().to_lowercase());

                    let content_type = request
                        .headers()
                        .iter()
                        .find(|h| h.field.equiv("Content-Type"))
                        .map(|h| h.value.as_str().to_lowercase());

                    let is_zstd = content_encoding.as_deref() == Some("zstd")
                        || content_type.as_deref()
                            == Some("application/vnd.apache.arrow.stream+zstd");

                    // Decompress if needed.
                    let body = if is_zstd {
                        match decompress_zstd(&body) {
                            Ok(d) => d,
                            Err(msg) => {
                                let _ = request.respond(
                                    tiny_http::Response::from_string(msg.to_string())
                                        .with_status_code(400),
                                );
                                continue;
                            }
                        }
                    } else {
                        body
                    };

                    // Deserialize Arrow IPC stream.
                    let batches = match decode_ipc_stream(&body) {
                        Ok(b) => b,
                        Err(msg) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(msg.to_string())
                                    .with_status_code(400),
                            );
                            continue;
                        }
                    };

                    // Send all batches to the pipeline.
                    let mut send_error: Option<u16> = None;
                    let mut sent_rows = false;
                    for batch in batches {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        sent_rows = true;
                        match tx.try_send(batch) {
                            Ok(()) => {}
                            Err(mpsc::TrySendError::Full(_)) => {
                                send_error = Some(429);
                                break;
                            }
                            Err(mpsc::TrySendError::Disconnected(_)) => {
                                send_error = Some(503);
                                break;
                            }
                        }
                    }

                    match send_error {
                        Some(429) => {
                            store_event(&health_clone, ReceiverHealthEvent::Backpressure);
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "too many requests: pipeline backpressure",
                                )
                                .with_status_code(429),
                            );
                        }
                        Some(503) => {
                            if !shutdown_clone.load(Ordering::Relaxed) {
                                store_event(&health_clone, ReceiverHealthEvent::FatalFailure);
                            }
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "service unavailable: pipeline disconnected",
                                )
                                .with_status_code(503),
                            );
                        }
                        Some(_) => unreachable!(),
                        None => {
                            store_event(
                                &health_clone,
                                if sent_rows {
                                    ReceiverHealthEvent::DeliveryAccepted
                                } else {
                                    ReceiverHealthEvent::DeliveryNoop
                                },
                            );
                            let _ = request.respond(
                                tiny_http::Response::from_string("").with_status_code(200),
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
            background_task: BackgroundHttpTask::new(server, handle),
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

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let msg = StringArray::from(vec![Some("alpha"), Some("beta")]);
        let code = Int64Array::from(vec![Some(1), Some(2)]);
        RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(code)])
            .expect("test batch creation should succeed")
    }

    fn serialize_batch(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .expect("writer init");
        writer.write(batch).expect("write batch");
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
