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

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use crate::InputError;

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
    server: std::sync::Arc<tiny_http::Server>,
    /// Keep the server thread handle alive.
    handle: Option<std::thread::JoinHandle<()>>,
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
        let server = std::sync::Arc::new(
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

        let server_clone = std::sync::Arc::clone(&server);
        let handle = std::thread::Builder::new()
            .name("arrow-ipc-receiver".into())
            .spawn(move || {
                for mut request in server_clone.incoming_requests() {
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
                    for batch in batches {
                        if batch.num_rows() == 0 {
                            continue;
                        }
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
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "too many requests: pipeline backpressure",
                                )
                                .with_status_code(429),
                            );
                        }
                        Some(503) => {
                            let _ = request.respond(
                                tiny_http::Response::from_string(
                                    "service unavailable: pipeline disconnected",
                                )
                                .with_status_code(503),
                            );
                        }
                        Some(_) => unreachable!(),
                        None => {
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
            server,
            handle: Some(handle),
        })
    }

    /// Returns the local address the HTTP server is bound to.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// Try to receive all available RecordBatches (non-blocking).
    pub fn try_recv_all(&self) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        while let Ok(batch) = self.rx.as_ref().unwrap().try_recv() {
            batches.push(batch);
        }
        batches
    }

    /// Blocking receive of the next RecordBatch.
    pub fn recv(&self) -> io::Result<RecordBatch> {
        self.rx
            .as_ref()
            .unwrap()
            .recv()
            .map_err(|_| io::Error::other("Arrow IPC receiver: channel disconnected"))
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> io::Result<RecordBatch> {
        self.rx
            .as_ref()
            .unwrap()
            .recv_timeout(timeout)
            .map_err(|e| match e {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
    fn decode_ipc_stream_empty_body() {
        let batches = decode_ipc_stream(b"").expect("empty body should succeed");
        assert!(batches.is_empty());
    }

    #[test]
    fn decode_ipc_stream_invalid_body() {
        let result = decode_ipc_stream(b"not arrow data");
        assert!(result.is_err());
    }
}

impl Drop for ArrowIpcReceiver {
    fn drop(&mut self) {
        self.rx.take();
        self.server.unblock();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
