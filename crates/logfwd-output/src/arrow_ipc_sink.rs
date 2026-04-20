//! Arrow IPC stream sink — serializes RecordBatches to Arrow IPC format and
//! POSTs them over HTTP.
//!
//! Optionally compresses with zstd. Uses reqwest for async HTTP.
//! Content-Type: `application/vnd.apache.arrow.stream` (or
//! `application/vnd.apache.arrow.stream+zstd` when compressed).

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use logfwd_types::diagnostics::ComponentStats;

use super::sink::{SendResult, Sink, SinkFactory};
use super::{BatchMetadata, Compression};
use crate::http_classify::{DEFAULT_RETRY_AFTER_SECS, parse_retry_after};
use crate::internal_columns::project_external_batch;

/// Content-Type for uncompressed Arrow IPC stream.
const CONTENT_TYPE_ARROW: &str = "application/vnd.apache.arrow.stream";
/// Content-Type for zstd-compressed Arrow IPC stream.
const CONTENT_TYPE_ARROW_ZSTD: &str = "application/vnd.apache.arrow.stream+zstd";

// ---------------------------------------------------------------------------
// ArrowIpcSink
// ---------------------------------------------------------------------------

/// Async sink that serializes RecordBatches as Arrow IPC streams and POSTs
/// them to an HTTP endpoint.
pub struct ArrowIpcSink {
    name: String,
    config: Arc<ArrowIpcSinkConfig>,
    client: reqwest::Client,
    /// Reusable buffer for serialized IPC bytes.
    ipc_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

/// Configuration shared across all `ArrowIpcSink` instances from the same
/// factory.
pub(crate) struct ArrowIpcSinkConfig {
    endpoint: String,
    compression: Compression,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

impl ArrowIpcSink {
    const INITIAL_IPC_BUFFER_CAPACITY: usize = 64 * 1024;

    pub(crate) fn new(
        name: String,
        config: Arc<ArrowIpcSinkConfig>,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> Self {
        ArrowIpcSink {
            name,
            config,
            client,
            ipc_buf: Vec::with_capacity(Self::INITIAL_IPC_BUFFER_CAPACITY),
            stats,
        }
    }

    /// Serialize a RecordBatch into Arrow IPC stream format.
    ///
    /// Writes the schema message followed by one batch message into
    /// `self.ipc_buf`. The stream is complete (includes EOS marker).
    fn serialize_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        self.ipc_buf.clear();
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let external_batch = project_external_batch(batch).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Arrow IPC internal column projection failed: {e}"),
            )
        })?;
        let batch = external_batch.as_ref().unwrap_or(batch);

        let mut writer =
            StreamWriter::try_new(&mut self.ipc_buf, &batch.schema()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Arrow IPC writer init failed: {e}"),
                )
            })?;
        writer.write(batch).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Arrow IPC write failed: {e}"),
            )
        })?;
        writer.finish().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Arrow IPC finish failed: {e}"),
            )
        })?;

        Ok(())
    }

    /// Build the HTTP payload from the IPC buffer.
    ///
    /// For `Compression::None`, this transfers ownership of `self.ipc_buf`
    /// into the request payload and avoids a full-buffer clone.
    fn build_payload(&mut self) -> io::Result<Vec<u8>> {
        match self.config.compression {
            Compression::Zstd => zstd::bulk::compress(&self.ipc_buf, 1).map_err(io::Error::other),
            Compression::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                io::Write::write_all(&mut encoder, &self.ipc_buf)?;
                encoder.finish()
            }
            Compression::None => Ok(std::mem::take(&mut self.ipc_buf)),
        }
    }

    /// Rebuild reusable IPC buffer capacity after uncompressed send.
    ///
    /// `reqwest` consumes the payload `Vec<u8>`, so when we avoid cloning by
    /// moving `self.ipc_buf` into the request body, we intentionally allocate
    /// a fresh reusable buffer with the prior capacity.
    fn restore_uncompressed_buffer_capacity(&mut self, prior_capacity: usize) {
        if self.config.compression == Compression::None && self.ipc_buf.capacity() < prior_capacity
        {
            self.ipc_buf =
                Vec::with_capacity(prior_capacity.max(Self::INITIAL_IPC_BUFFER_CAPACITY));
        }
    }

    /// POST the payload to the configured endpoint.
    ///
    /// Returns `SendResult::RetryAfter` for 429 / 5xx so the worker pool can
    /// handle backoff. Client errors are rejected immediately.
    async fn do_send(&self, payload: Vec<u8>) -> io::Result<SendResult> {
        let content_type = match self.config.compression {
            Compression::Zstd => CONTENT_TYPE_ARROW_ZSTD,
            _ => CONTENT_TYPE_ARROW,
        };

        let mut req = self
            .client
            .post(&self.config.endpoint)
            .header("Content-Type", content_type);

        req = match self.config.compression {
            Compression::Zstd => req.header("Content-Encoding", "zstd"),
            Compression::Gzip => req.header("Content-Encoding", "gzip"),
            Compression::None => req,
        };

        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        let response = req.body(payload).send().await.map_err(io::Error::other)?;
        let status = response.status();

        if status.is_success() {
            return Ok(SendResult::Ok);
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = parse_retry_after(response.headers().get("Retry-After"))
                .unwrap_or(Duration::from_secs(DEFAULT_RETRY_AFTER_SECS));
            let _ = response.text().await.unwrap_or_default();
            return Ok(SendResult::RetryAfter(retry_after));
        }

        if status.is_server_error() {
            let retry_after = parse_retry_after(response.headers().get("Retry-After"))
                .unwrap_or(Duration::from_secs(DEFAULT_RETRY_AFTER_SECS));
            // Consume the response body to free the connection.
            let _body = response.text().await.unwrap_or_default();
            return Ok(SendResult::RetryAfter(retry_after));
        }

        // 4xx client error — not retryable.
        let body = response.text().await.unwrap_or_default();
        Ok(SendResult::Rejected(format!("HTTP {status}: {body}")))
    }
}

impl Sink for ArrowIpcSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Err(e) = self.serialize_batch(batch) {
                return SendResult::from_io_error(e);
            }
            if self.ipc_buf.is_empty() {
                return SendResult::Ok;
            }

            let prior_capacity = self.ipc_buf.capacity();
            let payload = match self.build_payload() {
                Ok(p) => p,
                Err(e) => return SendResult::IoError(e),
            };
            let payload_len = payload.len() as u64;
            let row_count = batch.num_rows() as u64;

            self.restore_uncompressed_buffer_capacity(prior_capacity);
            let send_result = self.do_send(payload).await;
            let result = match send_result {
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
// ArrowIpcSinkFactory
// ---------------------------------------------------------------------------

/// Creates `ArrowIpcSink` instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` so they reuse the same
/// connection pool, TLS sessions, and DNS cache.
pub struct ArrowIpcSinkFactory {
    name: String,
    config: Arc<ArrowIpcSinkConfig>,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl ArrowIpcSinkFactory {
    /// Create a new factory.
    ///
    /// - `endpoint`: Target HTTP URL
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

        Ok(ArrowIpcSinkFactory {
            name,
            config: Arc::new(ArrowIpcSinkConfig {
                endpoint,
                compression,
                headers: parsed_headers,
            }),
            client,
            stats,
        })
    }
}

impl SinkFactory for ArrowIpcSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(ArrowIpcSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            self.client.clone(),
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Serialization helpers (public for tests / receiver)
// ---------------------------------------------------------------------------

/// Serialize a RecordBatch to Arrow IPC stream bytes.
///
/// The output is a complete IPC stream: schema message, one batch, EOS marker.
pub fn serialize_ipc(batch: &RecordBatch) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(batch.num_rows() * 128);
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Arrow IPC writer init failed: {e}"),
        )
    })?;
    writer.write(batch).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Arrow IPC write failed: {e}"),
        )
    })?;
    writer.finish().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Arrow IPC finish failed: {e}"),
        )
    })?;
    Ok(buf)
}

/// Deserialize RecordBatches from Arrow IPC stream bytes.
///
/// Returns all batches in the stream.
pub fn deserialize_ipc(bytes: &[u8]) -> io::Result<Vec<RecordBatch>> {
    let cursor = io::Cursor::new(bytes);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Arrow IPC reader failed: {e}"),
        )
    })?;
    reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Arrow IPC read batch failed: {e}"),
        )
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
            Field::new("latency", DataType::Float64, true),
        ]));
        let message = StringArray::from(vec![Some("hello world"), Some("goodbye"), None]);
        let status = Int64Array::from(vec![Some(200), Some(500), Some(404)]);
        let latency = Float64Array::from(vec![Some(1.5), None, Some(3.5)]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(message), Arc::new(status), Arc::new(latency)],
        )
        .expect("test batch creation should succeed")
    }

    #[test]
    fn roundtrip_arrow_ipc_no_compression() {
        let batch = make_test_batch();
        let bytes = serialize_ipc(&batch).expect("serialize should succeed");
        assert!(!bytes.is_empty(), "serialized bytes should not be empty");

        let batches = deserialize_ipc(&bytes).expect("deserialize should succeed");
        assert_eq!(batches.len(), 1, "should produce exactly one batch");

        let decoded = &batches[0];
        assert_eq!(decoded.num_rows(), batch.num_rows());
        assert_eq!(decoded.num_columns(), batch.num_columns());
        assert_eq!(decoded.schema(), batch.schema());

        // Verify column data matches.
        for col_idx in 0..batch.num_columns() {
            assert_eq!(
                decoded.column(col_idx).as_ref(),
                batch.column(col_idx).as_ref(),
                "column {col_idx} mismatch"
            );
        }
    }

    #[test]
    fn roundtrip_arrow_ipc_with_zstd() {
        let batch = make_test_batch();
        let ipc_bytes = serialize_ipc(&batch).expect("serialize should succeed");

        // Compress with zstd.
        let compressed = zstd::bulk::compress(&ipc_bytes, 1).expect("zstd compress should succeed");

        // Decompress.
        let decompressed = zstd::bulk::decompress(&compressed, ipc_bytes.len() * 2)
            .expect("zstd decompress should succeed");

        let batches = deserialize_ipc(&decompressed).expect("deserialize should succeed");
        assert_eq!(batches.len(), 1);

        let decoded = &batches[0];
        assert_eq!(decoded.num_rows(), batch.num_rows());
        for col_idx in 0..batch.num_columns() {
            assert_eq!(
                decoded.column(col_idx).as_ref(),
                batch.column(col_idx).as_ref(),
                "column {col_idx} mismatch after zstd roundtrip"
            );
        }
    }

    #[test]
    fn empty_batch_serializes_to_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let batch = RecordBatch::new_empty(schema);

        // Verify the sink's serialize_batch produces empty output for 0-row batch.
        let config = Arc::new(ArrowIpcSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::None,
            headers: Vec::new(),
        });
        let stats = Arc::new(ComponentStats::new());
        let client = reqwest::Client::new();
        let mut sink = ArrowIpcSink::new("test".to_string(), config, client, stats);
        sink.serialize_batch(&batch).expect("serialize empty batch");
        assert!(
            sink.ipc_buf.is_empty(),
            "empty batch should produce no IPC bytes"
        );
    }

    #[test]
    fn deserialize_invalid_bytes_returns_error() {
        let result = deserialize_ipc(b"not arrow ipc data");
        assert!(result.is_err(), "invalid bytes should produce an error");
    }

    #[test]
    fn uncompressed_payload_moves_ipc_buffer_without_clone() {
        let batch = make_test_batch();
        let config = Arc::new(ArrowIpcSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::None,
            headers: Vec::new(),
        });
        let stats = Arc::new(ComponentStats::new());
        let client = reqwest::Client::new();
        let mut sink = ArrowIpcSink::new("test".to_string(), config, client, stats);

        sink.serialize_batch(&batch)
            .expect("serialize batch should succeed");
        let prior_capacity = sink.ipc_buf.capacity();
        let prior_ptr = sink.ipc_buf.as_ptr();

        let payload = sink.build_payload().expect("build payload should succeed");
        assert_eq!(
            payload.as_ptr(),
            prior_ptr,
            "uncompressed payload should take ownership of existing IPC allocation"
        );
        assert!(
            sink.ipc_buf.is_empty(),
            "IPC buffer should be empty after ownership transfer"
        );
        assert_eq!(
            sink.ipc_buf.capacity(),
            0,
            "taken IPC buffer should leave zero-capacity Vec"
        );

        sink.restore_uncompressed_buffer_capacity(prior_capacity);
        assert!(
            sink.ipc_buf.capacity() >= prior_capacity,
            "IPC reusable capacity should be restored after send"
        );
    }

    #[test]
    fn sink_serialization_drops_internal_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("__source_id", DataType::Int64, true),
            Field::new("__typename", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(Int64Array::from(vec![Some(7)])),
                Arc::new(StringArray::from(vec![Some("LogEvent")])),
            ],
        )
        .expect("valid batch");
        let config = Arc::new(ArrowIpcSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::None,
            headers: Vec::new(),
        });
        let stats = Arc::new(ComponentStats::new());
        let client = reqwest::Client::new();
        let mut sink = ArrowIpcSink::new("test".to_string(), config, client, stats);

        sink.serialize_batch(&batch).expect("serialize batch");

        let cursor = io::Cursor::new(sink.ipc_buf.as_slice());
        let mut reader = StreamReader::try_new(cursor, None).expect("stream reader");
        let decoded = reader
            .next()
            .expect("one batch")
            .expect("valid decoded batch");
        assert!(decoded.column_by_name("message").is_some());
        assert!(decoded.column_by_name("__source_id").is_none());
        assert!(decoded.column_by_name("__typename").is_some());
    }

    #[tokio::test]
    async fn uncompressed_send_restores_ipc_buffer_after_transport_error() {
        let batch = make_test_batch();
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("ephemeral test listener should bind");
        let addr = listener
            .local_addr()
            .expect("ephemeral test listener should have an address");
        drop(listener);

        let config = Arc::new(ArrowIpcSinkConfig {
            endpoint: format!("http://{addr}/arrow"),
            compression: Compression::None,
            headers: Vec::new(),
        });
        let stats = Arc::new(ComponentStats::new());
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(200))
            .build()
            .expect("test client should build");
        let mut sink = ArrowIpcSink::new("test".to_string(), config, client, stats);
        let metadata = BatchMetadata {
            resource_attrs: Arc::new(Vec::new()),
            observed_time_ns: 0,
        };

        let result = sink.send_batch(&batch, &metadata).await;
        assert!(
            matches!(result, SendResult::IoError(_)),
            "closed endpoint should produce a transport error"
        );
        assert!(
            sink.ipc_buf.capacity() >= ArrowIpcSink::INITIAL_IPC_BUFFER_CAPACITY,
            "uncompressed transport errors should not leave the reusable IPC buffer at zero capacity"
        );
    }
}
