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

use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;

use logfwd_types::diagnostics::ComponentStats;

use super::sink::{SendResult, Sink, SinkFactory};
use super::{BatchMetadata, Compression};
use crate::http_classify::{DEFAULT_RETRY_AFTER_SECS, parse_retry_after};
use flate2::Compression as GzLevel;
use flate2::write::GzEncoder;
use std::io::Write;

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
    /// Write IPC streams using the legacy pre-1.0 Arrow IPC format.
    pub write_legacy_ipc_format: bool,
    /// IPC write buffer size in bytes.
    pub buffer_size_bytes: Option<usize>,
    /// Number of Arrow records per IPC batch.
    ///
    /// Reserved for a future TCP socket mode; not yet used by the HTTP sink.
    #[expect(dead_code)]
    pub batch_size: Option<usize>,
    /// Write the Arrow schema message immediately upon connection.
    ///
    /// Reserved for a future TCP socket mode; not yet used by the HTTP sink
    /// (the HTTP `StreamWriter` always writes the schema on init).
    #[expect(dead_code)]
    pub write_schema_on_connect: bool,
}

impl ArrowIpcSink {
    pub(crate) fn new(
        name: String,
        config: Arc<ArrowIpcSinkConfig>,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let buf_capacity = config.buffer_size_bytes.unwrap_or(64 * 1024);
        ArrowIpcSink {
            name,
            config,
            client,
            ipc_buf: Vec::with_capacity(buf_capacity),
            stats,
        }
    }

    /// Serialize a RecordBatch into Arrow IPC stream format.
    ///
    /// Writes the schema message followed by one batch message into
    /// `self.ipc_buf`. The stream is complete (includes EOS marker).
    /// Uses the legacy IPC format if `write_legacy_ipc_format` is set.
    fn serialize_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        self.ipc_buf.clear();
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let write_options = if self.config.write_legacy_ipc_format {
            IpcWriteOptions::try_new(8, true, arrow::ipc::MetadataVersion::V4).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Arrow IPC write options failed: {e}"),
                )
            })?
        } else {
            IpcWriteOptions::default()
        };

        let mut writer =
            StreamWriter::try_new_with_options(&mut self.ipc_buf, &batch.schema(), write_options)
                .map_err(|e| {
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

    /// Compress the IPC buffer with zstd if configured.
    fn maybe_compress(&self) -> io::Result<Vec<u8>> {
        match self.config.compression {
            Compression::Zstd => zstd::bulk::compress(&self.ipc_buf, 1).map_err(io::Error::other),
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), GzLevel::fast());
                encoder.write_all(&self.ipc_buf)?;
                encoder.finish()
            }
            Compression::Lz4 => {
                let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                encoder.write_all(&self.ipc_buf)?;
                encoder.finish().map_err(io::Error::other)
            }
            Compression::None => Ok(self.ipc_buf.clone()),
        }
    }

    /// POST the payload to the configured endpoint.
    ///
    /// Returns `SendResult::RetryAfter` for 429 / 5xx so the worker pool can
    /// handle backoff. Client errors are rejected immediately.
    async fn do_send(&self, payload: Vec<u8>) -> io::Result<SendResult> {
        let content_type = match self.config.compression {
            Compression::Zstd => CONTENT_TYPE_ARROW_ZSTD,
            Compression::Lz4 => "application/vnd.apache.arrow.stream+lz4",
            _ => CONTENT_TYPE_ARROW,
        };

        let mut req = self
            .client
            .post(&self.config.endpoint)
            .header("Content-Type", content_type);

        if self.config.compression == Compression::Zstd {
            req = req.header("Content-Encoding", "zstd");
        } else if self.config.compression == Compression::Gzip {
            req = req.header("Content-Encoding", "gzip");
        } else if self.config.compression == Compression::Lz4 {
            req = req.header("Content-Encoding", "lz4");
        }

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

            let payload = match self.maybe_compress() {
                Ok(p) => p,
                Err(e) => return SendResult::IoError(e),
            };
            let payload_len = payload.len() as u64;
            let row_count = batch.num_rows() as u64;

            let result = match self.do_send(payload).await {
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
    /// - `write_legacy_ipc_format`: Use legacy pre-1.0 Arrow IPC format
    /// - `buffer_size_bytes`: IPC write buffer size in bytes
    /// - `batch_size`: Number of Arrow records per IPC batch
    /// - `write_schema_on_connect`: Write Arrow schema immediately upon connection
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        endpoint: String,
        compression: Compression,
        headers: Vec<(String, String)>,
        write_legacy_ipc_format: bool,
        buffer_size_bytes: Option<usize>,
        batch_size: Option<usize>,
        write_schema_on_connect: bool,
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
                write_legacy_ipc_format,
                buffer_size_bytes,
                batch_size,
                write_schema_on_connect,
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
    #[test]
    fn arrow_ipc_gzip_compression() {
        use arrow::array::StringArray;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)])),
            vec![
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")]))
                    as arrow::array::ArrayRef,
            ],
        )
        .unwrap();

        let config_none = Arc::new(ArrowIpcSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::None,
            headers: Vec::new(),
            write_legacy_ipc_format: false,
            buffer_size_bytes: None,
            batch_size: None,
            write_schema_on_connect: false,
        });
        let config_gzip = Arc::new(ArrowIpcSinkConfig {
            endpoint: "http://localhost:9999".to_string(),
            compression: Compression::Gzip,
            headers: Vec::new(),
            write_legacy_ipc_format: false,
            buffer_size_bytes: None,
            batch_size: None,
            write_schema_on_connect: false,
        });

        let client = reqwest::Client::new();
        let stats = Arc::new(ComponentStats::new());

        let mut sink_none =
            ArrowIpcSink::new("t1".to_string(), config_none, client.clone(), stats.clone());
        let mut sink_gzip = ArrowIpcSink::new("t2".to_string(), config_gzip, client, stats);

        sink_none.serialize_batch(&batch).unwrap();
        sink_gzip.serialize_batch(&batch).unwrap();

        let uncompressed = sink_none.maybe_compress().unwrap();
        let compressed = sink_gzip.maybe_compress().unwrap();

        assert!(!compressed.is_empty());
        assert_ne!(uncompressed.len(), compressed.len());
        assert_ne!(uncompressed, compressed);
    }

    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

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
    fn roundtrip_arrow_ipc_with_gzip() {
        let batch = make_test_batch();
        let ipc_bytes = serialize_ipc(&batch).expect("serialize should succeed");

        // Compress with gzip
        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
        Write::write_all(&mut encoder, &ipc_bytes).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress with gzip
        use std::io::Read;
        let mut decoder = flate2::read::GzDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        let batches = deserialize_ipc(&decompressed).expect("deserialize should succeed");
        assert_eq!(batches.len(), 1);

        let decoded = &batches[0];
        assert_eq!(decoded.num_rows(), batch.num_rows());
        for col_idx in 0..batch.num_columns() {
            assert_eq!(
                decoded.column(col_idx).as_ref(),
                batch.column(col_idx).as_ref(),
                "column {col_idx} mismatch after gzip roundtrip"
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
            write_legacy_ipc_format: false,
            buffer_size_bytes: None,
            batch_size: None,
            write_schema_on_connect: false,
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
}
