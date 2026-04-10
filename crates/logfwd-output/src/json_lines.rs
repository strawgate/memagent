use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use logfwd_types::diagnostics::ComponentStats;

use crate::http_classify;
use crate::sink::{SendResult, Sink, SinkFactory};

use super::{Compression, build_col_infos, write_row_json};

// ---------------------------------------------------------------------------
// JsonLinesSink
// ---------------------------------------------------------------------------

/// Writes newline-delimited JSON and POSTs over HTTP.
pub struct JsonLinesSink {
    name: String,
    url: String,
    headers: HeaderMap,
    pub(crate) batch_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    compression: Compression,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

fn parse_headers(headers: &[(String, String)]) -> io::Result<HeaderMap> {
    let mut parsed = HeaderMap::with_capacity(headers.len());
    for (k, v) in headers {
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid HTTP header name '{k}': {e}"),
            )
        })?;
        let value = HeaderValue::from_str(v).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid HTTP header value for '{k}': {e}"),
            )
        })?;
        parsed.append(name, value);
    }
    Ok(parsed)
}

impl JsonLinesSink {
    pub fn new(
        name: String,
        url: String,
        headers: HeaderMap,
        compression: Compression,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(64)
            .build()
            .unwrap_or_else(|e| {
                tracing::warn!("reqwest client builder failed: {e}");
                reqwest::Client::new()
            });
        JsonLinesSink {
            name,
            url,
            headers,
            batch_buf: Vec::with_capacity(64 * 1024),
            compress_buf: Vec::new(),
            compression,
            client,
            stats,
        }
    }

    /// Serialize the batch into `self.batch_buf` as newline-delimited JSON.
    pub fn serialize_batch(&mut self, batch: &RecordBatch) -> io::Result<u64> {
        self.batch_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(0);
        }

        let cols = build_col_infos(batch);
        for row in 0..num_rows {
            write_row_json(batch, row, &cols, &mut self.batch_buf)?;
            self.batch_buf.push(b'\n');
        }
        Ok(num_rows as u64)
    }

    async fn post_payload(&self, payload: Vec<u8>) -> io::Result<SendResult> {
        let mut req = self.client.post(&self.url).headers(self.headers.clone());

        if self.compression == Compression::Gzip {
            req = req.header("Content-Encoding", "gzip");
        } else if self.compression == Compression::Zstd {
            req = req.header("Content-Encoding", "zstd");
        }
        req = req.header("Content-Type", "application/x-ndjson");

        let response = req.body(payload).send().await.map_err(io::Error::other)?;
        if response.status().is_success() {
            return Ok(SendResult::Ok);
        }

        let retry_after = response.headers().get("Retry-After").cloned();
        let status = response.status().as_u16();
        let mut body_bytes = Vec::new();
        let mut truncated = false;
        let mut response = response;
        while let Some(chunk) = response.chunk().await.map_err(io::Error::other)? {
            if body_bytes.len() < MAX_ERROR_BODY_BYTES {
                let remaining = MAX_ERROR_BODY_BYTES - body_bytes.len();
                if chunk.len() <= remaining {
                    body_bytes.extend_from_slice(&chunk);
                } else {
                    body_bytes.extend_from_slice(&chunk[..remaining]);
                    truncated = true;
                    break;
                }
            } else {
                truncated = true;
                break;
            }
        }
        let mut body = String::from_utf8_lossy(&body_bytes).into_owned();
        if truncated {
            body.push_str("…[truncated]");
        }
        if let Some(result) = http_classify::classify_http_status(
            status,
            retry_after.as_ref(),
            &format!("HTTP output: {body}"),
        ) {
            return Ok(result);
        }
        Err(io::Error::other(format!(
            "HTTP output failed with {status}: {body}"
        )))
    }

    fn maybe_compress(&mut self) -> io::Result<Vec<u8>> {
        self.compress_buf.clear();
        match self.compression {
            Compression::None => {
                std::mem::swap(&mut self.batch_buf, &mut self.compress_buf);
                Ok(std::mem::take(&mut self.compress_buf))
            }
            Compression::Gzip => {
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::with_capacity(self.batch_buf.len() / 2),
                    flate2::Compression::default(),
                );
                use std::io::Write as _;
                encoder.write_all(&self.batch_buf)?;
                encoder.finish()
            }
            Compression::Zstd => {
                zstd::stream::copy_encode(&self.batch_buf[..], &mut self.compress_buf, 1)
                    .map_err(io::Error::other)?;
                let cap = self.compress_buf.capacity();
                Ok(std::mem::replace(
                    &mut self.compress_buf,
                    Vec::with_capacity(cap),
                ))
            }
        }
    }
}

impl Sink for JsonLinesSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a crate::BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            let rows_written = match self.serialize_batch(batch) {
                Ok(n) => n,
                Err(e) => return SendResult::from_io_error(e),
            };
            if self.batch_buf.is_empty() {
                return SendResult::Ok;
            }
            let rows = rows_written;
            let payload = match self.maybe_compress() {
                Ok(p) => p,
                Err(e) => return SendResult::from_io_error(e),
            };
            let bytes = payload.len() as u64;
            match self.post_payload(payload).await {
                Ok(SendResult::Ok) => {
                    self.stats.inc_lines(rows);
                    self.stats.inc_bytes(bytes);
                    SendResult::Ok
                }
                Ok(other) => other,
                Err(e) => SendResult::from_io_error(e),
            }
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

/// Factory that creates [`JsonLinesSink`] instances for a named HTTP endpoint.
///
/// Implements [`SinkFactory`] so the pipeline can spawn per-worker sinks that
/// each own their own HTTP client and serialisation buffers.
pub struct JsonLinesSinkFactory {
    name: String,
    endpoint: String,
    headers: Vec<(String, String)>,
    compression: Compression,
    stats: Arc<ComponentStats>,
}

impl JsonLinesSinkFactory {
    /// Create a new factory for a JSON-Lines-over-HTTP sink.
    ///
    /// # Arguments
    /// * `name`        – Human-readable component name used in logs and metrics.
    /// * `endpoint`    – Target URL that batches are POSTed to.
    /// * `headers`     – Extra HTTP headers forwarded on every request.
    /// * `compression` – Compression scheme applied before sending.
    /// * `stats`       – Shared stats handle for recording delivery metrics.
    pub fn new(
        name: String,
        endpoint: String,
        headers: Vec<(String, String)>,
        compression: Compression,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            name,
            endpoint,
            headers,
            compression,
            stats,
        }
    }
}

impl SinkFactory for JsonLinesSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let headers = parse_headers(&self.headers)?;
        Ok(Box::new(JsonLinesSink::new(
            self.name.clone(),
            self.endpoint.clone(),
            headers,
            self.compression,
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
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use logfwd_types::diagnostics::ComponentStats;

    fn make_sink() -> JsonLinesSink {
        JsonLinesSink::new(
            "test".to_string(),
            "http://localhost:1".to_string(), // unreachable — tests only call serialize_batch
            HeaderMap::new(),
            Compression::None,
            Arc::new(ComponentStats::default()),
        )
    }

    fn batch_with_body_only(body_values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(body_values))]).unwrap()
    }

    fn batch_body_plus_nulls(body_values: Vec<&str>) -> RecordBatch {
        let n = body_values.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true), // will be all-null
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(body_values)),
                Arc::new(StringArray::from(vec![None::<&str>; n])), // all null
            ],
        )
        .unwrap()
    }

    fn batch_body_with_non_null_col() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["raw data"])),
                Arc::new(StringArray::from(vec!["ERROR"])), // not null — blocks passthrough
            ],
        )
        .unwrap()
    }

    fn batch_no_body() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap()
    }

    // --- serialize_batch ---

    #[test]
    fn serialize_body_only_column_as_json() {
        let mut sink = make_sink();
        let batch = batch_with_body_only(vec![r#"{"a":1}"#, r#"{"b":2}"#]);
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines,
            vec![r#"{"body":"{\"a\":1}"}"#, r#"{"body":"{\"b\":2}"}"#]
        );
    }

    #[test]
    fn serialize_body_plus_null_cols_as_json() {
        let mut sink = make_sink();
        let batch = batch_body_plus_nulls(vec!["raw-line-1", "raw-line-2"]);
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines,
            vec![
                r#"{"body":"raw-line-1","level":null}"#,
                r#"{"body":"raw-line-2","level":null}"#
            ]
        );
    }

    #[test]
    fn serialize_json_path_produces_valid_ndjson() {
        let mut sink = make_sink();
        let batch = batch_body_with_non_null_col();
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        // Each line must be a valid JSON object ending with \n
        for line in out.lines() {
            assert!(line.starts_with('{'), "expected JSON object, got: {line}");
            assert!(line.ends_with('}'));
        }
        assert!(
            out.contains("ERROR"),
            "field value must appear in JSON output"
        );
    }

    #[test]
    fn serialize_batch_without_body_still_serializes_rows() {
        let mut sink = make_sink();
        let batch = batch_no_body();
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        assert_eq!(out.trim(), r#"{"msg":"hello"}"#);
    }

    // --- serialize_batch: edge cases ---

    #[test]
    fn serialize_empty_batch_produces_no_output() {
        let mut sink = make_sink();
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();
        sink.serialize_batch(&batch).unwrap();
        assert!(sink.batch_buf.is_empty());
    }

    #[test]
    fn serialize_clear_buf_between_calls() {
        // buf from previous call must not leak into next call
        let mut sink = make_sink();
        sink.serialize_batch(&batch_with_body_only(vec!["first"]))
            .unwrap();
        assert!(!sink.batch_buf.is_empty());
        let empty_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let empty_batch = RecordBatch::try_new(
            empty_schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();
        sink.serialize_batch(&empty_batch).unwrap();
        assert!(
            sink.batch_buf.is_empty(),
            "buf should be cleared between calls"
        );
    }

    #[test]
    fn parse_headers_rejects_invalid_name() {
        let err = parse_headers(&[("bad header".to_string(), "v".to_string())])
            .expect_err("invalid header names should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn parse_headers_rejects_invalid_value() {
        let err = parse_headers(&[("x-test".to_string(), "bad\r\nvalue".to_string())])
            .expect_err("invalid header values should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
