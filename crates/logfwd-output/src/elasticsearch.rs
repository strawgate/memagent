use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{BatchMetadata, build_col_infos, write_row_json};

// ---------------------------------------------------------------------------
// ElasticsearchAsyncSink — reqwest-based async implementation of Sink
// ---------------------------------------------------------------------------

/// Configuration shared across all `ElasticsearchAsyncSink` instances from
/// the same factory.
pub(crate) struct ElasticsearchConfig {
    endpoint: String,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
    compress: bool,
    request_mode: ElasticsearchRequestMode,
    /// Maximum uncompressed bulk payload size in bytes. Batches that serialize
    /// larger than this are split in half and sent as separate `_bulk` requests.
    /// Default: 5 MiB — safe for Elasticsearch Serverless and self-hosted.
    max_bulk_bytes: usize,
    /// Target chunk size for experimental streaming bodies.
    stream_chunk_bytes: usize,
    /// Precomputed `_bulk` URL with `filter_path` to avoid per-request allocation.
    bulk_url: String,
    /// Precomputed `{"index":{"_index":"<name>"}}\n` bytes — avoids a `format!`
    /// allocation on every `serialize_batch` call.
    action_bytes: Box<[u8]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElasticsearchRequestMode {
    Buffered,
    Streaming,
}

/// Async Elasticsearch sink using reqwest.
///
/// Implements the [`super::sink::Sink`] trait for use with `OutputWorkerPool`.
/// All workers share the same `reqwest::Client` (cloned from the factory) to
/// reuse connection pools, TLS sessions, and DNS caches.
pub struct ElasticsearchAsyncSink {
    config: Arc<ElasticsearchConfig>,
    client: reqwest::Client,
    name: String,
    pub(crate) batch_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl ElasticsearchAsyncSink {
    pub(crate) fn new(
        name: String,
        config: Arc<ElasticsearchConfig>,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> Self {
        ElasticsearchAsyncSink {
            name,
            config,
            client,
            // Start with a modest initial capacity; the buffer will grow to the
            // right size after the first batch and stay there (via reserve in
            // send_batch_inner after each send).
            batch_buf: Vec::with_capacity(64 * 1024),
            stats,
        }
    }

    /// Returns the number of bytes currently serialized in the internal buffer.
    /// Useful for benchmarks and diagnostics.
    pub fn serialized_len(&self) -> usize {
        self.batch_buf.len()
    }

    /// Serialize the batch into `self.batch_buf` in Elasticsearch bulk format.
    ///
    /// Each row produces two lines:
    /// 1. Action: `{"index":{"_index":"<index>"}}`
    /// 2. Document: JSON-serialized record, with `@timestamp` injected if absent.
    pub fn serialize_batch(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) -> io::Result<()> {
        self.batch_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Pre-computed in config — no allocation on the hot path.
        let action_bytes = &self.config.action_bytes;

        // Derive @timestamp from metadata (ISO-8601 UTC).
        // Computed once per batch using a stack-allocated buffer — no heap allocation.
        let ts_nanos = metadata.observed_time_ns;
        let ts_secs = ts_nanos / 1_000_000_000;
        let ts_frac = ts_nanos % 1_000_000_000;
        // Stack buffer: ,"@timestamp":"YYYY-MM-DDTHH:MM:SS.fffffffffZ"}  (47 bytes)
        let mut ts_buf = [0u8; 47];
        write_ts_suffix(&mut ts_buf, ts_secs, ts_frac);
        let ts_no_comma = &ts_buf[1..]; // without leading ',' (for empty-doc case)

        // Pre-reserve capacity to avoid multiple Vec reallocations while writing.
        // Estimate: action line + ~256 bytes JSON per row.
        let estimated = num_rows * (action_bytes.len() + 256);
        self.batch_buf.reserve(estimated);

        let cols = build_col_infos(batch);
        // Check whether any output field is named `@timestamp` or `_timestamp`
        // to avoid injecting a duplicate timestamp key into the document.
        let has_timestamp_col = cols
            .iter()
            .any(|c| c.field_name == "@timestamp" || c.field_name == "_timestamp");

        for row in 0..num_rows {
            self.batch_buf.extend_from_slice(action_bytes);
            // Write doc JSON, replacing trailing `}` with @timestamp if needed.
            let doc_start = self.batch_buf.len();
            write_row_json(batch, row, &cols, &mut self.batch_buf)?;
            self.batch_buf.push(b'\n');

            // Inject @timestamp unless the batch already has a timestamp column.
            if !has_timestamp_col {
                // Replace trailing `}\n` with `,"@timestamp":"..."}` + `\n`.
                let len = self.batch_buf.len();
                // Remove the trailing `}\n` (2 bytes).
                if !self.batch_buf.ends_with(b"}\n") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "serialize_batch: JSON document did not end with '}\\n' — internal invariant violated",
                    ));
                }
                let trim_to = len - 2;
                // For the first field (empty doc `{}`), emit `{"@timestamp":"..."}`
                if trim_to == doc_start + 1 {
                    // doc was `{}`; replace with `{"@timestamp":"..."}`.
                    self.batch_buf.truncate(doc_start);
                    self.batch_buf.push(b'{');
                    self.batch_buf.extend_from_slice(ts_no_comma);
                } else {
                    self.batch_buf.truncate(trim_to);
                    self.batch_buf.extend_from_slice(&ts_buf);
                }
                self.batch_buf.push(b'\n');
            }
        }
        Ok(())
    }

    /// Extract the `took` field (milliseconds) from an ES bulk response body.
    /// Returns `None` if the field is absent or not parseable.
    fn extract_took(body: &[u8]) -> Option<u64> {
        const PREFIX: &[u8] = b"\"took\":";
        let pos = memchr::memmem::find(body, PREFIX)?;
        let rest = body[pos + PREFIX.len()..].trim_ascii_start();
        let end = rest
            .iter()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(rest.len());
        if end == 0 {
            return None;
        }
        std::str::from_utf8(&rest[..end]).ok()?.parse().ok()
    }

    /// Parse the ES bulk API response body for per-document errors.
    ///
    /// Returns `Ok(())` if all documents succeeded (`errors: false`), or
    /// an `Err` with the first failure's type and reason.
    fn parse_bulk_response(body: &[u8]) -> io::Result<()> {
        if memchr::memmem::find(body, b"\"errors\":true").is_none() {
            return Ok(());
        }
        let v: serde_json::Value = serde_json::from_slice(body).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse ES bulk response: {e}"),
            )
        })?;
        let items = v
            .get("items")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ES bulk response missing 'items' array",
                )
            })?;
        for item in items {
            let action = item
                .as_object()
                .and_then(|obj| obj.values().next())
                .and_then(serde_json::Value::as_object);
            if let Some(action_obj) = action {
                if let Some(error) = action_obj.get("error") {
                    let error_type = error
                        .get("type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let reason = error
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("no reason provided");
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("ES bulk error: {error_type}: {reason}"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Query Elasticsearch using ES|QL and receive Arrow IPC response.
    pub async fn query_arrow(&self, query: &str) -> io::Result<Vec<RecordBatch>> {
        let query_body = serde_json::json!({ "query": query });
        let query_bytes = serde_json::to_vec(&query_body).map_err(io::Error::other)?;
        let url = format!("{}/_query", self.config.endpoint);
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/vnd.apache.arrow.stream");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }
        let response = req
            .body(query_bytes)
            .send()
            .await
            .map_err(io::Error::other)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(io::Error::other(format!(
                "ES query failed (HTTP {status}): {body}"
            )));
        }
        let body = response.bytes().await.map_err(io::Error::other)?;
        let cursor = io::Cursor::new(body);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse Arrow IPC stream: {e}"),
            )
        })?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)
    }

    /// Send a batch, proactively splitting into sub-batches that fit within
    /// `max_bulk_bytes`. Also splits reactively on 413 Payload Too Large.
    fn send_batch_inner<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = io::Result<super::sink::SendResult>> + Send + 'a>>
    {
        Box::pin(async move {
            const MAX_SPLIT_DEPTH: usize = 6; // up to 64 sub-batches

            let n = batch.num_rows();
            if n == 0 {
                return Ok(super::sink::SendResult::Ok);
            }

            if self.config.request_mode == ElasticsearchRequestMode::Streaming {
                return match self
                    .do_send_streaming(batch.clone(), metadata.clone())
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(e)
                        if e.kind() == io::ErrorKind::InvalidInput
                            && n > 1
                            && depth < MAX_SPLIT_DEPTH =>
                    {
                        self.send_split_halves(batch, metadata, depth).await
                    }
                    Err(e) => Err(e),
                };
            }

            self.serialize_batch(batch, metadata)?;
            if self.batch_buf.is_empty() {
                return Ok(super::sink::SendResult::Ok);
            }

            let payload_len = self.batch_buf.len();
            // Remember the serialized size so we can restore warm capacity after
            // the buffer is moved into `do_send`.
            let prev_cap = self.batch_buf.capacity();
            let max_bytes = self.config.max_bulk_bytes;

            // Proactive split: if serialized payload exceeds max_bulk_bytes, split
            // the batch in half and send each half separately.
            if payload_len > max_bytes && n > 1 && depth < MAX_SPLIT_DEPTH {
                self.batch_buf.clear(); // discard oversized payload
                return self.send_split_halves(batch, metadata, depth).await;
            }

            // Move the serialized payload out of batch_buf so we can pass it to
            // do_send without copying.  `mem::take` leaves batch_buf as Vec::new()
            // (zero capacity, no allocation).  After do_send we restore capacity so
            // the next serialize_batch call doesn't have to grow from scratch.
            let body = std::mem::take(&mut self.batch_buf);
            let row_count = n as u64;

            match self.do_send(body).await {
                Ok(result) => {
                    // Restore warm capacity so the next serialize_batch avoids
                    // repeated small-step growth.
                    self.batch_buf.reserve(prev_cap);
                    if matches!(result, super::sink::SendResult::Ok) {
                        self.stats.inc_lines(row_count);
                        self.stats.inc_bytes(payload_len as u64);
                    }
                    Ok(result)
                }
                // Reactive split on 413 — server limit lower than our max_bulk_bytes.
                Err(e)
                    if e.kind() == io::ErrorKind::InvalidInput
                        && n > 1
                        && depth < MAX_SPLIT_DEPTH =>
                {
                    self.batch_buf.reserve(prev_cap);
                    self.send_split_halves(batch, metadata, depth).await
                }
                Err(e) => {
                    self.batch_buf.reserve(prev_cap);
                    Err(e)
                }
            }
        })
    }

    /// Split a batch in half and send each half sequentially.
    async fn send_split_halves(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        depth: usize,
    ) -> io::Result<super::sink::SendResult> {
        let n = batch.num_rows();
        let mid = n / 2;
        let left = batch.slice(0, mid);
        let right = batch.slice(mid, n - mid);
        let r1 = self.send_batch_inner(&left, metadata, depth + 1).await?;
        if !matches!(r1, super::sink::SendResult::Ok) {
            return Ok(r1);
        }
        self.send_batch_inner(&right, metadata, depth + 1).await
    }

    async fn do_send(&self, body: Vec<u8>) -> io::Result<super::sink::SendResult> {
        let body_len = body.len();

        let mut req = self
            .client
            .post(&self.config.bulk_url)
            .header("Content-Type", "application/x-ndjson");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        tracing::Span::current().record("req_bytes", body_len as u64);
        let req = if self.config.compress {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(&body).map_err(io::Error::other)?;
            let compressed = enc.finish().map_err(io::Error::other)?;
            tracing::Span::current().record("cmp_bytes", compressed.len() as u64);
            req.header("Content-Encoding", "gzip").body(compressed)
        } else {
            req.body(body)
        };

        let t0 = std::time::Instant::now();
        let response = req.send().await.map_err(io::Error::other)?;
        let send_ns = t0.elapsed().as_nanos() as u64;
        tracing::Span::current().record("send_ns", send_ns);

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Parse Retry-After header (seconds integer) or use default.
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5);
            return Ok(super::sink::SendResult::RetryAfter(
                std::time::Duration::from_secs(retry_after),
            ));
        }

        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            // 413: payload exceeds server limit. Return a transient error so the
            // caller can split the batch and retry smaller halves.
            let detail = response.text().await.unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ES returned 413 Payload Too Large (body {body_len} bytes): {detail}"),
            ));
        }

        if status.is_client_error() {
            return Ok(super::sink::SendResult::Rejected(format!(
                "ES rejected batch with HTTP {status}"
            )));
        }

        if !status.is_success() {
            return Err(io::Error::other(format!("ES returned HTTP {status}")));
        }

        let body = response.bytes().await.map_err(io::Error::other)?;
        let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
        tracing::Span::current().record("recv_ns", recv_ns);
        tracing::Span::current().record("resp_bytes", body.len() as u64);
        if let Some(took) = Self::extract_took(&body) {
            tracing::Span::current().record("took_ms", took);
        }
        Self::parse_bulk_response(&body)?;
        Ok(super::sink::SendResult::Ok)
    }

    fn send_chunk(
        tx: &mpsc::Sender<io::Result<Vec<u8>>>,
        chunk: &mut Vec<u8>,
        emitted: &AtomicU64,
        min_capacity: usize,
    ) -> io::Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        emitted.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        let next_capacity = chunk.capacity().max(min_capacity);
        let body = std::mem::replace(chunk, Vec::with_capacity(next_capacity));
        tx.blocking_send(Ok(body))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ES body receiver dropped"))
    }

    fn serialize_batch_streaming(
        batch: RecordBatch,
        metadata: BatchMetadata,
        config: Arc<ElasticsearchConfig>,
        tx: mpsc::Sender<io::Result<Vec<u8>>>,
        emitted: Arc<AtomicU64>,
    ) -> io::Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let action_bytes = &config.action_bytes;
        let ts_nanos = metadata.observed_time_ns;
        let ts_secs = ts_nanos / 1_000_000_000;
        let ts_frac = ts_nanos % 1_000_000_000;
        let mut ts_buf = [0u8; 47];
        write_ts_suffix(&mut ts_buf, ts_secs, ts_frac);
        let ts_no_comma = &ts_buf[1..];

        let cols = build_col_infos(&batch);
        let has_timestamp_col = cols
            .iter()
            .any(|c| c.field_name == "@timestamp" || c.field_name == "_timestamp");

        let mut chunk = Vec::with_capacity(config.stream_chunk_bytes.max(64 * 1024));
        for row in 0..num_rows {
            chunk.extend_from_slice(action_bytes);
            let doc_start = chunk.len();
            write_row_json(&batch, row, &cols, &mut chunk)?;
            chunk.push(b'\n');

            if !has_timestamp_col {
                let len = chunk.len();
                if !chunk.ends_with(b"}\n") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "serialize_batch_streaming: JSON document did not end with '}\\n' — internal invariant violated",
                    ));
                }
                let trim_to = len - 2;
                if trim_to == doc_start + 1 {
                    chunk.truncate(doc_start);
                    chunk.push(b'{');
                    chunk.extend_from_slice(ts_no_comma);
                } else {
                    chunk.truncate(trim_to);
                    chunk.extend_from_slice(&ts_buf);
                }
                chunk.push(b'\n');
            }

            if chunk.len() >= config.stream_chunk_bytes {
                Self::send_chunk(&tx, &mut chunk, emitted.as_ref(), config.stream_chunk_bytes)?;
            }
        }

        Self::send_chunk(&tx, &mut chunk, emitted.as_ref(), config.stream_chunk_bytes)?;
        Ok(())
    }

    async fn do_send_streaming(
        &self,
        batch: RecordBatch,
        metadata: BatchMetadata,
    ) -> io::Result<super::sink::SendResult> {
        let row_count = batch.num_rows() as u64;
        let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>(4);
        let emitted = Arc::new(AtomicU64::new(0));
        let producer_emitted = Arc::clone(&emitted);
        let config = Arc::clone(&self.config);
        let producer = tokio::task::spawn_blocking(move || {
            Self::serialize_batch_streaming(batch, metadata, config, tx, producer_emitted)
        });

        let mut req = self
            .client
            .post(&self.config.bulk_url)
            .header("Content-Type", "application/x-ndjson");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        let body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));
        let t0 = std::time::Instant::now();
        let response = req.body(body).send().await.map_err(io::Error::other)?;
        let send_ns = t0.elapsed().as_nanos() as u64;
        tracing::Span::current().record("send_ns", send_ns);

        producer
            .await
            .map_err(|e| io::Error::other(format!("ES streaming producer task failed: {e}")))??;

        let payload_len = emitted.load(Ordering::Relaxed) as usize;
        tracing::Span::current().record("req_bytes", payload_len as u64);

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5);
            return Ok(super::sink::SendResult::RetryAfter(
                std::time::Duration::from_secs(retry_after),
            ));
        }

        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            let detail = response.text().await.unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "ES returned 413 Payload Too Large (streamed body {payload_len} bytes): {detail}"
                ),
            ));
        }

        if status.is_client_error() {
            return Ok(super::sink::SendResult::Rejected(format!(
                "ES rejected batch with HTTP {status}"
            )));
        }

        if !status.is_success() {
            return Err(io::Error::other(format!("ES returned HTTP {status}")));
        }

        let body = response.bytes().await.map_err(io::Error::other)?;
        let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
        tracing::Span::current().record("recv_ns", recv_ns);
        tracing::Span::current().record("resp_bytes", body.len() as u64);
        if let Some(took) = Self::extract_took(&body) {
            tracing::Span::current().record("took_ms", took);
        }
        Self::parse_bulk_response(&body)?;
        self.stats.inc_lines(row_count);
        self.stats.inc_bytes(payload_len as u64);
        Ok(super::sink::SendResult::Ok)
    }
}

impl super::sink::Sink for ElasticsearchAsyncSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> std::pin::Pin<Box<dyn Future<Output = io::Result<super::sink::SendResult>> + Send + 'a>>
    {
        Box::pin(async move { self.send_batch_inner(batch, metadata, 0).await })
    }

    fn flush(&mut self) -> std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// ElasticsearchSinkFactory
// ---------------------------------------------------------------------------

/// Creates `ElasticsearchAsyncSink` instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` (which is internally
/// `Arc`-wrapped) so they reuse the same connection pool, TLS sessions,
/// and DNS cache.
pub struct ElasticsearchSinkFactory {
    name: String,
    pub(crate) config: Arc<ElasticsearchConfig>,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl ElasticsearchSinkFactory {
    /// Create a new factory.
    ///
    /// - `endpoint`: Base URL (e.g. `http://localhost:9200`)
    /// - `index`: Target index name (e.g. `logs`)
    /// - `headers`: Authentication headers (e.g. `Authorization: ApiKey …`)
    /// - `compress`: Enable gzip compression of the request body
    /// - `request_mode`: Buffered or experimental streaming bulk body mode
    pub fn new(
        name: String,
        endpoint: String,
        index: String,
        headers: Vec<(String, String)>,
        compress: bool,
        request_mode: ElasticsearchRequestMode,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        if compress && request_mode == ElasticsearchRequestMode::Streaming {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "streaming Elasticsearch request mode does not support gzip compression yet",
            ));
        }

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

        // 30s timeout: generous enough for large bulk responses, short enough to
        // detect dead connections before the pipeline stalls.
        // 64 max idle per host: allows all workers (typically ≤16) to keep warm
        // connections without excessive socket churn.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .pool_max_idle_per_host(64)
            .build()
            .map_err(io::Error::other)?;

        let endpoint = endpoint.trim_end_matches('/').to_string();
        let bulk_url = format!(
            "{}/_bulk?filter_path=errors,took,items.*.error,items.*.status",
            endpoint
        );

        // Pre-compute the action line bytes once so serialize_batch doesn't have
        // to allocate a String on every call.
        let action_line = format!("{{\"index\":{{\"_index\":\"{index}\"}}}}\n");

        Ok(ElasticsearchSinkFactory {
            name,
            config: Arc::new(ElasticsearchConfig {
                endpoint,
                headers: parsed_headers,
                compress,
                request_mode,
                max_bulk_bytes: 5 * 1024 * 1024, // 5 MiB default
                stream_chunk_bytes: 64 * 1024,
                bulk_url,
                action_bytes: action_line.into_bytes().into_boxed_slice(),
            }),
            client,
            stats,
        })
    }
}

impl super::sink::SinkFactory for ElasticsearchSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::sink::Sink>> {
        Ok(Box::new(ElasticsearchAsyncSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            self.client.clone(), // reqwest::Client is Arc-wrapped, clone is cheap
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl ElasticsearchSinkFactory {
    /// Create a concrete `ElasticsearchAsyncSink` without boxing it.
    ///
    /// Intended for benchmarks and tests that need access to
    /// `serialize_batch` or `serialized_len` directly.
    pub fn create_sink(&self) -> ElasticsearchAsyncSink {
        ElasticsearchAsyncSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            self.client.clone(),
            Arc::clone(&self.stats),
        )
    }
}

// ---------------------------------------------------------------------------
// UTC timestamp formatting (no chrono dependency)
// ---------------------------------------------------------------------------

/// Write a Unix timestamp (seconds) as `YYYY-MM-DDTHH:MM:SS` directly into
/// `buf[0..19]`, zero-allocation.
///
/// Handles dates from 1970 to ~2100 correctly via the Gregorian calendar.
fn write_unix_timestamp_utc_into(buf: &mut [u8; 19], secs: u64) {
    let days = secs / 86400;
    let time = secs % 86400;
    let h = time / 3600;
    let m = (time % 3600) / 60;
    let s = time % 60;

    let mut year = 1970u32;
    let mut remaining = days;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }
    let leap = is_leap_year(year);
    let month_days: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1u32;
    for &md in &month_days {
        if remaining < md as u64 {
            break;
        }
        remaining -= md as u64;
        month += 1;
    }
    let day = (remaining + 1) as u32;

    // Write YYYY-MM-DDTHH:MM:SS into the 19-byte buffer.
    debug_assert!(year <= 9999, "year must be <= 9999, got {year}");
    buf[0] = b'0' + (year / 1000) as u8;
    buf[1] = b'0' + (year / 100 % 10) as u8;
    buf[2] = b'0' + (year / 10 % 10) as u8;
    buf[3] = b'0' + (year % 10) as u8;
    buf[4] = b'-';
    buf[5] = b'0' + (month / 10) as u8;
    buf[6] = b'0' + (month % 10) as u8;
    buf[7] = b'-';
    buf[8] = b'0' + (day / 10) as u8;
    buf[9] = b'0' + (day % 10) as u8;
    buf[10] = b'T';
    buf[11] = b'0' + (h / 10) as u8;
    buf[12] = b'0' + (h % 10) as u8;
    buf[13] = b':';
    buf[14] = b'0' + (m / 10) as u8;
    buf[15] = b'0' + (m % 10) as u8;
    buf[16] = b':';
    buf[17] = b'0' + (s / 10) as u8;
    buf[18] = b'0' + (s % 10) as u8;
}

/// Fill `out[0..47]` with the full `@timestamp` suffix used in bulk documents:
///
/// ```text
/// ,"@timestamp":"YYYY-MM-DDTHH:MM:SS.fffffffffZ"}
/// ```
///
/// The leading `,` is at index 0; callers that need the no-comma form
/// (empty-document case) can simply slice `&out[1..]`.
///
/// Zero-allocation: all work happens on the stack-allocated `out` buffer.
fn write_ts_suffix(out: &mut [u8; 47], secs: u64, frac: u64) {
    debug_assert!(frac < 1_000_000_000, "frac must be < 1e9, got {frac}");
    out[0] = b',';
    out[1..15].copy_from_slice(b"\"@timestamp\":\"");
    let mut dt = [0u8; 19];
    write_unix_timestamp_utc_into(&mut dt, secs);
    out[15..34].copy_from_slice(&dt);
    out[34] = b'.';
    // Write 9-digit zero-padded nanosecond fraction.
    let mut f = frac;
    for i in (35..44).rev() {
        out[i] = b'0' + (f % 10) as u8;
        f /= 10;
    }
    out[44] = b'Z';
    out[45] = b'"';
    out[46] = b'}';
}

/// Format a Unix timestamp (seconds) as `YYYY-MM-DDTHH:MM:SS` in UTC.
///
/// Wraps [`write_unix_timestamp_utc_into`] for use in unit tests.
/// Production code uses [`write_ts_suffix`] directly to avoid the String allocation.
#[cfg(test)]
fn format_unix_timestamp_utc(secs: u64) -> String {
    let mut buf = [0u8; 19];
    write_unix_timestamp_utc_into(&mut buf, secs);
    // `[u8; 19]` is Copy; Vec::from avoids the extra clone that to_vec() would do.
    // write_unix_timestamp_utc_into only writes ASCII digits and punctuation.
    String::from_utf8(Vec::from(buf)).expect("timestamp bytes are valid UTF-8")
}

fn is_leap_year(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn zero_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 0,
        }
    }

    fn make_test_sink(index: &str) -> ElasticsearchAsyncSink {
        let factory = ElasticsearchSinkFactory::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            index.to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            Arc::new(ComponentStats::default()),
        )
        .expect("factory creation failed");
        let client = reqwest::Client::new();
        ElasticsearchAsyncSink::new(
            "test".to_string(),
            Arc::clone(&factory.config),
            client,
            Arc::new(ComponentStats::default()),
        )
    }

    #[test]
    fn serialize_batch_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ERROR", "WARN"])),
                Arc::new(Int64Array::from(vec![500, 404])),
            ],
        )
        .expect("batch creation failed");

        let mut sink = make_test_sink("logs");
        let meta = zero_metadata();

        sink.serialize_batch(&batch, &meta)
            .expect("serialize failed");
        let output = String::from_utf8_lossy(&sink.batch_buf);

        // Should produce 4 lines: action + doc for each of 2 rows.
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains(r#"{"index":{"_index":"logs"}}"#));
        assert!(lines[1].contains(r#""level":"ERROR""#));
        assert!(lines[1].contains(r#""status":500"#));
        assert!(lines[1].contains(r#""@timestamp""#));
        assert!(lines[2].contains(r#"{"index":{"_index":"logs"}}"#));
        assert!(lines[3].contains(r#""level":"WARN""#));
    }

    #[test]
    fn parse_bulk_response_success() {
        let response = br#"{"took":5,"errors":false,"items":[{"index":{"_id":"1","status":201}}]}"#;
        ElasticsearchAsyncSink::parse_bulk_response(response).expect("should not error on success");
    }

    #[test]
    fn parse_bulk_response_error() {
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"_id":"1","status":201}},
                {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}}
            ]
        }"#;
        let err = ElasticsearchAsyncSink::parse_bulk_response(response)
            .expect_err("should error on bulk failure");
        assert!(err.to_string().contains("mapper_parsing_exception"));
    }

    #[test]
    fn empty_batch_produces_empty_output() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "level",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .expect("batch creation failed");

        let mut sink = make_test_sink("logs");
        let meta = zero_metadata();

        sink.serialize_batch(&batch, &meta)
            .expect("serialize failed");
        assert!(sink.batch_buf.is_empty());
    }

    // --- is_leap_year ---

    #[test]
    fn leap_year_divisible_by_400() {
        assert!(is_leap_year(1600));
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2400));
    }

    #[test]
    fn not_leap_year_divisible_by_100_not_400() {
        assert!(!is_leap_year(1700));
        assert!(!is_leap_year(1800));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2100));
    }

    #[test]
    fn leap_year_divisible_by_4_not_100() {
        assert!(is_leap_year(2024));
        assert!(is_leap_year(2020));
        assert!(is_leap_year(1996));
    }

    #[test]
    fn not_leap_year_not_divisible_by_4() {
        assert!(!is_leap_year(2023));
        assert!(!is_leap_year(2019));
        assert!(!is_leap_year(1999));
    }

    // --- format_unix_timestamp_utc ---

    #[test]
    fn timestamp_epoch_is_1970_01_01() {
        assert_eq!(format_unix_timestamp_utc(0), "1970-01-01T00:00:00");
    }

    #[test]
    fn timestamp_one_second() {
        assert_eq!(format_unix_timestamp_utc(1), "1970-01-01T00:00:01");
    }

    #[test]
    fn timestamp_one_day() {
        assert_eq!(format_unix_timestamp_utc(86400), "1970-01-02T00:00:00");
    }

    #[test]
    fn timestamp_y2k() {
        // 2000-01-01T00:00:00 UTC = 946684800
        assert_eq!(format_unix_timestamp_utc(946684800), "2000-01-01T00:00:00");
    }

    #[test]
    fn timestamp_leap_day_2000() {
        // 2000-02-29T00:00:00 UTC = 951782400
        assert_eq!(format_unix_timestamp_utc(951782400), "2000-02-29T00:00:00");
    }

    #[test]
    fn timestamp_non_leap_year_march() {
        // 2001-03-01T00:00:00 UTC = 983404800 (2001 is not a leap year)
        assert_eq!(format_unix_timestamp_utc(983404800), "2001-03-01T00:00:00");
    }

    #[test]
    fn timestamp_mid_of_day() {
        // 1970-01-01T12:34:56 = 45296
        assert_eq!(format_unix_timestamp_utc(45296), "1970-01-01T12:34:56");
    }

    #[test]
    fn timestamp_end_of_year() {
        // 1970-12-31T23:59:59 = 31535999
        assert_eq!(format_unix_timestamp_utc(31535999), "1970-12-31T23:59:59");
    }

    // --- parse_bulk_response: edge cases ---

    #[test]
    fn parse_bulk_response_empty_items_array() {
        let response = br#"{"took":0,"errors":false,"items":[]}"#;
        ElasticsearchAsyncSink::parse_bulk_response(response).expect("empty items should succeed");
    }

    #[test]
    fn parse_bulk_response_malformed_json_without_errors_true_is_ok() {
        // The implementation uses fast-path memchr for "errors":true.
        // Malformed JSON that doesn't contain that string is treated as success.
        ElasticsearchAsyncSink::parse_bulk_response(b"not valid json")
            .expect("no errors:true → treated as ok");
    }

    #[test]
    fn parse_bulk_response_malformed_json_after_errors_true_returns_err() {
        // If "errors":true is present but the full JSON parse fails, return an error.
        ElasticsearchAsyncSink::parse_bulk_response(b"\"errors\":true {{{malformed")
            .expect_err("malformed JSON after errors:true should error");
    }

    #[test]
    fn parse_bulk_response_errors_false_does_not_error() {
        // errors:false means success even if items have non-200 status
        let response = br#"{"took":1,"errors":false,"items":[{"index":{"_id":"1","status":200}}]}"#;
        ElasticsearchAsyncSink::parse_bulk_response(response).expect("errors:false must succeed");
    }
}

// ---------------------------------------------------------------------------
// Snapshot tests (insta)
// ---------------------------------------------------------------------------
//
// NOTE: Snapshot files are auto-generated on the first run via `cargo insta test`.
// Until then, the tests will fail with "snapshot not found" — run:
//   cargo insta test --review
// to accept the initial snapshots.

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn zero_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 0,
        }
    }

    fn make_test_sink() -> ElasticsearchAsyncSink {
        let factory = ElasticsearchSinkFactory::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "test-index".to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            Arc::new(ComponentStats::default()),
        )
        .expect("factory creation failed");
        let client = reqwest::Client::new();
        ElasticsearchAsyncSink::new(
            "test".to_string(),
            Arc::clone(&factory.config),
            client,
            Arc::new(ComponentStats::default()),
        )
    }

    /// Snapshot: basic multi-type batch (level, status, duration_ms).
    /// Regression guard: field name preservation + type serialization.
    #[test]
    fn snapshot_basic_multi_type_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
            Field::new("duration_ms", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ERROR", "INFO", "WARN"])),
                Arc::new(Int64Array::from(vec![500i64, 200, 404])),
                Arc::new(Float64Array::from(vec![125.5f64, 3.2, 87.0])),
            ],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("basic_multi_type", output);
    }

    /// Snapshot: nullable columns — null values must appear as JSON null.
    #[test]
    fn snapshot_nullable_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
                Arc::new(Int64Array::from(vec![Some(1i64), Some(2), None])),
            ],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("nullable_columns", output);
    }

    /// Snapshot: strings with special JSON characters (quotes, backslash, newlines).
    /// Regression guard: escape_json correctness.
    #[test]
    fn snapshot_special_char_strings() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                r#"say "hello" world"#,
                "line1\nline2\ttab",
                r"back\slash",
                "control\x00char",
            ]))],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("special_char_strings", output);
    }

    /// Snapshot: single row with all-null nullable fields produces valid output.
    #[test]
    fn snapshot_all_null_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None as Option<&str>])),
                Arc::new(Int64Array::from(vec![None as Option<i64>])),
            ],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("all_null_fields", output);
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Prove is_leap_year satisfies all four cases of the Gregorian calendar rule
    /// exhaustively for every possible u32 year value.
    #[kani::proof]
    fn verify_is_leap_year_gregorian_rules() {
        let y: u32 = kani::any();

        kani::cover!(is_leap_year(y), "at least one leap year found");
        kani::cover!(!is_leap_year(y), "at least one non-leap year found");

        // Rule 1: divisible by 400 → always leap
        if y % 400 == 0 {
            assert!(is_leap_year(y), "div by 400 must be leap: {y}");
        }
        // Rule 2: divisible by 100 but not 400 → not leap
        if y % 100 == 0 && y % 400 != 0 {
            assert!(
                !is_leap_year(y),
                "div by 100 but not 400 must not be leap: {y}"
            );
        }
        // Rule 3: divisible by 4 but not 100 → leap
        if y % 4 == 0 && y % 100 != 0 {
            assert!(is_leap_year(y), "div by 4 but not 100 must be leap: {y}");
        }
        // Rule 4: not divisible by 4 → not leap
        if y % 4 != 0 {
            assert!(!is_leap_year(y), "not div by 4 must not be leap: {y}");
        }
    }

    /// Prove write_ts_suffix produces valid ASCII for any representative
    /// timestamp in the first 16 years of the epoch (bounded for solver speed).
    #[kani::proof]
    fn verify_write_ts_suffix_ascii() {
        // Restrict to first ~16 years (0..504921600) to keep the solver tractable.
        let secs: u64 = kani::any();
        kani::assume(secs < 504_921_600); // 1970-01-01 to 1985-12-31
        let frac: u64 = kani::any();
        kani::assume(frac < 1_000_000_000);

        let mut buf = [0u8; 47];
        write_ts_suffix(&mut buf, secs, frac);

        // Every byte must be printable ASCII (32..=126).
        for &b in &buf {
            assert!(b >= 32 && b <= 126, "non-printable byte in ts_suffix");
        }
        // Structural checks: leading comma, fixed string markers.
        assert_eq!(buf[0], b',');
        assert_eq!(buf[1], b'"');
        assert_eq!(buf[34], b'.');
        assert_eq!(buf[44], b'Z');
        assert_eq!(buf[45], b'"');
        assert_eq!(buf[46], b'}');
    }
}
