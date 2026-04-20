use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

pub use logfwd_config::ElasticsearchRequestMode;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{BatchMetadata, build_col_infos, write_row_json};

// ---------------------------------------------------------------------------
// ElasticsearchSink — reqwest-based async implementation of Sink
// ---------------------------------------------------------------------------

/// Configuration shared across all `ElasticsearchSink` instances from
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

/// Async Elasticsearch sink using reqwest.
///
/// Implements the [`super::sink::Sink`] trait for use with `OutputWorkerPool`.
/// All workers share the same `reqwest::Client` (cloned from the factory) to
/// reuse connection pools, TLS sessions, and DNS caches.
pub struct ElasticsearchSink {
    config: Arc<ElasticsearchConfig>,
    client: reqwest::Client,
    name: String,
    pub(crate) batch_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl ElasticsearchSink {
    pub(crate) fn new(
        name: String,
        config: Arc<ElasticsearchConfig>,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> Self {
        ElasticsearchSink {
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
        // Check whether any output field is a recognised timestamp column
        // (any of: `_timestamp`, `@timestamp`, `timestamp`, `time`, `ts`)
        // to avoid injecting a duplicate timestamp key into the document.
        let has_timestamp_col = cols.iter().any(|c| {
            field_names::matches_any(
                &c.field_name,
                field_names::TIMESTAMP,
                field_names::TIMESTAMP_VARIANTS,
            )
        });

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
        const PREFIX: &[u8] = b"\"took\"";
        let pos = memchr::memmem::find(body, PREFIX)?;
        let rest = body[pos + PREFIX.len()..].trim_ascii_start();
        if !rest.starts_with(b":") {
            return None;
        }
        let rest = rest[1..].trim_ascii_start();
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
    ///
    /// **Duplication safety (#1880):** The parser scans ALL items rather than
    /// stopping at the first error. When some items succeeded and others
    /// failed, it returns `InvalidData` (permanent) to prevent the worker
    /// pool from retrying the full batch and duplicating already-accepted
    /// rows.
    ///
    /// Error kinds:
    /// - `Other`: ALL items failed with retryable errors (429/5xx) — safe to retry the whole batch
    /// - `InvalidData`: mixed success/failure, all-permanent failures, or structural anomalies —
    ///   maps to `Rejected` in the caller so the worker pool does not retry
    fn parse_bulk_response(body: &[u8]) -> io::Result<()> {
        #[derive(serde::Deserialize)]
        struct BulkHeader {
            errors: bool,
        }

        let header: BulkHeader = serde_json::from_slice(body).map_err(|e| {
            io::Error::other(format!("failed to parse ES bulk response header: {e}"))
        })?;

        if !header.errors {
            return Ok(());
        }

        // Only do full parse on the error path to avoid hot-path allocations.
        let v: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| io::Error::other(format!("failed to parse ES bulk response: {e}")))?;

        let items = v
            .get("items")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| io::Error::other("ES bulk response missing 'items' array"))?;

        if items.is_empty() {
            return Err(io::Error::other(
                "ES bulk response indicated errors but 'items' array is empty",
            ));
        }

        // Scan ALL items to count succeeded vs failed, rather than stopping at
        // the first error. This is critical to prevent duplication (#1880):
        // if some items succeeded and others failed, we must NOT return a
        // retryable error (which would cause the worker pool to retry
        // already-accepted rows).
        let mut succeeded: usize = 0;
        let mut retryable_failures: usize = 0;
        let mut permanent_failures: usize = 0;
        let mut first_error_msg: Option<String> = None;

        for item in items {
            let action = item
                .as_object()
                .and_then(|obj| obj.values().next())
                .and_then(serde_json::Value::as_object);
            if let Some(action_obj) = action {
                let status = action_obj
                    .get("status")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        io::Error::other("ES bulk response item missing numeric status field")
                    })?;

                if status < 300 {
                    succeeded += 1;
                    continue;
                }

                // Build error message from the item.
                let error_detail = if let Some(error) = action_obj.get("error") {
                    let error_type = error
                        .get("type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let reason = error
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("no reason provided");
                    format!("(status {status}): {error_type}: {reason}")
                } else {
                    format!("(status {status}): missing item error details")
                };

                if status == 429 || (500..600).contains(&status) {
                    retryable_failures += 1;
                } else {
                    permanent_failures += 1;
                }

                if first_error_msg.is_none() {
                    first_error_msg = Some(error_detail);
                }
            }
        }

        // Determine outcome based on the full picture of all items.
        let first_error = first_error_msg.unwrap_or_default();

        if succeeded > 0 && (retryable_failures > 0 || permanent_failures > 0) {
            // MIXED: some items were accepted, others failed. Return InvalidData
            // (permanent) to prevent the worker pool from retrying the full batch
            // and duplicating the {succeeded} already-delivered rows (#1880).
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ES bulk partial failure: {succeeded} succeeded, \
                     {retryable_failures} retryable, {permanent_failures} rejected; \
                     not retrying to prevent duplication of {succeeded} delivered rows. \
                     First error: {first_error}"
                ),
            ));
        }

        if retryable_failures > 0 && permanent_failures == 0 {
            // ALL items failed with retryable errors — safe to retry the full batch.
            return Err(io::Error::other(format!(
                "ES bulk transient error: all {retryable_failures} items failed. \
                 First error: {first_error}"
            )));
        }

        if permanent_failures > 0 {
            // All items failed with permanent errors — do not retry.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ES bulk error: all {permanent_failures} items rejected. \
                     First error: {first_error}"
                ),
            ));
        }

        // errors: true but no specific error found in items — treat as failure rather
        // than silently returning Ok (which would cause data loss by masking the error).
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ES bulk response indicated errors but no error details found in items",
        ))
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
            if payload_len > max_bytes {
                if n > 1 && depth < MAX_SPLIT_DEPTH {
                    self.batch_buf.clear(); // discard oversized payload
                    return self.send_split_halves(batch, metadata, depth).await;
                }
                // Row is too large to fit in a single bulk request even on its own.
                return Ok(super::sink::SendResult::Rejected(format!(
                    "single-row batch ({} bytes) exceeds max_bulk_bytes ({})",
                    payload_len, max_bytes
                )));
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

    /// Maximum internal retries for a split half when the other half already
    /// succeeded. Keeps the retry window short — if the transient failure
    /// persists, we return `Rejected` to prevent the worker pool from
    /// retrying the full batch and duplicating the successful half.
    const SPLIT_INTERNAL_RETRIES: usize = 2;

    /// Base delay between internal split-half retries (doubles each attempt).
    const SPLIT_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

    /// Retry a failed split half internally up to `SPLIT_INTERNAL_RETRIES`
    /// times with exponential backoff. Used when the other half already
    /// succeeded, so we cannot let the worker pool retry the full batch.
    async fn retry_split_half(
        &mut self,
        half: &RecordBatch,
        metadata: &BatchMetadata,
        depth: usize,
    ) -> io::Result<super::sink::SendResult> {
        let mut delay = Self::SPLIT_RETRY_BASE_DELAY;
        for _ in 0..Self::SPLIT_INTERNAL_RETRIES {
            tokio::time::sleep(delay).await;
            delay *= 2;
            let result = Self::classify_split_result(
                self.send_batch_inner(half, metadata, depth + 1).await,
            )?;
            match result {
                super::sink::SendResult::Ok | super::sink::SendResult::Rejected(_) => {
                    return Ok(result);
                }
                super::sink::SendResult::IoError(_) | super::sink::SendResult::RetryAfter(_) => {
                    // Continue retrying.
                }
            }
        }
        // Exhausted internal retries — the half is still failing.
        Ok(super::sink::SendResult::Rejected(
            "elasticsearch: split half failed after internal retries".to_string(),
        ))
    }

    /// Split a batch in half and send each half sequentially.
    ///
    /// **Duplication safety (#1873):** Both halves are always attempted
    /// regardless of individual outcomes. When one half succeeds but the
    /// other fails with a transient error, the failed half is retried
    /// internally (up to `SPLIT_INTERNAL_RETRIES` times) rather than
    /// propagating a retryable result to the worker pool — which would
    /// retry the entire original batch and duplicate the successful half.
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

        // Always send left half first.
        let left_result =
            Self::classify_split_result(self.send_batch_inner(&left, metadata, depth + 1).await)?;
        let left_ok = matches!(left_result, super::sink::SendResult::Ok);

        // Always send right half, even if left failed (#1873).
        let right_result =
            Self::classify_split_result(self.send_batch_inner(&right, metadata, depth + 1).await)?;
        let right_ok = matches!(right_result, super::sink::SendResult::Ok);

        // When one half succeeded and the other failed transiently, retry the
        // failed half internally rather than bubbling a retryable result to the
        // worker pool (which would re-send the successful half).
        let is_retryable = |r: &super::sink::SendResult| {
            matches!(
                r,
                super::sink::SendResult::IoError(_) | super::sink::SendResult::RetryAfter(_)
            )
        };

        match (left_ok, right_ok) {
            (true, true) => Ok(super::sink::SendResult::Ok),
            (true, false) if is_retryable(&right_result) => {
                // Left succeeded, right failed transiently — retry right internally.
                tracing::warn!(
                    left_rows = mid,
                    right_rows = n - mid,
                    "elasticsearch: left split half delivered, right failed transiently; \
                     retrying right half internally to prevent duplication"
                );
                let right_retry = self.retry_split_half(&right, metadata, depth).await?;
                if right_retry.is_ok() {
                    Ok(super::sink::SendResult::Ok)
                } else {
                    // Right exhausted retries. Return Rejected to prevent the
                    // worker pool from retrying the full batch.
                    Ok(super::sink::SendResult::Rejected(format!(
                        "elasticsearch: right split half failed after internal retry; \
                         left half ({mid} rows) was delivered successfully, \
                         right half ({} rows) could not be delivered",
                        n - mid
                    )))
                }
            }
            (false, true) if is_retryable(&left_result) => {
                // Right succeeded, left failed transiently — retry left internally.
                tracing::warn!(
                    left_rows = mid,
                    right_rows = n - mid,
                    "elasticsearch: right split half delivered, left failed transiently; \
                     retrying left half internally to prevent duplication"
                );
                let left_retry = self.retry_split_half(&left, metadata, depth).await?;
                if left_retry.is_ok() {
                    Ok(super::sink::SendResult::Ok)
                } else {
                    Ok(super::sink::SendResult::Rejected(format!(
                        "elasticsearch: left split half failed after internal retry; \
                         right half ({} rows) was delivered successfully, \
                         left half ({mid} rows) could not be delivered",
                        n - mid
                    )))
                }
            }
            _ => {
                // Both failed, or one succeeded and other was permanently rejected.
                // For the both-failed case: safe to propagate retryable since no
                // rows were delivered. For permanent rejections: merge normally.
                Ok(Self::merge_split_send_results(left_result, right_result))
            }
        }
    }

    /// Convert permanent ES rejections from `Err` to `Ok(Rejected)` so they
    /// flow through `merge_split_send_results` instead of short-circuiting via
    /// `?`. `parse_bulk_response` returns `InvalidData` for document-level
    /// errors (e.g. mapper_parsing_exception) inside an HTTP 200 response.
    /// These are terminal — retrying is futile — but as `Err` they would skip
    /// the right half of a split send.
    fn classify_split_result(
        result: io::Result<super::sink::SendResult>,
    ) -> io::Result<super::sink::SendResult> {
        match result {
            Ok(result) => Ok(result),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData
                ) =>
            {
                Ok(super::sink::SendResult::Rejected(e.to_string()))
            }
            Err(e) => Err(e),
        }
    }

    /// Merge split-half outcomes into the single-result `Sink` contract.
    ///
    /// Precedence is `IoError` > `RetryAfter` > `Rejected` > `Ok`. When both
    /// halves retry, use the longer delay. The shape intentionally mirrors
    /// fanout result reduction so mixed outcomes do not hide retryable work.
    fn merge_split_send_results(
        left: super::sink::SendResult,
        right: super::sink::SendResult,
    ) -> super::sink::SendResult {
        match (left, right) {
            (super::sink::SendResult::Ok, super::sink::SendResult::Ok) => {
                super::sink::SendResult::Ok
            }
            (super::sink::SendResult::Rejected(left), super::sink::SendResult::Rejected(right)) => {
                super::sink::SendResult::Rejected(format!(
                    "left split rejected: {left}; right split rejected: {right}"
                ))
            }
            (super::sink::SendResult::Rejected(reason), super::sink::SendResult::Ok) => {
                super::sink::SendResult::Rejected(format!("left split rejected: {reason}"))
            }
            (super::sink::SendResult::Ok, super::sink::SendResult::Rejected(reason)) => {
                super::sink::SendResult::Rejected(format!("right split rejected: {reason}"))
            }
            (
                super::sink::SendResult::RetryAfter(left),
                super::sink::SendResult::RetryAfter(right),
            ) => super::sink::SendResult::RetryAfter(left.max(right)),
            (super::sink::SendResult::IoError(error), _)
            | (_, super::sink::SendResult::IoError(error)) => {
                super::sink::SendResult::IoError(error)
            }
            (super::sink::SendResult::RetryAfter(delay), _)
            | (_, super::sink::SendResult::RetryAfter(delay)) => {
                super::sink::SendResult::RetryAfter(delay)
            }
        }
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
        tracing::Span::current().record("status_code", status.as_u16() as u64);

        // 413 is ES-specific: triggers reactive batch splitting.
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            let detail = response.text().await.unwrap_or_default();
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ES returned 413 Payload Too Large (body {body_len} bytes): {detail}"),
            ));
        }

        if !status.is_success() {
            let retry_after = response.headers().get("Retry-After").cloned();
            let detail = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            if let Some(send_result) = crate::http_classify::classify_http_status(
                status.as_u16(),
                retry_after.as_ref(),
                &format!("ES: {detail}"),
            ) {
                return Ok(send_result);
            }
            // classify_http_status handles all non-2xx; unreachable in practice.
            return Err(io::Error::other(format!("ES: HTTP {status}: {detail}")));
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
        // Fixes #1680: use the same broad check as `serialize_batch` — canonical
        // variants (`timestamp`, `time`, `ts`) must suppress synthetic @timestamp
        // injection in streaming mode too.
        let has_timestamp_col = cols.iter().any(|c| {
            field_names::matches_any(
                &c.field_name,
                field_names::TIMESTAMP,
                field_names::TIMESTAMP_VARIANTS,
            )
        });

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
        tracing::Span::current().record("status_code", status.as_u16() as u64);

        // 413 is ES-specific: triggers reactive batch splitting.
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            let detail = response.text().await.unwrap_or_default();
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "ES returned 413 Payload Too Large (streamed body {payload_len} bytes): {detail}"
                ),
            ));
        }

        if !status.is_success() {
            let retry_after = response.headers().get("Retry-After").cloned();
            let detail = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            if let Some(send_result) = crate::http_classify::classify_http_status(
                status.as_u16(),
                retry_after.as_ref(),
                &format!("ES: {detail}"),
            ) {
                return Ok(send_result);
            }
            // classify_http_status handles all non-2xx; unreachable in practice.
            return Err(io::Error::other(format!("ES: HTTP {status}: {detail}")));
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

impl super::sink::Sink for ElasticsearchSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> std::pin::Pin<Box<dyn Future<Output = super::sink::SendResult> + Send + 'a>> {
        Box::pin(async move {
            match self.send_batch_inner(batch, metadata, 0).await {
                Ok(r) => r,
                Err(e) => match e.kind() {
                    // InvalidInput: proactive-split exhausted (single row too large) or
                    // serialization error — permanent, do not retry.
                    // InvalidData: ES bulk API returned item-level errors (e.g.
                    // mapper_parsing_exception, strict_dynamic_mapping_exception) inside a
                    // 200 OK response — also permanent, retrying the same document is futile.
                    io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
                        super::sink::SendResult::Rejected(e.to_string())
                    }
                    _ => super::sink::SendResult::IoError(e),
                },
            }
        })
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

/// Creates `ElasticsearchSink` instances for the output worker pool.
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
        Self::new_with_client(
            name,
            endpoint,
            index,
            headers,
            compress,
            request_mode,
            reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(64),
            stats,
        )
    }

    #[allow(clippy::too_many_arguments)]
    /// Creates an Elasticsearch sink factory with a caller-provided HTTP client builder.
    ///
    /// The caller owns all cross-cutting HTTP client policy on `client_builder`, including
    /// request timeout, connection-pool sizing, TLS roots, mTLS identity, and certificate
    /// verification behavior. This constructor validates Elasticsearch-specific request
    /// mode and compression compatibility, then builds and reuses one `reqwest::Client`
    /// for every sink created by the factory.
    pub fn new_with_client(
        name: String,
        endpoint: String,
        index: String,
        headers: Vec<(String, String)>,
        compress: bool,
        request_mode: ElasticsearchRequestMode,
        client_builder: reqwest::ClientBuilder,
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

        let client = client_builder.build().map_err(io::Error::other)?;

        let endpoint = endpoint.trim_end_matches('/').to_string();
        let bulk_url = format!(
            "{}/_bulk?filter_path=errors,took,items.*.error,items.*.status",
            endpoint
        );

        // Pre-compute the action line bytes once so serialize_batch doesn't have
        // to allocate a String on every call.
        let escaped_index = serde_json::to_string(&index).map_err(io::Error::other)?;
        let action_line = format!("{{\"index\":{{\"_index\":{escaped_index}}}}}\n");

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
        Ok(Box::new(ElasticsearchSink::new(
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
    /// Create a concrete `ElasticsearchSink` without boxing it.
    ///
    /// Intended for benchmarks and tests that need access to
    /// `serialize_batch` or `serialized_len` directly.
    pub fn create_sink(&self) -> ElasticsearchSink {
        ElasticsearchSink::new(
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
/// Handles dates from 1970 to 9999 correctly via the Gregorian calendar.
/// Timestamps beyond year 9999 are clamped to 9999-12-31T23:59:59 to fit
/// the 19-byte ASCII buffer and comply with ISO 8601 4-digit year limit.
pub(crate) fn write_unix_timestamp_utc_into(buf: &mut [u8; 19], mut secs: u64) {
    const MAX_SECS: u64 = 253402300799; // 9999-12-31T23:59:59
    if secs > MAX_SECS {
        secs = MAX_SECS;
    }

    let days = secs / 86400;
    let time = secs % 86400;
    let h = time / 3600;
    let m = (time % 3600) / 60;
    let s = time % 60;

    let mut year = 1970u32;
    let mut remaining = days;

    // Jump in 400-year blocks (146097 days) to ensure O(1) performance
    // regardless of how large the timestamp is.
    let blocks400 = remaining / 146097;
    year += blocks400 as u32 * 400;
    remaining %= 146097;

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

#[cfg(test)]
fn write_ts_suffix_simple(secs: u64, frac: u64) -> Vec<u8> {
    let secs = secs.min(253402300799); // 9999-12-31T23:59:59
    let ts = format_unix_timestamp_utc(secs);
    format!(",\"@timestamp\":\"{ts}.{frac:09}Z\"}}").into_bytes()
}

pub(crate) fn is_leap_year(y: u32) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use proptest::prelude::*;
    use proptest::string::string_regex;
    use std::sync::{Arc, OnceLock};

    type RandomRow = (
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<f64>,
        Option<bool>,
    );

    fn zero_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 0,
        }
    }

    fn shared_test_client() -> reqwest::Client {
        static TEST_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
        TEST_CLIENT.get_or_init(reqwest::Client::new).clone()
    }

    fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(any::<char>(), 0..=max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn make_test_sink(index: &str) -> ElasticsearchSink {
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
        let client = shared_test_client();
        ElasticsearchSink::new(
            "test".to_string(),
            Arc::clone(&factory.config),
            client,
            Arc::new(ComponentStats::default()),
        )
    }

    fn serialize_batch_simple_for_test(
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        index: &str,
    ) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        if batch.num_rows() == 0 {
            return Ok(out);
        }

        let escaped_index = serde_json::to_string(index).map_err(io::Error::other)?;
        let action_line = format!("{{\"index\":{{\"_index\":{escaped_index}}}}}\n");

        let ts_nanos = metadata.observed_time_ns;
        let ts_secs = ts_nanos / 1_000_000_000;
        let ts_frac = ts_nanos % 1_000_000_000;
        let ts_text = format!("{}.{ts_frac:09}Z", format_unix_timestamp_utc(ts_secs));

        let cols = build_col_infos(batch);
        let has_timestamp_col = cols.iter().any(|c| {
            field_names::matches_any(
                &c.field_name,
                field_names::TIMESTAMP,
                field_names::TIMESTAMP_VARIANTS,
            )
        });

        for row in 0..batch.num_rows() {
            out.extend_from_slice(action_line.as_bytes());

            let doc_start = out.len();
            write_row_json(batch, row, &cols, &mut out)?;
            out.push(b'\n');

            if !has_timestamp_col {
                if !out.ends_with(b"}\n") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "serialize_batch_simple_for_test: JSON doc must end with }\\n",
                    ));
                }
                let trim_to = out.len() - 2;
                if trim_to == doc_start + 1 {
                    out.truncate(doc_start);
                    let suffix = format!("{{\"@timestamp\":\"{ts_text}\"}}");
                    out.extend_from_slice(suffix.as_bytes());
                } else {
                    out.truncate(trim_to);
                    let suffix = format!(",\"@timestamp\":\"{ts_text}\"}}");
                    out.extend_from_slice(suffix.as_bytes());
                }
                out.push(b'\n');
            }
        }

        Ok(out)
    }

    fn parse_json_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
        if bytes.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut lines = bytes.split(|b| *b == b'\n').peekable();
        while let Some(line) = lines.next() {
            if line.is_empty() {
                assert!(
                    lines.peek().is_none(),
                    "ndjson must not contain interior blank lines"
                );
                continue;
            }
            out.push(serde_json::from_slice::<serde_json::Value>(line).expect("valid ndjson line"));
        }
        out
    }

    fn build_random_batch(rows: &[RandomRow], include_timestamp: bool) -> RecordBatch {
        let mut fields = Vec::new();
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        if include_timestamp {
            fields.push(Field::new(field_names::TIMESTAMP_AT, DataType::Utf8, true));
            let ts: Vec<Option<String>> = (0..rows.len())
                .map(|i| {
                    if i % 3 == 0 {
                        None
                    } else {
                        Some(format!("2026-04-08T12:00:{:02}.123456789Z", i % 60))
                    }
                })
                .collect();
            columns.push(Arc::new(StringArray::from(
                ts.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
            )));
        }

        fields.push(Field::new("level", DataType::Utf8, true));
        fields.push(Field::new("message", DataType::Utf8, true));
        fields.push(Field::new("status", DataType::Int64, true));
        fields.push(Field::new("duration_ms", DataType::Float64, true));
        fields.push(Field::new("active", DataType::Boolean, true));

        let levels: Vec<Option<String>> = rows.iter().map(|r| r.0.clone()).collect();
        let messages: Vec<Option<String>> = rows.iter().map(|r| r.1.clone()).collect();
        let statuses: Vec<Option<i64>> = rows.iter().map(|r| r.2).collect();
        let durations: Vec<Option<f64>> = rows.iter().map(|r| r.3).collect();
        let active: Vec<Option<bool>> = rows.iter().map(|r| r.4).collect();

        columns.push(Arc::new(StringArray::from(
            levels.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(StringArray::from(
            messages.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(statuses)));
        columns.push(Arc::new(arrow::array::Float64Array::from(durations)));
        columns.push(Arc::new(arrow::array::BooleanArray::from(active)));

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid random batch")
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

    /// Regression test: batches with a canonical timestamp column name (`timestamp`,
    /// `time`, `ts`) must NOT have a synthetic `@timestamp` injected.
    ///
    /// Before the fix, `has_timestamp_col` only checked `@timestamp`/`_timestamp`,
    /// so a column named `timestamp`, `time`, or `ts` would cause two timestamp
    /// fields in the ES document — the user's original field plus the injected
    /// `@timestamp` derived from `metadata.observed_time_ns`.
    #[test]
    fn canonical_timestamp_variants_suppress_at_timestamp_injection() {
        for col_name in &["timestamp", "time", "ts"] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("msg", DataType::Utf8, false),
                Field::new(*col_name, DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["hello"])),
                    Arc::new(StringArray::from(vec!["2024-01-01T00:00:00Z"])),
                ],
            )
            .expect("batch creation failed");

            let mut sink = make_test_sink("logs");
            let meta = zero_metadata();
            sink.serialize_batch(&batch, &meta)
                .expect("serialize failed");
            let output = String::from_utf8_lossy(&sink.batch_buf);
            let lines: Vec<&str> = output.lines().collect();
            // doc line is index 1
            assert!(
                !lines[1].contains(r#""@timestamp""#),
                "column '{}': expected no @timestamp injection but got: {}",
                col_name,
                lines[1]
            );
        }
    }

    /// Regression test for issue #1680.
    ///
    /// `serialize_batch_streaming` used a narrow `has_timestamp_col` check that
    /// only matched `@timestamp`/`_timestamp`.  A column named `timestamp`, `time`,
    /// or `ts` caused a spurious `@timestamp` injection alongside the user's column.
    #[test]
    fn streaming_canonical_timestamp_variants_suppress_at_timestamp_injection() {
        use std::sync::atomic::AtomicU64;

        // Build a minimal streaming config.
        let factory = ElasticsearchSinkFactory::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "test-index".to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Streaming,
            Arc::new(ComponentStats::default()),
        )
        .expect("factory creation failed");
        let config = Arc::clone(&factory.config);

        for col_name in &["timestamp", "time", "ts"] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("msg", DataType::Utf8, false),
                Field::new(*col_name, DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["hello"])),
                    Arc::new(StringArray::from(vec!["2024-01-01T00:00:00Z"])),
                ],
            )
            .expect("batch creation failed");

            // We need a tokio runtime to drive the mpsc channel.
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("tokio runtime");
            let (tx, mut rx) = mpsc::channel::<io::Result<Vec<u8>>>(16);
            let emitted = Arc::new(AtomicU64::new(0));

            rt.block_on(async {
                let config_clone = Arc::clone(&config);
                let batch_clone = batch.clone();
                let meta = BatchMetadata {
                    resource_attrs: Arc::new(vec![]),
                    observed_time_ns: 0,
                };
                let tx_clone = tx.clone();
                drop(tx); // ensure channel closes when producer is done
                tokio::task::spawn_blocking(move || {
                    ElasticsearchSink::serialize_batch_streaming(
                        batch_clone,
                        meta,
                        config_clone,
                        tx_clone,
                        emitted,
                    )
                    .expect("serialize_batch_streaming should not error");
                })
                .await
                .expect("task must not panic");

                // Collect all chunks.
                let mut output = Vec::new();
                while let Some(chunk) = rx.recv().await {
                    output.extend_from_slice(&chunk.expect("no io error"));
                }

                let doc_line = String::from_utf8_lossy(&output)
                    .lines()
                    .nth(1)
                    .unwrap_or("")
                    .to_string();
                assert!(
                    !doc_line.contains(r#""@timestamp""#),
                    "streaming column '{}': expected no @timestamp injection but got: {}",
                    col_name,
                    doc_line
                );
            });
        }
    }

    #[test]
    fn parse_bulk_response_success() {
        let response = br#"{"took":5,"errors":false,"items":[{"index":{"_id":"1","status":201}}]}"#;
        ElasticsearchSink::parse_bulk_response(response).expect("should not error on success");
    }

    proptest! {
        #[test]
        fn proptest_serialize_batch_fast_matches_simple(
            rows in proptest::collection::vec(
                (
                    prop::option::of(unicode_string(8)),
                    prop::option::of(unicode_string(32)),
                    prop::option::of(any::<i64>()),
                    prop::option::of((-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 10.0)),
                    prop::option::of(any::<bool>()),
                ),
                0..40
            ),
            include_timestamp in any::<bool>(),
            index in string_regex("[A-Za-z0-9_.-]{1,16}").expect("regex"),
            observed_time_ns in any::<u64>()
        ) {
            let batch = build_random_batch(&rows, include_timestamp);
            let metadata = BatchMetadata {
                resource_attrs: Arc::new(vec![]),
                observed_time_ns,
            };

            let mut sink = make_test_sink(index.as_str());
            sink.serialize_batch(&batch, &metadata).expect("fast serialize must succeed");
            let simple = serialize_batch_simple_for_test(&batch, &metadata, index.as_str())
                .expect("simple serialize must succeed");

            prop_assert_eq!(
                parse_json_lines(&sink.batch_buf),
                parse_json_lines(&simple),
                "serialize_batch fast path drifted from simple reference"
            );
        }
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
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("should error on bulk failure");
        // Mixed result: 1 succeeded + 1 rejected → InvalidData to prevent duplication (#1880).
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "mixed success/failure must be permanent to prevent duplication"
        );
        assert!(
            err.to_string().contains("partial failure"),
            "error should mention partial failure: {err}"
        );
        assert!(err.to_string().contains("mapper_parsing_exception"));
    }

    #[test]
    fn parse_bulk_response_retryable_item_error_is_transient() {
        // All items failed with 429 (no successes) — safe to retry the full batch.
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("status 429 bulk item error must be transient");
        assert_eq!(
            err.kind(),
            io::ErrorKind::Other,
            "all-429 item-level errors should be retried"
        );
    }

    #[test]
    fn parse_bulk_response_status_only_retryable_item_error_is_transient() {
        // All items failed with status-only 429 (no successes) — safe to retry.
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"status":429}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("status-only 429 bulk item error must be transient");
        assert_eq!(
            err.kind(),
            io::ErrorKind::Other,
            "status-only 429 item-level errors should be retried"
        );
    }

    #[test]
    fn parse_bulk_response_status_only_permanent_item_error_is_invalid_data() {
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"status":400}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("status-only 400 bulk item error must be permanent");
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "status-only 400 item-level errors should be rejected"
        );
    }

    /// Regression test for issue #1675.
    ///
    /// `parse_bulk_response` used to return `Ok(())` when `errors:true` but no
    /// parseable error could be found in `items[]`.  The batch would then be
    /// silently treated as successfully delivered.
    #[test]
    fn parse_bulk_errors_true_without_parseable_error_returns_err() {
        // Simulate a malformed ES response where errors:true but no item has
        // an "error" key — the path that previously fell through to Ok(()).
        let response = br#"{"took":5,"errors":true,"items":[{"index":{"status":200}}]}"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("errors:true with no parseable error must return Err");
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "errors:true must be classified as InvalidData so send_batch rejects it"
        );
        assert!(
            err.to_string().contains("no error details found"),
            "error message should mention no error details: {err}"
        );
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
        ElasticsearchSink::parse_bulk_response(response).expect("empty items should succeed");
    }

    #[test]
    fn parse_bulk_response_malformed_json_is_error() {
        ElasticsearchSink::parse_bulk_response(b"not valid json")
            .expect_err("malformed json should be an error");
    }

    #[test]
    fn parse_bulk_response_errors_false_does_not_error() {
        // errors:false means success even if items have non-200 status
        let response = br#"{"took":1,"errors":false,"items":[{"index":{"_id":"1","status":200}}]}"#;
        ElasticsearchSink::parse_bulk_response(response).expect("errors:false must succeed");
    }

    #[test]
    fn test_index_name_escaping() {
        let index = "logs\"-and-\\backslashes";
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

        let action_line = std::str::from_utf8(&factory.config.action_bytes).unwrap();
        // Should be: {"index":{"_index":"logs\"-and-\\backslashes"}}\n
        assert!(
            action_line.contains(r#"_index":"logs\"-and-\\backslashes""#),
            "got: {}",
            action_line
        );
    }

    #[test]
    fn test_timestamp_extreme_values() {
        // Year 10000+: 253402300800 seconds is 10000-01-01T00:00:00
        let secs_y10k = 253402300800;
        let ts = format_unix_timestamp_utc(secs_y10k);
        // Current implementation will either produce "10000-..." (corrupting the 19-byte buf)
        // or debug_panic. We want it to clamp to 9999-12-31T23:59:59.
        assert_eq!(ts, "9999-12-31T23:59:59");

        // u64::MAX should not hang (regression for #1087)
        let _ = format_unix_timestamp_utc(u64::MAX);
    }

    #[test]
    fn test_parse_bulk_response_whitespace() {
        // Bug #1095: parser misses errors if there's whitespace
        let response = br#" { "took": 5, "errors" :  true , "items": [] } "#;
        ElasticsearchSink::parse_bulk_response(response)
            .expect_err("should detect errors:true with whitespace");
    }

    #[test]
    fn test_extract_took_whitespace() {
        let response = br#" { "took" :  123 , "errors": false } "#;
        assert_eq!(ElasticsearchSink::extract_took(response), Some(123));
    }

    proptest! {
        #[test]
        fn proptest_write_ts_suffix_fast_matches_simple(
            secs in any::<u64>(),
            frac in 0u64..1_000_000_000u64
        ) {
            let mut fast = [0u8; 47];
            write_ts_suffix(&mut fast, secs, frac);
            let simple = write_ts_suffix_simple(secs, frac);
            prop_assert_eq!(fast.as_slice(), simple.as_slice());
        }
    }

    /// Local microbenchmark for timestamp suffix writer.
    ///
    /// Run with:
    /// `cargo test -p logfwd-output elasticsearch::tests::bench_write_ts_suffix_fast_vs_simple --release -- --ignored --nocapture`
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_write_ts_suffix_fast_vs_simple() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 300_000;
        let inputs: Vec<(u64, u64)> = (0..N)
            .map(|i| {
                let secs = (i as u64).wrapping_mul(2654435761) % (253402300799 + 5000);
                let frac = (i as u64).wrapping_mul(11400714819323198485u64) % 1_000_000_000;
                (secs, frac)
            })
            .collect();

        let t0 = Instant::now();
        for &(secs, frac) in &inputs {
            let mut out = [0u8; 47];
            write_ts_suffix(&mut out, secs, frac);
            black_box(out);
        }
        let fast = t0.elapsed();

        let t0 = Instant::now();
        for &(secs, frac) in &inputs {
            let out = write_ts_suffix_simple(secs, frac);
            black_box(out.len());
        }
        let simple = t0.elapsed();

        eprintln!("ts suffix bench N={N}");
        eprintln!("  fast={:?}", fast);
        eprintln!("  simple={:?}", simple);
    }

    #[test]
    fn test_parse_bulk_response_malformed_is_error() {
        // Bug #1094: malformed bodies treated as success
        ElasticsearchSink::parse_bulk_response(b"{}")
            .expect_err("missing errors field should be an error");
        ElasticsearchSink::parse_bulk_response(b"not json")
            .expect_err("malformed json should be an error");
    }

    // Regression test for issue #1675: when errors:true but no item has an "error" key,
    // the function must return Err rather than Ok (which would silently mask the failure).
    #[test]
    fn parse_bulk_response_errors_true_no_error_key_is_error() {
        // ES says errors:true but all items look successful (no "error" key).
        // This can happen with malformed/truncated responses or unexpected ES formats.
        let response = br#"{"took":1,"errors":true,"items":[{"index":{"_id":"1","status":200}}]}"#;
        ElasticsearchSink::parse_bulk_response(response)
            .expect_err("errors:true must return Err even when no item has an 'error' key");
    }

    /// Local microbenchmark for serialize_batch fast path vs simple baseline.
    ///
    /// Run with:
    /// `cargo test -p logfwd-output elasticsearch::tests::bench_serialize_batch_fast_vs_simple --release -- --ignored --nocapture`
    #[test]
    #[ignore = "microbenchmark"]
    fn bench_serialize_batch_fast_vs_simple() {
        use std::hint::black_box;
        use std::time::Instant;

        let rows: Vec<RandomRow> = (0..2_000)
            .map(|i| {
                (
                    Some(match i % 4 {
                        0 => "INFO".to_string(),
                        1 => "WARN".to_string(),
                        2 => "ERROR".to_string(),
                        _ => "DEBUG".to_string(),
                    }),
                    Some(format!("request-{i}-payload")),
                    Some(200 + (i % 7) as i64),
                    Some((i % 1000) as f64 / 10.0),
                    Some(i % 3 != 0),
                )
            })
            .collect();
        let batch = build_random_batch(&rows, false);
        let metadata = BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 1_710_000_000_123_456_789,
        };
        let index = "bench-index";

        let mut sink = make_test_sink(index);
        sink.serialize_batch(&batch, &metadata).unwrap();
        let simple_once = serialize_batch_simple_for_test(&batch, &metadata, index).unwrap();
        assert_eq!(
            parse_json_lines(&sink.batch_buf),
            parse_json_lines(&simple_once),
            "fast and simple serializers must remain semantically equivalent"
        );

        const ITERS: usize = 120;
        let t0 = Instant::now();
        for _ in 0..ITERS {
            sink.serialize_batch(&batch, &metadata).unwrap();
            black_box(sink.serialized_len());
        }
        let fast = t0.elapsed();

        let t0 = Instant::now();
        for _ in 0..ITERS {
            let out = serialize_batch_simple_for_test(&batch, &metadata, index).unwrap();
            black_box(out.len());
        }
        let simple = t0.elapsed();

        eprintln!(
            "serialize_batch bench rows={} iters={ITERS}",
            batch.num_rows()
        );
        eprintln!("  fast={:?}", fast);
        eprintln!("  simple={:?}", simple);
    }

    #[test]
    fn test_send_batch_oversized_single_row() {
        use crate::sink::Sink;
        let factory = ElasticsearchSinkFactory::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            Arc::new(ComponentStats::default()),
        )
        .unwrap();
        let mut sink = factory.create_sink();

        // Use a row large enough to exceed 5MB default limit.
        let large_str = "A".repeat(5 * 1024 * 1024 + 1024);
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![large_str]))])
                .unwrap();

        let meta = zero_metadata();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(sink.send_batch(&batch, &meta));
        match result {
            crate::sink::SendResult::Rejected(msg) => {
                assert!(msg.contains("exceeds max_bulk_bytes"));
            }
            _ => panic!("Expected Rejected, got {:?}", result),
        }
    }

    /// Regression test for #1212 (ES sink only):
    /// ES bulk item errors (InvalidData from parse_bulk_response) must map to
    /// `SendResult::Rejected`, not `SendResult::IoError`.  Before the fix the
    /// `send_batch` error classifier only caught `InvalidInput`, so `InvalidData`
    /// fell through to `IoError` and the worker pool would retry indefinitely.
    #[test]
    fn parse_bulk_item_error_maps_to_rejected_not_io_error() {
        // parse_bulk_response returns Err(InvalidData) when the bulk response
        // contains item-level errors even inside a 200 OK.  Confirm the error
        // kind is InvalidData so the send_batch classifier can reject it.
        // All items failed permanently (no successes).
        let response = br#"{
            "took":3,
            "errors":true,
            "items":[
                {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [ts]"},"status":400}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("parse_bulk_response must fail on item error");
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "bulk item errors must use InvalidData kind so send_batch maps them to Rejected"
        );
        assert!(err.to_string().contains("mapper_parsing_exception"));

        // Verify send_batch converts InvalidData -> Rejected (not IoError).
        // We exercise this through the classifier arm directly to avoid needing
        // a live HTTP server.
        let classified = match err.kind() {
            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
                crate::sink::SendResult::Rejected(err.to_string())
            }
            _ => crate::sink::SendResult::IoError(err),
        };
        match classified {
            crate::sink::SendResult::Rejected(msg) => {
                assert!(msg.contains("mapper_parsing_exception"), "got: {msg}");
            }
            other => {
                panic!("ES bulk item error must be Rejected, not retried as IoError; got {other:?}")
            }
        }
    }

    #[test]
    fn split_result_preserves_rejection_when_other_half_is_ok() {
        let result = ElasticsearchSink::merge_split_send_results(
            crate::sink::SendResult::Rejected("left bad doc".to_string()),
            crate::sink::SendResult::Ok,
        );

        match result {
            crate::sink::SendResult::Rejected(reason) => {
                assert!(reason.contains("left split rejected"), "got: {reason}");
                assert!(reason.contains("left bad doc"), "got: {reason}");
            }
            other => panic!("expected rejected split result, got {other:?}"),
        }
    }

    #[test]
    fn split_result_combines_two_terminal_rejections() {
        let result = ElasticsearchSink::merge_split_send_results(
            crate::sink::SendResult::Rejected("left bad doc".to_string()),
            crate::sink::SendResult::Rejected("right bad doc".to_string()),
        );

        match result {
            crate::sink::SendResult::Rejected(reason) => {
                assert!(reason.contains("left bad doc"), "got: {reason}");
                assert!(reason.contains("right bad doc"), "got: {reason}");
            }
            other => panic!("expected rejected split result, got {other:?}"),
        }
    }

    #[test]
    fn split_result_keeps_retryable_result_visible() {
        let delay = Duration::from_secs(3);
        let result = ElasticsearchSink::merge_split_send_results(
            crate::sink::SendResult::Rejected("left bad doc".to_string()),
            crate::sink::SendResult::RetryAfter(delay),
        );

        match result {
            crate::sink::SendResult::RetryAfter(actual) => assert_eq!(actual, delay),
            other => panic!("expected retryable split result, got {other:?}"),
        }
    }

    #[test]
    fn split_result_io_error_takes_precedence_over_retry() {
        let result = ElasticsearchSink::merge_split_send_results(
            crate::sink::SendResult::IoError(io::Error::other("network")),
            crate::sink::SendResult::RetryAfter(Duration::from_secs(3)),
        );

        assert!(
            matches!(result, crate::sink::SendResult::IoError(_)),
            "expected io error to take precedence, got {result:?}"
        );
    }

    #[test]
    fn classify_split_result_converts_invalid_data_to_rejected() {
        let err = io::Error::new(io::ErrorKind::InvalidData, "mapper_parsing_exception");
        let result = ElasticsearchSink::classify_split_result(Err(err));
        match result {
            Ok(crate::sink::SendResult::Rejected(msg)) => {
                assert!(msg.contains("mapper_parsing_exception"), "got: {msg}");
            }
            other => panic!("expected Ok(Rejected), got {other:?}"),
        }
    }

    #[test]
    fn classify_split_result_converts_invalid_input_to_rejected() {
        let err = io::Error::new(io::ErrorKind::InvalidInput, "413 Payload Too Large");
        let result = ElasticsearchSink::classify_split_result(Err(err));
        match result {
            Ok(crate::sink::SendResult::Rejected(msg)) => {
                assert!(msg.contains("413 Payload Too Large"), "got: {msg}");
            }
            other => panic!("expected Ok(Rejected), got {other:?}"),
        }
    }

    #[test]
    fn classify_split_result_preserves_io_errors() {
        let err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
        let result = ElasticsearchSink::classify_split_result(Err(err));
        assert!(result.is_err(), "expected Err for IO error");
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn classify_split_result_passes_through_ok() {
        let result = ElasticsearchSink::classify_split_result(Ok(crate::sink::SendResult::Ok));
        match result {
            Ok(crate::sink::SendResult::Ok) => {}
            other => panic!("expected Ok(Ok), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn split_rejection_on_left_still_sends_right_half() {
        use crate::sink::Sink;

        let mut server = mockito::Server::new_async().await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
        )
        .expect("test batch should be valid");
        let metadata = zero_metadata();

        let mut sizing_sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", usize::MAX),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );
        sizing_sink
            .serialize_batch(&batch, &metadata)
            .expect("full batch should serialize");
        let full_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(0, 1), &metadata)
            .expect("left half should serialize");
        let left_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(1, 1), &metadata)
            .expect("right half should serialize");
        let right_len = sizing_sink.batch_buf.len();
        let split_threshold = left_len.max(right_len) + 1;
        assert!(full_len > split_threshold);

        let left_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("left-row".to_string()))
            .with_status(400)
            .with_body("left bad doc")
            .create_async()
            .await;
        let right_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("right-row".to_string()))
            .with_status(200)
            .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
            .create_async()
            .await;

        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", split_threshold),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );

        let result = sink.send_batch(&batch, &metadata).await;
        match result {
            crate::sink::SendResult::Rejected(reason) => {
                assert!(reason.contains("left split rejected"), "got: {reason}");
                assert!(reason.contains("left bad doc"), "got: {reason}");
            }
            other => panic!("expected left rejection after right half send, got {other:?}"),
        }
        left_mock.assert_async().await;
        right_mock.assert_async().await;
    }

    /// Regression test for the CodeRabbit review on #2267:
    /// When the left split half returns HTTP 200 with `errors: true` (permanent
    /// item rejection via `parse_bulk_response` → `InvalidData`), the right
    /// half must still be attempted.  Before `classify_split_result`, the `?`
    /// operator propagated the `Err(InvalidData)` and skipped the right half.
    #[tokio::test]
    async fn split_bulk_item_error_on_left_still_sends_right_half() {
        use crate::sink::Sink;

        let mut server = mockito::Server::new_async().await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
        )
        .expect("test batch should be valid");
        let metadata = zero_metadata();

        // Measure serialized sizes to pick a split_threshold that forces splitting.
        let mut sizing_sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", usize::MAX),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );
        sizing_sink
            .serialize_batch(&batch, &metadata)
            .expect("full batch should serialize");
        let full_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(0, 1), &metadata)
            .expect("left half should serialize");
        let left_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(1, 1), &metadata)
            .expect("right half should serialize");
        let right_len = sizing_sink.batch_buf.len();
        let split_threshold = left_len.max(right_len) + 1;
        assert!(full_len > split_threshold);

        // Left half: HTTP 200 with errors:true — permanent item rejection.
        // parse_bulk_response returns Err(InvalidData) for this.
        let left_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("left-row".to_string()))
            .with_status(200)
            .with_body(
                r#"{"took":1,"errors":true,"items":[{"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [ts]"},"status":400}}]}"#,
            )
            .create_async()
            .await;
        // Right half: success.
        let right_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("right-row".to_string()))
            .with_status(200)
            .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
            .create_async()
            .await;

        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", split_threshold),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );

        let result = sink.send_batch(&batch, &metadata).await;
        match result {
            crate::sink::SendResult::Rejected(reason) => {
                assert!(
                    reason.contains("left split rejected"),
                    "expected left split label, got: {reason}"
                );
                assert!(
                    reason.contains("mapper_parsing_exception"),
                    "expected ES error type, got: {reason}"
                );
            }
            other => panic!(
                "expected Rejected from left bulk item error after right half send, got {other:?}"
            ),
        }
        // Both mocks must have been hit — the right half was not skipped.
        left_mock.assert_async().await;
        right_mock.assert_async().await;
    }

    // -----------------------------------------------------------------------
    // Bug #1873: split-half retry duplication prevention tests
    // -----------------------------------------------------------------------

    /// When left succeeds but right gets a transient error (429), the ES sink
    /// must retry the right half internally rather than returning a retryable
    /// result to the worker pool (which would re-send the already-delivered
    /// left half).
    #[tokio::test]
    async fn split_left_ok_right_retry_does_not_duplicate() {
        use crate::sink::Sink;

        let mut server = mockito::Server::new_async().await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
        )
        .expect("test batch should be valid");
        let metadata = zero_metadata();

        // Measure serialized sizes to pick a threshold that forces splitting.
        let mut sizing_sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", usize::MAX),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );
        sizing_sink
            .serialize_batch(&batch, &metadata)
            .expect("full batch should serialize");
        let full_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(0, 1), &metadata)
            .expect("left half should serialize");
        let left_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(1, 1), &metadata)
            .expect("right half should serialize");
        let right_len = sizing_sink.batch_buf.len();
        let split_threshold = left_len.max(right_len) + 1;
        assert!(full_len > split_threshold);

        // Left half: success.
        let left_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("left-row".to_string()))
            .with_status(200)
            .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
            .expect(1) // left must be sent exactly once — no duplication
            .create_async()
            .await;
        // Right half: first attempt returns 429, internal retry succeeds.
        let right_fail_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("right-row".to_string()))
            .with_status(429)
            .with_body("too many requests")
            .expect(1) // first attempt fails
            .create_async()
            .await;
        let right_ok_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("right-row".to_string()))
            .with_status(200)
            .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
            .expect(1) // internal retry succeeds
            .create_async()
            .await;

        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", split_threshold),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );

        let result = sink.send_batch(&batch, &metadata).await;
        assert!(
            matches!(result, crate::sink::SendResult::Ok),
            "expected Ok after internal retry of right half, got {result:?}"
        );
        // Left must be sent exactly once — no duplication.
        left_mock.assert_async().await;
        right_fail_mock.assert_async().await;
        right_ok_mock.assert_async().await;
    }

    /// When left succeeds and right exhausts all internal retries, the result
    /// must be `Rejected` (not `IoError` or `RetryAfter`) to prevent the
    /// worker pool from retrying the full batch and duplicating the left half.
    #[tokio::test]
    async fn split_left_ok_right_exhausted_returns_rejected() {
        use crate::sink::Sink;

        let mut server = mockito::Server::new_async().await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
        )
        .expect("test batch should be valid");
        let metadata = zero_metadata();

        let mut sizing_sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", usize::MAX),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );
        sizing_sink
            .serialize_batch(&batch, &metadata)
            .expect("full batch should serialize");
        let full_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(0, 1), &metadata)
            .expect("left half should serialize");
        let left_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(1, 1), &metadata)
            .expect("right half should serialize");
        let right_len = sizing_sink.batch_buf.len();
        let split_threshold = left_len.max(right_len) + 1;
        assert!(full_len > split_threshold);

        // Left half: success.
        let left_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("left-row".to_string()))
            .with_status(200)
            .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
            .expect(1) // left must be sent exactly once
            .create_async()
            .await;
        // Right half: always fails with 429 (initial + SPLIT_INTERNAL_RETRIES).
        let right_mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .match_body(mockito::Matcher::Regex("right-row".to_string()))
            .with_status(429)
            .with_body("too many requests")
            .expect_at_least(2) // initial + at least 1 retry
            .create_async()
            .await;

        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", split_threshold),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );

        let result = sink.send_batch(&batch, &metadata).await;
        match result {
            crate::sink::SendResult::Rejected(reason) => {
                assert!(
                    reason.contains("failed after internal retry"),
                    "expected internal retry exhaustion message, got: {reason}"
                );
                assert!(
                    reason.contains("delivered successfully") || reason.contains("was delivered"),
                    "should warn that left half was already delivered: {reason}"
                );
            }
            other => panic!(
                "expected Rejected to prevent worker-pool-level retry duplication, got {other:?}"
            ),
        }
        left_mock.assert_async().await;
        right_mock.assert_async().await;
    }

    // -----------------------------------------------------------------------
    // Bug #1880: bulk partial success duplication prevention tests
    // -----------------------------------------------------------------------

    /// When an ES bulk response contains mixed results (some 200, some 429),
    /// `parse_bulk_response` must return a permanent error to prevent the
    /// worker pool from retrying already-accepted rows.
    #[test]
    fn bulk_partial_success_returns_permanent_error() {
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"_id":"1","status":201}},
                {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}},
                {"index":{"_id":"3","status":201}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("mixed success/failure must be an error");
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "mixed results must be permanent to prevent duplication of 2 delivered rows"
        );
        assert!(
            err.to_string().contains("partial failure"),
            "error should mention partial failure: {err}"
        );
        assert!(
            err.to_string().contains("2 succeeded"),
            "error should report succeeded count: {err}"
        );
        assert!(
            err.to_string().contains("1 retryable"),
            "error should report retryable count: {err}"
        );
    }

    /// When ALL items in the bulk response failed with retryable errors
    /// (429/5xx), `parse_bulk_response` should return a transient error
    /// since it's safe to retry the full batch (no rows were accepted).
    #[test]
    fn bulk_all_retryable_returns_transient() {
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}},
                {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("all-retryable must be an error");
        assert_eq!(
            err.kind(),
            io::ErrorKind::Other,
            "all-retryable failures should be transient (safe to retry full batch)"
        );
    }

    /// When ALL items in the bulk response failed with permanent errors
    /// (4xx non-429), `parse_bulk_response` should return a permanent error.
    #[test]
    fn bulk_all_permanent_returns_rejected() {
        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}},
                {"index":{"error":{"type":"strict_dynamic_mapping_exception","reason":"unmapped field"},"status":400}}
            ]
        }"#;
        let err = ElasticsearchSink::parse_bulk_response(response)
            .expect_err("all-permanent must be an error");
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "all-permanent failures should be rejected"
        );
        assert!(
            err.to_string().contains("2 items rejected"),
            "error should report count: {err}"
        );
    }

    /// When both split halves fail with transient errors (no rows delivered),
    /// the merged result should still be retryable since it's safe for the
    /// worker pool to retry the full batch.
    #[tokio::test]
    async fn split_both_halves_fail_returns_retryable() {
        use crate::sink::Sink;

        let mut server = mockito::Server::new_async().await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
        )
        .expect("test batch should be valid");
        let metadata = zero_metadata();

        let mut sizing_sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", usize::MAX),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );
        sizing_sink
            .serialize_batch(&batch, &metadata)
            .expect("full batch should serialize");
        let full_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(0, 1), &metadata)
            .expect("left half should serialize");
        let left_len = sizing_sink.batch_buf.len();
        sizing_sink
            .serialize_batch(&batch.slice(1, 1), &metadata)
            .expect("right half should serialize");
        let right_len = sizing_sink.batch_buf.len();
        let split_threshold = left_len.max(right_len) + 1;
        assert!(full_len > split_threshold);

        // Both halves fail with 503.
        let _mock = server
            .mock("POST", "/_bulk")
            .match_query(mockito::Matcher::Any)
            .with_status(503)
            .with_body("service unavailable")
            .expect(2) // both halves attempted
            .create_async()
            .await;

        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            test_es_config(&server.url(), "logs", split_threshold),
            reqwest::Client::new(),
            Arc::new(ComponentStats::default()),
        );

        let result = sink.send_batch(&batch, &metadata).await;
        assert!(
            matches!(
                result,
                crate::sink::SendResult::IoError(_) | crate::sink::SendResult::RetryAfter(_)
            ),
            "both halves failed — safe to return retryable for worker pool retry, got {result:?}"
        );
    }

    fn test_es_config(
        endpoint: &str,
        index: &str,
        max_bulk_bytes: usize,
    ) -> Arc<ElasticsearchConfig> {
        let escaped_index = serde_json::to_string(index).expect("test index should serialize");
        Arc::new(ElasticsearchConfig {
            endpoint: endpoint.to_string(),
            headers: Vec::new(),
            compress: false,
            request_mode: ElasticsearchRequestMode::Buffered,
            max_bulk_bytes,
            stream_chunk_bytes: 64 * 1024,
            bulk_url: format!(
                "{}/_bulk?filter_path=errors,took,items.*.error,items.*.status",
                endpoint.trim_end_matches('/')
            ),
            action_bytes: format!("{{\"index\":{{\"_index\":{escaped_index}}}}}\n")
                .into_bytes()
                .into_boxed_slice(),
        })
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

    fn make_test_sink() -> ElasticsearchSink {
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
        ElasticsearchSink::new(
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
