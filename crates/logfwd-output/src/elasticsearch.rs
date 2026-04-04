use std::io;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

use super::{BatchMetadata, build_col_infos, write_row_json};

// ---------------------------------------------------------------------------
// ElasticsearchAsyncSink — reqwest-based async implementation of Sink
// ---------------------------------------------------------------------------

/// Configuration shared across all `ElasticsearchAsyncSink` instances from
/// the same factory.
pub(crate) struct ElasticsearchConfig {
    endpoint: String,
    index: String,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
    compress: bool,
    /// Maximum uncompressed bulk payload size in bytes. Batches that serialize
    /// larger than this are split in half and sent as separate `_bulk` requests.
    /// Default: 5 MiB — safe for Elasticsearch Serverless and self-hosted.
    max_bulk_bytes: usize,
    /// Precomputed `_bulk` URL with `filter_path` to avoid per-request allocation.
    bulk_url: String,
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
            batch_buf: Vec::with_capacity(64 * 1024),
            stats,
        }
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

        let action_line = format!("{{\"index\":{{\"_index\":\"{}\"}}}}\n", self.config.index);
        let action_bytes = action_line.as_bytes();

        // Derive @timestamp from metadata (ISO-8601 UTC).
        let ts_nanos = metadata.observed_time_ns;
        let ts_secs = ts_nanos / 1_000_000_000;
        let ts_frac = ts_nanos % 1_000_000_000;
        let ts_str = format!(
            ",\"@timestamp\":\"{}.{:09}Z\"}}",
            format_unix_timestamp_utc(ts_secs),
            ts_frac
        );

        let cols = build_col_infos(batch);
        // Check normalized JSON output names (field_name strips type suffixes like
        // `_str`/`_int`), not raw Arrow field names. A column `@timestamp_str` is
        // serialized as `@timestamp`, so we must check the normalized name to avoid
        // injecting a duplicate timestamp key into the document.
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
                    self.batch_buf
                        .extend_from_slice(ts_str.trim_start_matches(',').as_bytes());
                } else {
                    self.batch_buf.truncate(trim_to);
                    self.batch_buf.extend_from_slice(ts_str.as_bytes());
                }
                self.batch_buf.push(b'\n');
            }
        }
        Ok(())
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

            self.serialize_batch(batch, metadata)?;
            if self.batch_buf.is_empty() {
                return Ok(super::sink::SendResult::Ok);
            }

            let payload_len = self.batch_buf.len();
            let max_bytes = self.config.max_bulk_bytes;

            // Proactive split: if serialized payload exceeds max_bulk_bytes, split
            // the batch in half and send each half separately.
            if payload_len > max_bytes && n > 1 && depth < MAX_SPLIT_DEPTH {
                self.batch_buf.clear(); // discard oversized payload
                return self.send_split_halves(batch, metadata, depth).await;
            }

            let body = std::mem::replace(&mut self.batch_buf, Vec::with_capacity(64 * 1024));
            let row_count = n as u64;

            match self.do_send(body).await {
                Ok(result) => {
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
                    self.send_split_halves(batch, metadata, depth).await
                }
                Err(e) => Err(e),
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

        let req = if self.config.compress {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(&body).map_err(io::Error::other)?;
            let compressed = enc.finish().map_err(io::Error::other)?;
            req.header("Content-Encoding", "gzip").body(compressed)
        } else {
            req.body(body)
        };

        let response = req.send().await.map_err(io::Error::other)?;

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
        Self::parse_bulk_response(&body)?;
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
    pub fn new(
        name: String,
        endpoint: String,
        index: String,
        headers: Vec<(String, String)>,
        compress: bool,
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

        Ok(ElasticsearchSinkFactory {
            name,
            config: Arc::new(ElasticsearchConfig {
                endpoint,
                index,
                headers: parsed_headers,
                compress,
                max_bulk_bytes: 5 * 1024 * 1024, // 5 MiB default
                bulk_url,
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

// ---------------------------------------------------------------------------
// UTC timestamp formatting (no chrono dependency)
// ---------------------------------------------------------------------------

/// Format a Unix timestamp (seconds) as `YYYY-MM-DDTHH:MM:SS` in UTC.
///
/// This minimal implementation handles dates from 1970 to 2100 without pulling
/// in the chrono crate. The fractional seconds are formatted by the caller.
fn format_unix_timestamp_utc(secs: u64) -> String {
    // Days since epoch
    let days = secs / 86400;
    let time = secs % 86400;
    let h = time / 3600;
    let m = (time % 3600) / 60;
    let s = time % 60;

    // Gregorian calendar calculation
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
    let day = remaining + 1;
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}")
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
            Field::new("level_str", DataType::Utf8, false),
            Field::new("status_int", DataType::Int64, false),
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
            "level_str",
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

    /// Snapshot: basic multi-type batch (level_str, status_int, duration_float).
    /// Regression guard: field name normalization + type serialization.
    #[test]
    fn snapshot_basic_multi_type_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level_str", DataType::Utf8, false),
            Field::new("status_int", DataType::Int64, false),
            Field::new("duration_ms_float", DataType::Float64, false),
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
            Field::new("msg_str", DataType::Utf8, true),
            Field::new("code_int", DataType::Int64, true),
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "msg_str",
            DataType::Utf8,
            false,
        )]));
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
            Field::new("msg_str", DataType::Utf8, true),
            Field::new("code_int", DataType::Int64, true),
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
}
