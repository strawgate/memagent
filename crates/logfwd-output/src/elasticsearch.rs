use std::io;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

use super::{
    BatchMetadata, HTTP_MAX_RETRIES, HTTP_RETRY_INITIAL_DELAY_MS, OutputSink, build_col_infos,
    is_transient_error, write_row_json,
};

// ---------------------------------------------------------------------------
// ElasticsearchSink
// ---------------------------------------------------------------------------

/// Sends log records to Elasticsearch via the bulk API.
///
/// Formats batches as newline-delimited JSON bulk requests:
/// ```json
/// {"index":{"_index":"logs"}}
/// {"level":"ERROR","msg":"failed","status":500}
/// ```
///
/// Each record produces two lines: action metadata + document. The default
/// action is `index` (upsert). The bulk API response is parsed for per-document
/// errors; if any document fails, the entire batch fails and can be retried.
pub struct ElasticsearchSink {
    name: String,
    endpoint: String,
    index: String,
    headers: Vec<(String, String)>,
    batch_buf: Vec<u8>,
    http_agent: ureq::Agent,
    stats: Arc<ComponentStats>,
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink.
    ///
    /// - `endpoint`: Base URL (e.g. `http://localhost:9200`)
    /// - `index`: Target index name
    /// - `headers`: Authentication and custom headers
    pub fn new(
        name: String,
        endpoint: String,
        index: String,
        headers: Vec<(String, String)>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        // Normalize endpoint — remove trailing slash for consistent URL building.
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let http_agent = ureq::config::Config::builder()
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .build()
            .new_agent();
        ElasticsearchSink {
            name,
            endpoint,
            index,
            headers,
            batch_buf: Vec::with_capacity(64 * 1024),
            http_agent,
            stats,
        }
    }

    /// Serialize the batch into `self.batch_buf` as Elasticsearch bulk format.
    ///
    /// Each row produces:
    /// 1. Action line: `{"index":{"_index":"<index>"}}`
    /// 2. Document line: JSON-serialized record
    ///
    /// The action `index` performs an upsert (create or replace). Alternative
    /// actions (`create`, `update`, `delete`) are not yet configurable but could
    /// be added via config fields.
    pub fn serialize_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        self.batch_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Pre-allocate action line buffer (reused per row to avoid allocations).
        // Format: {"index":{"_index":"<index>"}}
        let mut action_line = Vec::with_capacity(128);
        action_line.extend_from_slice(b"{\"index\":{\"_index\":\"");
        action_line.extend_from_slice(self.index.as_bytes());
        action_line.extend_from_slice(b"\"}}\n");

        let cols = build_col_infos(batch);
        for row in 0..num_rows {
            // Action line
            self.batch_buf.extend_from_slice(&action_line);

            // Document line
            write_row_json(batch, row, &cols, &mut self.batch_buf)?;
            self.batch_buf.push(b'\n');
        }
        Ok(())
    }

    /// Parse the bulk API response to check for errors.
    ///
    /// ES bulk responses have this shape:
    /// ```json
    /// {
    ///   "took": 5,
    ///   "errors": true,
    ///   "items": [
    ///     {"index":{"_index":"logs","_id":"1","status":201}},
    ///     {"index":{"error":{"type":"mapper_parsing_exception"},"status":400}}
    ///   ]
    /// }
    /// ```
    ///
    /// If `errors: true`, at least one document failed. This implementation
    /// returns an error with a summary of the first failure. Future enhancement:
    /// track per-document errors in metrics (#future).
    #[allow(clippy::unused_self)]
    fn parse_bulk_response(&self, body: &[u8]) -> io::Result<()> {
        // Lightweight parse: check `"errors":true` without full JSON deserialization.
        // If present, parse fully to extract error details.
        if memchr::memmem::find(body, b"\"errors\":true").is_none() {
            return Ok(());
        }

        // Full parse to extract error details.
        let v: serde_json::Value = serde_json::from_slice(body).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse ES bulk response: {e}"),
            )
        })?;

        let Some(items) = v.get("items").and_then(serde_json::Value::as_array) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ES bulk response missing 'items' array",
            ));
        };

        // Find first error.
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
    ///
    /// This method demonstrates ES|QL's ability to return results in Arrow IPC format,
    /// which is more efficient than JSON for large result sets.
    ///
    /// # Arguments
    /// * `query` - ES|QL query string (e.g., "FROM logs | WHERE level == 'ERROR' | LIMIT 1000")
    ///
    /// # Returns
    /// A vector of `RecordBatch` containing the query results.
    ///
    /// # Example
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use logfwd_output::ElasticsearchSink;
    /// # use logfwd_io::diagnostics::ComponentStats;
    /// let mut sink = ElasticsearchSink::new(
    ///     "test".into(),
    ///     "http://localhost:9200".into(),
    ///     "logs".into(),
    ///     vec![],
    ///     Arc::new(ComponentStats::default()),
    /// );
    /// let batches = sink.query_arrow("FROM logs | LIMIT 100").unwrap();
    /// ```
    pub fn query_arrow(&self, query: &str) -> io::Result<Vec<RecordBatch>> {
        // Build ES|QL query request body
        let query_body = serde_json::json!({
            "query": query
        });
        let query_bytes = serde_json::to_vec(&query_body).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize ES|QL query: {e}"),
            )
        })?;

        // Build request URL for ES|QL endpoint
        let url = format!("{}/_query", self.endpoint);

        // Retry with exponential backoff for transient failures
        let build_req = || {
            let mut req = self.http_agent.post(&url);
            for (k, v) in &self.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            // Request Arrow IPC stream format
            req.header("Content-Type", "application/json")
                .header("Accept", "application/vnd.apache.arrow.stream")
        };

        let mut delay_ms: u64 = HTTP_RETRY_INITIAL_DELAY_MS;
        let mut attempt: u32 = 0;
        loop {
            match build_req().send(&query_bytes) {
                Ok(response) => {
                    // Read response body as Arrow IPC stream
                    let body = response.into_body().read_to_vec().map_err(|e| {
                        io::Error::other(format!("failed to read ES|QL response: {e}"))
                    })?;

                    // Parse Arrow IPC stream
                    let cursor = io::Cursor::new(body);
                    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("failed to parse Arrow IPC stream: {e}"),
                        )
                    })?;

                    // Collect all batches
                    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
                    return batches.map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("failed to read Arrow IPC batch: {e}"),
                        )
                    });
                }
                Err(e) if attempt < HTTP_MAX_RETRIES && is_transient_error(&e) => {
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    delay_ms *= 2;
                    attempt += 1;
                }
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }
    }
}

impl OutputSink for ElasticsearchSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.serialize_batch(batch)?;
        if self.batch_buf.is_empty() {
            return Ok(());
        }

        // Build request URL: {endpoint}/_bulk or {endpoint}/{index}/_bulk.
        // Using `/_bulk` with per-action `_index` is more flexible for future
        // multi-index support.
        let url = format!("{}/_bulk", self.endpoint);

        // Retry with exponential backoff for transient failures.
        // 1 initial attempt + up to HTTP_MAX_RETRIES retries; delays: 100ms → 200ms → 400ms.
        let build_req = || {
            let mut req = self.http_agent.post(&url);
            for (k, v) in &self.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.header("Content-Type", "application/x-ndjson")
        };
        let mut delay_ms: u64 = HTTP_RETRY_INITIAL_DELAY_MS;
        let mut attempt: u32 = 0;
        loop {
            match build_req().send(&self.batch_buf) {
                Ok(response) => {
                    // Read response body to check for bulk errors.
                    let body = response.into_body().read_to_vec().map_err(|e| {
                        io::Error::other(format!("failed to read ES response: {e}"))
                    })?;
                    self.parse_bulk_response(&body)?;

                    self.stats.inc_lines(batch.num_rows() as u64);
                    self.stats.inc_bytes(self.batch_buf.len() as u64);
                    return Ok(());
                }
                Err(e) if attempt < HTTP_MAX_RETRIES && is_transient_error(&e) => {
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    delay_ms *= 2;
                    attempt += 1;
                }
                Err(e) => return Err(io::Error::other(e.to_string())),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// ElasticsearchAsyncSink — reqwest-based async implementation of Sink
// ---------------------------------------------------------------------------

/// Configuration shared across all `ElasticsearchAsyncSink` instances from
/// the same factory.
struct ElasticsearchConfig {
    endpoint: String,
    index: String,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
    compress: bool,
}

/// Async Elasticsearch sink using reqwest.
///
/// Implements the [`super::sink::Sink`] trait for use with `OutputWorkerPool`.
/// Each worker owns its own `reqwest::Client` so idle worker shutdown drops
/// its connection pool immediately rather than keeping it alive via a shared Arc.
pub struct ElasticsearchAsyncSink {
    config: Arc<ElasticsearchConfig>,
    client: reqwest::Client,
    name: String,
    batch_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl ElasticsearchAsyncSink {
    fn new(
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
    fn serialize_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()> {
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
            "\",\"@timestamp\":\"{}.{:09}Z\"}}",
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
                debug_assert!(self.batch_buf.ends_with(b"}\n"), "JSON must end with }}\\n");
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

    async fn do_send(&self, body: Vec<u8>) -> io::Result<super::sink::SendResult> {
        let url = format!("{}/_bulk", self.config.endpoint);

        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-ndjson");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        let req = if self.config.compress {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
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
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<super::sink::SendResult>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.serialize_batch(batch, metadata)?;
            if self.batch_buf.is_empty() {
                return Ok(super::sink::SendResult::Ok);
            }
            let body = self.batch_buf.clone();
            let byte_len = body.len();
            let row_count = batch.num_rows() as u64;
            let result = self.do_send(body).await?;
            if matches!(result, super::sink::SendResult::Ok) {
                self.stats.inc_lines(row_count);
                self.stats.inc_bytes(byte_len as u64);
            }
            Ok(result)
        })
    }

    fn flush(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// ElasticsearchSinkFactory
// ---------------------------------------------------------------------------

/// Creates `ElasticsearchAsyncSink` instances for the output worker pool.
///
/// Each call to `create()` builds a fresh `reqwest::Client` for the new worker.
/// This means idle workers release their connection pool when they exit rather
/// than keeping it alive through a shared `Arc`.
pub struct ElasticsearchSinkFactory {
    name: String,
    config: Arc<ElasticsearchConfig>,
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

        Ok(ElasticsearchSinkFactory {
            name,
            config: Arc::new(ElasticsearchConfig {
                endpoint: endpoint.trim_end_matches('/').to_string(),
                index,
                headers: parsed_headers,
                compress,
            }),
            stats,
        })
    }

    fn build_client() -> io::Result<reqwest::Client> {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(io::Error::other)
    }
}

impl super::sink::SinkFactory for ElasticsearchSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::sink::Sink>> {
        let client = Self::build_client()?;
        Ok(Box::new(ElasticsearchAsyncSink::new(
            self.name.clone(),
            Arc::clone(&self.config),
            client,
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

        let stats = Arc::new(ComponentStats::default());
        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );

        sink.serialize_batch(&batch).expect("serialize failed");
        let output = String::from_utf8_lossy(&sink.batch_buf);

        // Should produce 4 lines: action + doc for each of 2 rows.
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains(r#"{"index":{"_index":"logs"}}"#));
        assert!(lines[1].contains(r#""level":"ERROR""#));
        assert!(lines[1].contains(r#""status":500"#));
        assert!(lines[2].contains(r#"{"index":{"_index":"logs"}}"#));
        assert!(lines[3].contains(r#""level":"WARN""#));
    }

    #[test]
    fn parse_bulk_response_success() {
        let stats = Arc::new(ComponentStats::default());
        let sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );

        let response = br#"{"took":5,"errors":false,"items":[{"index":{"_id":"1","status":201}}]}"#;
        sink.parse_bulk_response(response)
            .expect("should not error on success");
    }

    #[test]
    fn parse_bulk_response_error() {
        let stats = Arc::new(ComponentStats::default());
        let sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );

        let response = br#"{
            "took":5,
            "errors":true,
            "items":[
                {"index":{"_id":"1","status":201}},
                {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}}
            ]
        }"#;
        let err = sink
            .parse_bulk_response(response)
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

        let stats = Arc::new(ComponentStats::default());
        let mut sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );

        sink.serialize_batch(&batch).expect("serialize failed");
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
        let stats = Arc::new(ComponentStats::default());
        let sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );
        let response = br#"{"took":0,"errors":false,"items":[]}"#;
        sink.parse_bulk_response(response)
            .expect("empty items should succeed");
    }

    #[test]
    fn parse_bulk_response_malformed_json_without_errors_true_is_ok() {
        // The implementation uses fast-path memchr for "errors":true.
        // Malformed JSON that doesn't contain that string is treated as success.
        let stats = Arc::new(ComponentStats::default());
        let sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );
        sink.parse_bulk_response(b"not valid json")
            .expect("no errors:true → treated as ok");
    }

    #[test]
    fn parse_bulk_response_malformed_json_after_errors_true_returns_err() {
        // If "errors":true is present but the full JSON parse fails, return an error.
        let stats = Arc::new(ComponentStats::default());
        let sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );
        sink.parse_bulk_response(b"\"errors\":true {{{malformed")
            .expect_err("malformed JSON after errors:true should error");
    }

    #[test]
    fn parse_bulk_response_errors_false_does_not_error() {
        let stats = Arc::new(ComponentStats::default());
        let sink = ElasticsearchSink::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "logs".to_string(),
            vec![],
            stats,
        );
        // errors:false means success even if items have non-200 status
        let response = br#"{"took":1,"errors":false,"items":[{"index":{"_id":"1","status":200}}]}"#;
        sink.parse_bulk_response(response)
            .expect("errors:false must succeed");
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
