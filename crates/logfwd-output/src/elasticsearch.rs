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
}
