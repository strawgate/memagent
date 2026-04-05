use std::future::Future;
use std::io;
use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use logfwd_types::diagnostics::ComponentStats;

use super::{
    BatchMetadata, Compression, HTTP_MAX_RETRIES, HTTP_RETRY_INITIAL_DELAY_MS, build_col_infos,
    str_value, write_row_json,
};

// ---------------------------------------------------------------------------
// JsonLinesSink
// ---------------------------------------------------------------------------

/// Writes newline-delimited JSON and POSTs over HTTP.
pub struct JsonLinesSink {
    name: String,
    url: String,
    headers: Vec<(String, String)>,
    pub(crate) batch_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    compression: Compression,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl JsonLinesSink {
    /// Create a new JSON-lines sink.
    ///
    /// The caller supplies a shared `reqwest::Client` (typically cloned from
    /// the factory) so all workers reuse the same connection pool.
    pub fn new(
        name: String,
        url: String,
        headers: Vec<(String, String)>,
        compression: Compression,
        client: reqwest::Client,
        stats: Arc<ComponentStats>,
    ) -> Self {
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

    /// Check whether the batch is "raw passthrough eligible": has a `_raw` column
    /// and every other column is null for every row (no transforms modified fields).
    fn is_raw_passthrough(batch: &RecordBatch) -> bool {
        let schema = batch.schema();
        let has_raw = schema.fields().iter().any(|f| f.name() == "_raw");
        if !has_raw {
            return false;
        }
        // Simple heuristic: if the only non-null column is _raw, passthrough.
        // Use enumerated iteration to avoid `index_of` which could panic on
        // schema inconsistency (issue #317).
        for (idx, field) in schema.fields().iter().enumerate() {
            if field.name() == "_raw" {
                continue;
            }
            if batch.column(idx).null_count() < batch.num_rows() {
                return false;
            }
        }
        true
    }

    /// Serialize the batch into `self.batch_buf` as newline-delimited JSON.
    ///
    /// Returns `Err` if the schema claims `_raw` passthrough but the column is
    /// unexpectedly absent — this indicates a logic error and should not be
    /// silently swallowed (issue #317).
    pub fn serialize_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        self.batch_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        if Self::is_raw_passthrough(batch) {
            // Fast path: memcpy _raw values directly.
            // Use `position` instead of `index_of().expect(...)` to avoid panicking
            // if the schema is unexpectedly inconsistent (issue #317).
            let raw_idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == "_raw")
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "_raw column absent despite passthrough eligibility check",
                    )
                })?;
            let col = batch.column(raw_idx);
            for row in 0..num_rows {
                if !col.is_null(row) {
                    self.batch_buf
                        .extend_from_slice(str_value(col, row).as_bytes());
                    self.batch_buf.push(b'\n');
                }
            }
        } else {
            let cols = build_col_infos(batch);
            for row in 0..num_rows {
                write_row_json(batch, row, &cols, &mut self.batch_buf)?;
                self.batch_buf.push(b'\n');
            }
        }
        Ok(())
    }

    /// Serialize, compress (if configured), and POST the batch over HTTP.
    ///
    /// Retries transient failures (5xx, timeout, and connection errors) with
    /// exponential backoff. Returns `SendResult::RetryAfter` on HTTP 429 and
    /// `SendResult::Rejected` on non-429 4xx client errors.
    async fn send_batch_inner(
        &mut self,
        batch: &RecordBatch,
    ) -> io::Result<super::sink::SendResult> {
        self.serialize_batch(batch)?;
        if self.batch_buf.is_empty() {
            return Ok(super::sink::SendResult::Ok);
        }

        // Compress if configured. The compressed payload is stored in
        // `self.compress_buf` and lives until the next call.
        let payload: &[u8] = match self.compression {
            Compression::Gzip => {
                use flate2::Compression as GzLevel;
                use flate2::write::GzEncoder;
                use std::io::Write;
                self.compress_buf.clear();
                let mut enc = GzEncoder::new(&mut self.compress_buf, GzLevel::default());
                enc.write_all(&self.batch_buf)
                    .map_err(|e| io::Error::other(format!("gzip compression failed: {e}")))?;
                enc.finish().map_err(|e| {
                    io::Error::other(format!("gzip compression finish failed: {e}"))
                })?;
                &self.compress_buf
            }
            Compression::None => &self.batch_buf,
            Compression::Zstd => {
                unreachable!("zstd is not supported for json_lines; rejected at config validation")
            }
        };

        // Retry with exponential backoff for transient failures.
        // 1 initial attempt + up to HTTP_MAX_RETRIES retries; delays: 100ms → 200ms → 400ms.
        let mut delay_ms: u64 = HTTP_RETRY_INITIAL_DELAY_MS;
        let mut attempt: u32 = 0;

        // Own the payload once; on retries we call `.clone()` which allocates
        // but is acceptable for a path limited to HTTP_MAX_RETRIES iterations.
        let body = payload.to_vec();

        loop {
            let mut req = self
                .client
                .post(&self.url)
                .header("Content-Type", "application/x-ndjson");
            if self.compression == Compression::Gzip {
                req = req.header("Content-Encoding", "gzip");
            }
            for (k, v) in &self.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            let req = req.body(body.clone());

            match req.send().await {
                Ok(response) => {
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
                            "HTTP endpoint rejected batch with HTTP {status}"
                        )));
                    }

                    if status.is_server_error() {
                        if attempt < HTTP_MAX_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            delay_ms *= 2;
                            attempt += 1;
                            continue;
                        }
                        return Err(io::Error::other(format!(
                            "HTTP endpoint returned {status} after {attempt} retries"
                        )));
                    }

                    if status.is_success() {
                        self.stats.inc_lines(batch.num_rows() as u64);
                        self.stats.inc_bytes(payload.len() as u64);
                        return Ok(super::sink::SendResult::Ok);
                    }

                    return Err(io::Error::other(format!(
                        "HTTP endpoint returned non-success status {status}"
                    )));
                }
                Err(e) if attempt < HTTP_MAX_RETRIES && (e.is_timeout() || e.is_connect()) => {
                    // Retry transient transport errors only.
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms *= 2;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(io::Error::other(format!(
                        "HTTP request failed after {attempt} retries: {e}"
                    )));
                }
            }
        }
    }
}

impl super::sink::Sink for JsonLinesSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> std::pin::Pin<Box<dyn Future<Output = io::Result<super::sink::SendResult>> + Send + 'a>>
    {
        Box::pin(async move { self.send_batch_inner(batch).await })
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
// JsonLinesSinkFactory
// ---------------------------------------------------------------------------

/// Creates [`JsonLinesSink`] instances for the output worker pool.
///
/// All workers share a single `reqwest::Client` (which is internally
/// `Arc`-wrapped) so they reuse the same connection pool, TLS sessions,
/// and DNS cache.
pub struct JsonLinesSinkFactory {
    name: String,
    url: String,
    headers: Vec<(String, String)>,
    compression: Compression,
    client: reqwest::Client,
    stats: Arc<ComponentStats>,
}

impl JsonLinesSinkFactory {
    /// Create a new factory.
    ///
    /// Builds a shared `reqwest::Client` with a 30-second timeout and a
    /// generous idle-connection pool.
    pub fn new(
        name: String,
        url: String,
        headers: Vec<(String, String)>,
        compression: Compression,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .pool_max_idle_per_host(64)
            .build()
            .map_err(io::Error::other)?;
        Ok(JsonLinesSinkFactory {
            name,
            url,
            headers,
            compression,
            client,
            stats,
        })
    }
}

impl super::sink::SinkFactory for JsonLinesSinkFactory {
    fn create(&self) -> io::Result<Box<dyn super::sink::Sink>> {
        Ok(Box::new(JsonLinesSink::new(
            self.name.clone(),
            self.url.clone(),
            self.headers.clone(),
            self.compression,
            self.client.clone(), // reqwest::Client is Arc-wrapped, clone is cheap
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
        let client = reqwest::Client::new();
        JsonLinesSink::new(
            "test".to_string(),
            "http://localhost:1".to_string(), // unreachable — tests only call serialize_batch
            vec![],
            Compression::None,
            client,
            Arc::new(ComponentStats::default()),
        )
    }

    fn batch_with_raw_only(raw_values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(raw_values))]).unwrap()
    }

    fn batch_raw_plus_nulls(raw_values: Vec<&str>) -> RecordBatch {
        let n = raw_values.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true), // will be all-null
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(raw_values)),
                Arc::new(StringArray::from(vec![None::<&str>; n])), // all null
            ],
        )
        .unwrap()
    }

    fn batch_raw_with_non_null_col() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
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

    fn batch_no_raw() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap()
    }

    // --- is_raw_passthrough ---

    #[test]
    fn passthrough_raw_only_column() {
        let batch = batch_with_raw_only(vec!["line 1", "line 2"]);
        assert!(JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn passthrough_raw_plus_all_null_cols() {
        let batch = batch_raw_plus_nulls(vec!["line 1", "line 2"]);
        assert!(JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn no_passthrough_when_no_raw_col() {
        let batch = batch_no_raw();
        assert!(!JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn no_passthrough_when_non_null_col_present() {
        let batch = batch_raw_with_non_null_col();
        assert!(!JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn no_passthrough_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        assert!(!JsonLinesSink::is_raw_passthrough(&batch));
    }

    // --- serialize_batch: raw passthrough path ---

    #[test]
    fn serialize_passthrough_copies_raw_verbatim() {
        let mut sink = make_sink();
        let batch = batch_with_raw_only(vec![r#"{"a":1}"#, r#"{"b":2}"#]);
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
    }

    #[test]
    fn serialize_passthrough_raw_plus_null_cols() {
        let mut sink = make_sink();
        let batch = batch_raw_plus_nulls(vec!["raw-line-1", "raw-line-2"]);
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines, vec!["raw-line-1", "raw-line-2"]);
    }

    // --- serialize_batch: JSON serialization path ---

    #[test]
    fn serialize_json_path_produces_valid_ndjson() {
        let mut sink = make_sink();
        let batch = batch_raw_with_non_null_col();
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

    // --- serialize_batch: edge cases ---

    #[test]
    fn serialize_empty_batch_produces_no_output() {
        let mut sink = make_sink();
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
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
        sink.serialize_batch(&batch_with_raw_only(vec!["first"]))
            .unwrap();
        assert!(!sink.batch_buf.is_empty());
        let empty_schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
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
}
