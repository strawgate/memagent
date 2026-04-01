use std::io;
use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

use super::{
    BatchMetadata, HTTP_MAX_RETRIES, HTTP_RETRY_INITIAL_DELAY_MS, OutputSink, build_col_infos,
    is_transient_error, str_value, write_row_json,
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
    http_agent: ureq::Agent,
    stats: Arc<ComponentStats>,
}

impl JsonLinesSink {
    pub fn new(
        name: String,
        url: String,
        headers: Vec<(String, String)>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let http_agent = ureq::config::Config::builder()
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .build()
            .new_agent();
        JsonLinesSink {
            name,
            url,
            headers,
            batch_buf: Vec::with_capacity(64 * 1024),
            http_agent,
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
}

impl OutputSink for JsonLinesSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.serialize_batch(batch)?;
        if self.batch_buf.is_empty() {
            return Ok(());
        }

        // Retry with exponential backoff for transient failures.
        // 1 initial attempt + up to HTTP_MAX_RETRIES retries; delays: 100ms → 200ms → 400ms.
        // Note: `self.batch_buf` is re-sent as `&[u8]` on each attempt — no
        // allocation, but the full NDJSON payload is retransmitted each time.
        // This is acceptable as a temporary measure until SinkDriver (#319).
        let build_req = || {
            let mut req = self.http_agent.post(&self.url);
            for (k, v) in &self.headers {
                req = req.header(k.as_str(), v.as_str());
            }
            req.header("Content-Type", "application/x-ndjson")
        };
        let mut delay_ms: u64 = HTTP_RETRY_INITIAL_DELAY_MS;
        let mut attempt: u32 = 0;
        loop {
            match build_req().send(&self.batch_buf) {
                Ok(_) => {
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
