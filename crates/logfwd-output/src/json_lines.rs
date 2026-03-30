use std::io;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink, build_col_infos, str_value, write_row_json};

// ---------------------------------------------------------------------------
// JsonLinesSink
// ---------------------------------------------------------------------------

/// Writes newline-delimited JSON and POSTs over HTTP.
pub struct JsonLinesSink {
    name: String,
    url: String,
    headers: Vec<(String, String)>,
    pub batch_buf: Vec<u8>,
    http_agent: ureq::Agent,
}

impl JsonLinesSink {
    pub fn new(name: String, url: String, headers: Vec<(String, String)>) -> Self {
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
        for field in schema.fields().iter() {
            if field.name() == "_raw" {
                continue;
            }
            let idx = schema.index_of(field.name()).unwrap();
            if batch.column(idx).null_count() < batch.num_rows() {
                return false;
            }
        }
        true
    }

    /// Serialize the batch into `self.batch_buf` as newline-delimited JSON.
    pub fn serialize_batch(&mut self, batch: &RecordBatch) {
        self.batch_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        if Self::is_raw_passthrough(batch) {
            // Fast path: memcpy _raw values directly.
            let idx = batch
                .schema()
                .index_of("_raw")
                .expect("_raw column missing");
            let col = batch.column(idx);
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
                write_row_json(batch, row, &cols, &mut self.batch_buf);
                self.batch_buf.push(b'\n');
            }
        }
    }
}

impl OutputSink for JsonLinesSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.serialize_batch(batch);
        if self.batch_buf.is_empty() {
            return Ok(());
        }

        let mut req = self.http_agent.post(&self.url);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/x-ndjson");

        req.send(&self.batch_buf)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
