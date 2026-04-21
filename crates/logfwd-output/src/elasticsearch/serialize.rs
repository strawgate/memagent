//! Elasticsearch bulk serialization: converts RecordBatches to NDJSON bulk format.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use logfwd_types::field_names;
use tokio::sync::mpsc;

use super::super::{BatchMetadata, build_col_infos, write_row_json};
use super::timestamp::write_ts_suffix;
use super::types::{ElasticsearchConfig, ElasticsearchSink};

impl ElasticsearchSink {
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
            write_row_json(batch, row, &cols, &mut self.batch_buf, true)?;

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

    pub(super) fn send_chunk(
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
            .map_err(|_e| io::Error::new(io::ErrorKind::BrokenPipe, "ES body receiver dropped"))
    }

    pub(super) fn serialize_batch_streaming(
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
            write_row_json(&batch, row, &cols, &mut chunk, true)?;

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
}
