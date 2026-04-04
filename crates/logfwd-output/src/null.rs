//! Null output sink — discards all batches. Used for benchmarking and as a
//! blackhole collector target.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

use crate::{BatchMetadata, OutputSink};

/// Discards all data. Useful as the output side of a blackhole receiver
/// or for benchmarking pipeline overhead without network I/O.
///
/// Tracks the number of discarded batches and rows via atomic counters
/// that can be read from a diagnostics thread without locking.
pub struct NullSink {
    name: String,
    batches_discarded: AtomicU64,
    rows_discarded: AtomicU64,
    stats: Arc<ComponentStats>,
}

impl NullSink {
    pub fn new(name: impl Into<String>, stats: Arc<ComponentStats>) -> Self {
        Self {
            name: name.into(),
            batches_discarded: AtomicU64::new(0),
            rows_discarded: AtomicU64::new(0),
            stats,
        }
    }

    /// Total number of batches discarded since creation.
    pub fn batches_discarded(&self) -> u64 {
        self.batches_discarded.load(Ordering::Relaxed)
    }

    /// Total number of rows discarded since creation.
    pub fn rows_discarded(&self) -> u64 {
        self.rows_discarded.load(Ordering::Relaxed)
    }
}

impl OutputSink for NullSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.batches_discarded.fetch_add(1, Ordering::Relaxed);
        let num_rows = batch.num_rows() as u64;
        self.rows_discarded.fetch_add(num_rows, Ordering::Relaxed);
        // Track discarded lines and in-memory byte size so that diagnostics
        // show non-zero output metrics for null/blackhole pipelines.
        self.stats.inc_lines(num_rows);
        self.stats.inc_bytes(batch.get_array_memory_size() as u64);
        Ok(())
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
    use logfwd_io::diagnostics::ComponentStats;

    #[test]
    fn null_sink_accepts_and_discards() {
        let mut sink = NullSink::new("blackhole", Arc::new(ComponentStats::new()));
        assert_eq!(sink.name(), "blackhole");

        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let meta = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };
        assert!(sink.send_batch(&batch, &meta).is_ok());
        assert!(sink.flush().is_ok());

        assert_eq!(sink.batches_discarded(), 1);
        assert_eq!(sink.rows_discarded(), 0); // empty batch has 0 rows
    }

    #[test]
    fn null_sink_counts_rows() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let col = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let meta = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };

        let mut sink = NullSink::new("blackhole", Arc::new(ComponentStats::new()));
        sink.send_batch(&batch, &meta).unwrap();
        sink.send_batch(&batch, &meta).unwrap();

        assert_eq!(sink.batches_discarded(), 2);
        assert_eq!(sink.rows_discarded(), 6);
    }

    #[test]
    fn null_sink_increments_lines_and_bytes() {
        // Each sink is responsible for its own line and byte counting —
        // there is no central pipeline call that does it.
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let col = Int32Array::from(vec![10, 20, 30]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let meta = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };

        let stats = Arc::new(ComponentStats::new());
        let mut sink = NullSink::new("blackhole", Arc::clone(&stats));
        sink.send_batch(&batch, &meta).unwrap();
        sink.send_batch(&batch, &meta).unwrap();

        // Sink must increment lines_total once per row per batch.
        assert_eq!(
            stats.lines_total.load(Ordering::Relaxed),
            6,
            "sink must increment lines_total for each forwarded row"
        );
        // Bytes must be non-zero (in-memory batch size).
        assert!(
            stats.bytes_total.load(Ordering::Relaxed) > 0,
            "sink must report non-zero bytes"
        );
        // Row-level counters on the sink itself are still tracked.
        assert_eq!(sink.rows_discarded(), 6);
    }
}
