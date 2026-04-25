//! Null output sink — discards all batches. Used for benchmarking and as a
//! blackhole collector target.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;

use ffwd_types::diagnostics::ComponentStats;

use crate::BatchMetadata;
use crate::sink::SendResult;

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

impl crate::Sink for NullSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        self.batches_discarded.fetch_add(1, Ordering::Relaxed);
        let num_rows = batch.num_rows() as u64;
        self.rows_discarded.fetch_add(num_rows, Ordering::Relaxed);
        // Track discarded lines and in-memory byte size so that diagnostics
        // show non-zero output metrics for null/blackhole pipelines.
        self.stats.inc_lines(num_rows);
        self.stats.inc_bytes(batch.get_array_memory_size() as u64);
        Box::pin(async { SendResult::Ok })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// Factory that creates [`NullSink`] instances.
///
/// Each call to `create()` returns a fresh `NullSink` that shares the same
/// `ComponentStats` handle but has independent atomic counters.
pub struct NullSinkFactory {
    name: String,
    stats: Arc<ComponentStats>,
}

impl NullSinkFactory {
    /// Create a new factory that will produce null sinks with the given name
    /// and diagnostics handle.
    pub fn new(name: String, stats: Arc<ComponentStats>) -> Self {
        NullSinkFactory { name, stats }
    }
}

impl crate::SinkFactory for NullSinkFactory {
    fn create(&self) -> io::Result<Box<dyn crate::Sink>> {
        Ok(Box::new(NullSink::new(
            self.name.clone(),
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffwd_types::diagnostics::ComponentStats;

    #[tokio::test]
    async fn null_sink_accepts_and_discards() {
        use crate::Sink;

        let mut sink = NullSink::new("blackhole", Arc::new(ComponentStats::new()));
        assert_eq!(Sink::name(&sink), "blackhole");

        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let meta = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };
        sink.send_batch(&batch, &meta).await.unwrap();
        assert!(sink.flush().await.is_ok());

        assert_eq!(sink.batches_discarded(), 1);
        assert_eq!(sink.rows_discarded(), 0); // empty batch has 0 rows
    }

    #[tokio::test]
    async fn null_sink_counts_rows() {
        use crate::Sink;
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
        sink.send_batch(&batch, &meta).await.unwrap();
        sink.send_batch(&batch, &meta).await.unwrap();

        assert_eq!(sink.batches_discarded(), 2);
        assert_eq!(sink.rows_discarded(), 6);
    }

    #[tokio::test]
    async fn null_sink_increments_lines_and_bytes() {
        // Each sink is responsible for its own line and byte counting —
        // there is no central pipeline call that does it.
        use crate::Sink;
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
        sink.send_batch(&batch, &meta).await.unwrap();
        sink.send_batch(&batch, &meta).await.unwrap();

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

    #[test]
    fn null_sink_factory_creates_sinks() {
        use crate::SinkFactory;

        let factory = NullSinkFactory::new("blackhole".into(), Arc::new(ComponentStats::new()));
        assert_eq!(factory.name(), "blackhole");
        assert!(!factory.is_single_use());

        let sink1 = factory.create().expect("first create should succeed");
        let sink2 = factory.create().expect("second create should also succeed");
        assert_eq!(sink1.name(), "blackhole");
        assert_eq!(sink2.name(), "blackhole");
    }
}
