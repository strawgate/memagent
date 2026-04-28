use arrow::record_batch::RecordBatch;
use ffwd_output::BatchMetadata;
use smallvec::SmallVec;
use std::fmt::Debug;

/// Blocklist-based enrichment processor: marks rows whose source column
/// value appears in a preloaded CSV blocklist.
pub mod blocklist;
/// HTTP enrichment processor: per-key HTTP lookups with caching and
/// concurrency control.
pub mod http_enrich;

/// Processor that annotates records using a preloaded blocklist.
pub use blocklist::BlocklistProcessor;
/// Configuration and processor for per-row HTTP enrichment.
pub use http_enrich::{HttpEnrichConfig, HttpEnrichProcessor};

/// Error types for processor operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProcessorError {
    /// Transient error — retry the batch, do NOT advance checkpoint.
    #[error("transient processor error: {0}")]
    Transient(String),

    /// Permanent error — data is malformed, skip batch (future: dead-letter).
    #[error("permanent processor error: {0}")]
    Permanent(String),

    /// Fatal error — shut down the pipeline WITHOUT advancing checkpoint.
    #[error("fatal processor error: {0}")]
    Fatal(String),
}

/// A processor transforms batches in the pipeline between the SQL transform
/// and the output sinks. Processors are chained: the output of one feeds
/// the input of the next.
///
/// # Contract
///
/// - `process()` is synchronous. Processors needing async I/O should use
///   internal channels + background tasks.
/// - Stateless processors MUST pass through empty batches (return `smallvec![batch]`
///   when `batch.num_rows() == 0`) so heartbeats reach downstream stateful processors.
/// - Stateful processors should check their internal timers on every `process()` call
///   and emit timed-out state alongside normal output.
/// - `flush()` is called during shutdown. Return ALL buffered state.
pub trait Processor: Debug + Send + Sync {
    /// Process one batch. Return zero or more output batches.
    ///
    /// - Empty vec = batch buffered internally (stateful processors).
    /// - Empty input batch (num_rows == 0) = heartbeat signal. Stateless
    ///   processors should pass it through; stateful processors should
    ///   check their internal timers.
    fn process(
        &mut self,
        batch: RecordBatch,
        meta: &BatchMetadata,
    ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError>;

    /// Graceful shutdown. Flush ALL buffered state.
    /// Called once during pipeline shutdown, before output draining.
    fn flush(&mut self) -> SmallVec<[RecordBatch; 1]>;

    /// Human-readable name for metrics and logging.
    fn name(&self) -> &'static str;

    /// Whether this processor buffers data across batches.
    /// Used to determine if heartbeat empty batches should be sent through the chain.
    fn is_stateful(&self) -> bool;
}

/// Run a batch through a chain of processors.
///
/// Each processor's output is fed as input to the next. If any processor
/// returns an empty output, the chain short-circuits (nothing to feed downstream).
pub fn run_chain(
    processors: &mut [Box<dyn Processor>],
    batch: RecordBatch,
    meta: &BatchMetadata,
) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
    let mut current: SmallVec<[RecordBatch; 1]> = smallvec::smallvec![batch];

    for processor in processors.iter_mut() {
        let mut next = SmallVec::new();
        for b in current {
            next.extend(processor.process(b, meta)?);
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }

    Ok(current)
}

/// Cascading flush for shutdown: flush each processor in order, feeding
/// its output through all downstream processors (which also get re-flushed
/// to handle any data they buffer from the cascade).
pub fn cascading_flush(
    processors: &mut [Box<dyn Processor>],
    meta: &BatchMetadata,
) -> SmallVec<[RecordBatch; 1]> {
    let mut all_output = SmallVec::new();

    for i in 0..processors.len() {
        let (_, tail) = processors.split_at_mut(i);
        let Some((processor, remaining)) = tail.split_first_mut() else {
            continue;
        };
        let mut emitted = processor.flush();

        // Cascade through remaining processors
        for processor in remaining {
            let mut next = SmallVec::new();
            for b in emitted {
                match processor.process(b, meta) {
                    Ok(out) => next.extend(out),
                    Err(e) => {
                        tracing::warn!(
                            processor = processor.name(),
                            error = %e,
                            "processor error during cascading flush, skipping batch"
                        );
                    }
                }
            }
            // Re-flush: processor may have buffered data from the cascade
            next.extend(processor.flush());
            emitted = next;
        }

        all_output.extend(emitted);
    }

    all_output
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_meta() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        }
    }

    fn test_batch(rows: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let array = Int32Array::from((0..rows).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[derive(Debug)]
    struct PassthroughProcessor;
    impl Processor for PassthroughProcessor {
        fn process(
            &mut self,
            batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            Ok(smallvec::smallvec![batch])
        }
        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            SmallVec::new()
        }
        fn name(&self) -> &'static str {
            "passthrough"
        }
        fn is_stateful(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct FilterProcessor {
        min_rows: usize,
    }
    impl Processor for FilterProcessor {
        fn process(
            &mut self,
            batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            if batch.num_rows() >= self.min_rows {
                Ok(smallvec::smallvec![batch])
            } else {
                Ok(SmallVec::new())
            }
        }
        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            SmallVec::new()
        }
        fn name(&self) -> &'static str {
            "filter"
        }
        fn is_stateful(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct BufferingProcessor {
        buffer: Vec<RecordBatch>,
    }
    impl Processor for BufferingProcessor {
        fn process(
            &mut self,
            batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            if batch.num_rows() > 0 {
                self.buffer.push(batch);
            }
            Ok(SmallVec::new()) // always buffer, never emit during process
        }
        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            self.buffer.drain(..).collect()
        }
        fn name(&self) -> &'static str {
            "buffering"
        }
        fn is_stateful(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct ErrorProcessor {
        error_kind: &'static str,
    }
    impl Processor for ErrorProcessor {
        fn process(
            &mut self,
            _batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            match self.error_kind {
                "transient" => Err(ProcessorError::Transient("test transient".into())),
                "permanent" => Err(ProcessorError::Permanent("test permanent".into())),
                "fatal" => Err(ProcessorError::Fatal("test fatal".into())),
                _ => Ok(SmallVec::new()),
            }
        }
        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            SmallVec::new()
        }
        fn name(&self) -> &'static str {
            "error"
        }
        fn is_stateful(&self) -> bool {
            false
        }
    }

    #[test]
    fn empty_chain_passes_batch_through() {
        let batch = test_batch(5);
        let result = run_chain(&mut [], batch, &test_meta()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);
    }

    #[test]
    fn single_passthrough_preserves_rows() {
        let mut procs: Vec<Box<dyn Processor>> = vec![Box::new(PassthroughProcessor)];
        let result = run_chain(&mut procs, test_batch(5), &test_meta()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);
    }

    #[test]
    fn chain_filter_then_passthrough() {
        let mut procs: Vec<Box<dyn Processor>> = vec![
            Box::new(FilterProcessor { min_rows: 3 }),
            Box::new(PassthroughProcessor),
        ];
        // Batch with 5 rows passes filter
        let result = run_chain(&mut procs, test_batch(5), &test_meta()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);

        // Batch with 2 rows filtered out
        let result = run_chain(&mut procs, test_batch(2), &test_meta()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn chain_short_circuits_on_empty() {
        let mut procs: Vec<Box<dyn Processor>> = vec![
            Box::new(FilterProcessor { min_rows: 100 }),
            Box::new(PassthroughProcessor),
        ];
        // Small batch filtered out by first processor — second never runs
        let result = run_chain(&mut procs, test_batch(3), &test_meta()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn cascading_flush_stateful() {
        let mut procs: Vec<Box<dyn Processor>> = vec![
            Box::new(BufferingProcessor { buffer: vec![] }),
            Box::new(PassthroughProcessor),
        ];

        // Buffer 3 batches
        let meta = test_meta();
        let _ = run_chain(&mut procs, test_batch(1), &meta).unwrap();
        let _ = run_chain(&mut procs, test_batch(2), &meta).unwrap();
        let _ = run_chain(&mut procs, test_batch(3), &meta).unwrap();

        // Cascading flush should emit all 3
        let flushed = cascading_flush(&mut procs, &meta);
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    fn cascading_flush_chained_stateful() {
        let mut procs: Vec<Box<dyn Processor>> = vec![
            Box::new(BufferingProcessor { buffer: vec![] }),
            Box::new(BufferingProcessor { buffer: vec![] }),
        ];

        // Buffer 2 batches in first processor
        let meta = test_meta();
        let _ = run_chain(&mut procs, test_batch(1), &meta).unwrap();
        let _ = run_chain(&mut procs, test_batch(2), &meta).unwrap();

        // Cascading flush: first flushes → feeds second → second re-flushes
        let flushed = cascading_flush(&mut procs, &meta);
        assert_eq!(flushed.len(), 2);
    }

    #[test]
    fn heartbeat_passthrough() {
        // Empty batch should pass through stateless processors
        let mut procs: Vec<Box<dyn Processor>> = vec![Box::new(PassthroughProcessor)];
        let empty = test_batch(0);
        let result = run_chain(&mut procs, empty, &test_meta()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 0);
    }

    #[test]
    fn transient_error_propagates() {
        let mut procs: Vec<Box<dyn Processor>> = vec![Box::new(ErrorProcessor {
            error_kind: "transient",
        })];
        let result = run_chain(&mut procs, test_batch(5), &test_meta());
        assert!(matches!(result, Err(ProcessorError::Transient(_))));
    }

    #[test]
    fn permanent_error_propagates() {
        let mut procs: Vec<Box<dyn Processor>> = vec![Box::new(ErrorProcessor {
            error_kind: "permanent",
        })];
        let result = run_chain(&mut procs, test_batch(5), &test_meta());
        assert!(matches!(result, Err(ProcessorError::Permanent(_))));
    }

    #[test]
    fn fatal_error_propagates() {
        let mut procs: Vec<Box<dyn Processor>> = vec![Box::new(ErrorProcessor {
            error_kind: "fatal",
        })];
        let result = run_chain(&mut procs, test_batch(5), &test_meta());
        assert!(matches!(result, Err(ProcessorError::Fatal(_))));
    }
}
