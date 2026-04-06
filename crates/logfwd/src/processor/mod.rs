use arrow::record_batch::RecordBatch;
use std::fmt::Debug;

/// A processor hook allows executing logic after the DataFusion transform
/// but before the batch is sent to the output pool. It can be used for
/// tail-based sampling or other synchronous mutation/filtering on the batch.
pub trait Processor: Debug + Send + Sync {
    /// Process a batch. May return a modified batch, or an empty batch
    /// if all rows are filtered out.
    fn process(&mut self, batch: RecordBatch) -> RecordBatch;
}
