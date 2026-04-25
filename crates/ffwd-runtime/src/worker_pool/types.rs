use arrow::record_batch::RecordBatch;
use ffwd_output::BatchMetadata;
use ffwd_types::pipeline::{BatchTicket, Sending};

pub(super) const MAX_REJECTION_REASON_BYTES: usize = 256;

/// Ownership of `tickets` is transferred to the worker, which must ack or
/// reject every ticket before it exits.
pub struct WorkItem {
    pub batch: RecordBatch,
    pub metadata: BatchMetadata,
    pub tickets: Vec<BatchTicket<Sending, u64>>,
    /// Number of rows in the batch (for metrics recording at ack time).
    pub num_rows: u64,
    /// When this item was submitted to the pool (set by the caller, not submit()).
    /// Uses tokio::time::Instant so elapsed() measures simulated time under Turmoil.
    pub submitted_at: tokio::time::Instant,
    /// Nanoseconds spent in the scan stage (passed through for metrics at ack time).
    pub scan_ns: u64,
    /// Nanoseconds spent in the transform stage (passed through for metrics at ack time).
    pub transform_ns: u64,
    /// Batch ID for active-batch tracking in PipelineMetrics.
    pub batch_id: u64,
    /// The batch span — kept alive through the worker so output_ns can be recorded on it.
    pub span: tracing::Span,
}

/// Result from a worker after processing one [`WorkItem`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DeliveryOutcome {
    /// The batch was delivered successfully.
    Delivered,
    /// The sink rejected the batch permanently.
    Rejected { reason: String },
    /// Retries were exhausted before delivery could succeed.
    RetryExhausted,
    /// A hard per-batch timeout elapsed while sending.
    TimedOut,
    /// The pool was drained before this item could be delivered.
    PoolClosed,
    /// The selected worker channel closed before the item could be enqueued.
    WorkerChannelClosed,
    /// No worker could be created or selected for this item.
    NoWorkersAvailable,
    /// Catch-all for future sink result variants.
    InternalFailure,
}

impl DeliveryOutcome {
    /// Returns `true` when the batch reached the sink successfully.
    ///
    /// This is the compatibility gate used by the pipeline metrics path and
    /// checkpoint policy while the typed delivery contract is rolled out.
    pub const fn is_delivered(&self) -> bool {
        matches!(self, Self::Delivered)
    }

    /// Returns `true` when the sink rejected the data permanently.
    ///
    /// This is the only worker outcome that should currently advance the
    /// checkpoint without successful delivery.
    pub const fn is_permanent_reject(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}

pub(super) fn bound_rejection_reason(mut reason: String) -> String {
    if reason.len() <= MAX_REJECTION_REASON_BYTES {
        return reason;
    }
    let suffix = "...";
    let mut boundary = MAX_REJECTION_REASON_BYTES.saturating_sub(suffix.len());
    while boundary > 0 && !reason.is_char_boundary(boundary) {
        boundary -= 1;
    }
    reason.truncate(boundary);
    reason.push_str(suffix);
    reason
}

pub struct AckItem {
    /// The tickets from the corresponding `WorkItem`, plus delivery outcome.
    pub tickets: Vec<BatchTicket<Sending, u64>>,
    /// Delivery outcome from the worker/pool.
    pub outcome: DeliveryOutcome,
    /// Passed through from WorkItem for metrics recording.
    pub num_rows: u64,
    /// When the corresponding WorkItem was submitted.
    /// Uses tokio::time::Instant so elapsed() measures simulated time under Turmoil.
    pub submitted_at: tokio::time::Instant,
    /// Nanoseconds spent in the scan stage (passed through from WorkItem).
    pub scan_ns: u64,
    /// Nanoseconds spent in the transform stage (passed through from WorkItem).
    pub transform_ns: u64,
    /// Nanoseconds spent in the output stage (actual send time, measured by worker).
    pub output_ns: u64,
    /// Nanoseconds spent waiting in the pool queue before a worker picked it up.
    pub queue_wait_ns: u64,
    /// Nanoseconds of pure `send_batch` wall time (tightest measurement around the call).
    pub send_latency_ns: u64,
    /// Batch ID for active-batch tracking in PipelineMetrics.
    pub batch_id: u64,
    /// Output sink name that produced this ack result.
    pub output_name: String,
}

/// Message sent from pool to worker.
pub(super) enum WorkerMsg {
    /// Process this batch.
    Work(WorkItem),
    /// Finish current item (if any) then exit cleanly.
    Shutdown,
}

// ---------------------------------------------------------------------------
