use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;
use logfwd_types::pipeline::{BatchTicket, Sending};

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
    bound_reason_to_limit(&mut reason, MAX_REJECTION_REASON_BYTES);
    reason
}

fn bound_reason_to_limit(reason: &mut String, max_bytes: usize) {
    if reason.len() <= max_bytes {
        return;
    }
    const SUFFIX: &str = "...";
    if max_bytes <= SUFFIX.len() {
        let mut end = max_bytes;
        while end > 0 && !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
        return;
    }
    let mut boundary = max_bytes - SUFFIX.len();
    while boundary > 0 && !reason.is_char_boundary(boundary) {
        boundary -= 1;
    }
    reason.truncate(boundary);
    reason.push_str(SUFFIX);
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

#[cfg(test)]
mod tests {
    use super::{MAX_REJECTION_REASON_BYTES, bound_reason_to_limit, bound_rejection_reason};

    #[test]
    fn bound_rejection_reason_keeps_short_reason_unchanged() {
        let reason = "transient backend error".to_string();
        assert_eq!(bound_rejection_reason(reason.clone()), reason);
    }

    #[test]
    fn bound_rejection_reason_respects_global_limit_and_appends_suffix() {
        let reason = "x".repeat(MAX_REJECTION_REASON_BYTES + 64);
        let bounded = bound_rejection_reason(reason);
        assert_eq!(bounded.len(), MAX_REJECTION_REASON_BYTES);
        assert!(bounded.ends_with("..."));
    }

    #[test]
    fn bound_reason_to_limit_preserves_utf8_boundaries() {
        let mut reason = "é".repeat(8);
        // 8 * 2 bytes = 16 bytes; bounding to 7 bytes should become 4 bytes
        // ("éé") plus suffix.
        bound_reason_to_limit(&mut reason, 7);
        assert_eq!(reason, "éé...");
        assert_eq!(reason.len(), 7);
    }

    #[test]
    fn bound_reason_to_limit_handles_tiny_limits_without_overflowing() {
        let mut reason = "abcdef".to_string();
        bound_reason_to_limit(&mut reason, 2);
        assert_eq!(reason, "ab");
        assert_eq!(reason.len(), 2);
    }

    #[test]
    fn bound_reason_to_limit_tiny_limit_with_multibyte_does_not_panic() {
        // "é" is 2 bytes (0xC3 0xA9). With max_bytes=1 (< SUFFIX.len()),
        // truncating at byte 1 would split the char and panic without the
        // char-boundary walk-back.
        let mut reason = "é".to_string();
        bound_reason_to_limit(&mut reason, 1);
        assert!(reason.is_empty());
    }
}
