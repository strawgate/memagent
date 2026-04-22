pub struct OutputWorkerPool {
    /// Workers in MRU order. Front = most recently used.
    workers: VecDeque<WorkerHandle>,
    /// Factory for creating new sink instances.
    factory: Arc<dyn SinkFactory>,
    /// Channel to receive ack results from workers.
    ack_rx: mpsc::UnboundedReceiver<AckItem>,
    /// Sender half kept so workers can clone it.
    ack_tx: mpsc::UnboundedSender<AckItem>,
    /// Cancellation token sent to all workers.
    cancel: CancellationToken,
    /// Tracks all spawned worker tasks for clean drain.
    join_set: JoinSet<()>,
    /// Per-worker channel capacity (currently 1).
    /// Capacity 1 keeps MRU consolidation tight so busy workers appear full
    /// quickly and new work either fans out or back-pressures deterministically.
    channel_capacity: usize,
    /// Maximum number of concurrent workers.
    max_workers: usize,
    /// How long a worker waits with an empty channel before self-terminating.
    idle_timeout: Duration,
    /// Monotonically increasing worker ID counter.
    next_id: usize,
    /// Maximum retry duration for `SendResult::RetryAfter`.
    max_retry_delay: Duration,
    /// Pipeline metrics for updating active-batch worker assignment.
    metrics: Arc<PipelineMetrics>,
    /// Aggregated output health across live worker-local slots.
    output_health: Arc<OutputHealthTracker>,
    is_draining: bool,
}
