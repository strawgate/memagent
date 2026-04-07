//! Async output worker pool with MRU-first work consolidation.
//!
//! # Design
//!
//! Workers are long-lived tokio tasks, each owning one [`Sink`] instance
//! (and therefore its own HTTP connection pool). Workers are kept in a
//! [`VecDeque`] ordered Most-Recently-Used first. Dispatch always tries the
//! front worker first; only when that channel is full does it try the next,
//! and so on. This **consolidates work onto the fewest active workers**,
//! keeping cold workers idle long enough to hit their `idle_timeout` and
//! self-terminate — which closes their HTTP connections.
//!
//! # Scaling
//!
//! - Under low load: 1 active worker, rest idle → eventually close.
//! - Under burst: pool spawns workers (up to `max_workers`) one at a time.
//! - At `max_workers` with all channels full: `submit` async-waits on the
//!   front worker, providing natural back-pressure to the pipeline.
//!
//! # Safety invariants
//!
//! - Every submitted [`WorkItem`] is either delivered to a worker's channel
//!   or async-waited until a worker has capacity. Items are never dropped.
//! - Every in-flight batch ticket is acked or rejected before shutdown
//!   completes. The `drain` method joins all worker tasks.
//! - Worker panic is surfaced when the pool joins worker tasks during
//!   [`OutputWorkerPool::drain`]. Closed worker channels are pruned lazily on
//!   the next submit, and worker-slot cleanup is drop-guarded so control-plane
//!   health does not retain stale live-worker state after abrupt exits.
//!
//! # Kani proofs
//!
//! Pure dispatch logic is extracted into `dispatch_step` and proved with
//! Kani.  See the `kani_proofs` module below.

mod health;

use backon::{BackoffBuilder, ExponentialBuilder};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use logfwd_io::diagnostics::{ComponentHealth, ComponentStats, PipelineMetrics};
use logfwd_output::BatchMetadata;
use logfwd_output::sink::{OutputHealthEvent, SendResult, Sink, SinkFactory};
use logfwd_types::pipeline::{BatchTicket, Sending};

use arrow::record_batch::RecordBatch;

use self::health::{
    aggregate_output_health, idle_health_after_worker_insert, reduce_worker_slot_health,
};

#[cfg(not(test))]
const DRAIN_CANCEL_GRACE: Duration = Duration::from_secs(5);
#[cfg(test)]
const DRAIN_CANCEL_GRACE: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// Public message types
// ---------------------------------------------------------------------------

/// One batch of work to send to an output sink.
///
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
pub struct AckItem {
    /// The tickets from the corresponding `WorkItem`, plus delivery outcome.
    pub tickets: Vec<BatchTicket<Sending, u64>>,
    /// `true` if the batch was successfully delivered, `false` otherwise
    /// (permanent rejection, timeout, or panic recovery).
    pub success: bool,
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

// ---------------------------------------------------------------------------
// Internal worker messages
// ---------------------------------------------------------------------------

/// Message sent from pool to worker.
enum WorkerMsg {
    /// Process this batch.
    Work(WorkItem),
    /// Finish current item (if any) then exit cleanly.
    Shutdown,
}

// ---------------------------------------------------------------------------
// Worker config (shared across all workers in a pool)
// ---------------------------------------------------------------------------

struct WorkerConfig {
    idle_timeout: Duration,
    cancel: CancellationToken,
    max_retry_delay: Duration,
    metrics: Arc<PipelineMetrics>,
    output_health: Arc<OutputHealthTracker>,
}

struct WorkerSlotCleanup {
    output_health: Arc<OutputHealthTracker>,
    worker_id: usize,
}

impl Drop for WorkerSlotCleanup {
    fn drop(&mut self) {
        self.output_health.remove_worker(self.worker_id);
    }
}

// Worker handle (held by pool)
// ---------------------------------------------------------------------------

struct WorkerHandle {
    /// Channel to send work items to this worker.
    tx: mpsc::Sender<WorkerMsg>,
}

struct OutputHealthTracker {
    outputs: Vec<Arc<ComponentStats>>,
    state: std::sync::Mutex<OutputHealthState>,
}

struct OutputHealthState {
    worker_slots: HashMap<usize, ComponentHealth>,
    idle_health: ComponentHealth,
}

impl OutputHealthTracker {
    fn new(outputs: Vec<Arc<ComponentStats>>) -> Self {
        Self {
            outputs,
            state: std::sync::Mutex::new(OutputHealthState {
                worker_slots: HashMap::new(),
                idle_health: ComponentHealth::Healthy,
            }),
        }
    }

    fn publish(&self, health: ComponentHealth) {
        for stats in &self.outputs {
            stats.set_health(health);
        }
    }

    fn aggregate(state: &OutputHealthState) -> ComponentHealth {
        aggregate_output_health(state.idle_health, state.worker_slots.values().copied())
    }

    fn insert_worker(&self, worker_id: usize, initial: ComponentHealth) -> ComponentHealth {
        let mut state = self
            .state
            .lock()
            .expect("output health tracker mutex poisoned during worker insertion");
        state.idle_health = idle_health_after_worker_insert(state.idle_health);
        state.worker_slots.insert(worker_id, initial);
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    fn apply_worker_event(&self, worker_id: usize, event: OutputHealthEvent) -> ComponentHealth {
        let mut state = self
            .state
            .lock()
            .expect("output health tracker mutex poisoned during worker event");
        let Some(current) = state.worker_slots.get(&worker_id).copied() else {
            let aggregate = Self::aggregate(&state);
            tracing::warn!(
                worker_id,
                ?event,
                "worker_pool: ignoring output health event for unknown worker slot"
            );
            return aggregate;
        };
        let next = reduce_worker_slot_health(current, event);
        state.worker_slots.insert(worker_id, next);
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    fn remove_worker(&self, worker_id: usize) -> ComponentHealth {
        let mut state = self
            .state
            .lock()
            .expect("output health tracker mutex poisoned during worker removal");
        state.worker_slots.remove(&worker_id);
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    fn has_active_workers(&self) -> bool {
        !self
            .state
            .lock()
            .expect("output health tracker mutex poisoned during worker liveness check")
            .worker_slots
            .is_empty()
    }

    fn set_pool_health(&self, health: ComponentHealth) {
        let mut state = self
            .state
            .lock()
            .expect("output health tracker mutex poisoned during pool health update");
        state.idle_health = health;
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
    }

    fn clear_workers_and_set_pool_health(&self, health: ComponentHealth) -> ComponentHealth {
        let mut state = self
            .state
            .lock()
            .expect("output health tracker mutex poisoned during forced worker clear");
        state.worker_slots.clear();
        state.idle_health = health;
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    #[cfg(test)]
    fn slot_health(&self, worker_id: usize) -> Option<ComponentHealth> {
        self.state
            .lock()
            .expect("output health tracker mutex poisoned during test slot lookup")
            .worker_slots
            .get(&worker_id)
            .copied()
    }
}

// ---------------------------------------------------------------------------
// OutputWorkerPool
// ---------------------------------------------------------------------------

/// Dynamic output worker pool.
///
/// Call [`OutputWorkerPool::submit`] from the pipeline's async loop.
/// Call [`OutputWorkerPool::drain`] during shutdown.
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
    /// Per-worker channel capacity. Capacity 2 allows one item buffered
    /// while the worker is processing another — keeps throughput high while
    /// preserving work consolidation (worker "appears full" quickly).
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
}

impl OutputWorkerPool {
    /// Create a new pool. No workers are spawned until the first `submit`.
    pub fn new(
        factory: Arc<dyn SinkFactory>,
        max_workers: usize,
        idle_timeout: Duration,
        metrics: Arc<PipelineMetrics>,
    ) -> Self {
        assert!(
            max_workers >= 1,
            "OutputWorkerPool::new: max_workers must be >= 1, got {max_workers}"
        );
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let output_health = Arc::new(OutputHealthTracker::new(
            metrics
                .outputs
                .iter()
                .map(|(_, _, stats)| Arc::clone(stats))
                .collect(),
        ));
        OutputWorkerPool {
            workers: VecDeque::with_capacity(max_workers),
            factory,
            ack_rx,
            ack_tx,
            cancel: CancellationToken::new(),
            join_set: JoinSet::new(),
            channel_capacity: 1,
            max_workers,
            idle_timeout,
            next_id: 0,
            max_retry_delay: Duration::from_secs(30),
            metrics,
            output_health,
        }
    }

    /// Submit a work item to the pool.
    ///
    /// Dispatch strategy (MRU consolidation):
    /// 1. Try each worker front-to-back via `try_send`. Closed workers
    ///    (self-terminated after idle) are pruned lazily.
    /// 2. If all existing workers are full, spawn a new worker (if under
    ///    `max_workers`) and send to it.
    /// 3. If at `max_workers` and all full, async-wait on the front worker.
    ///    This yields the tokio task until a worker drains its queue.
    ///
    /// # Panics
    ///
    /// Panics if the pool has already been drained (cancel token fired).
    pub async fn submit(&mut self, item: WorkItem) {
        if self.cancel.is_cancelled() {
            // Pool has been drained — reject the item immediately rather than
            // silently losing it. This keeps the at-least-once invariant intact
            // even for callers that mistakenly submit after drain.
            tracing::warn!("worker_pool: submit after drain, rejecting batch immediately");
            let ticket_count = item.tickets.len();
            if self
                .ack_tx
                .send(AckItem {
                    tickets: item.tickets,
                    success: false,
                    num_rows: item.num_rows,
                    submitted_at: item.submitted_at,
                    scan_ns: item.scan_ns,
                    transform_ns: item.transform_ns,
                    output_ns: 0,
                    queue_wait_ns: 0,
                    send_latency_ns: 0,
                    batch_id: item.batch_id,
                    output_name: self.factory.name().to_string(),
                })
                .is_err()
            {
                tracing::error!(
                    ticket_count,
                    "worker_pool: ack channel closed, batch lost permanently"
                );
            }
            return;
        }

        let mut msg = WorkerMsg::Work(item);

        // --- Step 1: try_send MRU-first ---
        let mut i = 0;
        while i < self.workers.len() {
            // Try to send without blocking.
            match self.workers[i].tx.try_send(msg) {
                Ok(()) => {
                    // Promote this worker to front (MRU).
                    self.workers.swap(0, i);
                    return;
                }
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    msg = returned;
                    i += 1;
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => {
                    // Worker exited (idle timeout or panic). Prune it.
                    msg = returned;
                    self.workers.remove(i);
                    // Don't increment i — next handle slid into slot i.
                }
            }
        }

        // --- Step 2: spawn a new worker if under limit ---
        if self.workers.len() < self.max_workers {
            if let Ok(handle) = self.spawn_worker() {
                // New channel: guaranteed to have space.
                let _ = handle.tx.try_send(msg);
                self.workers.push_front(handle);
                return;
            }
            // Sink factory failed — fall through to back-pressure path.
        }

        // --- Step 3: at max or spawn failed — async-wait on MRU worker ---
        if let Some(front) = self.workers.front() {
            // Clone sender to avoid holding &mut self across await.
            let tx = front.tx.clone();
            // send().await blocks until the channel has space.
            if let Err(mpsc::error::SendError(WorkerMsg::Work(item))) = tx.send(msg).await {
                // Rare race: worker closed its channel between clone and send.
                // Reject explicitly rather than silently dropping.
                let ticket_count = item.tickets.len();
                if self
                    .ack_tx
                    .send(AckItem {
                        tickets: item.tickets,
                        success: false,
                        num_rows: item.num_rows,
                        submitted_at: item.submitted_at,
                        scan_ns: item.scan_ns,
                        transform_ns: item.transform_ns,
                        output_ns: 0,
                        queue_wait_ns: 0,
                        send_latency_ns: 0,
                        batch_id: item.batch_id,
                        output_name: self.factory.name().to_string(),
                    })
                    .is_err()
                {
                    tracing::error!(
                        ticket_count,
                        "worker_pool: ack channel closed, batch lost permanently"
                    );
                }
            }
            return;
        }

        // No workers available and spawn failed — reject the item explicitly.
        // This can happen when a single-use factory is exhausted (OnceFactory
        // after its first worker exits). Silently dropping would lose the ack.
        if let WorkerMsg::Work(item) = msg {
            tracing::error!("worker_pool: no workers available, rejecting batch");
            let ticket_count = item.tickets.len();
            if self
                .ack_tx
                .send(AckItem {
                    tickets: item.tickets,
                    success: false,
                    num_rows: item.num_rows,
                    submitted_at: item.submitted_at,
                    scan_ns: item.scan_ns,
                    transform_ns: item.transform_ns,
                    output_ns: 0,
                    queue_wait_ns: 0,
                    send_latency_ns: 0,
                    batch_id: item.batch_id,
                    output_name: self.factory.name().to_string(),
                })
                .is_err()
            {
                tracing::error!(
                    ticket_count,
                    "worker_pool: ack channel closed, batch lost permanently"
                );
            }
        }
    }

    /// Try to receive any pending ack items without blocking.
    ///
    /// Call this from the pipeline's `select!` loop to advance checkpoints.
    pub fn try_recv_ack(&mut self) -> Option<AckItem> {
        self.ack_rx.try_recv().ok()
    }

    /// Returns a mutable reference to the ack receiver for use in `select!`.
    pub fn ack_rx_mut(&mut self) -> &mut mpsc::UnboundedReceiver<AckItem> {
        &mut self.ack_rx
    }

    /// Three-phase shutdown:
    ///
    /// 1. **Signal**: send `Shutdown` to all workers so they finish their
    ///    current item and exit cleanly. New items must not be submitted
    ///    after calling `drain`.
    /// 2. **Wait**: join all worker tasks (with `graceful_timeout`).
    /// 3. **Force**: cancel any tasks still running after the timeout.
    pub async fn drain(&mut self, graceful_timeout: Duration) {
        self.output_health
            .set_pool_health(ComponentHealth::Stopping);
        let mut forced_abort = false;
        // Phase 1 — signal all workers.
        // Use try_send to avoid blocking if a worker's channel is full (e.g.,
        // it is stuck in send_batch). Dropping the Sender below also signals
        // EOF, so workers that miss the Shutdown message will still exit.
        let workers = std::mem::take(&mut self.workers);
        for handle in &workers {
            let _ = handle.tx.try_send(WorkerMsg::Shutdown);
        }
        drop(workers); // Release all Senders → workers see channel closed.

        // Phase 2 — wait with timeout.
        let drain_fut = async {
            while let Some(res) = self.join_set.join_next().await {
                if let Err(e) = res {
                    if e.is_panic() {
                        tracing::error!(error = ?e, "worker_pool: worker panicked during drain");
                    }
                }
            }
        };
        if tokio::time::timeout(graceful_timeout, drain_fut)
            .await
            .is_err()
        {
            tracing::warn!(
                timeout = ?graceful_timeout,
                "worker_pool: drain timeout, cancelling workers"
            );
            // Phase 3 — fire cancellation token so workers notice at their
            // next select! poll (after their current send_batch() returns).
            // Give a brief window for in-flight batches to complete and send
            // AckItems before we force-abort — per-batch timeout in
            // process_item() is 60 s, so 5 s here catches most cases where
            // the network hung after the batch was already sent.
            self.cancel.cancel();
            let _ = tokio::time::timeout(DRAIN_CANCEL_GRACE, async {
                while let Some(res) = self.join_set.join_next().await {
                    if let Err(e) = res {
                        if e.is_panic() {
                            tracing::error!(error = ?e, "worker_pool: worker panicked");
                        }
                    }
                }
            })
            .await;
            // Any tasks still alive after the second window are truly stuck;
            // abort them. AckItems for their in-flight batches are lost —
            // callers must treat a forced drain as a hard failure.
            self.join_set.shutdown().await;
            forced_abort = true;
        }
        // After this point all workers have exited and sent their final acks.
        if forced_abort {
            self.output_health
                .clear_workers_and_set_pool_health(ComponentHealth::Stopped);
        } else {
            self.output_health.set_pool_health(ComponentHealth::Stopped);
        }
    }

    /// Spawn a new worker task and return a handle.
    fn spawn_worker(&mut self) -> io::Result<WorkerHandle> {
        let id = self.next_id;
        self.next_id += 1;
        self.output_health
            .insert_worker(id, ComponentHealth::Starting);
        let sink = match self.factory.create() {
            Ok(sink) => sink,
            Err(err) => {
                self.output_health.remove_worker(id);
                if !self.output_health.has_active_workers() {
                    self.output_health.set_pool_health(ComponentHealth::Failed);
                }
                return Err(err);
            }
        };
        let (tx, rx) = mpsc::channel::<WorkerMsg>(self.channel_capacity);
        let ack_tx = self.ack_tx.clone();
        self.output_health
            .apply_worker_event(id, OutputHealthEvent::StartupSucceeded);
        let cfg = WorkerConfig {
            cancel: self.cancel.clone(),
            idle_timeout: self.idle_timeout,
            max_retry_delay: self.max_retry_delay,
            metrics: Arc::clone(&self.metrics),
            output_health: Arc::clone(&self.output_health),
        };

        self.join_set.spawn(worker_task(id, sink, rx, ack_tx, cfg));

        Ok(WorkerHandle { tx })
    }

    /// Active worker count (workers whose channels are still open).
    ///
    /// Workers that have self-terminated are pruned lazily; this count may
    /// temporarily include workers that have exited but not yet been pruned.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

/// Long-lived tokio task that owns one `Sink` and processes `WorkItem`s.
///
/// Exits when:
/// - The `rx` channel is closed (pool dropped the sender).
/// - No item arrives within `idle_timeout` (self-terminate to free connection).
/// - The `cancel` token is fired (hard shutdown).
async fn worker_task(
    id: usize,
    mut sink: Box<dyn Sink>,
    mut rx: mpsc::Receiver<WorkerMsg>,
    ack_tx: mpsc::UnboundedSender<AckItem>,
    cfg: WorkerConfig,
) {
    let WorkerConfig {
        idle_timeout,
        cancel,
        max_retry_delay,
        metrics,
        output_health,
    } = cfg;
    let _slot_cleanup = WorkerSlotCleanup {
        output_health: Arc::clone(&output_health),
        worker_id: id,
    };
    loop {
        tokio::select! {
            biased; // check cancel first
            () = cancel.cancelled() => break,
            msg = recv_with_idle_timeout(&mut rx, idle_timeout) => {
                match msg {
                    None => break, // idle timeout or channel closed
                    Some(WorkerMsg::Shutdown) => break,
                    Some(WorkerMsg::Work(item)) => {
                        let WorkItem {
                            batch,
                            metadata,
                            tickets,
                            num_rows,
                            submitted_at,
                            scan_ns,
                            transform_ns,
                            batch_id,
                            span,
                        } = item;
                        let queue_wait_ns = submitted_at.elapsed().as_nanos() as u64;
                        // Record which worker picked up this batch for the live dashboard.
                        let now_ns = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as u64;
                        metrics.assign_worker_to_active_batch(batch_id, id, now_ns);
                        let output_span = tracing::info_span!(
                            parent: &span, "output",
                            worker_id = id,
                            send_ns   = tracing::field::Empty,
                            recv_ns   = tracing::field::Empty,
                            took_ms   = tracing::field::Empty,
                            retries   = tracing::field::Empty,
                            req_bytes = tracing::field::Empty,
                            cmp_bytes = tracing::field::Empty,
                            resp_bytes = tracing::field::Empty,
                        );
                        let (success, send_latency_ns, retries) =
                            process_item(id, &mut *sink, &output_health, batch, &metadata, max_retry_delay)
                        .instrument(output_span.clone())
                        .await;
                        output_span.record("retries", retries);
                        let output_ns = submitted_at.elapsed().as_nanos() as u64 - queue_wait_ns;
                        // Remove from active_batches immediately — don't wait for the pipeline's
                        // ack select loop, which can be starved by flush_batch.await blocking.
                        metrics.finish_active_batch(batch_id);
                        drop(span);
                        if ack_tx
                            .send(AckItem {
                                tickets,
                                success,
                                num_rows,
                                submitted_at,
                                scan_ns,
                                transform_ns,
                                output_ns,
                                queue_wait_ns,
                                send_latency_ns,
                                batch_id,
                                output_name: sink.name().to_string(),
                            })
                            .is_err()
                        {
                            tracing::warn!(
                                worker_id = id,
                                num_rows,
                                success,
                                "worker: ack channel closed, ack lost"
                            );
                        }
                    }
                }
            }
        }
    }
    // Always shut the sink down when the worker exits so resources are flushed
    // and released. Health transitions for pipeline drain are driven by the
    // pool-level drain path; idle worker expiry should not make outputs appear
    // permanently unready because the pool can respawn workers on demand.
    if let Err(e) = sink.shutdown().await {
        tracing::error!(worker_id = id, error = %e, "worker_pool: sink shutdown failed");
        metrics.output_error(sink.name());
    }
}

/// Receive with idle timeout — returns `None` on timeout or channel close.
async fn recv_with_idle_timeout(
    rx: &mut mpsc::Receiver<WorkerMsg>,
    idle_timeout: Duration,
) -> Option<WorkerMsg> {
    tokio::time::timeout(idle_timeout, rx.recv())
        .await
        .ok() // Err = timed out → None
        .flatten() // None = channel closed → None
}

/// Process one batch with retry on `RetryAfter` and server errors.
///
/// Returns `(success, send_latency_ns, retries)` where `send_latency_ns` is
/// cumulative wall time inside `sink.send_batch()` across all attempts
/// (excludes backoff sleep).
async fn process_item(
    worker_id: usize,
    sink: &mut dyn Sink,
    output_health: &OutputHealthTracker,
    batch: RecordBatch,
    metadata: &BatchMetadata,
    max_retry_delay: Duration,
) -> (bool, u64, usize) {
    const MAX_RETRIES: usize = 3; // 1 initial + 3 retries = 4 total attempts
    const BATCH_TIMEOUT_SECS: u64 = 60;

    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(max_retry_delay)
        .with_factor(2.0)
        .with_max_times(MAX_RETRIES)
        .with_jitter()
        .build();

    let mut send_latency_ns: u64 = 0;
    let mut retries_count = 0;

    loop {
        // Hard per-batch timeout: prevents one slow/broken batch from
        // tying up the worker indefinitely.
        let send_start = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(BATCH_TIMEOUT_SECS),
            sink.send_batch(&batch, metadata),
        )
        .await;
        send_latency_ns += send_start.elapsed().as_nanos() as u64;

        match result {
            Err(_elapsed) => {
                tracing::error!(
                    worker_id,
                    timeout_secs = BATCH_TIMEOUT_SECS,
                    "worker_pool: batch send timed out"
                );
                output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                return (false, send_latency_ns, retries_count);
            }
            Ok(SendResult::Ok) => {
                output_health.apply_worker_event(worker_id, OutputHealthEvent::DeliverySucceeded);
                return (true, send_latency_ns, retries_count);
            }
            Ok(SendResult::Rejected(reason)) => {
                tracing::warn!(worker_id, %reason, "worker_pool: batch rejected");
                output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                return (false, send_latency_ns, retries_count);
            }
            Ok(SendResult::RetryAfter(retry_dur)) => {
                // Server specified delay — consume a backoff slot but use
                // the server's delay (capped at max_retry_delay).
                if backoff.next().is_none() {
                    tracing::error!(
                        worker_id,
                        max_retries = MAX_RETRIES,
                        "worker_pool: RetryAfter exceeded max retries"
                    );
                    output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                    return (false, send_latency_ns, retries_count);
                }
                retries_count += 1;
                let sleep_for = retry_dur.min(max_retry_delay);
                output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                tracing::warn!(worker_id, ?sleep_for, "worker_pool: rate-limited, retrying");
                tokio::time::sleep(sleep_for).await;
            }
            Ok(SendResult::IoError(e)) => match backoff.next() {
                Some(delay) => {
                    retries_count += 1;
                    output_health.apply_worker_event(worker_id, OutputHealthEvent::Retrying);
                    tracing::warn!(
                        worker_id,
                        sleep_ms = delay.as_millis() as u64,
                        error = %e,
                        "worker_pool: transient error, retrying with jitter"
                    );
                    tokio::time::sleep(delay).await;
                }
                None => {
                    tracing::error!(
                        worker_id,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "worker_pool: gave up after retries"
                    );
                    output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                    return (false, send_latency_ns, retries_count);
                }
            },
            // Future SendResult variants (#[non_exhaustive]) — treat as failure.
            Ok(_) => {
                output_health.apply_worker_event(worker_id, OutputHealthEvent::FatalFailure);
                return (false, send_latency_ns, retries_count);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Kani formal proofs — dispatch logic
// ---------------------------------------------------------------------------
//
// The tokio runtime and channel operations cannot be modelled by Kani.
// We extract the pure dispatch *decision* into a standalone function and
// prove its invariants symbolically.  The actual pool code mirrors this
// logic exactly so the proofs transfer.

/// Abstract channel state used in Kani models.
#[cfg_attr(kani, derive(kani::Arbitrary))]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChannelState {
    /// Channel has space — `try_send` would succeed.
    HasSpace,
    /// Channel is full — `try_send` would return `Full`.
    Full,
    /// Worker has exited — `try_send` would return `Closed`.
    Closed,
}

/// Outcome of one dispatch step over an array of workers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DispatchOutcome {
    /// Item sent to worker at this index.
    SentToIndex(usize),
    /// All workers full/closed but under limit — spawn a new worker.
    SpawnNew,
    /// All workers full, at limit — must async-wait on front worker.
    WaitOnFront,
}

/// Pure dispatch algorithm (no I/O). Proved by Kani.
///
/// Scans `states` front-to-back:
/// - Closed workers are skipped (counted as pruned).
/// - The first worker with `HasSpace` receives the item.
/// - If all non-closed workers are `Full`:
///   - If `active_count < max_workers`, returns `SpawnNew`.
///   - Otherwise returns `WaitOnFront`.
///
/// Preconditions: `max_workers >= 1`, `states.len() <= max_workers`.
pub fn dispatch_step(states: &[ChannelState], max_workers: usize) -> DispatchOutcome {
    for (i, &state) in states.iter().enumerate() {
        match state {
            ChannelState::Closed => {} // prune; try next
            ChannelState::HasSpace => return DispatchOutcome::SentToIndex(i),
            ChannelState::Full => {} // try next
        }
    }
    // No worker had space. Count active (non-closed) workers.
    let active = states
        .iter()
        .filter(|&&s| s != ChannelState::Closed)
        .count();
    if active < max_workers {
        DispatchOutcome::SpawnNew
    } else {
        DispatchOutcome::WaitOnFront
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    // -----------------------------------------------------------------------
    // Proof 1: Dispatch always sends to a valid index, spawns, or waits.
    // The item is NEVER silently dropped.
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_dispatch_never_drops_item() {
        const MAX_N: usize = 4;
        let n: usize = kani::any();
        kani::assume(n <= MAX_N);

        let max_workers: usize = kani::any();
        kani::assume(max_workers >= 1);
        kani::assume(max_workers <= 8);
        // Pool invariant: never more workers than max.
        kani::assume(n <= max_workers);

        let states: [ChannelState; MAX_N] = kani::any();

        let outcome = dispatch_step(&states[..n], max_workers);

        // Guard against vacuous proofs: confirm all three arms are reachable.
        kani::cover!(
            matches!(outcome, DispatchOutcome::SentToIndex(_)),
            "SentToIndex path reachable"
        );
        kani::cover!(
            matches!(outcome, DispatchOutcome::SpawnNew),
            "SpawnNew path reachable"
        );
        kani::cover!(
            matches!(outcome, DispatchOutcome::WaitOnFront),
            "WaitOnFront path reachable"
        );

        match outcome {
            DispatchOutcome::SentToIndex(i) => {
                // Must be a valid index.
                assert!(i < n);
                // Must point to a non-closed, non-full slot.
                assert_eq!(states[i], ChannelState::HasSpace);
            }
            DispatchOutcome::SpawnNew => {
                // Must be under the limit.
                let active = states[..n]
                    .iter()
                    .filter(|&&s| s != ChannelState::Closed)
                    .count();
                assert!(active < max_workers);
                // No HasSpace worker exists (else we'd have sent to it).
                assert!(!states[..n].iter().any(|&s| s == ChannelState::HasSpace));
            }
            DispatchOutcome::WaitOnFront => {
                // Must be at the limit.
                let active = states[..n]
                    .iter()
                    .filter(|&&s| s != ChannelState::Closed)
                    .count();
                assert_eq!(active, max_workers);
                // No HasSpace worker exists.
                assert!(!states[..n].iter().any(|&s| s == ChannelState::HasSpace));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Proof 2: SentToIndex always picks the FIRST HasSpace worker (MRU-first).
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_dispatch_picks_first_available() {
        const MAX_N: usize = 4;
        let n: usize = kani::any();
        kani::assume(n > 0 && n <= MAX_N);

        let max_workers: usize = kani::any();
        kani::assume(max_workers >= n);

        let states: [ChannelState; MAX_N] = kani::any();

        let outcome = dispatch_step(&states[..n], max_workers);

        // Guard: confirm MRU path (SentToIndex) is reachable under these inputs.
        kani::cover!(
            matches!(outcome, DispatchOutcome::SentToIndex(_)),
            "SentToIndex reachable in picks_first proof"
        );

        if let DispatchOutcome::SentToIndex(i) = outcome {
            // All workers before i must be Full or Closed.
            for j in 0..i {
                assert_ne!(states[j], ChannelState::HasSpace);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Proof 3: SpawnNew only fires when no HasSpace worker exists.
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_spawn_only_when_no_space() {
        const MAX_N: usize = 4;
        let n: usize = kani::any();
        kani::assume(n <= MAX_N);

        let max_workers: usize = kani::any();
        kani::assume(max_workers >= 1 && max_workers <= 8);
        kani::assume(n <= max_workers);

        let states: [ChannelState; MAX_N] = kani::any();
        let has_space = states[..n].iter().any(|&s| s == ChannelState::HasSpace);

        let outcome = dispatch_step(&states[..n], max_workers);

        // Guard: both branches (has_space / no_space) must be reachable.
        kani::cover!(has_space, "has_space=true path exercised");
        kani::cover!(!has_space, "has_space=false path exercised");

        if has_space {
            // Must send to an existing worker, not spawn.
            assert!(!matches!(outcome, DispatchOutcome::SpawnNew));
        }
    }

    // -----------------------------------------------------------------------
    // Proof 4: WaitOnFront only fires when active == max_workers.
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_wait_only_at_capacity() {
        const MAX_N: usize = 4;
        let n: usize = kani::any();
        kani::assume(n <= MAX_N);

        let max_workers: usize = kani::any();
        kani::assume(max_workers >= 1 && max_workers <= 4);
        kani::assume(n <= max_workers);

        let states: [ChannelState; MAX_N] = kani::any();
        let active = states[..n]
            .iter()
            .filter(|&&s| s != ChannelState::Closed)
            .count();

        let outcome = dispatch_step(&states[..n], max_workers);

        // Guard: WaitOnFront must be reachable (not vacuously avoided).
        kani::cover!(
            matches!(outcome, DispatchOutcome::WaitOnFront),
            "WaitOnFront reachable in wait_only_at_capacity proof"
        );

        if matches!(outcome, DispatchOutcome::WaitOnFront) {
            assert_eq!(active, max_workers);
        }
    }

    // -----------------------------------------------------------------------
    // Proof 5: With max_workers = 1 and a Full channel,
    //          dispatch always returns WaitOnFront (backpressure).
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn verify_single_worker_full_causes_wait() {
        let states = [ChannelState::Full];
        let outcome = dispatch_step(&states, 1);
        assert_eq!(outcome, DispatchOutcome::WaitOnFront);
    }

    // -----------------------------------------------------------------------
    // Proof 6: Empty worker list with max >= 1 always triggers SpawnNew.
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn verify_empty_pool_triggers_spawn() {
        let max_workers: usize = kani::any();
        kani::assume(max_workers >= 1);
        // Guard: ensure max_workers=1 AND max_workers>1 are both reachable.
        kani::cover!(max_workers == 1, "single-worker capacity exercised");
        kani::cover!(max_workers > 1, "multi-worker capacity exercised");
        let outcome = dispatch_step(&[], max_workers);
        assert_eq!(outcome, DispatchOutcome::SpawnNew);
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Pure dispatch_step tests (no async required)
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_empty_pool_spawns() {
        assert_eq!(dispatch_step(&[], 4), DispatchOutcome::SpawnNew);
    }

    #[test]
    fn dispatch_sends_to_first_available() {
        let states = [
            ChannelState::Full,
            ChannelState::HasSpace,
            ChannelState::HasSpace,
        ];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SentToIndex(1));
    }

    #[test]
    fn dispatch_skips_closed_workers() {
        let states = [
            ChannelState::Closed,
            ChannelState::Closed,
            ChannelState::HasSpace,
        ];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SentToIndex(2));
    }

    #[test]
    fn dispatch_all_closed_spawns_new() {
        let states = [ChannelState::Closed, ChannelState::Closed];
        // active = 0, max = 4 → spawn
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SpawnNew);
    }

    #[test]
    fn dispatch_all_full_at_max_waits() {
        let states = [ChannelState::Full, ChannelState::Full];
        assert_eq!(dispatch_step(&states, 2), DispatchOutcome::WaitOnFront);
    }

    #[test]
    fn dispatch_all_full_under_max_spawns() {
        let states = [ChannelState::Full, ChannelState::Full];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SpawnNew);
    }

    #[test]
    fn dispatch_single_full_at_limit_waits() {
        assert_eq!(
            dispatch_step(&[ChannelState::Full], 1),
            DispatchOutcome::WaitOnFront
        );
    }

    #[test]
    fn dispatch_prefers_existing_over_spawn() {
        // Even though we could spawn (under limit), we should send to existing worker.
        let states = [ChannelState::HasSpace];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SentToIndex(0));
    }

    // -----------------------------------------------------------------------
    // Pool integration tests (async)
    // -----------------------------------------------------------------------

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use logfwd_io::diagnostics::ComponentHealth;
    use logfwd_output::BatchMetadata;
    use logfwd_output::sink::{SendResult, Sink, SinkFactory};
    use std::collections::VecDeque;
    use std::future::pending;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    /// A sink that counts calls and optionally simulates failures.
    struct CountingSink {
        name: String,
        calls: Arc<AtomicU32>,
        fail: bool,
        fail_shutdown: bool,
    }

    impl Sink for CountingSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let calls = self.calls.clone();
            let fail = self.fail;
            Box::pin(async move {
                calls.fetch_add(1, Ordering::Relaxed);
                if fail {
                    SendResult::IoError(io::Error::other("injected failure"))
                } else {
                    SendResult::Ok
                }
            })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            let fail_shutdown = self.fail_shutdown;
            Box::pin(async move {
                if fail_shutdown {
                    Err(io::Error::other("shutdown failed"))
                } else {
                    Ok(())
                }
            })
        }
    }

    struct CountingSinkFactory {
        calls: Arc<AtomicU32>,
        fail: bool,
        fail_shutdown: bool,
    }

    impl SinkFactory for CountingSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(CountingSink {
                name: "counting".into(),
                calls: Arc::clone(&self.calls),
                fail: self.fail,
                fail_shutdown: self.fail_shutdown,
            }))
        }

        fn name(&self) -> &'static str {
            "counting"
        }
    }

    #[derive(Clone, Debug)]
    enum ScriptedResult {
        Ok,
        RetryAfter(Duration),
        Rejected(&'static str),
    }

    struct ScriptedSink {
        name: String,
        steps: Arc<Mutex<VecDeque<ScriptedResult>>>,
    }

    impl Sink for ScriptedSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ScriptedResult::Ok);
            Box::pin(async move {
                match step {
                    ScriptedResult::Ok => SendResult::Ok,
                    ScriptedResult::RetryAfter(d) => SendResult::RetryAfter(d),
                    ScriptedResult::Rejected(msg) => SendResult::Rejected(msg.to_string()),
                }
            })
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

    struct ScriptedSinkFactory {
        name: &'static str,
        steps: Arc<Mutex<VecDeque<ScriptedResult>>>,
    }

    impl ScriptedSinkFactory {
        fn new(name: &'static str, steps: Vec<ScriptedResult>) -> Self {
            Self {
                name,
                steps: Arc::new(Mutex::new(VecDeque::from(steps))),
            }
        }
    }

    impl SinkFactory for ScriptedSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(ScriptedSink {
                name: self.name.to_string(),
                steps: Arc::clone(&self.steps),
            }))
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    struct FailingCreateFactory {
        name: &'static str,
    }

    impl SinkFactory for FailingCreateFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Err(io::Error::other("create failed"))
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    struct HangingSink {
        name: String,
        entered_send: Arc<AtomicBool>,
    }

    impl Sink for HangingSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let entered_send = Arc::clone(&self.entered_send);
            Box::pin(async move {
                entered_send.store(true, Ordering::Release);
                pending::<SendResult>().await
            })
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

    struct HangingSinkFactory {
        entered_send: Arc<AtomicBool>,
    }

    impl SinkFactory for HangingSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(HangingSink {
                name: "hanging".into(),
                entered_send: Arc::clone(&self.entered_send),
            }))
        }

        fn name(&self) -> &'static str {
            "hanging"
        }
    }

    struct PanicSink {
        name: String,
        entered_send: Arc<AtomicBool>,
    }

    impl Sink for PanicSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let entered_send = Arc::clone(&self.entered_send);
            Box::pin(async move {
                entered_send.store(true, Ordering::Release);
                panic!("injected send_batch panic");
            })
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

    struct PanicSinkFactory {
        entered_send: Arc<AtomicBool>,
    }

    impl SinkFactory for PanicSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(PanicSink {
                name: "panic".into(),
                entered_send: Arc::clone(&self.entered_send),
            }))
        }

        fn name(&self) -> &'static str {
            "panic"
        }
    }

    struct SlowCreateFactory {
        entered_create: Arc<AtomicBool>,
        release_create: Arc<AtomicBool>,
    }

    impl SinkFactory for SlowCreateFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            self.entered_create.store(true, Ordering::Release);
            while !self.release_create.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(Box::new(CountingSink {
                name: "slow-create".into(),
                calls: Arc::new(AtomicU32::new(0)),
                fail: false,
                fail_shutdown: false,
            }))
        }

        fn name(&self) -> &'static str {
            "slow-create"
        }
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap()
    }

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    // We need BatchTickets for WorkItems. Since BatchTicket::new is crate-private
    // in logfwd-core, integration tests that need tickets go through pipeline.rs.
    // Here we only test pool mechanics with empty ticket vecs (valid for pool logic,
    // since ticket acking is handled by pipeline.rs separately).
    fn empty_work_item() -> WorkItem {
        WorkItem {
            batch: make_batch(),
            metadata: make_metadata(),
            tickets: vec![],
            num_rows: 0,
            submitted_at: tokio::time::Instant::now(),
            scan_ns: 0,
            transform_ns: 0,
            batch_id: 0,
            span: tracing::Span::none(),
        }
    }

    fn test_metrics() -> Arc<PipelineMetrics> {
        {
            let meter = logfwd_test_utils::test_meter();
            Arc::new(PipelineMetrics::new("test", "", &meter))
        }
    }

    async fn wait_for_flag(flag: &AtomicBool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while !flag.load(Ordering::Acquire) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for flag to become true"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_no_active_workers(pool: &OutputWorkerPool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while pool.output_health.has_active_workers() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for worker slots to clear"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn pool_spawns_worker_on_first_submit() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 4, Duration::from_secs(60), test_metrics());
        assert_eq!(pool.worker_count(), 0);

        pool.submit(empty_work_item()).await;
        // Give the worker time to process.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Exactly one worker was created.
        assert_eq!(pool.worker_count(), 1);
        // The sink's send_batch was called once.
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_consolidates_work_on_single_worker() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        // max_workers=4, but all items should go to the same worker (MRU).
        let mut pool = OutputWorkerPool::new(factory, 4, Duration::from_secs(60), test_metrics());

        // Submit 3 items sequentially (each after the previous is received).
        for _ in 0..3 {
            pool.submit(empty_work_item()).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Only 1 worker should exist (MRU consolidation).
        assert_eq!(pool.worker_count(), 1);

        pool.drain(Duration::from_secs(5)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn pool_acks_flow_back_to_receiver() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), test_metrics());

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let ack = pool.ack_rx_mut().try_recv();
        assert!(ack.is_ok(), "expected an ack item");
        assert!(ack.unwrap().success);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_sends_failure_ack_on_sink_error() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: true,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), test_metrics());

        pool.submit(empty_work_item()).await;
        // Allow retries to exhaust (4 attempts × 100ms-400ms backoff ≈ 700ms).
        tokio::time::sleep(Duration::from_millis(2000)).await;

        let ack = pool.ack_rx_mut().try_recv();
        assert!(ack.is_ok(), "expected an ack item after retries exhausted");
        assert!(!ack.unwrap().success, "ack should indicate failure");

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_respects_max_workers() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        // max 2 workers
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), test_metrics());

        // Submit many items quickly to saturate channels.
        for _ in 0..10 {
            pool.submit(empty_work_item()).await;
        }

        assert!(
            pool.worker_count() <= 2,
            "worker count exceeded max_workers"
        );

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_drain_propagates_shutdown_error() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: true,
        });

        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("counting", "counting");
        let metrics = Arc::new(pm);

        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        pool.drain(Duration::from_secs(5)).await;

        assert_eq!(
            out_stats.errors(),
            1,
            "expected output_error to increment stats when shutdown fails"
        );
        assert_eq!(
            out_stats.health(),
            ComponentHealth::Stopped,
            "shutdown failure should not make the output look permanently failed"
        );
    }

    #[tokio::test]
    async fn pool_drain_waits_for_in_flight() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), test_metrics());

        for _ in 0..5 {
            pool.submit(empty_work_item()).await;
        }

        // drain should wait until all workers finish.
        pool.drain(Duration::from_secs(5)).await;

        // All 5 items should have been processed.
        assert_eq!(calls.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn pool_retrying_output_marks_degraded_then_recovers() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("scripted", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(ScriptedSinkFactory::new(
            "scripted",
            vec![
                ScriptedResult::RetryAfter(Duration::from_millis(40)),
                ScriptedResult::Ok,
            ],
        ));
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(out_stats.health(), ComponentHealth::Degraded);

        tokio::time::sleep(Duration::from_millis(80)).await;
        let ack = pool.ack_rx_mut().try_recv().expect("expected ack item");
        assert!(ack.success);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_rejected_output_marks_failed() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("scripted", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(ScriptedSinkFactory::new(
            "scripted",
            vec![ScriptedResult::Rejected("bad request")],
        ));
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        let ack = pool.ack_rx_mut().try_recv().expect("expected ack item");
        assert!(!ack.success);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_create_failure_marks_output_failed() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("broken", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(FailingCreateFactory { name: "broken" });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let ack = pool.ack_rx_mut().try_recv().expect("expected rejected ack");
        assert!(!ack.success);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);
    }

    #[tokio::test]
    async fn pool_drain_marks_output_stopped() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("counting", "counting");
        let metrics = Arc::new(pm);
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        pool.drain(Duration::from_secs(5)).await;

        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
    }

    #[tokio::test]
    async fn idle_worker_exit_keeps_output_ready() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("counting", "counting");
        let metrics = Arc::new(pm);
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_millis(20), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[test]
    fn output_health_tracker_keeps_worst_live_worker_state() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_a = pm.add_output("output_0", "http");
        let out_b = pm.add_output("output_1", "stdout");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_a), Arc::clone(&out_b)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        tracker.insert_worker(2, ComponentHealth::Healthy);
        tracker.apply_worker_event(1, OutputHealthEvent::Retrying);

        assert_eq!(out_a.health(), ComponentHealth::Degraded);
        assert_eq!(out_b.health(), ComponentHealth::Degraded);
        assert_eq!(tracker.slot_health(1), Some(ComponentHealth::Degraded));
        assert_eq!(tracker.slot_health(2), Some(ComponentHealth::Healthy));

        tracker.apply_worker_event(2, OutputHealthEvent::DeliverySucceeded);

        assert_eq!(out_a.health(), ComponentHealth::Degraded);
        assert_eq!(out_b.health(), ComponentHealth::Degraded);

        tracker.remove_worker(1);

        assert_eq!(out_a.health(), ComponentHealth::Healthy);
        assert_eq!(out_b.health(), ComponentHealth::Healthy);
        assert_eq!(tracker.slot_health(1), None);
        assert_eq!(tracker.slot_health(2), Some(ComponentHealth::Healthy));
    }

    #[test]
    fn output_health_tracker_keeps_pool_phase_over_late_worker_events() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        tracker.set_pool_health(ComponentHealth::Stopping);
        assert_eq!(out_stats.health(), ComponentHealth::Stopping);

        tracker.apply_worker_event(1, OutputHealthEvent::DeliverySucceeded);
        assert_eq!(out_stats.health(), ComponentHealth::Stopping);

        tracker.remove_worker(1);
        assert_eq!(out_stats.health(), ComponentHealth::Stopping);

        tracker.set_pool_health(ComponentHealth::Stopped);
        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
    }

    #[test]
    fn output_health_tracker_keeps_failed_pool_phase_on_new_worker() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.set_pool_health(ComponentHealth::Failed);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        tracker.insert_worker(1, ComponentHealth::Starting);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);
        assert_eq!(tracker.slot_health(1), Some(ComponentHealth::Starting));
    }

    #[test]
    fn output_health_tracker_ignores_unknown_worker_events() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        let aggregate = tracker.apply_worker_event(999, OutputHealthEvent::FatalFailure);

        assert_eq!(aggregate, ComponentHealth::Healthy);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);
        assert_eq!(tracker.slot_health(1), Some(ComponentHealth::Healthy));
        assert_eq!(tracker.slot_health(999), None);
    }

    #[test]
    fn output_health_tracker_force_clear_drops_stale_worker_slots() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        tracker.insert_worker(2, ComponentHealth::Healthy);
        tracker.apply_worker_event(2, OutputHealthEvent::FatalFailure);
        assert!(tracker.has_active_workers());
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        let aggregate = tracker.clear_workers_and_set_pool_health(ComponentHealth::Stopped);

        assert_eq!(aggregate, ComponentHealth::Stopped);
        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
        assert!(!tracker.has_active_workers());
        assert_eq!(tracker.slot_health(1), None);
        assert_eq!(tracker.slot_health(2), None);
    }

    #[tokio::test]
    async fn create_failure_with_live_worker_does_not_mark_output_failed() {
        #[derive(Default)]
        struct OneShotFactory {
            created: AtomicBool,
        }

        impl SinkFactory for OneShotFactory {
            fn create(&self) -> io::Result<Box<dyn Sink>> {
                if self.created.swap(true, Ordering::Relaxed) {
                    Err(io::Error::other("create failed"))
                } else {
                    Ok(Box::new(CountingSink {
                        name: "oneshot".into(),
                        calls: Arc::new(AtomicU32::new(0)),
                        fail: false,
                        fail_shutdown: false,
                    }))
                }
            }

            fn name(&self) -> &'static str {
                "oneshot"
            }
        }

        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("oneshot", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(OneShotFactory::default());
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), metrics);

        let handle = pool.spawn_worker().expect("first worker should spawn");
        pool.workers.push_front(handle);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        let err = pool.spawn_worker().err().expect("second spawn should fail");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_worker_reports_starting_while_create_is_in_flight() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("slow-create", "http");
        let metrics = Arc::new(pm);
        let entered_create = Arc::new(AtomicBool::new(false));
        let release_create = Arc::new(AtomicBool::new(false));
        let factory = Arc::new(SlowCreateFactory {
            entered_create: Arc::clone(&entered_create),
            release_create: Arc::clone(&release_create),
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        let submit = tokio::spawn(async move {
            pool.submit(empty_work_item()).await;
            pool
        });

        wait_for_flag(&entered_create).await;
        assert_eq!(out_stats.health(), ComponentHealth::Starting);

        release_create.store(true, Ordering::Release);
        let mut pool = submit.await.expect("submit task should complete");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_forced_abort_clears_stale_worker_slots() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("hanging", "http");
        let metrics = Arc::new(pm);
        let entered_send = Arc::new(AtomicBool::new(false));
        let factory = Arc::new(HangingSinkFactory {
            entered_send: Arc::clone(&entered_send),
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        wait_for_flag(&entered_send).await;
        assert!(pool.output_health.has_active_workers());
        assert_eq!(
            pool.output_health.slot_health(0),
            Some(ComponentHealth::Healthy)
        );

        pool.drain(Duration::from_millis(10)).await;

        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
        assert!(!pool.output_health.has_active_workers());
        assert_eq!(pool.output_health.slot_health(0), None);
        assert!(
            pool.ack_rx_mut().try_recv().is_err(),
            "forced-abort path should not emit a success ack for the hung batch"
        );
    }

    #[tokio::test]
    async fn worker_panic_does_not_leave_stale_output_slot() {
        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("panic", "http");
        let metrics = Arc::new(pm);
        let entered_send = Arc::new(AtomicBool::new(false));
        let factory = Arc::new(PanicSinkFactory {
            entered_send: Arc::clone(&entered_send),
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        wait_for_flag(&entered_send).await;
        wait_for_no_active_workers(&pool).await;

        assert_eq!(pool.output_health.slot_health(0), None);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }
}
