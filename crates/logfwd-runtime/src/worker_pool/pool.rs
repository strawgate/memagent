use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use logfwd_diagnostics::diagnostics::{ComponentHealth, ComponentStats, PipelineMetrics};
use logfwd_output::sink::{OutputHealthEvent, SinkFactory};

use super::health::{
    aggregate_output_health, idle_health_after_worker_insert, reduce_worker_slot_health,
};
use super::types::{AckItem, DeliveryOutcome, WorkItem, WorkerMsg};
use super::worker::worker_task;

#[cfg(test)]
use super::dispatch::{ChannelState, DispatchOutcome, dispatch_step};
#[cfg(test)]
use super::types::{MAX_REJECTION_REASON_BYTES, bound_rejection_reason};

#[cfg(not(test))]
const DRAIN_CANCEL_GRACE: Duration = Duration::from_secs(5);
#[cfg(test)]
const DRAIN_CANCEL_GRACE: Duration = Duration::from_millis(50);

// Worker config (shared across all workers in a pool)
// ---------------------------------------------------------------------------

pub(super) struct WorkerConfig {
    pub(super) idle_timeout: Duration,
    pub(super) cancel: CancellationToken,
    pub(super) max_retry_delay: Duration,
    pub(super) metrics: Arc<PipelineMetrics>,
    pub(super) output_health: Arc<OutputHealthTracker>,
}

pub(super) struct WorkerSlotCleanup {
    pub(super) output_health: Arc<OutputHealthTracker>,
    pub(super) worker_id: usize,
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

pub(super) struct OutputHealthTracker {
    outputs: Vec<Arc<ComponentStats>>,
    state: std::sync::Mutex<OutputHealthState>,
}

struct OutputHealthState {
    worker_slots: HashMap<usize, ComponentHealth>,
    idle_health: ComponentHealth,
}

impl OutputHealthTracker {
    pub(super) fn new(outputs: Vec<Arc<ComponentStats>>) -> Self {
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

    pub(super) fn apply_worker_event(
        &self,
        worker_id: usize,
        event: OutputHealthEvent,
    ) -> ComponentHealth {
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

    pub(super) fn remove_worker(&self, worker_id: usize) -> ComponentHealth {
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
            is_draining: false,
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
    /// Submits after drain are rejected immediately: the pool logs a warning
    /// and emits an [`AckItem`] with [`DeliveryOutcome::PoolClosed`] unless the
    /// ack channel is already closed too.
    pub async fn submit(&mut self, item: WorkItem) {
        if self.cancel.is_cancelled() || self.is_draining {
            // Pool has been drained — reject the item immediately rather than
            // silently losing it. This keeps the at-least-once invariant intact
            // even for callers that mistakenly submit after drain.
            tracing::warn!("worker_pool: submit after drain, rejecting batch immediately");
            let ticket_count = item.tickets.len();
            if self
                .ack_tx
                .send(AckItem {
                    tickets: item.tickets,
                    outcome: DeliveryOutcome::PoolClosed,
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
        if self.workers.len() < self.max_workers
            && let Ok(handle) = self.spawn_worker()
        {
            match handle.tx.try_send(msg) {
                Ok(()) => {
                    self.workers.push_front(handle);
                    return;
                }
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    // Extremely unlikely for a fresh channel, but preserve the work item.
                    msg = returned;
                    self.workers.push_front(handle);
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => {
                    // Worker exited before first dispatch; preserve the work item and
                    // continue into the existing back-pressure/rejection paths.
                    msg = returned;
                }
            }
        }
        // Sink factory failed — fall through to back-pressure path.

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
                        outcome: DeliveryOutcome::WorkerChannelClosed,
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
                    outcome: DeliveryOutcome::NoWorkersAvailable,
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
        // Set is_draining immediately so any concurrent submit() calls that
        // race with the start of drain are rejected, closing the window between
        // the guard check in submit() and the actual teardown below.
        self.is_draining = true;
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::PoolDrainBegin,
        )
        .await;
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
                if let Err(e) = res
                    && e.is_panic()
                {
                    tracing::error!(error = ?e, "worker_pool: worker panicked during drain");
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
                    if let Err(e) = res
                        && e.is_panic()
                    {
                        tracing::error!(error = ?e, "worker_pool: worker panicked");
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
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::PoolDrainComplete { forced_abort },
        )
        .await;
        self.cancel.cancel();
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
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Pure dispatch_step tests (no async required)
    // -----------------------------------------------------------------------

    #[test]
    fn rejection_reason_bound_keeps_short_messages() {
        let reason = "bad request".to_string();
        assert_eq!(bound_rejection_reason(reason.clone()), reason);
    }

    #[test]
    fn rejection_reason_bound_truncates_long_messages() {
        let reason = "x".repeat(MAX_REJECTION_REASON_BYTES + 32);
        let bounded = bound_rejection_reason(reason);
        assert!(bounded.ends_with("..."));
        assert!(bounded.len() <= MAX_REJECTION_REASON_BYTES);
    }

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
    use arrow::record_batch::RecordBatch;
    use logfwd_diagnostics::diagnostics::ComponentHealth;
    use logfwd_output::sink::{SendResult, Sink, SinkFactory};
    use logfwd_output::{BatchMetadata, ElasticsearchRequestMode, ElasticsearchSinkFactory};
    use std::collections::{BTreeMap, VecDeque};
    use std::fmt;
    use std::future::pending;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use tiny_http::{Header, Response, Server, StatusCode};
    use tracing::{Event, Id, Subscriber};
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

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

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct CapturedSpan {
        name: String,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct CapturedEvent {
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct TraceCapture {
        inner: Arc<TraceCaptureInner>,
    }

    #[derive(Default)]
    struct TraceCaptureInner {
        active_spans: Mutex<BTreeMap<String, CapturedSpan>>,
        closed_spans: Mutex<Vec<CapturedSpan>>,
        events: Mutex<Vec<CapturedEvent>>,
    }

    #[derive(Default)]
    struct FieldCapture(BTreeMap<String, String>);

    impl tracing::field::Visit for FieldCapture {
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl TraceCapture {
        fn closed_output_span(&self) -> Option<CapturedSpan> {
            self.inner
                .closed_spans
                .lock()
                .expect("closed span mutex poisoned")
                .iter()
                .rev()
                .find(|span| span.name == "output")
                .cloned()
        }

        fn contains_event_message(&self, needle: &str) -> bool {
            self.inner
                .events
                .lock()
                .expect("event mutex poisoned")
                .iter()
                .any(|event| event.fields.get("message").is_some_and(|msg| msg == needle))
        }

        fn contains_event_field(&self, key: &str, needle: &str) -> bool {
            self.inner
                .events
                .lock()
                .expect("event mutex poisoned")
                .iter()
                .any(|event| {
                    event
                        .fields
                        .get(key)
                        .is_some_and(|value| value.contains(needle))
                })
        }
    }

    impl<S> Layer<S> for TraceCapture
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &Id,
            _ctx: Context<'_, S>,
        ) {
            let mut visitor = FieldCapture::default();
            attrs.record(&mut visitor);
            self.inner
                .active_spans
                .lock()
                .expect("active span mutex poisoned")
                .insert(
                    format!("{id:?}"),
                    CapturedSpan {
                        name: attrs.metadata().name().to_string(),
                        fields: visitor.0,
                    },
                );
        }

        fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldCapture::default();
            values.record(&mut visitor);
            if let Some(span) = self
                .inner
                .active_spans
                .lock()
                .expect("active span mutex poisoned")
                .get_mut(&format!("{id:?}"))
            {
                span.fields.extend(visitor.0);
            }
        }

        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldCapture::default();
            event.record(&mut visitor);
            self.inner
                .events
                .lock()
                .expect("event mutex poisoned")
                .push(CapturedEvent { fields: visitor.0 });
        }

        fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
            if let Some(span) = self
                .inner
                .active_spans
                .lock()
                .expect("active span mutex poisoned")
                .remove(&format!("{id:?}"))
            {
                self.inner
                    .closed_spans
                    .lock()
                    .expect("closed span mutex poisoned")
                    .push(span);
            }
        }
    }

    fn start_elasticsearch_mock(
        status: u16,
        body: &'static str,
        expected_requests: usize,
    ) -> (String, std::thread::JoinHandle<()>) {
        let server = Server::http("127.0.0.1:0").expect("server start failed");
        let addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(addr) => addr,
            _ => panic!("expected IP listen addr"),
        };
        let content_type =
            Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();

        let handle = std::thread::spawn(move || {
            for _ in 0..expected_requests {
                let mut req = match server.recv_timeout(Duration::from_secs(10)) {
                    Ok(Some(req)) => req,
                    Ok(None) => break,
                    Err(_) => break,
                };
                let mut request_body = Vec::new();
                req.as_reader()
                    .read_to_end(&mut request_body)
                    .expect("read request body");
                let response = Response::from_string(body)
                    .with_status_code(StatusCode(status))
                    .with_header(content_type.clone());
                req.respond(response).expect("respond");
            }
        });

        (format!("http://{addr}"), handle)
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
        assert_eq!(ack.unwrap().outcome, DeliveryOutcome::Delivered);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_transient_sink_errors_hold_until_pool_cancel() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: true,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), test_metrics());

        pool.submit(empty_work_item()).await;
        // Transient errors should keep the batch in-flight; no terminal ack yet.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            pool.ack_rx_mut().try_recv().is_err(),
            "transient failures should not terminalize before cancellation"
        );

        // Drain with a short graceful window to trigger cancellation.
        pool.drain(Duration::from_millis(50)).await;

        let ack = pool.ack_rx_mut().try_recv();
        assert!(ack.is_ok(), "expected an ack item after cancellation");
        assert_eq!(
            ack.unwrap().outcome,
            DeliveryOutcome::PoolClosed,
            "ack should report pool-closed cancellation for in-flight transient failures"
        );
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
        assert_eq!(ack.outcome, DeliveryOutcome::Delivered);
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
        assert_eq!(
            ack.outcome,
            DeliveryOutcome::Rejected {
                reason: "bad request".to_string()
            }
        );
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[test]
    fn elasticsearch_400_records_status_code_and_batch_rejected_log() {
        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let (endpoint, mock_handle) = start_elasticsearch_mock(
                400,
                r#"{"error":{"type":"mapper_parsing_exception","reason":"bad field"}}"#,
                1,
            );
            let meter = logfwd_test_utils::test_meter();
            let mut pm = PipelineMetrics::new("test", "", &meter);
            let out_stats = pm.add_output("test_es", "elasticsearch");
            let factory = Arc::new(
                ElasticsearchSinkFactory::new(
                    "test_es".to_string(),
                    endpoint,
                    "logs".to_string(),
                    vec![],
                    false,
                    ElasticsearchRequestMode::Buffered,
                    Arc::clone(&out_stats),
                )
                .expect("factory creation"),
            );
            let metrics = Arc::new(pm);
            let mut pool =
                OutputWorkerPool::new(factory, 1, Duration::from_secs(60), Arc::clone(&metrics));

            pool.submit(empty_work_item()).await;

            let ack = tokio::time::timeout(Duration::from_secs(5), pool.ack_rx_mut().recv())
                .await
                .expect("timed out waiting for ack")
                .expect("ack channel closed");
            assert!(
                matches!(ack.outcome, DeliveryOutcome::Rejected { .. }),
                "expected rejected outcome, got {:?}",
                ack.outcome
            );

            pool.drain(Duration::from_secs(5)).await;
            mock_handle.join().expect("mock thread panicked");
        });

        let output_span = capture.closed_output_span().expect("closed output span");
        assert_eq!(
            output_span.fields.get("status_code"),
            Some(&"400".to_string())
        );
        assert!(
            output_span
                .fields
                .get("resp_bytes")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|v| v > 0),
            "expected response bytes on failed Elasticsearch request: {output_span:?}"
        );
        assert!(
            capture.contains_event_message("worker_pool: batch rejected"),
            "expected rejection log event"
        );
        assert!(
            capture.contains_event_field("reason", "HTTP 400"),
            "expected rejection reason to include HTTP 400"
        );
    }

    #[test]
    fn elasticsearch_503_records_status_code_and_retry_logs() {
        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let (endpoint, mock_handle) = start_elasticsearch_mock(
                503,
                r#"{"error":{"type":"unavailable_shards_exception","reason":"busy"}}"#,
                4,
            );
            let meter = logfwd_test_utils::test_meter();
            let mut pm = PipelineMetrics::new("test", "", &meter);
            let out_stats = pm.add_output("test_es", "elasticsearch");
            let factory = Arc::new(
                ElasticsearchSinkFactory::new(
                    "test_es".to_string(),
                    endpoint,
                    "logs".to_string(),
                    vec![],
                    false,
                    ElasticsearchRequestMode::Buffered,
                    Arc::clone(&out_stats),
                )
                .expect("factory creation"),
            );
            let metrics = Arc::new(pm);
            let mut pool =
                OutputWorkerPool::new(factory, 1, Duration::from_secs(60), Arc::clone(&metrics));

            pool.submit(empty_work_item()).await;

            // 503 responses are transient. They should keep retrying until
            // cancellation instead of producing RetryExhausted.
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(
                pool.ack_rx_mut().try_recv().is_err(),
                "transient 503 path should remain in-flight before cancellation"
            );

            pool.drain(Duration::from_millis(50)).await;

            let ack = tokio::time::timeout(Duration::from_secs(5), pool.ack_rx_mut().recv())
                .await
                .expect("timed out waiting for ack")
                .expect("ack channel closed");
            assert_eq!(ack.outcome, DeliveryOutcome::PoolClosed);

            mock_handle.join().expect("mock thread panicked");
        });

        let output_span = capture.closed_output_span().expect("closed output span");
        assert_eq!(
            output_span.fields.get("status_code"),
            Some(&"503".to_string())
        );
        assert!(
            capture.contains_event_message("worker_pool: transient error, retrying"),
            "expected retry log event"
        );
        assert!(
            capture.contains_event_field("error", "503"),
            "expected logged error to include HTTP 503 detail"
        );
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
        assert_eq!(ack.outcome, DeliveryOutcome::NoWorkersAvailable);
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
        if let Ok(ack) = pool.ack_rx_mut().try_recv() {
            assert_ne!(
                ack.outcome,
                DeliveryOutcome::Delivered,
                "forced-abort path must never emit a delivered ack for the hung batch"
            );
        }
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

        let ack = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(ack) = pool.ack_rx_mut().try_recv() {
                    break ack;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("panicking worker must still emit an ack");
        assert_eq!(
            ack.outcome,
            DeliveryOutcome::InternalFailure,
            "panic path must surface InternalFailure to hold checkpoint seam"
        );

        assert_eq!(pool.output_health.slot_health(0), None);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[cfg(feature = "loom-tests")]
    #[test]
    fn loom_worker_removal_vs_late_event_model_never_resurrects_slot() {
        struct LoomState {
            worker_present: bool,
            slot_gen: u64,
            slot_health: ComponentHealth,
        }

        loom::model(|| {
            let state = loom::sync::Arc::new(loom::sync::Mutex::new(LoomState {
                worker_present: true,
                slot_gen: 0,
                slot_health: ComponentHealth::Starting,
            }));

            let remove_state = loom::sync::Arc::clone(&state);
            let remove = loom::thread::spawn(move || {
                let mut state = remove_state
                    .lock()
                    .expect("loom mutex poisoned while removing worker");
                state.slot_gen += 1;
                state.worker_present = false;
            });

            let event_state = loom::sync::Arc::clone(&state);
            let event = loom::thread::spawn(move || {
                let observed_gen = {
                    let state = event_state
                        .lock()
                        .expect("loom mutex poisoned while observing worker slot");
                    state.slot_gen
                };
                let mut state = event_state
                    .lock()
                    .expect("loom mutex poisoned while applying event");
                if state.worker_present && observed_gen == state.slot_gen {
                    state.slot_health = reduce_worker_slot_health(
                        state.slot_health,
                        OutputHealthEvent::FatalFailure,
                    );
                    true
                } else {
                    false
                }
            });

            remove.join().expect("remove thread should not panic");
            let event_applied = event.join().expect("event thread should not panic");

            let state = state
                .lock()
                .expect("loom mutex poisoned while validating state");
            assert!(
                !state.worker_present,
                "late events must not resurrect removed worker state"
            );
            assert_eq!(
                state.slot_gen, 1,
                "worker removal must advance the slot generation"
            );
            let expected_health = if event_applied {
                reduce_worker_slot_health(
                    ComponentHealth::Starting,
                    OutputHealthEvent::FatalFailure,
                )
            } else {
                ComponentHealth::Starting
            };
            assert_eq!(
                state.slot_health, expected_health,
                "stale events must not mutate the resurrectable slot state"
            );
        });
    }

    #[test]
    fn deterministic_worker_removal_late_event_orders_match_expected_health() {
        #[derive(Clone, Copy)]
        enum Step {
            Observe,
            Remove,
            Apply,
        }

        struct DeterministicState {
            worker_present: bool,
            slot_gen: u64,
            slot_health: ComponentHealth,
            observed_gen: Option<u64>,
        }

        fn run_order(order: [Step; 3]) -> (bool, ComponentHealth) {
            let mut state = DeterministicState {
                worker_present: true,
                slot_gen: 0,
                slot_health: ComponentHealth::Starting,
                observed_gen: None,
            };
            let mut event_applied = false;

            for step in order {
                match step {
                    Step::Observe => {
                        state.observed_gen = Some(state.slot_gen);
                    }
                    Step::Remove => {
                        state.slot_gen = state.slot_gen.saturating_add(1);
                        state.worker_present = false;
                    }
                    Step::Apply => {
                        if state.worker_present && state.observed_gen == Some(state.slot_gen) {
                            state.slot_health = reduce_worker_slot_health(
                                state.slot_health,
                                OutputHealthEvent::FatalFailure,
                            );
                            event_applied = true;
                        }
                    }
                }
            }

            (event_applied, state.slot_health)
        }

        let expected_degraded =
            reduce_worker_slot_health(ComponentHealth::Starting, OutputHealthEvent::FatalFailure);

        // Order 1: event fully completes before removal. Health can degrade.
        let (event_applied_before_removal, health_before_removal) =
            run_order([Step::Observe, Step::Apply, Step::Remove]);
        assert!(event_applied_before_removal);
        assert_eq!(health_before_removal, expected_degraded);

        // Order 2: remove happens before event apply. Stale event must be ignored.
        let (event_applied_after_removal, health_after_removal) =
            run_order([Step::Observe, Step::Remove, Step::Apply]);
        assert!(!event_applied_after_removal);
        assert_eq!(health_after_removal, ComponentHealth::Starting);
    }
}
