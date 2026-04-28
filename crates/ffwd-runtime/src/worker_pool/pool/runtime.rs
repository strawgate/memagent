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
            let Some(tx) = self.workers.get(i).map(|worker| worker.tx.clone()) else {
                break;
            };
            match tx.try_send(msg) {
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
