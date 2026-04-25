//! Pipeline `run` / `run_async` and ack methods.

use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;

use super::checkpoint_io::flush_checkpoint_with_retry;
use super::checkpoint_policy::{TicketDisposition, default_ticket_disposition};
use super::internal_faults;
use super::{ChannelMsg, Pipeline, now_nanos};
use crate::worker_pool::AckItem;
use ffwd_io::checkpoint::SourceCheckpoint;
use ffwd_output::BatchMetadata;

#[cfg(not(feature = "turmoil"))]
use super::InputTransform;

#[cfg(feature = "turmoil")]
use super::input_poll::async_input_poll_loop;

impl Pipeline {
    /// Run the pipeline until `shutdown` is cancelled. Blocks the calling thread.
    ///
    /// Delegates to `run_async` on a tokio runtime. The sync interface exists
    /// for test convenience; production uses `run_async` directly.
    pub fn run(&mut self, shutdown: &CancellationToken) -> io::Result<()> {
        self.validate_batch_settings()?;
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        if let Ok(threads_raw) = std::env::var("TOKIO_WORKER_THREADS")
            && let Ok(threads) = threads_raw.parse::<usize>()
            && threads > 0
        {
            builder.worker_threads(threads);
        }
        builder
            .build()
            .expect("failed to create tokio runtime")
            .block_on(self.run_async(shutdown))
    }

    /// Async pipeline loop. Input threads stay on OS threads; scanning,
    /// transform, and output run in the async context with `select!`.
    ///
    /// Output uses `block_in_place` to avoid blocking the tokio runtime
    /// while ureq sends HTTP requests. This overlaps input reading with
    /// output sending across different batches.
    ///
    /// Known limitations (acceptable for migration period):
    /// - Scanner and output use block_in_place, which tells tokio to
    ///   temporarily move other tasks off this worker thread. During slow
    ///   HTTP sends, flush_interval can't fire on this worker. Goes away
    ///   when ureq is replaced with an async HTTP client.
    /// - self.inputs.drain(..) makes this method non-reentrant.
    #[ffwd_lint_attrs::cancel_safe]
    pub async fn run_async(&mut self, shutdown: &CancellationToken) -> io::Result<()> {
        self.validate_batch_settings()?;
        assert_eq!(
            self.inputs.len(),
            self.input_transforms.len(),
            "run_async: inputs ({}) and input_transforms ({}) must match",
            self.inputs.len(),
            self.input_transforms.len(),
        );
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::PipelinePhase {
                phase: crate::turmoil_barriers::PipelinePhase::Running,
            },
        )
        .await;
        // Spawn input threads. Each polls its source, parses format, and
        // sends accumulated JSON lines through a bounded channel.
        // Backpressure: when the channel is full, the input thread blocks.
        const CHANNEL_CAPACITY: usize = 16;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(CHANNEL_CAPACITY);
        self.metrics.set_channel_capacity(CHANNEL_CAPACITY as u64);

        let batch_target = self.batch_target_bytes;
        let batch_timeout = self.batch_timeout;
        let poll_interval = self.poll_interval;
        // Non-turmoil: split I/O + CPU workers via InputPipelineManager.
        // Transforms are moved to CPU workers (scan + SQL happens there).
        #[cfg(not(feature = "turmoil"))]
        let manager = {
            let transforms: Vec<InputTransform> = self.input_transforms.drain(..).collect();
            super::input_pipeline::InputPipelineManager::spawn(
                self.inputs.drain(..).collect(),
                transforms,
                tx.clone(),
                Arc::clone(&self.metrics),
                shutdown.clone(),
                batch_target,
                batch_timeout,
                poll_interval,
            )
        };

        // Turmoil: async input tasks (scan + transform inline, same ChannelMsg output).
        #[cfg(feature = "turmoil")]
        let mut input_tasks = tokio::task::JoinSet::<()>::new();
        #[cfg(feature = "turmoil")]
        for (input_index, (input, transform)) in self
            .inputs
            .drain(..)
            .zip(self.input_transforms.drain(..))
            .enumerate()
        {
            let tx = tx.clone();
            let sd = shutdown.clone();
            let metrics = Arc::clone(&self.metrics);
            input_tasks.spawn(async_input_poll_loop(
                input,
                transform,
                tx,
                metrics,
                sd,
                batch_target,
                batch_timeout,
                poll_interval,
                input_index,
            ));
        }

        drop(tx); // Drop our copy so rx.recv() returns None when all inputs exit.

        let mut heartbeat_interval = tokio::time::interval(self.batch_timeout);
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut should_drain_input_channel = true;
        loop {
            tokio::select! {
                biased;  // arms evaluated in source order; ack is first to prevent starvation

                // Receive ack items from pool workers first — highest priority so that
                // worker slots are freed promptly and back-pressure is relieved before
                // we ingest or flush more data.
                ack = self.pool.ack_rx_mut().recv() => {
                    if let Some(ack) = ack
                        && self.apply_pool_ack(ack).await {
                            shutdown.cancel();
                            should_drain_input_channel = false;
                            break;
                        }
                }

                () = shutdown.cancelled() => {
                    break;
                }

                msg = rx.recv() => {
                    if let Some(msg) = msg {
                        self.metrics.dec_channel_depth();
                        if self.submit_batch(msg, shutdown).await {
                            // Terminal processor/checkpoint paths stop accepting
                            // additional channel messages.
                            should_drain_input_channel = false;
                            break;
                        }
                    } else {
                        break;
                    }
                }

                _ = heartbeat_interval.tick() => {
                    // Heartbeat for stateful processors: send an empty batch through
                    // the chain so stateful processors can check their internal timers
                    // and emit timed-out data.
                    if !self.processors.is_empty()
                        && self.processors.iter().any(|p| p.is_stateful())
                    {
                        let empty = RecordBatch::new_empty(
                            Arc::new(arrow::datatypes::Schema::empty()),
                        );
                        let meta = BatchMetadata {
                            resource_attrs: Arc::clone(&self.resource_attrs),
                            observed_time_ns: now_nanos(),
                        };
                        match crate::processor::run_chain(&mut self.processors, empty, &meta) {
                            Ok(batches) => {
                                let total_rows: u64 =
                                    batches.iter().map(|b| b.num_rows() as u64).sum();
                                if total_rows > 0 {
                                    // TODO(#1404): submit timed-out processor output to
                                    // output pool without BatchTickets (ticketless
                                    // submission).
                                    tracing::debug!(
                                        rows = total_rows,
                                        "stateful processor emitted rows during heartbeat (not yet submitted)"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "processor error during heartbeat");
                            }
                        }
                    }
                }
            }
        }

        if should_drain_input_channel && !internal_faults::shutdown_skip_channel_drain() {
            // Drain channel messages before joining input threads.
            // This prevents deadlock during shutdown if a producer is blocked in
            // `blocking_send` while the bounded channel is full.
            while let Some(msg) = rx.recv().await {
                self.metrics.dec_channel_depth();
                if self.submit_batch(msg, shutdown).await {
                    break;
                }
            }
        } else {
            // Terminal paths intentionally stop accepting more input, but the
            // receiver must still be closed before joining producers. Otherwise
            // a producer blocked on the bounded channel can keep shutdown stuck.
            drop(rx);
        }

        // All sender clones have now been dropped, so input threads/tasks can
        // be joined without risking a channel backpressure deadlock.
        #[cfg(not(feature = "turmoil"))]
        manager.join();
        #[cfg(feature = "turmoil")]
        while let Some(result) = input_tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(error = ?e, "pipeline: input task panicked");
            }
        }

        // Cascading flush: drain all buffered state from stateful processors.
        // Each processor's flushed output is fed through downstream processors.
        if !self.processors.is_empty() {
            let meta = BatchMetadata {
                resource_attrs: Arc::clone(&self.resource_attrs),
                observed_time_ns: now_nanos(),
            };
            let flushed = crate::processor::cascading_flush(&mut self.processors, &meta);
            let total_rows: u64 = flushed.iter().map(|b| b.num_rows() as u64).sum();
            if total_rows > 0 {
                // TODO(#1404): submit flushed processor batches to output pool
                // without BatchTickets (ticketless submission). This will be
                // needed when the first stateful processor (TailSampling) is
                // implemented. For now, log that data was flushed.
                tracing::info!(
                    rows = total_rows,
                    "cascading flush emitted rows from stateful processors (not yet submitted to output)"
                );
            }
        }

        // Drain the pool: signal workers to finish current item and exit,
        // then wait up to `pool_drain_timeout` for graceful shutdown.
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::PipelinePhase {
                phase: crate::turmoil_barriers::PipelinePhase::Draining,
            },
        )
        .await;
        self.pool.drain(self.pool_drain_timeout).await;

        // Drain remaining acks that workers sent before exiting.
        while let Some(ack) = self.pool.try_recv_ack() {
            self.apply_pool_ack(ack).await;
        }

        // Transition machine: Running → Draining → Stopped.
        if let Some(machine) = self.machine.take() {
            let draining = machine.begin_drain();
            match draining.stop() {
                Ok(stopped) => {
                    // All in-flight batches resolved — persist final checkpoints.
                    if let Some(ref mut store) = self.checkpoint_store {
                        for (source_id, offset) in stopped.final_checkpoints() {
                            store.update(SourceCheckpoint {
                                source_id: source_id.0,
                                path: None, // path is metadata, not required for restore
                                offset: *offset,
                            });
                        }
                        flush_checkpoint_with_retry(store.as_mut()).await;
                    }
                }
                Err(still_draining) => {
                    let abandoned = still_draining.in_flight_count();
                    tracing::warn!(
                        in_flight = abandoned,
                        "pipeline: force-stopping with in-flight batches (checkpoint may not reflect latest delivered data)"
                    );
                    let stopped = still_draining.force_stop();
                    // Persist what we have — committed checkpoints are still
                    // valid (they only reflect contiguously-acked batches).
                    if let Some(ref mut store) = self.checkpoint_store {
                        for (source_id, offset) in stopped.final_checkpoints() {
                            store.update(SourceCheckpoint {
                                source_id: source_id.0,
                                path: None,
                                offset: *offset,
                            });
                        }
                        flush_checkpoint_with_retry(store.as_mut()).await;
                    }
                }
            }
        }

        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::PipelinePhase {
                phase: crate::turmoil_barriers::PipelinePhase::Stopped,
            },
        )
        .await;
        Ok(())
    }

    /// Apply a pool `AckItem` at the worker/checkpoint seam.
    ///
    /// Called from the `select!` loop when a pool worker finishes a batch.
    #[allow(clippy::unused_async)] // async required for turmoil barrier await
    pub(super) async fn apply_pool_ack(&mut self, ack: AckItem) -> bool {
        let batch_id = ack.batch_id;
        #[cfg(feature = "turmoil")]
        let outcome_for_event = ack.outcome.clone();
        if self
            .metrics
            .inflight_batches
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |value| value.checked_sub(1),
            )
            .is_err()
        {
            tracing::warn!(
                batch_id,
                "pipeline: received ack with zero inflight_batches counter"
            );
        }
        self.metrics.finish_active_batch(batch_id);
        if ack.outcome.is_delivered() {
            self.metrics
                .record_batch(ack.num_rows, ack.scan_ns, ack.transform_ns, ack.output_ns);
            self.metrics.record_queue_wait(ack.queue_wait_ns);
            self.metrics.record_send_latency(ack.send_latency_ns);
            self.metrics
                .record_batch_latency(ack.submitted_at.elapsed().as_nanos() as u64);
        } else {
            if ack.outcome.is_permanent_reject() {
                self.metrics.inc_dropped_batch();
            }
            self.metrics.output_error(&ack.output_name);
        }
        let (has_held, checkpoint_advances) =
            self.ack_all_tickets(ack.tickets, default_ticket_disposition(&ack.outcome));
        #[cfg(not(feature = "turmoil"))]
        let _ = &checkpoint_advances;
        #[cfg(feature = "turmoil")]
        let checkpoint_advances = {
            let mut advances = checkpoint_advances;
            advances.sort_unstable();
            advances
        };
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::AckApplied {
                batch_id,
                outcome: outcome_for_event.clone(),
                checkpoint_advances,
            },
        )
        .await;
        #[cfg(feature = "turmoil")]
        {
            use crate::turmoil_barriers::{BatchTerminalState, RuntimeBarrierEvent};
            let disposition = default_ticket_disposition(&outcome_for_event);
            match disposition {
                TicketDisposition::Ack => {
                    crate::turmoil_barriers::trigger(RuntimeBarrierEvent::BatchTerminalized {
                        batch_id,
                        terminal_state: BatchTerminalState::Acked,
                    })
                    .await;
                }
                TicketDisposition::Reject => {
                    crate::turmoil_barriers::trigger(RuntimeBarrierEvent::BatchTerminalized {
                        batch_id,
                        terminal_state: BatchTerminalState::Rejected,
                    })
                    .await;
                }
                TicketDisposition::Hold => {
                    crate::turmoil_barriers::trigger(RuntimeBarrierEvent::BatchHeld { batch_id })
                        .await;
                }
            }
        }
        has_held
    }

    /// Finalize Sending tickets and apply receipts to the machine when present.
    /// When a checkpoint advances, the new offset is persisted to the store.
    /// Flushes are throttled to at most once per 5 seconds to avoid fsync storms.
    pub(super) fn ack_all_tickets(
        &mut self,
        tickets: Vec<ffwd_types::pipeline::BatchTicket<ffwd_types::pipeline::Sending, u64>>,
        disposition: TicketDisposition,
    ) -> (bool, Vec<(u64, u64)>) {
        let Some(ref mut machine) = self.machine else {
            return (false, Vec::new());
        };
        let mut any_advanced = false;
        let mut held = 0usize;
        let mut advances = Vec::new();
        for ticket in tickets {
            let receipt = match disposition {
                TicketDisposition::Ack => Some(ticket.ack()),
                TicketDisposition::Reject => Some(ticket.reject()),
                TicketDisposition::Hold => {
                    // Convert Sending -> Queued to satisfy the typestate
                    // contract without acknowledging the batch. We
                    // intentionally do not re-dispatch yet; the machine keeps
                    // this batch in-flight so checkpoints do not advance.
                    self.held_tickets.push(ticket.fail());
                    held += 1;
                    None
                }
            };
            if let Some(receipt) = receipt {
                let advance = machine.apply_ack(receipt);
                if advance.advanced
                    && let Some(offset) = advance.checkpoint
                {
                    advances.push((advance.source.0, offset));
                    if let Some(ref mut store) = self.checkpoint_store {
                        store.update(SourceCheckpoint {
                            source_id: advance.source.0,
                            path: None, // path is metadata, not required for restore
                            offset,
                        });
                    }
                    any_advanced = true;
                }
            }
        }
        if held > 0 {
            tracing::warn!(
                held_tickets = held,
                "pipeline: terminal hold requested; stopping ingestion so checkpoints do not advance past undelivered data"
            );
        }
        // Flush to disk at most once per checkpoint_flush_interval to amortize fsync cost.
        // Advance the timer even on failure to prevent retry flooding.
        if any_advanced && self.last_checkpoint_flush.elapsed() >= self.checkpoint_flush_interval {
            self.last_checkpoint_flush = tokio::time::Instant::now();
            if let Some(ref mut store) = self.checkpoint_store
                && let Err(e) = store.flush()
            {
                tracing::warn!(error = %e, "pipeline: checkpoint flush error");
            }
        }
        (held > 0, advances)
    }
}
