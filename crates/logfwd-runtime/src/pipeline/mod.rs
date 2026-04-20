//! Pipeline: inputs → per-input Scanner + SQL → processors → output sinks.
//!
//! Each input gets two dedicated OS threads: an I/O worker (polls source,
//! accumulates bytes) and a CPU worker (scans to RecordBatch, runs SQL).
//! See `input_pipeline` for the I/O/compute separation architecture.

mod build;
mod checkpoint_io;
mod checkpoint_policy;
mod health;
mod input_build;
pub(crate) mod input_pipeline;
mod input_poll;
mod internal_faults;
mod processor_stage;
mod submit;

use self::checkpoint_io::flush_checkpoint_with_retry;
use self::checkpoint_policy::{TicketDisposition, default_ticket_disposition};
#[cfg(feature = "turmoil")]
use self::input_poll::async_input_poll_loop;

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "turmoil"))]
use bytes::Bytes;
use bytes::BytesMut;

use arrow::record_batch::RecordBatch;
use logfwd_arrow::Scanner;

use crate::processor::Processor;
use crate::transform::SqlTransform;
use crate::worker_pool::{AckItem, OutputWorkerPool};
use logfwd_diagnostics::diagnostics::{ComponentHealth, ComponentStats, PipelineMetrics};
#[cfg(test)]
use logfwd_io::checkpoint::FileCheckpointStore;
use logfwd_io::checkpoint::{CheckpointStore, SourceCheckpoint};
#[cfg(test)]
use logfwd_io::format::FormatDecoder;
#[cfg(test)]
use logfwd_io::input::InputEvent;
use logfwd_io::input::InputSource;
use logfwd_io::tail::ByteOffset;
#[cfg(feature = "turmoil")]
use logfwd_output::SinkFactory;
#[cfg(test)]
use logfwd_output::build_sink_factory_v2;
use logfwd_output::{BatchMetadata, OnceAsyncFactory};
use logfwd_types::pipeline::{PipelineMachine, Running, SourceId};
use logfwd_types::source_metadata::{SourceMetadataPlan, SourcePathColumn};
use tokio_util::sync::CancellationToken;

fn source_metadata_style_source_path(
    style: logfwd_config::SourceMetadataStyle,
) -> SourcePathColumn {
    match style {
        logfwd_config::SourceMetadataStyle::Ecs => SourcePathColumn::Ecs,
        logfwd_config::SourceMetadataStyle::Otel => SourcePathColumn::Otel,
        logfwd_config::SourceMetadataStyle::Vector => SourcePathColumn::Vector,
        logfwd_config::SourceMetadataStyle::None
        | logfwd_config::SourceMetadataStyle::Fastforward => SourcePathColumn::None,
    }
}

fn source_metadata_style_needs_source_paths(style: logfwd_config::SourceMetadataStyle) -> bool {
    source_metadata_style_source_path(style)
        .to_column_name()
        .is_some()
}

// ---------------------------------------------------------------------------
// block_in_place shim for simulation
// ---------------------------------------------------------------------------

/// Scan a batch. Under `turmoil` feature (single-thread runtime), calls scan
/// directly since `block_in_place` panics. In production, scanning is done
/// by the CPU worker on its own OS thread (see `input_pipeline.rs`).
#[cfg(feature = "turmoil")]
#[inline]
fn scan_maybe_blocking(
    scanner: &mut Scanner,
    buf: Bytes,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    #[cfg(feature = "turmoil")]
    {
        scanner.scan(buf)
    }
    #[cfg(not(feature = "turmoil"))]
    {
        tokio::task::block_in_place(|| scanner.scan(buf))
    }
}

// ---------------------------------------------------------------------------
// Channel message types
// ---------------------------------------------------------------------------

/// Message from input workers to pipeline async loop.
///
/// Contains a scanned and SQL-transformed RecordBatch, ready for
/// the processor chain and output pool. Both production (CPU worker)
/// and simulation (turmoil async loop) produce this same message type.
pub(crate) struct ChannelMsg {
    pub batch: RecordBatch,
    pub checkpoints: HashMap<SourceId, ByteOffset>,
    pub queued_at: Option<tokio::time::Instant>,
    #[allow(dead_code)] // Used for tracing/debugging; not read in submit_batch.
    pub input_index: usize,
    pub scan_ns: u64,
    pub transform_ns: u64,
}

// ---------------------------------------------------------------------------
// Per-input state
// ---------------------------------------------------------------------------

/// Per-input scanning + SQL transform pair.
///
/// Each input gets its own `Scanner` (driven by `ScanConfig` derived from
/// its SQL) and `SqlTransform`. This enables per-input field extraction,
/// filtering, and schema reshaping with natural predicate pushdown.
struct InputTransform {
    scanner: Scanner,
    transform: SqlTransform,
    input_name: String,
    #[cfg_attr(feature = "turmoil", allow(dead_code))]
    source_metadata_plan: SourceMetadataPlan,
}

/// Consecutive scanned rows that came from the same input/source metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowOriginSpan {
    pub source_id: Option<SourceId>,
    pub input_name: Arc<str>,
    pub rows: usize,
}

struct InputState {
    /// The input source, wrapped in FramedInput for line framing + format processing.
    /// The pipeline receives scanner-ready bytes — it doesn't know about formats.
    source: Box<dyn InputSource>,
    /// Buffer accumulating scanner-ready bytes for batching.
    buf: BytesMut,
    /// Row-origin spans for the scanner-ready bytes currently in `buf`.
    #[cfg_attr(feature = "turmoil", allow(dead_code))]
    row_origins: Vec<RowOriginSpan>,
    /// Source path snapshots for source IDs represented in `row_origins`.
    #[cfg_attr(feature = "turmoil", allow(dead_code))]
    source_paths: HashMap<SourceId, String>,
    /// Input metrics (used for parse/rotation/truncation observability).
    stats: Arc<ComponentStats>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// A single pipeline: inputs → per-input Scanner + SQL transform → async output pool.
pub struct Pipeline {
    name: String,
    inputs: Vec<InputState>,
    /// Per-input Scanner + SqlTransform pairs. One per input config,
    /// indexed by the same order as `inputs`.
    input_transforms: Vec<InputTransform>,
    /// Optional post-transform processor chain (e.g., tail-based sampling).
    /// Batches flow through each processor in order. Empty vec = no processors.
    processors: Vec<Box<dyn Processor>>,
    /// Async worker pool. Workers own the actual sink connections; the pipeline
    /// submits `WorkItem`s and receives `AckItem`s for checkpoint advancement.
    pool: OutputWorkerPool,
    metrics: Arc<PipelineMetrics>,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    /// Static OTLP resource attributes (e.g. `service.name`) emitted with every batch.
    resource_attrs: Arc<Vec<(String, String)>>,
    /// Batch lifecycle state machine. Option because begin_drain() consumes self.
    /// Some during run_async, None only after shutdown drain transition.
    machine: Option<PipelineMachine<Running, u64>>,
    /// Durable checkpoint store. None when running without persistence (tests).
    checkpoint_store: Option<Box<dyn CheckpointStore>>,
    /// Tickets for batches that failed after send ownership was transferred.
    ///
    /// The runtime cannot requeue these without also retaining payload ownership,
    /// but it must keep the queued ticket alive so the lifecycle machine keeps
    /// the batch unresolved and checkpoints do not advance past undelivered data.
    held_tickets: Vec<logfwd_types::pipeline::BatchTicket<logfwd_types::pipeline::Queued, u64>>,
    /// Throttle checkpoint flushes to at most once per this interval.
    /// Uses tokio::time::Instant so the throttle works correctly under
    /// both real and simulated (Turmoil) time.
    last_checkpoint_flush: tokio::time::Instant,
    /// Checkpoint flush throttle interval. Default 5 seconds; overridable for tests.
    checkpoint_flush_interval: Duration,
    /// Maximum time to wait for worker-pool graceful drain during shutdown.
    pool_drain_timeout: Duration,
}

impl Pipeline {
    /// Replace the output sink with an async sink implementation.
    ///
    /// Wraps the sink in a single-worker pool via [`OnceAsyncFactory`].
    pub fn with_sink(mut self, sink: Box<dyn logfwd_output::Sink>) -> Self {
        let name = self.name.clone();
        let factory = Arc::new(OnceAsyncFactory::new(name, sink));
        self.pool = OutputWorkerPool::new(factory, 1, Duration::MAX, Arc::clone(&self.metrics));
        self
    }

    /// Add an input source for testing. Bypasses config-based input construction.
    ///
    /// Each new input gets its own passthrough `Scanner + SqlTransform` pair
    /// (`SELECT * FROM logs`) to keep `input_transforms` in sync with `inputs`.
    pub fn with_input(mut self, name: &str, source: Box<dyn InputSource>) -> Self {
        let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
        self.inputs.push(InputState {
            source,
            buf: BytesMut::with_capacity(self.batch_target_bytes),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            stats,
        });
        // Keep input_transforms in sync: one transform per input.
        while self.input_transforms.len() < self.inputs.len() {
            let transform =
                SqlTransform::new("SELECT * FROM logs").expect("default passthrough SQL");
            let scanner = Scanner::new(transform.scan_config());
            self.input_transforms.push(InputTransform {
                scanner,
                transform,
                input_name: name.to_string(),
                source_metadata_plan: SourceMetadataPlan::default(),
            });
        }
        self
    }

    /// Replace the checkpoint store. Useful for injecting an in-memory store in tests.
    pub fn with_checkpoint_store(mut self, store: Box<dyn CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Add a post-transform processor to the chain.
    ///
    /// # Panics
    ///
    /// Panics if `processor.is_stateful()` returns `true`. Stateful processors
    /// require deferred-ACK checkpointing support that is not yet implemented
    /// (tracked in #1404). Register only stateless processors until then.
    pub fn with_processor(mut self, processor: Box<dyn Processor>) -> Self {
        assert!(
            !processor.is_stateful(),
            "stateful processors are not yet supported: checkpointing path is incomplete \
             (see #1404). Register only stateless processors."
        );
        self.processors.push(processor);
        self
    }

    /// Append a chain of post-transform processors to any already registered via
    /// `with_processor`.  Calling `with_processor(a).with_processors(vec![b, c])`
    /// produces the three-stage chain `[a, b, c]`.
    ///
    /// # Panics
    ///
    /// Panics if any processor in `processors` returns `is_stateful() == true`.
    /// See [`with_processor`](Self::with_processor) for details.
    pub fn with_processors(mut self, processors: Vec<Box<dyn Processor>>) -> Self {
        for p in &processors {
            assert!(
                !p.is_stateful(),
                "stateful processors are not yet supported: checkpointing path is incomplete \
                 (see #1404). Register only stateless processors."
            );
        }
        self.processors.extend(processors);
        self
    }

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    /// Validate the SQL plan by running a probe batch through each
    /// per-input transform.
    ///
    /// Called by `dry-run` to surface planning errors (duplicate aliases,
    /// bad window specs, etc.) before the first real batch arrives.
    pub fn validate_sql_plan(&mut self) -> Result<(), String> {
        for it in &mut self.input_transforms {
            it.transform
                .validate_plan()
                .map_err(|e| format!("input '{}': {e}", it.input_name))?;
        }
        Ok(())
    }

    /// Override the batch flush timeout (for testing).
    pub fn set_batch_timeout(&mut self, timeout: Duration) {
        self.batch_timeout = timeout;
    }

    /// Override the batch target size in bytes (for testing).
    ///
    /// Must be called BEFORE `with_input()` since input buffers are sized
    /// from `batch_target_bytes`.
    #[cfg(feature = "turmoil")]
    pub fn set_batch_target_bytes(&mut self, bytes: usize) {
        self.batch_target_bytes = bytes;
    }

    fn validate_batch_settings(&self) -> io::Result<()> {
        if self.batch_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch_timeout must be greater than zero",
            ));
        }
        if self.batch_target_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch_target_bytes must be greater than zero",
            ));
        }
        Ok(())
    }

    /// Override the output worker-pool drain timeout (default 60s).
    ///
    /// Tests can shorten this to keep deterministic simulations fast while
    /// still exercising cancellation/force-stop behavior.
    pub fn set_pool_drain_timeout(&mut self, timeout: Duration) {
        self.pool_drain_timeout = timeout;
    }

    /// Create a minimal pipeline for simulation testing.
    ///
    /// Bypasses config parsing, filesystem, and OTel meter setup.
    /// Uses default scanner (JSON passthrough), identity SQL transform,
    /// and the provided sink.
    #[cfg(feature = "turmoil")]
    pub fn for_simulation(name: &str, sink: Box<dyn logfwd_output::Sink>) -> Self {
        let meter = opentelemetry::global::meter("test");
        let metrics = Arc::new(PipelineMetrics::new(name, "SELECT * FROM logs", &meter));
        let factory = Arc::new(OnceAsyncFactory::new(name.to_string(), sink));
        let pool = OutputWorkerPool::new(factory, 1, Duration::MAX, Arc::clone(&metrics));

        // Start with empty inputs and input_transforms so that each with_input()
        // call adds exactly one entry to both, keeping input_index in sync with
        // the transform and accumulator vectors in run_async.
        Pipeline {
            name: name.to_string(),
            inputs: vec![],
            input_transforms: vec![],
            processors: vec![],
            pool,
            metrics,
            batch_target_bytes: 64 * 1024,
            batch_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
            resource_attrs: Arc::new(vec![]),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store: None,
            held_tickets: Vec::new(),
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: build::DEFAULT_CHECKPOINT_FLUSH_INTERVAL,
            pool_drain_timeout: Duration::from_secs(60),
        }
    }

    /// Create a pipeline for simulation testing with a custom sink factory
    /// and configurable worker count. Enables multi-worker testing for
    /// out-of-order ack scenarios.
    #[cfg(feature = "turmoil")]
    pub fn for_simulation_with_factory(
        name: &str,
        factory: Arc<dyn SinkFactory>,
        max_workers: usize,
    ) -> Self {
        let meter = opentelemetry::global::meter("test");
        let metrics = Arc::new(PipelineMetrics::new(name, "SELECT * FROM logs", &meter));
        let pool = OutputWorkerPool::new(factory, max_workers, Duration::MAX, Arc::clone(&metrics));

        // Start with empty inputs and input_transforms so that each with_input()
        // call adds exactly one entry to both, keeping input_index in sync with
        // the transform and accumulator vectors in run_async.
        Pipeline {
            name: name.to_string(),
            inputs: vec![],
            input_transforms: vec![],
            processors: vec![],
            pool,
            metrics,
            batch_target_bytes: 64 * 1024,
            batch_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
            resource_attrs: Arc::new(vec![]),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store: None,
            held_tickets: Vec::new(),
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: build::DEFAULT_CHECKPOINT_FLUSH_INTERVAL,
            pool_drain_timeout: Duration::from_secs(60),
        }
    }

    /// Number of in-flight batches tracked by the state machine.
    /// Useful for test assertions on shutdown completeness.
    #[cfg(feature = "turmoil")]
    #[allow(clippy::redundant_closure_for_method_calls)]
    pub fn in_flight_count(&self) -> usize {
        self.machine.as_ref().map_or(0, |m| m.in_flight_count())
    }

    /// Override the checkpoint flush interval (default 5s). For testing
    /// checkpoint persistence timing without waiting real seconds.
    pub fn set_checkpoint_flush_interval(&mut self, interval: Duration) {
        self.checkpoint_flush_interval = interval;
    }

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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(16);

        let batch_target = self.batch_target_bytes;
        let batch_timeout = self.batch_timeout;
        let poll_interval = self.poll_interval;
        // Non-turmoil: split I/O + CPU workers via InputPipelineManager.
        // Transforms are moved to CPU workers (scan + SQL happens there).
        #[cfg(not(feature = "turmoil"))]
        let manager = {
            let transforms: Vec<InputTransform> = self.input_transforms.drain(..).collect();
            input_pipeline::InputPipelineManager::spawn(
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
    async fn apply_pool_ack(&mut self, ack: AckItem) -> bool {
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
        let (has_held, _checkpoint_advances) =
            self.ack_all_tickets(ack.tickets, default_ticket_disposition(&ack.outcome));
        #[cfg(feature = "turmoil")]
        let _checkpoint_advances = {
            let mut advances = _checkpoint_advances;
            advances.sort_unstable();
            advances
        };
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::AckApplied {
                batch_id,
                outcome: outcome_for_event,
                checkpoint_advances: _checkpoint_advances,
            },
        )
        .await;
        has_held
    }

    /// Finalize Sending tickets and apply receipts to the machine when present.
    /// When a checkpoint advances, the new offset is persisted to the store.
    /// Flushes are throttled to at most once per 5 seconds to avoid fsync storms.
    fn ack_all_tickets(
        &mut self,
        tickets: Vec<logfwd_types::pipeline::BatchTicket<logfwd_types::pipeline::Sending, u64>>,
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

// ---------------------------------------------------------------------------
// Input construction
// ---------------------------------------------------------------------------

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::Ordering;
    // std::time::Instant is cfg-gated in the parent module for turmoil compatibility,
    // but tests need it for timeout deadlines regardless of the turmoil feature flag.
    use serial_test::serial;
    use std::time::Instant;

    use arrow::record_batch::RecordBatch;
    use logfwd_config::{
        CompressionFormat, Format, OtlpProtocol, OutputConfig, OutputConfigV2, OutputType,
    };
    use logfwd_core::scan_config::ScanConfig;
    use logfwd_diagnostics::diagnostics::ComponentStats;
    use logfwd_output::{
        BatchMetadata,
        sink::{SendResult, Sink},
    };
    use logfwd_test_utils::sinks::{DevNullSink, FailingSink, FrozenSink, SlowSink};
    use logfwd_test_utils::test_meter;

    struct PanicSink;

    impl Sink for PanicSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            Box::pin(async move {
                panic!("injected panic for held-ticket shutdown test");
            })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            "panic"
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn test_build_sink_factory_stdout() {
        let cfg = OutputConfig {
            name: Some("test".to_string()),
            output_type: OutputType::Stdout,
            format: Some(Format::Json),
            ..Default::default()
        };
        let typed = OutputConfigV2::from(&cfg);
        let factory =
            build_sink_factory_v2("test", &typed, None, Arc::new(ComponentStats::new())).unwrap();
        assert_eq!(factory.name(), "test");
        let sink = factory.create().expect("create should succeed");
        assert_eq!(sink.name(), "test");
    }

    #[test]
    fn test_build_sink_factory_otlp() {
        let cfg = OutputConfig {
            name: Some("otel".to_string()),
            output_type: OutputType::Otlp,
            endpoint: Some("http://localhost:4318".to_string()),
            protocol: Some(OtlpProtocol::Http),
            compression: Some(CompressionFormat::Zstd),
            ..Default::default()
        };
        let typed = OutputConfigV2::from(&cfg);
        let factory =
            build_sink_factory_v2("otel", &typed, None, Arc::new(ComponentStats::new())).unwrap();
        assert_eq!(factory.name(), "otel");
    }

    #[test]
    fn test_build_sink_factory_missing_endpoint() {
        let cfg = OutputConfig {
            name: Some("bad".to_string()),
            output_type: OutputType::Otlp,
            ..Default::default()
        };
        let typed = OutputConfigV2::from(&cfg);
        let result = build_sink_factory_v2("bad", &typed, None, Arc::new(ComponentStats::new()));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("endpoint"), "got: {err}");
    }

    #[test]
    fn test_pipeline_from_config() {
        // Write a temp JSON file so the tailer has something to open.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn test_pipeline_from_config_with_transform() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level = 'ERROR'"
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn test_pipeline_from_config_per_input_sql() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
        sql: "SELECT level FROM logs WHERE level = 'ERROR'"
    outputs:
      - type: stdout
        format: json
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());

        let pipeline = pipeline.unwrap();
        assert_eq!(pipeline.input_transforms.len(), 1);
    }

    #[test]
    fn test_pipeline_from_config_per_input_sql_overrides_transform() {
        let dir = tempfile::tempdir().unwrap();
        let log1 = dir.path().join("app.log");
        let log2 = dir.path().join("sys.log");
        std::fs::write(&log1, b"{\"level\":\"INFO\"}\n").unwrap();
        std::fs::write(&log2, b"{\"level\":\"WARN\"}\n").unwrap();

        let yaml = format!(
            r#"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
        sql: "SELECT level FROM logs WHERE level = 'ERROR'"
      - type: file
        path: {}
        format: json
    transform: "SELECT * FROM logs WHERE level != 'DEBUG'"
    outputs:
      - type: stdout
        format: json
"#,
            log1.display(),
            log2.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());

        let pipeline = pipeline.unwrap();
        // Two inputs → two transforms
        assert_eq!(pipeline.input_transforms.len(), 2);
    }

    #[test]
    fn test_pipeline_from_config_propagates_batch_tuning() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r"
pipelines:
  default:
    inputs:
      - type: file
        path: {}
        format: json
    outputs:
      - type: stdout
        format: json
    batch_target_bytes: 8192
    batch_timeout_ms: 250
    poll_interval_ms: 42
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        assert_eq!(pipeline.batch_target_bytes, 8192);
        assert_eq!(pipeline.batch_timeout, Duration::from_millis(250));
        assert_eq!(pipeline.poll_interval, Duration::from_millis(42));
    }

    #[test]
    fn run_rejects_zero_batch_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline.batch_timeout = Duration::ZERO;
        let shutdown = CancellationToken::new();
        let err = pipeline
            .run(&shutdown)
            .expect_err("zero batch_timeout should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("batch_timeout"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn run_rejects_zero_batch_target_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline.batch_target_bytes = 0;
        let shutdown = CancellationToken::new();
        let err = pipeline
            .run(&shutdown)
            .expect_err("zero batch_target_bytes should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("batch_target_bytes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_pipeline_from_config_generator_record_profile() {
        let yaml = r#"
input:
  type: generator
  generator:
    events_per_sec: 25000
    batch_size: 1024
    profile: record
    attributes:
      benchmark_id: run-123
      pod_name: emitter-0
      stream_id: emitter-0
    sequence:
      field: seq
    event_created_unix_nano_field: event_created_unix_nano
output:
  type: "null"
"#;
        let config = logfwd_config::Config::load_str(yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
            .unwrap_or_else(|err| panic!("unexpected pipeline build error: {err}"));
        let events = pipeline.inputs[0].source.poll().unwrap();
        let mut bytes = Vec::new();
        for event in &events {
            match event {
                InputEvent::Data {
                    bytes: event_bytes, ..
                } => bytes.extend_from_slice(event_bytes),
                _ => panic!("expected generator data event"),
            }
        }
        let mut scanner = Scanner::new(ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
        });
        let batch = scanner.scan_detached(Bytes::from(bytes)).unwrap();
        let benchmark_id = batch
            .column_by_name("benchmark_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let pod_name = batch
            .column_by_name("pod_name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let stream_id = batch
            .column_by_name("stream_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let seq = batch
            .column_by_name("seq")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(benchmark_id.value(0), "run-123");
        assert_eq!(pod_name.value(0), "emitter-0");
        assert_eq!(stream_id.value(0), "emitter-0");
        assert_eq!(seq.value(0), 1);
        assert!(batch.column_by_name("event_created_unix_nano").is_some());
    }

    #[test]
    fn test_pipeline_from_config_file_output() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let output_path = dir.path().join("capture.ndjson");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: file
  path: {}
  format: json
",
            log_path.display(),
            output_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None);
        assert!(pipeline.is_ok(), "got: {:?}", pipeline.err());
    }

    #[test]
    fn otlp_structured_ingress_is_disabled_when_line_capture_is_required() {
        let scan_config = ScanConfig {
            line_field_name: Some(logfwd_types::field_names::BODY.to_string()),
            ..ScanConfig::default()
        };
        assert!(!input_build::otlp_uses_structured_ingress(&scan_config));
    }

    #[test]
    fn otlp_structured_ingress_is_enabled_when_line_capture_is_not_required() {
        let scan_config = ScanConfig {
            line_field_name: None,
            ..ScanConfig::default()
        };
        assert!(input_build::otlp_uses_structured_ingress(&scan_config));
    }

    #[test]
    fn test_pipeline_with_processor() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Write 4 lines.
        let mut data = String::new();
        for i in 0..4 {
            data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Custom processor that drops rows where seq is odd.
        #[derive(Debug)]
        struct DropOddProcessor;
        impl Processor for DropOddProcessor {
            fn process(
                &mut self,
                batch: RecordBatch,
                _meta: &BatchMetadata,
            ) -> Result<smallvec::SmallVec<[RecordBatch; 1]>, crate::processor::ProcessorError>
            {
                // Drop rows where seq is odd
                let seq_array = batch
                    .column_by_name("seq")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let mut indices = vec![];
                for i in 0..seq_array.len() {
                    if seq_array.value(i) % 2 == 0 {
                        indices.push(i as u32);
                    }
                }
                let indices_array = arrow::array::UInt32Array::from(indices);
                let filtered = arrow::compute::take_record_batch(&batch, &indices_array).unwrap();
                Ok(smallvec::smallvec![filtered])
            }

            fn flush(&mut self) -> smallvec::SmallVec<[RecordBatch; 1]> {
                smallvec::SmallVec::new()
            }

            fn name(&self) -> &'static str {
                "drop_odd"
            }

            fn is_stateful(&self) -> bool {
                false
            }
        }

        pipeline = pipeline.with_processor(Box::new(DropOddProcessor));

        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();
        let metrics = Arc::clone(&pipeline.metrics);

        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let errors = metrics.transform_errors.load(Ordering::Relaxed);
                let dropped = metrics.dropped_batches_total.load(Ordering::Relaxed);
                if errors > 0 || dropped > 0 || Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            sd_clone.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        // Input reads 4 lines, but transform output drops 2 lines via processor.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_in, 4,
            "expected transform_in to be 4, got {}",
            lines_in
        );

        let lines_out = pipeline
            .metrics
            .transform_out
            .lines_total
            .load(Ordering::Relaxed);
        assert_eq!(
            lines_out, 2,
            "expected transform_out to be 2, got {}",
            lines_out
        );
    }

    #[test]
    fn test_pipeline_run_one_batch() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Write JSON lines.
        let mut data = String::new();
        for i in 0..10 {
            data.push_str(&format!(
                r#"{{"level":"INFO","msg":"hello {}","status":{}}}"#,
                i,
                200 + i
            ));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Use a very short batch timeout so the test flushes quickly.
        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();
        let metrics = Arc::clone(&pipeline.metrics);

        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let errors = metrics.transform_errors.load(Ordering::Relaxed);
                let dropped = metrics.dropped_batches_total.load(Ordering::Relaxed);
                if errors > 0 || dropped > 0 || Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            sd_clone.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        // Verify metrics show data was processed.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }

    /// Data buffered in `buf` at the moment the shutdown token fires must
    /// not be silently discarded.  The final flush-on-shutdown should process
    /// any pending data before the pipeline exits.
    #[test]
    fn test_pipeline_flush_on_shutdown() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let mut data = String::new();
        for i in 0..5 {
            data.push_str(&format!(r#"{{"level":"INFO","msg":"hello {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Set batch_timeout large enough that the normal time-based flush will
        // never fire during the test — data stays in buf until shutdown.
        pipeline.batch_timeout = Duration::from_secs(60);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();

        // Cancel after enough time for the file to be polled and data to land
        // in buf, but well before the 60-second batch_timeout would fire.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            sd_clone.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "got: {:?}", result.err());

        // The shutdown flush must have forwarded the buffered lines.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
    }

    /// A SQL that references a column that does not exist in the batch should
    /// cause the transform to return an error.  The pipeline must log the error,
    /// drop that batch, and continue running — it must NOT return `Err`.
    #[test]
    fn test_pipeline_transform_error_skips_batch_continues() {
        use std::sync::atomic::Ordering;

        // SQL references a column that will never be present. Use generator
        // input so this test only exercises pipeline transform behavior and is
        // not sensitive to file-tail timing under heavy coverage.
        let yaml = r#"
input:
  type: generator
  generator:
    events_per_sec: 10000
    batch_size: 64
    profile: record
    attributes:
      benchmark_id: run-123
      pod_name: emitter-0
      stream_id: emitter-0
transform: "SELECT nonexistent_col FROM logs"
output:
  type: "null"
"#;
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.poll_interval = Duration::from_millis(5);

        let shutdown = CancellationToken::new();
        let sd_clone = shutdown.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            sd_clone.cancel();
        });

        // Pipeline must not return Err even though every batch will fail the
        // transform step.
        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline should survive transform errors but got: {:?}",
            result.err()
        );

        // At least one transform error must have been counted.
        let errors = pipeline.metrics.transform_errors.load(Ordering::Relaxed);
        assert!(errors > 0, "expected transform_errors > 0, got {errors}");

        // Every failed transform must also increment the dropped-batches counter.
        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert!(
            dropped > 0,
            "expected dropped_batches_total > 0, got {dropped}"
        );
    }

    // -----------------------------------------------------------------------
    // Async pipeline tests
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_pipeline_end_to_end() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("async_test.log");
        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(
                r#"{{"level":"INFO","msg":"async event {}","val":{}}}"#,
                i, i
            ));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Use devnull output to avoid stdout noise in test.
        pipeline = pipeline.with_sink(Box::new(DevNullSink));

        pipeline.batch_timeout = Duration::from_millis(50);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(
            result.is_ok(),
            "async pipeline should succeed: {:?}",
            result.err()
        );

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 100,
            "expected at least 100 lines through transform, got {lines_in}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_pipeline_shutdown_drains_data() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("drain_test.log");

        // Write data
        let mut data = String::new();
        for i in 0..50 {
            data.push_str(&format!(r#"{{"msg":"drain {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Give enough time for data to be read and processed, then shutdown.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        // After shutdown, all data should have been drained.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 50,
            "shutdown should drain all data, got {lines_in} lines (expected >= 50)"
        );
    }

    // -------------------------------------------------------------------
    // Shutdown stress tests
    // -------------------------------------------------------------------

    /// Shutdown must not deadlock when the bounded channel is full and
    /// input threads are blocked in blocking_send. This was a real bug:
    /// if the pipeline joins threads before draining the channel, the
    /// threads can't make progress and the join hangs forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_does_not_deadlock_on_full_channel() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("full_channel.log");

        // Write enough data to fill the channel (16 slots). Use a tiny
        // batch target so each line becomes its own channel message.
        let mut data = String::new();
        for i in 0..200 {
            data.push_str(&format!(r#"{{"i":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // SlowSink blocks the consumer, causing the channel to fill up.
        pipeline = pipeline.with_sink(Box::new(SlowSink {
            delay: Duration::from_millis(50),
        }));
        pipeline.batch_target_bytes = 64; // tiny batches → many channel sends
        pipeline.batch_timeout = Duration::from_millis(10);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Cancel quickly — while input threads are likely blocked on a full channel.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            sd.cancel();
        });

        // This must complete without deadlock. If the drain/join order is
        // wrong, this will hang and the test will time out.
        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "pipeline should not deadlock on shutdown with full channel"
        );
        assert!(
            result.unwrap().is_ok(),
            "pipeline should complete successfully"
        );
    }

    /// Terminal held-ticket shutdown must close the input channel before
    /// joining producers. Otherwise producers blocked on a full bounded channel
    /// can keep shutdown stuck forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn held_ticket_shutdown_does_not_deadlock_on_full_channel() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("held_ticket_full_channel.log");

        let mut data = String::new();
        for i in 0..200 {
            data.push_str(&format!(r#"{{"i":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        // A panic in the sink is converted into InternalFailure by the worker,
        // which maps to Hold and triggers terminal shutdown.
        pipeline = pipeline.with_sink(Box::new(PanicSink));
        pipeline.batch_target_bytes = 64;
        pipeline.batch_timeout = Duration::from_millis(10);
        pipeline.set_pool_drain_timeout(Duration::from_secs(1));

        let shutdown = CancellationToken::new();
        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "held-ticket shutdown should close the channel and not deadlock"
        );
        assert!(
            result.unwrap().is_ok(),
            "pipeline should complete terminal held-ticket shutdown cleanly"
        );
    }

    /// Shutdown with a slow output must still drain all buffered data.
    /// The output takes 50ms per batch, but shutdown should wait for
    /// the drain to complete rather than dropping data.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_drains_with_slow_output() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("slow_output.log");

        let mut data = String::new();
        for i in 0..50 {
            data.push_str(&format!(r#"{{"msg":"slow {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(SlowSink {
            delay: Duration::from_millis(20),
        }));
        pipeline.batch_timeout = Duration::from_millis(30);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 50,
            "slow output: all data should drain on shutdown, got {lines_in} (expected >= 50)"
        );
    }

    /// After all file data is read, shutdown must still drain everything.
    /// The file tailer keeps polling after EOF (it's a tailer, not a
    /// reader), so the pipeline only exits via shutdown signal.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_pipeline_processes_all_data_before_shutdown() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("eof_test.log");

        let mut data = String::new();
        for i in 0..30 {
            data.push_str(&format!(r#"{{"eof":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Wait for data to be processed, then shut down.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 30,
            "all file data should be processed before shutdown, got {lines_in} (expected >= 30)"
        );
    }

    /// Shutdown with zero data should complete instantly without errors.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_empty_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("empty.log");
        std::fs::write(&log_path, b"").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            sd.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "empty pipeline should shut down cleanly");
        assert!(result.unwrap().is_ok());
    }

    /// Immediate shutdown (cancel before run_async) must not panic or hang.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_immediate_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("immediate.log");
        std::fs::write(&log_path, b"{\"a\":1}\n").unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));

        let shutdown = CancellationToken::new();
        // Cancel immediately, before even starting.
        shutdown.cancel();

        let result =
            tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "immediate shutdown must not hang");
        assert!(result.unwrap().is_ok(), "immediate shutdown must not error");
    }

    /// Output errors must not crash the pipeline. Retry/control-plane
    /// failures are surfaced as output errors without advancing checkpoints.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_output_errors_do_not_crash() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("output_err.log");

        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(r#"{{"err":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        // FailingSink fails the first N batches, then succeeds.
        pipeline = pipeline.with_sink(Box::new(FailingSink {
            fail_count: 3,
            calls: 0,
        }));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(result.is_ok(), "output errors should not crash pipeline");

        // Data should still flow through transform even though output
        // failed on some batches.
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in > 0,
            "data should flow through transform despite output errors"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_fanout_output_errors_do_not_count_as_permanent_drops() {
        // This test verifies that a persistently-failing output does not
        // crash the pipeline and is not misclassified as a permanent drop.
        //
        // With the async worker pool, retries are built into the pool. An
        // always-failing sink causes the pool to exhaust retries and return a
        // non-delivered AckItem outcome that should HOLD the checkpoint seam.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("fanout_output_err.log");

        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(r#"{{"err":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
pipelines:
  default:
    inputs:
      - type: file
        path: "{}"
        format: json
    outputs:
      - type: stdout
        format: json
        name: always-failing
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        // always-failing: fails every call — workers retry indefinitely and the
        // pipeline must hold checkpoint progress instead of rejecting it.
        pipeline = pipeline.with_sink(Box::new(FailingSink::new(u32::MAX)));
        pipeline.batch_timeout = Duration::from_millis(20);
        // Short drain timeout so pool cancels workers promptly on shutdown.
        pipeline.set_pool_drain_timeout(Duration::from_secs(1));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            // Give workers time to start retrying.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(
            result.is_ok(),
            "fanout output errors should not crash pipeline"
        );

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in > 0,
            "data should still reach transform despite output failure"
        );

        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert_eq!(
            dropped, 0,
            "retry/control-plane failures should not count as permanent drops"
        );
    }

    /// Rapid repeated shutdown signals must not cause panics.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_repeated_shutdown_signals() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("repeat_shutdown.log");

        let mut data = String::new();
        for i in 0..50 {
            data.push_str(&format!(r#"{{"rep":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        // Cancel many times — CancellationToken should handle this gracefully.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            sd.cancel();
            sd.cancel();
            sd.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "repeated shutdown must not hang");
        assert!(result.unwrap().is_ok());
    }

    /// Large batch that exceeds batch_target_bytes on a single line
    /// must still be processed and not cause channel issues.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_oversized_single_line() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("big_line.log");

        // Single line with a large value (~10KB).
        let big_value = "x".repeat(10_000);
        let data = format!(r#"{{"big":"{}"}}"#, big_value);
        std::fs::write(&log_path, format!("{data}\n")).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));
        pipeline.batch_target_bytes = 1024; // Much smaller than the line
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 1,
            "oversized line should still be processed, got {lines_in}"
        );
    }

    /// A frozen output (blocks indefinitely on send_batch) must not
    /// prevent shutdown. The pipeline should still exit because shutdown
    /// cancellation breaks the select! loop, and the drain completes
    /// when input threads exit.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_shutdown_with_frozen_output() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("frozen.log");

        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!(r#"{{"frozen":{i}}}"#));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // FrozenSink: first call blocks until the token is cancelled.
        // This simulates an output that hangs (network timeout, deadlock).
        let freeze_token = CancellationToken::new();
        pipeline = pipeline.with_sink(Box::new(FrozenSink {
            release: freeze_token.clone(),
        }));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let ft = freeze_token.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
            // Release the frozen sink shortly after shutdown so the
            // block_in_place call can return and the pipeline can exit.
            tokio::time::sleep(Duration::from_millis(100)).await;
            ft.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "frozen output must not prevent shutdown (5s timeout)"
        );
        assert!(result.unwrap().is_ok());
    }

    /// SlowSink: verify that batch timeout flushes fire when the output
    /// is slow, and that the pipeline continues processing all data.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_slow_output_flush_by_timeout() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("slow_timeout.log");

        // Write enough data that multiple batches will be produced.
        let mut data = String::new();
        for i in 0..100 {
            data.push_str(&format!(r#"{{"msg":"timeout {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        // Slow output with a small delay. Combined with a short batch_timeout
        // and large batch_target_bytes, this forces flushes by timeout rather
        // than by size.
        pipeline = pipeline.with_sink(Box::new(SlowSink {
            delay: Duration::from_millis(10),
        }));
        pipeline.batch_timeout = Duration::from_millis(30);
        pipeline.batch_target_bytes = 1024 * 1024; // 1 MB — far larger than our data

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        assert!(
            lines_in >= 100,
            "slow output: pipeline should process all data, got {lines_in} (expected >= 100)"
        );

        let batches = pipeline.metrics.batches_total.load(Ordering::Relaxed);
        assert!(
            batches > 0,
            "slow output: at least one batch should succeed, got {batches}"
        );

        // In the split architecture, timeout-based flushing happens in the
        // I/O worker (not tracked by pipeline metrics). Verify data flowed
        // through the pipeline by checking batches_total above.
    }

    /// FrozenSink: verify that a frozen output causes dropped batches
    /// and output errors to be tracked in metrics while the pipeline
    /// still exits cleanly when released.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_frozen_output_tracks_metrics() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("frozen_metrics.log");

        let mut data = String::new();
        for i in 0..20 {
            data.push_str(&format!(r#"{{"msg":"frozen {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

        let freeze_token = CancellationToken::new();
        pipeline = pipeline.with_sink(Box::new(FrozenSink {
            release: freeze_token.clone(),
        }));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let ft = freeze_token.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sd.cancel();
            // Release the frozen sink so the block_in_place call returns.
            tokio::time::sleep(Duration::from_millis(100)).await;
            ft.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(
            result.is_ok(),
            "frozen output: pipeline must exit within timeout"
        );
        assert!(result.unwrap().is_ok());

        // With a frozen output, the input side should still have ingested
        // data (the input thread reads independently of the output).
        let lines_in = pipeline
            .metrics
            .transform_in
            .lines_total
            .load(Ordering::Relaxed);
        let output_nanos = pipeline.metrics.output_nanos_total.load(Ordering::Relaxed);

        // The frozen sink blocks in send_batch, so output_nanos should
        // reflect time spent waiting. At least some data should have
        // reached the transform stage.
        assert!(
            lines_in > 0,
            "frozen output: some lines should reach transform, got {lines_in}"
        );
        assert!(
            output_nanos > 0,
            "frozen output: output stage should record nonzero time, got {output_nanos}"
        );
    }

    /// Concurrent shutdown and data arrival: data written to the file
    /// after shutdown is signalled should not cause panics or hangs.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_concurrent_write_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("concurrent.log");

        // Start with some data.
        let mut initial = String::new();
        for i in 0..10 {
            initial.push_str(&format!(r#"{{"phase":"initial","i":{i}}}"#));
            initial.push('\n');
        }
        std::fs::write(&log_path, initial.as_bytes()).unwrap();

        let yaml = format!(
            r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );

        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline = pipeline.with_sink(Box::new(DevNullSink));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let lp = log_path.clone();

        // Concurrently: write more data AND shutdown at roughly the same time.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;

            // Write more data to the file (the tailer should pick it up).
            let mut more = String::new();
            for i in 10..30 {
                more.push_str(&format!(r#"{{"phase":"late","i":{i}}}"#));
                more.push('\n');
            }
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&lp).unwrap();
            f.write_all(more.as_bytes()).unwrap();
            f.flush().unwrap();

            // Shutdown shortly after the write.
            tokio::time::sleep(Duration::from_millis(50)).await;
            sd.cancel();
        });

        let result =
            tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

        assert!(result.is_ok(), "concurrent write + shutdown must not hang");
        assert!(result.unwrap().is_ok());
    }

    // -----------------------------------------------------------------------
    // PipelineMachine integration tests (#586, #587)
    // -----------------------------------------------------------------------

    /// Helper: build a pipeline from a log file path with a custom async sink.
    fn pipeline_with_sink(log_path: &std::path::Path, sink: Box<dyn Sink>) -> Pipeline {
        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
output:
  type: "null"
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
            .unwrap()
            .with_sink(sink)
    }

    #[test]
    fn test_machine_initialized_on_construction() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, "").unwrap();

        let pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        assert!(
            pipeline.machine.is_some(),
            "PipelineMachine must be initialized in from_config"
        );
    }

    #[test]
    fn test_machine_clean_shutdown_no_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        logfwd_test_utils::generate_json_lines(&log_path, 100, "machine-test");

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.batch_rows_total.load(Ordering::Relaxed) >= 100 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok());
        assert!(
            pipeline.machine.is_none(),
            "machine should be consumed by shutdown drain"
        );
    }

    #[test]
    fn test_flush_batch_with_zero_rows_still_completes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("empty.log");
        std::fs::write(&log_path, "").unwrap();

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok(), "empty file should not crash pipeline");
    }

    #[test]
    fn test_flush_batch_output_error_machine_still_drains() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        logfwd_test_utils::generate_json_lines(&log_path, 50, "fail-test");

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(FailingSink::new(u32::MAX)));
        pipeline.set_batch_timeout(Duration::from_millis(10));
        // Short drain timeout so pool cancels indefinitely-retrying workers promptly.
        pipeline.set_pool_drain_timeout(Duration::from_secs(1));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.outputs[0].2.errors() > 0 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(result.is_ok());
        assert!(
            pipeline.machine.is_none(),
            "machine should be consumed even with output errors"
        );
    }

    #[test]
    fn test_channel_msg_carries_checkpoints() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(4);

        let empty = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
        let mut checkpoints = HashMap::new();
        checkpoints.insert(SourceId(42), ByteOffset(1000));
        tx.try_send(ChannelMsg {
            batch: empty,
            checkpoints,
            queued_at: Some(tokio::time::Instant::now()),
            input_index: 0,
            scan_ns: 0,
            transform_ns: 0,
        })
        .unwrap();

        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.checkpoints.len(), 1);
        assert_eq!(msg.checkpoints[&SourceId(42)], ByteOffset(1000));
    }

    #[test]
    fn hold_disposition_retains_failed_ticket_without_advancing_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("hold.log");
        std::fs::write(&log_path, b"").unwrap();

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        let machine = pipeline.machine.as_mut().unwrap();
        let source = SourceId(42);
        let ticket = machine.create_batch(source, 1000);
        let ticket = machine.begin_send(ticket);

        let (has_held, _advances) = pipeline.ack_all_tickets(vec![ticket], TicketDisposition::Hold);
        assert!(
            has_held,
            "hold disposition must request terminal shutdown to bound held-ticket growth"
        );

        let machine = pipeline.machine.as_ref().unwrap();
        assert_eq!(
            machine.in_flight_count(),
            1,
            "held tickets must keep the source checkpoint unadvanced"
        );
        assert_eq!(machine.committed_checkpoint(source), None);
        assert_eq!(
            pipeline.held_tickets.len(),
            1,
            "hold must retain a failed queued ticket instead of dropping the sending ticket"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_apply_pool_ack_does_not_underflow_inflight_counter() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("underflow.log");
        std::fs::write(&log_path, "").unwrap();
        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));

        pipeline
            .metrics
            .inflight_batches
            .store(0, Ordering::Relaxed);
        pipeline
            .apply_pool_ack(AckItem {
                tickets: vec![],
                outcome: crate::worker_pool::DeliveryOutcome::InternalFailure,
                num_rows: 0,
                submitted_at: tokio::time::Instant::now(),
                scan_ns: 0,
                transform_ns: 0,
                output_ns: 0,
                queue_wait_ns: 0,
                send_latency_ns: 0,
                batch_id: 0,
                output_name: "test".to_string(),
            })
            .await;

        assert_eq!(
            pipeline.metrics.inflight_batches.load(Ordering::Relaxed),
            0,
            "inflight counter must not underflow on stray ack"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn held_pool_ack_requests_terminal_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("held-pool-ack.log");
        std::fs::write(&log_path, "").unwrap();
        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        let machine = pipeline.machine.as_mut().unwrap();
        let ticket = machine.create_batch(SourceId(42), 1000);
        let ticket = machine.begin_send(ticket);

        let should_stop = pipeline
            .apply_pool_ack(AckItem {
                tickets: vec![ticket],
                outcome: crate::worker_pool::DeliveryOutcome::InternalFailure,
                num_rows: 0,
                submitted_at: tokio::time::Instant::now(),
                scan_ns: 0,
                transform_ns: 0,
                output_ns: 0,
                queue_wait_ns: 0,
                send_latency_ns: 0,
                batch_id: 0,
                output_name: "test".to_string(),
            })
            .await;

        assert!(
            should_stop,
            "held worker outcomes must terminalize ingestion instead of accumulating tickets"
        );
        assert_eq!(pipeline.held_tickets.len(), 1);
        assert_eq!(
            pipeline
                .machine
                .as_ref()
                .unwrap()
                .committed_checkpoint(SourceId(42)),
            None
        );
    }

    #[cfg(feature = "internal-failpoints")]
    #[test]
    #[serial]
    fn test_failpoint_submit_before_pool_triggers_hold_and_shutdown() {
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "runtime::pipeline::submit::before_pool_submit",
            "1*return->off",
        )
        .expect("configure failpoint");

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("submit-failpoint.log");
        logfwd_test_utils::generate_json_lines(&log_path, 20, "submit-failpoint");

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "submit failpoint should trigger graceful shutdown without panic"
        );
        assert!(
            pipeline.machine.is_none(),
            "pipeline machine should still reach drained terminal state"
        );

        fail::remove("runtime::pipeline::submit::before_pool_submit");
        scenario.teardown();
    }

    #[cfg(feature = "internal-failpoints")]
    #[test]
    #[serial]
    fn test_failpoint_shutdown_skip_channel_drain_remains_safe() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("runtime::pipeline::run_async::skip_channel_drain", "return")
            .expect("configure failpoint");

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("shutdown-skip-drain.log");
        logfwd_test_utils::generate_json_lines(&log_path, 100, "skip-drain");

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            sd.cancel();
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "skip-drain failpoint must not deadlock or crash shutdown"
        );

        fail::remove("runtime::pipeline::run_async::skip_channel_drain");
        scenario.teardown();
    }

    #[test]
    fn test_transform_filter_all_rows_does_not_crash() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        // Write data that will be filtered by the transform
        logfwd_test_utils::generate_json_lines(&log_path, 50, "filter-test");

        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT * FROM logs WHERE level = 'NONEXISTENT'"
output:
  type: "null"
"#,
            log_path.display()
        );
        let config = logfwd_config::Config::load_str(&yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.batches_total.load(Ordering::Relaxed) > 0 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let result = pipeline.run(&shutdown);
        assert!(
            result.is_ok(),
            "pipeline with filtering transform must not crash"
        );
        assert!(
            pipeline.machine.is_none(),
            "machine should drain cleanly even when transform filters all rows"
        );

        // Bug #700 fix: batch_rows_total must be 0 when all rows are filtered
        // by the SQL WHERE clause. batches_total must still be incremented.
        let batches = pipeline.metrics.batches_total.load(Ordering::Relaxed);
        assert!(
            batches > 0,
            "batches_total must be incremented even when transform filters all rows, got {batches}"
        );
        let batch_rows = pipeline.metrics.batch_rows_total.load(Ordering::Relaxed);
        assert_eq!(
            batch_rows, 0,
            "batch_rows_total must be 0 when transform filters all rows, got {batch_rows}"
        );
    }

    /// Write N lines, run the pipeline, shut down cleanly. Verify that
    /// checkpoints.json exists and contains a non-zero offset.
    #[test]
    #[serial]
    fn test_checkpoint_persisted_after_clean_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        logfwd_test_utils::generate_json_lines(&log_path, 50, "cp-test");

        // Override data dir so checkpoints land in our temp dir.
        // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
        // accesses metrics/shutdown, not environment variables.
        unsafe {
            std::env::set_var("LOGFWD_DATA_DIR", dir.path());
        }

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.batch_rows_total.load(Ordering::Relaxed) >= 50 {
                    std::thread::sleep(Duration::from_millis(50));
                    sd.cancel();
                    return;
                }
                if Instant::now() > deadline {
                    sd.cancel();
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        pipeline.run(&shutdown).unwrap();

        // checkpoints.json must exist under the pipeline-scoped subdirectory.
        let cp_dir = dir.path().join("default");
        assert!(
            cp_dir.join("checkpoints.json").exists(),
            "checkpoints.json must exist after clean shutdown"
        );
        // Re-open the store to verify the checkpoints are readable.
        let store = FileCheckpointStore::open(&cp_dir).unwrap();
        let cps = store.load_all();
        assert!(!cps.is_empty(), "at least one checkpoint must be written");
        assert!(cps[0].offset > 0, "checkpoint offset must be non-zero");

        // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
        // accesses metrics/shutdown, not environment variables.
        unsafe {
            std::env::remove_var("LOGFWD_DATA_DIR");
        }
    }

    /// Write N lines, run pipeline, shut down, then write M more lines, run
    /// again. The second run must process only the M new lines.
    #[cfg(not(feature = "turmoil"))]
    #[test]
    #[serial]
    fn test_pipeline_resumes_from_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("resume.log");
        // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
        // accesses metrics/shutdown, not environment variables.
        unsafe {
            std::env::set_var("LOGFWD_DATA_DIR", dir.path());
        }

        // First run: write 20 lines, process them.
        logfwd_test_utils::generate_json_lines(&log_path, 20, "resume-first");
        {
            let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let counter_clone = Arc::clone(&counter);
            let sink = logfwd_test_utils::CountingSink::new(counter_clone);

            let mut pipeline = pipeline_with_sink(&log_path, Box::new(sink));
            pipeline.set_batch_timeout(Duration::from_millis(10));

            let shutdown = CancellationToken::new();
            let sd = shutdown.clone();
            let metrics = Arc::clone(pipeline.metrics());
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    if metrics.batch_rows_total.load(Ordering::Relaxed) >= 20 {
                        std::thread::sleep(Duration::from_millis(50));
                        sd.cancel();
                        return;
                    }
                    if Instant::now() > deadline {
                        sd.cancel();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            pipeline.run(&shutdown).unwrap();
            assert_eq!(
                counter.load(Ordering::Relaxed),
                20,
                "first run must process 20 rows"
            );
        }

        // Append 10 more lines.
        logfwd_test_utils::append_json_lines(&log_path, 10, "resume-second");

        // Second run: must process only the 10 new lines.
        {
            let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let counter_clone = Arc::clone(&counter);
            let sink = logfwd_test_utils::CountingSink::new(counter_clone);

            let mut pipeline = pipeline_with_sink(&log_path, Box::new(sink));
            pipeline.set_batch_timeout(Duration::from_millis(10));

            let shutdown = CancellationToken::new();
            let sd = shutdown.clone();
            let metrics = Arc::clone(pipeline.metrics());
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    if metrics.batch_rows_total.load(Ordering::Relaxed) >= 10 {
                        std::thread::sleep(Duration::from_millis(50));
                        sd.cancel();
                        return;
                    }
                    if Instant::now() > deadline {
                        sd.cancel();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
            pipeline.run(&shutdown).unwrap();
            assert_eq!(
                counter.load(Ordering::Relaxed),
                10,
                "second run must process only the 10 new lines, not re-read the first 20"
            );
        }

        // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
        // accesses metrics/shutdown, not environment variables.
        unsafe {
            std::env::remove_var("LOGFWD_DATA_DIR");
        }
    }
}

#[cfg(test)]
mod format_integration_tests {
    use super::*;
    use logfwd_core::scan_config::ScanConfig;

    /// JSON format: raw bytes pass directly through to scanner.
    #[test]
    fn json_format_direct_to_scanner() {
        let input =
            b"{\"level\":\"INFO\",\"msg\":\"hello\"}\n{\"level\":\"WARN\",\"msg\":\"world\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(input.to_vec())).unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string fields: bare names
        assert!(batch.schema().field_with_name("level").is_ok());
        assert!(batch.schema().field_with_name("msg").is_ok());
    }

    /// Raw format: lines captured as body when line_capture is true.
    #[test]
    fn raw_format_captures_lines() {
        let input = b"plain text line 1\nplain text line 2\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(input.to_vec())).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let body_col = batch
            .column_by_name("body")
            .expect("raw format should emit body column");
        if let Some(arr) = body_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            assert_eq!(arr.value(0), "plain text line 1");
            assert_eq!(arr.value(1), "plain text line 2");
        } else if let Some(arr) = body_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
        {
            assert_eq!(arr.value(0), "plain text line 1");
            assert_eq!(arr.value(1), "plain text line 2");
        } else {
            panic!("body column must be StringArray or StringViewArray");
        }
    }

    /// Raw format treats each line as an opaque input record captured in `body`,
    /// even when the payload itself looks like JSON.
    #[test]
    fn raw_format_json_line_prefers_full_line_body() {
        let input = br#"{"body":"app message","level":"INFO"}"#;
        let mut with_newline = input.to_vec();
        with_newline.push(b'\n');
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(with_newline)).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let body_col = batch
            .column_by_name("body")
            .expect("raw format should emit body column");
        if let Some(arr) = body_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            assert_eq!(arr.value(0), std::str::from_utf8(input).unwrap());
        } else if let Some(arr) = body_col
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
        {
            assert_eq!(arr.value(0), std::str::from_utf8(input).unwrap());
        } else {
            panic!("body column must be StringArray or StringViewArray");
        }
    }

    /// CRI format: message extracted, timestamp/stream/flag stripped.
    #[test]
    fn cri_extraction_produces_json() {
        let cri_input = b"2024-01-15T10:30:00Z stdout F {\"level\":\"INFO\",\"msg\":\"hello\"}\n";
        let mut out = Vec::new();
        let stats = Arc::new(ComponentStats::new());
        let mut fmt = FormatDecoder::cri(1024, Arc::clone(&stats));
        fmt.process_lines(cri_input, &mut out);

        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(out.clone())).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Single-type string field: bare name
        assert!(batch.schema().field_with_name("level").is_ok());
    }

    /// Mixed: CRI P+F → scanner produces correct fields.
    #[test]
    fn cri_pf_merge_then_scan() {
        let input = b"2024-01-15T10:30:00Z stdout P {\"level\":\"ER\n2024-01-15T10:30:00Z stdout F ROR\",\"msg\":\"boom\"}\n";
        let mut out = Vec::new();
        let stats = Arc::new(ComponentStats::new());
        let mut fmt = FormatDecoder::cri(1024, Arc::clone(&stats));
        fmt.process_lines(input, &mut out);

        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(out)).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}

#[cfg(test)]
mod proptest_pipeline {
    use super::*;
    use logfwd_core::scan_config::ScanConfig;
    use proptest::prelude::*;

    /// Generate random NDJSON: N lines of {"k":"v"} with random key/value lengths.
    fn arb_ndjson(max_lines: usize) -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(("[a-z]{1,8}", "[a-zA-Z0-9 ]{0,50}"), 1..=max_lines).prop_map(
            |fields| {
                let mut buf = Vec::new();
                for (key, val) in fields {
                    buf.extend_from_slice(b"{\"");
                    buf.extend_from_slice(key.as_bytes());
                    buf.extend_from_slice(b"\":\"");
                    buf.extend_from_slice(val.as_bytes());
                    buf.extend_from_slice(b"\"}\n");
                }
                buf
            },
        )
    }

    proptest! {
        /// Split arbitrary NDJSON at an arbitrary point. Both halves
        /// processed with remainder handling should produce the same
        /// RecordBatch row count as processing the whole buffer at once.
        #[test]
        #[allow(unused_assignments, unused_variables)]
        fn split_anywhere_same_row_count(
            ndjson in arb_ndjson(20),
            split_pct in 1u8..99u8,
        ) {
            let split_at = (ndjson.len() as u64 * split_pct as u64 / 100) as usize;
            let split_at = split_at.max(1).min(ndjson.len() - 1);

            // Whole buffer at once
            let config = ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                line_field_name: None,
                validate_utf8: false,
            };
            let mut scanner_whole = Scanner::new(config);
            let batch_whole = scanner_whole.scan_detached(Bytes::from(ndjson.clone())).unwrap();

            // Split into two chunks with remainder handling
            let chunk1 = &ndjson[..split_at];
            let chunk2 = &ndjson[split_at..];

            let mut buf = Vec::new();
            let mut remainder: Vec<u8> = Vec::new();

            // Process chunk1
            let mut combined = remainder;
            combined.extend_from_slice(chunk1);
            if let Some(pos) = memchr::memrchr(b'\n', &combined) {
                if pos + 1 < combined.len() {
                    remainder = combined[pos + 1..].to_vec();
                    combined.truncate(pos + 1);
                } else {
                    remainder = Vec::new();
                }
                buf.extend_from_slice(&combined);
            } else {
                remainder = combined;
            }

            // Process chunk2
            let mut combined = remainder;
            combined.extend_from_slice(chunk2);
            if let Some(pos) = memchr::memrchr(b'\n', &combined) {
                if pos + 1 < combined.len() {
                    combined.truncate(pos + 1);
                }
                buf.extend_from_slice(&combined);
            }

            let config2 = ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                line_field_name: None,
                validate_utf8: false,
            };
            let mut scanner_split = Scanner::new(config2);
            let batch_split = scanner_split.scan_detached(Bytes::from(buf.clone())).unwrap();

            prop_assert_eq!(
                batch_whole.num_rows(),
                batch_split.num_rows(),
                "split at {} of {}: row count mismatch",
                split_at,
                ndjson.len()
            );
        }

        /// CRI P/F sequences with random message content should always
        /// produce the same number of complete messages.
        #[test]
        fn cri_pf_random_sequences(
            messages in proptest::collection::vec("[a-zA-Z0-9]{1,30}", 1..=10),
            partials in proptest::collection::vec(0u8..3u8, 1..=10),
        ) {
            let mut input = Vec::new();
            let mut expected_count = 0usize;

            for (i, msg) in messages.iter().enumerate() {
                let num_partials = if i < partials.len() { partials[i] as usize } else { 0 };
                let msg_bytes = msg.as_bytes();

                if num_partials == 0 || msg_bytes.len() < 2 {
                    // Single F line
                    input.extend_from_slice(
                        format!("2024-01-15T10:30:{:02}Z stdout F {}\n", i % 60, msg).as_bytes(),
                    );
                    expected_count += 1;
                } else {
                    // Split into P chunks + final F
                    let chunk_size = (msg_bytes.len() / (num_partials + 1)).max(1);
                    let mut offset = 0;
                    for _ in 0..num_partials {
                        let end = (offset + chunk_size).min(msg_bytes.len());
                        let chunk = &msg[offset..end];
                        input.extend_from_slice(
                            format!("2024-01-15T10:30:{:02}Z stdout P {}\n", i % 60, chunk)
                                .as_bytes(),
                        );
                        offset = end;
                    }
                    let remaining = &msg[offset..];
                    input.extend_from_slice(
                        format!("2024-01-15T10:30:{:02}Z stdout F {}\n", i % 60, remaining)
                            .as_bytes(),
                    );
                    expected_count += 1;
                }
            }

            let mut out = Vec::new();
            let stats = Arc::new(ComponentStats::new());
            let mut fmt = FormatDecoder::cri(1024 * 1024, Arc::clone(&stats));
            fmt.process_lines(&input, &mut out);

            let line_count = out.iter().filter(|&&b| b == b'\n').count();
            prop_assert_eq!(
                line_count,
                expected_count,
                "expected {} complete messages, got {}",
                expected_count,
                line_count
            );
        }
    }
}
