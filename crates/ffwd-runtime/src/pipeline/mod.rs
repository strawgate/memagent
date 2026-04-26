//! Pipeline: inputs → per-input Scanner + SQL → processors → output sinks.
//!
//! Each input gets two dedicated OS threads: an I/O worker (polls source,
//! accumulates bytes) and a CPU worker (scans to RecordBatch, runs SQL).
//! See `input_pipeline` for the I/O/compute separation architecture.

mod buffered_input_policy;
mod build;
mod checkpoint_io;
mod checkpoint_policy;
mod cpu_worker;
mod health;
mod input_build;
pub(crate) mod input_pipeline;
mod input_poll;
mod internal_faults;
mod io_worker;
mod processor_stage;
mod run;
mod source_metadata;
mod submit;
pub mod topology;

#[cfg(test)]
mod pipeline_format_tests;
#[cfg(test)]
mod pipeline_proptest;
#[cfg(test)]
mod pipeline_tests;

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(test, feature = "turmoil"))]
use bytes::Bytes;
use bytes::BytesMut;

use arrow::record_batch::RecordBatch;
use ffwd_arrow::Scanner;

#[cfg(test)]
use self::checkpoint_policy::TicketDisposition;
use crate::processor::Processor;
use crate::transform::{Transform, create_transform};
#[cfg(test)]
use crate::worker_pool::AckItem;
use crate::worker_pool::OutputWorkerPool;
use ffwd_diagnostics::diagnostics::{ComponentHealth, ComponentStats, PipelineMetrics};
use ffwd_io::checkpoint::CheckpointStore;
#[cfg(test)]
use ffwd_io::checkpoint::FileCheckpointStore;
#[cfg(test)]
use ffwd_io::format::FormatDecoder;
use ffwd_io::input::InputSource;
#[cfg(test)]
use ffwd_io::input::SourceEvent;
use ffwd_io::tail::ByteOffset;
use ffwd_output::OnceAsyncFactory;
#[cfg(feature = "turmoil")]
use ffwd_output::SinkFactory;
#[cfg(test)]
use ffwd_output::build_sink_factory;
use ffwd_types::pipeline::{PipelineMachine, Running, SourceId};
use ffwd_types::source_metadata::{SourceMetadataPlan, SourcePathColumn};
#[cfg(test)]
use tokio_util::sync::CancellationToken;

fn source_metadata_style_source_path(style: ffwd_config::SourceMetadataStyle) -> SourcePathColumn {
    match style {
        ffwd_config::SourceMetadataStyle::Ecs => SourcePathColumn::Ecs,
        ffwd_config::SourceMetadataStyle::Otel => SourcePathColumn::Otel,
        ffwd_config::SourceMetadataStyle::Vector => SourcePathColumn::Vector,
        ffwd_config::SourceMetadataStyle::None | ffwd_config::SourceMetadataStyle::Fastforward => {
            SourcePathColumn::None
        }
        _ => SourcePathColumn::None,
    }
}

fn source_metadata_style_needs_source_paths(style: ffwd_config::SourceMetadataStyle) -> bool {
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
    scanner.scan(buf)
}

// ---------------------------------------------------------------------------
// Channel message types
// ---------------------------------------------------------------------------

/// Message from input workers to pipeline async loop.
///
/// Contains a scanned and SQL-transformed RecordBatch, ready for
/// the processor chain and output pool. Both production (CPU worker)
/// and simulation (turmoil async loop) produce this same message type.
pub(crate) struct ProcessedBatch {
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
/// its SQL) and `Box<dyn Transform>`. This enables per-input field extraction,
/// filtering, and schema reshaping with natural predicate pushdown.
struct SourcePipeline {
    scanner: Scanner,
    transform: Box<dyn Transform>,
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

struct IngestState {
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
    /// CRI metadata rows aligned to scanner-ready bytes currently in `buf`.
    #[cfg_attr(feature = "turmoil", allow(dead_code))]
    cri_metadata: ffwd_io::input::CriMetadata,
    /// Input metrics (used for parse/rotation/truncation observability).
    stats: Arc<ComponentStats>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// A single pipeline: inputs → per-input Scanner + SQL transform → async output pool.
pub struct Pipeline {
    name: String,
    inputs: Vec<IngestState>,
    /// Per-input Scanner + SqlTransform pairs. One per input config,
    /// indexed by the same order as `inputs`.
    input_transforms: Vec<SourcePipeline>,
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
    resource_attrs: Arc<[(String, String)]>,
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
    held_tickets: Vec<ffwd_types::pipeline::BatchTicket<ffwd_types::pipeline::Queued, u64>>,
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
    #[must_use]
    pub fn with_sink(mut self, sink: Box<dyn ffwd_output::Sink>) -> Self {
        let name = self.name.clone();
        let factory = Arc::new(OnceAsyncFactory::new(name, sink));
        self.pool = OutputWorkerPool::new(factory, 1, Duration::MAX, Arc::clone(&self.metrics));
        self
    }

    /// Add an input source for testing. Bypasses config-based input construction.
    ///
    /// Each new input gets its own passthrough `Scanner + Transform` pair
    /// (`SELECT * FROM logs`) to keep `input_transforms` in sync with `inputs`.
    #[must_use]
    pub fn with_input(mut self, name: &str, source: Box<dyn InputSource>) -> Self {
        let stats = Arc::new(ComponentStats::new_with_health(ComponentHealth::Starting));
        self.inputs.push(IngestState {
            source,
            buf: BytesMut::with_capacity(self.batch_target_bytes),
            row_origins: Vec::new(),
            source_paths: HashMap::new(),
            cri_metadata: ffwd_io::input::CriMetadata::default(),
            stats,
        });
        // Keep input_transforms in sync: one transform per input.
        while self.input_transforms.len() < self.inputs.len() {
            let transform = create_transform("SELECT * FROM logs")
                .expect("hardcoded passthrough 'SELECT * FROM logs' is always valid");
            let scanner = Scanner::new(transform.scan_config());
            self.input_transforms.push(SourcePipeline {
                scanner,
                transform,
                input_name: name.to_string(),
                source_metadata_plan: SourceMetadataPlan::default(),
            });
        }
        self
    }

    /// Replace the checkpoint store. Useful for injecting an in-memory store in tests.
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    pub fn for_simulation(name: &str, sink: Box<dyn ffwd_output::Sink>) -> Self {
        let factory = Arc::new(OnceAsyncFactory::new(name.to_string(), sink));
        Self::for_simulation_with_factory(name, factory, 1)
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
            resource_attrs: Arc::from([]),
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
