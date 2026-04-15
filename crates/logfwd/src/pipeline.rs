//! Pipeline: YAML config → inputs → Scanner → SQL transform → output sinks.
//!
//! Single thread per pipeline. All components are already built and tested;
//! this module wires them together.

mod health;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
// std::time::Instant is used in the non-turmoil input_poll_loop (buffered_since).
// Under turmoil that function is excluded, so the import would be unused.
#[cfg(not(feature = "turmoil"))]
use std::time::Instant;

use bytes::{Bytes, BytesMut};

use crate::batch_accumulator::{AccumulatorAction, BatchAccumulator};

use opentelemetry::metrics::Meter;
use tracing::Instrument;

use crate::processor::Processor;
use crate::worker_pool::{AckItem, OutputWorkerPool, WorkItem};
use logfwd_arrow::scanner::Scanner;
use logfwd_config::{
    EnrichmentConfig, Format, GeneratorAttributeValueConfig, GeneratorComplexityConfig,
    GeneratorProfileConfig, GeoDatabaseFormat, InputConfig, InputType, PipelineConfig,
};
use logfwd_io::checkpoint::{
    CheckpointStore, FileCheckpointStore, SourceCheckpoint, default_data_dir,
};
use logfwd_io::diagnostics::{ComponentHealth, ComponentStats, PipelineMetrics};
use logfwd_io::format::FormatDecoder;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{FileInput, InputEvent, InputSource};
use logfwd_io::tail::{ByteOffset, TailConfig};
use logfwd_output::{
    AsyncFanoutFactory, BatchMetadata, OnceAsyncFactory, SinkFactory, build_sink_factory,
};
use logfwd_transform::SqlTransform;
use logfwd_types::pipeline::{PipelineMachine, Running, SourceId};
use tokio_util::sync::CancellationToken;

use self::health::{HealthTransitionEvent, reduce_component_health};

// ---------------------------------------------------------------------------
// block_in_place shim for simulation
// ---------------------------------------------------------------------------

/// Scan a batch. In production (multi-thread runtime), uses `block_in_place`
/// to avoid starving other tasks. Under `turmoil` feature (single-thread
/// runtime), calls scan directly since `block_in_place` panics.
#[inline]
fn scan_maybe_blocking(
    scanner: &mut Scanner,
    buf: Bytes,
) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
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

/// Message from input thread to pipeline async loop.
enum ChannelMsg {
    /// Data with per-file checkpoint metadata.
    Data {
        /// Accumulated scanner-ready bytes (refcounted).
        bytes: Bytes,
        /// Per-file (source_id, byte_offset) at time of send.
        checkpoints: Vec<(SourceId, ByteOffset)>,
        /// When the input thread enqueued this message. Used to measure
        /// queue wait time (time data sat in channel before processing).
        /// Uses tokio::time::Instant so elapsed() measures simulated time
        /// under Turmoil, not wall-clock time.
        queued_at: tokio::time::Instant,
        /// Which input config produced this data, indexing into
        /// `Pipeline::input_transforms`.
        input_index: usize,
    },
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
    #[allow(dead_code)]
    input_name: String,
}

struct InputState {
    /// The input source, wrapped in FramedInput for line framing + format processing.
    /// The pipeline receives scanner-ready bytes — it doesn't know about formats.
    source: Box<dyn InputSource>,
    /// Buffer accumulating scanner-ready bytes for batching.
    buf: BytesMut,
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
    /// Throttle checkpoint flushes to at most once per this interval.
    /// Uses tokio::time::Instant so the throttle works correctly under
    /// both real and simulated (Turmoil) time.
    last_checkpoint_flush: tokio::time::Instant,
    /// Checkpoint flush throttle interval. Default 5 seconds; overridable for tests.
    checkpoint_flush_interval: Duration,
}

impl Pipeline {
    /// Construct a pipeline from parsed YAML config.
    pub fn from_config(
        name: &str,
        config: &PipelineConfig,
        meter: &Meter,
        base_path: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        if config.workers == Some(0) {
            return Err("workers must be >= 1".to_string());
        }
        if config.batch_target_bytes == Some(0) {
            return Err("batch_target_bytes must be > 0".to_string());
        }

        // Collect enrichment sources once — they are shared across all
        // per-input transforms.
        let mut enrichment_tables: Vec<Arc<dyn logfwd_transform::enrichment::EnrichmentTable>> =
            Vec::new();
        let mut geo_database: Option<Arc<dyn logfwd_transform::enrichment::GeoDatabase>> = None;

        for enrichment in &config.enrichment {
            match enrichment {
                EnrichmentConfig::GeoDatabase(geo_cfg) => {
                    let mut path = PathBuf::from(&geo_cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }

                    let db: Arc<dyn logfwd_transform::enrichment::GeoDatabase> = match geo_cfg
                        .format
                    {
                        GeoDatabaseFormat::Mmdb => {
                            let mmdb = logfwd_transform::udf::geo_lookup::MmdbDatabase::open(&path)
                                .map_err(|e| {
                                    format!("failed to open geo database '{}': {e}", path.display())
                                })?;
                            Arc::new(mmdb)
                        }
                        _ => {
                            return Err(format!(
                                "unsupported geo database format: {:?}",
                                geo_cfg.format
                            ));
                        }
                    };
                    if geo_cfg.refresh_interval.is_some() {
                        tracing::warn!(
                            "geo_database refresh_interval is not yet implemented, database will not auto-reload"
                        );
                    }
                    geo_database = Some(db);
                }
                EnrichmentConfig::Static(cfg) => {
                    let labels: Vec<(String, String)> = cfg
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let table = Arc::new(
                        logfwd_transform::enrichment::StaticTable::new(&cfg.table_name, &labels)
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                    );
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::HostInfo(_) => {
                    let table = Arc::new(logfwd_transform::enrichment::HostInfoTable::new());
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::K8sPath(cfg) => {
                    let table = Arc::new(logfwd_transform::enrichment::K8sPathTable::new(
                        &cfg.table_name,
                    ));
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::Csv(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(logfwd_transform::enrichment::CsvFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::Jsonl(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(logfwd_transform::enrichment::JsonLinesFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
            }
        }

        // The pipeline-level SQL is the fallback for inputs without their own.
        let pipeline_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");

        // For PipelineMetrics, use the pipeline-level SQL as the label.
        let mut metrics = PipelineMetrics::new(name, pipeline_sql, meter);

        // Open checkpoint store scoped to this pipeline name.
        // Only create the directory if LOGFWD_DATA_DIR is explicitly set
        // (prevents tests from polluting the default data dir).
        let checkpoint_dir = default_data_dir().join(name);
        let checkpoint_store = if checkpoint_dir.exists()
            || std::env::var_os("LOGFWD_DATA_DIR").is_some()
        {
            match FileCheckpointStore::open(&checkpoint_dir) {
                Ok(s) => Some(Box::new(s) as Box<dyn CheckpointStore>),
                Err(e) => {
                    tracing::warn!(error = %e, "could not open checkpoint store — starting from beginning");
                    None
                }
            }
        } else {
            None
        };
        let saved_checkpoints: Vec<SourceCheckpoint> = checkpoint_store
            .as_ref()
            .map(|s| s.load_all())
            .unwrap_or_default();

        // Build per-input InputTransform and InputState.
        let mut inputs = Vec::new();
        let mut input_transforms = Vec::new();

        for (i, input_cfg) in config.inputs.iter().enumerate() {
            let mut resolved_cfg = input_cfg.clone();
            if let Some(path_str) = &input_cfg.path {
                let mut path = PathBuf::from(path_str);
                if path.is_relative()
                    && let Some(base) = base_path
                {
                    path = base.join(path);
                }
                if let Ok(abs_path) = std::fs::canonicalize(&path) {
                    resolved_cfg.path = Some(abs_path.to_string_lossy().into_owned());
                } else {
                    resolved_cfg.path = Some(path.to_string_lossy().into_owned());
                }
            }

            let input_name = input_cfg
                .name
                .clone()
                .unwrap_or_else(|| format!("input_{i}"));
            let input_type_str = format!("{:?}", input_cfg.input_type).to_lowercase();
            let input_stats = metrics.add_input(&input_name, &input_type_str);

            // Determine the SQL for this input: per-input > pipeline-level > passthrough.
            let input_sql = input_cfg.sql.as_deref().unwrap_or(pipeline_sql);

            let mut transform = SqlTransform::new(input_sql).map_err(|e| e.to_string())?;

            // Wire up shared enrichment sources to this transform.
            if let Some(ref db) = geo_database {
                transform.set_geo_database(Arc::clone(db));
            }
            for table in &enrichment_tables {
                transform
                    .add_enrichment_table(Arc::clone(table))
                    .map_err(|e| format!("input '{}': enrichment error: {e}", input_name))?;
            }

            let mut scan_config = transform.scan_config();
            // Raw format sends non-JSON lines to the scanner. Without keep_raw,
            // these produce empty rows. Force keep_raw so every line is captured.
            if matches!(input_cfg.format, Some(Format::Raw)) {
                scan_config.keep_raw = true;
            }
            let scanner = Scanner::new(scan_config);

            input_transforms.push(InputTransform {
                scanner,
                transform,
                input_name: input_name.clone(),
            });

            inputs.push(build_input_state(&input_name, &resolved_cfg, input_stats)?);
        }

        // Restore previously saved file offsets by fingerprint (SourceId).
        for cp in &saved_checkpoints {
            let source_id = SourceId(cp.source_id);
            for input in &mut inputs {
                input.source.set_offset_by_source(source_id, cp.offset);
            }
        }

        // Build output sink factory → pool.
        let factory: Arc<dyn SinkFactory> = if config.outputs.len() == 1 {
            let output_cfg = &config.outputs[0];
            let output_name = output_cfg
                .name
                .clone()
                .unwrap_or_else(|| "output_0".to_string());
            let output_type_str = format!("{:?}", output_cfg.output_type).to_lowercase();
            let output_stats = metrics.add_output(&output_name, &output_type_str);
            build_sink_factory(&output_name, output_cfg, base_path, output_stats)
                .map_err(|e| e.to_string())?
        } else {
            let mut factories: Vec<Arc<dyn SinkFactory>> = Vec::new();
            for (i, output_cfg) in config.outputs.iter().enumerate() {
                let output_name = output_cfg
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("output_{i}"));
                let output_type_str = format!("{:?}", output_cfg.output_type).to_lowercase();
                let output_stats = metrics.add_output(&output_name, &output_type_str);
                factories.push(
                    build_sink_factory(&output_name, output_cfg, base_path, output_stats)
                        .map_err(|e| e.to_string())?,
                );
            }
            let fanout_name = name.to_string();
            Arc::new(AsyncFanoutFactory::new(fanout_name, factories))
        };

        // Single-use factories (e.g. OnceAsyncFactory wrapping a pre-built
        // sink) can only create one worker and that worker must never
        // idle-expire — if it exits, create() returns an error and the
        // output stops permanently.
        let (max_workers, idle_timeout) = if factory.is_single_use() {
            (1, Duration::MAX) // never idle-expire the sole worker
        } else {
            (config.workers.unwrap_or(4), Duration::from_secs(30))
        };
        let metrics = Arc::new(metrics);
        let pool = OutputWorkerPool::new(factory, max_workers, idle_timeout, Arc::clone(&metrics));

        // Convert resource_attrs HashMap to a sorted Vec for deterministic output.
        let mut resource_attrs: Vec<(String, String)> = config
            .resource_attrs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        resource_attrs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        debug_assert_eq!(
            inputs.len(),
            input_transforms.len(),
            "inputs and input_transforms must have the same length"
        );

        Ok(Pipeline {
            name: name.to_string(),
            inputs,
            input_transforms,
            processors: vec![],
            pool,
            metrics,
            batch_target_bytes: config.batch_target_bytes.unwrap_or(4 * 1024 * 1024),
            batch_timeout: Duration::from_millis(config.batch_timeout_ms.unwrap_or(100)),
            poll_interval: Duration::from_millis(config.poll_interval_ms.unwrap_or(10)),
            resource_attrs: Arc::new(resource_attrs),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store,
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: Duration::from_secs(5),
        })
    }

    /// Replace the output sink with an async [`Sink`].
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
    /// Called by `--dry-run` to surface planning errors (duplicate aliases,
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

    /// Create a minimal pipeline for simulation testing.
    ///
    /// Bypasses config parsing, filesystem, and OTel meter setup.
    /// Uses default scanner (JSON passthrough), identity SQL transform,
    /// and the provided sink.
    #[cfg(feature = "turmoil")]
    pub fn for_simulation(name: &str, sink: Box<dyn logfwd_output::Sink>) -> Self {
        use logfwd_arrow::scanner::Scanner;
        use logfwd_core::scan_config::ScanConfig;

        let scan_config = ScanConfig::default();
        let scanner = Scanner::new(scan_config);
        let transform = SqlTransform::new("SELECT * FROM logs").expect("default SQL");
        let meter = opentelemetry::global::meter("test");
        let metrics = Arc::new(PipelineMetrics::new(name, "SELECT * FROM logs", &meter));
        let factory = Arc::new(OnceAsyncFactory::new(name.to_string(), sink));
        let pool = OutputWorkerPool::new(factory, 1, Duration::MAX, Arc::clone(&metrics));

        Pipeline {
            name: name.to_string(),
            inputs: vec![],
            input_transforms: vec![InputTransform {
                scanner,
                transform,
                input_name: "simulation".to_string(),
            }],
            processors: vec![],
            pool,
            metrics,
            batch_target_bytes: 64 * 1024,
            batch_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
            resource_attrs: Arc::new(vec![]),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store: None,
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: Duration::from_secs(5),
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
        use logfwd_arrow::scanner::Scanner;
        use logfwd_core::scan_config::ScanConfig;

        let scan_config = ScanConfig::default();
        let scanner = Scanner::new(scan_config);
        let transform = SqlTransform::new("SELECT * FROM logs").expect("default SQL");
        let meter = opentelemetry::global::meter("test");
        let metrics = Arc::new(PipelineMetrics::new(name, "SELECT * FROM logs", &meter));
        let pool = OutputWorkerPool::new(factory, max_workers, Duration::MAX, Arc::clone(&metrics));

        Pipeline {
            name: name.to_string(),
            inputs: vec![],
            input_transforms: vec![InputTransform {
                scanner,
                transform,
                input_name: "simulation".to_string(),
            }],
            processors: vec![],
            pool,
            metrics,
            batch_target_bytes: 64 * 1024,
            batch_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
            resource_attrs: Arc::new(vec![]),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store: None,
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: Duration::from_secs(5),
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
    /// Run the pipeline until `shutdown` is cancelled. Blocks the calling thread.
    ///
    /// Delegates to `run_async` on a tokio runtime. The sync interface exists
    /// for test convenience; production uses `run_async` directly.
    pub fn run(&mut self, shutdown: &CancellationToken) -> io::Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
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
        assert_eq!(
            self.inputs.len(),
            self.input_transforms.len(),
            "run_async: inputs ({}) and input_transforms ({}) must match",
            self.inputs.len(),
            self.input_transforms.len(),
        );
        // Spawn input threads. Each polls its source, parses format, and
        // sends accumulated JSON lines through a bounded channel.
        // Backpressure: when the channel is full, the input thread blocks.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(16);

        let batch_target = self.batch_target_bytes;
        let batch_timeout = self.batch_timeout;
        let poll_interval = self.poll_interval;
        #[cfg(not(feature = "turmoil"))]
        let mut input_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();
        #[cfg(feature = "turmoil")]
        let mut input_tasks = tokio::task::JoinSet::<()>::new();

        for (input_index, input) in self.inputs.drain(..).enumerate() {
            let tx = tx.clone();
            let sd = shutdown.clone();
            let metrics = Arc::clone(&self.metrics);

            #[cfg(not(feature = "turmoil"))]
            {
                input_handles.push(std::thread::spawn(move || {
                    input_poll_loop(
                        input,
                        tx,
                        metrics,
                        sd,
                        batch_target,
                        batch_timeout,
                        poll_interval,
                        input_index,
                    );
                }));
            }

            #[cfg(feature = "turmoil")]
            {
                input_tasks.spawn(async_input_poll_loop(
                    input,
                    tx,
                    metrics,
                    sd,
                    batch_target,
                    batch_timeout,
                    poll_interval,
                    input_index,
                ));
            }
        }
        drop(tx); // Drop our copy so rx.recv() returns None when all inputs exit.

        // Per-input accumulators: one per input, indexed by input_index.
        let num_inputs = self.input_transforms.len();
        let mut accumulators: Vec<BatchAccumulator> = (0..num_inputs)
            .map(|_| BatchAccumulator::new(self.batch_target_bytes))
            .collect();

        let mut flush_interval = tokio::time::interval(self.batch_timeout);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;  // arms evaluated in source order; ack is first to prevent starvation

                // Receive ack items from pool workers first — highest priority so that
                // worker slots are freed promptly and back-pressure is relieved before
                // we ingest or flush more data.
                ack = self.pool.ack_rx_mut().recv() => {
                    if let Some(ack) = ack {
                        self.apply_pool_ack(ack);
                    }
                }

                () = shutdown.cancelled() => {
                    break;
                }

                msg = rx.recv() => {
                    match msg {
                        Some(ChannelMsg::Data { bytes, checkpoints, queued_at, input_index }) => {
                            if let AccumulatorAction::Flush { data, checkpoints, queued_at, .. } =
                                accumulators[input_index].ingest(bytes, checkpoints, queued_at)
                            {
                                self.metrics.inc_flush_by_size();
                                self.flush_batch_from(data, checkpoints, "size", queued_at, input_index).await;
                                // Do NOT reset flush_interval here — size-triggered flushes
                                // from one input must not starve timeout flushes for others.
                            }
                        }
                        None => break,
                    }
                }

                _ = flush_interval.tick() => {
                    for (idx, acc) in accumulators.iter_mut().enumerate() {
                        if let AccumulatorAction::Flush { data, checkpoints, queued_at, .. } =
                            acc.check_timeout()
                        {
                            self.metrics.inc_flush_by_timeout();
                            self.flush_batch_from(data, checkpoints, "timeout", queued_at, idx).await;
                        }
                    }

                    // Heartbeat for stateful processors: send an empty batch through
                    // the chain so stateful processors can check their internal timers
                    // and emit timed-out data.
                    if !self.processors.is_empty()
                        && self.processors.iter().any(|p| p.is_stateful())
                        && accumulators.iter().all(BatchAccumulator::is_empty)
                    {
                        let empty = arrow::record_batch::RecordBatch::new_empty(
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

        // Drain channel messages before joining input threads.
        // This prevents deadlock during shutdown if a producer is blocked in
        // `blocking_send` while the bounded channel is full.
        while let Some(msg) = rx.recv().await {
            match msg {
                ChannelMsg::Data {
                    bytes,
                    checkpoints,
                    queued_at,
                    input_index,
                } => {
                    if let AccumulatorAction::Flush {
                        data,
                        checkpoints,
                        queued_at,
                        ..
                    } = accumulators[input_index].ingest(bytes, checkpoints, queued_at)
                    {
                        self.flush_batch_from(data, checkpoints, "drain", queued_at, input_index)
                            .await;
                    }
                }
            }
        }

        // All sender clones have now been dropped, so input threads/tasks can
        // be joined without risking a channel backpressure deadlock.
        #[cfg(not(feature = "turmoil"))]
        for h in input_handles {
            if let Err(e) = h.join() {
                tracing::error!(error = ?e, "pipeline: input thread panicked");
            }
        }
        #[cfg(feature = "turmoil")]
        while let Some(result) = input_tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(error = ?e, "pipeline: input task panicked");
            }
        }

        // Flush any remaining buffered data from all accumulators.
        for (idx, acc) in accumulators.iter_mut().enumerate() {
            if let Some((data, checkpoints, queued_at)) = acc.drain() {
                self.flush_batch_from(data, checkpoints, "drain", queued_at, idx)
                    .await;
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
        // then wait up to 60s for graceful shutdown.
        self.pool.drain(Duration::from_secs(60)).await;

        // Drain remaining acks that workers sent before exiting.
        while let Some(ack) = self.pool.try_recv_ack() {
            self.apply_pool_ack(ack);
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

        Ok(())
    }

    /// Scan + transform + output a batch of accumulated JSON lines.
    ///
    /// Creates BatchTickets from checkpoint metadata for each contributing
    /// file source. Tickets are Queued before scan/transform (safe to drop
    /// on error), then begin_send after (must ack or reject).
    #[tracing::instrument(
        name = "batch",
        skip_all,
        fields(
            pipeline = %self.name,
            bytes_in = tracing::field::Empty,
            flush_reason = tracing::field::Empty,
            queue_wait_ns = tracing::field::Empty,
            input_rows = tracing::field::Empty,
            output_rows = tracing::field::Empty,
            errors = tracing::field::Empty,
        )
    )]
    async fn flush_batch_from(
        &mut self,
        data: Bytes,
        checkpoints: HashMap<SourceId, ByteOffset>,
        flush_reason: &'static str,
        queued_at: Option<tokio::time::Instant>,
        input_index: usize,
    ) {
        if data.is_empty() {
            return;
        }

        let batch_id = self.metrics.alloc_batch_id();
        self.metrics.begin_active_batch(batch_id, now_nanos());

        let combined = data;

        // Record batch-level attributes now that we know the input size.
        let span = tracing::Span::current();
        span.record("bytes_in", combined.len() as u64);
        span.record("flush_reason", flush_reason);
        if let Some(qa) = queued_at {
            span.record("queue_wait_ns", qa.elapsed().as_nanos() as u64);
        }

        // Create Queued tickets (one per contributing file source).
        // These are lightweight — NOT tracked by the machine yet.
        // Safe to drop on scan/transform error.
        let tickets = if let Some(ref mut machine) = self.machine {
            checkpoints
                .into_iter()
                .map(|(sid, offset)| machine.create_batch(sid, offset.0))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Scan (CPU-bound, ~1-5ms per 4MB batch). scan_maybe_blocking
        // uses block_in_place in production, direct call under turmoil.
        // Use tokio::time::Instant so elapsed() measures simulated time under Turmoil.
        let t0 = tokio::time::Instant::now();
        let batch = {
            let scan_span =
                tracing::info_span!("scan", pipeline = %self.name, rows = tracing::field::Empty);
            let _entered = scan_span.enter();
            let b = match scan_maybe_blocking(
                &mut self.input_transforms[input_index].scanner,
                combined,
            ) {
                Ok(b) => b,
                Err(e) => {
                    // Queued tickets dropped here — safe, not tracked by machine.
                    self.metrics.inc_scan_error();
                    self.metrics.inc_parse_error();
                    self.metrics.inc_dropped_batch();
                    tracing::warn!(error = %e, "pipeline: scan error (batch dropped)");
                    // Must use `span` (the batch root) not `Span::current()` here —
                    // _entered is still live so current() points to the scan child span.
                    span.record("errors", 1u64);
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
            };
            scan_span.record("rows", b.num_rows() as u64);
            b
        }; // _entered and scan_span drop here → span ends
        let scan_elapsed = t0.elapsed();
        self.metrics.advance_active_batch(
            batch_id,
            "transform",
            scan_elapsed.as_nanos() as u64,
            now_nanos(),
        );

        // begin_send all tickets — machine now tracks them, MUST ack or reject.
        let sending: Vec<_> = if let Some(ref mut machine) = self.machine {
            tickets.into_iter().map(|t| machine.begin_send(t)).collect()
        } else {
            // No machine (shouldn't happen during normal run). Drop tickets.
            drop(tickets);
            Vec::new()
        };

        // Handle zero-row scan results. Still ack tickets — the input bytes
        // were consumed. Without this, filtered data causes infinite re-read.
        if batch.num_rows() == 0 {
            self.ack_all_tickets(sending, true);
            // Record batch with 0 rows so batches_total and scan timing are tracked.
            self.metrics
                .record_batch(0, scan_elapsed.as_nanos() as u64, 0, 0);
            self.metrics.finish_active_batch(batch_id);
            return;
        }

        let num_rows = batch.num_rows() as u64;
        self.metrics.transform_in.inc_lines(num_rows);

        tracing::Span::current().record("input_rows", num_rows);

        // Transform (already async).
        // Use tokio::time::Instant so elapsed() measures simulated time under Turmoil.
        let t1 = tokio::time::Instant::now();
        let result = match self.input_transforms[input_index]
            .transform
            .execute(batch)
            .instrument(tracing::info_span!("transform", pipeline = %self.name, rows_in = num_rows))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.metrics.inc_transform_error();
                self.metrics.inc_dropped_batch();
                tracing::warn!(error = %e, "pipeline: transform error (batch dropped)");
                tracing::Span::current().record("errors", 1u64);
                // Reject tickets — transform failed, data not delivered.
                self.ack_all_tickets(sending, false);
                self.metrics.finish_active_batch(batch_id);
                return;
            }
        };
        let transform_elapsed = t1.elapsed();
        self.metrics.advance_active_batch(
            batch_id,
            "output",
            transform_elapsed.as_nanos() as u64,
            now_nanos(),
        );
        // Run through processor chain.
        let results = if self.processors.is_empty() {
            smallvec::smallvec![result]
        } else {
            let meta = BatchMetadata {
                resource_attrs: Arc::clone(&self.resource_attrs),
                observed_time_ns: now_nanos(),
            };
            match crate::processor::run_chain(&mut self.processors, result, &meta) {
                Ok(batches) => batches,
                Err(crate::processor::ProcessorError::Transient(e)) => {
                    tracing::warn!(error = %e, "transient processor error, rejecting batch");
                    self.ack_all_tickets(sending, false);
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
                Err(crate::processor::ProcessorError::Permanent(e)) => {
                    tracing::error!(error = %e, "permanent processor error, dropping batch");
                    self.ack_all_tickets(sending, true); // ack but don't forward
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
                Err(crate::processor::ProcessorError::Fatal(e)) => {
                    tracing::error!(error = %e, "fatal processor error, shutting down");
                    self.ack_all_tickets(sending, false);
                    self.metrics.finish_active_batch(batch_id);
                    // TODO(#1404): trigger pipeline shutdown on fatal processor error
                    return;
                }
            }
        };

        let total_rows: u64 = results.iter().map(|b| b.num_rows() as u64).sum();
        self.metrics.transform_out.inc_lines(total_rows);

        // Handle zero-row results (SQL WHERE filtered all rows).
        //
        // SAFETY: ACKing here is correct ONLY when no stateful processor is
        // present. A stateful processor that returns an empty vec has buffered
        // the batch internally; ACKing would advance the checkpoint past data
        // that has not yet been written, which is a data-loss risk on restart.
        //
        // This path is currently safe because no stateful processors exist yet.
        // Once stateful processors are introduced the two cases (filtered vs
        // buffered) must be distinguished — either via a richer return type from
        // run_chain or by deferring the ACK until ticketless submission drains.
        // TODO(#1404): guard this ACK behind "no stateful processor buffered"
        if total_rows == 0 {
            debug_assert!(
                !self.processors.iter().any(|p| p.is_stateful()),
                "ACKing on empty output is unsafe when stateful processors are present"
            );
            self.ack_all_tickets(sending, true);
            self.metrics.record_batch(
                0,
                scan_elapsed.as_nanos() as u64,
                transform_elapsed.as_nanos() as u64,
                0,
            );
            self.metrics.finish_active_batch(batch_id);
            return;
        }

        // Concatenate multiple processor outputs back to a single batch for
        // submission. Future: submit as a work group.
        let result = if results.len() == 1 {
            results.into_iter().next().expect("checked len == 1")
        } else {
            match arrow::compute::concat_batches(&results[0].schema(), results.iter()) {
                Ok(batch) => batch,
                Err(e) => {
                    // A schema mismatch here indicates a processor bug — processors must
                    // emit batches with a consistent schema.  Treat it as a fatal processor
                    // error: reject the tickets (no checkpoint advance) and abort this batch
                    // so the pipeline can continue rather than crashing the process.
                    tracing::error!(
                        error = %e,
                        "processor chain returned incompatible schemas; rejecting batch"
                    );
                    self.ack_all_tickets(sending, false);
                    self.metrics.finish_active_batch(batch_id);
                    return;
                }
            }
        };

        let out_rows = result.num_rows() as u64;
        tracing::Span::current().record("output_rows", out_rows);

        // Submit to the async worker pool. The pool worker will send an
        // AckItem back via the ack channel; the select! loop calls
        // apply_pool_ack to advance the machine and record metrics.
        let metadata = BatchMetadata {
            resource_attrs: Arc::clone(&self.resource_attrs),
            observed_time_ns: now_nanos(),
        };
        // Use tokio::time::Instant so queue-wait and batch-latency metrics
        // measure simulated time under Turmoil, not wall-clock time.
        let submitted_at = tokio::time::Instant::now();
        self.pool
            .submit(WorkItem {
                num_rows: result.num_rows() as u64,
                batch: result,
                metadata,
                tickets: sending,
                submitted_at,
                scan_ns: scan_elapsed.as_nanos() as u64,
                transform_ns: transform_elapsed.as_nanos() as u64,
                batch_id,
                span: tracing::Span::current(),
            })
            .await;
        self.metrics
            .inflight_batches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Apply a pool `AckItem` — ack or reject its tickets and advance the machine.
    ///
    /// Called from the `select!` loop when a pool worker finishes a batch.
    fn apply_pool_ack(&mut self, ack: AckItem) {
        self.metrics
            .inflight_batches
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.finish_active_batch(ack.batch_id);
        if ack.success {
            self.metrics
                .record_batch(ack.num_rows, ack.scan_ns, ack.transform_ns, ack.output_ns);
            self.metrics.record_queue_wait(ack.queue_wait_ns);
            self.metrics.record_send_latency(ack.send_latency_ns);
            self.metrics
                .record_batch_latency(ack.submitted_at.elapsed().as_nanos() as u64);
        } else {
            self.metrics.inc_dropped_batch();
            self.metrics.output_error(&ack.output_name);
        }
        self.ack_all_tickets(ack.tickets, ack.success);
    }

    /// Ack or reject all Sending tickets and apply receipts to the machine.
    /// When a checkpoint advances, the new offset is persisted to the store.
    /// Flushes are throttled to at most once per 5 seconds to avoid fsync storms.
    fn ack_all_tickets(
        &mut self,
        tickets: Vec<logfwd_types::pipeline::BatchTicket<logfwd_types::pipeline::Sending, u64>>,
        success: bool,
    ) {
        let Some(ref mut machine) = self.machine else {
            return;
        };
        let mut any_advanced = false;
        for ticket in tickets {
            let receipt = if success {
                ticket.ack()
            } else {
                ticket.reject()
            };
            let advance = machine.apply_ack(receipt);
            if advance.advanced {
                if let (Some(ref mut store), Some(offset)) =
                    (self.checkpoint_store.as_mut(), advance.checkpoint)
                {
                    store.update(SourceCheckpoint {
                        source_id: advance.source.0,
                        path: None, // path is metadata, not required for restore
                        offset,
                    });
                    any_advanced = true;
                }
            }
        }
        // Flush to disk at most once per checkpoint_flush_interval to amortize fsync cost.
        // Advance the timer even on failure to prevent retry flooding.
        if any_advanced && self.last_checkpoint_flush.elapsed() >= self.checkpoint_flush_interval {
            self.last_checkpoint_flush = tokio::time::Instant::now();
            if let Some(ref mut store) = self.checkpoint_store {
                if let Err(e) = store.flush() {
                    tracing::warn!(error = %e, "pipeline: checkpoint flush error");
                }
            }
        }
    }
}

/// Input polling loop for a dedicated OS thread. Reads from the source,
/// parses format, and sends accumulated JSON lines through the channel.
///
/// Accumulates data in the input's `buf` across multiple poll cycles
/// before sending. Sends when the buffer reaches `batch_target_bytes` or
/// after `batch_timeout` of accumulated data, whichever comes first.
/// This avoids flooding the channel with tiny fragments.
#[cfg(not(feature = "turmoil"))]
#[allow(clippy::too_many_arguments)]
fn input_poll_loop(
    mut input: InputState,
    tx: tokio::sync::mpsc::Sender<ChannelMsg>,
    metrics: Arc<PipelineMetrics>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    input_index: usize,
) {
    // Track when the buffer first became non-empty, not when we last
    // sent. This ensures batch_timeout measures "time since first data
    // arrived in this batch", preventing tiny flushes after idle periods.
    let mut buffered_since: Option<Instant> = None;
    loop {
        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            break;
        }

        // FramedInput handles newline framing, remainder management, and
        // format processing (CRI/Auto/passthrough). Events arriving here
        // contain scanner-ready bytes.
        let events = match input.source.poll() {
            Ok(e) => e,
            Err(e) => {
                input.stats.set_health(reduce_component_health(
                    input.stats.health(),
                    HealthTransitionEvent::PollFailed,
                ));
                tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));

        if events.is_empty() {
            std::thread::sleep(poll_interval);
        } else {
            for event in events {
                match event {
                    InputEvent::Data { bytes, .. } => {
                        input.buf.extend_from_slice(&bytes);
                    }
                    InputEvent::Rotated { .. } => {
                        input.stats.inc_rotations();
                        tracing::info!(input = input.source.name(), "input.file_rotated");
                    }
                    InputEvent::Truncated { .. } => {
                        input.stats.inc_rotations();
                        tracing::info!(input = input.source.name(), "input.file_truncated");
                    }
                    InputEvent::EndOfFile { .. } => {}
                }
            }
            if buffered_since.is_none() && !input.buf.is_empty() {
                buffered_since = Some(Instant::now());
            }
        }

        // Send when buffer reaches target size OR timeout since first
        // data in this batch has elapsed.
        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let should_send =
            input.buf.len() >= batch_target_bytes || (!input.buf.is_empty() && timeout_elapsed);
        if should_send {
            let data = input.buf.split().freeze();

            // Snapshot file offsets (hot path — no PathBuf allocation).
            let checkpoints = input.source.checkpoint_data();

            let msg = ChannelMsg::Data {
                bytes: data,
                checkpoints,
                queued_at: tokio::time::Instant::now(),
                input_index,
            };
            if blocking_send_channel_msg(input.source.name(), &tx, &metrics, msg).is_err() {
                break;
            }
            buffered_since = None;
        }
    }

    // Drain any remaining buffered data on shutdown.
    if !input.buf.is_empty() {
        let data = input.buf.split().freeze();
        let checkpoints = input.source.checkpoint_data();
        let msg = ChannelMsg::Data {
            bytes: data,
            checkpoints,
            queued_at: tokio::time::Instant::now(),
            input_index,
        };
        send_shutdown_drain_msg_blocking(input.source.name(), &tx, &metrics, msg);
    }
    input.stats.set_health(reduce_component_health(
        input.stats.health(),
        HealthTransitionEvent::ShutdownCompleted,
    ));
}

/// Flush checkpoint store with bounded retry (3 attempts, 100ms between).
///
/// The final shutdown flush is the last chance to persist checkpoint progress.
/// A single failure here loses all checkpoint advancement from the run.
/// Retry with brief sleeps to handle transient I/O errors (disk busy, NFS glitch).
///
/// Uses `tokio::time::sleep` for the retry delay so the async task yields
/// between attempts (Turmoil-compatible). Note: `store.flush()` itself is
/// synchronous I/O; only the inter-retry sleep is non-blocking.
async fn flush_checkpoint_with_retry(store: &mut dyn CheckpointStore) {
    const MAX_ATTEMPTS: u32 = 3;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    for attempt in 0..MAX_ATTEMPTS {
        match store.flush() {
            Ok(()) => {
                if attempt > 0 {
                    tracing::info!(attempt, "pipeline: checkpoint flush succeeded after retry");
                }
                return;
            }
            Err(e) => {
                if attempt + 1 < MAX_ATTEMPTS {
                    tracing::warn!(
                        attempt,
                        error = %e,
                        "pipeline: checkpoint flush failed, retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                } else {
                    tracing::error!(
                        attempts = MAX_ATTEMPTS,
                        error = %e,
                        "pipeline: checkpoint flush failed after all retries — \
                         checkpoint progress from this run may be lost"
                    );
                }
            }
        }
    }
}

#[cfg(not(feature = "turmoil"))]
fn blocking_send_channel_msg(
    input_name: &str,
    tx: &tokio::sync::mpsc::Sender<ChannelMsg>,
    metrics: &PipelineMetrics,
    msg: ChannelMsg,
) -> Result<(), tokio::sync::mpsc::error::SendError<ChannelMsg>> {
    match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
            tracing::warn!(input = input_name, "input.backpressure");
            metrics.inc_backpressure_stall();
            tx.blocking_send(msg)
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(msg)) => {
            Err(tokio::sync::mpsc::error::SendError(msg))
        }
    }
}

#[cfg(not(feature = "turmoil"))]
fn send_shutdown_drain_msg_blocking(
    input_name: &str,
    tx: &tokio::sync::mpsc::Sender<ChannelMsg>,
    metrics: &PipelineMetrics,
    msg: ChannelMsg,
) {
    if let Err(e) = blocking_send_channel_msg(input_name, tx, metrics, msg) {
        tracing::warn!(
            input = input_name,
            error = %e,
            "input.channel_closed_on_shutdown_drain"
        );
    }
}

/// Async version of `input_poll_loop` for simulation testing.
/// Uses `tokio::time::sleep` instead of `std::thread::sleep` so that
/// Turmoil's simulated time advances deterministically.
#[cfg(feature = "turmoil")]
#[allow(clippy::too_many_arguments)]
async fn async_input_poll_loop(
    mut input: InputState,
    tx: tokio::sync::mpsc::Sender<ChannelMsg>,
    _metrics: Arc<PipelineMetrics>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    input_index: usize,
) {
    // Use tokio::time::Instant (not std::time::Instant) so that elapsed()
    // measures simulated time under Turmoil, not real wall-clock time.
    let mut buffered_since: Option<tokio::time::Instant> = None;
    loop {
        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            break;
        }

        let events = match input.source.poll() {
            Ok(e) => e,
            Err(e) => {
                input.stats.set_health(reduce_component_health(
                    input.stats.health(),
                    HealthTransitionEvent::PollFailed,
                ));
                tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));

        if events.is_empty() {
            tokio::time::sleep(poll_interval).await;
        } else {
            for event in events {
                match event {
                    InputEvent::Data { bytes, .. } => {
                        input.buf.extend_from_slice(&bytes);
                    }
                    InputEvent::Rotated { .. } => {
                        input.stats.inc_rotations();
                    }
                    InputEvent::Truncated { .. } => {
                        input.stats.inc_rotations();
                    }
                    InputEvent::EndOfFile { .. } => {}
                }
            }
            if buffered_since.is_none() && !input.buf.is_empty() {
                buffered_since = Some(tokio::time::Instant::now());
            }
        }

        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let should_send =
            input.buf.len() >= batch_target_bytes || (!input.buf.is_empty() && timeout_elapsed);
        if should_send {
            let data = input.buf.split().freeze();
            let checkpoints = input.source.checkpoint_data();
            let msg = ChannelMsg::Data {
                bytes: data,
                checkpoints,
                queued_at: tokio::time::Instant::now(),
                input_index,
            };
            if tx.send(msg).await.is_err() {
                break;
            }
            buffered_since = None;
        }
    }

    // Drain remaining
    if !input.buf.is_empty() {
        let data = input.buf.split().freeze();
        let checkpoints = input.source.checkpoint_data();
        let msg = ChannelMsg::Data {
            bytes: data,
            checkpoints,
            queued_at: tokio::time::Instant::now(),
            input_index,
        };
        if let Err(e) = tx.send(msg).await {
            tracing::warn!(
                input = input.source.name(),
                error = %e,
                "input.channel_closed_on_shutdown_drain"
            );
        }
    }
    input.stats.set_health(reduce_component_health(
        input.stats.health(),
        HealthTransitionEvent::ShutdownCompleted,
    ));
}

// ---------------------------------------------------------------------------
// Input construction
// ---------------------------------------------------------------------------

/// Build a format processor from the config format.
fn make_format(
    name: &str,
    input_type: InputType,
    format: &Format,
    stats: &Arc<ComponentStats>,
) -> Result<FormatDecoder, String> {
    const CRI_MAX_MESSAGE: usize = 2 * 1024 * 1024;
    let proc = match format {
        Format::Cri => FormatDecoder::cri(CRI_MAX_MESSAGE, Arc::clone(stats)),
        Format::Auto => FormatDecoder::auto(CRI_MAX_MESSAGE, Arc::clone(stats)),
        Format::Json => FormatDecoder::passthrough_json(Arc::clone(stats)),
        Format::Raw => FormatDecoder::passthrough(Arc::clone(stats)),
        unsupported => {
            return Err(format!(
                "input '{name}': format {:?} is not supported for {:?} inputs",
                unsupported, input_type
            ));
        }
    };
    Ok(proc)
}

fn validate_input_format(name: &str, input_type: InputType, format: &Format) -> Result<(), String> {
    match input_type {
        InputType::Generator | InputType::Otlp => {
            if !matches!(format, Format::Json) {
                return Err(format!(
                    "input '{name}': format {:?} is not supported for {:?} inputs (expected json)",
                    format, input_type
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn build_input_state(
    name: &str,
    cfg: &InputConfig,
    stats: Arc<ComponentStats>,
) -> Result<InputState, String> {
    let (raw_source, format, buf_cap): (Box<dyn InputSource>, Format, usize) = match cfg.input_type
    {
        InputType::File => {
            let path = cfg
                .path
                .as_ref()
                .ok_or_else(|| format!("input '{name}': file input requires 'path'"))?;
            let format = cfg.format.clone().unwrap_or(Format::Auto);
            let mut tail_config = TailConfig {
                start_from_end: false,
                poll_interval_ms: 50,
                read_buf_size: 256 * 1024,
                max_open_files: cfg.max_open_files.unwrap_or(1024),
                ..Default::default()
            };
            if let Some(interval) = cfg.glob_rescan_interval_ms {
                tail_config.glob_rescan_interval_ms = interval;
            }
            let is_glob = path.contains('*') || path.contains('?') || path.contains('[');
            let source = if is_glob {
                FileInput::new_with_globs(name.to_string(), &[path.as_str()], tail_config)
            } else {
                FileInput::new(name.to_string(), &[PathBuf::from(path)], tail_config)
            }
            .map_err(|e| format!("input '{name}': failed to create tailer: {e}"))?;
            validate_input_format(name, InputType::File, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Generator => {
            use logfwd_io::generator::{
                GeneratorAttributeValue, GeneratorComplexity, GeneratorConfig,
                GeneratorGeneratedField, GeneratorInput, GeneratorProfile,
            };
            let generator_cfg = cfg.generator.as_ref();
            let config = GeneratorConfig {
                events_per_sec: generator_cfg.and_then(|c| c.events_per_sec).unwrap_or(0),
                batch_size: generator_cfg.and_then(|c| c.batch_size).unwrap_or(1000),
                total_events: generator_cfg.and_then(|c| c.total_events).unwrap_or(0),
                complexity: match generator_cfg.and_then(|c| c.complexity.clone()) {
                    Some(GeneratorComplexityConfig::Complex) => GeneratorComplexity::Complex,
                    Some(GeneratorComplexityConfig::Simple) | None => GeneratorComplexity::Simple,
                    Some(_) => GeneratorComplexity::Simple,
                },
                profile: match generator_cfg.and_then(|c| c.profile.clone()) {
                    Some(GeneratorProfileConfig::Record) => GeneratorProfile::Record,
                    Some(GeneratorProfileConfig::Logs) | None => GeneratorProfile::Logs,
                    Some(_) => GeneratorProfile::Logs,
                },
                attributes: generator_cfg
                    .map(|c| {
                        c.attributes
                            .iter()
                            .map(|(k, v)| {
                                let value = match v {
                                    GeneratorAttributeValueConfig::String(v) => {
                                        GeneratorAttributeValue::String(v.clone())
                                    }
                                    GeneratorAttributeValueConfig::Null => {
                                        GeneratorAttributeValue::Null
                                    }
                                    GeneratorAttributeValueConfig::Integer(v) => {
                                        GeneratorAttributeValue::Integer(*v)
                                    }
                                    GeneratorAttributeValueConfig::Float(v) => {
                                        GeneratorAttributeValue::Float(*v)
                                    }
                                    GeneratorAttributeValueConfig::Bool(v) => {
                                        GeneratorAttributeValue::Bool(*v)
                                    }
                                    _ => GeneratorAttributeValue::Null,
                                };
                                (k.clone(), value)
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                sequence: generator_cfg.and_then(|c| {
                    c.sequence.as_ref().map(|seq| GeneratorGeneratedField {
                        field: seq.field.clone(),
                        start: seq.start.unwrap_or(1),
                    })
                }),
                event_created_unix_nano_field: generator_cfg
                    .and_then(|c| c.event_created_unix_nano_field.clone()),
                ..Default::default()
            };
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Generator, &format)?;
            let source = GeneratorInput::new(name, config);
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Otlp => {
            let addr = cfg
                .listen
                .as_ref()
                .ok_or_else(|| format!("input '{name}': otlp input requires 'listen'"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Otlp, &format)?;
            let source = logfwd_io::otlp_receiver::OtlpReceiverInput::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to start OTLP receiver: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Udp => {
            let addr = cfg
                .listen
                .as_ref()
                .ok_or_else(|| format!("input '{name}': udp input requires 'listen'"))?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for UDP inputs (CRI is a file-based container log format)"
                ));
            }
            let source = logfwd_io::udp_input::UdpInput::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to bind UDP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Udp, &format)?;
            (Box::new(source), format, 1024 * 1024)
        }
        InputType::Tcp => {
            let addr = cfg
                .listen
                .as_ref()
                .ok_or_else(|| format!("input '{name}': tcp input requires 'listen'"))?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for TCP inputs (CRI is a file-based container log format)"
                ));
            }
            let source = logfwd_io::tcp_input::TcpInput::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to bind TCP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Tcp, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        _ => {
            return Err(format!(
                "input '{name}': type {:?} not yet supported",
                cfg.input_type
            ));
        }
    };

    // Wrap the raw transport with framing + format processing.
    let format_proc = make_format(name, cfg.input_type.clone(), &format, &stats)?;
    let framed = FramedInput::new(raw_source, format_proc, Arc::clone(&stats));

    Ok(InputState {
        source: Box::new(framed),
        buf: BytesMut::with_capacity(buf_cap),
        stats,
    })
}

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
    use std::sync::atomic::Ordering;
    // std::time::Instant is cfg-gated in the parent module for turmoil compatibility,
    // but tests need it for timeout deadlines regardless of the turmoil feature flag.
    use serial_test::serial;
    use std::time::Instant;

    use logfwd_arrow::scanner::Scanner;
    use logfwd_config::{Format, OutputConfig, OutputType};
    use logfwd_core::scan_config::ScanConfig;
    use logfwd_io::diagnostics::ComponentStats;
    use logfwd_test_utils::sinks::{DevNullSink, FailingSink, FrozenSink, SlowSink};
    use logfwd_test_utils::test_meter;

    #[test]
    fn test_build_sink_factory_stdout() {
        let cfg = OutputConfig {
            name: Some("test".to_string()),
            output_type: OutputType::Stdout,
            format: Some(Format::Json),
            ..Default::default()
        };
        let factory =
            build_sink_factory("test", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
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
            protocol: Some("http".to_string()),
            compression: Some("zstd".to_string()),
            ..Default::default()
        };
        let factory =
            build_sink_factory("otel", &cfg, None, Arc::new(ComponentStats::new())).unwrap();
        assert_eq!(factory.name(), "otel");
    }

    #[test]
    fn test_build_sink_factory_missing_endpoint() {
        let cfg = OutputConfig {
            name: Some("bad".to_string()),
            output_type: OutputType::Otlp,
            ..Default::default()
        };
        let result = build_sink_factory("bad", &cfg, None, Arc::new(ComponentStats::new()));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("endpoint"), "got: {err}");
    }

    #[cfg(not(feature = "turmoil"))]
    #[test]
    fn test_shutdown_drain_send_closed_channel_is_non_fatal() {
        let (tx, rx) = tokio::sync::mpsc::channel::<ChannelMsg>(1);
        drop(rx);
        let metrics = PipelineMetrics::new("default", "SELECT * FROM logs", &test_meter());
        let msg = ChannelMsg::Data {
            bytes: Bytes::from_static(b"{\"level\":\"INFO\"}\n"),
            checkpoints: vec![],
            queued_at: tokio::time::Instant::now(),
            input_index: 0,
        };
        send_shutdown_drain_msg_blocking("test-input", &tx, &metrics, msg);
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
  type: null
"#;
        let config = logfwd_config::Config::load_str(yaml).unwrap();
        let pipe_cfg = &config.pipelines["default"];
        let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
            .unwrap_or_else(|err| panic!("unexpected pipeline build error: {err}"));
        let events = pipeline.inputs[0].source.poll().unwrap();
        let bytes = match &events[0] {
            InputEvent::Data { bytes, .. } => bytes,
            _ => panic!("expected generator data event"),
        };
        let mut scanner = Scanner::new(ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
        });
        let batch = scanner.scan_detached(Bytes::from(bytes.clone())).unwrap();
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
                batch: arrow::record_batch::RecordBatch,
                _meta: &BatchMetadata,
            ) -> Result<
                smallvec::SmallVec<[arrow::record_batch::RecordBatch; 1]>,
                crate::processor::ProcessorError,
            > {
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

            fn flush(&mut self) -> smallvec::SmallVec<[arrow::record_batch::RecordBatch; 1]> {
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

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
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

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
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

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Write JSON lines that do NOT contain `nonexistent_col`.
        let mut data = String::new();
        for i in 0..5 {
            data.push_str(&format!(r#"{{"level":"INFO","msg":"hello {}"}}"#, i));
            data.push('\n');
        }
        std::fs::write(&log_path, data.as_bytes()).unwrap();

        // SQL references a column that will never be present → DataFusion
        // returns an error at execution time, not at parse / new() time.
        let yaml = format!(
            r#"
input:
  type: file
  path: {}
  format: json
transform: "SELECT nonexistent_col FROM logs"
output:
  type: stdout
  format: json
"#,
            log_path.display()
        );
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

    #[cfg(not(feature = "turmoil"))]
    #[tokio::test]
    async fn test_blocking_send_records_backpressure_stall() {
        use std::sync::atomic::Ordering;

        let meter = test_meter();
        let metrics = Arc::new(PipelineMetrics::new("test", "SELECT * FROM logs", &meter));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(1);

        // Fill the channel (capacity=1).
        tx.try_send(ChannelMsg::Data {
            bytes: Bytes::from_static(&[1]),
            checkpoints: vec![],
            queued_at: tokio::time::Instant::now(),
            input_index: 0,
        })
        .unwrap();

        let tx2 = tx.clone();
        let metrics2 = Arc::clone(&metrics);
        let handle = std::thread::spawn(move || {
            blocking_send_channel_msg(
                "test",
                &tx2,
                &metrics2,
                ChannelMsg::Data {
                    bytes: Bytes::from_static(&[2]),
                    checkpoints: vec![],
                    queued_at: tokio::time::Instant::now(),
                    input_index: 0,
                },
            )
        });

        for _ in 0..50 {
            if metrics.backpressure_stalls.load(Ordering::Relaxed) > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(
            metrics.backpressure_stalls.load(Ordering::Relaxed),
            1,
            "full channel should increment backpressure_stalls before blocking"
        );

        // Drain both messages.
        let msg1 = rx.recv().await.unwrap();
        let msg2 = rx.recv().await.unwrap();
        assert!(
            matches!(msg1, ChannelMsg::Data { bytes, .. } if bytes == Bytes::from_static(&[1]))
        );
        assert!(
            matches!(msg2, ChannelMsg::Data { bytes, .. } if bytes == Bytes::from_static(&[2]))
        );
        assert!(
            handle.join().unwrap().is_ok(),
            "blocking send should succeed"
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

    /// Output errors must not crash the pipeline — batches are dropped
    /// and processing continues.
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
    async fn test_async_fanout_output_errors_only_increment_failing_output() {
        // This test verifies that a persistently-failing output does not
        // crash the pipeline and is recorded as a dropped batch.
        //
        // With the async worker pool, retries are built into the pool. An
        // always-failing sink causes the pool to exhaust retries and return
        // AckItem { success: false }, which increments dropped_batches_total.
        //
        // Per-sink error tracking within async fanout is not currently
        // supported by the pool's AckItem protocol (tracked in issue #702).
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
        // always-failing: fails every call — pool exhausts retries → dropped
        pipeline = pipeline.with_sink(Box::new(FailingSink::new(u32::MAX)));
        pipeline.batch_timeout = Duration::from_millis(20);

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        tokio::spawn(async move {
            // Give pool time to exhaust retries (4 attempts × ~100ms backoff ≈ 700ms)
            tokio::time::sleep(Duration::from_millis(1500)).await;
            sd.cancel();
        });

        let result = pipeline.run_async(&shutdown).await;
        assert!(
            result.is_ok(),
            "fanout output errors should not crash pipeline"
        );

        let dropped = pipeline
            .metrics
            .dropped_batches_total
            .load(Ordering::Relaxed);
        assert!(
            dropped > 0,
            "expected dropped_batches_total > 0, got {dropped}"
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

        let by_timeout = pipeline.metrics.flush_by_timeout.load(Ordering::Relaxed);
        assert!(
            by_timeout > 0,
            "slow output: at least one flush should be by timeout, got {by_timeout}"
        );
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
    fn pipeline_with_sink(
        log_path: &std::path::Path,
        sink: Box<dyn logfwd_output::Sink>,
    ) -> Pipeline {
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

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();

        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.dropped_batches_total.load(Ordering::Relaxed) > 0 {
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
    fn test_channel_msg_data_carries_checkpoints() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(4);

        tx.try_send(ChannelMsg::Data {
            bytes: Bytes::from_static(b"test\n"),
            checkpoints: vec![(SourceId(42), ByteOffset(1000))],
            queued_at: tokio::time::Instant::now(),
            input_index: 0,
        })
        .unwrap();

        let msg = rx.try_recv().unwrap();
        assert!(matches!(&msg, ChannelMsg::Data { checkpoints, .. } if checkpoints.len() == 1));
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
    use logfwd_arrow::scanner::Scanner;
    use logfwd_core::scan_config::ScanConfig;

    /// JSON format: raw bytes pass directly through to scanner.
    #[test]
    fn json_format_direct_to_scanner() {
        let input =
            b"{\"level\":\"INFO\",\"msg\":\"hello\"}\n{\"level\":\"WARN\",\"msg\":\"world\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: false,
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(input.to_vec())).unwrap();
        assert_eq!(batch.num_rows(), 2);
        // Single-type string fields: bare names
        assert!(batch.schema().field_with_name("level").is_ok());
        assert!(batch.schema().field_with_name("msg").is_ok());
    }

    /// Raw format: lines captured as _raw when keep_raw is true.
    #[test]
    fn raw_format_captures_lines() {
        let input = b"plain text line 1\nplain text line 2\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            keep_raw: true,
            validate_utf8: false,
        };
        let mut scanner = Scanner::new(config);
        let batch = scanner.scan_detached(Bytes::from(input.to_vec())).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field_with_name("_raw").is_ok());
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
            keep_raw: false,
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
            keep_raw: false,
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
    use logfwd_arrow::scanner::Scanner;
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
                keep_raw: false,
                validate_utf8: false,
            };
            let mut scanner_whole = Scanner::new(ScanConfig { wanted_fields: vec![], extract_all: true, keep_raw: false, validate_utf8: false });
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

            let config2 = ScanConfig { wanted_fields: vec![], extract_all: true, keep_raw: false, validate_utf8: false }; let mut scanner_split = Scanner::new(config2);
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
