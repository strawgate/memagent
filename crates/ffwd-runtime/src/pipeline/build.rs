use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::Meter;

#[cfg(feature = "datafusion")]
use ffwd_config::{EnrichmentConfig, GeoDatabaseFormat};
use ffwd_config::{Format, InputTypeConfig, OutputConfigV2, PipelineConfig, SourceMetadataStyle};
use ffwd_diagnostics::diagnostics::PipelineMetrics;
use ffwd_io::checkpoint::{
    CheckpointStore, FileCheckpointStore, SourceCheckpoint, default_data_dir,
};
use ffwd_output::{AsyncFanoutFactory, SinkFactory, build_sink_factory};
use ffwd_types::field_names;
use ffwd_types::pipeline::{PipelineMachine, SourceId};
use ffwd_types::source_metadata::SourceMetadataPlan;

use super::input_build::build_input_state;
#[cfg(not(feature = "turmoil"))]
use super::source_metadata_style_source_path;
use super::{Pipeline, SourcePipeline, source_metadata_style_needs_source_paths};

// ── Pipeline defaults ──────────────────────────────────────────────────
/// Default output worker count when `pipelines.<name>.workers` is unset.
pub(crate) const DEFAULT_WORKERS: usize = 4;
/// Default target batch size in bytes; reaching this target triggers a flush.
pub(crate) const DEFAULT_BATCH_TARGET_BYTES: usize = 4 * 1024 * 1024;
/// Default maximum time a partial batch waits before flushing.
pub(crate) const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_millis(100);
/// Default interval between input polls when `poll_interval_ms` is unset.
pub(crate) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Default idle duration before recyclable output workers shut down.
pub(crate) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Default minimum interval between checkpoint flushes.
pub(crate) const DEFAULT_CHECKPOINT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Default maximum time to wait for worker-pool drain before cancellation.
pub(crate) const DEFAULT_POOL_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

fn input_type_exposes_public_source_paths(type_config: &InputTypeConfig) -> bool {
    // Keep this list tied to InputSource::source_paths() implementations.
    matches!(
        type_config,
        InputTypeConfig::File(_) | InputTypeConfig::S3(_)
    )
}

/// Spawn a periodic reload task for a file-backed enrichment table.
///
/// Calls `table.reload()` on a blocking thread every `interval_secs`.
/// `kind` is used for log messages (e.g. "CSV", "JSONL", "KV file").
#[cfg(feature = "datafusion")]
fn spawn_enrichment_reload<T, E>(
    table: &Arc<T>,
    table_name: &str,
    kind: &'static str,
    interval_secs: u64,
    reload: fn(&T) -> Result<usize, E>,
) where
    T: Send + Sync + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let t = Arc::clone(table);
    let name = table_name.to_string();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let t2 = Arc::clone(&t);
                match tokio::task::spawn_blocking(move || reload(&t2)).await {
                    Ok(Ok(n)) => tracing::debug!(
                        table = %name, rows = n,
                        "{kind} enrichment table reloaded"
                    ),
                    Ok(Err(e)) => tracing::warn!(
                        table = %name, error = %e,
                        "{kind} enrichment table reload failed"
                    ),
                    Err(e) => tracing::warn!(
                        table = %name, error = %e,
                        "{kind} enrichment table reload task panicked"
                    ),
                }
            }
        });
    } else {
        tracing::warn!(
            table = %name,
            "refresh_interval ignored: no active Tokio runtime"
        );
    }
}

impl Pipeline {
    /// Construct a pipeline from parsed YAML config.
    pub fn from_config(
        name: &str,
        config: &PipelineConfig,
        meter: &Meter,
        base_path: Option<&Path>,
    ) -> Result<Self, String> {
        Self::from_config_with_data_dir(name, config, meter, base_path, None)
    }

    /// Construct a pipeline from parsed YAML config using an explicit state directory.
    pub fn from_config_with_data_dir(
        name: &str,
        config: &PipelineConfig,
        meter: &Meter,
        base_path: Option<&Path>,
        data_dir: Option<&Path>,
    ) -> Result<Self, String> {
        if config.inputs.is_empty() {
            return Err("at least one input is required".to_string());
        }
        if config.outputs.is_empty() {
            return Err("at least one output is required".to_string());
        }
        if config.workers == Some(0) {
            return Err("workers must be >= 1".to_string());
        }
        if config.batch_target_bytes == Some(0) {
            return Err("batch_target_bytes must be > 0".to_string());
        }

        // Collect enrichment sources once — they are shared across all
        // per-input transforms.
        #[cfg(feature = "datafusion")]
        let (enrichment_tables, geo_database) = {
            let mut enrichment_tables: Vec<Arc<dyn crate::transform::enrichment::EnrichmentTable>> =
                Vec::new();
            let mut geo_database: Option<Arc<dyn crate::transform::enrichment::GeoDatabase>> = None;

            for enrichment in &config.enrichment {
                match enrichment {
                    EnrichmentConfig::GeoDatabase(geo_cfg) => {
                        let mut path = PathBuf::from(&geo_cfg.path);
                        if path.is_relative()
                            && let Some(base) = base_path
                        {
                            path = base.join(path);
                        }

                        let initial_db: Arc<dyn crate::transform::enrichment::GeoDatabase> =
                            match geo_cfg.format {
                                GeoDatabaseFormat::Mmdb => Arc::new(
                                    crate::transform::udf::geo_lookup::MmdbDatabase::open(&path)
                                        .map_err(|e| {
                                            format!(
                                                "failed to open geo database '{}': {e}",
                                                path.display()
                                            )
                                        })?,
                                ),
                                GeoDatabaseFormat::CsvRange => Arc::new(
                                    crate::transform::udf::CsvRangeDatabase::open(&path).map_err(
                                        |e| {
                                            format!(
                                                "failed to open CSV geo database '{}': {e}",
                                                path.display()
                                            )
                                        },
                                    )?,
                                ),
                                _ => {
                                    return Err(format!(
                                        "unsupported geo database format for '{}'",
                                        path.display()
                                    ));
                                }
                            };

                        if let Some(interval_secs) =
                            geo_cfg.refresh_interval.map(ffwd_config::PositiveSecs::get)
                        {
                            let reloadable = Arc::new(
                                crate::transform::enrichment::ReloadableGeoDb::new(initial_db),
                            );
                            let reload_handle = reloadable.reload_handle();
                            let reload_path = path.clone();
                            let reload_format = geo_cfg.format.clone();

                            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                                handle.spawn(async move {
                                let mut ticker = tokio::time::interval(Duration::from_secs(
                                    interval_secs,
                                ));
                                ticker.tick().await;
                                loop {
                                    ticker.tick().await;
                                    let p = reload_path.clone();
                                    let fmt = reload_format.clone();
                                    let result = tokio::task::spawn_blocking(move || -> Result<Arc<dyn crate::transform::enrichment::GeoDatabase>, String> {
                                        match fmt {
                                            GeoDatabaseFormat::Mmdb => {
                                                crate::transform::udf::geo_lookup::MmdbDatabase::open(&p)
                                                    .map(|db| Arc::new(db) as Arc<dyn crate::transform::enrichment::GeoDatabase>)
                                                    .map_err(|e| e.to_string())
                                            }
                                            GeoDatabaseFormat::CsvRange => {
                                                crate::transform::udf::CsvRangeDatabase::open(&p)
                                                    .map(|db| Arc::new(db) as Arc<dyn crate::transform::enrichment::GeoDatabase>)
                                                    .map_err(|e| e.to_string())
                                            }
                                            _ => Err(format!("unsupported geo database format for reload: {fmt:?}")),
                                        }
                                    })
                                    .await;
                                    match result {
                                        Ok(Ok(db)) => {
                                            reload_handle.replace(db);
                                            tracing::info!(
                                                path = %reload_path.display(),
                                                "geo database reloaded"
                                            );
                                        }
                                        Ok(Err(e)) => tracing::warn!(
                                            path = %reload_path.display(),
                                            error = %e,
                                            "geo database reload failed, keeping previous"
                                        ),
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            "geo database reload task panicked"
                                        ),
                                    }
                                }
                            });
                            } else {
                                tracing::warn!(
                                    path = %path.display(),
                                    "refresh_interval ignored: no active Tokio runtime"
                                );
                            }

                            geo_database = Some(
                                reloadable as Arc<dyn crate::transform::enrichment::GeoDatabase>,
                            );
                        } else {
                            geo_database = Some(initial_db);
                        }
                    }
                    EnrichmentConfig::Static(cfg) => {
                        let labels: Vec<(String, String)> = cfg
                            .labels
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        let table = Arc::new(
                            crate::transform::enrichment::StaticTable::new(
                                &cfg.table_name,
                                &labels,
                            )
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                        );
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::HostInfo(cfg) => {
                        let style = match cfg.style {
                            ffwd_config::HostInfoStyle::Raw => {
                                crate::transform::enrichment::HostInfoStyle::Raw
                            }
                            ffwd_config::HostInfoStyle::Ecs => {
                                crate::transform::enrichment::HostInfoStyle::Ecs
                            }
                            ffwd_config::HostInfoStyle::Otel => {
                                crate::transform::enrichment::HostInfoStyle::Otel
                            }
                        };
                        let table = Arc::new(
                            crate::transform::enrichment::HostInfoTable::with_style(style),
                        );
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::K8sPath(cfg) => {
                        let table = Arc::new(crate::transform::enrichment::K8sPathTable::new(
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
                        let table = Arc::new(crate::transform::enrichment::CsvFileTable::new(
                            &cfg.table_name,
                            &path,
                        ));
                        table
                            .reload()
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                        if let Some(interval_secs) =
                            cfg.refresh_interval.map(ffwd_config::PositiveSecs::get)
                        {
                            spawn_enrichment_reload(
                                &table,
                                &cfg.table_name,
                                "CSV",
                                interval_secs,
                                crate::transform::enrichment::CsvFileTable::reload,
                            );
                        }
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::Jsonl(cfg) => {
                        let mut path = PathBuf::from(&cfg.path);
                        if path.is_relative()
                            && let Some(base) = base_path
                        {
                            path = base.join(path);
                        }
                        let table =
                            Arc::new(crate::transform::enrichment::JsonLinesFileTable::new(
                                &cfg.table_name,
                                &path,
                            ));
                        table
                            .reload()
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                        if let Some(interval_secs) =
                            cfg.refresh_interval.map(ffwd_config::PositiveSecs::get)
                        {
                            spawn_enrichment_reload(
                                &table,
                                &cfg.table_name,
                                "JSONL",
                                interval_secs,
                                crate::transform::enrichment::JsonLinesFileTable::reload,
                            );
                        }
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::EnvVars(cfg) => {
                        let table = Arc::new(
                            crate::transform::enrichment::EnvTable::from_prefix(
                                &cfg.table_name,
                                &cfg.prefix,
                            )
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                        );
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::ProcessInfo(_) => {
                        let table = Arc::new(crate::transform::enrichment::ProcessInfoTable::new());
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::KvFile(cfg) => {
                        let mut path = PathBuf::from(&cfg.path);
                        if path.is_relative()
                            && let Some(base) = base_path
                        {
                            path = base.join(path);
                        }
                        let table = Arc::new(crate::transform::enrichment::KvFileTable::new(
                            &cfg.table_name,
                            &path,
                        ));
                        table
                            .reload()
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                        if let Some(interval_secs) =
                            cfg.refresh_interval.map(ffwd_config::PositiveSecs::get)
                        {
                            spawn_enrichment_reload(
                                &table,
                                &cfg.table_name,
                                "KV file",
                                interval_secs,
                                crate::transform::enrichment::KvFileTable::reload,
                            );
                        }
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::NetworkInfo(_) => {
                        let table = Arc::new(crate::transform::enrichment::NetworkInfoTable::new());
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::ContainerInfo(_) => {
                        let table =
                            Arc::new(crate::transform::enrichment::ContainerInfoTable::new());
                        enrichment_tables.push(table);
                    }
                    EnrichmentConfig::K8sClusterInfo(_) => {
                        let table =
                            Arc::new(crate::transform::enrichment::K8sClusterInfoTable::new());
                        enrichment_tables.push(table);
                    }
                    _ => return Err("unsupported enrichment type".to_owned()),
                }
            }

            (enrichment_tables, geo_database)
        };

        #[cfg(not(feature = "datafusion"))]
        if !config.enrichment.is_empty() {
            return Err(
                "pipeline enrichment requires DataFusion. Build default/full ffwd \
                 (or add `--features datafusion`)"
                    .to_string(),
            );
        }

        // The pipeline-level SQL is the fallback for inputs without their own.
        let pipeline_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");

        // For PipelineMetrics, use the pipeline-level SQL as the label.
        let mut metrics = PipelineMetrics::new(name, pipeline_sql, meter);

        // Open checkpoint store scoped to this pipeline name.
        let checkpoint_dir = data_dir
            .map_or_else(default_data_dir, Path::to_path_buf)
            .join(name);
        // In tests, avoid creating a default data dir unless explicitly requested.
        // In non-test builds, always try to open/create the checkpoint store so
        // first-run persistence works without out-of-band directory creation.
        let should_open_checkpoint_store =
            should_open_checkpoint_store(&checkpoint_dir, data_dir.is_some());
        let checkpoint_store = if should_open_checkpoint_store {
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

        // Build per-input SourcePipeline and IngestState.
        let mut inputs = Vec::new();
        let mut input_transforms = Vec::new();

        for (i, input_cfg) in config.inputs.iter().enumerate() {
            let mut resolved_cfg = input_cfg.clone();
            if let InputTypeConfig::File(ref mut f) = resolved_cfg.type_config {
                let mut path = PathBuf::from(&f.path);
                if path.is_relative()
                    && let Some(base) = base_path
                {
                    path = base.join(path);
                }
                if let Ok(abs_path) = std::fs::canonicalize(&path) {
                    f.path = abs_path.to_string_lossy().into_owned();
                } else {
                    f.path = path.to_string_lossy().into_owned();
                }
            }

            // Resolve journal_directory relative to config base_path.
            if let InputTypeConfig::Journald(ref mut j) = resolved_cfg.type_config
                && let Some(ref mut jd) = j.journald
                && let Some(ref dir) = jd.journal_directory
            {
                let mut path = PathBuf::from(dir);
                if path.is_relative()
                    && let Some(base) = base_path
                {
                    path = base.join(path);
                }
                jd.journal_directory = Some(path.to_string_lossy().into_owned());
            }

            let input_name = input_cfg
                .name
                .clone()
                .unwrap_or_else(|| format!("input_{i}"));
            let input_type_str = input_cfg.input_type().to_string();
            let input_stats = metrics.add_input(&input_name, &input_type_str);

            // Determine the SQL for this input: per-input > pipeline-level > passthrough.
            let input_sql = input_cfg.sql.as_deref().unwrap_or(pipeline_sql);

            let transform =
                crate::transform::SqlTransform::new(input_sql).map_err(|e| e.to_string())?;
            #[cfg(feature = "datafusion")]
            let mut transform = transform;

            // Wire up shared enrichment sources to this transform.
            #[cfg(feature = "datafusion")]
            {
                if let Some(ref db) = geo_database {
                    transform.set_geo_database(Arc::clone(db));
                }
                for table in &enrichment_tables {
                    transform
                        .add_enrichment_table(Arc::clone(table))
                        .map_err(|e| format!("input '{input_name}': enrichment error: {e}"))?;
                }
            }

            let mut scan_config = transform.scan_config();
            // Raw format sends plain text directly to the scanner, so capture
            // the original line in the canonical body field for downstream SQL
            // and sinks. Auto mode wraps plain-text fallback into JSON.
            if matches!(input_cfg.format, Some(Format::Raw)) {
                scan_config.line_field_name = Some(field_names::BODY.to_string());
            }
            let scanner = ffwd_arrow::scanner::Scanner::new(scan_config);
            let explicit_source_metadata_plan =
                transform.analyzer().explicit_source_metadata_plan();
            if explicit_source_metadata_plan.has_any()
                && input_cfg.source_metadata == SourceMetadataStyle::None
            {
                return Err(format!(
                    "pipeline '{name}' input '{input_name}': SQL references source metadata \
                     columns such as __source_id, but source_metadata is disabled; \
                     set source_metadata: fastforward on this input"
                ));
            }
            if explicit_source_metadata_plan.has_source_id
                && input_cfg.source_metadata != SourceMetadataStyle::Fastforward
            {
                return Err(format!(
                    "pipeline '{name}' input '{input_name}': SQL references internal source \
                     metadata column __source_id, but source_metadata is configured as {}; \
                     set source_metadata: fastforward on this input",
                    input_cfg.source_metadata
                ));
            }
            if source_metadata_style_needs_source_paths(input_cfg.source_metadata)
                && !input_type_exposes_public_source_paths(&input_cfg.type_config)
            {
                return Err(format!(
                    "pipeline '{name}' input '{input_name}': source_metadata style {} requires \
                     source paths, but input type {} does not expose public source path snapshots; \
                     currently only file and s3 inputs support public source path metadata styles",
                    input_cfg.source_metadata,
                    input_cfg.input_type()
                ));
            }
            #[cfg(feature = "turmoil")]
            let source_metadata_plan = {
                if input_cfg.source_metadata != SourceMetadataStyle::None {
                    return Err(format!(
                        "pipeline '{name}' input '{input_name}': source_metadata is not \
                         supported when built with the turmoil feature; the turmoil input path \
                         does not attach source metadata before SQL"
                    ));
                }
                SourceMetadataPlan::default()
            };
            #[cfg(not(feature = "turmoil"))]
            let source_metadata_plan = SourceMetadataPlan {
                has_source_id: input_cfg.source_metadata == SourceMetadataStyle::Fastforward,
                source_path: source_metadata_style_source_path(input_cfg.source_metadata),
            };

            input_transforms.push(SourcePipeline {
                scanner,
                transform,
                input_name: input_name.clone(),
                source_metadata_plan,
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
            build_output_factory_from_config(0, output_cfg, base_path, &mut metrics)?
        } else {
            let mut factories: Vec<Arc<dyn SinkFactory>> = Vec::new();
            for (i, output_cfg) in config.outputs.iter().enumerate() {
                factories.push(build_output_factory_from_config(
                    i,
                    output_cfg,
                    base_path,
                    &mut metrics,
                )?);
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
            (
                config.workers.unwrap_or(DEFAULT_WORKERS),
                DEFAULT_IDLE_TIMEOUT,
            )
        };
        let metrics = Arc::new(metrics);
        let pool = crate::worker_pool::OutputWorkerPool::new(
            factory,
            max_workers,
            idle_timeout,
            Arc::clone(&metrics),
        );

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
            batch_target_bytes: config
                .batch_target_bytes
                .unwrap_or(DEFAULT_BATCH_TARGET_BYTES),
            batch_timeout: config
                .batch_timeout_ms
                .map_or(DEFAULT_BATCH_TIMEOUT, Into::into),
            poll_interval: config
                .poll_interval_ms
                .map_or(DEFAULT_POLL_INTERVAL, Into::into),
            resource_attrs: Arc::from(resource_attrs),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store,
            held_tickets: Vec::new(),
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: DEFAULT_CHECKPOINT_FLUSH_INTERVAL,
            pool_drain_timeout: DEFAULT_POOL_DRAIN_TIMEOUT,
        })
    }
}

fn build_output_factory_from_config(
    index: usize,
    output_cfg: &OutputConfigV2,
    base_path: Option<&Path>,
    metrics: &mut PipelineMetrics,
) -> Result<Arc<dyn SinkFactory>, String> {
    let output_name = output_cfg
        .name()
        .map_or_else(|| format!("output_{index}"), str::to_owned);
    let output_type_str = output_cfg.output_type().to_string();
    let output_stats = metrics.add_output(&output_name, &output_type_str);

    build_sink_factory(&output_name, output_cfg, base_path, output_stats).map_err(|e| e.to_string())
}

fn should_open_checkpoint_store(checkpoint_dir: &Path, has_explicit_data_dir: bool) -> bool {
    if has_explicit_data_dir {
        return true;
    }

    if std::env::var_os("LOGFWD_DATA_DIR").is_some() {
        return true;
    }

    if cfg!(test) {
        return checkpoint_dir.exists();
    }

    std::env::var_os("LOGFWD_DISABLE_DEFAULT_CHECKPOINTS").is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffwd_config::{InputConfig, InputTypeConfig, OutputConfigV2, StdoutOutputConfig};
    use ffwd_types::source_metadata::SourcePathColumn;

    fn minimal_input(path: String) -> InputConfig {
        InputConfig {
            name: Some("input".to_string()),
            format: Some(Format::Json),
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::File(ffwd_config::FileTypeConfig {
                path,
                poll_interval_ms: None,
                read_buf_size: None,
                per_file_read_budget_bytes: None,
                adaptive_fast_polls_max: None,
                max_open_files: None,
                glob_rescan_interval_ms: None,
                start_at: None,
                encoding: None,
                follow_symlinks: None,
                ignore_older_secs: None,
                multiline: None,
                max_line_bytes: None,
            }),
        }
    }

    fn minimal_output() -> OutputConfigV2 {
        OutputConfigV2::Stdout(StdoutOutputConfig::default())
    }

    fn minimal_config(path: String) -> PipelineConfig {
        PipelineConfig {
            inputs: vec![minimal_input(path)],
            outputs: vec![minimal_output()],
            transform: None,
            enrichment: vec![],
            resource_attrs: std::collections::HashMap::new(),
            workers: None,
            batch_target_bytes: None,
            batch_timeout_ms: None,
            poll_interval_ms: None,
        }
    }

    #[test]
    fn from_config_accepts_native_typed_output_entry() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"msg\":\"hello\"}\n").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.outputs = vec![OutputConfigV2::Stdout(StdoutOutputConfig {
            name: Some("typed_stdout".to_string()),
            format: None,
        })];

        Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
            .expect("native typed output entry should build");
    }

    #[test]
    fn from_config_uses_explicit_data_dir_for_checkpoint_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log_path = dir.path().join("in.log");
        let data_dir = dir.path().join("state");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").expect("write input");
        let cfg = PipelineConfig {
            inputs: vec![minimal_input(log_path.to_string_lossy().into_owned())],
            transform: None,
            outputs: vec![minimal_output()],
            enrichment: Vec::new(),
            resource_attrs: Default::default(),
            workers: None,
            batch_target_bytes: None,
            batch_timeout_ms: None,
            poll_interval_ms: None,
        };

        let pipeline = Pipeline::from_config_with_data_dir(
            "p",
            &cfg,
            &ffwd_test_utils::test_meter(),
            None,
            Some(&data_dir),
        )
        .expect("pipeline should build with explicit data dir");

        assert!(
            pipeline.checkpoint_store.is_some(),
            "explicit data dir should open a checkpoint store"
        );
        assert!(
            data_dir.join("p").is_dir(),
            "checkpoint store should be rooted under the explicit data dir"
        );
    }

    // Zero batch_timeout_ms and poll_interval_ms are now rejected at parse
    // time by the PositiveMillis newtype, so there is no runtime test needed.

    #[test]
    fn workers_zero_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.workers = Some(0);
        let err = Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
            .err()
            .unwrap();
        assert!(
            err.contains("workers must be >= 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_metadata_disabled_rejects_explicit_source_column() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"msg\":\"hello\"}\n").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.transform = Some("SELECT __source_id FROM logs".to_string());

        let err = Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
            .err()
            .expect("explicit source metadata should require opt-in");
        assert!(
            err.contains("source_metadata is disabled"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_metadata_enabled_allows_explicit_source_column() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"msg\":\"hello\"}\n").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.inputs[0].source_metadata = SourceMetadataStyle::Fastforward;
        config.transform = Some("SELECT __source_id FROM logs".to_string());

        let pipeline =
            Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
                .expect("source metadata opt-in should allow source columns");
        assert!(
            pipeline.input_transforms[0]
                .source_metadata_plan
                .has_source_id
        );
    }

    #[test]
    fn public_source_metadata_style_rejects_explicit_source_id() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"msg\":\"hello\"}\n").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.inputs[0].source_metadata = SourceMetadataStyle::Ecs;
        config.transform = Some("SELECT __source_id FROM logs".to_string());

        let err = Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
            .err()
            .expect("public metadata style should reject internal source id");
        assert!(
            err.contains("source_metadata is configured as ecs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn public_source_path_style_rejects_inputs_without_source_paths() {
        let mut config = PipelineConfig {
            inputs: vec![InputConfig {
                name: Some("udp-in".to_string()),
                format: Some(Format::Json),
                sql: None,
                source_metadata: SourceMetadataStyle::Ecs,
                type_config: InputTypeConfig::Udp(ffwd_config::UdpTypeConfig {
                    listen: "127.0.0.1:0".to_string(),
                    max_message_size_bytes: None,
                    so_rcvbuf: None,
                }),
            }],
            transform: None,
            outputs: vec![minimal_output()],
            enrichment: Vec::new(),
            resource_attrs: Default::default(),
            workers: None,
            batch_target_bytes: None,
            batch_timeout_ms: None,
            poll_interval_ms: None,
        };

        let err = Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
            .err()
            .expect("public source path style should require path-capable input");
        assert!(
            err.contains("does not expose public source path snapshots"),
            "unexpected error: {err}"
        );

        config.inputs[0].source_metadata = SourceMetadataStyle::Fastforward;
        Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
            .expect("fastforward source id style does not require source paths");
    }

    #[test]
    fn public_source_path_style_allows_s3_key_snapshots() {
        let config = PipelineConfig {
            inputs: vec![InputConfig {
                name: Some("s3-in".to_string()),
                format: Some(Format::Json),
                sql: None,
                source_metadata: SourceMetadataStyle::Ecs,
                type_config: InputTypeConfig::S3(ffwd_config::S3TypeConfig {
                    s3: ffwd_config::S3InputConfig {
                        bucket: "logs".to_string(),
                        region: None,
                        endpoint: None,
                        prefix: None,
                        sqs_queue_url: None,
                        start_after: None,
                        access_key_id: Some("test-key".to_string()),
                        secret_access_key: Some("test-secret".to_string()),
                        session_token: None,
                        part_size_bytes: None,
                        max_concurrent_fetches: None,
                        max_concurrent_objects: None,
                        visibility_timeout_secs: None,
                        compression: Some(ffwd_config::S3CompressionConfig::Gzip),
                        poll_interval_ms: None,
                    },
                }),
            }],
            transform: None,
            outputs: vec![minimal_output()],
            enrichment: Vec::new(),
            resource_attrs: Default::default(),
            workers: None,
            batch_target_bytes: None,
            batch_timeout_ms: None,
            poll_interval_ms: None,
        };

        let result =
            Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None);
        #[cfg(feature = "s3")]
        result.expect("S3 public source path style should build when S3 is enabled");
        #[cfg(not(feature = "s3"))]
        {
            let err = result
                .err()
                .expect("S3 public source path style should reach S3 build validation");
            assert!(
                err.contains("S3 input requires the 's3' feature"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn fastforward_source_metadata_attaches_source_id_for_select_star() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"msg\":\"hello\"}\n").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.inputs[0].source_metadata = SourceMetadataStyle::Fastforward;

        let pipeline =
            Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
                .expect("SELECT * should build without source metadata columns");

        assert_eq!(
            pipeline.input_transforms[0].source_metadata_plan,
            SourceMetadataPlan {
                has_source_id: true,
                source_path: SourcePathColumn::None,
            }
        );
    }

    #[test]
    fn source_metadata_disabled_keeps_select_star_narrow() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"{\"msg\":\"hello\"}\n").unwrap();

        let config = minimal_config(log_path.display().to_string());
        let pipeline =
            Pipeline::from_config("default", &config, &ffwd_test_utils::test_meter(), None)
                .expect("SELECT * should build without source metadata");

        assert_eq!(
            pipeline.input_transforms[0].source_metadata_plan,
            SourceMetadataPlan::default()
        );
    }
}
