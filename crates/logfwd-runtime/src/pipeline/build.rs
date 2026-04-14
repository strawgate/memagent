use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::Meter;

#[cfg(feature = "datafusion")]
use logfwd_config::{EnrichmentConfig, GeoDatabaseFormat};
use logfwd_config::{Format, InputTypeConfig, PipelineConfig};
use logfwd_diagnostics::diagnostics::PipelineMetrics;
use logfwd_io::checkpoint::{
    CheckpointStore, FileCheckpointStore, SourceCheckpoint, default_data_dir,
};
use logfwd_output::{AsyncFanoutFactory, SinkFactory, build_sink_factory};
use logfwd_types::field_names;
use logfwd_types::pipeline::{PipelineMachine, SourceId};

use super::input_build::build_input_state;
use super::{InputTransform, Pipeline};

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
        if config.batch_timeout_ms == Some(0) {
            return Err("batch_timeout_ms must be > 0".to_string());
        }
        if config.poll_interval_ms == Some(0) {
            return Err("poll_interval_ms must be > 0".to_string());
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

                        if let Some(interval_secs) = geo_cfg.refresh_interval {
                            let reloadable = Arc::new(
                                crate::transform::enrichment::ReloadableGeoDb::new(initial_db),
                            );
                            let reload_handle = reloadable.reload_handle();
                            let reload_path = path.clone();
                            let reload_format = geo_cfg.format.clone();

                            tokio::spawn(async move {
                                let mut ticker = tokio::time::interval(Duration::from_secs(
                                    interval_secs.max(1),
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
                                            _ => Err(format!("unsupported geo database format for reload: {:?}", fmt)),
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
                    EnrichmentConfig::HostInfo(_) => {
                        let table = Arc::new(crate::transform::enrichment::HostInfoTable::new());
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
                        if let Some(interval_secs) = cfg.refresh_interval {
                            let t = Arc::clone(&table);
                            let name = cfg.table_name.clone();
                            tokio::spawn(async move {
                                let mut ticker = tokio::time::interval(Duration::from_secs(
                                    interval_secs.max(1),
                                ));
                                ticker.tick().await;
                                loop {
                                    ticker.tick().await;
                                    let t2 = Arc::clone(&t);
                                    match tokio::task::spawn_blocking(move || t2.reload()).await {
                                        Ok(Ok(n)) => tracing::debug!(
                                            table = %name, rows = n,
                                            "CSV enrichment table reloaded"
                                        ),
                                        Ok(Err(e)) => tracing::warn!(
                                            table = %name, error = %e,
                                            "CSV enrichment table reload failed"
                                        ),
                                        Err(e) => tracing::warn!(
                                            table = %name, error = %e,
                                            "CSV enrichment table reload task panicked"
                                        ),
                                    }
                                }
                            });
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
                        if let Some(interval_secs) = cfg.refresh_interval {
                            let t = Arc::clone(&table);
                            let name = cfg.table_name.clone();
                            tokio::spawn(async move {
                                let mut ticker = tokio::time::interval(Duration::from_secs(
                                    interval_secs.max(1),
                                ));
                                ticker.tick().await;
                                loop {
                                    ticker.tick().await;
                                    let t2 = Arc::clone(&t);
                                    match tokio::task::spawn_blocking(move || t2.reload()).await {
                                        Ok(Ok(n)) => tracing::debug!(
                                            table = %name, rows = n,
                                            "JSONL enrichment table reloaded"
                                        ),
                                        Ok(Err(e)) => tracing::warn!(
                                            table = %name, error = %e,
                                            "JSONL enrichment table reload failed"
                                        ),
                                        Err(e) => tracing::warn!(
                                            table = %name, error = %e,
                                            "JSONL enrichment table reload task panicked"
                                        ),
                                    }
                                }
                            });
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
                        if let Some(interval_secs) = cfg.refresh_interval {
                            let t = Arc::clone(&table);
                            let name = cfg.table_name.clone();
                            tokio::spawn(async move {
                                let mut ticker = tokio::time::interval(Duration::from_secs(
                                    interval_secs.max(1),
                                ));
                                ticker.tick().await;
                                loop {
                                    ticker.tick().await;
                                    let t2 = Arc::clone(&t);
                                    match tokio::task::spawn_blocking(move || t2.reload()).await {
                                        Ok(Ok(n)) => tracing::debug!(
                                            table = %name, columns = n,
                                            "KV file enrichment table reloaded"
                                        ),
                                        Ok(Err(e)) => tracing::warn!(
                                            table = %name, error = %e,
                                            "KV file enrichment table reload failed"
                                        ),
                                        Err(e) => tracing::warn!(
                                            table = %name, error = %e,
                                            "KV file enrichment table reload task panicked"
                                        ),
                                    }
                                }
                            });
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
                }
            }

            (enrichment_tables, geo_database)
        };

        #[cfg(not(feature = "datafusion"))]
        if !config.enrichment.is_empty() {
            return Err(
                "pipeline enrichment requires DataFusion. Build default/full logfwd \
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

        // Build per-input InputTransform and InputState.
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
                        .map_err(|e| format!("input '{}': enrichment error: {e}", input_name))?;
                }
            }

            let mut scan_config = transform.scan_config();
            // Raw format sends plain text directly to the scanner, so capture
            // the original line in the canonical body field for downstream SQL
            // and sinks. Auto mode wraps plain-text fallback into JSON.
            if matches!(input_cfg.format, Some(Format::Raw)) {
                scan_config.line_field_name = Some(field_names::BODY.to_string());
            }
            let scanner = logfwd_arrow::scanner::Scanner::new(scan_config);

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

        if config.batch_timeout_ms == Some(0) {
            return Err("batch_timeout_ms must be > 0".to_string());
        }
        if config.poll_interval_ms == Some(0) {
            return Err("poll_interval_ms must be > 0".to_string());
        }

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
                .map_or(DEFAULT_BATCH_TIMEOUT, Duration::from_millis),
            poll_interval: config
                .poll_interval_ms
                .map_or(DEFAULT_POLL_INTERVAL, Duration::from_millis),
            resource_attrs: Arc::new(resource_attrs),
            machine: Some(PipelineMachine::new().start()),
            checkpoint_store,
            held_tickets: Vec::new(),
            last_checkpoint_flush: tokio::time::Instant::now(),
            checkpoint_flush_interval: DEFAULT_CHECKPOINT_FLUSH_INTERVAL,
            pool_drain_timeout: DEFAULT_POOL_DRAIN_TIMEOUT,
        })
    }
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
    use logfwd_config::{InputConfig, InputTypeConfig, OutputConfig, OutputType};

    fn minimal_input(path: String) -> InputConfig {
        InputConfig {
            name: Some("input".to_string()),
            format: Some(Format::Json),
            sql: None,
            type_config: InputTypeConfig::File(logfwd_config::FileTypeConfig {
                path,
                poll_interval_ms: None,
                read_buf_size: None,
                per_file_read_budget_bytes: None,
                adaptive_fast_polls_max: None,
                max_open_files: None,
                glob_rescan_interval_ms: None,
            }),
        }
    }

    fn minimal_output() -> OutputConfig {
        OutputConfig {
            output_type: OutputType::Stdout,
            ..Default::default()
        }
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
            &logfwd_test_utils::test_meter(),
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

    #[test]
    fn from_config_rejects_zero_batch_and_poll_timeouts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log_path = dir.path().join("in.log");
        std::fs::write(&log_path, b"{\"level\":\"INFO\"}\n").expect("write input");
        let cfg = PipelineConfig {
            inputs: vec![minimal_input(log_path.to_string_lossy().into_owned())],
            transform: None,
            outputs: vec![minimal_output()],
            enrichment: Vec::new(),
            resource_attrs: Default::default(),
            workers: None,
            batch_target_bytes: None,
            batch_timeout_ms: Some(0),
            poll_interval_ms: None,
        };

        let batch_err =
            match Pipeline::from_config("p", &cfg, &logfwd_test_utils::test_meter(), None) {
                Ok(_) => panic!("zero batch timeout must be rejected"),
                Err(err) => err,
            };
        assert!(
            batch_err.contains("batch_timeout_ms must be > 0"),
            "unexpected error: {batch_err}"
        );
    }

    #[test]
    fn workers_zero_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        std::fs::write(&log_path, b"").unwrap();

        let mut config = minimal_config(log_path.display().to_string());
        config.workers = Some(0);
        let err = Pipeline::from_config("default", &config, &logfwd_test_utils::test_meter(), None)
            .err()
            .unwrap();
        assert!(
            err.contains("workers must be >= 1"),
            "unexpected error: {err}"
        );
    }
}
