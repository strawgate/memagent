//! Config and pipeline validation logic used by `validate`, `dry-run`,
//! `effective-config`, and the wizard.

use ffwd_runtime::pipeline::topology::{PipelineSpec, compile_topology};
use std::sync::Arc;

use opentelemetry::metrics::MeterProvider;

use crate::cli::{CliError, bold, green, red, reset};

// ---------------------------------------------------------------------------
// Public validation entry points
// ---------------------------------------------------------------------------

/// Validate config by building all pipelines. Used by `validate` and `dry-run`.
fn validate_pipelines_inner<FReady, FError>(
    config: &ffwd_config::Config,
    base_path: Option<&std::path::Path>,
    mut on_ready: FReady,
    mut on_error: FError,
) -> Result<(), CliError>
where
    FReady: FnMut(&str),
    FError: FnMut(String),
{
    use ffwd::pipeline::Pipeline;

    // Build a no-op meter for validation (no OTel export needed).
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
    let meter = meter_provider.meter("ffwd");

    let mut errors = 0;
    for (name, pipe_cfg) in &config.pipelines {
        if let Err(err) = validate_pipeline_read_only(pipe_cfg, base_path) {
            on_error(format!("pipeline '{name}': {err}"));
            errors += 1;
            continue;
        }

        match Pipeline::from_config(name, pipe_cfg, &meter, base_path) {
            Ok(mut pipeline) => {
                // Execute a probe batch through the SQL plan to catch planning
                // errors (duplicate aliases, bad window specs) that only
                // surface on the first real batch at runtime.
                if let Err(e) = pipeline.validate_sql_plan() {
                    on_error(format!("pipeline '{name}' SQL plan: {e}"));
                    errors += 1;
                    continue;
                }

                let spec = PipelineSpec {
                    name,
                    config: pipe_cfg,
                };
                if let Err(e) = compile_topology(&spec) {
                    on_error(format!("pipeline '{name}' topology compilation: {e}"));
                    errors += 1;
                } else {
                    on_ready(name);
                }
            }
            Err(e) => {
                on_error(format!("pipeline '{name}': {e}"));
                errors += 1;
            }
        }
    }

    if errors > 0 {
        return Err(CliError::Config(format!(
            "{errors} error(s) during validation"
        )));
    }

    Ok(())
}

pub(crate) fn validate_pipelines_read_only<FReady, FError>(
    config: &ffwd_config::Config,
    base_path: Option<&std::path::Path>,
    mut on_ready: FReady,
    mut on_error: FError,
) -> Result<(), CliError>
where
    FReady: FnMut(&str),
    FError: FnMut(String),
{
    let mut errors = 0;
    for (name, pipe_cfg) in &config.pipelines {
        match validate_pipeline_read_only(pipe_cfg, base_path) {
            Ok(()) => {
                on_ready(name);
            }
            Err(err) => {
                on_error(format!("pipeline '{name}': {err}"));
                errors += 1;
            }
        }
    }

    if errors > 0 {
        return Err(CliError::Config(format!(
            "{errors} error(s) during validation"
        )));
    }

    Ok(())
}

pub(crate) fn validate_pipelines(
    config: &ffwd_config::Config,
    dry_run: bool,
    base_path: Option<&std::path::Path>,
) -> Result<(), CliError> {
    validate_pipelines_inner(
        config,
        base_path,
        |name| {
            // Success output goes to stdout so scripts can capture it.
            println!("  {}ready{}: {}{name}{}", green(), reset(), bold(), reset());
        },
        |err| {
            eprintln!("  {}error{}: {err}", red(), reset());
        },
    )?;

    let label = if dry_run { "dry run ok" } else { "config ok" };
    // Success summary goes to stdout so scripts can parse it reliably.
    println!(
        "{}{label}{}: {} pipeline(s)",
        green(),
        reset(),
        config.pipelines.len(),
    );
    Ok(())
}

pub(crate) fn validate_generated_config_read_only(
    config_yaml: &str,
    output_path: &std::path::Path,
) -> Result<(), CliError> {
    let base_path = output_path.parent();
    let config = ffwd_config::Config::load_str_with_base_path(config_yaml, base_path)
        .map_err(|e| CliError::Config(e.to_string()))?;
    let mut validation_errors = Vec::new();
    let result = validate_pipelines_read_only(
        &config,
        base_path,
        |_name| {},
        |err| validation_errors.push(err),
    );
    match result {
        Ok(()) => Ok(()),
        Err(_) if validation_errors.is_empty() => Err(CliError::Config(
            "generated config failed validation".to_owned(),
        )),
        Err(_) => Err(CliError::Config(format!(
            "generated config failed validation:\n{}",
            validation_errors.join("\n")
        ))),
    }
}

// ---------------------------------------------------------------------------
// Per-pipeline validation
// ---------------------------------------------------------------------------

fn validate_transform_probe_read_only(
    transform: &mut ffwd::transform::SqlTransform,
) -> Result<(), String> {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let scan_config = transform.scan_config();
    let fields: Vec<Field> = if scan_config.extract_all || scan_config.wanted_fields.is_empty() {
        vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
        ]
    } else {
        scan_config
            .wanted_fields
            .iter()
            .map(|field| Field::new(field.name.as_str(), DataType::Utf8, true))
            .collect()
    };

    let schema = Arc::new(Schema::new(fields.clone()));
    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|_| Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("failed to build probe batch: {e}"))?;
    transform
        .execute_blocking(batch)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

const GENERATOR_LOGS_SIMPLE_COLUMNS: &[&str] = &[
    "timestamp",
    "level",
    "message",
    "duration_ms",
    "request_id",
    "service",
    "status",
];

/// Internal columns attached from CRI sidecar metadata (`_timestamp`,
/// `_stream`) plus the plain-text fallback field (`body`).
const CRI_INTERNAL_COLUMNS: &[&str] = &["_timestamp", "_stream", "body"];

/// Describes how strictly column validation should be applied.
enum KnownColumnsMode {
    /// All columns in the input are known — any column not in the list is
    /// rejected. Used for inputs with a fixed schema (e.g. generator/logs/simple).
    Strict(&'static [&'static str]),
    /// Only internal/framework columns (typically `_`-prefixed) are known.
    /// Columns starting with `_` that are not in the list are rejected;
    /// other column names are assumed to be user-defined data fields and
    /// allowed. Used for inputs that parse dynamic JSON content (CRI, JSON).
    InternalOnly(&'static [&'static str]),
}

fn known_input_columns_read_only(
    input_cfg: &ffwd_config::InputConfig,
) -> Option<KnownColumnsMode> {
    use ffwd_config::{
        Format, GeneratorComplexityConfig, GeneratorProfileConfig, InputTypeConfig,
    };

    match &input_cfg.type_config {
        // Generator logs/simple has a stable built-in schema. Other profiles
        // or complexities are broader or user-defined, so skip strict checks.
        InputTypeConfig::Generator(generator_cfg) => {
            let is_logs_profile = generator_cfg
                .generator
                .as_ref()
                .and_then(|cfg| cfg.profile.as_ref())
                .is_none_or(|profile| matches!(profile, GeneratorProfileConfig::Logs));
            let is_simple_complexity = generator_cfg
                .generator
                .as_ref()
                .and_then(|cfg| cfg.complexity.as_ref())
                .is_none_or(|complexity| matches!(complexity, GeneratorComplexityConfig::Simple));
            if !is_logs_profile || !is_simple_complexity {
                None
            } else {
                Some(KnownColumnsMode::Strict(GENERATOR_LOGS_SIMPLE_COLUMNS))
            }
        }
        // File/stdin inputs with an explicit CRI format have known internal
        // columns attached from the CRI sidecar. JSON body keys are dynamic,
        // so we only validate `_`-prefixed names.
        InputTypeConfig::File(_) | InputTypeConfig::Stdin(_) => {
            match input_cfg.format.as_ref() {
                Some(Format::Cri) => Some(KnownColumnsMode::InternalOnly(CRI_INTERNAL_COLUMNS)),
                // JSON and raw formats have fully dynamic schemas — no
                // internal columns to validate.
                _ => None,
            }
        }
        _ => None,
    }
}

fn validate_known_columns_read_only(
    input_name: &str,
    transform: &ffwd::transform::SqlTransform,
    mode: Option<KnownColumnsMode>,
) -> Result<(), String> {
    let Some(mode) = mode else {
        return Ok(());
    };

    // SELECT * expands to whatever the engine provides — skip validation.
    let analyzer = transform.analyzer();
    if analyzer.uses_select_star {
        return Ok(());
    }

    let scan_config = transform.scan_config();
    if scan_config.extract_all {
        return Ok(());
    }

    let (known_columns, internal_only) = match &mode {
        KnownColumnsMode::Strict(cols) => (*cols, false),
        KnownColumnsMode::InternalOnly(cols) => (*cols, true),
    };

    let known: std::collections::HashSet<&str> = known_columns.iter().copied().collect();

    let mut unknown: Vec<String> = scan_config
        .wanted_fields
        .iter()
        .map(|field| field.name.as_str())
        .filter(|column| {
            // Strip table qualifier (e.g. "logs.level" -> "level").
            let bare = match column.rfind('.') {
                Some(pos) => &column[pos + 1..],
                None => column,
            };
            let lower = bare.to_lowercase();

            // In internal-only mode, allow columns that don't start with `_`
            // because they could be user-defined JSON keys.
            if internal_only && !lower.starts_with('_') {
                return false;
            }

            !known.contains(lower.as_str())
        })
        .map(str::to_owned)
        .collect();
    unknown.sort_unstable();
    unknown.dedup();

    if unknown.is_empty() {
        return Ok(());
    }

    let mut supported: Vec<&str> = known_columns.to_vec();
    supported.sort_unstable();

    if internal_only {
        Err(format!(
            "input '{input_name}': SQL references unknown internal column(s) {unknown} \
             for this input format (known internal columns: {supported}; \
             non-underscore-prefixed columns are assumed to be user-defined JSON keys)",
            unknown = unknown.join(", "),
            supported = supported.join(", "),
        ))
    } else {
        Err(format!(
            "input '{input_name}': SQL references unknown column(s) {} for this input schema (known: {})",
            unknown.join(", "),
            supported.join(", ")
        ))
    }
}

fn validate_pipeline_read_only(
    config: &ffwd_config::PipelineConfig,
    base_path: Option<&std::path::Path>,
) -> Result<(), String> {
    use ffwd::transform::SqlTransform;
    #[cfg(feature = "datafusion")]
    use ffwd_config::{EnrichmentConfig, GeoDatabaseFormat};
    use ffwd_config::{Format, InputTypeConfig};
    #[cfg(feature = "datafusion")]
    use std::path::PathBuf;

    if config.workers == Some(0) {
        return Err("workers must be >= 1".to_owned());
    }
    if config.batch_target_bytes == Some(0) {
        return Err("batch_target_bytes must be > 0".to_owned());
    }
    #[cfg(not(feature = "datafusion"))]
    let _ = base_path;

    #[cfg(feature = "datafusion")]
    let (enrichment_tables, geo_database) = {
        let mut enrichment_tables: Vec<Arc<dyn ffwd::transform::enrichment::EnrichmentTable>> =
            Vec::new();
        let mut geo_database: Option<Arc<dyn ffwd::transform::enrichment::GeoDatabase>> = None;

        for enrichment in &config.enrichment {
            match enrichment {
                EnrichmentConfig::GeoDatabase(geo_cfg) => {
                    let mut path = PathBuf::from(&geo_cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let db: Arc<dyn ffwd::transform::enrichment::GeoDatabase> =
                        match geo_cfg.format {
                            GeoDatabaseFormat::Mmdb => {
                                let mmdb =
                                    ffwd::transform::udf::geo_lookup::MmdbDatabase::open(&path)
                                        .map_err(|e| {
                                        format!(
                                            "failed to open geo database '{}': {e}",
                                            path.display()
                                        )
                                    })?;
                                Arc::new(mmdb)
                            }
                            GeoDatabaseFormat::CsvRange => {
                                let csv = ffwd::transform::udf::CsvRangeDatabase::open(&path)
                                    .map_err(|e| {
                                        format!(
                                            "failed to open CSV range geo database '{}': {e}",
                                            path.display()
                                        )
                                    })?;
                                Arc::new(csv)
                            }
                            _ => {
                                return Err(format!(
                                    "unsupported geo database format: {:?}",
                                    geo_cfg.format
                                ));
                            }
                        };
                    geo_database = Some(db);
                }
                EnrichmentConfig::Static(cfg) => {
                    let labels: Vec<(String, String)> = cfg
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let table = Arc::new(
                        ffwd::transform::enrichment::StaticTable::new(&cfg.table_name, &labels)
                            .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                    );
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::HostInfo(cfg) => {
                    let style = match cfg.style {
                        ffwd_config::HostInfoStyle::Raw => {
                            ffwd::transform::enrichment::HostInfoStyle::Raw
                        }
                        ffwd_config::HostInfoStyle::Ecs => {
                            ffwd::transform::enrichment::HostInfoStyle::Ecs
                        }
                        ffwd_config::HostInfoStyle::Otel => {
                            ffwd::transform::enrichment::HostInfoStyle::Otel
                        }
                    };
                    enrichment_tables.push(Arc::new(
                        ffwd::transform::enrichment::HostInfoTable::with_style(style),
                    ));
                }
                EnrichmentConfig::K8sPath(cfg) => {
                    enrichment_tables.push(Arc::new(
                        ffwd::transform::enrichment::K8sPathTable::new(&cfg.table_name),
                    ));
                }
                EnrichmentConfig::Csv(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(ffwd::transform::enrichment::CsvFileTable::new(
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
                    let table = Arc::new(ffwd::transform::enrichment::JsonLinesFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::EnvVars(cfg) => {
                    let table = Arc::new(
                        ffwd::transform::enrichment::EnvTable::from_prefix(
                            &cfg.table_name,
                            &cfg.prefix,
                        )
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?,
                    );
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::ProcessInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        ffwd::transform::enrichment::ProcessInfoTable::new(),
                    ));
                }
                EnrichmentConfig::KvFile(cfg) => {
                    let mut path = PathBuf::from(&cfg.path);
                    if path.is_relative()
                        && let Some(base) = base_path
                    {
                        path = base.join(path);
                    }
                    let table = Arc::new(ffwd::transform::enrichment::KvFileTable::new(
                        &cfg.table_name,
                        &path,
                    ));
                    table
                        .reload()
                        .map_err(|e| format!("enrichment '{}': {e}", cfg.table_name))?;
                    enrichment_tables.push(table);
                }
                EnrichmentConfig::NetworkInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        ffwd::transform::enrichment::NetworkInfoTable::new(),
                    ));
                }
                EnrichmentConfig::ContainerInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        ffwd::transform::enrichment::ContainerInfoTable::new(),
                    ));
                }
                EnrichmentConfig::K8sClusterInfo(_) => {
                    enrichment_tables.push(Arc::new(
                        ffwd::transform::enrichment::K8sClusterInfoTable::new(),
                    ));
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
                .to_owned(),
        );
    }

    let pipeline_sql = config.transform.as_deref().unwrap_or("SELECT * FROM logs");
    for (i, input_cfg) in config.inputs.iter().enumerate() {
        let input_name = input_cfg
            .name
            .clone()
            .unwrap_or_else(|| format!("input_{i}"));
        if matches!(
            input_cfg.type_config,
            InputTypeConfig::LinuxEbpfSensor(_)
                | InputTypeConfig::MacosEsSensor(_)
                | InputTypeConfig::WindowsEbpfSensor(_)
                | InputTypeConfig::HostMetrics(_)
                | InputTypeConfig::ArrowIpc(_)
        ) {
            if input_cfg.format.is_some() {
                return Err(format!(
                    "input '{input_name}': sensor inputs do not support 'format' (Arrow-native input)"
                ));
            }
        } else {
            let format = input_cfg
                .format
                .clone()
                .unwrap_or(match input_cfg.type_config {
                    InputTypeConfig::File(_) | InputTypeConfig::Stdin(_) => Format::Auto,
                    _ => Format::Json,
                });
            validate_input_format_read_only(&input_name, input_cfg.input_type(), &format)?;
        }

        let input_sql = input_cfg.sql.as_deref().unwrap_or(pipeline_sql);
        let mut transform = SqlTransform::new(input_sql).map_err(|e| e.to_string())?;
        let known_columns = if config.enrichment.is_empty() {
            known_input_columns_read_only(input_cfg)
        } else {
            None
        };
        validate_known_columns_read_only(&input_name, &transform, known_columns)?;
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
        validate_transform_probe_read_only(&mut transform)
            .map_err(|e| format!("input '{input_name}': {e}"))?;
    }

    Ok(())
}

pub(crate) fn validate_input_format_read_only(
    name: &str,
    input_type: ffwd_config::InputType,
    format: &ffwd_config::Format,
) -> Result<(), String> {
    use ffwd_config::{Format, InputType};

    let supported = match input_type {
        InputType::File => matches!(
            format,
            Format::Cri | Format::Auto | Format::Json | Format::Raw
        ),
        InputType::Stdin => format.is_stdin_compatible(),
        InputType::Generator | InputType::Otlp => matches!(format, Format::Json),
        InputType::Http => matches!(format, Format::Json | Format::Raw),
        InputType::Udp | InputType::Tcp => matches!(format, Format::Json | Format::Raw),
        InputType::ArrowIpc => false,
        InputType::Journald => matches!(format, Format::Json),
        other => {
            tracing::warn!(
                "validate_input_format_read_only: unhandled input type {other:?} for input {name}"
            );
            return Err(format!(
                "input '{name}': type {other:?} is not yet supported in read-only validation"
            ));
        }
    };

    if supported {
        return Ok(());
    }

    Err(format!(
        "input '{name}': format {format:?} is not supported for {input_type:?} inputs"
    ))
}

// ---------------------------------------------------------------------------
// Test helpers (cfg(test) only)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn collect_yaml_files_recursive(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_yaml_files_recursive(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "yaml") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn replace_env_placeholders_for_example_validation(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0usize;

    while let Some(rel_start) = input[cursor..].find("${") {
        let start = cursor + rel_start;
        out.push_str(&input[cursor..start]);
        let value_start = start + 2;
        if let Some(rel_end) = input[value_start..].find('}') {
            let end = value_start + rel_end;
            if end > value_start {
                out.push_str("example-env-value");
            } else {
                out.push_str("${}");
            }
            cursor = end + 1;
        } else {
            out.push_str(&input[start..]);
            cursor = input.len();
            break;
        }
    }

    if cursor < input.len() {
        out.push_str(&input[cursor..]);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_pipeline_yaml(input_body: &str, output_body: &str) -> String {
        single_pipeline_yaml_with_sections(input_body, None, output_body, None)
    }

    fn single_pipeline_yaml_with_transform(
        input_body: &str,
        transform: &str,
        output_body: &str,
    ) -> String {
        single_pipeline_yaml_with_sections(input_body, Some(transform), output_body, None)
    }

    fn single_pipeline_yaml_with_sections(
        input_body: &str,
        transform: Option<&str>,
        output_body: &str,
        pipeline_extra: Option<&str>,
    ) -> String {
        let mut yaml = String::from("pipelines:\n  default:\n    inputs:\n");
        push_block_sequence(&mut yaml, 6, input_body);
        if let Some(transform) = transform {
            yaml.push_str("    transform: |\n");
            for line in transform.lines() {
                yaml.push_str("      ");
                yaml.push_str(line);
                yaml.push('\n');
            }
        }
        yaml.push_str("    outputs:\n");
        push_block_sequence(&mut yaml, 6, output_body);
        if let Some(extra) = pipeline_extra {
            for line in extra.lines().filter(|line| !line.trim().is_empty()) {
                yaml.push_str("    ");
                yaml.push_str(line);
                yaml.push('\n');
            }
        }
        yaml
    }

    fn push_block_sequence(yaml: &mut String, indent: usize, body: &str) {
        let prefix = " ".repeat(indent);
        let rest_prefix = " ".repeat(indent + 2);
        for (idx, line) in body
            .lines()
            .filter(|line| !line.trim().is_empty())
            .enumerate()
        {
            if idx == 0 {
                yaml.push_str(&prefix);
                yaml.push_str("- ");
                yaml.push_str(line.trim_start());
                yaml.push('\n');
            } else {
                yaml.push_str(&rest_prefix);
                yaml.push_str(line);
                yaml.push('\n');
            }
        }
    }

    #[test]
    fn validate_input_format_read_only_accepts_stdin_auto() {
        validate_input_format_read_only(
            "stdin",
            ffwd_config::InputType::Stdin,
            &ffwd_config::Format::Auto,
        )
        .expect("stdin should accept auto format");
    }

    #[test]
    fn validate_pipelines_read_only_rejects_otlp_raw() {
        let yaml = single_pipeline_yaml(
            "type: otlp\nlisten: 127.0.0.1:4318\nformat: raw",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let result = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(matches!(result, Err(CliError::Config(_))));
    }

    #[test]
    fn validate_pipelines_read_only_rejects_sensor_format() {
        let yaml = single_pipeline_yaml("type: linux_ebpf_sensor\nformat: raw", "type: \"null\"");
        let err = ffwd_config::Config::load_str(&yaml)
            .expect_err("sensor format should fail config validation");
        assert!(
            err.to_string()
                .contains("sensor inputs do not support 'format'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_pipelines_read_only_accepts_arrow_native_sensor() {
        let yaml = single_pipeline_yaml(
            "type: linux_ebpf_sensor\nsensor:\n  poll_interval_ms: 1000",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        validate_pipelines_read_only(&config, None, |_name| {}, |_err| {})
            .expect("sensor input without format should validate");
    }

    #[test]
    fn generated_wizard_config_is_validated_before_write() {
        let output_path = std::path::Path::new("ffwd.generated.yaml");
        let invalid_yaml = single_pipeline_yaml_with_transform(
            "type: otlp\nlisten: 127.0.0.1:4318",
            "SELECT level AS x, msg AS x FROM logs",
            "type: \"null\"",
        );
        let err = validate_generated_config_read_only(&invalid_yaml, output_path)
            .expect_err("invalid generated config should fail validation");
        assert!(
            err.to_string()
                .contains("generated config failed validation"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn effective_config_validation_matches_runtime_sql_planning_errors() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: otlp\nlisten: 127.0.0.1:4318",
            "SELECT level AS x, msg AS x FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_err(),
            "runtime validation should reject duplicate aliases"
        );
        assert!(
            read_only.is_err(),
            "effective-config validation should reject duplicate aliases"
        );
    }

    #[test]
    fn issue_1955_dry_run_rejects_unknown_generator_logs_column() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: generator",
            "SELECT missing_column FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_err(),
            "dry-run validation should reject unknown columns"
        );
        assert!(
            read_only.is_err(),
            "read-only validation should reject unknown columns"
        );
    }

    #[test]
    fn issue_1955_dry_run_accepts_mixed_case_generator_logs_columns() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: generator",
            "SELECT Level FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_ok(),
            "dry-run validation should accept case-insensitive column references"
        );
        assert!(
            read_only.is_ok(),
            "read-only validation should accept case-insensitive column references"
        );
    }

    #[test]
    fn issue_1955_dry_run_rejects_generator_message_source_parts() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: generator",
            "SELECT method FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_err(),
            "dry-run validation should reject fields embedded only inside message"
        );
        assert!(
            read_only.is_err(),
            "read-only validation should reject fields embedded only inside message"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_strict_check_when_enrichment_tables_are_present() {
        let yaml = single_pipeline_yaml_with_sections(
            "type: generator",
            Some("SELECT labels.environment FROM logs CROSS JOIN labels"),
            "type: \"null\"",
            Some(
                "enrichment:\n  - type: static\n    table_name: labels\n    labels:\n      environment: production",
            ),
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let runtime = validate_pipelines(&config, true, None);
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            runtime.is_ok(),
            "dry-run validation should not reject enrichment-only columns"
        );
        assert!(
            read_only.is_ok(),
            "read-only validation should not reject enrichment-only columns"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_strict_check_for_complex_generator_logs() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: generator\ngenerator:\n  complexity: complex",
            "SELECT bytes_in FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "complex generator profile has additional fields and should not use simple schema checks"
        );
    }

    #[test]
    fn issue_1955_dry_run_rejects_unknown_cri_internal_column() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: file\npath: /var/log/*.log\nformat: cri",
            "SELECT _timestampp FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_err(),
            "CRI format should reject misspelled internal columns like _timestampp"
        );
    }

    #[test]
    fn issue_1955_dry_run_accepts_valid_cri_columns() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: file\npath: /var/log/*.log\nformat: cri",
            "SELECT _timestamp, _stream, body FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "CRI format should accept known internal columns: {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_accepts_cri_user_json_columns() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: file\npath: /var/log/*.log\nformat: cri",
            "SELECT level, msg, custom_field FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "CRI format should allow non-underscore-prefixed columns (user JSON keys): {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_allows_json_format_any_columns() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: file\npath: /var/log/*.log\nformat: json",
            "SELECT custom_field FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "JSON format has fully dynamic schema — should allow any columns: {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_cri_check_when_enrichment_present() {
        let yaml = single_pipeline_yaml_with_sections(
            "type: file\npath: /var/log/*.log\nformat: cri",
            Some("SELECT _unknown_col FROM logs CROSS JOIN labels"),
            "type: \"null\"",
            Some(
                "enrichment:\n  - type: static\n    table_name: labels\n    labels:\n      environment: production",
            ),
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "enrichment tables may add columns — skip strict internal column checks: {read_only:?}"
        );
    }

    #[test]
    fn issue_1955_dry_run_skips_cri_check_for_auto_format() {
        let yaml = single_pipeline_yaml_with_transform(
            "type: file\npath: /var/log/*.log",
            "SELECT _unknown_col FROM logs",
            "type: \"null\"",
        );
        let config = ffwd_config::Config::load_str(&yaml).expect("config should parse");
        let read_only = validate_pipelines_read_only(&config, None, |_name| {}, |_err| {});
        assert!(
            read_only.is_ok(),
            "auto format (default for file) can't be validated statically: {read_only:?}"
        );
    }

    #[test]
    fn replace_env_placeholders_rewrites_all_braced_vars() {
        let input = "a: ${FOO}\nb: ${BAR_BAZ}\nc: ${}\nd: ${UNTERMINATED";
        let actual = replace_env_placeholders_for_example_validation(input);
        assert_eq!(
            actual,
            "a: example-env-value\nb: example-env-value\nc: ${}\nd: ${UNTERMINATED"
        );
    }

    #[test]
    fn all_yaml_examples_parse_and_validate_read_only() {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut yaml_files = Vec::new();

        collect_yaml_files_recursive(&repo_root.join("examples"), &mut yaml_files)
            .expect("should collect yaml files under examples/");
        collect_yaml_files_recursive(&repo_root.join("bench/scenarios"), &mut yaml_files)
            .expect("should collect yaml files under bench/scenarios/");
        yaml_files.sort();

        assert!(
            !yaml_files.is_empty(),
            "expected at least one yaml example in examples/ or bench/scenarios/"
        );

        let mut failures = Vec::new();
        for path in yaml_files {
            let raw = match std::fs::read_to_string(&path) {
                Ok(value) => value,
                Err(err) => {
                    failures.push(format!("{}: failed to read file: {err}", path.display()));
                    continue;
                }
            };

            // Keep example validation hermetic: inline secrets placeholders with dummy values.
            let raw = replace_env_placeholders_for_example_validation(&raw);

            let config = match ffwd_config::Config::load_str(&raw) {
                Ok(cfg) => cfg,
                Err(err) => {
                    failures.push(format!(
                        "{}: failed to parse config yaml: {err}",
                        path.display()
                    ));
                    continue;
                }
            };

            if let Err(err) =
                validate_pipelines_read_only(&config, path.parent(), |_name| {}, |_err| {})
            {
                failures.push(format!(
                    "{}: failed read-only config validation: {err}",
                    path.display()
                ));
            }
        }

        assert!(
            failures.is_empty(),
            "yaml examples must parse and validate read-only:\n{}",
            failures.join("\n")
        );
    }
}
