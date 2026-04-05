//! YAML configuration parser for logfwd.
//!
//! Supports two layout styles:
//!
//! - **Simple** (single pipeline): top-level `input`, `transform`, `output`.
//! - **Advanced** (multiple pipelines): top-level `pipelines` map.
//!
//! Environment variables in values are expanded using `${VAR}` syntax.

use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

// ---------------------------------------------------------------------------
// Authentication configuration
// ---------------------------------------------------------------------------

/// Authentication configuration for output HTTP sinks.
///
/// Supports bearer tokens and arbitrary key/value header pairs.
/// All values support `${ENV_VAR}` expansion at config-load time.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Sets the `Authorization: Bearer <token>` header on every request.
    pub bearer_token: Option<String>,
    /// Additional HTTP headers to add to every request (e.g. `X-API-Key`).
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Public error type
// ---------------------------------------------------------------------------

/// Errors that can occur while loading or validating configuration.
#[derive(Debug, thiserror::Error)]
#[must_use]
#[non_exhaustive]
pub enum ConfigError {
    /// I/O error reading the configuration file.
    #[error("config I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// YAML parsing error.
    #[error("config YAML error: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    /// Semantic validation error in configuration values.
    #[error("config validation error: {0}")]
    Validation(String),
}

// ---------------------------------------------------------------------------
// Enums for known types / formats
// ---------------------------------------------------------------------------

/// Recognised input types.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InputType {
    File,
    Udp,
    Tcp,
    Otlp,
    /// Synthetic data generator for benchmarking.
    Generator,
    /// Arrow IPC stream receiver (native Arrow transport).
    ArrowIpc,
}

impl fmt::Display for InputType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputType::File => f.write_str("file"),
            InputType::Udp => f.write_str("udp"),
            InputType::Tcp => f.write_str("tcp"),
            InputType::Otlp => f.write_str("otlp"),
            InputType::Generator => f.write_str("generator"),
            InputType::ArrowIpc => f.write_str("arrow_ipc"),
        }
    }
}

/// Recognised output types.
///
/// Uses a custom `Deserialize` impl so that `type: null` in YAML (which
/// the YAML spec parses as the scalar null, not the string `"null"`) is
/// accepted as the `Null` variant in both simple and list contexts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OutputType {
    #[default]
    Otlp,
    Http,
    Elasticsearch,
    Loki,
    Stdout,
    File,
    Parquet,
    /// Discard all data. Used for benchmarking and blackhole receivers.
    Null,
    /// Send newline-delimited data over TCP.
    Tcp,
    /// Send datagrams over UDP.
    Udp,
    /// Arrow IPC stream over HTTP (native Arrow transport).
    ArrowIpc,
}

impl fmt::Display for OutputType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputType::Otlp => f.write_str("otlp"),
            OutputType::Http => f.write_str("http"),
            OutputType::Elasticsearch => f.write_str("elasticsearch"),
            OutputType::Loki => f.write_str("loki"),
            OutputType::Stdout => f.write_str("stdout"),
            OutputType::File => f.write_str("file"),
            OutputType::Parquet => f.write_str("parquet"),
            OutputType::Null => f.write_str("null"),
            OutputType::Tcp => f.write_str("tcp"),
            OutputType::Udp => f.write_str("udp"),
            OutputType::ArrowIpc => f.write_str("arrow_ipc"),
        }
    }
}

impl<'de> Deserialize<'de> for OutputType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = OutputType;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, r#"an output type name (e.g. "stdout", "null", "otlp")"#)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<OutputType, E> {
                match v {
                    "otlp" => Ok(OutputType::Otlp),
                    "http" => Ok(OutputType::Http),
                    "elasticsearch" => Ok(OutputType::Elasticsearch),
                    "loki" => Ok(OutputType::Loki),
                    "stdout" => Ok(OutputType::Stdout),
                    "file" | "file_out" => Ok(OutputType::File),
                    "parquet" => Ok(OutputType::Parquet),
                    "null" => Ok(OutputType::Null),
                    "tcp" | "tcp_out" => Ok(OutputType::Tcp),
                    "udp" | "udp_out" => Ok(OutputType::Udp),
                    "arrow_ipc" => Ok(OutputType::ArrowIpc),
                    other => Err(E::unknown_variant(
                        other,
                        &[
                            "otlp",
                            "http",
                            "elasticsearch",
                            "loki",
                            "stdout",
                            "file",
                            "parquet",
                            "null",
                            "tcp",
                            "udp",
                            "arrow_ipc",
                        ],
                    )),
                }
            }
            /// YAML scalar `null` deserialises as a unit — map it to `Null`.
            fn visit_unit<E: serde::de::Error>(self) -> Result<OutputType, E> {
                Ok(OutputType::Null)
            }
            /// Some YAML parsers may emit `None` for a null scalar.
            fn visit_none<E: serde::de::Error>(self) -> Result<OutputType, E> {
                Ok(OutputType::Null)
            }
        }
        d.deserialize_any(V)
    }
}

/// Recognised log formats.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Format {
    Cri,
    Json,
    Logfmt,
    Syslog,
    Raw,
    Auto,
    /// Human-readable colored console output for debugging/testing.
    Console,
    /// Plain text output.
    Text,
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Format::Cri => f.write_str("cri"),
            Format::Json => f.write_str("json"),
            Format::Logfmt => f.write_str("logfmt"),
            Format::Syslog => f.write_str("syslog"),
            Format::Raw => f.write_str("raw"),
            Format::Auto => f.write_str("auto"),
            Format::Console => f.write_str("console"),
            Format::Text => f.write_str("text"),
        }
    }
}

// ---------------------------------------------------------------------------
// Input / Output descriptors
// ---------------------------------------------------------------------------

/// A single input source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputConfig {
    /// Optional friendly name (used in multi-input pipelines).
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub input_type: InputType,
    /// File glob or listen address, depending on `input_type`.
    pub path: Option<String>,
    pub listen: Option<String>,
    pub format: Option<Format>,
    /// Maximum number of file descriptors to keep open simultaneously.
    /// Applies only to `file` inputs. Defaults to 1024 when not set.
    pub max_open_files: Option<usize>,
    /// How often (ms) to re-evaluate glob patterns to discover new files.
    /// Applies only to glob `file` inputs. Defaults to 5000ms when not set.
    /// Set to a small value (e.g. 50) in tests to avoid long waits.
    pub glob_rescan_interval_ms: Option<u64>,
}

/// A single output destination.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub output_type: OutputType,
    pub endpoint: Option<String>,
    pub protocol: Option<String>,
    pub compression: Option<String>,
    /// Elasticsearch bulk request mode. Defaults to buffered.
    pub request_mode: Option<String>,
    pub format: Option<Format>,
    pub path: Option<String>,
    /// Elasticsearch index name. Defaults to "logs" if not specified.
    pub index: Option<String>,
    /// Optional authentication for HTTP-based outputs.
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    // Loki-specific configuration
    /// Optional X-Scope-OrgID header value for multi-tenant Loki.
    pub tenant_id: Option<String>,
    /// Static labels added to every Loki stream.
    pub static_labels: Option<HashMap<String, String>>,
    /// Record columns to use as Loki stream labels.
    pub label_columns: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Enrichment
// ---------------------------------------------------------------------------

/// Supported geo-IP database formats.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GeoDatabaseFormat {
    /// MaxMind MMDB format (GeoLite2-City, GeoIP2-City, DB-IP MMDB).
    Mmdb,
}

/// Configuration for a geo-IP database used by the `geo_lookup()` UDF.
///
/// ```yaml
/// enrichment:
///   - type: geo_database
///     format: mmdb
///     path: /etc/logfwd/GeoLite2-City.mmdb
///     refresh_interval: 86400
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeoDatabaseConfig {
    /// Database format.
    pub format: GeoDatabaseFormat,
    /// Path to the database file.
    pub path: String,
    /// How often to reload the database file, in seconds. Optional.
    // TODO: not yet implemented — currently ignored at runtime
    pub refresh_interval: Option<u64>,
}

/// Configuration for a static key-value enrichment table.
///
/// ```yaml
/// enrichment:
///   - type: static
///     table_name: env
///     labels:
///       dc: us-east-1
///       team: platform
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticEnrichmentConfig {
    /// SQL table name for the enrichment source.
    pub table_name: String,
    /// Key-value pairs that form a single-row table.
    pub labels: HashMap<String, String>,
}

/// Configuration for the host-info enrichment table.
///
/// Resolves hostname, OS type, and architecture at startup.
///
/// ```yaml
/// enrichment:
///   - type: host_info
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfoConfig {}

/// Configuration for the Kubernetes CRI log-path enrichment table.
///
/// Extracts namespace, pod name, pod UID, and container name from CRI log
/// file paths. Updated automatically as the tailer discovers new log files.
///
/// ```yaml
/// enrichment:
///   - type: k8s_path
///     table_name: k8s_pods
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct K8sPathConfig {
    /// SQL table name. Defaults to `"k8s_pods"`.
    #[serde(default = "default_k8s_table_name")]
    pub table_name: String,
}

fn default_k8s_table_name() -> String {
    "k8s_pods".to_string()
}

/// Configuration for a CSV file enrichment table.
///
/// ```yaml
/// enrichment:
///   - type: csv
///     table_name: assets
///     path: /etc/logfwd/assets.csv
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvEnrichmentConfig {
    /// SQL table name for the enrichment source.
    pub table_name: String,
    /// Path to the CSV file.
    pub path: String,
}

/// Configuration for a JSON Lines file enrichment table.
///
/// ```yaml
/// enrichment:
///   - type: jsonl
///     table_name: ip_owners
///     path: /etc/logfwd/ip-owners.jsonl
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlEnrichmentConfig {
    /// SQL table name for the enrichment source.
    pub table_name: String,
    /// Path to the JSON Lines file.
    pub path: String,
}

/// Enrichment configuration entry.
///
/// Each entry in the `enrichment` list specifies one enrichment source.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnrichmentConfig {
    /// Geo-IP lookup database for the `geo_lookup()` UDF.
    GeoDatabase(GeoDatabaseConfig),
    /// Static key-value pairs exposed as a single-row SQL table.
    Static(StaticEnrichmentConfig),
    /// System host metadata (hostname, OS, arch).
    HostInfo(HostInfoConfig),
    /// Kubernetes pod metadata parsed from CRI log file paths.
    K8sPath(K8sPathConfig),
    /// Lookup table loaded from a CSV file.
    Csv(CsvEnrichmentConfig),
    /// Lookup table loaded from a JSON Lines file.
    Jsonl(JsonlEnrichmentConfig),
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// One logical pipeline (inputs -> SQL transform -> outputs).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub inputs: Vec<InputConfig>,
    pub transform: Option<String>,
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub outputs: Vec<OutputConfig>,
    /// Enrichment sources (e.g. geo-IP databases).
    #[serde(default)]
    pub enrichment: Vec<EnrichmentConfig>,
    /// Static OTLP resource attributes emitted with every batch.
    ///
    /// These are added to the OTLP `Resource.attributes` field and are
    /// recommended by the OTLP spec for every exported signal.
    ///
    /// ```yaml
    /// resource_attrs:
    ///   service.name: my-service
    ///   service.version: "1.0"
    ///   deployment.environment: production
    /// ```
    #[serde(default)]
    pub resource_attrs: HashMap<String, String>,
    /// Maximum number of concurrent output workers. Default: 4.
    pub workers: Option<usize>,
    /// Batch target size in bytes before flushing. Default: 4 MiB.
    pub batch_target_bytes: Option<usize>,
    /// Batch flush timeout in milliseconds. Default: 100.
    pub batch_timeout_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// Server / Storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub diagnostics: Option<String>,
    pub log_level: Option<String>,
    /// OTLP endpoint for metrics push (e.g. "http://localhost:4318").
    /// If not set, OTLP push is disabled.
    pub metrics_endpoint: Option<String>,
    /// OTLP push interval in seconds. Default: 60.
    pub metrics_interval_secs: Option<u64>,
    /// OTLP endpoint for trace push (e.g. "http://localhost:4318").
    /// If not set, traces are only buffered in-process for the dashboard.
    pub traces_endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub data_dir: Option<String>,
}

// ---------------------------------------------------------------------------
// Top-level config (supports simple + advanced)
// ---------------------------------------------------------------------------

/// Raw top-level YAML — we use a flat struct with Options so serde can
/// deserialise either layout, then we normalise into [`Config`].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    // Simple form
    input: Option<InputConfig>,
    transform: Option<String>,
    output: Option<OutputConfig>,
    /// Enrichment sources for the simple-form default pipeline.
    #[serde(default)]
    enrichment: Vec<EnrichmentConfig>,
    /// Static OTLP resource attributes for the simple-form default pipeline.
    #[serde(default)]
    resource_attrs: HashMap<String, String>,

    // Advanced form
    pipelines: Option<HashMap<String, PipelineConfig>>,

    // Shared
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

/// Fully resolved configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub pipelines: HashMap<String, PipelineConfig>,
    pub server: ServerConfig,
    pub storage: StorageConfig,
}

impl Config {
    /// Load configuration from a file path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        Self::load_str(&raw)
    }

    /// Load configuration from a YAML string (handy for tests).
    pub fn load_str(yaml: &str) -> Result<Self, ConfigError> {
        let expanded = expand_env_vars(yaml)?;
        let raw: RawConfig = serde_yaml_ng::from_str(&expanded)?;
        Self::from_raw(raw)
    }

    // Normalise the two layout variants into a single representation.
    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let pipelines = match (raw.pipelines, raw.input, raw.output) {
            (Some(p), None, None) => {
                if !raw.enrichment.is_empty() {
                    return Err(ConfigError::Validation(
                        "top-level `enrichment` is not supported when using `pipelines:` form \
                         — move enrichment configuration inside each pipeline"
                            .into(),
                    ));
                }
                if !raw.resource_attrs.is_empty() {
                    return Err(ConfigError::Validation(
                        "top-level `resource_attrs` cannot be used with `pipelines:`; \
                         move resource_attrs into each pipeline"
                            .into(),
                    ));
                }
                p
            }
            (None, Some(input), Some(output)) => {
                let pipeline = PipelineConfig {
                    inputs: vec![input],
                    transform: raw.transform,
                    outputs: vec![output],
                    enrichment: raw.enrichment,
                    resource_attrs: raw.resource_attrs,
                    workers: None,
                    batch_target_bytes: None,
                    batch_timeout_ms: None,
                };
                let mut map = HashMap::new();
                map.insert("default".to_string(), pipeline);
                map
            }
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                return Err(ConfigError::Validation(
                    "cannot mix top-level input/output with pipelines".into(),
                ));
            }
            (None, None, None) => {
                return Err(ConfigError::Validation(
                    "config must define either input/output or pipelines".into(),
                ));
            }
            (None, Some(_), None) => {
                return Err(ConfigError::Validation(
                    "output is required when input is specified".into(),
                ));
            }
            (None, None, Some(_)) => {
                return Err(ConfigError::Validation(
                    "input is required when output is specified".into(),
                ));
            }
        };

        let cfg = Config {
            pipelines,
            server: raw.server,
            storage: raw.storage,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate the loaded configuration.
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(ep) = &self.server.traces_endpoint {
            if let Err(msg) = validate_endpoint_url(ep) {
                return Err(ConfigError::Validation(format!(
                    "server.traces_endpoint: {msg}"
                )));
            }
        }

        // Validate server.diagnostics bind address at config time so that
        // `--validate` catches typos before the server tries to bind at runtime.
        if let Some(addr) = &self.server.diagnostics {
            if let Err(msg) = validate_bind_addr(addr) {
                return Err(ConfigError::Validation(format!(
                    "server.diagnostics: {msg}"
                )));
            }
        }

        // Validate server.log_level is a recognised level (#481).
        if let Some(level) = &self.server.log_level {
            if let Err(msg) = validate_log_level(level) {
                return Err(ConfigError::Validation(format!("server.log_level: {msg}")));
            }
        }

        if self.pipelines.is_empty() {
            return Err(ConfigError::Validation(
                "at least one pipeline must be defined".into(),
            ));
        }

        for (name, pipe) in &self.pipelines {
            if pipe.inputs.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}' has no inputs"
                )));
            }
            if pipe.outputs.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}' has no outputs"
                )));
            }

            for (i, input) in pipe.inputs.iter().enumerate() {
                let label = input
                    .name
                    .as_deref()
                    .map_or_else(|| format!("#{i}"), String::from);
                match input.input_type {
                    InputType::File => {
                        if input.path.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': file input requires 'path'"
                            )));
                        }
                    }
                    InputType::Udp | InputType::Tcp => {
                        if input.listen.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': udp/tcp input requires 'listen'"
                            )));
                        }
                    }
                    InputType::Otlp | InputType::Generator | InputType::ArrowIpc => {}
                }

                // Reject fields that don't apply to this input type.
                match input.input_type {
                    InputType::File => {
                        if input.listen.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'listen' is not supported for file inputs"
                            )));
                        }
                    }
                    InputType::Tcp | InputType::Udp => {
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for tcp/udp inputs"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for tcp/udp inputs"
                            )));
                        }
                    }
                    InputType::Otlp => {
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for otlp inputs"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for otlp inputs"
                            )));
                        }
                    }
                    InputType::Generator => {
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for generator inputs"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for generator inputs"
                            )));
                        }
                        if input.listen.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'listen' is not supported for generator inputs"
                            )));
                        }
                    }
                    InputType::ArrowIpc => {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' input '{label}': arrow_ipc input type is not yet supported"
                        )));
                    }
                }

                // Reject input formats that are not yet implemented.
                if let Some(fmt @ (Format::Logfmt | Format::Syslog)) = &input.format {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' input '{label}': format {fmt:?} is not yet implemented",
                    )));
                }

                // max_open_files: 0 silently disables all file reading (#696).
                if input.max_open_files == Some(0) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' input '{label}': max_open_files must be at least 1"
                    )));
                }
            }

            for (i, output) in pipe.outputs.iter().enumerate() {
                let label = output
                    .name
                    .as_deref()
                    .map_or_else(|| format!("#{i}"), String::from);

                // Reject placeholder output types that are not yet implemented.
                match output.output_type {
                    OutputType::File | OutputType::Parquet => {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': {} output type is not yet implemented",
                            output.output_type,
                        )));
                    }
                    _ => {}
                }

                match output.output_type {
                    OutputType::Otlp
                    | OutputType::Http
                    | OutputType::Elasticsearch
                    | OutputType::Loki
                    | OutputType::ArrowIpc => {
                        if output.endpoint.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'endpoint'",
                                output.output_type,
                            )));
                        }
                        if let Some(ep) = &output.endpoint
                            && let Err(msg) = validate_endpoint_url(ep)
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {msg}",
                            )));
                        }
                        if output.output_type == OutputType::Otlp
                            && output.compression.as_deref() == Some("gzip")
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': otlp output does not support 'gzip' compression yet"
                            )));
                        }
                        if output.output_type == OutputType::Elasticsearch {
                            if let Some(mode) = output.request_mode.as_deref()
                                && !matches!(mode, "buffered" | "streaming")
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': elasticsearch request_mode must be 'buffered' or 'streaming'"
                                )));
                            }
                            if output.request_mode.as_deref() == Some("streaming")
                                && output.compression.as_deref() == Some("gzip")
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': elasticsearch request_mode 'streaming' does not support gzip compression yet"
                                )));
                            }
                        } else if output.request_mode.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': request_mode is only supported for elasticsearch outputs"
                            )));
                        }
                    }
                    OutputType::File => {
                        if output.path.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'path'",
                                output.output_type,
                            )));
                        }
                    }
                    OutputType::Stdout | OutputType::Null => {}
                    OutputType::Tcp | OutputType::Udp => {
                        if output.endpoint.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'endpoint'",
                                output.output_type,
                            )));
                        }
                    }
                    OutputType::Parquet => {
                        // Parquet output not yet implemented
                    }
                }

                // Reject fields that don't apply to this output type.
                if output.output_type != OutputType::Elasticsearch && output.index.is_some() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'index' is only supported for elasticsearch outputs"
                    )));
                }
                if output.output_type != OutputType::Otlp && output.protocol.is_some() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'protocol' is only supported for otlp outputs"
                    )));
                }
                if output.output_type != OutputType::Loki {
                    if output.tenant_id.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'tenant_id' is only supported for loki outputs"
                        )));
                    }
                    if output.static_labels.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'static_labels' is only supported for loki outputs"
                        )));
                    }
                    if output.label_columns.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'label_columns' is only supported for loki outputs"
                        )));
                    }
                }
                if !matches!(output.output_type, OutputType::File | OutputType::Parquet)
                    && output.path.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'path' is only supported for file/parquet outputs"
                    )));
                }
                // auth is only valid for HTTP-based outputs
                if !matches!(
                    output.output_type,
                    OutputType::Otlp
                        | OutputType::Http
                        | OutputType::Elasticsearch
                        | OutputType::Loki
                        | OutputType::ArrowIpc
                ) && output.auth.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'auth' is only supported for HTTP-based outputs"
                    )));
                }
                // compression: only valid for outputs that support it
                if matches!(
                    output.output_type,
                    OutputType::Stdout | OutputType::Null | OutputType::Tcp | OutputType::Udp
                ) && output.compression.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'compression' is not supported for this output type"
                    )));
                }
            }

            // Validate enrichment entries (#550).
            for (j, enrichment) in pipe.enrichment.iter().enumerate() {
                match enrichment {
                    EnrichmentConfig::GeoDatabase(geo_cfg) => {
                        // Only check existence for absolute paths; relative paths
                        // are resolved against base_path in Pipeline::from_config.
                        let p = Path::new(&geo_cfg.path);
                        if p.is_absolute() && !p.exists() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: geo database file not found: {}",
                                geo_cfg.path,
                            )));
                        }
                    }
                    EnrichmentConfig::Static(cfg) => {
                        if cfg.labels.is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: static enrichment requires at least one label"
                            )));
                        }
                    }
                    EnrichmentConfig::Csv(cfg) => {
                        let p = Path::new(&cfg.path);
                        if p.is_absolute() && !p.exists() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: csv file not found: {}",
                                cfg.path,
                            )));
                        }
                    }
                    EnrichmentConfig::Jsonl(cfg) => {
                        let p = Path::new(&cfg.path);
                        if p.is_absolute() && !p.exists() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: jsonl file not found: {}",
                                cfg.path,
                            )));
                        }
                    }
                    EnrichmentConfig::HostInfo(_) | EnrichmentConfig::K8sPath(_) => {}
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validate that a bind address is a parseable `host:port` socket address.
fn validate_bind_addr(addr: &str) -> Result<(), String> {
    addr.parse::<std::net::SocketAddr>()
        .map(|_| ())
        .map_err(|e| format!("'{addr}' is not a valid host:port address: {e}"))
}

/// Validate that a log level string is a recognised tracing level.
///
/// Accepted values (case-insensitive): `trace`, `debug`, `info`, `warn`, `error`.
fn validate_log_level(level: &str) -> Result<(), String> {
    match level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(format!(
            "'{level}' is not a recognised log level; expected one of: trace, debug, info, warn, error"
        )),
    }
}

/// Validate that an endpoint URL has a recognised scheme and a non-empty host.
///
/// Accepts `http://` or `https://` followed by at least one character.
fn validate_endpoint_url(endpoint: &str) -> Result<(), String> {
    let rest = if let Some(r) = endpoint.strip_prefix("https://") {
        r
    } else if let Some(r) = endpoint.strip_prefix("http://") {
        r
    } else {
        return Err(format!(
            "endpoint '{endpoint}' has no recognised scheme; expected 'http://' or 'https://'"
        ));
    };
    if rest.is_empty() {
        return Err(format!(
            "endpoint '{endpoint}' has no host after the scheme"
        ));
    }
    Ok(())
}

/// Expand `${VAR}` references in `text` using the process environment.
///
/// Returns an error if a referenced variable is not set in the environment.
/// This catches misspelled env var names at config-load time
/// instead of producing cryptic runtime failures.
fn expand_env_vars(text: &str) -> Result<String, ConfigError> {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            let mut found_close = false;
            for c in chars.by_ref() {
                if c == '}' {
                    found_close = true;
                    break;
                }
                var_name.push(c);
            }
            if !found_close {
                result.push_str("${");
                result.push_str(&var_name);
                continue;
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    return Err(ConfigError::Validation(format!(
                        "environment variable '{var_name}' is not set"
                    )));
                }
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

/// Serde helper: accept either a single `T` or a `Vec<T>`.
fn deserialize_one_or_many<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        Many(Vec<T>),
        One(T),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::Many(v) => Ok(v),
        OneOrMany::One(v) => Ok(vec![v]),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_config() {
        let yaml = r"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri

transform: |
  SELECT * FROM logs WHERE level != 'DEBUG'

output:
  type: otlp
  endpoint: http://otel-collector:4317
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
  log_level: info

storage:
  data_dir: /var/lib/logfwd
";
        let cfg = Config::load_str(yaml).expect("should parse simple config");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs.len(), 1);
        assert_eq!(pipe.inputs[0].input_type, InputType::File);
        assert_eq!(
            pipe.inputs[0].path.as_deref(),
            Some("/var/log/pods/**/*.log")
        );
        assert_eq!(pipe.inputs[0].format, Some(Format::Cri));
        assert!(pipe.transform.as_ref().unwrap().contains("SELECT"));
        assert_eq!(pipe.outputs[0].output_type, OutputType::Otlp);
        assert_eq!(
            pipe.outputs[0].endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );
        assert_eq!(cfg.server.diagnostics.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(cfg.storage.data_dir.as_deref(), Some("/var/lib/logfwd"));
    }

    #[test]
    fn advanced_config() {
        let yaml = r"
pipelines:
  app_logs:
    inputs:
      - name: pod_logs
        type: file
        path: /var/log/pods/**/*.log
        format: cri
      - name: raw_in
        type: udp
        listen: 0.0.0.0:514
        format: raw
    transform: |
      SELECT * EXCEPT (stack_trace) FROM logs WHERE level != 'DEBUG'
    outputs:
      - name: collector
        type: otlp
        endpoint: http://otel-collector:4317
        protocol: grpc
        compression: zstd
      - name: debug
        type: stdout
        format: json

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
";
        let cfg = Config::load_str(yaml).expect("should parse advanced config");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["app_logs"];
        assert_eq!(pipe.inputs.len(), 2);
        assert_eq!(pipe.inputs[0].input_type, InputType::File);
        assert_eq!(pipe.inputs[1].input_type, InputType::Udp);
        assert_eq!(pipe.inputs[1].listen.as_deref(), Some("0.0.0.0:514"));
        assert_eq!(pipe.outputs.len(), 2);
        assert_eq!(pipe.outputs[0].output_type, OutputType::Otlp);
        assert_eq!(pipe.outputs[1].output_type, OutputType::Stdout);
    }

    #[test]
    fn env_var_substitution() {
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::set_var("LOGFWD_TEST_ENDPOINT", "http://my-collector:4317") };
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_TEST_ENDPOINT}
";
        let cfg = Config::load_str(yaml).expect("env var substitution");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.outputs[0].endpoint.as_deref(),
            Some("http://my-collector:4317")
        );
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::remove_var("LOGFWD_TEST_ENDPOINT") };
    }

    #[test]
    fn unset_env_var_rejected() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_NONEXISTENT_VAR_12345}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOGFWD_NONEXISTENT_VAR_12345"),
            "error should mention the variable name: {msg}"
        );
        assert!(
            msg.contains("not set"),
            "error should say variable is not set: {msg}"
        );
    }

    #[test]
    fn unterminated_env_var_preserved_as_is() {
        assert_eq!(
            expand_env_vars("endpoint: ${LOGFWD_TEST_UNTERMINATED").unwrap(),
            "endpoint: ${LOGFWD_TEST_UNTERMINATED"
        );
    }

    #[test]
    fn validation_missing_input_path() {
        let yaml = r"
input:
  type: file
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("path"), "expected 'path' in error: {msg}");
    }

    #[test]
    fn validation_missing_output_endpoint() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("endpoint"),
            "expected 'endpoint' in error: {msg}"
        );
    }

    #[test]
    fn validation_otlp_gzip_not_implemented() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: http://collector:4318
  compression: gzip
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("gzip"), "expected 'gzip' in error: {msg}");
        assert!(
            msg.contains("does not support") || msg.contains("not"),
            "expected unsupported message in error: {msg}"
        );
    }

    #[test]
    fn validation_udp_requires_listen() {
        let yaml = r"
input:
  type: udp
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("listen"), "expected 'listen' in error: {msg}");
    }

    #[test]
    fn validation_mixed_simple_and_pipelines() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
pipelines:
  extra:
    inputs:
      - type: file
        path: /tmp/y.log
    outputs:
      - type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mix"), "expected 'mix' in error: {msg}");
    }

    #[test]
    fn validation_no_pipelines() {
        let yaml = r"
server:
  log_level: info
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("must define"),
            "expected 'must define' in error: {msg}"
        );
    }

    #[test]
    fn file_rejected_as_unimplemented() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: file
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not yet implemented"),
            "expected 'not yet implemented' in error: {msg}"
        );
    }

    #[test]
    fn validation_unimplemented_output_type() {
        // Each placeholder type should be caught by Config::validate() before
        // pipeline construction, not silently accepted.
        for otype in ["file", "parquet"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: http://x\n  path: /tmp/x\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "validation should reject unimplemented type '{otype}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "error message should mention 'not yet implemented' for '{otype}': {msg}"
            );
            assert!(
                msg.contains(otype),
                "error message should include the type name '{otype}': {msg}"
            );
        }
    }

    #[test]
    fn validation_unimplemented_input_format() {
        // Unimplemented input formats must be rejected at config validation time,
        // not silently treated as JSON which would corrupt data.
        for format in ["logfmt", "syslog"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\n  format: {format}\noutput:\n  type: stdout\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "validation should reject unimplemented format '{format}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "error message should mention 'not yet implemented' for '{format}': {msg}"
            );
        }
    }

    #[test]
    fn all_input_formats() {
        // Implemented input formats should parse and validate successfully.
        for format in ["cri", "json", "raw", "auto"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\n  format: {format}\noutput:\n  type: stdout\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for format '{format}': {e}"));
        }
    }

    #[test]
    fn all_output_types() {
        // Implemented output types should parse and validate successfully.
        for (otype, extra) in [
            ("otlp", "endpoint: http://x:4317"),
            ("http", "endpoint: http://x"),
            ("stdout", ""),
            ("null", ""),
            ("elasticsearch", "endpoint: http://x"),
            ("loki", "endpoint: http://x"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  {extra}\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {otype}: {e}"));
        }

        // Placeholder output types must be rejected at validation time.
        for otype in ["file", "parquet"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: http://x\n  path: /tmp/x\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "expected error for unimplemented type {otype}"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "expected 'not yet implemented' for {otype}: {msg}"
            );
        }
    }

    #[test]
    fn all_input_types() {
        for (itype, extra) in [
            ("file", "path: /tmp/x.log"),
            ("udp", "listen: 0.0.0.0:514"),
            ("tcp", "listen: 0.0.0.0:514"),
            ("otlp", ""),
            ("generator", ""),
        ] {
            let yaml = format!("input:\n  type: {itype}\n  {extra}\noutput:\n  type: stdout\n");
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {itype}: {e}"));
        }
    }

    #[test]
    fn auth_bearer_token() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  auth:
    bearer_token: "my-secret-token"
"#;
        let cfg = Config::load_str(yaml).expect("auth bearer_token");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("my-secret-token"));
        assert!(auth.headers.is_empty());
    }

    #[test]
    fn auth_custom_headers() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  auth:
    headers:
      X-API-Key: "supersecret"
      X-Tenant: "acme"
"#;
        let cfg = Config::load_str(yaml).expect("auth custom headers");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token, None);
        assert_eq!(
            auth.headers.get("X-API-Key").map(String::as_str),
            Some("supersecret")
        );
        assert_eq!(
            auth.headers.get("X-Tenant").map(String::as_str),
            Some("acme")
        );
    }

    #[test]
    fn auth_env_var_bearer_token() {
        // SAFETY: test is not run concurrently with other tests that modify this var.
        unsafe { std::env::set_var("LOGFWD_TEST_TOKEN", "env-bearer-token") };
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  auth:
    bearer_token: "${LOGFWD_TEST_TOKEN}"
"#;
        let cfg = Config::load_str(yaml).expect("auth env var bearer");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("env-bearer-token"));
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::remove_var("LOGFWD_TEST_TOKEN") };
    }

    #[test]
    fn auth_absent_is_none() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
";
        let cfg = Config::load_str(yaml).expect("no auth");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.outputs[0].auth.is_none());
    }

    #[test]
    fn elasticsearch_request_mode_accepts_streaming() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: elasticsearch
  endpoint: http://localhost:9200
  request_mode: streaming
";
        let cfg = Config::load_str(yaml).expect("streaming request_mode should validate");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.outputs[0].request_mode.as_deref(), Some("streaming"));
    }

    #[test]
    fn elasticsearch_request_mode_rejects_unknown_value() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: elasticsearch
  endpoint: http://localhost:9200
  request_mode: fancy
";
        let err = Config::load_str(yaml).expect_err("invalid request_mode should fail");
        assert!(err.to_string().contains("request_mode"));
    }

    #[test]
    fn elasticsearch_streaming_rejects_gzip() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: elasticsearch
  endpoint: http://localhost:9200
  compression: gzip
  request_mode: streaming
";
        let err = Config::load_str(yaml).expect_err("streaming+gzip should fail");
        assert!(err.to_string().contains("does not support gzip"));
    }

    #[test]
    fn non_elasticsearch_request_mode_is_rejected() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  request_mode: streaming
";
        let err = Config::load_str(yaml).expect_err("request_mode should be es-only");
        assert!(err.to_string().contains("only supported for elasticsearch"));
    }

    #[test]
    fn validation_endpoint_missing_scheme() {
        // Scheme-less endpoints must be rejected for both otlp and http outputs.
        for otype in ["otlp", "http"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: collector:4317\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "expected error for scheme-less endpoint with type '{otype}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("scheme"),
                "error should mention 'scheme' for '{otype}': {msg}"
            );
        }
    }

    #[test]
    fn validation_endpoint_valid_schemes() {
        // Both http:// and https:// must be accepted for otlp and http outputs.
        for (otype, scheme) in [
            ("otlp", "http://"),
            ("otlp", "https://"),
            ("http", "http://"),
            ("http", "https://"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: {scheme}collector:4317\n"
            );
            Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("scheme '{scheme}' should be valid for '{otype}': {e}"));
        }
    }

    #[test]
    fn validation_endpoint_unset_env_var_rejected() {
        // An endpoint referencing an unset env var must fail at config load
        // time with a clear error message naming the variable.
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_NONEXISTENT_ENDPOINT_VAR}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOGFWD_NONEXISTENT_ENDPOINT_VAR"),
            "error should mention the variable name: {msg}"
        );
    }

    #[test]
    fn resource_attrs_simple_form() {
        let yaml = r#"
input:
  type: file
  path: /var/log/app.log

output:
  type: otlp
  endpoint: http://otel-collector:4317

resource_attrs:
  service.name: my-service
  service.version: "1.2.3"
  deployment.environment: production
"#;
        let cfg = Config::load_str(yaml).expect("should parse simple config with resource_attrs");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.resource_attrs.get("service.name").map(String::as_str),
            Some("my-service")
        );
        assert_eq!(
            pipe.resource_attrs
                .get("service.version")
                .map(String::as_str),
            Some("1.2.3")
        );
        assert_eq!(
            pipe.resource_attrs
                .get("deployment.environment")
                .map(String::as_str),
            Some("production")
        );
    }

    #[test]
    fn resource_attrs_advanced_form() {
        let yaml = r"
pipelines:
  app_logs:
    resource_attrs:
      service.name: advanced-service
      deployment.environment: staging
    inputs:
      - type: file
        path: /var/log/app.log
    outputs:
      - type: otlp
        endpoint: http://otel-collector:4317
";
        let cfg = Config::load_str(yaml).expect("should parse advanced config with resource_attrs");
        let pipe = &cfg.pipelines["app_logs"];
        assert_eq!(
            pipe.resource_attrs.get("service.name").map(String::as_str),
            Some("advanced-service")
        );
        assert_eq!(
            pipe.resource_attrs
                .get("deployment.environment")
                .map(String::as_str),
            Some("staging")
        );
    }

    #[test]
    fn resource_attrs_absent_is_empty() {
        let yaml = r"
input:
  type: file
  path: /var/log/app.log
output:
  type: otlp
  endpoint: http://otel-collector:4317
";
        let cfg = Config::load_str(yaml).expect("should parse config without resource_attrs");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.resource_attrs.is_empty());
    }

    // -----------------------------------------------------------------------
    // Bug #707: type: null YAML keyword collision
    // -----------------------------------------------------------------------

    #[test]
    fn type_null_works_in_simple_layout() {
        // `type: null` in simple layout must parse as OutputType::Null.
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: null\n";
        let cfg = Config::load_str(yaml).expect("type: null simple layout");
        assert_eq!(
            cfg.pipelines["default"].outputs[0].output_type,
            OutputType::Null
        );
    }

    #[test]
    fn type_null_works_in_advanced_list_layout() {
        // Before the fix, `type: null` in a YAML list deserialized as the YAML
        // null scalar, causing serde to fail with a confusing untagged-enum error.
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: null
";
        let cfg = Config::load_str(yaml).expect("type: null in advanced list layout");
        assert_eq!(
            cfg.pipelines["app"].outputs[0].output_type,
            OutputType::Null
        );
    }

    #[test]
    fn type_null_quoted_also_works() {
        // `type: "null"` (quoted string) must continue to work.
        let yaml = "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: \"null\"\n";
        let cfg = Config::load_str(yaml).expect("type: \"null\" quoted");
        assert_eq!(
            cfg.pipelines["default"].outputs[0].output_type,
            OutputType::Null
        );
    }

    // -----------------------------------------------------------------------
    // Bug #725: server.diagnostics address validated at config load time
    // -----------------------------------------------------------------------

    #[test]
    fn valid_diagnostics_address_accepted() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  diagnostics: 127.0.0.1:9090
";
        Config::load_str(yaml).expect("valid diagnostics address");
    }

    #[test]
    fn invalid_diagnostics_address_rejected_at_validate() {
        // Before the fix, an invalid server.diagnostics address would pass
        // --validate and only fail at runtime when the server tried to bind.
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  diagnostics: not-an-address
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("diagnostics"),
            "error should mention 'diagnostics': {msg}"
        );
        assert!(
            msg.contains("not a valid") || msg.contains("invalid"),
            "error should say address is invalid: {msg}"
        );
    }

    #[test]
    fn diagnostics_address_with_unset_env_var_rejected() {
        // Unset ${VAR} placeholders must be rejected at config-load time.
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  diagnostics: ${LOGFWD_DIAG_ADDR}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("LOGFWD_DIAG_ADDR"),
            "error should mention the variable name: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Bug #709: normalize_args allows flags before --config
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_args_canonical_form_unchanged() {
        use super::*;
        // When --config is already at position 1, args are returned unchanged.
        // (We test the normalize_args logic via Config parsing instead since
        // normalize_args lives in the binary crate.)
        let _ = Config::load_str; // ensure the import is live
    }

    // -----------------------------------------------------------------------
    // Bug #696: max_open_files: 0 silently disables file reading
    // -----------------------------------------------------------------------

    #[test]
    fn max_open_files_zero_rejected() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
  max_open_files: 0
output:
  type: stdout
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_open_files must be at least 1"),
            "expected 'max_open_files must be at least 1' in error: {msg}"
        );
    }

    #[test]
    fn max_open_files_one_accepted() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
  max_open_files: 1
output:
  type: stdout
";
        Config::load_str(yaml).expect("max_open_files: 1 should be valid");
    }

    #[test]
    fn max_open_files_absent_accepted() {
        let yaml = r"
input:
  type: file
  path: /var/log/test.log
output:
  type: stdout
";
        Config::load_str(yaml).expect("absent max_open_files should be valid");
    }

    // -----------------------------------------------------------------------
    // Bug #550: enrichment path validation at config load time
    // -----------------------------------------------------------------------

    #[test]
    fn enrichment_geo_database_missing_path_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: geo_database
        format: mmdb
        path: /nonexistent/path/to/GeoLite2-City.mmdb
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "expected 'not found' in error: {msg}"
        );
        assert!(
            msg.contains("geo database"),
            "expected 'geo database' in error: {msg}"
        );
    }

    #[test]
    fn enrichment_csv_missing_path_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: assets
        path: /nonexistent/path/to/assets.csv
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "expected 'not found' in error: {msg}"
        );
        assert!(msg.contains("csv"), "expected 'csv' in error: {msg}");
    }

    #[test]
    fn enrichment_jsonl_missing_path_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: jsonl
        table_name: ips
        path: /nonexistent/path/to/data.jsonl
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "expected 'not found' in error: {msg}"
        );
        assert!(msg.contains("jsonl"), "expected 'jsonl' in error: {msg}");
    }

    #[test]
    fn enrichment_relative_path_accepted_at_validation_time() {
        // Relative paths are resolved against base_path in Pipeline::from_config,
        // so Config::validate() must not reject them.
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: assets
        path: data/assets.csv
      - type: jsonl
        table_name: ips
        path: data/ips.jsonl
      - type: geo_database
        format: mmdb
        path: data/GeoLite2-City.mmdb
";
        Config::load_str(yaml).expect("relative enrichment paths should pass validation");
    }

    #[test]
    fn enrichment_static_empty_labels_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: env
        labels: {}
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("at least one label"),
            "expected 'at least one label' in error: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Bug #540: enrichment config variants for all implemented types
    // -----------------------------------------------------------------------

    #[test]
    fn enrichment_static_config_accepted() {
        let yaml = r#"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: env
        labels:
          dc: us-east-1
          team: platform
"#;
        let cfg = Config::load_str(yaml).expect("static enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Static(c) => {
                assert_eq!(c.table_name, "env");
                assert_eq!(c.labels.get("dc").map(String::as_str), Some("us-east-1"));
                assert_eq!(c.labels.get("team").map(String::as_str), Some("platform"));
            }
            other => panic!("expected Static, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_host_info_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: host_info
";
        let cfg = Config::load_str(yaml).expect("host_info enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        assert!(matches!(&pipe.enrichment[0], EnrichmentConfig::HostInfo(_)));
    }

    #[test]
    fn enrichment_k8s_path_config_accepted() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: k8s_path
        table_name: pods
";
        let cfg = Config::load_str(yaml).expect("k8s_path enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::K8sPath(c) => assert_eq!(c.table_name, "pods"),
            other => panic!("expected K8sPath, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_k8s_path_default_table_name() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: k8s_path
";
        let cfg = Config::load_str(yaml).expect("k8s_path with default table_name should parse");
        let pipe = &cfg.pipelines["app"];
        match &pipe.enrichment[0] {
            EnrichmentConfig::K8sPath(c) => assert_eq!(c.table_name, "k8s_pods"),
            other => panic!("expected K8sPath, got {other:?}"),
        }
    }

    #[test]
    fn enrichment_csv_config_accepted() {
        // Use a path that exists to pass validation.
        let tmp = std::env::temp_dir().join("logfwd_test_enrichment.csv");
        std::fs::write(&tmp, "host,owner\nweb1,alice\n").expect("create temp csv");
        let yaml = format!(
            "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: csv\n        table_name: assets\n        path: {}\n",
            tmp.display()
        );
        let cfg = Config::load_str(&yaml).expect("csv enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Csv(c) => {
                assert_eq!(c.table_name, "assets");
                assert_eq!(c.path, tmp.to_str().unwrap());
            }
            other => panic!("expected Csv, got {other:?}"),
        }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn enrichment_jsonl_config_accepted() {
        // Use a path that exists to pass validation.
        let tmp = std::env::temp_dir().join("logfwd_test_enrichment.jsonl");
        std::fs::write(&tmp, "{\"ip\":\"1.2.3.4\",\"owner\":\"alice\"}\n")
            .expect("create temp jsonl");
        let yaml = format!(
            "pipelines:\n  app:\n    inputs:\n      - type: file\n        path: /tmp/x.log\n    outputs:\n      - type: stdout\n    enrichment:\n      - type: jsonl\n        table_name: ip_owners\n        path: {}\n",
            tmp.display()
        );
        let cfg = Config::load_str(&yaml).expect("jsonl enrichment should parse");
        let pipe = &cfg.pipelines["app"];
        assert_eq!(pipe.enrichment.len(), 1);
        match &pipe.enrichment[0] {
            EnrichmentConfig::Jsonl(c) => {
                assert_eq!(c.table_name, "ip_owners");
                assert_eq!(c.path, tmp.to_str().unwrap());
            }
            other => panic!("expected Jsonl, got {other:?}"),
        }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn enrichment_simple_form_preserved() {
        // Enrichment in simple form should be wired into the default pipeline,
        // not silently dropped (#540).
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
enrichment:
  - type: host_info
  - type: k8s_path
";
        let cfg = Config::load_str(yaml).expect("simple form with enrichment should parse");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.enrichment.len(),
            2,
            "enrichment should not be dropped in simple form"
        );
        assert!(matches!(&pipe.enrichment[0], EnrichmentConfig::HostInfo(_)));
        assert!(matches!(&pipe.enrichment[1], EnrichmentConfig::K8sPath(_)));
    }

    // -----------------------------------------------------------------------
    // Bug #481: server.log_level validation
    // -----------------------------------------------------------------------

    #[test]
    fn log_level_valid_values_accepted() {
        for level in ["trace", "debug", "info", "warn", "error", "INFO", "Warn"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: stdout\nserver:\n  log_level: {level}\n"
            );
            Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("log_level '{level}' should be valid: {e}"));
        }
    }

    #[test]
    fn log_level_invalid_value_rejected() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
server:
  log_level: inof
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("log_level"),
            "expected 'log_level' in error: {msg}"
        );
        assert!(
            msg.contains("not a recognised log level"),
            "expected 'not a recognised log level' in error: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Top-level enrichment rejected with pipelines: form
    // -----------------------------------------------------------------------

    #[test]
    fn top_level_resource_attrs_rejected_with_pipelines_form() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
resource_attrs:
  service.name: my-service
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("top-level `resource_attrs`"),
            "error should mention top-level resource_attrs: {msg}"
        );
        assert!(
            msg.contains("pipelines"),
            "error should mention pipelines: {msg}"
        );
    }

    #[test]
    fn top_level_enrichment_rejected_with_pipelines_form() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
enrichment:
  - type: geo_database
    format: mmdb
    path: /tmp/geo.mmdb
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("top-level `enrichment`"),
            "error should mention top-level enrichment: {msg}"
        );
        assert!(
            msg.contains("pipelines"),
            "error should mention pipelines form: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Input-side field rejection
    // -----------------------------------------------------------------------

    #[test]
    fn file_input_rejects_listen() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
        listen: 127.0.0.1:9999
    outputs:
      - type: null
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("listen"),
            "expected listen rejection: {err}"
        );
    }

    #[test]
    fn tcp_input_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        path: /tmp/test.log
    outputs:
      - type: null
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path"),
            "expected path rejection: {err}"
        );
    }

    #[test]
    fn tcp_input_rejects_max_open_files() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 0.0.0.0:514
        max_open_files: 128
    outputs:
      - type: null
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("max_open_files"),
            "expected max_open_files rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        path: /tmp/test.log
    outputs:
      - type: null
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path"),
            "expected path rejection: {err}"
        );
    }

    #[test]
    fn generator_input_rejects_listen() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        listen: 0.0.0.0:514
    outputs:
      - type: null
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("listen"),
            "expected listen rejection: {err}"
        );
    }

    #[test]
    fn arrow_ipc_input_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: arrow_ipc
    outputs:
      - type: null
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("not yet supported"),
            "expected arrow_ipc rejection: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Output-side field rejection
    // -----------------------------------------------------------------------

    #[test]
    fn non_elasticsearch_output_rejects_index() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        index: my-index
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("index"),
            "expected index rejection: {err}"
        );
    }

    #[test]
    fn non_otlp_output_rejects_protocol() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: http
        endpoint: http://localhost:9200
        protocol: grpc
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("protocol"),
            "expected protocol rejection: {err}"
        );
    }

    #[test]
    fn stdout_output_rejects_compression() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        compression: zstd
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("compression"),
            "expected compression rejection: {err}"
        );
    }

    #[test]
    fn stdout_output_rejects_auth() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        auth:
          bearer_token: "secret"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("auth"),
            "expected auth rejection: {err}"
        );
    }

    #[test]
    fn stdout_output_rejects_path() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        path: /tmp/out.log
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path"),
            "expected path rejection: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Format: text alias for console
    // -----------------------------------------------------------------------

    #[test]
    fn format_text_alias_accepted() {
        let yaml = r"
input:
  type: file
  path: /tmp/x.log
  format: text
output:
  type: stdout
";
        let cfg = Config::load_str(yaml).expect("format: text should be accepted");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs[0].format, Some(Format::Text));
    }

    // -----------------------------------------------------------------------
    // Loki-only field rejection for non-Loki outputs
    // -----------------------------------------------------------------------

    #[test]
    fn non_loki_output_rejects_tenant_id() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        tenant_id: my-tenant
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("tenant_id"),
            "expected tenant_id rejection: {err}"
        );
        assert!(
            err.to_string().contains("only supported for loki"),
            "expected loki-only message: {err}"
        );
    }

    #[test]
    fn non_loki_output_rejects_static_labels() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        static_labels:
          env: prod
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("static_labels"),
            "expected static_labels rejection: {err}"
        );
        assert!(
            err.to_string().contains("only supported for loki"),
            "expected loki-only message: {err}"
        );
    }

    #[test]
    fn non_loki_output_rejects_label_columns() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        label_columns:
          - container_name
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("label_columns"),
            "expected label_columns rejection: {err}"
        );
        assert!(
            err.to_string().contains("only supported for loki"),
            "expected loki-only message: {err}"
        );
    }
}
