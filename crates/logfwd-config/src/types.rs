use crate::compat;
use crate::serde_helpers::deserialize_one_or_many;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

/// Authentication configuration for output HTTP sinks.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
#[must_use]
#[non_exhaustive]
pub enum ConfigError {
    #[error("config I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config YAML error: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    #[error("config validation error: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InputType {
    File,
    Udp,
    Tcp,
    Otlp,
    Http,
    Generator,
    /// Linux beta sensor input (eBPF-oriented runtime path).
    LinuxSensorBeta,
    /// macOS beta sensor input (EndpointSecurity-oriented runtime path).
    MacosSensorBeta,
    /// Windows beta sensor input (eBPF/ETW hybrid-oriented runtime path).
    WindowsSensorBeta,
    ArrowIpc,
}

impl fmt::Display for InputType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputType::File => f.write_str("file"),
            InputType::Udp => f.write_str("udp"),
            InputType::Tcp => f.write_str("tcp"),
            InputType::Otlp => f.write_str("otlp"),
            InputType::Http => f.write_str("http"),
            InputType::Generator => f.write_str("generator"),
            InputType::LinuxSensorBeta => f.write_str("linux_sensor_beta"),
            InputType::MacosSensorBeta => f.write_str("macos_sensor_beta"),
            InputType::WindowsSensorBeta => f.write_str("windows_sensor_beta"),
            InputType::ArrowIpc => f.write_str("arrow_ipc"),
        }
    }
}

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
    Null,
    Tcp,
    Udp,
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
                compat::parse_output_type_name(v).ok_or_else(|| {
                    E::unknown_variant(v, compat::supported_output_type_names_for_errors())
                })
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<OutputType, E> {
                Ok(OutputType::Null)
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<OutputType, E> {
                Ok(OutputType::Null)
            }
        }

        d.deserialize_any(V)
    }
}

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
    Console,
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

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorProfileConfig {
    #[default]
    Logs,
    Record,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorComplexityConfig {
    #[default]
    Simple,
    Complex,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum GeneratorAttributeValueConfig {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorSequenceConfig {
    pub field: String,
    pub start: Option<u64>,
}

/// Timestamp configuration for the `logs` profile generator.
///
/// Controls the base timestamp and per-event step for generated log lines.
/// Only valid for `profile: logs` (default); rejected for `profile: record`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorTimestampConfig {
    /// ISO8601 datetime (`YYYY-MM-DDTHH:MM:SSZ`) or `"now"`.
    /// Default: `"2024-01-15T00:00:00Z"`.
    pub start: Option<String>,
    /// Milliseconds between events. Negative = backward. Default: 1.
    pub step_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethodConfig {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HttpInputConfig {
    pub path: Option<String>,
    pub strict_path: Option<bool>,
    pub method: Option<HttpMethodConfig>,
    pub max_request_body_size: Option<usize>,
    pub response_code: Option<u16>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GeneratorInputConfig {
    pub events_per_sec: Option<u64>,
    pub batch_size: Option<usize>,
    pub total_events: Option<u64>,
    pub complexity: Option<GeneratorComplexityConfig>,
    pub profile: Option<GeneratorProfileConfig>,
    #[serde(default)]
    pub attributes: HashMap<String, GeneratorAttributeValueConfig>,
    pub sequence: Option<GeneratorSequenceConfig>,
    pub event_created_unix_nano_field: Option<String>,
    /// Timestamp configuration for the `logs` profile.
    pub timestamp: Option<GeneratorTimestampConfig>,
}

/// Platform beta sensor configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PlatformSensorBetaInputConfig {
    /// Sensor heartbeat cadence. Defaults to 10_000 when omitted.
    pub poll_interval_ms: Option<u64>,
    /// Emit periodic heartbeat rows while the sensor is idle. Defaults to true.
    pub emit_heartbeat: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsInputConfig {
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub client_ca_file: Option<String>,
    #[serde(default)]
    pub require_client_auth: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputConfig {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub input_type: InputType,
    pub path: Option<String>,
    pub listen: Option<String>,
    pub format: Option<Format>,
    /// File input poll cadence in milliseconds (default: 50, minimum: 1).
    pub poll_interval_ms: Option<u64>,
    /// File tail read buffer in bytes (default: 262_144, minimum: 1, maximum: 4_194_304).
    pub read_buf_size: Option<usize>,
    /// Maximum bytes read per file per poll (default: 262_144, minimum: 1).
    pub per_file_read_budget_bytes: Option<usize>,
    pub max_open_files: Option<usize>,
    pub glob_rescan_interval_ms: Option<u64>,
    #[serde(default)]
    pub generator: Option<GeneratorInputConfig>,
    #[serde(default)]
    pub sensor_beta: Option<PlatformSensorBetaInputConfig>,
    #[serde(default)]
    pub http: Option<HttpInputConfig>,
    pub sql: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsInputConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub output_type: OutputType,
    pub endpoint: Option<String>,
    pub protocol: Option<String>,
    pub compression: Option<String>,
    pub request_mode: Option<String>,
    pub format: Option<Format>,
    pub path: Option<String>,
    pub index: Option<String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    pub tenant_id: Option<String>,
    pub static_labels: Option<HashMap<String, String>>,
    pub label_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GeoDatabaseFormat {
    Mmdb,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeoDatabaseConfig {
    pub format: GeoDatabaseFormat,
    pub path: String,
    pub refresh_interval: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticEnrichmentConfig {
    pub table_name: String,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfoConfig {}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct K8sPathConfig {
    #[serde(default = "default_k8s_table_name")]
    pub table_name: String,
}

fn default_k8s_table_name() -> String {
    "k8s_pods".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvEnrichmentConfig {
    pub table_name: String,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlEnrichmentConfig {
    pub table_name: String,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnrichmentConfig {
    GeoDatabase(GeoDatabaseConfig),
    Static(StaticEnrichmentConfig),
    HostInfo(HostInfoConfig),
    K8sPath(K8sPathConfig),
    Csv(CsvEnrichmentConfig),
    Jsonl(JsonlEnrichmentConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub inputs: Vec<InputConfig>,
    pub transform: Option<String>,
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub outputs: Vec<OutputConfig>,
    #[serde(default)]
    pub enrichment: Vec<EnrichmentConfig>,
    #[serde(default)]
    pub resource_attrs: HashMap<String, String>,
    pub workers: Option<usize>,
    pub batch_target_bytes: Option<usize>,
    pub batch_timeout_ms: Option<u64>,
    pub poll_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub diagnostics: Option<String>,
    pub log_level: Option<String>,
    pub metrics_endpoint: Option<String>,
    pub metrics_interval_secs: Option<u64>,
    pub traces_endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub data_dir: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub pipelines: HashMap<String, PipelineConfig>,
    pub server: ServerConfig,
    pub storage: StorageConfig,
}
