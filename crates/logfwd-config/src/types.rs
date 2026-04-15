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
    /// Linux eBPF sensor input.
    #[serde(rename = "linux_ebpf_sensor", alias = "linux_sensor_beta")]
    LinuxEbpfSensor,
    /// macOS EndpointSecurity sensor input.
    #[serde(
        rename = "macos_es_sensor",
        alias = "macos_sensor_beta",
        alias = "macos_endpointsecurity_sensor"
    )]
    MacosEsSensor,
    /// Windows eBPF sensor input.
    #[serde(rename = "windows_ebpf_sensor", alias = "windows_sensor_beta")]
    WindowsEbpfSensor,
    ArrowIpc,
    /// Journald (systemd journal) input via native `sd_journal` API or
    /// `journalctl` subprocess fallback.
    Journald,
    /// Host metrics input (process snapshots, CPU, memory, network stats via sysinfo).
    #[serde(rename = "host_metrics")]
    HostMetrics,
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
            InputType::LinuxEbpfSensor => f.write_str("linux_ebpf_sensor"),
            InputType::MacosEsSensor => f.write_str("macos_es_sensor"),
            InputType::WindowsEbpfSensor => f.write_str("windows_ebpf_sensor"),
            InputType::ArrowIpc => f.write_str("arrow_ipc"),
            InputType::Journald => f.write_str("journald"),
            InputType::HostMetrics => f.write_str("host_metrics"),
        }
    }
}

/// OTLP protobuf decode strategy for OTLP inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtobufDecodeModeConfig {
    /// Decode through the generated prost OTLP model.
    #[default]
    Prost,
    /// Try the experimental direct Arrow projection first, falling back to
    /// prost for valid OTLP shapes the projection path does not cover.
    ProjectedFallback,
    /// Decode only through the experimental direct Arrow projection path.
    ProjectedOnly,
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
    /// Max bytes to drain per poll call. Default matches OTLP receiver (1GB).
    pub max_drained_bytes_per_poll: Option<usize>,
    pub response_code: Option<u16>,
    /// Optional static body returned on successful ingest.
    /// Must be omitted when `response_code` is `204`.
    pub response_body: Option<String>,
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

/// Host metrics configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HostMetricsInputConfig {
    /// Sensor sample cadence. Defaults to 10_000 when omitted.
    pub poll_interval_ms: Option<u64>,
    /// Deprecated no-op retained for backward compatibility.
    ///
    /// Sensor inputs are Arrow-native and do not emit heartbeat rows.
    pub emit_heartbeat: Option<bool>,
    /// Optional path to a JSON control file for runtime sensor tuning.
    pub control_path: Option<String>,
    /// How often to check `control_path` for updates. Defaults to 1_000 when omitted.
    pub control_reload_interval_ms: Option<u64>,
    /// Optional explicit enabled families for this platform.
    ///
    /// `None` means "use platform defaults". `Some([])` means "disable all".
    pub enabled_families: Option<Vec<String>>,
    /// Emit periodic per-family sample rows. Defaults to true.
    pub emit_signal_rows: Option<bool>,
    /// Upper bound on data rows emitted per collection cycle. Defaults to 256.
    pub max_rows_per_poll: Option<usize>,
    /// Path to the compiled eBPF kernel binary (required for `linux_ebpf_sensor`).
    pub ebpf_binary_path: Option<String>,
    /// Maximum events to drain per poll cycle (default: 4096).
    pub max_events_per_poll: Option<usize>,
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

/// Journald (systemd journal) input configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournaldInputConfig {
    /// Systemd units to include. If empty, all units are collected.
    /// Unit names without a `.` are suffixed with `.service` automatically.
    #[serde(default)]
    pub include_units: Vec<String>,
    /// Systemd units to exclude.
    #[serde(default)]
    pub exclude_units: Vec<String>,
    /// Only include entries from the current boot (default: true).
    #[serde(default = "default_true")]
    pub current_boot_only: bool,
    /// Only include entries appended after the receiver starts (default: false).
    /// When false, reads all history from the current boot.
    #[serde(default)]
    pub since_now: bool,
    /// Path to `journalctl` binary. Defaults to `journalctl` (found via PATH).
    pub journalctl_path: Option<String>,
    /// Custom journal directory (passed as `--directory=<path>`).
    pub journal_directory: Option<String>,
    /// Journal namespace (passed as `--namespace=<ns>`).
    pub journal_namespace: Option<String>,
    /// Backend to use for reading the journal.
    ///
    /// - `auto` (default): use native `sd_journal` API if `libsystemd.so.0` is
    ///   available, otherwise fall back to a `journalctl` subprocess.
    /// - `native`: require the native `sd_journal` API; error if unavailable.
    /// - `subprocess`: always use a `journalctl` subprocess.
    #[serde(default)]
    pub backend: JournaldBackendConfig,
}

/// Which journal-reading backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JournaldBackendConfig {
    /// Use native API if available, otherwise subprocess (default).
    #[default]
    Auto,
    /// Require the native `sd_journal` C API via `dlopen`.
    Native,
    /// Always use a `journalctl` subprocess.
    Subprocess,
}

fn default_true() -> bool {
    true
}

impl Default for JournaldInputConfig {
    fn default() -> Self {
        Self {
            include_units: Vec::new(),
            exclude_units: Vec::new(),
            current_boot_only: true,
            since_now: false,
            journalctl_path: None,
            journal_directory: None,
            journal_namespace: None,
            backend: JournaldBackendConfig::Auto,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct InputConfig {
    pub name: Option<String>,
    pub format: Option<Format>,
    pub sql: Option<String>,
    #[serde(flatten)]
    pub type_config: InputTypeConfig,
}

impl InputConfig {
    /// Returns the [`InputType`] for this input.
    pub fn input_type(&self) -> InputType {
        self.type_config.input_type()
    }
}

/// Tagged‐union carrying per‐input‐type configuration.
///
/// Serde tags on `"type"` and uses `deny_unknown_fields` on each variant
/// struct, so a YAML like `type: file` with an `listen:` key is rejected
/// at parse time instead of silently ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputTypeConfig {
    File(FileTypeConfig),
    Udp(UdpTypeConfig),
    Tcp(TcpTypeConfig),
    Otlp(OtlpTypeConfig),
    Http(HttpTypeConfig),
    Generator(GeneratorTypeConfig),
    #[serde(rename = "linux_ebpf_sensor", alias = "linux_sensor_beta")]
    LinuxEbpfSensor(SensorTypeConfig),
    #[serde(
        rename = "macos_es_sensor",
        alias = "macos_sensor_beta",
        alias = "macos_endpointsecurity_sensor"
    )]
    MacosEsSensor(SensorTypeConfig),
    #[serde(rename = "windows_ebpf_sensor", alias = "windows_sensor_beta")]
    WindowsEbpfSensor(SensorTypeConfig),
    ArrowIpc(ArrowIpcTypeConfig),
    Journald(JournaldTypeConfig),
    /// Host metrics input (process snapshots, CPU, memory, network stats via sysinfo).
    #[serde(rename = "host_metrics")]
    HostMetrics(SensorTypeConfig),
}

impl InputTypeConfig {
    /// Map the variant back to the flat [`InputType`] discriminant.
    pub fn input_type(&self) -> InputType {
        match self {
            Self::File(_) => InputType::File,
            Self::Udp(_) => InputType::Udp,
            Self::Tcp(_) => InputType::Tcp,
            Self::Otlp(_) => InputType::Otlp,
            Self::Http(_) => InputType::Http,
            Self::Generator(_) => InputType::Generator,
            Self::LinuxEbpfSensor(_) => InputType::LinuxEbpfSensor,
            Self::MacosEsSensor(_) => InputType::MacosEsSensor,
            Self::WindowsEbpfSensor(_) => InputType::WindowsEbpfSensor,
            Self::ArrowIpc(_) => InputType::ArrowIpc,
            Self::Journald(_) => InputType::Journald,
            Self::HostMetrics(_) => InputType::HostMetrics,
        }
    }
}

// ── Per-type config structs ────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileTypeConfig {
    pub path: String,
    /// File input poll cadence in milliseconds (default: 50, minimum: 1).
    pub poll_interval_ms: Option<u64>,
    /// File tail read buffer in bytes (default: 262_144, minimum: 1, maximum: 4_194_304).
    pub read_buf_size: Option<usize>,
    /// Maximum bytes read per file per poll (default: 262_144, minimum: 1).
    pub per_file_read_budget_bytes: Option<usize>,
    /// Immediate repoll budget armed when a file poll hits read budget
    /// (default: 8, set to 0 to disable adaptive fast repolls).
    pub adaptive_fast_polls_max: Option<u8>,
    pub max_open_files: Option<usize>,
    pub glob_rescan_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpTypeConfig {
    pub listen: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpTypeConfig {
    pub listen: String,
    #[serde(default)]
    pub tls: Option<TlsInputConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpTypeConfig {
    pub listen: String,
    /// Prefix applied to OTLP resource attributes when flattening into columns.
    /// Defaults to `resource.attributes.` when omitted.
    pub resource_prefix: Option<String>,
    /// Experimental OTLP protobuf decode strategy. Defaults to `prost`.
    pub protobuf_decode_mode: Option<OtlpProtobufDecodeModeConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTypeConfig {
    pub listen: String,
    #[serde(default)]
    pub http: Option<HttpInputConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GeneratorTypeConfig {
    #[serde(default)]
    pub generator: Option<GeneratorInputConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SensorTypeConfig {
    #[serde(default, alias = "sensor_beta")]
    pub sensor: Option<HostMetricsInputConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcTypeConfig {
    pub listen: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JournaldTypeConfig {
    #[serde(default)]
    pub journald: Option<JournaldInputConfig>,
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
