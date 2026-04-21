use crate::compat;
use crate::serde_helpers::{
    PositiveMillis, PositiveSecs, deserialize_from_string_or_value, deserialize_one_or_many,
    deserialize_option_from_string_or_value, deserialize_option_strict_string,
    deserialize_option_string_map_strict_values, deserialize_option_vec_strict_string,
    deserialize_strict_string, deserialize_string_map_strict_values, deserialize_vec_strict_string,
};
use crate::shared::{TlsClientConfig, TlsServerConfig};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

/// Conservative upper bound for per-pipeline worker count.
pub(crate) const PIPELINE_WORKERS_MAX: usize = 1024;

/// Authentication configuration for output HTTP sinks.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub bearer_token: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_map_strict_values")]
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
    /// Finite input read from process standard input.
    Stdin,
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
    /// AWS S3 (and S3-compatible) object storage input.
    S3,
}

impl fmt::Display for InputType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputType::File => f.write_str("file"),
            InputType::Udp => f.write_str("udp"),
            InputType::Tcp => f.write_str("tcp"),
            InputType::Otlp => f.write_str("otlp"),
            InputType::Http => f.write_str("http"),
            InputType::Stdin => f.write_str("stdin"),
            InputType::Generator => f.write_str("generator"),
            InputType::LinuxEbpfSensor => f.write_str("linux_ebpf_sensor"),
            InputType::MacosEsSensor => f.write_str("macos_es_sensor"),
            InputType::WindowsEbpfSensor => f.write_str("windows_ebpf_sensor"),
            InputType::ArrowIpc => f.write_str("arrow_ipc"),
            InputType::Journald => f.write_str("journald"),
            InputType::HostMetrics => f.write_str("host_metrics"),
            InputType::S3 => f.write_str("s3"),
        }
    }
}

/// Controls which source metadata columns are attached to scanned records.
///
/// `none` is the default and disables attachment. `fastforward` emits the
/// internal `__source_id` handle. `ecs` emits ECS/Beats `file.path` and accepts
/// the serde alias `beats`. `otel` emits `log.file.path`. `vector` emits
/// `file`. Values serialize and parse as `snake_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceMetadataStyle {
    /// Do not attach source metadata columns.
    #[default]
    None,
    /// Attach FastForward internal metadata (`__source_id`).
    Fastforward,
    /// Attach ECS/Beats-style public metadata columns.
    #[serde(alias = "beats")]
    Ecs,
    /// Attach OpenTelemetry-style public metadata columns.
    Otel,
    /// Attach Vector-style public metadata columns.
    Vector,
}

impl fmt::Display for SourceMetadataStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Fastforward => f.write_str("fastforward"),
            Self::Ecs => f.write_str("ecs"),
            Self::Otel => f.write_str("otel"),
            Self::Vector => f.write_str("vector"),
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

impl OutputType {
    /// Return whether this output type requires an explicit `endpoint`.
    ///
    /// ```
    /// use logfwd_config::OutputType;
    ///
    /// assert!(OutputType::Otlp.is_endpoint_required());
    /// assert!(!OutputType::Stdout.is_endpoint_required());
    /// ```
    pub fn is_endpoint_required(&self) -> bool {
        matches!(
            self,
            Self::Otlp
                | Self::Http
                | Self::Elasticsearch
                | Self::Loki
                | Self::ArrowIpc
                | Self::Tcp
                | Self::Udp
        )
    }

    /// Return whether this output type supports HTTP-style `auth`.
    ///
    /// ```
    /// use logfwd_config::OutputType;
    ///
    /// assert!(OutputType::Loki.is_auth_supported());
    /// assert!(!OutputType::Tcp.is_auth_supported());
    /// ```
    pub fn is_auth_supported(&self) -> bool {
        matches!(
            self,
            Self::Otlp | Self::Http | Self::Elasticsearch | Self::Loki | Self::ArrowIpc
        )
    }
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
        }

        d.deserialize_any(V)
    }
}

/// Request-body compression configured for output sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum CompressionFormat {
    /// Do not compress request bodies.
    None,
    /// Compress request bodies with gzip.
    Gzip,
    /// Compress request bodies with zstd.
    Zstd,
}

impl fmt::Display for CompressionFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionFormat::None => f.write_str("none"),
            CompressionFormat::Gzip => f.write_str("gzip"),
            CompressionFormat::Zstd => f.write_str("zstd"),
        }
    }
}

/// OTLP transport protocol configured for OTLP outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum OtlpProtocol {
    /// Send OTLP data over HTTP.
    Http,
    /// Send OTLP data over gRPC.
    Grpc,
}

impl fmt::Display for OtlpProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OtlpProtocol::Http => f.write_str("http"),
            OtlpProtocol::Grpc => f.write_str("grpc"),
        }
    }
}

/// Elasticsearch bulk request construction mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ElasticsearchRequestMode {
    /// Build the Elasticsearch bulk request body in memory before sending.
    Buffered,
    /// Stream Elasticsearch bulk request body chunks while sending.
    Streaming,
}

impl fmt::Display for ElasticsearchRequestMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElasticsearchRequestMode::Buffered => f.write_str("buffered"),
            ElasticsearchRequestMode::Streaming => f.write_str("streaming"),
        }
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

impl Format {
    /// Returns whether this format can be decoded from stdin input.
    pub fn is_stdin_compatible(&self) -> bool {
        matches!(self, Self::Auto | Self::Cri | Self::Json | Self::Raw)
    }
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
    /// Realistic Envoy edge-proxy access logs.
    Envoy,
    /// CRI-formatted Kubernetes container logs.
    CriK8s,
    /// Wide structured logs with 20+ fields.
    Wide,
    /// Narrow JSON logs with 5 fields.
    Narrow,
    /// CloudTrail-like AWS audit log events.
    CloudTrail,
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
    String(#[serde(deserialize_with = "deserialize_strict_string")] String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Unsupported(serde_yaml_ng::Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorSequenceConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub field: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
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
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub start: Option<String>,
    /// Milliseconds between events. Negative = backward. Default: 1.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
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
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub path: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub strict_path: Option<bool>,
    pub method: Option<HttpMethodConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_request_body_size: Option<usize>,
    /// Max bytes to drain per poll call. Default matches OTLP receiver (1GB).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_drained_bytes_per_poll: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub response_code: Option<u16>,
    /// Optional static body returned on successful ingest.
    /// Must be omitted when `response_code` is `204`.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub response_body: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GeneratorInputConfig {
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub events_per_second: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub num_lines: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub message_template: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub field_count: Option<usize>,

    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub events_per_sec: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_size: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub total_events: Option<u64>,
    pub complexity: Option<GeneratorComplexityConfig>,
    pub profile: Option<GeneratorProfileConfig>,
    #[serde(default)]
    pub attributes: HashMap<String, GeneratorAttributeValueConfig>,
    pub sequence: Option<GeneratorSequenceConfig>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub event_created_unix_nano_field: Option<String>,
    /// Timestamp configuration for the `logs` profile.
    pub timestamp: Option<GeneratorTimestampConfig>,
}

/// Host metrics configuration.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HostMetricsInputConfig {
    /// Sensor sample cadence. Defaults to 10_000 when omitted.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub poll_interval_ms: Option<PositiveMillis>,
    /// Optional path to a JSON control file for runtime sensor tuning.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub control_path: Option<String>,
    /// How often to check `control_path` for updates. Defaults to 1_000 when omitted.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub control_reload_interval_ms: Option<PositiveMillis>,
    /// Optional explicit enabled families for this platform.
    ///
    /// `None` means "use platform defaults". `Some([])` means "disable all".
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub enabled_families: Option<Vec<String>>,
    /// Emit periodic per-family sample rows. Defaults to true.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub emit_signal_rows: Option<bool>,
    /// Upper bound on data rows emitted per collection cycle. Defaults to 256.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_rows_per_poll: Option<usize>,
    /// Upper bound on process rows emitted per collection cycle.
    ///
    /// Defaults to 1024. Set to 0 or omit for the default.
    pub max_process_rows_per_poll: Option<usize>,
    /// Path to the compiled eBPF kernel binary (required for `linux_ebpf_sensor`).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub ebpf_binary_path: Option<String>,
    /// Maximum events to drain per poll cycle (default: 4096).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_events_per_poll: Option<usize>,
    /// Glob patterns for process names to include (e.g., `["nginx*", "python"]`).
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub include_process_names: Option<Vec<String>>,
    /// Glob patterns for process names to exclude.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub exclude_process_names: Option<Vec<String>>,
    /// Event types to enable for `linux_ebpf_sensor` inputs (e.g., `["exec", "tcp_connect"]`).
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub include_event_types: Option<Vec<String>>,
    /// Event types to disable for `linux_ebpf_sensor` inputs. Excludes take precedence over includes.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub exclude_event_types: Option<Vec<String>>,
    /// Ring buffer size in kilobytes.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub ring_buffer_size_kb: Option<usize>,
    /// Optional list of scrapers to run (e.g. `["cpu", "memory", "disk", "network", "filesystem"]`).
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub scrapers: Option<Vec<String>>,
    /// Cadence for metrics collection in milliseconds.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub collection_interval_ms: Option<PositiveMillis>,
    /// List of disk devices to include.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub disk_include_devices: Option<Vec<String>>,
    /// List of disk devices to exclude.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub disk_exclude_devices: Option<Vec<String>>,
    /// List of network interfaces to include.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub network_include_interfaces: Option<Vec<String>>,
    /// List of network interfaces to exclude.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub network_exclude_interfaces: Option<Vec<String>>,
    /// List of filesystem mount points to include.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub filesystem_include_mount_points: Option<Vec<String>>,
    /// List of filesystem mount points to exclude.
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub filesystem_exclude_mount_points: Option<Vec<String>>,
}

/// Journald (systemd journal) input configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournaldInputConfig {
    /// Systemd units to include. If empty, all units are collected.
    /// Unit names without a `.` are suffixed with `.service` automatically.
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    pub include_units: Vec<String>,
    /// Systemd units to exclude.
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    pub exclude_units: Vec<String>,
    /// Syslog identifiers (`SYSLOG_IDENTIFIER=`) to include.
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    pub identifiers: Vec<String>,
    /// Priority/log levels (e.g. `0`, `3`, `info`, `err`) to include.
    #[serde(default, deserialize_with = "deserialize_vec_strict_string")]
    pub priorities: Vec<String>,
    /// Path to persist the cursor. Allows resuming after restarts.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub cursor_path: Option<String>,
    /// Include `_BOOT_ID` field in output (default: false).
    #[serde(default, deserialize_with = "deserialize_from_string_or_value")]
    pub include_boot_id: bool,
    /// Only include entries from the current boot (default: true).
    #[serde(
        default = "default_true",
        deserialize_with = "deserialize_from_string_or_value"
    )]
    pub current_boot_only: bool,
    /// Only include entries appended after the receiver starts (default: false).
    /// When false, reads all history from the current boot.
    #[serde(default, deserialize_with = "deserialize_from_string_or_value")]
    pub since_now: bool,
    /// Path to `journalctl` binary. Defaults to `journalctl` (found via PATH).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub journalctl_path: Option<String>,
    /// Custom journal directory (passed as `--directory=<path>`).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub journal_directory: Option<String>,
    /// Journal namespace (passed as `--namespace=<ns>`).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
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
            identifiers: Vec::new(),
            priorities: Vec::new(),
            cursor_path: None,
            include_boot_id: false,
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
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    pub format: Option<Format>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub sql: Option<String>,
    /// Source metadata attachment style.
    ///
    /// `none` is the default. `fastforward` attaches only the internal
    /// `__source_id` handle. Public styles attach normal source metadata
    /// columns using that schema's naming convention.
    #[serde(default)]
    pub source_metadata: SourceMetadataStyle,
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
    /// Configuration for `type: stdin`.
    Stdin(StdinTypeConfig),
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
    /// AWS S3 (and S3-compatible) object storage input.
    S3(S3TypeConfig),
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
            Self::Stdin(_) => InputType::Stdin,
            Self::Generator(_) => InputType::Generator,
            Self::LinuxEbpfSensor(_) => InputType::LinuxEbpfSensor,
            Self::MacosEsSensor(_) => InputType::MacosEsSensor,
            Self::WindowsEbpfSensor(_) => InputType::WindowsEbpfSensor,
            Self::ArrowIpc(_) => InputType::ArrowIpc,
            Self::Journald(_) => InputType::Journald,
            Self::HostMetrics(_) => InputType::HostMetrics,
            Self::S3(_) => InputType::S3,
        }
    }
}

// ── Per-type config structs ────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub path: String,
    /// File input poll cadence in milliseconds (default: 50, minimum: 1).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub poll_interval_ms: Option<PositiveMillis>,
    /// File tail read buffer in bytes (default: 262_144, minimum: 1, maximum: 4_194_304).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub read_buf_size: Option<usize>,
    /// Maximum bytes read per file per poll (default: 262_144, minimum: 1).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub per_file_read_budget_bytes: Option<usize>,
    /// Immediate repoll budget armed when a file poll hits read budget
    /// (default: 8, set to 0 to disable adaptive fast repolls).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub adaptive_fast_polls_max: Option<u8>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_open_files: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub glob_rescan_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_message_size_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub so_rcvbuf: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    #[serde(default)]
    pub tls: Option<TlsServerConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_clients: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub connection_timeout_ms: Option<PositiveMillis>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub read_timeout_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    /// Experimental OTLP protobuf decode strategy. Defaults to `prost`.
    pub protobuf_decode_mode: Option<OtlpProtobufDecodeModeConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_recv_message_size_bytes: Option<usize>,
    #[serde(default)]
    pub tls: Option<TlsServerConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub grpc_keepalive_time_ms: Option<PositiveMillis>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub grpc_max_concurrent_streams: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    #[serde(default)]
    pub http: Option<HttpInputConfig>,
}

/// Configuration for stdin input.
///
/// Stdin input has no input-specific fields; unknown fields are rejected so
/// file-tail options such as `path` do not silently apply to command input.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StdinTypeConfig {}

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
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_connections: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_message_size_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JournaldTypeConfig {
    #[serde(default)]
    pub journald: Option<JournaldInputConfig>,
}

/// Tagged-union wrapper for S3 input configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3TypeConfig {
    /// S3-specific input settings (bucket, region, credentials, tuning).
    pub s3: S3InputConfig,
}

/// Configuration for the S3 (and S3-compatible) object storage input.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3InputConfig {
    /// S3 bucket name.
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub bucket: String,
    /// AWS region (e.g. `"us-east-1"`). Defaults to `"us-east-1"`.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub region: Option<String>,
    /// Override S3 endpoint URL (e.g. `"http://localhost:9000"` for MinIO).
    /// When set, path-style addressing is used automatically.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    /// Only process keys with this prefix.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub prefix: Option<String>,
    /// SQS queue URL for event-driven object discovery.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub sqs_queue_url: Option<String>,
    /// `ListObjectsV2` `StartAfter` key for resumable prefix scanning.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub start_after: Option<String>,
    /// AWS access key ID. Falls back to `AWS_ACCESS_KEY_ID` env var.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub access_key_id: Option<String>,
    /// AWS secret access key. Falls back to `AWS_SECRET_ACCESS_KEY` env var.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub secret_access_key: Option<String>,
    /// AWS session token for temporary credentials. Falls back to `AWS_SESSION_TOKEN` env var.
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub session_token: Option<String>,
    /// Range-GET part size in bytes. Default: 8 MiB.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub part_size_bytes: Option<u64>,
    /// Max concurrent range GET tasks per object. Default: 8.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_concurrent_fetches: Option<usize>,
    /// Max objects being fetched simultaneously. Default: 4.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_concurrent_objects: Option<usize>,
    /// SQS visibility timeout in seconds. Default: 300.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub visibility_timeout_secs: Option<u32>,
    /// Compression override: `"auto"`, `"gzip"` (or `"gz"`), `"zstd"` (or
    /// `"zst"`), `"snappy"` (or `"sz"`), `"none"` (or `"identity"`).
    /// Default: `"auto"` (detect from key extension or Content-Encoding).
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub compression: Option<String>,
    /// Polling interval for `ListObjectsV2` mode in milliseconds. Default: 5000.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub poll_interval_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum OutputConfigV2 {
    Otlp(OtlpOutputConfig),
    Http(HttpOutputConfig),
    Elasticsearch(ElasticsearchOutputConfig),
    Loki(LokiOutputConfig),
    Stdout(StdoutOutputConfig),
    File(FileOutputConfig),
    Parquet(ParquetOutputConfig),
    Null(NullOutputConfig),
    Tcp(SocketOutputConfig),
    Udp(SocketOutputConfig),
    ArrowIpc(ArrowIpcOutputConfig),
}

impl OutputConfigV2 {
    /// Return the optional user-provided output name for this typed output.
    pub fn name(&self) -> Option<&str> {
        match self {
            OutputConfigV2::Otlp(config) => config.name.as_deref(),
            OutputConfigV2::Http(config) => config.name.as_deref(),
            OutputConfigV2::Elasticsearch(config) => config.name.as_deref(),
            OutputConfigV2::Loki(config) => config.name.as_deref(),
            OutputConfigV2::Stdout(config) => config.name.as_deref(),
            OutputConfigV2::File(config) => config.name.as_deref(),
            OutputConfigV2::Parquet(config) => config.name.as_deref(),
            OutputConfigV2::Null(config) => config.name.as_deref(),
            OutputConfigV2::Tcp(config) => config.name.as_deref(),
            OutputConfigV2::Udp(config) => config.name.as_deref(),
            OutputConfigV2::ArrowIpc(config) => config.name.as_deref(),
        }
    }

    /// Return the endpoint for output variants that connect to a destination.
    ///
    /// ```
    /// use logfwd_config::{OutputConfigV2, SocketOutputConfig, StdoutOutputConfig};
    ///
    /// let tcp = OutputConfigV2::Tcp(SocketOutputConfig {
    ///     endpoint: Some("127.0.0.1:15140".to_owned()),
    ///     ..Default::default()
    /// });
    /// assert_eq!(tcp.endpoint(), Some("127.0.0.1:15140"));
    ///
    /// let stdout = OutputConfigV2::Stdout(StdoutOutputConfig::default());
    /// assert_eq!(stdout.endpoint(), None);
    /// ```
    pub fn endpoint(&self) -> Option<&str> {
        match self {
            OutputConfigV2::Otlp(config) => config.endpoint.as_deref(),
            OutputConfigV2::Http(config) => config.endpoint.as_deref(),
            OutputConfigV2::Elasticsearch(config) => config.endpoint.as_deref(),
            OutputConfigV2::Loki(config) => config.endpoint.as_deref(),
            OutputConfigV2::Tcp(config) => config.endpoint.as_deref(),
            OutputConfigV2::Udp(config) => config.endpoint.as_deref(),
            OutputConfigV2::ArrowIpc(config) => config.endpoint.as_deref(),
            OutputConfigV2::Stdout(_)
            | OutputConfigV2::File(_)
            | OutputConfigV2::Parquet(_)
            | OutputConfigV2::Null(_) => None,
        }
    }

    /// Return authentication settings for output variants that support them.
    ///
    /// ```
    /// use logfwd_config::{AuthConfig, OtlpOutputConfig, OutputConfigV2, SocketOutputConfig};
    ///
    /// let otlp = OutputConfigV2::Otlp(OtlpOutputConfig {
    ///     auth: Some(AuthConfig {
    ///         bearer_token: Some("token".to_owned()),
    ///         ..Default::default()
    ///     }),
    ///     ..Default::default()
    /// });
    /// assert_eq!(
    ///     otlp.auth().and_then(|auth| auth.bearer_token.as_deref()),
    ///     Some("token")
    /// );
    ///
    /// let tcp = OutputConfigV2::Tcp(SocketOutputConfig::default());
    /// assert!(tcp.auth().is_none());
    /// ```
    pub fn auth(&self) -> Option<&AuthConfig> {
        match self {
            OutputConfigV2::Otlp(config) => config.auth.as_ref(),
            OutputConfigV2::Http(config) => config.auth.as_ref(),
            OutputConfigV2::Elasticsearch(config) => config.auth.as_ref(),
            OutputConfigV2::Loki(config) => config.auth.as_ref(),
            OutputConfigV2::ArrowIpc(config) => config.auth.as_ref(),
            OutputConfigV2::Stdout(_)
            | OutputConfigV2::File(_)
            | OutputConfigV2::Parquet(_)
            | OutputConfigV2::Null(_)
            | OutputConfigV2::Tcp(_)
            | OutputConfigV2::Udp(_) => None,
        }
    }

    /// Return the flat output type tag corresponding to this typed variant.
    pub fn output_type(&self) -> OutputType {
        match self {
            OutputConfigV2::Otlp(_) => OutputType::Otlp,
            OutputConfigV2::Http(_) => OutputType::Http,
            OutputConfigV2::Elasticsearch(_) => OutputType::Elasticsearch,
            OutputConfigV2::Loki(_) => OutputType::Loki,
            OutputConfigV2::Stdout(_) => OutputType::Stdout,
            OutputConfigV2::File(_) => OutputType::File,
            OutputConfigV2::Parquet(_) => OutputType::Parquet,
            OutputConfigV2::Null(_) => OutputType::Null,
            OutputConfigV2::Tcp(_) => OutputType::Tcp,
            OutputConfigV2::Udp(_) => OutputType::Udp,
            OutputConfigV2::ArrowIpc(_) => OutputType::ArrowIpc,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OtlpOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub protocol: Option<OtlpProtocol>,
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
    #[serde(
        default,
        deserialize_with = "deserialize_option_string_map_strict_values"
    )]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub retry_attempts: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub retry_initial_backoff_ms: Option<PositiveMillis>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub retry_max_backoff_ms: Option<PositiveMillis>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub request_timeout_ms: Option<PositiveMillis>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_size: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_timeout_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HttpOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    pub format: Option<Format>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ElasticsearchOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    #[serde(default)]
    pub request_mode: Option<ElasticsearchRequestMode>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub index: Option<String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub request_timeout_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LokiOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub tenant_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_option_string_map_strict_values"
    )]
    pub static_labels: Option<HashMap<String, String>>,
    #[serde(default, deserialize_with = "deserialize_option_vec_strict_string")]
    pub label_columns: Option<Vec<String>>,
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub request_timeout_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StdoutOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    pub format: Option<Format>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
/// File output configuration for the typed V2 schema.
///
/// File compression is intentionally not part of the typed schema; unknown
/// fields are rejected during deserialization.
///
/// ```
/// use logfwd_config::{Config, OutputConfigV2};
///
/// let cfg = Config::load_str(r#"
/// input:
///   type: generator
/// output:
///   type: file
///   path: ./out.ndjson
/// "#)
/// .expect("file output should parse");
///
/// assert!(matches!(
///     &cfg.pipelines["default"].outputs[0],
///     OutputConfigV2::File(_)
/// ));
/// ```
pub struct FileOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub path: Option<String>,
    pub format: Option<Format>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ParquetOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub path: Option<String>,
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    pub format: Option<Format>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NullOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SocketOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub host: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub port: Option<u16>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub buffer_size_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_size: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub write_schema_on_connect: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GeoDatabaseFormat {
    /// MaxMind GeoIP2 / GeoLite2 `.mmdb` binary format.
    Mmdb,
    /// CSV file with `ip_range_start`, `ip_range_end` columns plus optional
    /// `country_code`, `country_name`, `stateprov`, `city`, `latitude`,
    /// `longitude`, `asn`, `org` columns.  Compatible with DB-IP Lite exports.
    CsvRange,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeoDatabaseConfig {
    pub format: GeoDatabaseFormat,
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub path: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub refresh_interval: Option<PositiveSecs>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    #[serde(deserialize_with = "deserialize_string_map_strict_values")]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfoConfig {}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct K8sPathConfig {
    #[serde(
        default = "default_k8s_table_name",
        deserialize_with = "deserialize_strict_string"
    )]
    pub table_name: String,
}

fn default_k8s_table_name() -> String {
    "k8s_pods".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub path: String,
    /// Reload the file from disk every N seconds. If absent the file is read
    /// once at startup and never reloaded.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub refresh_interval: Option<PositiveSecs>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub path: String,
    /// Reload the file from disk every N seconds. If absent the file is read
    /// once at startup and never reloaded.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub refresh_interval: Option<PositiveSecs>,
}

/// Enriches logs with a single-row table populated from environment variables
/// whose names begin with `prefix`.  The prefix is stripped and the remainder
/// lower-cased to form column names.
///
/// ```yaml
/// enrichment:
///   - type: env_vars
///     table_name: deploy_meta
///     prefix: LOGFWD_META_
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvVarsEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    /// Environment variable name prefix to filter on (e.g. `"LOGFWD_META_"`).
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub prefix: String,
}

/// Agent self-metadata enrichment: `agent_name`, `agent_version`, `pid`, `start_time`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessInfoConfig {}

/// Parse a KEY=value properties file into a one-row enrichment table.
///
/// Supports bare, double-quoted, and single-quoted values.  Lines starting
/// with `#` are comments.  Column names are lower-cased key names.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvFileEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub path: String,
    /// Reload the file from disk every N seconds (must be >= 1).
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub refresh_interval: Option<PositiveSecs>,
}

/// Network interface metadata: `hostname`, `primary_ipv4`, `primary_ipv6`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkInfoConfig {}

/// Container runtime detection: `container_id`, `container_runtime`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerInfoConfig {}

/// Kubernetes cluster metadata from the downward API: `node_name`, `cluster_name`, etc.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct K8sClusterInfoConfig {}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnrichmentConfig {
    GeoDatabase(GeoDatabaseConfig),
    Static(StaticEnrichmentConfig),
    HostInfo(HostInfoConfig),
    K8sPath(K8sPathConfig),
    Csv(CsvEnrichmentConfig),
    Jsonl(JsonlEnrichmentConfig),
    /// Populate a one-row enrichment table from environment variables.
    EnvVars(EnvVarsEnrichmentConfig),
    /// Agent self-metadata: `agent_name`, `agent_version`, `pid`, `start_time`.
    ProcessInfo(ProcessInfoConfig),
    /// Parse a KEY=value properties file into a one-row enrichment table.
    KvFile(KvFileEnrichmentConfig),
    /// Network interface metadata: hostname, IPs.
    NetworkInfo(NetworkInfoConfig),
    /// Container runtime detection: container ID, runtime name.
    ContainerInfo(ContainerInfoConfig),
    /// Kubernetes cluster metadata from downward API.
    K8sClusterInfo(K8sClusterInfoConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub inputs: Vec<InputConfig>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub transform: Option<String>,
    /// Output sinks for this pipeline.
    ///
    /// YAML accepts either one output mapping or a list because this field uses
    /// `deserialize_one_or_many`. Each item is parsed as an `OutputConfigV2`
    /// tagged enum variant, so fields are strict for the selected output type
    /// and unknown or variant-inapplicable fields are rejected during
    /// deserialization.
    ///
    /// ```
    /// use logfwd_config::{Config, OutputConfigV2};
    ///
    /// let single = Config::load_str(r#"
    /// pipelines:
    ///   app:
    ///     inputs:
    ///       - type: stdin
    ///     outputs:
    ///       type: stdout
    /// "#).expect("single output form should parse");
    /// assert!(matches!(
    ///     &single.pipelines["app"].outputs[0],
    ///     OutputConfigV2::Stdout(_)
    /// ));
    ///
    /// let list = Config::load_str(r#"
    /// pipelines:
    ///   app:
    ///     inputs:
    ///       - type: stdin
    ///     outputs:
    ///       - type: stdout
    ///       - type: "null"
    /// "#).expect("list output form should parse");
    /// assert!(matches!(
    ///     &list.pipelines["app"].outputs[0],
    ///     OutputConfigV2::Stdout(_)
    /// ));
    /// assert!(matches!(
    ///     &list.pipelines["app"].outputs[1],
    ///     OutputConfigV2::Null(_)
    /// ));
    /// ```
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub outputs: Vec<OutputConfigV2>,
    #[serde(default)]
    pub enrichment: Vec<EnrichmentConfig>,
    #[serde(default, deserialize_with = "deserialize_string_map_strict_values")]
    pub resource_attrs: HashMap<String, String>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub workers: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_target_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_timeout_ms: Option<PositiveMillis>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub poll_interval_ms: Option<PositiveMillis>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub diagnostics: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub log_level: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub metrics_endpoint: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub metrics_interval_secs: Option<PositiveSecs>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub traces_endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub data_dir: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub pipelines: HashMap<String, PipelineConfig>,
    pub server: ServerConfig,
    pub storage: StorageConfig,
}
