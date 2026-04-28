use crate::serde_helpers::{
    PositiveMillis, PositiveSecs, deserialize_from_string_or_value,
    deserialize_option_from_string_or_value, deserialize_option_strict_string,
    deserialize_option_vec_strict_string, deserialize_strict_string, deserialize_vec_strict_string,
};
use crate::shared::{MultilineConfig, TlsServerConfig};
use serde::Deserialize;
use std::collections::HashMap;

use super::common::{
    Format, HttpMethodConfig, InputType, OtlpProtobufDecodeModeConfig, SourceMetadataStyle,
};

/// Built-in generator profile used by the synthetic input.
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

/// Complexity level for generated synthetic records.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorComplexityConfig {
    #[default]
    Simple,
    Complex,
}

/// Scalar YAML value accepted for generator static attributes.
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

/// Monotonic numeric field emitted by the generator.
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
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

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GeneratorInputConfig {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
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
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Deserialize)]
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
    #[serde(rename = "linux_ebpf_sensor")]
    LinuxEbpfSensor(SensorTypeConfig),
    #[serde(rename = "macos_es_sensor")]
    MacosEsSensor(SensorTypeConfig),
    #[serde(rename = "windows_ebpf_sensor")]
    WindowsEbpfSensor(SensorTypeConfig),
    #[serde(rename = "macos_log")]
    MacosLog(MacosLogTypeConfig),
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
            Self::MacosLog(_) => InputType::MacosLog,
            Self::ArrowIpc(_) => InputType::ArrowIpc,
            Self::Journald(_) => InputType::Journald,
            Self::HostMetrics(_) => InputType::HostMetrics,
            Self::S3(_) => InputType::S3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub start_at: Option<String>,
    pub encoding: Option<Format>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub follow_symlinks: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub ignore_older_secs: Option<PositiveSecs>,
    #[serde(default)]
    pub multiline: Option<MultilineConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_line_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_message_size_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub so_rcvbuf: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StdinTypeConfig {}

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GeneratorTypeConfig {
    #[serde(default)]
    pub generator: Option<GeneratorInputConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SensorTypeConfig {
    #[serde(default)]
    pub sensor: Option<HostMetricsInputConfig>,
}

/// Tagged-union wrapper for macOS OSLog input configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacosLogTypeConfig {
    #[serde(default)]
    pub macos_log: Option<MacosLogInputConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MacosLogInputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub level: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub subsystem: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub process: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcTypeConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub listen: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_connections: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_message_size_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JournaldTypeConfig {
    #[serde(default)]
    pub journald: Option<JournaldInputConfig>,
}

/// Tagged-union wrapper for S3 input configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3TypeConfig {
    /// S3-specific input settings (bucket, region, credentials, tuning).
    pub s3: S3InputConfig,
}

/// Compression override for S3 object reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum S3CompressionConfig {
    /// Infer compression from object metadata and key suffix.
    Auto,
    /// Force gzip decompression.
    Gzip,
    /// Force zstd decompression.
    Zstd,
    /// Force snappy decompression.
    Snappy,
    /// Treat objects as uncompressed.
    None,
}

/// Configuration for the S3 (and S3-compatible) object storage input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    /// Compression override. Default: `auto` (detect from key extension or
    /// Content-Encoding).
    #[serde(default)]
    pub compression: Option<S3CompressionConfig>,
    /// Polling interval for `ListObjectsV2` mode in milliseconds. Default: 5000.
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub poll_interval_ms: Option<PositiveMillis>,
}
