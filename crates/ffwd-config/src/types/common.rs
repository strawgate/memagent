use crate::serde_helpers::{
    deserialize_option_strict_string, deserialize_string_map_strict_values,
};
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

/// Error returned while loading, deserializing, or validating configuration.
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

/// Canonical input type tags accepted in pipeline inputs.
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
    #[serde(rename = "linux_ebpf_sensor")]
    LinuxEbpfSensor,
    /// macOS EndpointSecurity sensor input.
    #[serde(rename = "macos_es_sensor")]
    MacosEsSensor,
    /// Windows eBPF sensor input.
    #[serde(rename = "windows_ebpf_sensor")]
    WindowsEbpfSensor,
    #[serde(rename = "macos_log")]
    MacosLog,
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
            InputType::MacosLog => f.write_str("macos_log"),
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
/// internal `__source_id` handle. `ecs` emits ECS-style `file.path`. `otel`
/// emits `log.file.path`. `vector` emits `file`. Values serialize and parse as
/// `snake_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourceMetadataStyle {
    /// Do not attach source metadata columns.
    #[default]
    None,
    /// Attach FastForward internal metadata (`__source_id`).
    Fastforward,
    /// Attach ECS-style public metadata columns.
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
#[non_exhaustive]
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

/// Canonical output type tags accepted in pipeline outputs.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OutputType {
    #[default]
    Otlp,
    Http,
    Elasticsearch,
    Loki,
    Stdout,
    File,
    Null,
    Tcp,
    Udp,
    ArrowIpc,
}

impl OutputType {
    /// Return whether this output type requires an explicit `endpoint`.
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
            OutputType::Null => f.write_str("null"),
            OutputType::Tcp => f.write_str("tcp"),
            OutputType::Udp => f.write_str("udp"),
            OutputType::ArrowIpc => f.write_str("arrow_ipc"),
        }
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

/// Record serialization formats accepted by inputs and outputs.
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

/// HTTP methods accepted by HTTP input/output configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum HttpMethodConfig {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}
