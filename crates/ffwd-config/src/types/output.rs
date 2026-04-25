use crate::serde_helpers::{
    PositiveMillis, PositiveSecs, deserialize_option_from_string_or_value,
    deserialize_option_strict_string, deserialize_option_string_map_strict_values,
    deserialize_option_vec_strict_string,
};
use crate::shared::{BatchConfig, NetworkConfig, RetryConfig, RotationConfig, TlsClientConfig};
use serde::Deserialize;
use std::collections::HashMap;

use super::common::{
    AuthConfig, CompressionFormat, ElasticsearchRequestMode, Format, OtlpProtocol, OutputType,
};

/// Tagged output configuration for a pipeline sink.
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
    Null(NullOutputConfig),
    Tcp(TcpOutputConfig),
    Udp(UdpOutputConfig),
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
            OutputConfigV2::Null(config) => config.name.as_deref(),
            OutputConfigV2::Tcp(config) => config.name.as_deref(),
            OutputConfigV2::Udp(config) => config.name.as_deref(),
            OutputConfigV2::ArrowIpc(config) => config.name.as_deref(),
        }
    }

    /// Return the endpoint for output variants that connect to a destination.
    pub fn endpoint(&self) -> Option<&str> {
        match self {
            OutputConfigV2::Otlp(config) => config.endpoint.as_deref(),
            OutputConfigV2::Http(config) => config.endpoint.as_deref(),
            OutputConfigV2::Elasticsearch(config) => config.endpoint.as_deref(),
            OutputConfigV2::Loki(config) => config.endpoint.as_deref(),
            OutputConfigV2::Tcp(config) => config.endpoint.as_deref(),
            OutputConfigV2::Udp(config) => config.endpoint.as_deref(),
            OutputConfigV2::ArrowIpc(config) => config.endpoint.as_deref(),
            OutputConfigV2::Stdout(_) | OutputConfigV2::File(_) | OutputConfigV2::Null(_) => None,
        }
    }

    /// Return authentication settings for output variants that support them.
    pub fn auth(&self) -> Option<&AuthConfig> {
        match self {
            OutputConfigV2::Otlp(config) => config.auth.as_ref(),
            OutputConfigV2::Http(config) => config.auth.as_ref(),
            OutputConfigV2::Elasticsearch(config) => config.auth.as_ref(),
            OutputConfigV2::Loki(config) => config.auth.as_ref(),
            OutputConfigV2::ArrowIpc(config) => config.auth.as_ref(),
            OutputConfigV2::Stdout(_)
            | OutputConfigV2::File(_)
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
            OutputConfigV2::Null(_) => OutputType::Null,
            OutputConfigV2::Tcp(_) => OutputType::Tcp,
            OutputConfigV2::Udp(_) => OutputType::Udp,
            OutputConfigV2::ArrowIpc(_) => OutputType::ArrowIpc,
        }
    }
}

/// OTLP output configuration.
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

/// Generic HTTP JSON-lines output configuration.
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

/// Elasticsearch bulk output configuration.
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
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub request_timeout_ms: Option<PositiveMillis>,
}

/// Grafana Loki output configuration.
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
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub request_timeout_ms: Option<PositiveMillis>,
}

/// Standard output sink configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StdoutOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    pub format: Option<Format>,
}

/// Local file output configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct FileOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub path: Option<String>,
    #[serde(default)]
    pub compression: Option<CompressionFormat>,
    #[serde(default)]
    pub rotation: Option<RotationConfig>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub delimiter: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub path_template: Option<String>,
    pub format: Option<Format>,
}

/// Null output configuration for intentionally dropping records.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NullOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
}

/// TCP output configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TcpOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    pub encoding: Option<Format>,
    pub framing: Option<Format>,
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
    #[serde(default)]
    pub keepalive: Option<NetworkConfig>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub timeout_secs: Option<PositiveSecs>,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

/// UDP output configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct UdpOutputConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub endpoint: Option<String>,
    pub encoding: Option<Format>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub max_datagram_size_bytes: Option<usize>,
}

/// Arrow IPC HTTP output configuration.
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
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub buffer_size_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub batch_size: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub write_schema_on_connect: Option<bool>,
}
