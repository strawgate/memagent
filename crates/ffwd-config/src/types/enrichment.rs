use crate::serde_helpers::{
    PositiveSecs, deserialize_option_from_string_or_value, deserialize_strict_string,
    deserialize_string_map_strict_values,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

/// Supported on-disk GeoIP database formats.
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

/// GeoIP database enrichment source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeoDatabaseConfig {
    pub format: GeoDatabaseFormat,
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub path: String,
    #[serde(default, deserialize_with = "deserialize_option_from_string_or_value")]
    pub refresh_interval: Option<PositiveSecs>,
}

/// Static labels exposed as a one-row enrichment table.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    #[serde(deserialize_with = "deserialize_string_map_strict_values")]
    pub labels: HashMap<String, String>,
}

/// Controls the column-naming convention for host metadata enrichment.
///
/// `raw` (default) uses short internal names (`hostname`, `os_type`, etc.).
/// `ecs`/`beats` emits ECS `host.*` / `host.os.*` names.
/// `otel` emits OpenTelemetry semantic-convention names (`host.name`,
/// `os.type`, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HostInfoStyle {
    /// Short internal names: `hostname`, `os_type`, `os_arch`, etc.
    #[default]
    Raw,
    /// ECS / Beats field names: `host.hostname`, `host.os.type`, etc.
    #[serde(alias = "beats")]
    Ecs,
    /// OpenTelemetry semantic conventions: `host.name`, `os.type`, etc.
    Otel,
}

impl fmt::Display for HostInfoStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raw => f.write_str("raw"),
            Self::Ecs => f.write_str("ecs"),
            Self::Otel => f.write_str("otel"),
        }
    }
}

/// Host metadata enrichment with built-in fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInfoConfig {
    /// Column-naming convention for the host metadata enrichment table.
    #[serde(default)]
    pub style: HostInfoStyle,
}

/// Kubernetes pod metadata parsed from container log paths.
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

/// CSV-backed enrichment table loaded from disk.
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

/// JSON Lines-backed enrichment table loaded from disk.
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
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvVarsEnrichmentConfig {
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub table_name: String,
    /// Environment variable name prefix to filter on (e.g. `"FFWD_META_"`).
    #[serde(deserialize_with = "deserialize_strict_string")]
    pub prefix: String,
}

/// Agent self-metadata enrichment: `agent_name`, `agent_version`, `pid`, `start_time`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessInfoConfig {}

/// Parse a KEY=value properties file into a one-row enrichment table.
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

/// Tagged enrichment configuration for pipeline lookup tables.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
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
