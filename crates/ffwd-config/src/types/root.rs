use crate::serde_helpers::{
    PositiveMillis, PositiveSecs, deserialize_option_from_string_or_value,
    deserialize_option_strict_string, deserialize_string_map_strict_values,
};
use serde::Deserialize;
use std::collections::HashMap;

use super::{EnrichmentConfig, InputConfig, OutputConfigV2};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default)]
    pub inputs: Vec<InputConfig>,
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub transform: Option<String>,
    /// Output sinks for this pipeline.
    #[serde(default)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default, deserialize_with = "deserialize_option_strict_string")]
    pub data_dir: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub pipelines: HashMap<String, PipelineConfig>,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub opamp: Option<OpampConfig>,
}

/// OpAMP connection configuration for central management.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpampConfig {
    /// OpAMP server endpoint (e.g., `http://localhost:4320/v1/opamp`).
    pub endpoint: String,
    /// Optional API key for authentication.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Instance UID — set to a fixed UUID, or "auto" (default) to generate and persist.
    #[serde(default = "default_instance_uid")]
    pub instance_uid: String,
    /// Service name reported to the OpAMP server.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Polling interval in seconds (default: 30).
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Whether to accept remote configuration from the server (default: true).
    #[serde(default = "default_accept_remote_config")]
    pub accept_remote_config: bool,
}

impl Default for OpampConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            api_key: None,
            instance_uid: default_instance_uid(),
            service_name: default_service_name(),
            poll_interval_secs: default_poll_interval_secs(),
            accept_remote_config: default_accept_remote_config(),
        }
    }
}

fn default_instance_uid() -> String {
    "auto".to_string()
}

fn default_service_name() -> String {
    "ffwd".to_string()
}

fn default_poll_interval_secs() -> u64 {
    30
}

fn default_accept_remote_config() -> bool {
    true
}
