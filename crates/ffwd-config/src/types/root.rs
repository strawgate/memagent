use crate::serde_helpers::{
    PositiveMillis, PositiveSecs, deserialize_option_from_string_or_value,
    deserialize_option_strict_string, deserialize_string_map_strict_values,
};
use serde::Deserialize;
use std::collections::HashMap;

use super::{EnrichmentConfig, InputConfig, OutputConfigV2};

#[derive(Debug, Clone, Deserialize)]
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
