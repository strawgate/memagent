use crate::env::expand_env_vars;
use crate::types::{
    Config, ConfigError, EnrichmentConfig, InputConfig, OutputConfig, PipelineConfig, ServerConfig,
    StorageConfig,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    input: Option<InputConfig>,
    transform: Option<String>,
    output: Option<OutputConfig>,
    #[serde(default)]
    enrichment: Vec<EnrichmentConfig>,
    #[serde(default)]
    resource_attrs: HashMap<String, String>,
    pipelines: Option<HashMap<String, PipelineConfig>>,
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

impl Config {
    /// Load configuration from a YAML file path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        Self::load_str(&raw)
    }

    /// Parse configuration from a YAML string.
    pub fn load_str(yaml: &str) -> Result<Self, ConfigError> {
        let expanded = expand_env_vars(yaml)?;
        let raw: RawConfig = serde_yaml_ng::from_str(&expanded)?;
        Self::from_raw(raw)
    }

    /// Expand `${VAR}` environment variables in raw YAML without parsing.
    pub fn expand_env_str(yaml: &str) -> Result<String, ConfigError> {
        expand_env_vars(yaml)
    }

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
                if raw.transform.is_some() {
                    return Err(ConfigError::Validation(
                        "top-level `transform` cannot be used with `pipelines:`; \
                         move transform SQL into each pipeline"
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
                    poll_interval_ms: None,
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
}
