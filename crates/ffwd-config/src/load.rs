use crate::env::expand_env_vars;
use crate::types::{
    Config, ConfigError, InputTypeConfig, PipelineConfig, ServerConfig, StorageConfig,
};
use config as config_rs;
use serde::Deserialize;
use serde_yaml_ng::Value;
use serde_yaml_ng::value::Tag;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    pipelines: Option<HashMap<String, PipelineConfig>>,
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

impl Config {
    /// Load configuration from a YAML file path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)?;
        Self::load_str_with_base_path(&raw, path.parent())
    }

    /// Parse configuration from a YAML string.
    pub fn load_str(yaml: impl AsRef<str>) -> Result<Self, ConfigError> {
        Self::load_str_with_base_path(yaml.as_ref(), None)
    }

    /// Parse configuration from YAML with a base path for relative-path validation.
    pub fn load_str_with_base_path(
        yaml: &str,
        base_path: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let mut value: Value = serde_yaml_ng::from_str(yaml)?;
        expand_env_vars_in_yaml_value(&mut value)?;
        let raw = deserialize_raw_config_with_path(value)?;
        Self::from_raw(raw, base_path)
    }

    /// Expand `${VAR}` environment variables in raw YAML without parsing.
    pub fn expand_env_str(yaml: &str) -> Result<String, ConfigError> {
        expand_env_vars(yaml)
    }

    /// Expand `${VAR}` environment variables inside a parsed YAML tree, then
    /// serialize the expanded tree. Expanded environment values remain strings.
    pub fn expand_env_yaml_str(yaml: &str) -> Result<String, ConfigError> {
        let mut value: Value = serde_yaml_ng::from_str(yaml)?;
        expand_env_vars_in_yaml_value(&mut value)?;
        serde_yaml_ng::to_string(&value).map_err(ConfigError::from)
    }

    fn from_raw(raw: RawConfig, base_path: Option<&Path>) -> Result<Self, ConfigError> {
        let pipelines = raw.pipelines.ok_or_else(|| {
            ConfigError::Validation("config must define top-level `pipelines`".into())
        })?;

        let mut cfg = Config {
            pipelines,
            server: raw.server,
            storage: raw.storage,
        };
        cfg.normalize();
        cfg.validate_with_base_path(base_path)?;
        Ok(cfg)
    }

    /// Trim whitespace from user-supplied list values so that runtime
    /// comparison (exact equality) matches what validation accepted.
    fn normalize(&mut self) {
        for pipeline in self.pipelines.values_mut() {
            for input in &mut pipeline.inputs {
                let sensor_cfg = match &mut input.type_config {
                    InputTypeConfig::LinuxEbpfSensor(s)
                    | InputTypeConfig::MacosEsSensor(s)
                    | InputTypeConfig::WindowsEbpfSensor(s)
                    | InputTypeConfig::HostMetrics(s) => s.sensor.as_mut(),
                    _ => None,
                };
                if let Some(cfg) = sensor_cfg {
                    trim_string_list(&mut cfg.include_event_types);
                    trim_string_list(&mut cfg.exclude_event_types);
                }
            }
        }
    }
}

fn trim_string_list(list: &mut Option<Vec<String>>) {
    if let Some(values) = list {
        for value in values {
            let trimmed = value.trim();
            if trimmed.len() != value.len() {
                *value = trimmed.to_owned();
            }
        }
    }
}

fn deserialize_raw_config_with_path(value: Value) -> Result<RawConfig, ConfigError> {
    config_rs::Value::new(
        None,
        config_rs::ValueKind::Table(yaml_value_to_config_root(value)?),
    )
    .try_deserialize()
    .map_err(config_deserialization_error)
}

fn config_deserialization_error(err: config_rs::ConfigError) -> ConfigError {
    ConfigError::Validation(format!("config deserialization error: {err}"))
}

fn yaml_value_to_config_root(
    value: Value,
) -> Result<config_rs::Map<String, config_rs::Value>, ConfigError> {
    match yaml_value_to_config_value(value)?.kind {
        config_rs::ValueKind::Nil => Ok(config_rs::Map::new()),
        config_rs::ValueKind::Table(map) => Ok(map),
        _ => Err(ConfigError::Validation(
            "config root must be a YAML mapping".into(),
        )),
    }
}

fn yaml_value_to_config_value(value: Value) -> Result<config_rs::Value, ConfigError> {
    let kind = match value {
        Value::Null => config_rs::ValueKind::Nil,
        Value::Bool(value) => config_rs::ValueKind::Boolean(value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                config_rs::ValueKind::I64(value)
            } else if let Some(value) = value.as_u64() {
                config_rs::ValueKind::U64(value)
            } else if let Some(value) = value.as_f64() {
                config_rs::ValueKind::Float(value)
            } else {
                return Err(ConfigError::Validation(
                    "unsupported YAML numeric value in config".into(),
                ));
            }
        }
        Value::String(value) => config_rs::ValueKind::String(value),
        Value::Sequence(values) => config_rs::ValueKind::Array(
            values
                .into_iter()
                .map(yaml_value_to_config_value)
                .collect::<Result<_, _>>()?,
        ),
        Value::Mapping(values) => {
            let mut map = config_rs::Map::new();
            for (key, value) in values {
                let key = yaml_key_to_config_key(key)?;
                let value = yaml_value_to_config_value(value)?;
                if map.insert(key, value).is_some() {
                    return Err(ConfigError::Validation(
                        "environment variable expansion produced duplicate YAML mapping key".into(),
                    ));
                }
            }
            config_rs::ValueKind::Table(map)
        }
        Value::Tagged(tagged) => {
            if is_yaml_string_tag(&tagged.tag) {
                return yaml_value_to_config_value(tagged.value);
            }
            return Err(unsupported_yaml_tag_error(&tagged.tag));
        }
    };

    Ok(config_rs::Value::new(None, kind))
}

fn yaml_key_to_config_key(key: Value) -> Result<String, ConfigError> {
    match key {
        Value::Null => Ok("null".to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value),
        Value::Tagged(tagged) => {
            if is_yaml_string_tag(&tagged.tag) {
                yaml_key_to_config_key(tagged.value)
            } else {
                Err(unsupported_yaml_tag_error(&tagged.tag))
            }
        }
        Value::Sequence(_) | Value::Mapping(_) => Err(ConfigError::Validation(
            "YAML mapping keys must be scalar values".into(),
        )),
    }
}

fn expand_env_vars_in_yaml_value(value: &mut Value) -> Result<(), ConfigError> {
    match value {
        Value::String(text) => {
            *text = expand_env_vars(text)?;
        }
        Value::Sequence(items) => {
            for item in items {
                expand_env_vars_in_yaml_value(item)?;
            }
        }
        Value::Mapping(map) => {
            let old = std::mem::take(map);
            for (mut key, mut val) in old {
                expand_env_vars_in_yaml_value(&mut key)?;
                expand_env_vars_in_yaml_value(&mut val)?;
                if map.insert(key, val).is_some() {
                    return Err(ConfigError::Validation(
                        "environment variable expansion produced duplicate YAML mapping key".into(),
                    ));
                }
            }
        }
        Value::Tagged(tagged) => {
            if is_yaml_string_tag(&tagged.tag) {
                // An explicit string tag (!!str or !str) means the user
                // wants a string value. Env substitution already produces
                // string data, so unwrap the tag after expansion.
                if let Value::String(text) = &mut tagged.value {
                    let expanded = expand_env_vars(text)?;
                    *value = Value::String(expanded);
                    return Ok(());
                }
            }
            expand_env_vars_in_yaml_value(&mut tagged.value)?;
        }
        _ => {}
    }

    Ok(())
}

fn is_yaml_string_tag(tag: &Tag) -> bool {
    tag == "str" || tag == "tag:yaml.org,2002:str"
}

fn unsupported_yaml_tag_error(tag: &Tag) -> ConfigError {
    ConfigError::Validation(format!("unsupported explicit YAML tag in config: {tag}"))
}
