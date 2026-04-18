use crate::env::expand_env_vars;
use crate::types::{
    Config, ConfigError, EnrichmentConfig, InputConfig, OutputConfig, PipelineConfig, ServerConfig,
    StorageConfig,
};
use serde::Deserialize;
use serde_yaml_ng::Value;
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
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)?;
        Self::load_str_with_base_path(&raw, path.parent())
    }

    /// Parse configuration from a YAML string.
    pub fn load_str(yaml: &str) -> Result<Self, ConfigError> {
        Self::load_str_with_base_path(yaml, None)
    }

    /// Parse configuration from YAML with a base path for relative-path validation.
    pub fn load_str_with_base_path(
        yaml: &str,
        base_path: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let (marked_yaml, quoted_placeholders) = mark_quoted_exact_env_placeholders(yaml);
        let mut value: Value = serde_yaml_ng::from_str(&marked_yaml)?;
        expand_env_vars_in_yaml_value(&mut value, &quoted_placeholders)?;
        let raw: RawConfig = serde_yaml_ng::from_value(value)?;
        Self::from_raw(raw, base_path)
    }

    /// Expand `${VAR}` environment variables in raw YAML without parsing.
    pub fn expand_env_str(yaml: &str) -> Result<String, ConfigError> {
        expand_env_vars(yaml)
    }

    fn from_raw(raw: RawConfig, base_path: Option<&Path>) -> Result<Self, ConfigError> {
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
        cfg.validate_with_base_path(base_path)?;
        Ok(cfg)
    }
}

fn expand_env_vars_in_yaml_value(
    value: &mut Value,
    quoted_placeholders: &HashMap<String, String>,
) -> Result<(), ConfigError> {
    match value {
        Value::String(text) => {
            if let Some(original) = quoted_placeholders.get(text.as_str()) {
                *text = expand_env_vars(original)?;
                return Ok(());
            }

            let original = text.clone();
            let expanded = expand_env_vars(&original)?;
            if is_exact_env_placeholder(&original) {
                *value = coerce_expanded_yaml_scalar(&expanded);
            } else {
                *text = expanded;
            }
        }
        Value::Sequence(items) => {
            for item in items {
                expand_env_vars_in_yaml_value(item, quoted_placeholders)?;
            }
        }
        Value::Mapping(map) => {
            let old = std::mem::take(map);
            for (mut key, mut val) in old {
                expand_env_vars_in_yaml_value(&mut key, quoted_placeholders)?;
                expand_env_vars_in_yaml_value(&mut val, quoted_placeholders)?;
                if map.insert(key, val).is_some() {
                    return Err(ConfigError::Validation(
                        "environment variable expansion produced duplicate YAML mapping key".into(),
                    ));
                }
            }
        }
        Value::Tagged(tagged) => {
            expand_env_vars_in_yaml_value(&mut tagged.value, quoted_placeholders)?;
        }
        _ => {}
    }

    Ok(())
}

fn is_exact_env_placeholder(text: &str) -> bool {
    let Some(name) = text
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
    else {
        return false;
    };
    !name.is_empty() && !name.contains("${") && !name.contains('}')
}

fn coerce_expanded_yaml_scalar(text: &str) -> Value {
    match serde_yaml_ng::from_str::<Value>(text) {
        Ok(value @ (Value::Null | Value::Bool(_) | Value::Number(_))) => value,
        _ => Value::String(text.to_owned()),
    }
}

fn mark_quoted_exact_env_placeholders(yaml: &str) -> (String, HashMap<String, String>) {
    let mut marked = String::with_capacity(yaml.len());
    let mut placeholders = HashMap::new();
    let mut cursor = 0usize;
    let mut chars = yaml.char_indices().peekable();

    while let Some((start, ch)) = chars.next() {
        let quote @ ('\'' | '"') = ch else {
            continue;
        };
        if !is_yaml_quoted_scalar_start(yaml, start) {
            continue;
        }

        let Some((end, text)) = scan_yaml_quoted_scalar(yaml, start, quote) else {
            continue;
        };
        while chars.peek().is_some_and(|(idx, _)| *idx < end) {
            chars.next();
        }

        marked.push_str(&yaml[cursor..start]);
        if is_exact_env_placeholder(&text) {
            let marker = format!("__LOGFWD_QUOTED_ENV_PLACEHOLDER_{}__", placeholders.len());
            marked.push(quote);
            marked.push_str(&marker);
            marked.push(quote);
            placeholders.insert(marker, text);
        } else {
            marked.push_str(&yaml[start..end]);
        }
        cursor = end;
    }
    marked.push_str(&yaml[cursor..]);

    (marked, placeholders)
}

fn is_yaml_quoted_scalar_start(yaml: &str, quote_start: usize) -> bool {
    let line_start = yaml[..quote_start]
        .rfind('\n')
        .map_or(0, |idx| idx.saturating_add(1));
    yaml[line_start..quote_start]
        .chars()
        .rev()
        .find(|ch| !ch.is_whitespace())
        .is_none_or(|ch| matches!(ch, ':' | '-' | ',' | '[' | '{' | '?'))
}

fn scan_yaml_quoted_scalar(yaml: &str, quote_start: usize, quote: char) -> Option<(usize, String)> {
    let mut text = String::new();
    let body_start = quote_start + quote.len_utf8();
    let mut chars = yaml[body_start..].char_indices().peekable();

    while let Some((rel_idx, ch)) = chars.next() {
        let idx = body_start + rel_idx;
        if quote == '\'' && ch == '\'' {
            if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                chars.next();
                text.push('\'');
                continue;
            }
            return Some((idx + ch.len_utf8(), text));
        }
        if quote == '"' && ch == '"' {
            return Some((idx + ch.len_utf8(), text));
        }
        if quote == '"' && ch == '\\' {
            if let Some((_, escaped)) = chars.next() {
                text.push('\\');
                text.push(escaped);
            }
            continue;
        }
        text.push(ch);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::mark_quoted_exact_env_placeholders;

    #[test]
    fn quoted_exact_env_placeholder_is_marked() {
        let (marked, placeholders) =
            mark_quoted_exact_env_placeholders(r#"path: "${LOGFWD_TEST_PATH}""#);

        assert_eq!(marked, r#"path: "__LOGFWD_QUOTED_ENV_PLACEHOLDER_0__""#);
        assert_eq!(
            placeholders.get("__LOGFWD_QUOTED_ENV_PLACEHOLDER_0__"),
            Some(&"${LOGFWD_TEST_PATH}".to_owned())
        );
    }

    #[test]
    fn escaped_double_quoted_env_placeholder_is_not_marked() {
        let yaml = r#"path: "\${LOGFWD_TEST_PATH}""#;

        let (marked, placeholders) = mark_quoted_exact_env_placeholders(yaml);

        assert_eq!(marked, yaml);
        assert!(placeholders.is_empty());
    }
}
