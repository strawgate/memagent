//! YAML configuration parser for logfwd.
//!
//! Supports two layout styles:
//!
//! - **Simple** (single pipeline): top-level `input`, `transform`, `output`.
//! - **Advanced** (multiple pipelines): top-level `pipelines` map.
//!
//! Environment variables in values are expanded using `${VAR}` syntax.

use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

// ---------------------------------------------------------------------------
// Public error type
// ---------------------------------------------------------------------------

/// Errors that can occur while loading or validating configuration.
#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Yaml(serde_yaml::Error),
    Validation(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "config I/O error: {e}"),
            ConfigError::Yaml(e) => write!(f, "config YAML error: {e}"),
            ConfigError::Validation(msg) => write!(f, "config validation error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        ConfigError::Io(e)
    }
}

impl From<serde_yaml::Error> for ConfigError {
    fn from(e: serde_yaml::Error) -> Self {
        ConfigError::Yaml(e)
    }
}

// ---------------------------------------------------------------------------
// Enums for known types / formats
// ---------------------------------------------------------------------------

/// Recognised input types.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputType {
    File,
    Udp,
    Tcp,
    Otlp,
}

/// Recognised output types.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputType {
    Otlp,
    Http,
    Elasticsearch,
    Loki,
    Stdout,
    FileOut,
    Parquet,
}

/// Recognised log formats.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Cri,
    Json,
    Logfmt,
    Syslog,
    Raw,
    Auto,
    /// Human-readable colored console output for debugging/testing.
    Console,
}

// ---------------------------------------------------------------------------
// Input / Output descriptors
// ---------------------------------------------------------------------------

/// A single input source.
#[derive(Debug, Clone, Deserialize)]
pub struct InputConfig {
    /// Optional friendly name (used in multi-input pipelines).
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub input_type: InputType,
    /// File glob or listen address, depending on `input_type`.
    pub path: Option<String>,
    pub listen: Option<String>,
    pub format: Option<Format>,
}

/// A single output destination.
#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub output_type: OutputType,
    pub endpoint: Option<String>,
    pub protocol: Option<String>,
    pub compression: Option<String>,
    pub format: Option<Format>,
    pub path: Option<String>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// One logical pipeline (inputs -> SQL transform -> outputs).
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub inputs: Vec<InputConfig>,
    pub transform: Option<String>,
    #[serde(default, deserialize_with = "deserialize_one_or_many")]
    pub outputs: Vec<OutputConfig>,
}

// ---------------------------------------------------------------------------
// Server / Storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerConfig {
    pub diagnostics: Option<String>,
    pub log_level: Option<String>,
    /// OTLP endpoint for metrics push (e.g. "http://localhost:4318").
    /// If not set, OTLP push is disabled.
    pub metrics_endpoint: Option<String>,
    /// OTLP push interval in seconds. Default: 60.
    pub metrics_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageConfig {
    pub data_dir: Option<String>,
}

// ---------------------------------------------------------------------------
// Top-level config (supports simple + advanced)
// ---------------------------------------------------------------------------

/// Raw top-level YAML — we use a flat struct with Options so serde can
/// deserialise either layout, then we normalise into [`Config`].
#[derive(Debug, Deserialize)]
struct RawConfig {
    // Simple form
    input: Option<InputConfig>,
    transform: Option<String>,
    output: Option<OutputConfig>,

    // Advanced form
    pipelines: Option<HashMap<String, PipelineConfig>>,

    // Shared
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

/// Fully resolved configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub pipelines: HashMap<String, PipelineConfig>,
    pub server: ServerConfig,
    pub storage: StorageConfig,
}

impl Config {
    /// Load configuration from a file path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        Self::load_str(&raw)
    }

    /// Load configuration from a YAML string (handy for tests).
    pub fn load_str(yaml: &str) -> Result<Self, ConfigError> {
        let expanded = expand_env_vars(yaml);
        let raw: RawConfig = serde_yaml::from_str(&expanded)?;
        Self::from_raw(raw)
    }

    // Normalise the two layout variants into a single representation.
    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let pipelines = match (raw.pipelines, raw.input, raw.output) {
            (Some(p), None, None) => p,
            (None, Some(input), Some(output)) => {
                let pipeline = PipelineConfig {
                    inputs: vec![input],
                    transform: raw.transform,
                    outputs: vec![output],
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

    /// Validate the loaded configuration.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.pipelines.is_empty() {
            return Err(ConfigError::Validation(
                "at least one pipeline must be defined".into(),
            ));
        }

        for (name, pipe) in &self.pipelines {
            if pipe.inputs.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}' has no inputs"
                )));
            }
            if pipe.outputs.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}' has no outputs"
                )));
            }

            for (i, input) in pipe.inputs.iter().enumerate() {
                let label = input
                    .name
                    .as_deref()
                    .map(String::from)
                    .unwrap_or_else(|| format!("#{i}"));
                match input.input_type {
                    InputType::File => {
                        if input.path.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': file input requires 'path'"
                            )));
                        }
                    }
                    InputType::Udp | InputType::Tcp => {
                        if input.listen.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': udp/tcp input requires 'listen'"
                            )));
                        }
                    }
                    InputType::Otlp => {}
                }
            }

            for (i, output) in pipe.outputs.iter().enumerate() {
                let label = output
                    .name
                    .as_deref()
                    .map(String::from)
                    .unwrap_or_else(|| format!("#{i}"));
                match output.output_type {
                    OutputType::Otlp
                    | OutputType::Http
                    | OutputType::Elasticsearch
                    | OutputType::Loki => {
                        if output.endpoint.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'endpoint'",
                                output_type_name(&output.output_type),
                            )));
                        }
                    }
                    OutputType::FileOut | OutputType::Parquet => {
                        if output.path.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'path'",
                                output_type_name(&output.output_type),
                            )));
                        }
                    }
                    OutputType::Stdout => {}
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn output_type_name(t: &OutputType) -> &'static str {
    match t {
        OutputType::Otlp => "otlp",
        OutputType::Http => "http",
        OutputType::Elasticsearch => "elasticsearch",
        OutputType::Loki => "loki",
        OutputType::Stdout => "stdout",
        OutputType::FileOut => "file_out",
        OutputType::Parquet => "parquet",
    }
}

/// Expand `${VAR}` references in `text` using the process environment.
fn expand_env_vars(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    // Leave the placeholder intact if the var is not set.
                    result.push_str("${");
                    result.push_str(&var_name);
                    result.push('}');
                }
            }
        } else {
            result.push(ch);
        }
    }

    result
}

/// Serde helper: accept either a single `T` or a `Vec<T>`.
fn deserialize_one_or_many<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        Many(Vec<T>),
        One(T),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::Many(v) => Ok(v),
        OneOrMany::One(v) => Ok(vec![v]),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_config() {
        let yaml = r#"
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri

transform: |
  SELECT * FROM logs WHERE level != 'DEBUG'

output:
  type: otlp
  endpoint: otel-collector:4317
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
  log_level: info

storage:
  data_dir: /var/lib/logfwd
"#;
        let cfg = Config::load_str(yaml).expect("should parse simple config");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["default"];
        assert_eq!(pipe.inputs.len(), 1);
        assert_eq!(pipe.inputs[0].input_type, InputType::File);
        assert_eq!(
            pipe.inputs[0].path.as_deref(),
            Some("/var/log/pods/**/*.log")
        );
        assert_eq!(pipe.inputs[0].format, Some(Format::Cri));
        assert!(pipe.transform.as_ref().unwrap().contains("SELECT"));
        assert_eq!(pipe.outputs[0].output_type, OutputType::Otlp);
        assert_eq!(
            pipe.outputs[0].endpoint.as_deref(),
            Some("otel-collector:4317")
        );
        assert_eq!(cfg.server.diagnostics.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(cfg.storage.data_dir.as_deref(), Some("/var/lib/logfwd"));
    }

    #[test]
    fn advanced_config() {
        let yaml = r#"
pipelines:
  app_logs:
    inputs:
      - name: pod_logs
        type: file
        path: /var/log/pods/**/*.log
        format: cri
      - name: syslog_in
        type: udp
        listen: 0.0.0.0:514
        format: syslog
    transform: |
      SELECT * EXCEPT (stack_trace) FROM logs WHERE level != 'DEBUG'
    outputs:
      - name: collector
        type: otlp
        endpoint: otel-collector:4317
        protocol: grpc
        compression: zstd
      - name: debug
        type: stdout
        format: json

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
"#;
        let cfg = Config::load_str(yaml).expect("should parse advanced config");
        assert_eq!(cfg.pipelines.len(), 1);
        let pipe = &cfg.pipelines["app_logs"];
        assert_eq!(pipe.inputs.len(), 2);
        assert_eq!(pipe.inputs[0].input_type, InputType::File);
        assert_eq!(pipe.inputs[1].input_type, InputType::Udp);
        assert_eq!(pipe.inputs[1].listen.as_deref(), Some("0.0.0.0:514"));
        assert_eq!(pipe.outputs.len(), 2);
        assert_eq!(pipe.outputs[0].output_type, OutputType::Otlp);
        assert_eq!(pipe.outputs[1].output_type, OutputType::Stdout);
    }

    #[test]
    fn env_var_substitution() {
        // SAFETY: this test is not run concurrently with other tests that
        // depend on the same environment variable.
        unsafe { std::env::set_var("LOGFWD_TEST_ENDPOINT", "my-collector:4317") };
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_TEST_ENDPOINT}
"#;
        let cfg = Config::load_str(yaml).expect("env var substitution");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.outputs[0].endpoint.as_deref(),
            Some("my-collector:4317")
        );
        unsafe { std::env::remove_var("LOGFWD_TEST_ENDPOINT") };
    }

    #[test]
    fn unset_env_var_preserved() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_NONEXISTENT_VAR_12345}
"#;
        let cfg = Config::load_str(yaml).expect("unset env preserved");
        let pipe = &cfg.pipelines["default"];
        assert_eq!(
            pipe.outputs[0].endpoint.as_deref(),
            Some("${LOGFWD_NONEXISTENT_VAR_12345}")
        );
    }

    #[test]
    fn validation_missing_input_path() {
        let yaml = r#"
input:
  type: file
output:
  type: stdout
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("path"), "expected 'path' in error: {msg}");
    }

    #[test]
    fn validation_missing_output_endpoint() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("endpoint"),
            "expected 'endpoint' in error: {msg}"
        );
    }

    #[test]
    fn validation_udp_requires_listen() {
        let yaml = r#"
input:
  type: udp
output:
  type: stdout
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("listen"), "expected 'listen' in error: {msg}");
    }

    #[test]
    fn validation_mixed_simple_and_pipelines() {
        let yaml = r#"
input:
  type: file
  path: /tmp/x.log
output:
  type: stdout
pipelines:
  extra:
    inputs:
      - type: file
        path: /tmp/y.log
    outputs:
      - type: stdout
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("mix"), "expected 'mix' in error: {msg}");
    }

    #[test]
    fn validation_no_pipelines() {
        let yaml = r#"
server:
  log_level: info
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("must define"),
            "expected 'must define' in error: {msg}"
        );
    }

    #[test]
    fn file_out_requires_path() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: file_out
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("path"), "expected 'path' in error: {msg}");
    }

    #[test]
    fn all_output_types() {
        for (otype, extra) in [
            ("otlp", "endpoint: x:4317"),
            ("http", "endpoint: http://x"),
            ("elasticsearch", "endpoint: http://x"),
            ("loki", "endpoint: http://x"),
            ("stdout", ""),
            ("file_out", "path: /tmp/out.log"),
            ("parquet", "path: /tmp/out.parquet"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  {extra}\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {otype}: {e}"));
        }
    }

    #[test]
    fn all_input_types() {
        for (itype, extra) in [
            ("file", "path: /tmp/x.log"),
            ("udp", "listen: 0.0.0.0:514"),
            ("tcp", "listen: 0.0.0.0:514"),
            ("otlp", ""),
        ] {
            let yaml = format!("input:\n  type: {itype}\n  {extra}\noutput:\n  type: stdout\n");
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {itype}: {e}"));
        }
    }
}
