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
// Authentication configuration
// ---------------------------------------------------------------------------

/// Authentication configuration for output HTTP sinks.
///
/// Supports bearer tokens and arbitrary key/value header pairs.
/// All values support `${ENV_VAR}` expansion at config-load time.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    /// Sets the `Authorization: Bearer <token>` header on every request.
    pub bearer_token: Option<String>,
    /// Additional HTTP headers to add to every request (e.g. `X-API-Key`).
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

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
#[non_exhaustive]
pub enum InputType {
    File,
    Udp,
    Tcp,
    Otlp,
}

/// Recognised output types.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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
#[non_exhaustive]
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
    /// Maximum number of file descriptors to keep open simultaneously.
    /// Applies only to `file` inputs. Defaults to 1024 when not set.
    pub max_open_files: Option<usize>,
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
    /// Optional authentication for HTTP-based outputs.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Enrichment
// ---------------------------------------------------------------------------

/// Supported geo-IP database formats.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GeoDatabaseFormat {
    /// MaxMind MMDB format (GeoLite2-City, GeoIP2-City, DB-IP MMDB).
    Mmdb,
}

/// Configuration for a geo-IP database used by the `geo_lookup()` UDF.
///
/// ```yaml
/// enrichment:
///   - type: geo_database
///     format: mmdb
///     path: /etc/logfwd/GeoLite2-City.mmdb
///     refresh_interval: 86400
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct GeoDatabaseConfig {
    /// Database format.
    pub format: GeoDatabaseFormat,
    /// Path to the database file.
    pub path: String,
    /// How often to reload the database file, in seconds. Optional.
    // TODO: not yet implemented — currently ignored at runtime
    pub refresh_interval: Option<u64>,
}

/// Enrichment configuration entry.
///
/// Each entry in the `enrichment` list specifies one enrichment source.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnrichmentConfig {
    /// Geo-IP lookup database for the `geo_lookup()` UDF.
    GeoDatabase(GeoDatabaseConfig),
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
    /// Enrichment sources (e.g. geo-IP databases).
    #[serde(default)]
    pub enrichment: Vec<EnrichmentConfig>,
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
                    enrichment: Vec::new(),
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
                    .map_or_else(|| format!("#{i}"), String::from);
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
                    .map_or_else(|| format!("#{i}"), String::from);

                // Reject placeholder output types that are not yet implemented.
                match output.output_type {
                    OutputType::Elasticsearch | OutputType::Loki | OutputType::Parquet => {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': {} output type is not yet implemented",
                            output_type_name(&output.output_type),
                        )));
                    }
                    _ => {}
                }

                match output.output_type {
                    OutputType::Otlp | OutputType::Http => {
                        if output.endpoint.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'endpoint'",
                                output_type_name(&output.output_type),
                            )));
                        }
                        if let Some(ep) = &output.endpoint
                            && let Err(msg) = validate_endpoint_url(ep)
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {msg}",
                            )));
                        }
                    }
                    OutputType::FileOut => {
                        if output.path.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'path'",
                                output_type_name(&output.output_type),
                            )));
                        }
                    }
                    OutputType::Stdout => {}
                    // Elasticsearch, Loki, Parquet are already rejected above.
                    OutputType::Elasticsearch | OutputType::Loki | OutputType::Parquet => {
                        unreachable!("placeholder types are rejected before this match")
                    }
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

/// Validate that an endpoint URL has a recognised scheme and a non-empty host.
///
/// Accepts `http://` or `https://` followed by at least one character.
/// Values that still contain unexpanded `${VAR}` placeholders are skipped
/// so that the `expand_env_vars` behaviour is not broken.
fn validate_endpoint_url(endpoint: &str) -> Result<(), String> {
    // Defer validation for values that still contain unexpanded env var placeholders.
    if endpoint.contains("${") {
        return Ok(());
    }
    let rest = if let Some(r) = endpoint.strip_prefix("https://") {
        r
    } else if let Some(r) = endpoint.strip_prefix("http://") {
        r
    } else {
        return Err(format!(
            "endpoint '{endpoint}' has no recognised scheme; expected 'http://' or 'https://'"
        ));
    };
    if rest.is_empty() {
        return Err(format!(
            "endpoint '{endpoint}' has no host after the scheme"
        ));
    }
    Ok(())
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
  endpoint: http://otel-collector:4317
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
            Some("http://otel-collector:4317")
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
        endpoint: http://otel-collector:4317
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
        unsafe { std::env::set_var("LOGFWD_TEST_ENDPOINT", "http://my-collector:4317") };
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
            Some("http://my-collector:4317")
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
    fn validation_unimplemented_output_type() {
        // Each placeholder type should be caught by Config::validate() before
        // pipeline construction, not silently accepted.
        for otype in ["elasticsearch", "loki", "parquet"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: http://x\n  path: /tmp/x\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "validation should reject unimplemented type '{otype}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "error message should mention 'not yet implemented' for '{otype}': {msg}"
            );
            assert!(
                msg.contains(otype),
                "error message should include the type name '{otype}': {msg}"
            );
        }
    }

    #[test]
    fn all_output_types() {
        // Implemented output types should parse and validate successfully.
        for (otype, extra) in [
            ("otlp", "endpoint: http://x:4317"),
            ("http", "endpoint: http://x"),
            ("stdout", ""),
            ("file_out", "path: /tmp/out.log"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  {extra}\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| panic!("failed for {otype}: {e}"));
        }

        // Placeholder output types must be rejected at validation time.
        for otype in ["elasticsearch", "loki", "parquet"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: http://x\n  path: /tmp/x\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "expected error for unimplemented type {otype}"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("not yet implemented"),
                "expected 'not yet implemented' for {otype}: {msg}"
            );
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

    #[test]
    fn auth_bearer_token() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  auth:
    bearer_token: "my-secret-token"
"#;
        let cfg = Config::load_str(yaml).expect("auth bearer_token");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("my-secret-token"));
        assert!(auth.headers.is_empty());
    }

    #[test]
    fn auth_custom_headers() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  auth:
    headers:
      X-API-Key: "supersecret"
      X-Tenant: "acme"
"#;
        let cfg = Config::load_str(yaml).expect("auth custom headers");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token, None);
        assert_eq!(
            auth.headers.get("X-API-Key").map(|s| s.as_str()),
            Some("supersecret")
        );
        assert_eq!(
            auth.headers.get("X-Tenant").map(|s| s.as_str()),
            Some("acme")
        );
    }

    #[test]
    fn auth_env_var_bearer_token() {
        // SAFETY: test is not run concurrently with other tests that modify this var.
        unsafe { std::env::set_var("LOGFWD_TEST_TOKEN", "env-bearer-token") };
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
  auth:
    bearer_token: "${LOGFWD_TEST_TOKEN}"
"#;
        let cfg = Config::load_str(yaml).expect("auth env var bearer");
        let pipe = &cfg.pipelines["default"];
        let auth = pipe.outputs[0].auth.as_ref().expect("auth present");
        assert_eq!(auth.bearer_token.as_deref(), Some("env-bearer-token"));
        unsafe { std::env::remove_var("LOGFWD_TEST_TOKEN") };
    }

    #[test]
    fn auth_absent_is_none() {
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: http
  endpoint: http://localhost:9200
"#;
        let cfg = Config::load_str(yaml).expect("no auth");
        let pipe = &cfg.pipelines["default"];
        assert!(pipe.outputs[0].auth.is_none());
    }

    #[test]
    fn validation_endpoint_missing_scheme() {
        // Scheme-less endpoints must be rejected for both otlp and http outputs.
        for otype in ["otlp", "http"] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: collector:4317\n"
            );
            let result = Config::load_str(&yaml);
            assert!(
                result.is_err(),
                "expected error for scheme-less endpoint with type '{otype}'"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("scheme"),
                "error should mention 'scheme' for '{otype}': {msg}"
            );
        }
    }

    #[test]
    fn validation_endpoint_valid_schemes() {
        // Both http:// and https:// must be accepted for otlp and http outputs.
        for (otype, scheme) in [
            ("otlp", "http://"),
            ("otlp", "https://"),
            ("http", "http://"),
            ("http", "https://"),
        ] {
            let yaml = format!(
                "input:\n  type: file\n  path: /tmp/x.log\noutput:\n  type: {otype}\n  endpoint: {scheme}collector:4317\n"
            );
            Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("scheme '{scheme}' should be valid for '{otype}': {e}"));
        }
    }

    #[test]
    fn validation_endpoint_unexpanded_env_var_skipped() {
        // An endpoint whose value is still an unexpanded placeholder must not
        // fail URL validation — the user may supply the value at runtime.
        let yaml = r#"
input:
  type: file
  path: /var/log/test.log
output:
  type: otlp
  endpoint: ${LOGFWD_NONEXISTENT_ENDPOINT_VAR}
"#;
        // Should succeed (unexpanded placeholder passes through without error).
        Config::load_str(yaml).expect("unexpanded env var in endpoint should not fail validation");
    }
}
