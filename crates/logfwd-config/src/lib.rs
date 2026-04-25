//! YAML configuration parser for logfwd.
//!
//! First-party examples and generated configs use a top-level `pipelines` map.
//!
//! Environment variables in values are expanded using `${VAR}` syntax.

/// Shared metadata for config starter templates and generated reference tables.
///
/// # Examples
///
/// ```
/// use logfwd_config::docspec::{input_template, INPUT_TYPE_DOCS};
///
/// assert!(input_template("file_json").is_some());
/// assert!(!INPUT_TYPE_DOCS.is_empty());
/// ```
pub mod docspec;
mod env;
mod load;
mod serde_helpers;
mod shared;
mod types;
mod validate;

pub use serde_helpers::{PositiveMillis, PositiveSecs};

#[cfg(test)]
pub(crate) use env::expand_env_vars;
pub use shared::{
    BatchConfig, MultilineConfig, NetworkConfig, RetryConfig, RotationConfig, TlsClientConfig,
    TlsServerConfig,
};
pub use types::{
    ArrowIpcOutputConfig, ArrowIpcTypeConfig, AuthConfig, CompressionFormat, Config, ConfigError,
    CsvEnrichmentConfig, ElasticsearchOutputConfig, ElasticsearchRequestMode, EnrichmentConfig,
    FileOutputConfig, FileTypeConfig, Format, GeneratorAttributeValueConfig,
    GeneratorComplexityConfig, GeneratorInputConfig, GeneratorProfileConfig,
    GeneratorSequenceConfig, GeneratorTypeConfig, GeoDatabaseConfig, GeoDatabaseFormat,
    HostInfoConfig, HostInfoStyle, HostMetricsInputConfig, HttpInputConfig, HttpMethodConfig,
    HttpOutputConfig, HttpTypeConfig, InputConfig, InputType, InputTypeConfig,
    JournaldBackendConfig, JournaldInputConfig, JournaldTypeConfig, JsonlEnrichmentConfig,
    K8sPathConfig, LokiOutputConfig, NullOutputConfig, OtlpOutputConfig,
    OtlpProtobufDecodeModeConfig, OtlpProtocol, OtlpTypeConfig, OutputConfigV2, OutputType,
    PipelineConfig, S3CompressionConfig, S3InputConfig, S3TypeConfig, SensorTypeConfig,
    ServerConfig, SourceMetadataStyle, StaticEnrichmentConfig, StdoutOutputConfig, StorageConfig,
    TcpOutputConfig, TcpTypeConfig, UdpOutputConfig, UdpTypeConfig,
};
pub use validate::validate_host_port;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Asserts that `Config::load_str(yaml)` fails and the error message contains
/// every given substring (AND logic). Replaces the repetitive
/// `unwrap_err → to_string → contains` pattern across test files.
#[cfg(test)]
macro_rules! assert_config_err {
    ($yaml:expr, $($substr:expr),+ $(,)?) => {{
        let err = $crate::Config::load_str($yaml).unwrap_err();
        let msg = err.to_string();
        $(assert!(msg.contains($substr), "expected {:?} in error: {msg}", $substr);)+
    }};
}

#[cfg(test)]
mod test_yaml;
#[cfg(test)]
mod tests_config_parsing;
#[cfg(test)]
mod tests_enrichment;
#[cfg(test)]
mod tests_generator_config;
#[cfg(test)]
mod tests_generator_unsupported;
#[cfg(test)]
mod tests_input;
#[cfg(test)]
mod tests_otlp_config;
#[cfg(test)]
mod tests_output;
#[cfg(test)]
mod tests_sensor;
#[cfg(test)]
mod tests_server;
#[cfg(test)]
mod tests_static_labels;
#[cfg(test)]
mod tests_validation;
