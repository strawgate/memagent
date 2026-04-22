//! YAML configuration parser for logfwd.
//!
//! Supports two layout styles:
//!
//! - **Simple** (single pipeline): top-level `input`, `transform`, `output`.
//! - **Advanced** (multiple pipelines): top-level `pipelines` map.
//!
//! Environment variables in values are expanded using `${VAR}` syntax.

mod compat;
/// Shared metadata for config starter templates and generated reference tables.
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
pub use shared::{TlsClientConfig, TlsServerConfig};
pub use types::{
    ArrowIpcOutputConfig, ArrowIpcTypeConfig, AuthConfig, CompressionFormat, Config, ConfigError,
    CsvEnrichmentConfig, ElasticsearchOutputConfig, ElasticsearchRequestMode, EnrichmentConfig,
    FileOutputConfig, FileTypeConfig, Format, GeneratorAttributeValueConfig,
    GeneratorComplexityConfig, GeneratorInputConfig, GeneratorProfileConfig,
    GeneratorSequenceConfig, GeneratorTypeConfig, GeoDatabaseConfig, GeoDatabaseFormat,
    HostInfoConfig, HostMetricsInputConfig, HttpInputConfig, HttpMethodConfig, HttpOutputConfig,
    HttpTypeConfig, InputConfig, InputType, InputTypeConfig, JournaldBackendConfig,
    JournaldInputConfig, JournaldTypeConfig, JsonlEnrichmentConfig, K8sPathConfig,
    LokiOutputConfig, NullOutputConfig, OtlpOutputConfig, OtlpProtobufDecodeModeConfig,
    OtlpProtocol, OtlpTypeConfig, OutputConfigV2, OutputType, ParquetOutputConfig, PipelineConfig,
    S3InputConfig, S3TypeConfig, SensorTypeConfig, ServerConfig, SocketOutputConfig,
    SourceMetadataStyle, StaticEnrichmentConfig, StdoutOutputConfig, StorageConfig, TcpTypeConfig,
    UdpTypeConfig,
};
pub use validate::validate_host_port;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
