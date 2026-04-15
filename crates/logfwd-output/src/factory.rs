use std::path::{Path, PathBuf};
use std::sync::Arc;

use logfwd_config::{Format, OutputConfig, OutputType};
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;

use crate::arrow_ipc_sink::ArrowIpcSinkFactory;
use crate::build_auth_headers;
use crate::elasticsearch::{ElasticsearchRequestMode, ElasticsearchSinkFactory};
use crate::error::OutputError;
use crate::file_sink::FileSinkFactory;
use crate::json_lines::JsonLinesSinkFactory;
use crate::loki::LokiSinkFactory;
use crate::metadata::Compression;
use crate::null::NullSinkFactory;
use crate::otlp_sink::{OtlpProtocol, OtlpSinkFactory};
use crate::sink::SinkFactory;
use crate::stdout::{StdoutFormat, StdoutSinkFactory};
use crate::tcp_sink::TcpSinkFactory;
use crate::udp_sink::UdpSinkFactory;

/// Build an `Arc<dyn SinkFactory>` from an output configuration.
///
/// Returns a factory that creates a fresh sink per worker. Most sink types
/// support multiple workers; the factory can be called repeatedly.
pub fn build_sink_factory(
    name: &str,
    cfg: &OutputConfig,
    base_path: Option<&Path>,
    stats: Arc<ComponentStats>,
) -> Result<Arc<dyn SinkFactory>, OutputError> {
    let auth_headers = build_auth_headers(cfg.auth.as_ref());

    match cfg.output_type {
        OutputType::Elasticsearch => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!(
                    "output '{name}': elasticsearch requires 'endpoint'"
                ))
            })?;
            let index = cfg
                .index
                .as_ref()
                .or(cfg.path.as_ref())
                .map_or("logs", String::as_str)
                .to_string();
            let compress = match cfg.compression.as_deref() {
                Some("gzip") => true,
                Some("none") | None => false,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': elasticsearch does not support '{other}' compression (use 'gzip' or omit)"
                    )));
                }
            };
            let request_mode = match cfg.request_mode.as_deref() {
                Some("streaming") => ElasticsearchRequestMode::Streaming,
                _ => ElasticsearchRequestMode::Buffered,
            };
            let factory = ElasticsearchSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                index,
                auth_headers,
                compress,
                request_mode,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': elasticsearch factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputType::Loki => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': loki requires 'endpoint'"))
            })?;
            let factory = LokiSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                cfg.tenant_id.clone(),
                cfg.static_labels
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                cfg.label_columns.clone().unwrap_or_default(),
                auth_headers,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': loki factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputType::ArrowIpc => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': arrow_ipc requires 'endpoint'"))
            })?;
            let compression = match cfg.compression.as_deref() {
                Some("zstd") => Compression::Zstd,
                Some("none") => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': arrow_ipc does not support '{other}' compression (use 'zstd', 'none', or omit)"
                    )));
                }
                None => Compression::None,
            };
            let factory = ArrowIpcSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                compression,
                auth_headers,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': arrow_ipc factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputType::Http => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': http requires 'endpoint'"))
            })?;
            let compression = match cfg.compression.as_deref() {
                Some("zstd") => Compression::Zstd,
                Some("gzip") => Compression::Gzip,
                Some("none") | None => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unknown HTTP compression '{other}' (expected 'zstd', 'gzip', or 'none')"
                    )));
                }
            };
            Ok(Arc::new(JsonLinesSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                auth_headers,
                compression,
                stats,
            )))
        }
        OutputType::Udp => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': udp requires 'endpoint'"))
            })?;
            let factory = UdpSinkFactory::new(name.to_string(), endpoint.clone(), stats);
            Ok(Arc::new(factory))
        }
        OutputType::Otlp => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': OTLP requires 'endpoint'"))
            })?;
            let protocol = match cfg.protocol.as_deref() {
                Some("grpc") => OtlpProtocol::Grpc,
                Some("http") | None => OtlpProtocol::Http,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unknown OTLP protocol '{other}' (expected 'http' or 'grpc')"
                    )));
                }
            };
            let compression = match cfg.compression.as_deref() {
                Some("zstd") => Compression::Zstd,
                Some("gzip") => Compression::Gzip,
                Some("none") | None => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unknown OTLP compression '{other}' (expected 'zstd', 'gzip', or 'none')"
                    )));
                }
            };
            let factory = OtlpSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                protocol,
                compression,
                auth_headers,
                field_names::BODY.to_string(),
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': otlp factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputType::Stdout => {
            let fmt = match cfg.format.as_ref() {
                Some(Format::Json) => StdoutFormat::Json,
                Some(Format::Console) => StdoutFormat::Console,
                Some(Format::Text) => StdoutFormat::Text,
                // Default to console output when no format is specified.
                None => StdoutFormat::Console,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': stdout does not support '{other:?}' format (use json, console, or text)"
                    )));
                }
            };
            Ok(Arc::new(StdoutSinkFactory::new(
                name.to_string(),
                fmt,
                stats,
            )))
        }
        OutputType::File => {
            let path = cfg.path.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': file requires 'path'"))
            })?;

            // Allow compression but we don't implement gzip/zstd file writing in this iteration.
            // The pipeline validator checks for "gzip" | "zstd" | "none".
            let compression = cfg
                .compression
                .clone()
                .unwrap_or_else(|| "none".to_string());
            if compression.to_lowercase() != "none" {
                return Err(OutputError::Construction(format!(
                    "output '{name}': file compression '{compression}' is supported in config but not yet implemented"
                )));
            }

            let mut resolved_path = PathBuf::from(path);
            if resolved_path.is_relative()
                && let Some(base) = base_path
            {
                resolved_path = base.join(resolved_path);
            }

            // Both `format` (legacy) and `encoding` (standard) control the same thing here.
            let mut enc_str = "json".to_string();
            if let Some(fmt) = cfg.format.as_ref() {
                enc_str = match fmt {
                    Format::Json => "json".to_string(),
                    Format::Text => "text".to_string(),
                    other => {
                        return Err(OutputError::Construction(format!(
                            "output '{name}': file format {other:?} is not supported (use json or text)"
                        )));
                    }
                };
            }
            if let Some(enc) = cfg.encoding.as_ref() {
                enc_str = enc.to_lowercase();
            }

            let fmt = match enc_str.as_str() {
                "json" | "ndjson" => StdoutFormat::Json,
                "text" => StdoutFormat::Text,
                other => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': file encoding {other:?} is not supported (use json, ndjson, or text)"
                    )));
                }
            };

            let append = cfg.append.unwrap_or(true);
            let delimiter = cfg.delimiter.clone().unwrap_or_else(|| "\n".to_string());
            if delimiter != "\n" {
                return Err(OutputError::Construction(format!(
                    "output '{name}': file delimiter '{delimiter}' is supported in config but only '\\n' is implemented"
                )));
            }
            if cfg.rotation.is_some() {
                return Err(OutputError::Construction(format!(
                    "output '{name}': file rotation is supported in config but not yet implemented"
                )));
            }

            let factory = FileSinkFactory::new(
                name.to_string(),
                resolved_path.to_string_lossy().into_owned(),
                fmt,
                append,
                compression,
                cfg.rotation.clone(),
                delimiter,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': file factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputType::Tcp => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': tcp requires 'endpoint'"))
            })?;
            Ok(Arc::new(TcpSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                stats,
            )))
        }
        OutputType::Null => Ok(Arc::new(NullSinkFactory::new(name.to_string(), stats))),
        _ => Err(OutputError::Construction(format!(
            "output '{name}': type {:?} not yet supported",
            cfg.output_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::build_sink_factory;
    use std::sync::Arc;

    use logfwd_config::{OutputConfig, OutputType};
    use logfwd_types::diagnostics::ComponentStats;

    #[test]
    fn build_sink_factory_arrow_ipc_accepts_none_compression() {
        let cfg = OutputConfig {
            output_type: OutputType::ArrowIpc,
            endpoint: Some("http://localhost:4318/v1/logs".to_string()),
            compression: Some("none".to_string()),
            ..Default::default()
        };

        let result = build_sink_factory("arrow", &cfg, None, Arc::new(ComponentStats::new()));
        assert!(
            result.is_ok(),
            "arrow_ipc should accept explicit 'none' compression"
        );
    }
}
