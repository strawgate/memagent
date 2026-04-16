use std::path::{Path, PathBuf};
use std::sync::Arc;

use logfwd_config::{Format, OutputConfig, OutputType};
use logfwd_types::diagnostics::ComponentStats;

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
            // Accept either an explicit `endpoint` URL or a `host` + `port` pair.
            let endpoint = if let Some(ep) = cfg.endpoint.as_ref() {
                ep.clone()
            } else if let (Some(host), Some(port)) = (cfg.host.as_ref(), cfg.port) {
                format!("http://{host}:{port}")
            } else {
                return Err(OutputError::Construction(format!(
                    "output '{name}': arrow_ipc requires either 'endpoint' or both 'host' and 'port'"
                )));
            };
            let compression = match cfg.compression.as_deref() {
                Some("zstd") => Compression::Zstd,
                Some("lz4") => Compression::Lz4,
                Some("none") => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': arrow_ipc does not support '{other}' compression (use 'lz4', 'zstd', 'none', or omit)"
                    )));
                }
                None => Compression::None,
            };
            let write_legacy_ipc_format = cfg.write_legacy_ipc_format.unwrap_or(false);
            let write_schema_on_connect = cfg.write_schema_on_connect.unwrap_or(false);
            let factory = ArrowIpcSinkFactory::new(
                name.to_string(),
                endpoint,
                compression,
                auth_headers,
                write_legacy_ipc_format,
                cfg.buffer_size_bytes,
                cfg.batch_size,
                write_schema_on_connect,
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

            let mut client_builder = reqwest::Client::builder()
                .timeout(std::time::Duration::from_millis(
                    cfg.request_timeout_ms.unwrap_or(30_000),
                ))
                .pool_max_idle_per_host(64);

            #[allow(clippy::collapsible_if)]
            if let Some(tls) = &cfg.tls {
                if tls.insecure_skip_verify {
                    client_builder = client_builder.danger_accept_invalid_certs(true);
                }
                if let Some(ca) = &tls.ca_file {
                    if let Ok(ca_cert) = std::fs::read(ca) {
                        if let Ok(cert) = reqwest::Certificate::from_pem(&ca_cert) {
                            client_builder = client_builder.add_root_certificate(cert);
                        }
                    }
                }
                if let (Some(cert), Some(key)) = (&tls.cert_file, &tls.key_file) {
                    if let (Ok(cert_data), Ok(key_data)) = (std::fs::read(cert), std::fs::read(key))
                    {
                        let mut pem = cert_data;
                        pem.push(b'\n');
                        pem.extend(key_data);
                        if let Ok(identity) = reqwest::Identity::from_pem(&pem) {
                            client_builder = client_builder.identity(identity);
                        }
                    }
                }
            }

            let mut all_headers = auth_headers;
            if let Some(cfg_headers) = &cfg.headers {
                for (k, v) in cfg_headers {
                    all_headers.push((k.clone(), v.clone()));
                }
            }

            let client = client_builder.build().map_err(|e| {
                OutputError::Construction(format!(
                    "output '{name}': otlp client builder error: {e}"
                ))
            })?;

            if cfg.retry_attempts.is_some()
                || cfg.retry_initial_backoff_ms.is_some()
                || cfg.retry_max_backoff_ms.is_some()
                || cfg.batch_size.is_some()
                || cfg.batch_timeout_ms.is_some()
            {
                tracing::warn!(
                    "output '{}': retry and batching configuration options are currently handled by the pipeline runner and are not applied directly in the otlp sink yet.",
                    name
                );
            }

            let factory = OtlpSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                protocol,
                compression,
                all_headers,
                logfwd_types::field_names::BODY.to_string(),
                client,
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
            if let Some(compression) = cfg.compression.as_deref() {
                return Err(OutputError::Construction(format!(
                    "output '{name}': file does not support '{compression}' compression"
                )));
            }
            let mut resolved_path = PathBuf::from(path);
            if resolved_path.is_relative()
                && let Some(base) = base_path
            {
                resolved_path = base.join(resolved_path);
            }
            let fmt = match cfg.format.as_ref() {
                Some(Format::Json) | None => StdoutFormat::Json,
                Some(Format::Text) => StdoutFormat::Text,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': file format {other:?} is not supported (use json or text)"
                    )));
                }
            };
            let factory = FileSinkFactory::new(
                name.to_string(),
                resolved_path.to_string_lossy().into_owned(),
                fmt,
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
