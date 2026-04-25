use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ffwd_config::{CompressionFormat, Format, OtlpProtocol, OutputConfigV2, TlsClientConfig};
use ffwd_types::diagnostics::ComponentStats;

use crate::arrow_ipc_sink::ArrowIpcSinkFactory;
use crate::build_auth_headers;
use crate::elasticsearch::{ElasticsearchRequestMode, ElasticsearchSinkFactory};
use crate::error::OutputError;
use crate::file_sink::FileSinkFactory;
use crate::json_lines::JsonLinesSinkFactory;
use crate::loki::LokiSinkFactory;
use crate::metadata::Compression;
use crate::null::NullSinkFactory;
use crate::otlp_sink::OtlpSinkFactory;
use crate::sink::SinkFactory;
use crate::stdout::{StdoutFormat, StdoutSinkFactory};
use crate::tcp_sink::TcpSinkFactory;
use crate::udp_sink::UdpSinkFactory;

fn build_http_client_builder(
    name: &str,
    tls: Option<&TlsClientConfig>,
    request_timeout_ms: Option<ffwd_config::PositiveMillis>,
) -> Result<reqwest::ClientBuilder, OutputError> {
    let mut client_builder = reqwest::Client::builder()
        .timeout(request_timeout_ms.map_or(Duration::from_secs(30), Into::into))
        .pool_max_idle_per_host(64);

    if let Some(tls) = tls {
        if tls.insecure_skip_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }
        if let Some(ca) = &tls.ca_file {
            let ca_cert = read_tls_file(name, "ca_file", ca)?;
            let cert = reqwest::Certificate::from_pem(&ca_cert).map_err(|e| {
                OutputError::Construction(format!(
                    "output '{name}': invalid tls ca_file '{ca}': {e}"
                ))
            })?;
            client_builder = client_builder.add_root_certificate(cert);
        }
        match (&tls.cert_file, &tls.key_file) {
            (Some(cert), Some(key)) => {
                let cert_data = read_tls_file(name, "cert_file", cert)?;
                let key_data = read_tls_file(name, "key_file", key)?;
                let mut pem = cert_data;
                pem.push(b'\n');
                pem.extend(key_data);
                let identity = reqwest::Identity::from_pem(&pem).map_err(|e| {
                    OutputError::Construction(format!(
                        "output '{name}': invalid tls cert_file/key_file '{cert}'/'{key}': {e}"
                    ))
                })?;
                client_builder = client_builder.identity(identity);
            }
            (Some(_), None) => {
                return Err(OutputError::Construction(format!(
                    "output '{name}': tls cert_file requires tls key_file"
                )));
            }
            (None, Some(_)) => {
                return Err(OutputError::Construction(format!(
                    "output '{name}': tls key_file requires tls cert_file"
                )));
            }
            (None, None) => {}
        }
    }

    Ok(client_builder)
}

fn read_tls_file(name: &str, field: &str, path: &str) -> Result<Vec<u8>, OutputError> {
    std::fs::read(path).map_err(|e| {
        OutputError::Construction(format!(
            "output '{name}': failed to read tls {field} '{path}': {e}"
        ))
    })
}

/// Build an `Arc<dyn SinkFactory>` from a typed output configuration.
///
/// Returns a factory that creates a fresh sink per worker. Most sink types
/// support multiple workers; the factory can be called repeatedly.
pub fn build_sink_factory(
    name: &str,
    cfg: &OutputConfigV2,
    base_path: Option<&Path>,
    stats: Arc<ComponentStats>,
) -> Result<Arc<dyn SinkFactory>, OutputError> {
    match cfg {
        OutputConfigV2::Elasticsearch(cfg) => {
            let auth_headers = build_auth_headers(cfg.auth.as_ref());
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!(
                    "output '{name}': elasticsearch requires 'endpoint'"
                ))
            })?;
            let index = cfg
                .index
                .as_ref()
                .map_or("logs", String::as_str)
                .to_string();
            let compress = match cfg.compression {
                Some(CompressionFormat::Gzip) => true,
                Some(CompressionFormat::None) | None => false,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': elasticsearch does not support '{other}' compression (use 'gzip', 'none', or omit)"
                    )));
                }
            };
            let request_mode = match cfg.request_mode {
                Some(ElasticsearchRequestMode::Streaming) => ElasticsearchRequestMode::Streaming,
                Some(ElasticsearchRequestMode::Buffered) | None => {
                    ElasticsearchRequestMode::Buffered
                }
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unsupported elasticsearch request_mode '{other}' (use 'buffered', 'streaming', or omit)"
                    )));
                }
            };
            let factory = ElasticsearchSinkFactory::new_with_client(
                name.to_string(),
                endpoint.clone(),
                index,
                auth_headers,
                compress,
                request_mode,
                build_http_client_builder(name, cfg.tls.as_ref(), cfg.request_timeout_ms)?,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': elasticsearch factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputConfigV2::Loki(cfg) => {
            let auth_headers = build_auth_headers(cfg.auth.as_ref());
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': loki requires 'endpoint'"))
            })?;
            let factory = LokiSinkFactory::new_with_client(
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
                build_http_client_builder(name, cfg.tls.as_ref(), cfg.request_timeout_ms)?,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': loki factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputConfigV2::ArrowIpc(cfg) => {
            let auth_headers = build_auth_headers(cfg.auth.as_ref());
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': arrow_ipc requires 'endpoint'"))
            })?;
            let compression = match cfg.compression {
                Some(CompressionFormat::Zstd) => Compression::Zstd,
                Some(CompressionFormat::None) | None => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': arrow_ipc does not support '{other}' compression (use 'zstd', 'none', or omit)"
                    )));
                }
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
        OutputConfigV2::Http(cfg) => {
            let auth_headers = build_auth_headers(cfg.auth.as_ref());
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': http requires 'endpoint'"))
            })?;
            let compression = match cfg.compression {
                Some(CompressionFormat::Zstd) => Compression::Zstd,
                Some(CompressionFormat::Gzip) => Compression::Gzip,
                Some(CompressionFormat::None) | None => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unknown HTTP compression '{other}' (expected 'zstd', 'gzip', or 'none')"
                    )));
                }
            };
            let factory = JsonLinesSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                auth_headers,
                compression,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': http factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputConfigV2::Udp(cfg) => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': udp requires 'endpoint'"))
            })?;
            let factory = UdpSinkFactory::new(name.to_string(), endpoint.clone(), stats);
            Ok(Arc::new(factory))
        }
        OutputConfigV2::Otlp(cfg) => {
            let auth_headers = build_auth_headers(cfg.auth.as_ref());
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': OTLP requires 'endpoint'"))
            })?;
            let protocol = match cfg.protocol {
                Some(OtlpProtocol::Grpc) => OtlpProtocol::Grpc,
                Some(OtlpProtocol::Http) | None => OtlpProtocol::Http,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unsupported OTLP protocol '{other}' (use 'http', 'grpc', or omit)"
                    )));
                }
            };
            let compression = match cfg.compression {
                Some(CompressionFormat::Zstd) => Compression::Zstd,
                Some(CompressionFormat::Gzip) => Compression::Gzip,
                Some(CompressionFormat::None) | None => Compression::None,
                Some(other) => {
                    return Err(OutputError::Construction(format!(
                        "output '{name}': unknown OTLP compression '{other}' (expected 'zstd', 'gzip', or 'none')"
                    )));
                }
            };

            let client_builder =
                build_http_client_builder(name, cfg.tls.as_ref(), cfg.request_timeout_ms)?;

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
                ffwd_types::field_names::BODY.to_string(),
                client,
                stats,
            )
            .map_err(|e| {
                OutputError::Construction(format!("output '{name}': otlp factory: {e}"))
            })?;
            Ok(Arc::new(factory))
        }
        OutputConfigV2::Stdout(cfg) => {
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
        OutputConfigV2::File(cfg) => {
            let path = cfg.path.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': file requires 'path'"))
            })?;
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
        OutputConfigV2::Tcp(cfg) => {
            let endpoint = cfg.endpoint.as_ref().ok_or_else(|| {
                OutputError::Construction(format!("output '{name}': tcp requires 'endpoint'"))
            })?;
            Ok(Arc::new(TcpSinkFactory::new(
                name.to_string(),
                endpoint.clone(),
                stats,
            )))
        }
        OutputConfigV2::Null(_) => Ok(Arc::new(NullSinkFactory::new(name.to_string(), stats))),
        _ => Err(OutputError::Construction(format!(
            "output '{name}': unknown output type"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::build_sink_factory;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use ffwd_config::{
        ArrowIpcOutputConfig, CompressionFormat, ElasticsearchOutputConfig, LokiOutputConfig,
        OutputConfigV2, PositiveMillis, StdoutOutputConfig, TlsClientConfig,
    };
    use ffwd_types::diagnostics::ComponentStats;

    #[test]
    fn arrow_ipc_accepts_none_compression() {
        let cfg = OutputConfigV2::ArrowIpc(ArrowIpcOutputConfig {
            endpoint: Some("http://localhost:4318/v1/logs".to_string()),
            compression: Some(CompressionFormat::None),
            ..Default::default()
        });

        let result = build_sink_factory("arrow", &cfg, None, Arc::new(ComponentStats::new()));
        assert!(
            result.is_ok(),
            "arrow_ipc should accept explicit 'none' compression"
        );
    }

    #[test]
    fn stdout_builds_from_typed_variant() {
        let cfg = OutputConfigV2::Stdout(StdoutOutputConfig::default());

        let result = build_sink_factory("stdout", &cfg, None, Arc::new(ComponentStats::new()));
        assert!(result.is_ok(), "typed stdout config should build a sink");
    }

    #[test]
    fn elasticsearch_accepts_tls_and_request_timeout() {
        let cfg = OutputConfigV2::Elasticsearch(ElasticsearchOutputConfig {
            endpoint: Some("https://localhost:9200".to_string()),
            request_timeout_ms: PositiveMillis::new(5_000),
            tls: Some(TlsClientConfig {
                insecure_skip_verify: true,
                ..Default::default()
            }),
            ..Default::default()
        });

        let result = build_sink_factory("es", &cfg, None, Arc::new(ComponentStats::new()));
        assert!(
            result.is_ok(),
            "elasticsearch should accept tls/request_timeout config"
        );
    }

    #[test]
    fn loki_accepts_tls_and_request_timeout() {
        let cfg = OutputConfigV2::Loki(LokiOutputConfig {
            endpoint: Some("https://localhost:3100".to_string()),
            request_timeout_ms: PositiveMillis::new(5_000),
            tls: Some(TlsClientConfig {
                insecure_skip_verify: true,
                ..Default::default()
            }),
            static_labels: Some(HashMap::from([("app".to_string(), "ffwd".to_string())])),
            ..Default::default()
        });

        let result = build_sink_factory("loki", &cfg, None, Arc::new(ComponentStats::new()));
        assert!(
            result.is_ok(),
            "loki should accept tls/request_timeout config"
        );
    }

    #[test]
    fn elasticsearch_rejects_unreadable_tls_ca_file() {
        let cfg = OutputConfigV2::Elasticsearch(ElasticsearchOutputConfig {
            endpoint: Some("https://localhost:9200".to_string()),
            tls: Some(TlsClientConfig {
                ca_file: Some("/path/that/does/not/exist/ca.pem".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let result = build_sink_factory("es", &cfg, None, Arc::new(ComponentStats::new()));
        let err = match result {
            Ok(_) => panic!("missing tls ca_file should reject sink construction"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("failed to read tls ca_file"));
    }

    #[test]
    fn loki_rejects_malformed_tls_ca_file() {
        let path =
            std::env::temp_dir().join(format!("ffwd-output-invalid-ca-{}.pem", std::process::id()));
        fs::write(
            &path,
            b"-----BEGIN CERTIFICATE-----\nnot-valid-base64\n-----END CERTIFICATE-----\n",
        )
        .expect("write invalid ca file");
        let path = path.to_string_lossy().into_owned();
        let cfg = OutputConfigV2::Loki(LokiOutputConfig {
            endpoint: Some("https://localhost:3100".to_string()),
            tls: Some(TlsClientConfig {
                ca_file: Some(path.clone()),
                ..Default::default()
            }),
            static_labels: Some(HashMap::from([("app".to_string(), "ffwd".to_string())])),
            ..Default::default()
        });

        let result = build_sink_factory("loki", &cfg, None, Arc::new(ComponentStats::new()));
        let _ = fs::remove_file(path);
        let err = match result {
            Ok(_) => panic!("malformed tls ca_file should reject sink construction"),
            Err(err) => err,
        };
        // The malformed body gets past Certificate::from_pem and fails deeper
        // inside reqwest's builder. Assert on the crate's own wrapper ("loki
        // factory:") instead of the reqwest-specific "builder error" text so
        // the test is stable across reqwest/rustls updates.
        assert!(
            err.to_string().contains("loki factory"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn elasticsearch_rejects_tls_cert_without_key() {
        let cfg = OutputConfigV2::Elasticsearch(ElasticsearchOutputConfig {
            endpoint: Some("https://localhost:9200".to_string()),
            tls: Some(TlsClientConfig {
                cert_file: Some("/tmp/client.pem".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let result = build_sink_factory("es", &cfg, None, Arc::new(ComponentStats::new()));
        let err = match result {
            Ok(_) => panic!("half-configured mTLS identity should reject sink construction"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("cert_file requires tls key_file"));
    }

    #[test]
    fn loki_rejects_tls_key_without_cert() {
        let cfg = OutputConfigV2::Loki(LokiOutputConfig {
            endpoint: Some("https://localhost:3100".to_string()),
            tls: Some(TlsClientConfig {
                key_file: Some("/tmp/client.key".to_string()),
                ..Default::default()
            }),
            static_labels: Some(HashMap::from([("app".to_string(), "ffwd".to_string())])),
            ..Default::default()
        });

        let result = build_sink_factory("loki", &cfg, None, Arc::new(ComponentStats::new()));
        let err = match result {
            Ok(_) => panic!("half-configured mTLS identity should reject sink construction"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("key_file requires tls cert_file"));
    }
}
