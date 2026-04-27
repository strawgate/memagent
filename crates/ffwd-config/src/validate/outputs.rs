use crate::types::{
    CompressionFormat, ConfigError, ElasticsearchRequestMode, Format, OutputConfigV2, OutputType,
};
use std::collections::HashMap;

use super::common::sanitize_identifier;
use super::common::validate_host_port;
use super::common::validation_message;
use super::endpoints::validate_endpoint_url;

fn validate_url_output_endpoint(
    pipeline_name: &str,
    label: &str,
    output_type: OutputType,
    endpoint: Option<&str>,
) -> Result<(), ConfigError> {
    let Some(endpoint) = endpoint else {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': {output_type} output requires 'endpoint'",
        )));
    };

    if let Err(err) = validate_endpoint_url(endpoint) {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': {}",
            validation_message(err)
        )));
    }

    Ok(())
}

fn validate_socket_output_endpoint(
    pipeline_name: &str,
    label: &str,
    output_type: OutputType,
    endpoint: Option<&str>,
) -> Result<(), ConfigError> {
    let Some(endpoint) = endpoint else {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': {output_type} output requires 'endpoint'",
        )));
    };

    if let Err(err) = validate_host_port(endpoint) {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': {}",
            validation_message(err)
        )));
    }

    Ok(())
}

fn reject_unsupported_output_field(
    pipeline_name: &str,
    label: &str,
    output_type: &str,
    field: &str,
    is_set: bool,
) -> Result<(), ConfigError> {
    if is_set {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': {output_type} output does not support '{field}' yet"
        )));
    }
    Ok(())
}

fn validate_elasticsearch_index(
    pipeline_name: &str,
    label: &str,
    index: Option<&str>,
) -> Result<(), ConfigError> {
    if let Some(index) = index
        && index.trim().is_empty()
    {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': elasticsearch 'index' must not be empty"
        )));
    }
    if let Some(index) = index
        && let Some(bad) = es_illegal_index_char(index)
    {
        let reason = if matches!(bad, '-' | '_' | '+') && index.starts_with(bad) {
            format!("has illegal prefix '{bad}'")
        } else {
            format!("contains illegal character '{bad}'")
        };
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{label}': elasticsearch index '{index}' {reason}"
        )));
    }

    Ok(())
}

fn validate_loki_labels(
    pipeline_name: &str,
    label: &str,
    static_labels: Option<&HashMap<String, String>>,
    label_columns: Option<&[String]>,
) -> Result<(), ConfigError> {
    if let Some(labels) = static_labels {
        for (key, value) in labels {
            if key.trim().is_empty() || value.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': 'static_labels' keys and values must not be empty"
                )));
            }
        }
    }

    // Detect intra-map collisions within static_labels after sanitization deterministically.
    if let Some(static_labels) = static_labels {
        let mut keys: Vec<&String> = static_labels.keys().collect();
        keys.sort();
        let mut seen: HashMap<String, &str> = HashMap::new();
        for key in &keys {
            let sanitized = sanitize_identifier(key);
            if let Some(existing) = seen.get(&sanitized) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': loki static_labels key '{key}' sanitizes to '{sanitized}' which collides with existing key '{existing}'"
                )));
            }
            seen.insert(sanitized, key);
        }
    }

    // Detect intra-map collisions within label_columns after sanitization.
    if let Some(label_columns) = label_columns {
        let mut seen: HashMap<String, &str> = HashMap::new();
        for col in label_columns {
            let sanitized = sanitize_identifier(col);
            if let Some(existing) = seen.get(&sanitized) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': loki label_columns entry '{col}' sanitizes to '{sanitized}' which collides with existing entry '{existing}'"
                )));
            }
            seen.insert(sanitized, col.as_str());
        }
    }

    // Detect cross-map collisions between static_labels and label_columns
    // after sanitization.
    if let (Some(static_labels), Some(label_columns)) = (static_labels, label_columns) {
        let mut sanitized_static: HashMap<String, &str> = HashMap::new();
        for key in static_labels.keys() {
            sanitized_static.insert(sanitize_identifier(key), key.as_str());
        }
        if let Some(conflict) = label_columns
            .iter()
            .find(|col| sanitized_static.contains_key(&sanitize_identifier(col)))
        {
            let sanitized = sanitize_identifier(conflict);
            let static_key = sanitized_static
                .get(&sanitized)
                .copied()
                .unwrap_or(conflict.as_str());
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{label}': loki label '{conflict}' (sanitizes to '{sanitized}') conflicts with 'static_labels' key '{static_key}'"
            )));
        }
    }

    Ok(())
}

pub(super) fn validate_output_config(
    pipeline_name: &str,
    label: &str,
    output: &OutputConfigV2,
) -> Result<(), ConfigError> {
    match output {
        OutputConfigV2::Otlp(config) => {
            validate_url_output_endpoint(
                pipeline_name,
                label,
                OutputType::Otlp,
                config.endpoint.as_deref(),
            )?;

            if let (Some(initial), Some(max)) =
                (config.retry_initial_backoff_ms, config.retry_max_backoff_ms)
                && initial > max
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': 'retry_initial_backoff_ms' must be <= 'retry_max_backoff_ms'"
                )));
            }
        }
        OutputConfigV2::Http(config) => {
            validate_url_output_endpoint(
                pipeline_name,
                label,
                OutputType::Http,
                config.endpoint.as_deref(),
            )?;

            if let Some(format) = &config.format
                && !matches!(format, Format::Json)
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': http output only supports format json"
                )));
            }
        }
        OutputConfigV2::Elasticsearch(config) => {
            validate_url_output_endpoint(
                pipeline_name,
                label,
                OutputType::Elasticsearch,
                config.endpoint.as_deref(),
            )?;
            validate_elasticsearch_index(pipeline_name, label, config.index.as_deref())?;

            if matches!(
                config.request_mode,
                Some(ElasticsearchRequestMode::Streaming)
            ) && matches!(config.compression, Some(CompressionFormat::Gzip))
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': elasticsearch request_mode 'streaming' does not support gzip compression yet"
                )));
            }

            if let Some(compression) = config.compression
                && !matches!(
                    compression,
                    CompressionFormat::Gzip | CompressionFormat::None
                )
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': elasticsearch compression must be 'gzip' or 'none', got '{compression}'"
                )));
            }
        }
        OutputConfigV2::Loki(config) => {
            validate_url_output_endpoint(
                pipeline_name,
                label,
                OutputType::Loki,
                config.endpoint.as_deref(),
            )?;
            validate_loki_labels(
                pipeline_name,
                label,
                config.static_labels.as_ref(),
                config.label_columns.as_deref(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "loki",
                "compression",
                config.compression.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "loki",
                "retry",
                config.retry.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "loki",
                "batch",
                config.batch.is_some(),
            )?;
        }
        OutputConfigV2::Stdout(config) => {
            if let Some(format) = &config.format
                && !matches!(format, Format::Json | Format::Text | Format::Console)
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': stdout output only supports format json, text, or console"
                )));
            }
        }
        OutputConfigV2::File(config) => {
            match config.path.as_deref() {
                None => {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' output '{label}': file output requires 'path'",
                    )));
                }
                Some(path) if path.trim().is_empty() => {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' output '{label}': file output 'path' must not be empty"
                    )));
                }
                Some(_) => {}
            }
            if let Some(format) = &config.format
                && !matches!(format, Format::Json | Format::Text)
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': file output only supports format json or text"
                )));
            }
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "file",
                "compression",
                config.compression.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "file",
                "rotation",
                config.rotation.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "file",
                "delimiter",
                config.delimiter.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "file",
                "path_template",
                config.path_template.is_some(),
            )?;
        }
        OutputConfigV2::Null(_) => {}
        OutputConfigV2::Tcp(config) => {
            validate_socket_output_endpoint(
                pipeline_name,
                label,
                OutputType::Tcp,
                config.endpoint.as_deref(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "encoding",
                config.encoding.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "framing",
                config.framing.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "tls",
                config.tls.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "keepalive",
                config.keepalive.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "timeout_secs",
                config.timeout_secs.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "retry",
                config.retry.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "tcp",
                "batch",
                config.batch.is_some(),
            )?;
        }
        OutputConfigV2::Udp(config) => {
            validate_socket_output_endpoint(
                pipeline_name,
                label,
                OutputType::Udp,
                config.endpoint.as_deref(),
            )?;
            if config.max_datagram_size_bytes == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': udp.max_datagram_size_bytes must be at least 1"
                )));
            }
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "udp",
                "encoding",
                config.encoding.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "udp",
                "max_datagram_size_bytes",
                config.max_datagram_size_bytes.is_some(),
            )?;
        }
        OutputConfigV2::ArrowIpc(config) => {
            validate_url_output_endpoint(
                pipeline_name,
                label,
                OutputType::ArrowIpc,
                config.endpoint.as_deref(),
            )?;

            if let Some(compression) = config.compression
                && !matches!(
                    compression,
                    CompressionFormat::Zstd | CompressionFormat::None
                )
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{label}': arrow_ipc output only supports 'zstd' or 'none' compression, not '{compression}'"
                )));
            }
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "arrow_ipc",
                "buffer_size_bytes",
                config.buffer_size_bytes.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "arrow_ipc",
                "batch_size",
                config.batch_size.is_some(),
            )?;
            reject_unsupported_output_field(
                pipeline_name,
                label,
                "arrow_ipc",
                "write_schema_on_connect",
                config.write_schema_on_connect.is_some(),
            )?;
        }
    }

    Ok(())
}

pub(super) fn es_illegal_index_char(index: &str) -> Option<char> {
    if matches!(index, "." | "..") {
        return Some('.');
    }
    if let Some(c) = index.chars().next()
        && matches!(c, '-' | '_' | '+')
    {
        return Some(c);
    }
    index.chars().find(|c| {
        c.is_ascii_uppercase()
            || matches!(
                c,
                '*' | '?' | '"' | '<' | '>' | '|' | ' ' | ',' | '#' | ':' | '\\' | '/'
            )
    })
}
