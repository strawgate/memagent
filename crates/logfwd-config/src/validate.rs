use crate::types::{
    Config, ConfigError, EnrichmentConfig, Format, GeneratorAttributeValueConfig,
    GeneratorProfileConfig, InputType, OutputType,
};
use std::path::Path;
use url::Url;

impl Config {
    /// Validate the loaded configuration.
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if let Some(ep) = &self.server.traces_endpoint {
            if let Err(msg) = validate_endpoint_url(ep) {
                return Err(ConfigError::Validation(format!(
                    "server.traces_endpoint: {msg}"
                )));
            }
        }

        // Validate server.diagnostics bind address at config time so that
        // `validate` catches typos before the server tries to bind at runtime.
        if let Some(addr) = &self.server.diagnostics {
            if let Err(msg) = validate_bind_addr(addr) {
                return Err(ConfigError::Validation(format!(
                    "server.diagnostics: {msg}"
                )));
            }
        }

        // Validate server.log_level is a recognised level (#481).
        if let Some(level) = &self.server.log_level {
            if let Err(msg) = validate_log_level(level) {
                return Err(ConfigError::Validation(format!("server.log_level: {msg}")));
            }
        }

        if self.pipelines.is_empty() {
            return Err(ConfigError::Validation(
                "at least one pipeline must be defined".into(),
            ));
        }

        for (name, pipe) in &self.pipelines {
            if pipe.batch_timeout_ms == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}': batch_timeout_ms must be greater than 0"
                )));
            }
            if pipe.poll_interval_ms == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}': poll_interval_ms must be greater than 0"
                )));
            }
            if pipe.workers == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}': workers must be greater than 0"
                )));
            }
            if pipe.batch_target_bytes == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{name}': batch_target_bytes must be greater than 0"
                )));
            }
            if let Some(sql) = &pipe.transform {
                if sql.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}': transform SQL cannot be empty"
                    )));
                }
            }
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

                if input.input_type == InputType::ArrowIpc {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' input '{label}': arrow_ipc input type is not yet supported"
                    )));
                }
                match input.input_type {
                    InputType::File => match &input.path {
                        None => {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': file input requires 'path'"
                            )));
                        }
                        Some(p) if p.trim().is_empty() => {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': file input 'path' must not be empty"
                            )));
                        }
                        _ => {}
                    },
                    InputType::Udp | InputType::Tcp => {
                        if input.listen.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': udp/tcp input requires 'listen'"
                            )));
                        }
                        if let Some(addr) = &input.listen {
                            if let Err(msg) = validate_bind_addr(addr) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                        }
                        if let Some(tls) = &input.tls {
                            if matches!(input.input_type, InputType::Udp) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': TLS is not supported for UDP inputs (DTLS is not implemented)"
                                )));
                            }
                            if matches!(input.input_type, InputType::Tcp) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': TLS is not yet supported for TCP inputs (runtime TLS termination is not implemented)"
                                )));
                            }
                            let has_cert =
                                tls.cert_file.as_ref().is_some_and(|v| !v.trim().is_empty());
                            let has_key =
                                tls.key_file.as_ref().is_some_and(|v| !v.trim().is_empty());
                            if has_cert != has_key {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': tls.cert_file and tls.key_file must be set together"
                                )));
                            }
                            if !has_cert && tls.require_client_auth {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': tls.require_client_auth requires tls.cert_file and tls.key_file"
                                )));
                            }
                            if tls.require_client_auth
                                && tls
                                    .client_ca_file
                                    .as_ref()
                                    .is_none_or(|v| v.trim().is_empty())
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': tls.client_ca_file is required when tls.require_client_auth=true"
                                )));
                            }
                        }
                    }
                    InputType::Otlp | InputType::Http => {
                        if input.listen.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'listen' is required for {} inputs",
                                input.input_type
                            )));
                        }
                        if let Some(addr) = &input.listen {
                            if let Err(msg) = validate_bind_addr(addr) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                        }
                    }
                    InputType::Generator | InputType::ArrowIpc => {}
                }

                // Reject fields that don't apply to this input type.
                match input.input_type {
                    InputType::File => {
                        if input.generator.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'generator' settings are only supported for generator inputs"
                            )));
                        }
                        if input.http.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'http' settings are only supported for http inputs"
                            )));
                        }
                        if input.listen.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'listen' is not supported for file inputs"
                            )));
                        }
                        if input.tls.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'tls' is not supported for file inputs"
                            )));
                        }
                    }
                    InputType::Tcp | InputType::Udp => {
                        if input.generator.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'generator' settings are only supported for generator inputs"
                            )));
                        }
                        if input.http.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'http' settings are only supported for http inputs"
                            )));
                        }
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for tcp/udp inputs"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for tcp/udp inputs"
                            )));
                        }
                        if input.glob_rescan_interval_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'glob_rescan_interval_ms' is not supported for tcp/udp inputs"
                            )));
                        }
                    }
                    InputType::Otlp => {
                        if input.tls.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'tls' is not supported for otlp inputs"
                            )));
                        }
                        if input.http.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'http' settings are only supported for http inputs"
                            )));
                        }
                        if input.generator.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'generator' settings are only supported for generator inputs"
                            )));
                        }
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for otlp inputs"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for otlp inputs"
                            )));
                        }
                        if input.glob_rescan_interval_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'glob_rescan_interval_ms' is not supported for otlp inputs"
                            )));
                        }
                    }
                    InputType::Http => {
                        if input.tls.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'tls' is not supported for http inputs"
                            )));
                        }
                        if input.generator.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'generator' settings are only supported for generator inputs"
                            )));
                        }
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for http inputs; use http.path"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for http inputs"
                            )));
                        }
                        if input.glob_rescan_interval_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'glob_rescan_interval_ms' is not supported for http inputs"
                            )));
                        }
                        if let Some(http) = &input.http {
                            if let Some(path) = &http.path
                                && !path.starts_with('/')
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': http.path must start with '/'"
                                )));
                            }
                            if http.max_request_body_size == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': http.max_request_body_size must be at least 1"
                                )));
                            }
                            if let Some(code) = http.response_code
                                && !matches!(code, 200 | 201 | 202 | 204)
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': http.response_code must be one of 200, 201, 202, 204"
                                )));
                            }
                        }
                    }
                    InputType::Generator => {
                        if input.tls.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'tls' is not supported for generator inputs"
                            )));
                        }
                        if input.listen.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'listen' is not supported for generator inputs; use generator.events_per_sec"
                            )));
                        }
                        if input.http.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'http' settings are only supported for http inputs"
                            )));
                        }
                        if input.path.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'path' is not supported for generator inputs"
                            )));
                        }
                        if input.max_open_files.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'max_open_files' is not supported for generator inputs"
                            )));
                        }
                        if input.glob_rescan_interval_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'glob_rescan_interval_ms' is not supported for generator inputs"
                            )));
                        }
                        if input.generator.as_ref().and_then(|cfg| cfg.batch_size) == Some(0) {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': generator.batch_size must be at least 1"
                            )));
                        }
                        if let Some(generator) = &input.generator {
                            let is_record_profile =
                                matches!(generator.profile, Some(GeneratorProfileConfig::Record));
                            if generator.attributes.keys().any(|key| key.trim().is_empty()) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': generator.attributes keys must not be empty"
                                )));
                            }
                            if generator
                                .attributes
                                .values()
                                .any(|value| matches!(value, GeneratorAttributeValueConfig::Float(v) if !v.is_finite()))
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': generator.attributes float values must be finite"
                                )));
                            }
                            if !is_record_profile
                                && (!generator.attributes.is_empty()
                                    || generator.sequence.is_some()
                                    || generator.event_created_unix_nano_field.is_some())
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': generator.attributes, generator.sequence, and generator.event_created_unix_nano_field require generator.profile=record"
                                )));
                            }
                            if let Some(sequence) = &generator.sequence {
                                if sequence.field.trim().is_empty() {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': generator.sequence.field must not be empty"
                                    )));
                                }
                                if generator.attributes.contains_key(&sequence.field) {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': generator.sequence.field must not duplicate a generator.attributes key"
                                    )));
                                }
                            }
                            if generator
                                .event_created_unix_nano_field
                                .as_deref()
                                .is_some_and(|field| field.trim().is_empty())
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': generator.event_created_unix_nano_field must not be empty"
                                )));
                            }
                            if let Some(field) = generator.event_created_unix_nano_field.as_deref()
                            {
                                if generator.attributes.contains_key(field) {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': generator.event_created_unix_nano_field must not duplicate a generator.attributes key"
                                    )));
                                }
                                if generator
                                    .sequence
                                    .as_ref()
                                    .is_some_and(|sequence| sequence.field == field)
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': generator.event_created_unix_nano_field must not duplicate generator.sequence.field"
                                    )));
                                }
                            }
                        }
                    }
                    InputType::ArrowIpc => {
                        if input.generator.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'generator' settings are only supported for generator inputs"
                            )));
                        }
                        if input.http.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' input '{label}': 'http' settings are only supported for http inputs"
                            )));
                        }
                    }
                }

                // Reject input formats that are not yet implemented.
                if let Some(fmt @ (Format::Logfmt | Format::Syslog)) = &input.format {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' input '{label}': format {fmt:?} is not yet implemented",
                    )));
                }

                // max_open_files: 0 silently disables all file reading (#696).
                if input.max_open_files == Some(0) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' input '{label}': max_open_files must be at least 1"
                    )));
                }

                // Reject whitespace-only per-input SQL (mirrors pipeline-level check).
                if let Some(sql) = &input.sql {
                    if sql.trim().is_empty() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' input '{label}': per-input sql cannot be empty"
                        )));
                    }
                }
            }

            for (i, output) in pipe.outputs.iter().enumerate() {
                let label = output
                    .name
                    .as_deref()
                    .map_or_else(|| format!("#{i}"), String::from);

                // Reject placeholder output types that are not yet implemented.
                if matches!(output.output_type, OutputType::Parquet | OutputType::Http) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': {} output type is not yet implemented",
                        output.output_type,
                    )));
                }

                match output.output_type {
                    OutputType::Otlp
                    | OutputType::Elasticsearch
                    | OutputType::Loki
                    | OutputType::ArrowIpc => {
                        if output.endpoint.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'endpoint'",
                                output.output_type,
                            )));
                        }
                        if let Some(ep) = &output.endpoint
                            && let Err(msg) = validate_endpoint_url(ep)
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {msg}",
                            )));
                        }
                        if output.output_type == OutputType::Elasticsearch {
                            if let Some(idx) = &output.index
                                && idx.trim().is_empty()
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': elasticsearch 'index' must not be empty"
                                )));
                            }
                            if let Some(mode) = output.request_mode.as_deref()
                                && !matches!(mode, "buffered" | "streaming")
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': elasticsearch request_mode must be 'buffered' or 'streaming'"
                                )));
                            }
                            if output.request_mode.as_deref() == Some("streaming")
                                && output.compression.as_deref() == Some("gzip")
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': elasticsearch request_mode 'streaming' does not support gzip compression yet"
                                )));
                            }
                        } else if output.request_mode.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': request_mode is only supported for elasticsearch outputs"
                            )));
                        }
                    }
                    OutputType::File => {
                        match &output.path {
                            None => {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': {} output requires 'path'",
                                    output.output_type,
                                )));
                            }
                            Some(p) if p.trim().is_empty() => {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': file output 'path' must not be empty"
                                )));
                            }
                            _ => {}
                        }
                        if let Some(fmt) = &output.format
                            && !matches!(fmt, Format::Json | Format::Text)
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': file output only supports format json or text"
                            )));
                        }
                    }
                    OutputType::Stdout => {
                        if let Some(fmt) = &output.format
                            && !matches!(fmt, Format::Json | Format::Text | Format::Console)
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': stdout output only supports format json, text, or console"
                            )));
                        }
                    }
                    OutputType::Null => {}
                    OutputType::Tcp | OutputType::Udp => {
                        if output.endpoint.is_none() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': {} output requires 'endpoint'",
                                output.output_type,
                            )));
                        }
                        if let Some(ep) = &output.endpoint {
                            if let Err(msg) = validate_host_port(ep) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': {msg}",
                                )));
                            }
                        }
                    }
                    // Http and Parquet are not yet implemented — already
                    // rejected by the check above; these arms are unreachable
                    // but required for exhaustiveness.
                    OutputType::Http | OutputType::Parquet => {}
                }

                // Reject fields that don't apply to this output type.
                if output.output_type != OutputType::Elasticsearch && output.index.is_some() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'index' is only supported for elasticsearch outputs"
                    )));
                }
                if output.output_type == OutputType::Loki && output.compression.is_some() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'compression' is not supported for loki outputs"
                    )));
                }
                if output.output_type != OutputType::Otlp && output.protocol.is_some() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'protocol' is only supported for otlp outputs"
                    )));
                }
                if output.output_type != OutputType::Loki {
                    if output.tenant_id.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'tenant_id' is only supported for loki outputs"
                        )));
                    }
                    if output.static_labels.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'static_labels' is only supported for loki outputs"
                        )));
                    }
                    if output.label_columns.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'label_columns' is only supported for loki outputs"
                        )));
                    }
                }
                if !matches!(output.output_type, OutputType::File | OutputType::Parquet)
                    && output.path.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'path' is only supported for file/parquet outputs"
                    )));
                }
                // auth is only valid for HTTP-based outputs
                if !matches!(
                    output.output_type,
                    OutputType::Otlp
                        | OutputType::Http
                        | OutputType::Elasticsearch
                        | OutputType::Loki
                        | OutputType::ArrowIpc
                ) && output.auth.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'auth' is only supported for HTTP-based outputs"
                    )));
                }
                // compression: only valid for outputs that support it
                if matches!(
                    output.output_type,
                    OutputType::Stdout
                        | OutputType::Null
                        | OutputType::Tcp
                        | OutputType::Udp
                        | OutputType::File
                ) && output.compression.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{name}' output '{label}': 'compression' is not supported for this output type"
                    )));
                }
            }

            // Validate enrichment entries (#550).
            for (j, enrichment) in pipe.enrichment.iter().enumerate() {
                match enrichment {
                    EnrichmentConfig::GeoDatabase(geo_cfg) => {
                        if geo_cfg.path.trim().is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: geo_database 'path' must not be empty"
                            )));
                        }
                        // Only check existence for absolute paths; relative paths
                        // are resolved against base_path in Pipeline::from_config.
                        let p = Path::new(&geo_cfg.path);
                        if p.is_absolute() && !p.exists() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: geo database file not found: {}",
                                geo_cfg.path,
                            )));
                        }
                    }
                    EnrichmentConfig::Static(cfg) => {
                        if cfg.table_name.is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: table_name must not be empty"
                            )));
                        }
                        if cfg.labels.is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: static enrichment requires at least one label"
                            )));
                        }
                    }
                    EnrichmentConfig::Csv(cfg) => {
                        if cfg.table_name.is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: table_name must not be empty"
                            )));
                        }
                        if cfg.path.trim().is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: csv 'path' must not be empty"
                            )));
                        }
                        let p = Path::new(&cfg.path);
                        if p.is_absolute() && !p.exists() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: csv file not found: {}",
                                cfg.path,
                            )));
                        }
                    }
                    EnrichmentConfig::Jsonl(cfg) => {
                        if cfg.table_name.is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: table_name must not be empty"
                            )));
                        }
                        if cfg.path.trim().is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: jsonl 'path' must not be empty"
                            )));
                        }
                        let p = Path::new(&cfg.path);
                        if p.is_absolute() && !p.exists() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: jsonl file not found: {}",
                                cfg.path,
                            )));
                        }
                    }
                    EnrichmentConfig::K8sPath(cfg) => {
                        if cfg.table_name.is_empty() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' enrichment #{j}: table_name must not be empty"
                            )));
                        }
                    }
                    EnrichmentConfig::HostInfo(_) => {}
                }
            }

            // Guard against feedback loops: reject configs where a file output
            // path matches a file input path in the same pipeline (#1596).
            let file_input_paths: Vec<std::path::PathBuf> = pipe
                .inputs
                .iter()
                .filter(|i| i.input_type == InputType::File)
                .filter_map(|i| i.path.as_deref())
                .filter(|p| !p.contains('*') && !p.contains('?'))
                .map(std::path::PathBuf::from)
                .collect();

            let normalized_file_inputs: Vec<(std::path::PathBuf, std::path::PathBuf)> =
                file_input_paths
                    .into_iter()
                    .map(|p| {
                        let norm = normalize_path_for_compare(&p);
                        (p, norm)
                    })
                    .collect();

            for (j, output) in pipe.outputs.iter().enumerate() {
                let out_label = output
                    .name
                    .as_deref()
                    .map_or_else(|| format!("#{j}"), String::from);

                if !matches!(output.output_type, OutputType::File | OutputType::Parquet) {
                    continue;
                }
                let Some(out_path) = output.path.as_deref() else {
                    continue;
                };
                let out_pb = std::path::PathBuf::from(out_path);
                let out_norm = normalize_path_for_compare(&out_pb);
                for (in_pb, in_norm) in &normalized_file_inputs {
                    if out_norm == *in_norm {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{out_label}': output path '{}' is the same \
                             as file input path '{}' — this creates an unbounded feedback loop",
                            out_path,
                            in_pb.display(),
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

/// Compare paths for config-level equivalence: prefer canonical paths when they
/// exist; fall back to lexical normalisation so relative aliases like `./a.log`
/// and `logs/../a.log` are treated as the same path.
fn normalize_path_for_compare(path: &Path) -> std::path::PathBuf {
    path.canonicalize()
        .unwrap_or_else(|_| normalize_path_lexically(path))
}

fn normalize_path_lexically(path: &Path) -> std::path::PathBuf {
    use std::path::Component;

    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let mut tail = out.components();
                match tail.next_back() {
                    Some(Component::Normal(_)) => {
                        out.pop();
                    }
                    Some(Component::CurDir) => {}
                    Some(Component::ParentDir) | None => out.push(component.as_os_str()),
                    Some(Component::RootDir) | Some(Component::Prefix(_)) => {}
                }
            }
            Component::Normal(_) | Component::RootDir | Component::Prefix(_) => {
                out.push(component.as_os_str());
            }
        }
    }

    if out.as_os_str().is_empty() {
        std::path::PathBuf::from(".")
    } else {
        out
    }
}

/// Validate that a bind address is a parseable `host:port` socket address.
fn validate_bind_addr(addr: &str) -> Result<(), String> {
    validate_host_port(addr)
}

/// Validate that a string has a valid `host:port` format where port is a u16.
///
/// Accepts IP addresses (v4 and v6) as well as hostnames, consistent with the
/// runtime `TcpListener::bind` behaviour.  Use this function anywhere an
/// address is validated so that CLI and config validation remain in sync.
pub fn validate_host_port(addr: &str) -> Result<(), String> {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        return Err(format!("'{addr}' is a URL, expected host:port"));
    }

    let (host, port_str) = if addr.starts_with('[') {
        // Use find (first ']') not rfind (last ']') so that inputs like
        // "[::1]]:4317" are rejected rather than treating "[::1]]" as the host.
        let close_bracket = addr
            .find(']')
            .ok_or_else(|| format!("'{addr}' has mismatched brackets"))?;
        let inner = &addr[1..close_bracket];
        if inner.is_empty() {
            return Err(format!(
                "'{addr}' has an empty IPv6 address inside brackets"
            ));
        }
        inner
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| format!("'{addr}' contains a non-IPv6 value inside brackets"))?;
        if !addr[close_bracket..].starts_with("]:") {
            return Err(format!("'{addr}' is missing a port after IPv6 brackets"));
        }
        let port_str = &addr[close_bracket + 2..];
        (&addr[..=close_bracket], port_str)
    } else {
        addr.rsplit_once(':')
            .ok_or_else(|| format!("'{addr}' is missing a port (expected format host:port)"))?
    };

    if host.is_empty() {
        return Err(format!("'{addr}' has an empty host"));
    }

    // Reject path-like hosts (e.g. "host/path:80") — these are likely
    // malformed URLs rather than intentional host:port values. (#1461)
    if host.contains('/') {
        return Err(format!(
            "'{addr}' host contains a '/' (expected host:port, not a URL path)"
        ));
    }

    // Reject unmatched closing bracket outside of IPv6 brackets (e.g. "host]:80").
    if !addr.starts_with('[') && host.contains(']') {
        return Err(format!("'{addr}' has an unmatched ']' in the host"));
    }

    if !addr.starts_with('[') && host.contains(':') {
        return Err(format!(
            "'{addr}' has multiple colons without IPv6 brackets"
        ));
    }

    port_str
        .parse::<u16>()
        .map_err(|_| format!("'{addr}' has an invalid port '{port_str}'"))?;
    Ok(())
}

/// Validate that a log level string is a recognised tracing level.
///
/// Accepted values (case-insensitive): `trace`, `debug`, `info`, `warn`, `error`.
fn validate_log_level(level: &str) -> Result<(), String> {
    match level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(format!(
            "'{level}' is not a recognised log level; expected one of: trace, debug, info, warn, error"
        )),
    }
}

#[cfg(test)]
mod validate_host_port_tests {
    use super::*;

    #[test]
    fn validate_host_port_works() {
        assert!(validate_host_port("127.0.0.1:4317").is_ok());
        assert!(validate_host_port("localhost:4317").is_ok());
        assert!(validate_host_port("my-host.internal:8080").is_ok());
        assert!(validate_host_port("[::1]:4317").is_ok());
        assert!(validate_host_port("[2001:db8::1]:80").is_ok());

        assert!(
            validate_host_port(":4317")
                .unwrap_err()
                .contains("empty host")
        );
        assert!(
            validate_host_port("http://localhost:4317")
                .unwrap_err()
                .contains("URL")
        );
        assert!(
            validate_host_port("https://localhost:4317")
                .unwrap_err()
                .contains("URL")
        );
        assert!(
            validate_host_port("foo:bar:4317")
                .unwrap_err()
                .contains("multiple colons")
        );
        assert!(
            validate_host_port("localhost")
                .unwrap_err()
                .contains("missing a port")
        );
        assert!(
            validate_host_port("localhost:")
                .unwrap_err()
                .contains("invalid port")
        );
        assert!(
            validate_host_port("localhost:999999")
                .unwrap_err()
                .contains("invalid port")
        );
        assert!(
            validate_host_port("[::1]")
                .unwrap_err()
                .contains("missing a port")
        );
        assert!(
            validate_host_port("[::1]:")
                .unwrap_err()
                .contains("invalid port")
        );
        // Empty IPv6 brackets — []:8080 has no host
        assert!(validate_host_port("[]:8080").unwrap_err().contains("empty"));
        // Double closing bracket — [::1]]:4317 is malformed
        assert!(
            validate_host_port("[::1]]:4317")
                .unwrap_err()
                .contains("missing a port")
        );
        // Path-like host rejected (#1461)
        assert!(
            validate_host_port("foo/bar:4317")
                .unwrap_err()
                .contains("/")
        );
        // Unmatched closing bracket rejected (#1461)
        assert!(validate_host_port("foo]:4317").unwrap_err().contains("]"));
    }

    #[test]
    fn validate_bind_addr_works() {
        assert!(validate_bind_addr("127.0.0.1:4317").is_ok());
        assert!(validate_bind_addr("localhost:4317").is_ok());
        assert!(validate_bind_addr("[::1]:4317").is_ok());
        assert!(validate_bind_addr("http://localhost:4317").is_err());
    }
}

/// Validate that an endpoint URL has a recognised scheme and a non-empty host.
fn validate_endpoint_url(endpoint: &str) -> Result<(), String> {
    let parsed =
        Url::parse(endpoint).map_err(|_| format!("endpoint '{endpoint}' is not a valid URL"))?;

    let rest = if endpoint
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
    {
        &endpoint[8..]
    } else if endpoint
        .get(..7)
        .is_some_and(|p| p.eq_ignore_ascii_case("http://"))
    {
        &endpoint[7..]
    } else {
        return Err(format!(
            "endpoint '{endpoint}' has no recognised scheme; expected 'http://' or 'https://'"
        ));
    };

    // Reject malformed authority forms like `http:///bulk` or `https://?x=1`.
    if rest.is_empty() || rest.starts_with('/') || rest.starts_with('?') || rest.starts_with('#') {
        return Err(format!(
            "endpoint '{endpoint}' has no host after the scheme"
        ));
    }

    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(format!(
            "endpoint '{endpoint}' has no host after the scheme"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod validate_endpoint_url_tests {
    use super::validate_endpoint_url;

    #[test]
    fn endpoint_url_requires_http_or_https_with_host() {
        assert!(validate_endpoint_url("http://localhost:4317").is_ok());
        assert!(validate_endpoint_url("https://example.com/path").is_ok());
        assert!(validate_endpoint_url("HTTP://EXAMPLE.COM").is_ok());

        for bad in [
            "http:///bulk",
            "https://?x=1",
            "http://   ",
            "ftp://example.com",
        ] {
            assert!(
                validate_endpoint_url(bad).is_err(),
                "expected endpoint validation error for {bad}"
            );
        }
    }
}

#[cfg(test)]
mod feedback_loop_tests {
    use crate::types::Config;

    #[test]
    fn file_output_same_as_input_rejected() {
        let yaml = r#"
pipelines:
  looping:
    inputs:
      - type: file
        path: /tmp/logfwd-feedback-test.log
    outputs:
      - type: file
        path: /tmp/logfwd-feedback-test.log
        format: json
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("feedback loop") || msg.contains("same as file input"),
            "expected feedback-loop rejection, got: {msg}"
        );
    }

    #[test]
    fn file_output_different_from_input_allowed() {
        let yaml = r#"
pipelines:
  ok:
    inputs:
      - type: file
        path: /tmp/logfwd-input.log
    outputs:
      - type: file
        path: /tmp/logfwd-output.log
        format: json
"#;
        Config::load_str(yaml).expect("different input/output paths should be allowed");
    }

    #[test]
    fn file_output_same_as_input_rejected_after_lexical_normalization() {
        let yaml = r#"
pipelines:
  looping:
    inputs:
      - type: file
        path: ./tmp/logs/../app.log
    outputs:
      - type: file
        path: tmp/./app.log
        format: json
"#;
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("feedback loop") || msg.contains("same as file input"),
            "expected normalized-path feedback-loop rejection, got: {msg}"
        );
    }
}

#[cfg(test)]
mod validate_empty_field_tests {
    use crate::types::Config;

    #[test]
    fn file_input_empty_path_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: ""
    outputs:
      - type: stdout
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path"),
            "expected path rejection: {err}"
        );
        assert!(
            err.to_string().contains("must not be empty"),
            "expected 'must not be empty' message: {err}"
        );
    }

    #[test]
    fn file_input_whitespace_path_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: \"   \"\n    outputs:\n      - type: stdout\n";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("path") && err.to_string().contains("must not be empty"),
            "whitespace-only path must be rejected: {err}"
        );
    }

    #[test]
    fn elasticsearch_empty_index_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: http://localhost:9200
        index: ""
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("index"),
            "expected index rejection: {err}"
        );
        assert!(
            err.to_string().contains("must not be empty"),
            "expected 'must not be empty' message: {err}"
        );
    }

    #[test]
    fn elasticsearch_whitespace_index_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: elasticsearch\n        endpoint: http://localhost:9200\n        index: \"   \"\n";
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("index") && err.to_string().contains("must not be empty"),
            "whitespace-only index must be rejected: {err}"
        );
    }
}

// -----------------------------------------------------------------------
// Bug #1644: empty enrichment table_name rejected by --validate
// -----------------------------------------------------------------------

#[cfg(test)]
mod validate_enrichment_table_name_tests {
    use crate::types::Config;

    #[test]
    fn enrichment_static_empty_table_name_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: static
        table_name: ''
        labels:
          key: val
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("table_name must not be empty"),
            "expected 'table_name must not be empty' in error: {msg}"
        );
    }

    #[test]
    fn enrichment_csv_empty_table_name_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: csv
        table_name: ''
        path: relative/path/assets.csv
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("table_name must not be empty"),
            "expected 'table_name must not be empty' in error: {msg}"
        );
    }

    #[test]
    fn enrichment_jsonl_empty_table_name_rejected() {
        let yaml = r"
pipelines:
  app:
    inputs:
      - type: file
        path: /tmp/x.log
    outputs:
      - type: stdout
    enrichment:
      - type: jsonl
        table_name: ''
        path: relative/path/ips.jsonl
";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("table_name must not be empty"),
            "expected 'table_name must not be empty' in error: {msg}"
        );
    }
}
