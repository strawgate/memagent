mod common;
mod endpoints;
mod listeners;
mod outputs;
mod paths;
mod sensors;
#[cfg(test)]
mod tests;

use crate::shared::TlsServerConfig;
use crate::types::{
    Config, ConfigError, EnrichmentConfig, Format, GeneratorAttributeValueConfig,
    GeneratorProfileConfig, InputConfig, InputType, InputTypeConfig, JournaldBackendConfig,
    PIPELINE_WORKERS_MAX, PipelineConfig, ServerConfig, StorageConfig,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use common::{MAX_READ_BUF_SIZE, output_label, output_path_for_feedback_loop, validation_message};
use endpoints::{
    validate_bind_addr, validate_endpoint_url, validate_iso8601_timestamp, validate_log_level,
};
use listeners::{normalize_unit_name, sensor_supported_families_csv, track_listen_addr_uniqueness};
use outputs::validate_output_config;
use paths::{
    is_glob_match_possible, normalize_path_key_for_compare, path_for_config_compare,
    validate_file_output_path_writable,
};
use sensors::{is_sensor_family_supported, validate_sensor_event_type_filters};

pub use endpoints::validate_host_port;

impl Config {
    /// Validate the loaded configuration using a base path for relative paths.
    pub fn validate_with_base_path(&self, base_path: Option<&Path>) -> Result<(), ConfigError> {
        validate_server_config(&self.server)?;
        validate_storage_config(&self.storage)?;

        if self.pipelines.is_empty() {
            return Err(ConfigError::Validation(
                "at least one pipeline must be defined".into(),
            ));
        }

        let mut all_errors: Vec<String> = Vec::new();
        let mut seen_listen_addrs: HashMap<String, String> = HashMap::new();
        let mut seen_file_output_paths: HashMap<std::path::PathBuf, String> = HashMap::new();

        let mut pipeline_names: Vec<&str> = self.pipelines.keys().map(String::as_str).collect();
        pipeline_names.sort_unstable();
        for name in pipeline_names {
            let pipe = &self.pipelines[name];
            let result = validate_pipeline(
                name,
                pipe,
                base_path,
                &mut seen_listen_addrs,
                &mut seen_file_output_paths,
            );
            match result {
                Ok(()) => {}
                Err(ConfigError::Validation(msg)) => all_errors.push(msg),
                Err(other) => return Err(other),
            }
        }

        all_errors.sort();
        if all_errors.is_empty() {
            Ok(())
        } else if all_errors.len() == 1 {
            Err(ConfigError::Validation(
                all_errors
                    .into_iter()
                    .next()
                    .expect("guarded by len == 1 check"),
            ))
        } else {
            Err(ConfigError::Validation(format!(
                "{} validation error(s):\n  {}",
                all_errors.len(),
                all_errors.join("\n  ")
            )))
        }
    }
}

fn validate_server_config(server: &ServerConfig) -> Result<(), ConfigError> {
    if let Some(ep) = &server.traces_endpoint
        && let Err(err) = validate_endpoint_url(ep)
    {
        return Err(ConfigError::Validation(format!(
            "server.traces_endpoint: {}",
            validation_message(err)
        )));
    }

    if let Some(ep) = &server.metrics_endpoint
        && let Err(err) = validate_endpoint_url(ep)
    {
        return Err(ConfigError::Validation(format!(
            "server.metrics_endpoint: {}",
            validation_message(err)
        )));
    }

    if let Some(addr) = &server.diagnostics
        && let Err(err) = validate_bind_addr(addr)
    {
        return Err(ConfigError::Validation(format!(
            "server.diagnostics: {}",
            validation_message(err)
        )));
    }

    if let Some(level) = &server.log_level
        && let Err(err) = validate_log_level(level)
    {
        return Err(ConfigError::Validation(format!(
            "server.log_level: {}",
            validation_message(err)
        )));
    }

    Ok(())
}

fn validate_storage_config(storage: &StorageConfig) -> Result<(), ConfigError> {
    if let Some(ref dir) = storage.data_dir {
        let path = Path::new(dir);
        if path.exists() {
            let md = path.metadata().map_err(|e| {
                ConfigError::Validation(format!(
                    "storage.data_dir '{dir}' metadata lookup failed: {e}"
                ))
            })?;
            if !md.is_dir() {
                return Err(ConfigError::Validation(format!(
                    "storage.data_dir '{dir}' exists but is not a directory"
                )));
            }
        }
    }
    Ok(())
}

fn validate_pipeline(
    name: &str,
    pipe: &PipelineConfig,
    base_path: Option<&Path>,
    seen_listen_addrs: &mut HashMap<String, String>,
    seen_file_output_paths: &mut HashMap<std::path::PathBuf, String>,
) -> Result<(), ConfigError> {
    validate_pipeline_structure(name, pipe)?;
    validate_pipeline_names(name, pipe)?;

    for (i, input) in pipe.inputs.iter().enumerate() {
        let label = input
            .name
            .as_deref()
            .map_or_else(|| format!("#{i}"), String::from);
        validate_pipeline_input(name, &label, input, seen_listen_addrs)?;
    }

    for (i, output) in pipe.outputs.iter().enumerate() {
        let label = output_label(output, i);
        validate_output_config(name, &label, output)?;
    }

    for (j, enrichment) in pipe.enrichment.iter().enumerate() {
        validate_pipeline_enrichment(name, j, enrichment)?;
    }

    validate_pipeline_feedback_loops(name, pipe, base_path, seen_file_output_paths)?;
    Ok(())
}

fn validate_pipeline_structure(name: &str, pipe: &PipelineConfig) -> Result<(), ConfigError> {
    if let Some(workers) = pipe.workers
        && !(1..=PIPELINE_WORKERS_MAX).contains(&workers)
    {
        return Err(ConfigError::Validation(format!(
            "pipeline '{name}': workers must be in range 1..={PIPELINE_WORKERS_MAX} (got {workers})"
        )));
    }
    if pipe.batch_target_bytes == Some(0) {
        return Err(ConfigError::Validation(format!(
            "pipeline '{name}': batch_target_bytes must be greater than 0"
        )));
    }
    if let Some(sql) = &pipe.transform
        && sql.trim().is_empty()
    {
        return Err(ConfigError::Validation(format!(
            "pipeline '{name}': transform SQL cannot be empty"
        )));
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
    Ok(())
}

fn validate_pipeline_names(name: &str, pipe: &PipelineConfig) -> Result<(), ConfigError> {
    let mut seen_input_names: HashSet<&str> = HashSet::new();
    for (i, input) in pipe.inputs.iter().enumerate() {
        if let Some(input_name) = input.name.as_deref()
            && !seen_input_names.insert(input_name)
        {
            return Err(ConfigError::Validation(format!(
                "pipeline '{name}' input '#{i}': duplicate input name '{input_name}'"
            )));
        }
    }

    let mut seen_output_names: HashSet<&str> = HashSet::new();
    for (i, output) in pipe.outputs.iter().enumerate() {
        if let Some(output_name) = output.name()
            && !seen_output_names.insert(output_name)
        {
            return Err(ConfigError::Validation(format!(
                "pipeline '{name}' output '#{i}': duplicate output name '{output_name}'"
            )));
        }
    }

    Ok(())
}

fn validate_tls_server_config(
    pipeline_name: &str,
    input_label: &str,
    input_type: &str,
    tls: &TlsServerConfig,
) -> Result<(), ConfigError> {
    let cert_file = tls
        .cert_file
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let key_file = tls
        .key_file
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let client_ca_file = match tls.client_ca_file.as_deref() {
        Some(path) => {
            let path = path.trim();
            if path.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': {input_type} tls client_ca_file must not be empty"
                )));
            }
            Some(path)
        }
        None => None,
    };

    if tls.require_client_auth && client_ca_file.is_none() {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': {input_type} tls require_client_auth requires tls.client_ca_file"
        )));
    }
    if client_ca_file.is_some() && !tls.require_client_auth {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': {input_type} tls client_ca_file requires tls.require_client_auth: true"
        )));
    }

    if cert_file.is_none() || key_file.is_none() {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': {input_type} tls requires both tls.cert_file and tls.key_file"
        )));
    }

    Ok(())
}

fn validate_pipeline_input(
    pipeline_name: &str,
    input_label: &str,
    input: &InputConfig,
    seen_listen_addrs: &mut HashMap<String, String>,
) -> Result<(), ConfigError> {
    match &input.type_config {
        InputTypeConfig::File(f) => {
            if f.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': file input 'path' must not be empty"
                )));
            }
            if f.read_buf_size == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': 'read_buf_size' must be at least 1"
                )));
            }
            if let Some(sz) = f.read_buf_size
                && sz > MAX_READ_BUF_SIZE
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': 'read_buf_size' must not exceed {MAX_READ_BUF_SIZE} (4 MiB)"
                )));
            }
            if f.per_file_read_budget_bytes == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': 'per_file_read_budget_bytes' must be at least 1"
                )));
            }
            if f.max_open_files == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': max_open_files must be at least 1"
                )));
            }
        }
        InputTypeConfig::Udp(u) => {
            if let Err(err) = validate_bind_addr(&u.listen) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': {}",
                    validation_message(err)
                )));
            }
            if u.max_message_size_bytes == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': max_message_size_bytes cannot be 0"
                )));
            }
            if u.so_rcvbuf == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': so_rcvbuf cannot be 0"
                )));
            }

            track_listen_addr_uniqueness(
                seen_listen_addrs,
                "udp",
                pipeline_name,
                input_label,
                &u.listen,
            )?;
        }
        InputTypeConfig::Tcp(t) => {
            if let Err(err) = validate_bind_addr(&t.listen) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': {}",
                    validation_message(err)
                )));
            }
            if let Some(tls) = &t.tls {
                validate_tls_server_config(pipeline_name, input_label, "tcp", tls)?;
            }
            if t.max_clients == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': max_clients cannot be 0"
                )));
            }
            track_listen_addr_uniqueness(
                seen_listen_addrs,
                "tcp",
                pipeline_name,
                input_label,
                &t.listen,
            )?;
        }
        InputTypeConfig::Otlp(o) => {
            if let Err(err) = validate_bind_addr(&o.listen) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': {}",
                    validation_message(err)
                )));
            }
            if o.max_recv_message_size_bytes == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': otlp.max_recv_message_size_bytes must be at least 1"
                )));
            }
            if let Some(tls) = &o.tls {
                validate_tls_server_config(pipeline_name, input_label, "otlp", tls)?;
            }

            track_listen_addr_uniqueness(
                seen_listen_addrs,
                "tcp",
                pipeline_name,
                input_label,
                &o.listen,
            )?;
        }
        InputTypeConfig::Http(h) => {
            if let Err(err) = validate_bind_addr(&h.listen) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': {}",
                    validation_message(err)
                )));
            }
            if let Some(http) = &h.http {
                if let Some(path) = &http.path
                    && !path.starts_with('/')
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': http.path must start with '/'"
                    )));
                }
                if http.max_request_body_size == Some(0) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': http.max_request_body_size must be at least 1"
                    )));
                }
                if http.max_drained_bytes_per_poll == Some(0) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': http.max_drained_bytes_per_poll must be at least 1"
                    )));
                }
                if let Some(code) = http.response_code
                    && !matches!(code, 200 | 201 | 202 | 204)
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': http.response_code must be one of 200, 201, 202, 204"
                    )));
                }
                if http.response_code == Some(204) && http.response_body.is_some() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': http.response_body is not allowed when http.response_code is 204"
                    )));
                }
            }

            track_listen_addr_uniqueness(
                seen_listen_addrs,
                "tcp",
                pipeline_name,
                input_label,
                &h.listen,
            )?;
        }
        InputTypeConfig::Stdin(_) => {
            if let Some(fmt) = &input.format
                && !fmt.is_stdin_compatible()
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': stdin input only supports format auto, cri, json, or raw (got {fmt})"
                )));
            }
        }
        InputTypeConfig::Generator(g) => {
            if g.generator.as_ref().and_then(|cfg| cfg.batch_size) == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': generator.batch_size must be at least 1"
                )));
            }
            if let Some(generator) = &g.generator {
                let is_record_profile =
                    matches!(generator.profile, Some(GeneratorProfileConfig::Record));
                if generator.attributes.keys().any(|key| key.trim().is_empty()) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': generator.attributes keys must not be empty"
                    )));
                }
                if generator
                    .attributes
                    .values()
                    .any(|value| matches!(value, GeneratorAttributeValueConfig::Float(v) if !v.is_finite()))
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': generator.attributes float values must be finite"
                    )));
                }
                if let Some((key, _)) = generator
                    .attributes
                    .iter()
                    .find(|(_, v)| matches!(v, GeneratorAttributeValueConfig::Unsupported(_)))
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': generator.attributes '{key}' has an unsupported type (expected scalar value)"
                    )));
                }
                if !is_record_profile
                    && (!generator.attributes.is_empty()
                        || generator.sequence.is_some()
                        || generator.event_created_unix_nano_field.is_some())
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': generator.attributes, generator.sequence, and generator.event_created_unix_nano_field require generator.profile=record"
                    )));
                }
                if let Some(sequence) = &generator.sequence {
                    if sequence.field.trim().is_empty() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.sequence.field must not be empty"
                        )));
                    }
                    if generator.attributes.contains_key(&sequence.field) {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.sequence.field must not duplicate a generator.attributes key"
                        )));
                    }
                }
                if generator
                    .event_created_unix_nano_field
                    .as_deref()
                    .is_some_and(|field| field.trim().is_empty())
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': generator.event_created_unix_nano_field must not be empty"
                    )));
                }
                if let Some(field) = generator.event_created_unix_nano_field.as_deref() {
                    if generator.attributes.contains_key(field) {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.event_created_unix_nano_field must not duplicate a generator.attributes key"
                        )));
                    }
                    if generator
                        .sequence
                        .as_ref()
                        .is_some_and(|sequence| sequence.field == field)
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.event_created_unix_nano_field must not duplicate generator.sequence.field"
                        )));
                    }
                }
                if let Some(ts) = &generator.timestamp {
                    if is_record_profile {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.timestamp is only supported for the logs profile"
                        )));
                    }
                    if ts.step_ms == Some(0) {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.timestamp.step_ms must not be zero"
                        )));
                    }
                    if let Some(start) = &ts.start
                        && !start.eq_ignore_ascii_case("now")
                        && let Err(err) = validate_iso8601_timestamp(start)
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': generator.timestamp.start: {}",
                            validation_message(err)
                        )));
                    }
                }
            }
        }
        InputTypeConfig::MacosLog(s) => {
            if let Some(config) = &s.macos_log {
                if let Some(level) = &config.level
                    && level.trim().is_empty()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': macos_log 'level' cannot be empty"
                    )));
                }
                if let Some(subsystem) = &config.subsystem
                    && subsystem.trim().is_empty()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': macos_log 'subsystem' cannot be empty"
                    )));
                }
                if let Some(process) = &config.process
                    && process.trim().is_empty()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': macos_log 'process' cannot be empty"
                    )));
                }
            }
        }
        InputTypeConfig::LinuxEbpfSensor(s)
        | InputTypeConfig::MacosEsSensor(s)
        | InputTypeConfig::WindowsEbpfSensor(s)
        | InputTypeConfig::HostMetrics(s) => {
            let input_type = input.input_type();
            if input.format.is_some() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': sensor inputs do not support 'format' (Arrow-native input)"
                )));
            }
            if s.sensor
                .as_ref()
                .and_then(|cfg| cfg.control_path.as_deref())
                .is_some_and(|path| path.trim().is_empty())
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': sensor.control_path must not be empty"
                )));
            }
            if s.sensor.as_ref().and_then(|cfg| cfg.max_rows_per_poll) == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': sensor.max_rows_per_poll must be at least 1"
                )));
            }
            if let Some(families) = s
                .sensor
                .as_ref()
                .and_then(|cfg| cfg.enabled_families.as_ref())
            {
                for family in families {
                    let normalized = family.trim();
                    if normalized.is_empty() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': sensor.enabled_families entries must not be empty"
                        )));
                    }
                    if !is_sensor_family_supported(&input_type, normalized) {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': unknown sensor family '{normalized}' for {} input (supported: {})",
                            input_type,
                            sensor_supported_families_csv(&input_type)
                        )));
                    }
                }
            }
            if let Some(sensor) = s.sensor.as_ref() {
                validate_sensor_event_type_filters(
                    &input_type,
                    pipeline_name,
                    input_label,
                    sensor.include_event_types.as_deref(),
                    sensor.exclude_event_types.as_deref(),
                )?;
            }
            if matches!(input.input_type(), InputType::HostMetrics) {
                if s.sensor
                    .as_ref()
                    .and_then(|cfg| cfg.ebpf_binary_path.as_ref())
                    .is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': sensor.ebpf_binary_path is not supported for host_metrics inputs"
                    )));
                }
                if s.sensor
                    .as_ref()
                    .and_then(|cfg| cfg.max_events_per_poll)
                    .is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': sensor.max_events_per_poll is not supported for host_metrics inputs"
                    )));
                }
            }
            if s.sensor.as_ref().and_then(|cfg| cfg.ring_buffer_size_kb) == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': sensor.ring_buffer_size_kb must be at least 1"
                )));
            }
        }
        InputTypeConfig::ArrowIpc(a) => {
            if let Err(err) = validate_bind_addr(&a.listen) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': {}",
                    validation_message(err)
                )));
            }
            if input.format.is_some() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': 'format' is not supported for arrow_ipc inputs"
                )));
            }
            if a.max_connections == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': max_connections cannot be 0"
                )));
            }
            if a.max_message_size_bytes == Some(0) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': max_message_size_bytes cannot be 0"
                )));
            }

            track_listen_addr_uniqueness(
                seen_listen_addrs,
                "tcp",
                pipeline_name,
                input_label,
                &a.listen,
            )?;
        }
        InputTypeConfig::Journald(j) => {
            if let Some(jd) = &j.journald {
                if jd.backend == JournaldBackendConfig::Native
                    && jd.journal_directory.is_some()
                    && jd.journal_namespace.is_some()
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': 'journal_directory' and 'journal_namespace' cannot both be set with native backend"
                    )));
                }

                if jd
                    .journalctl_path
                    .as_deref()
                    .is_some_and(|s| s.trim().is_empty())
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': 'journalctl_path' must not be blank"
                    )));
                }
                if jd
                    .journal_directory
                    .as_deref()
                    .is_some_and(|s| s.trim().is_empty())
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': 'journal_directory' must not be blank"
                    )));
                }
                if jd
                    .journal_namespace
                    .as_deref()
                    .is_some_and(|s| s.trim().is_empty())
                {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': 'journal_namespace' must not be blank"
                    )));
                }

                for unit in jd.include_units.iter().chain(jd.exclude_units.iter()) {
                    if unit.trim().is_empty() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': unit names must not be blank"
                        )));
                    }
                }

                let norm_excludes: Vec<String> = jd
                    .exclude_units
                    .iter()
                    .map(|u| normalize_unit_name(u.trim()))
                    .collect();
                for unit in &jd.include_units {
                    let normalized = normalize_unit_name(unit.trim());
                    if norm_excludes.contains(&normalized) {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{pipeline_name}' input '{input_label}': unit '{unit}' appears in both include_units and exclude_units"
                        )));
                    }
                }
            }
            if let Some(fmt) = &input.format
                && !matches!(fmt, Format::Json)
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': journald input only supports format: json (got {fmt:?})"
                )));
            }
        }
        InputTypeConfig::S3(s) => {
            let s3_cfg = &s.s3;
            if s3_cfg.bucket.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': s3.bucket must not be empty"
                )));
            }
            if let Some(ref endpoint) = s3_cfg.endpoint {
                let ep = endpoint.trim();
                if ep.is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': s3.endpoint must not be empty"
                    )));
                }
                if !ep.starts_with("http://") && !ep.starts_with("https://") {
                    return Err(ConfigError::Validation(format!(
                        "pipeline '{pipeline_name}' input '{input_label}': s3.endpoint must start with http:// or https://"
                    )));
                }
            }
            if let Some(ps) = s3_cfg.part_size_bytes
                && ps == 0
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': s3.part_size_bytes must be at least 1"
                )));
            }
            if let Some(f) = s3_cfg.max_concurrent_fetches
                && f == 0
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': s3.max_concurrent_fetches must be at least 1"
                )));
            }
            if let Some(o) = s3_cfg.max_concurrent_objects
                && o == 0
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': s3.max_concurrent_objects must be at least 1"
                )));
            }
            if let Some(vt) = s3_cfg.visibility_timeout_secs
                && vt < 30
            {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' input '{input_label}': s3.visibility_timeout_secs must be at least 30"
                )));
            }
        }
    }

    if let Some(fmt @ (Format::Logfmt | Format::Syslog)) = &input.format {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': format {fmt:?} is not yet implemented",
        )));
    }

    if let Some(sql) = &input.sql
        && sql.trim().is_empty()
    {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': per-input sql cannot be empty"
        )));
    }

    Ok(())
}

fn validate_pipeline_enrichment(
    pipeline_name: &str,
    index: usize,
    enrichment: &EnrichmentConfig,
) -> Result<(), ConfigError> {
    match enrichment {
        EnrichmentConfig::GeoDatabase(geo_cfg) => {
            if geo_cfg.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: geo_database 'path' must not be empty"
                )));
            }
            let p = Path::new(&geo_cfg.path);
            if p.is_absolute() && !p.exists() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: geo database file not found: {}",
                    geo_cfg.path,
                )));
            }
        }
        EnrichmentConfig::Static(cfg) => {
            if cfg.table_name.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: table_name must not be empty"
                )));
            }
            if cfg.labels.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: static enrichment requires at least one label"
                )));
            }
        }
        EnrichmentConfig::Csv(cfg) => {
            if cfg.table_name.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: table_name must not be empty"
                )));
            }
            if cfg.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: csv 'path' must not be empty"
                )));
            }
            let p = Path::new(&cfg.path);
            if p.is_absolute() && !p.exists() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: csv file not found: {}",
                    cfg.path,
                )));
            }
        }
        EnrichmentConfig::Jsonl(cfg) => {
            if cfg.table_name.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: table_name must not be empty"
                )));
            }
            if cfg.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: jsonl 'path' must not be empty"
                )));
            }
            let p = Path::new(&cfg.path);
            if p.is_absolute() && !p.exists() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: jsonl file not found: {}",
                    cfg.path,
                )));
            }
        }
        EnrichmentConfig::K8sPath(cfg) => {
            if cfg.table_name.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: table_name must not be empty"
                )));
            }
        }
        EnrichmentConfig::HostInfo(_)
        | EnrichmentConfig::ProcessInfo(_)
        | EnrichmentConfig::NetworkInfo(_)
        | EnrichmentConfig::ContainerInfo(_)
        | EnrichmentConfig::K8sClusterInfo(_) => {}
        EnrichmentConfig::EnvVars(cfg) => {
            if cfg.table_name.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: table_name must not be empty"
                )));
            }
            if cfg.prefix.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: env_vars 'prefix' must not be empty"
                )));
            }
        }
        EnrichmentConfig::KvFile(cfg) => {
            if cfg.table_name.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: table_name must not be empty"
                )));
            }
            if cfg.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: kv_file 'path' must not be empty"
                )));
            }
            let p = Path::new(&cfg.path);
            if p.is_absolute() && !p.exists() {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' enrichment #{index}: kv_file not found: {}",
                    cfg.path,
                )));
            }
        }
    }

    Ok(())
}

fn validate_pipeline_feedback_loops(
    pipeline_name: &str,
    pipe: &PipelineConfig,
    base_path: Option<&Path>,
    seen_file_output_paths: &mut HashMap<std::path::PathBuf, String>,
) -> Result<(), ConfigError> {
    let (exact_input_paths, glob_input_patterns) =
        collect_pipeline_file_inputs_for_feedback_loop(pipe, base_path);

    for (index, output) in pipe.outputs.iter().enumerate() {
        let out_label = output_label(output, index);

        let Some(out_path) = output_path_for_feedback_loop(output) else {
            continue;
        };
        let out_pb = path_for_config_compare(out_path, base_path);
        let out_norm = normalize_path_key_for_compare(&out_pb);

        if let Some(prev) = seen_file_output_paths.get(&out_norm) {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{out_label}': file output path '{out_path}' duplicates {prev}"
            )));
        }
        seen_file_output_paths.insert(
            out_norm.clone(),
            format!("pipeline '{pipeline_name}' output '{out_label}'"),
        );

        for (in_pb, in_norm) in &exact_input_paths {
            if out_norm == *in_norm {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{out_label}': output path '{}' is the same \
                 as file input path '{}' — this creates an unbounded feedback loop",
                    out_path,
                    in_pb.display(),
                )));
            }
        }

        let resolved_out_path = out_norm.to_string_lossy();
        for glob_pattern in &glob_input_patterns {
            if is_glob_match_possible(glob_pattern, &resolved_out_path) {
                return Err(ConfigError::Validation(format!(
                    "pipeline '{pipeline_name}' output '{out_label}': output path '{out_path}' \
                 could match file input glob '{glob_pattern}' — this creates an \
                 unbounded feedback loop",
                )));
            }
        }

        validate_file_output_path_writable(pipeline_name, &out_label, out_path, base_path)?;
    }

    Ok(())
}

fn collect_pipeline_file_inputs_for_feedback_loop(
    pipe: &PipelineConfig,
    base_path: Option<&Path>,
) -> (Vec<(std::path::PathBuf, std::path::PathBuf)>, Vec<String>) {
    let mut exact_input_paths: Vec<(std::path::PathBuf, std::path::PathBuf)> = Vec::new();
    let mut glob_input_patterns: Vec<String> = Vec::new();

    for input in &pipe.inputs {
        if let InputTypeConfig::File(f) = &input.type_config {
            let path = &f.path;
            if path.contains('*') || path.contains('?') || path.contains('[') {
                let resolved = path_for_config_compare(path, base_path);
                let resolved = normalize_path_key_for_compare(&resolved);
                glob_input_patterns.push(resolved.to_string_lossy().into_owned());
            } else {
                let pb = path_for_config_compare(path, base_path);
                let norm = normalize_path_key_for_compare(&pb);
                exact_input_paths.push((pb, norm));
            }
        }
    }

    (exact_input_paths, glob_input_patterns)
}
