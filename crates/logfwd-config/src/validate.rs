use crate::types::{
    CompressionFormat, Config, ConfigError, ElasticsearchRequestMode, EnrichmentConfig, Format,
    GeneratorAttributeValueConfig, GeneratorProfileConfig, InputType, InputTypeConfig,
    JournaldBackendConfig, OutputType, PIPELINE_WORKERS_MAX,
};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::Path;
use url::Url;

const MAX_READ_BUF_SIZE: usize = 4_194_304;

impl Config {
    /// Validate the loaded configuration using a base path for relative paths.
    pub fn validate_with_base_path(&self, base_path: Option<&Path>) -> Result<(), ConfigError> {
        if let Some(ep) = &self.server.traces_endpoint
            && let Err(msg) = validate_endpoint_url(ep)
        {
            return Err(ConfigError::Validation(format!(
                "server.traces_endpoint: {msg}"
            )));
        }

        // Validate server.metrics_endpoint at config time (#1892).
        if let Some(ep) = &self.server.metrics_endpoint
            && let Err(msg) = validate_endpoint_url(ep)
        {
            return Err(ConfigError::Validation(format!(
                "server.metrics_endpoint: {msg}"
            )));
        }

        // Validate server.diagnostics bind address at config time so that
        // `validate` catches typos before the server tries to bind at runtime.
        if let Some(addr) = &self.server.diagnostics
            && let Err(msg) = validate_bind_addr(addr)
        {
            return Err(ConfigError::Validation(format!(
                "server.diagnostics: {msg}"
            )));
        }

        // Validate server.log_level is a recognised level (#481).
        if let Some(level) = &self.server.log_level
            && let Err(msg) = validate_log_level(level)
        {
            return Err(ConfigError::Validation(format!("server.log_level: {msg}")));
        }

        // Validate storage.data_dir is either absent/non-existent or an existing directory.
        if let Some(ref dir) = self.storage.data_dir {
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
            let result = (|| -> Result<(), ConfigError> {
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

                for (i, input) in pipe.inputs.iter().enumerate() {
                    let label = input
                        .name
                        .as_deref()
                        .map_or_else(|| format!("#{i}"), String::from);

                    match &input.type_config {
                        InputTypeConfig::File(f) => {
                            if f.path.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': file input 'path' must not be empty"
                                )));
                            }
                            if f.read_buf_size == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': 'read_buf_size' must be at least 1"
                                )));
                            }
                            if let Some(sz) = f.read_buf_size
                                && sz > MAX_READ_BUF_SIZE
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': 'read_buf_size' must not exceed {MAX_READ_BUF_SIZE} (4 MiB)"
                                )));
                            }
                            if f.per_file_read_budget_bytes == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': 'per_file_read_budget_bytes' must be at least 1"
                                )));
                            }
                            if f.max_open_files == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': max_open_files must be at least 1"
                                )));
                            }
                        }
                        InputTypeConfig::Udp(u) => {
                            if let Err(msg) = validate_bind_addr(&u.listen) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                            if u.max_message_size_bytes == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': max_message_size_bytes cannot be 0"
                                )));
                            }
                            if u.so_rcvbuf == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': so_rcvbuf cannot be 0"
                                )));
                            }

                            track_listen_addr_uniqueness(
                                &mut seen_listen_addrs,
                                "udp",
                                name,
                                &label,
                                &u.listen,
                            )?;
                        }
                        InputTypeConfig::Tcp(t) => {
                            if let Err(msg) = validate_bind_addr(&t.listen) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                            if let Some(tls) = &t.tls {
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
                                let client_ca_file = tls
                                    .client_ca_file
                                    .as_deref()
                                    .map(str::trim)
                                    .filter(|v| !v.is_empty());

                                if client_ca_file.is_some() || tls.require_client_auth {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': tcp tls client authentication is not supported (tls.client_ca_file and tls.require_client_auth are reserved for #2332)"
                                    )));
                                }

                                if cert_file.is_none() || key_file.is_none() {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': tcp tls requires both tls.cert_file and tls.key_file"
                                    )));
                                }
                            }
                            if t.max_connections == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': max_connections cannot be 0"
                                )));
                            }
                            track_listen_addr_uniqueness(
                                &mut seen_listen_addrs,
                                "tcp",
                                name,
                                &label,
                                &t.listen,
                            )?;
                        }
                        InputTypeConfig::Otlp(o) => {
                            if let Err(msg) = validate_bind_addr(&o.listen) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                            if o.max_recv_message_size_bytes == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': otlp.max_recv_message_size_bytes must be at least 1"
                                )));
                            }

                            track_listen_addr_uniqueness(
                                &mut seen_listen_addrs,
                                "tcp",
                                name,
                                &label,
                                &o.listen,
                            )?;
                        }
                        InputTypeConfig::Http(h) => {
                            if let Err(msg) = validate_bind_addr(&h.listen) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                            if let Some(http) = &h.http {
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
                                if http.max_drained_bytes_per_poll == Some(0) {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': http.max_drained_bytes_per_poll must be at least 1"
                                    )));
                                }
                                if let Some(code) = http.response_code
                                    && !matches!(code, 200 | 201 | 202 | 204)
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': http.response_code must be one of 200, 201, 202, 204"
                                    )));
                                }
                                if http.response_code == Some(204) && http.response_body.is_some() {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': http.response_body is not allowed when http.response_code is 204"
                                    )));
                                }
                            }

                            track_listen_addr_uniqueness(
                                &mut seen_listen_addrs,
                                "tcp",
                                name,
                                &label,
                                &h.listen,
                            )?;
                        }
                        InputTypeConfig::Stdin(_) => {
                            if let Some(fmt) = &input.format
                                && !fmt.is_stdin_compatible()
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': stdin input only supports format auto, cri, json, or raw (got {fmt})"
                                )));
                            }
                        }
                        InputTypeConfig::Generator(g) => {
                            if g.generator.as_ref().and_then(|cfg| cfg.batch_size) == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': generator.batch_size must be at least 1"
                                )));
                            }
                            if let Some(generator) = &g.generator {
                                let is_record_profile = matches!(
                                    generator.profile,
                                    Some(GeneratorProfileConfig::Record)
                                );
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
                                if let Some((key, _)) =
                                    generator.attributes.iter().find(|(_, v)| {
                                        matches!(v, GeneratorAttributeValueConfig::Unsupported(_))
                                    })
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': generator.attributes '{key}' has an unsupported type (expected scalar value)"
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
                                if let Some(field) =
                                    generator.event_created_unix_nano_field.as_deref()
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
                                if let Some(ts) = &generator.timestamp {
                                    if is_record_profile {
                                        return Err(ConfigError::Validation(format!(
                                            "pipeline '{name}' input '{label}': generator.timestamp is only supported for the logs profile"
                                        )));
                                    }
                                    if ts.step_ms == Some(0) {
                                        return Err(ConfigError::Validation(format!(
                                            "pipeline '{name}' input '{label}': generator.timestamp.step_ms must not be zero"
                                        )));
                                    }
                                    if let Some(start) = &ts.start
                                        && !start.eq_ignore_ascii_case("now")
                                        && let Err(e) = validate_iso8601_timestamp(start)
                                    {
                                        return Err(ConfigError::Validation(format!(
                                            "pipeline '{name}' input '{label}': generator.timestamp.start: {e}"
                                        )));
                                    }
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
                                    "pipeline '{name}' input '{label}': sensor inputs do not support 'format' (Arrow-native input)"
                                )));
                            }
                            if s.sensor
                                .as_ref()
                                .and_then(|cfg| cfg.control_path.as_deref())
                                .is_some_and(|path| path.trim().is_empty())
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': sensor.control_path must not be empty"
                                )));
                            }
                            if s.sensor.as_ref().and_then(|cfg| cfg.max_rows_per_poll) == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': sensor.max_rows_per_poll must be at least 1"
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
                                            "pipeline '{name}' input '{label}': sensor.enabled_families entries must not be empty"
                                        )));
                                    }
                                    if !is_sensor_family_supported(&input_type, normalized) {
                                        return Err(ConfigError::Validation(format!(
                                            "pipeline '{name}' input '{label}': unknown sensor family '{normalized}' for {} input (supported: {})",
                                            input_type,
                                            sensor_supported_families_csv(&input_type)
                                        )));
                                    }
                                }
                            }
                            if let Some(sensor) = s.sensor.as_ref() {
                                validate_sensor_event_type_filters(
                                    &input_type,
                                    name,
                                    &label,
                                    sensor.include_event_types.as_deref(),
                                    sensor.exclude_event_types.as_deref(),
                                )?;
                            }
                            // NOTE: ebpf_binary_path for linux_ebpf_sensor is validated
                            // at runtime when the sensor loads, not here — the path may
                            // be auto-discovered or provided via environment variable.
                            //
                            // Reject eBPF-specific fields on host_metrics inputs.
                            if matches!(input.input_type(), InputType::HostMetrics) {
                                if s.sensor
                                    .as_ref()
                                    .and_then(|cfg| cfg.ebpf_binary_path.as_ref())
                                    .is_some()
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': sensor.ebpf_binary_path is not supported for host_metrics inputs"
                                    )));
                                }
                                if s.sensor
                                    .as_ref()
                                    .and_then(|cfg| cfg.max_events_per_poll)
                                    .is_some()
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': sensor.max_events_per_poll is not supported for host_metrics inputs"
                                    )));
                                }
                            }
                            if s.sensor.as_ref().and_then(|cfg| cfg.ring_buffer_size_kb) == Some(0)
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': sensor.ring_buffer_size_kb must be at least 1"
                                )));
                            }
                        }
                        InputTypeConfig::ArrowIpc(a) => {
                            if let Err(msg) = validate_bind_addr(&a.listen) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': {msg}"
                                )));
                            }
                            if input.format.is_some() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': 'format' is not supported for arrow_ipc inputs"
                                )));
                            }
                            if a.max_connections == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': max_connections cannot be 0"
                                )));
                            }
                            if a.max_message_size_bytes == Some(0) {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': max_message_size_bytes cannot be 0"
                                )));
                            }

                            track_listen_addr_uniqueness(
                                &mut seen_listen_addrs,
                                "tcp",
                                name,
                                &label,
                                &a.listen,
                            )?;
                        }
                        InputTypeConfig::Journald(j) => {
                            if let Some(jd) = &j.journald {
                                // journal_directory and journal_namespace are mutually exclusive
                                // in the native backend (directory opens a specific path, namespace
                                // opens a named journal). The subprocess backend supports both
                                // flags together, so only reject when the native API is required.
                                if jd.backend == JournaldBackendConfig::Native
                                    && jd.journal_directory.is_some()
                                    && jd.journal_namespace.is_some()
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': 'journal_directory' and 'journal_namespace' cannot both be set with native backend"
                                    )));
                                }

                                // Reject blank/whitespace-only optional string fields.
                                if jd
                                    .journalctl_path
                                    .as_deref()
                                    .is_some_and(|s| s.trim().is_empty())
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': 'journalctl_path' must not be blank"
                                    )));
                                }
                                if jd
                                    .journal_directory
                                    .as_deref()
                                    .is_some_and(|s| s.trim().is_empty())
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': 'journal_directory' must not be blank"
                                    )));
                                }
                                if jd
                                    .journal_namespace
                                    .as_deref()
                                    .is_some_and(|s| s.trim().is_empty())
                                {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': 'journal_namespace' must not be blank"
                                    )));
                                }

                                // Reject blank/whitespace-only unit names.
                                for unit in jd.include_units.iter().chain(jd.exclude_units.iter()) {
                                    if unit.trim().is_empty() {
                                        return Err(ConfigError::Validation(format!(
                                            "pipeline '{name}' input '{label}': unit names must not be blank"
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
                                            "pipeline '{name}' input '{label}': unit '{unit}' appears in both include_units and exclude_units"
                                        )));
                                    }
                                }
                            }
                            // Journald always produces JSON; reject other formats at config time.
                            if let Some(fmt) = &input.format
                                && !matches!(fmt, Format::Json)
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': journald input only supports format: json (got {fmt:?})"
                                )));
                            }
                        }
                        InputTypeConfig::S3(s) => {
                            let s3_cfg = &s.s3;
                            if s3_cfg.bucket.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': s3.bucket must not be empty"
                                )));
                            }
                            if let Some(ref endpoint) = s3_cfg.endpoint {
                                let ep = endpoint.trim();
                                if ep.is_empty() {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': s3.endpoint must not be empty"
                                    )));
                                }
                                if !ep.starts_with("http://") && !ep.starts_with("https://") {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': s3.endpoint must start with http:// or https://"
                                    )));
                                }
                            }
                            if let Some(ref comp) = s3_cfg.compression {
                                let valid = [
                                    "auto", "gzip", "gz", "zstd", "zst", "snappy", "sz", "none",
                                    "identity",
                                ];
                                if !valid.iter().any(|v| v.eq_ignore_ascii_case(comp)) {
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' input '{label}': unknown s3.compression value '{comp}' \
                                         (valid: auto, gzip, gz, zstd, zst, snappy, sz, none, identity)"
                                    )));
                                }
                            }
                            if let Some(ps) = s3_cfg.part_size_bytes
                                && ps == 0
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': s3.part_size_bytes must be at least 1"
                                )));
                            }
                            if let Some(f) = s3_cfg.max_concurrent_fetches
                                && f == 0
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': s3.max_concurrent_fetches must be at least 1"
                                )));
                            }
                            if let Some(o) = s3_cfg.max_concurrent_objects
                                && o == 0
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': s3.max_concurrent_objects must be at least 1"
                                )));
                            }
                            if let Some(vt) = s3_cfg.visibility_timeout_secs
                                && vt < 30
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' input '{label}': s3.visibility_timeout_secs must be at least 30"
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

                    // Reject whitespace-only per-input SQL (mirrors pipeline-level check).
                    if let Some(sql) = &input.sql
                        && sql.trim().is_empty()
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' input '{label}': per-input sql cannot be empty"
                        )));
                    }
                }

                for (i, output) in pipe.outputs.iter().enumerate() {
                    let output = output.validation_config();
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
                                if let Some(idx) = &output.index
                                    && let Some(bad) = es_illegal_index_char(idx)
                                {
                                    let reason =
                                        if matches!(bad, '-' | '_' | '+') && idx.starts_with(bad) {
                                            format!("has illegal prefix '{bad}'")
                                        } else {
                                            format!("contains illegal character '{bad}'")
                                        };
                                    return Err(ConfigError::Validation(format!(
                                        "pipeline '{name}' output '{label}': elasticsearch index '{idx}' {reason}"
                                    )));
                                }
                                if matches!(
                                    output.request_mode,
                                    Some(ElasticsearchRequestMode::Streaming)
                                ) && matches!(output.compression, Some(CompressionFormat::Gzip))
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
                            if let Some(ep) = &output.endpoint
                                && let Err(msg) = validate_host_port(ep)
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': {msg}",
                                )));
                            }
                            if output.format.is_some() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': {} output does not support 'format'; remove the field",
                                    output.output_type,
                                )));
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
                    if output.output_type == OutputType::ArrowIpc
                        && let Some(c) = output.compression
                        && !matches!(c, CompressionFormat::Zstd | CompressionFormat::None)
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': arrow_ipc output only supports 'zstd' or 'none' compression, not '{c}'"
                        )));
                    }

                    if !matches!(
                        output.output_type,
                        OutputType::Otlp | OutputType::Elasticsearch | OutputType::Loki
                    ) && output.tls.is_some()
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'tls' is only supported for otlp, elasticsearch, and loki outputs"
                        )));
                    }
                    if output.output_type != OutputType::Otlp {
                        if output.headers.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'headers' is only supported for otlp outputs"
                            )));
                        }
                        if output.retry_attempts.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'retry_attempts' is only supported for otlp outputs"
                            )));
                        }
                        if output.retry_initial_backoff_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'retry_initial_backoff_ms' is only supported for otlp outputs"
                            )));
                        }
                        if output.retry_max_backoff_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'retry_max_backoff_ms' is only supported for otlp outputs"
                            )));
                        }
                        if !matches!(
                            output.output_type,
                            OutputType::Elasticsearch | OutputType::Loki
                        ) && output.request_timeout_ms.is_some()
                        {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'request_timeout_ms' is only supported for otlp, elasticsearch, and loki outputs"
                            )));
                        }
                        if output.batch_timeout_ms.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'batch_timeout_ms' is only supported for otlp outputs"
                            )));
                        }
                    }

                    // Validate cross-field OTLP relationships.
                    if output.output_type == OutputType::Otlp
                        && let (Some(initial), Some(max)) =
                            (output.retry_initial_backoff_ms, output.retry_max_backoff_ms)
                        && initial > max
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'retry_initial_backoff_ms' must be <= 'retry_max_backoff_ms'"
                        )));
                    }

                    if output.output_type != OutputType::Otlp && output.protocol.is_some() {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': 'protocol' is only supported for otlp outputs"
                        )));
                    }
                    // Validate compression values per output type.
                    if let Some(c) = output.compression {
                        match output.output_type {
                            OutputType::Elasticsearch
                                if !matches!(
                                    c,
                                    CompressionFormat::Gzip | CompressionFormat::None
                                ) =>
                            {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': elasticsearch compression must be 'gzip' or 'none', got '{c}'"
                                )));
                            }
                            // OTLP accepts every `CompressionFormat` variant.
                            // ArrowIpc allows zstd/none and is validated above.
                            // Other types either reject compression entirely or accept any.
                            _ => {}
                        }
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

                    if let Some(labels) = &output.static_labels {
                        for (k, v) in labels {
                            if k.trim().is_empty() || v.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' output '{label}': 'static_labels' keys and values must not be empty"
                                )));
                            }
                        }
                    }

                    if output.output_type == OutputType::Loki
                        && let (Some(static_labels), Some(label_columns)) =
                            (&output.static_labels, &output.label_columns)
                        && let Some(conflict) = label_columns
                            .iter()
                            .map(String::as_str)
                            .find(|col| static_labels.contains_key(*col))
                    {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{label}': loki label '{conflict}' is defined in both 'label_columns' and 'static_labels'"
                        )));
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

                    if output.output_type != OutputType::ArrowIpc {
                        if output.host.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'host' is only supported for arrow_ipc outputs"
                            )));
                        }
                        if output.port.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'port' is only supported for arrow_ipc outputs"
                            )));
                        }
                        if output.write_legacy_ipc_format.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'write_legacy_ipc_format' is only supported for arrow_ipc outputs"
                            )));
                        }
                        if output.buffer_size_bytes.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'buffer_size_bytes' is only supported for arrow_ipc outputs"
                            )));
                        }
                        if output.batch_size.is_some() && output.output_type != OutputType::Otlp {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'batch_size' is only supported for otlp and arrow_ipc outputs"
                            )));
                        }
                        if output.write_schema_on_connect.is_some() {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{label}': 'write_schema_on_connect' is only supported for arrow_ipc outputs"
                            )));
                        }
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
                        EnrichmentConfig::ProcessInfo(_) => {}
                        EnrichmentConfig::NetworkInfo(_) => {}
                        EnrichmentConfig::ContainerInfo(_) => {}
                        EnrichmentConfig::K8sClusterInfo(_) => {}
                        EnrichmentConfig::EnvVars(cfg) => {
                            if cfg.table_name.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' enrichment #{j}: table_name must not be empty"
                                )));
                            }
                            if cfg.prefix.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' enrichment #{j}: env_vars 'prefix' must not be empty"
                                )));
                            }
                        }
                        EnrichmentConfig::KvFile(cfg) => {
                            if cfg.table_name.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' enrichment #{j}: table_name must not be empty"
                                )));
                            }
                            if cfg.path.trim().is_empty() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' enrichment #{j}: kv_file 'path' must not be empty"
                                )));
                            }
                            let p = Path::new(&cfg.path);
                            if p.is_absolute() && !p.exists() {
                                return Err(ConfigError::Validation(format!(
                                    "pipeline '{name}' enrichment #{j}: kv_file not found: {}",
                                    cfg.path,
                                )));
                            }
                        }
                    }
                }

                // Guard against feedback loops: reject configs where a file output
                // path matches a file input path in the same pipeline (#1596).
                // Collect file input paths (exact) and glob patterns separately.
                let mut exact_input_paths: Vec<(std::path::PathBuf, std::path::PathBuf)> =
                    Vec::new();
                let mut glob_input_patterns: Vec<String> = Vec::new();

                for input in &pipe.inputs {
                    if let InputTypeConfig::File(f) = &input.type_config {
                        let p = &f.path;
                        if p.contains('*') || p.contains('?') || p.contains('[') {
                            // Resolve globs against base_path so relative glob
                            // patterns compare correctly with resolved output paths.
                            let resolved = path_for_config_compare(p, base_path);
                            let resolved = normalize_path_key_for_compare(&resolved);
                            glob_input_patterns.push(resolved.to_string_lossy().into_owned());
                        } else {
                            let pb = path_for_config_compare(p, base_path);
                            let norm = normalize_path_key_for_compare(&pb);
                            exact_input_paths.push((pb, norm));
                        }
                    }
                }

                for (j, output) in pipe.outputs.iter().enumerate() {
                    let output = output.validation_config();
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
                    let out_pb = path_for_config_compare(out_path, base_path);
                    let out_norm = normalize_path_key_for_compare(&out_pb);

                    if let Some(prev) = seen_file_output_paths.get(&out_norm) {
                        return Err(ConfigError::Validation(format!(
                            "pipeline '{name}' output '{out_label}': file output path '{out_path}' duplicates {prev}"
                        )));
                    }
                    seen_file_output_paths.insert(
                        out_norm.clone(),
                        format!("pipeline '{name}' output '{out_label}'"),
                    );

                    // Check exact input path match.
                    for (in_pb, in_norm) in &exact_input_paths {
                        if out_norm == *in_norm {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{out_label}': output path '{}' is the same \
                             as file input path '{}' — this creates an unbounded feedback loop",
                                out_path,
                                in_pb.display(),
                            )));
                        }
                    }

                    // Check if the output path could match any glob input pattern.
                    let resolved_out_path = out_norm.to_string_lossy();
                    for glob_pattern in &glob_input_patterns {
                        if is_glob_match_possible(glob_pattern, &resolved_out_path) {
                            return Err(ConfigError::Validation(format!(
                                "pipeline '{name}' output '{out_label}': output path '{out_path}' \
                             could match file input glob '{glob_pattern}' — this creates an \
                             unbounded feedback loop",
                            )));
                        }
                    }

                    validate_file_output_path_writable(name, &out_label, out_path, base_path)?;
                }

                Ok(())
            })();
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

/// Compare paths for config-level equivalence: prefer canonical paths when they
/// exist; fall back to lexical normalisation so relative aliases like `./a.log`
/// and `logs/../a.log` are treated as the same path.
fn normalize_path_for_compare(path: &Path) -> std::path::PathBuf {
    path.canonicalize()
        .unwrap_or_else(|_| normalize_path_lexically(path))
}

fn normalize_path_key_for_compare(path: &Path) -> std::path::PathBuf {
    let normalized = normalize_path_for_compare(path);
    #[cfg(windows)]
    {
        std::path::PathBuf::from(normalized.to_string_lossy().to_lowercase())
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

fn validate_file_output_path_writable(
    pipeline_name: &str,
    output_label: &str,
    output_path: &str,
    base_path: Option<&Path>,
) -> Result<(), ConfigError> {
    let resolved = path_for_config_compare(output_path, base_path);
    let parent = match resolved.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Path::new("."),
        Some(parent) => parent,
        None => Path::new("."),
    };

    let parent_meta = parent.metadata().map_err(|e| {
        ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{output_label}': file output parent directory '{}' is not usable: {e}",
            parent.display()
        ))
    })?;
    if !parent_meta.is_dir() {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{output_label}': file output parent '{}' is not a directory",
            parent.display()
        )));
    }
    if !resolved.exists() && parent_meta.permissions().readonly() {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' output '{output_label}': file output parent '{}' is read-only",
            parent.display()
        )));
    }

    if resolved.exists() {
        let md = resolved.metadata().map_err(|e| {
            ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{output_label}': failed to inspect file output path '{}': {e}",
                resolved.display()
            ))
        })?;
        if md.is_dir() {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{output_label}': file output path '{}' is a directory",
                resolved.display()
            )));
        }
        if md.permissions().readonly() {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' output '{output_label}': file output path '{}' is read-only",
                resolved.display()
            )));
        }
    }

    Ok(())
}

fn path_for_config_compare(path: &str, base_path: Option<&Path>) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(path);
    if path.is_relative()
        && let Some(base) = base_path
    {
        // Resolve base to absolute so lexical normalization produces
        // comparable paths even when the base itself is relative.
        let abs_base = if base.is_relative() {
            std::env::current_dir().map_or_else(|_| base.to_path_buf(), |cwd| cwd.join(base))
        } else {
            base.to_path_buf()
        };
        return abs_base.join(path);
    }
    path
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

/// Normalize a systemd unit name for comparison.
///
/// Unit names without a `.` suffix get `.service` appended, matching
/// runtime behavior (e.g. `sshd` → `sshd.service`).
fn normalize_unit_name(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{name}.service")
    }
}

/// Track listen address uniqueness across all pipelines for one transport.
fn track_listen_addr_uniqueness(
    seen_listen_addrs: &mut HashMap<String, String>,
    transport: &str,
    pipeline_name: &str,
    input_label: &str,
    listen: &str,
) -> Result<(), ConfigError> {
    let Some(listen_key) = canonical_listen_addr_key(transport, listen).map_err(|msg| {
        ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': {msg}"
        ))
    })?
    else {
        return Ok(());
    };
    let current_ref = format!("pipeline '{pipeline_name}' input '{input_label}'");
    if let Some(previous_ref) = seen_listen_addrs.get(&listen_key) {
        return Err(ConfigError::Validation(format!(
            "{current_ref}: listen address '{listen}' duplicates {previous_ref}"
        )));
    }
    seen_listen_addrs.insert(listen_key, current_ref);
    Ok(())
}

fn canonical_listen_addr_key(transport: &str, listen: &str) -> Result<Option<String>, String> {
    let (host, port_str) = if listen.starts_with('[') {
        let close_bracket = listen
            .find(']')
            .ok_or_else(|| format!("'{listen}' has mismatched brackets"))?;
        if !listen[close_bracket..].starts_with("]:") {
            return Err(format!("'{listen}' is missing a port after IPv6 brackets"));
        }
        (&listen[..=close_bracket], &listen[close_bracket + 2..])
    } else {
        listen
            .rsplit_once(':')
            .ok_or_else(|| format!("'{listen}' is missing a port (expected format host:port)"))?
    };
    let port = port_str
        .parse::<u16>()
        .map_err(|_| format!("'{listen}' has an invalid port '{port_str}'"))?;
    if port == 0 {
        return Ok(None);
    }
    Ok(Some(format!(
        "{transport}:{}:{port}",
        canonical_listen_host_key(host)
    )))
}

fn canonical_listen_host_key(host: &str) -> String {
    let bare_host = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    bare_host
        .parse::<IpAddr>()
        .map_or_else(|_| bare_host.to_lowercase(), |addr| addr.to_string())
}

fn sensor_supported_families(input_type: &InputType) -> &'static [&'static str] {
    match input_type {
        InputType::LinuxEbpfSensor => &["process", "file", "network", "dns", "authz"],
        InputType::HostMetrics => {
            #[cfg(target_os = "linux")]
            {
                &["process", "file", "network", "dns", "authz"]
            }
            #[cfg(target_os = "macos")]
            {
                &["process", "file", "network", "dns", "module", "authz"]
            }
            #[cfg(target_os = "windows")]
            {
                &[
                    "process", "file", "network", "dns", "module", "registry", "authz",
                ]
            }
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            {
                &[]
            }
        }
        InputType::MacosEsSensor => &["process", "file", "network", "dns", "module", "authz"],
        InputType::WindowsEbpfSensor => &[
            "process", "file", "network", "dns", "module", "registry", "authz",
        ],
        _ => &[],
    }
}

fn sensor_supported_families_csv(input_type: &InputType) -> &'static str {
    match input_type {
        InputType::LinuxEbpfSensor => "process,file,network,dns,authz",
        InputType::HostMetrics => {
            #[cfg(target_os = "linux")]
            {
                "process,file,network,dns,authz"
            }
            #[cfg(target_os = "macos")]
            {
                "process,file,network,dns,module,authz"
            }
            #[cfg(target_os = "windows")]
            {
                "process,file,network,dns,module,registry,authz"
            }
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            {
                ""
            }
        }
        InputType::MacosEsSensor => "process,file,network,dns,module,authz",
        InputType::WindowsEbpfSensor => "process,file,network,dns,module,registry,authz",
        _ => "",
    }
}

fn is_sensor_family_supported(input_type: &InputType, name: &str) -> bool {
    sensor_supported_families(input_type).contains(&name)
}

const PLATFORM_SENSOR_EVENT_TYPES: &[&str] = &[
    "exec",
    "exit",
    "tcp_connect",
    "tcp_accept",
    "file_open",
    "file_delete",
    "file_rename",
    "setuid",
    "setgid",
    "module_load",
    "ptrace",
    "memfd_create",
    "dns_query",
];

const PLATFORM_SENSOR_EVENT_TYPES_CSV: &str = "exec,exit,tcp_connect,tcp_accept,file_open,file_delete,file_rename,setuid,setgid,module_load,ptrace,memfd_create,dns_query";

fn validate_sensor_event_type_filters(
    input_type: &InputType,
    pipeline_name: &str,
    input_label: &str,
    include_event_types: Option<&[String]>,
    exclude_event_types: Option<&[String]>,
) -> Result<(), ConfigError> {
    if include_event_types.is_none() && exclude_event_types.is_none() {
        return Ok(());
    }

    if *input_type != InputType::LinuxEbpfSensor {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': sensor.include_event_types and sensor.exclude_event_types are only supported for linux_ebpf_sensor inputs"
        )));
    }

    validate_sensor_event_type_list(
        pipeline_name,
        input_label,
        "include_event_types",
        include_event_types,
    )?;
    validate_sensor_event_type_list(
        pipeline_name,
        input_label,
        "exclude_event_types",
        exclude_event_types,
    )?;
    Ok(())
}

fn validate_sensor_event_type_list(
    pipeline_name: &str,
    input_label: &str,
    field: &str,
    event_types: Option<&[String]>,
) -> Result<(), ConfigError> {
    let Some(event_types) = event_types else {
        return Ok(());
    };

    for event_type in event_types {
        let normalized = event_type.trim();
        if normalized.is_empty() {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' input '{input_label}': sensor.{field} entries must not be empty"
            )));
        }
        if event_type != normalized {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' input '{input_label}': sensor.{field} entry '{event_type}' has leading or trailing whitespace"
            )));
        }
        if !PLATFORM_SENSOR_EVENT_TYPES.contains(&normalized) {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' input '{input_label}': unknown sensor event type '{normalized}' for linux_ebpf_sensor input (supported: {PLATFORM_SENSOR_EVENT_TYPES_CSV})"
            )));
        }
    }

    Ok(())
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
    if !addr.starts_with('[') && host.contains('[') {
        return Err(format!("'{addr}' has an unmatched '[' in the host"));
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

/// Validate an ISO8601 timestamp string: `YYYY-MM-DDTHH:MM:SSZ`.
///
/// Checks both format and semantic validity (month/day/hour/min/sec ranges,
/// including correct days-per-month with leap year handling).
///
/// Note: `logfwd-io::generator::parse_iso8601_to_epoch_ms` performs the same
/// validation plus epoch conversion.  We duplicate the range checks here because
/// `logfwd-config` cannot depend on `logfwd-io` (wrong crate-dependency direction).
fn validate_iso8601_timestamp(s: &str) -> Result<(), String> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return Err(format!(
            "must be \"now\" or YYYY-MM-DDTHH:MM:SSZ format, got {s:?}"
        ));
    }
    let digits = |off: usize, n: usize| -> Result<u32, String> {
        let mut v = 0u32;
        for i in 0..n {
            let c = b[off + i];
            if !c.is_ascii_digit() {
                return Err(format!("non-digit character in {s:?}"));
            }
            v = v * 10 + (c - b'0') as u32;
        }
        Ok(v)
    };
    let year = digits(0, 4)? as i32;
    let month = digits(5, 2)?;
    let day = digits(8, 2)?;
    let hour = digits(11, 2)?;
    let min = digits(14, 2)?;
    let sec = digits(17, 2)?;

    if !(1..=12).contains(&month) {
        return Err(format!("month {month} out of range 1-12 in {s:?}"));
    }
    if hour > 23 || min > 59 || sec > 59 {
        return Err(format!("time component out of range in {s:?}"));
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => unreachable!(),
    };
    if day < 1 || day > max_day {
        return Err(format!(
            "day {day} out of range for {year:04}-{month:02} (max {max_day}) in {s:?}"
        ));
    }
    Ok(())
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
        // Unmatched opening bracket rejected (#2060)
        assert!(
            validate_host_port("foo[bar:4317")
                .unwrap_err()
                .contains("[")
        );
    }

    #[test]
    fn validate_bind_addr_works() {
        assert!(validate_bind_addr("127.0.0.1:4317").is_ok());
        assert!(validate_bind_addr("localhost:4317").is_ok());
        assert!(validate_bind_addr("[::1]:4317").is_ok());
        assert!(validate_bind_addr("http://localhost:4317").is_err());
    }
}

/// Redact userinfo (username:password) from a URL for safe inclusion in error
/// messages.  Replaces `scheme://user:pass@host` with `scheme://***@host`.
const REDACTED_URL_USERINFO: &str = "***redacted***";

/// Redact userinfo (username:password) from a URL for safe inclusion in error
/// messages.
fn redact_url(endpoint: &str) -> String {
    // Try to parse; if that fails, just redact anything between :// and @.
    if let Ok(mut parsed) = Url::parse(endpoint) {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            let _ = parsed.set_username(REDACTED_URL_USERINFO);
            let _ = parsed.set_password(None);
        }
        parsed.to_string()
    } else if let Some(scheme_end) = endpoint.find("://") {
        let after_scheme = &endpoint[scheme_end + 3..];
        if let Some(at) = after_scheme.find('@') {
            format!(
                "{}://{}@{}",
                &endpoint[..scheme_end],
                REDACTED_URL_USERINFO,
                &after_scheme[at + 1..]
            )
        } else {
            endpoint.to_string()
        }
    } else if let Some(at) = endpoint.rfind('@') {
        // Last-resort redaction for malformed URLs: hide authority userinfo.
        let authority_start = endpoint.find("://").map_or(0, |idx| idx + 3);
        let prefix = &endpoint[..authority_start];
        let suffix = &endpoint[at + 1..];
        format!("{prefix}{REDACTED_URL_USERINFO}@{suffix}")
    } else {
        endpoint.to_string()
    }
}

/// Validate that an endpoint URL has a recognised scheme and a non-empty host.
fn validate_endpoint_url(endpoint: &str) -> Result<(), String> {
    let safe = redact_url(endpoint);

    let parsed =
        Url::parse(endpoint).map_err(|_| format!("endpoint '{safe}' is not a valid URL"))?;

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!(
            "endpoint '{safe}' must not include credentials in the URL; use output.auth instead"
        ));
    }

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
            "endpoint '{safe}' has no recognised scheme; expected 'http://' or 'https://'"
        ));
    };

    // Reject malformed authority forms like `http:///bulk` or `https://?x=1`.
    if rest.is_empty() || rest.starts_with('/') || rest.starts_with('?') || rest.starts_with('#') {
        return Err(format!("endpoint '{safe}' has no host after the scheme"));
    }

    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(format!("endpoint '{safe}' has no host after the scheme"));
    }

    Ok(())
}

/// Check if a file path could match a glob pattern by comparing the directory
/// prefix. A glob like `/var/log/*.log` has prefix `/var/log/` and would match
/// any output file in that directory with a `.log` extension.
fn is_glob_match_possible(glob_pattern: &str, file_path: &str) -> bool {
    let glob_path = Path::new(glob_pattern);
    let file = Path::new(file_path);

    let glob_dir = glob_path.parent().map(normalize_path_for_compare);
    let file_dir = file.parent().map(normalize_path_for_compare);

    let same_directory = matches!((&glob_dir, &file_dir), (Some(g), Some(f)) if g == f);
    let recursive_double_star = glob_pattern.contains("**");
    let recursive_root_match = if recursive_double_star {
        let prefix = glob_pattern
            .split("**")
            .next()
            .unwrap_or("")
            .trim_end_matches(std::path::MAIN_SEPARATOR);
        if prefix.is_empty() {
            false
        } else {
            let normalized_prefix = normalize_path_lexically(Path::new(prefix));
            let normalized_file = normalize_path_lexically(file);
            normalized_file.starts_with(&normalized_prefix)
        }
    } else {
        false
    };
    let recursive_prefix_match = if recursive_double_star {
        if let (Some(g), Some(f)) = (&glob_dir, &file_dir) {
            let mut prefix = std::path::PathBuf::new();
            let mut saw_recursive = false;
            for component in g.components() {
                if component.as_os_str() == "**" {
                    saw_recursive = true;
                    break;
                }
                prefix.push(component.as_os_str());
            }
            saw_recursive && f.starts_with(prefix)
        } else {
            false
        }
    } else {
        false
    };
    let directory_wildcard_prefix_match = {
        let raw_glob_dir = glob_path.parent().and_then(|p| p.to_str()).unwrap_or("");
        if raw_glob_dir.contains(['*', '?', '[']) {
            let prefix = raw_glob_dir
                .split(|c| ['*', '?', '['].contains(&c))
                .next()
                .unwrap_or("")
                .trim_end_matches(std::path::MAIN_SEPARATOR);
            if prefix.is_empty() {
                true
            } else {
                let normalized_prefix = normalize_path_lexically(Path::new(prefix));
                let normalized_file = normalize_path_lexically(file);
                normalized_file
                    .to_string_lossy()
                    .starts_with(normalized_prefix.to_string_lossy().as_ref())
            }
        } else {
            false
        }
    };

    // If the file is in the same directory as the glob (or the glob uses a
    // recursive `**` prefix that includes the file directory), it could match.
    if same_directory
        || recursive_prefix_match
        || recursive_root_match
        || directory_wildcard_prefix_match
    {
        // Also check filename pattern if the glob has a simple `*.ext` form.
        if let Some(glob_name) = glob_path.file_name().and_then(|n| n.to_str())
            && let Some(file_name) = file.file_name().and_then(|n| n.to_str())
        {
            if let Some(ext) = glob_name.strip_prefix('*') {
                if ext.contains(['*', '?', '[']) {
                    return true;
                }
                return file_name.ends_with(ext);
            }

            // Literal filename (no wildcard chars) must match exactly.
            if !glob_name.contains(['*', '?', '[']) {
                return glob_name == file_name;
            }

            // Pattern contains wildcard syntax we don't parse here —
            // conservatively report a possible match.
            return true;
        }
        return true;
    }
    false
}

/// Return the first illegal character in an Elasticsearch index name, or None.
/// ES rejects: uppercase, `*`, `?`, `"`, `<`, `>`, `|`, ` `, `,`, `#`, `:`, `\`, `/`.
fn es_illegal_index_char(index: &str) -> Option<char> {
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

    #[test]
    fn endpoint_url_redacts_userinfo_for_malformed_urls() {
        let err = validate_endpoint_url("https://user:secret@/bulk").expect_err("must fail");
        assert!(
            err.contains("***redacted***") && !err.contains("secret"),
            "malformed endpoint errors must redact userinfo: {err}"
        );
    }
}

#[cfg(test)]
mod feedback_loop_tests {
    use super::is_glob_match_possible;
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

    #[test]
    fn glob_could_match_literal_filename_requires_exact_name() {
        assert!(is_glob_match_possible(
            "/var/log/access.log",
            "/var/log/access.log"
        ));
        assert!(!is_glob_match_possible(
            "/var/log/access.log",
            "/var/log/other.log"
        ));
    }

    #[test]
    fn glob_could_match_wildcard_suffix_pattern() {
        assert!(is_glob_match_possible(
            "/var/log/*.log",
            "/var/log/access.log"
        ));
        assert!(!is_glob_match_possible(
            "/var/log/*.log",
            "/var/log/access.txt"
        ));
    }

    #[test]
    fn glob_could_match_rejects_different_directory() {
        assert!(!is_glob_match_possible("/var/log/*.log", "/tmp/access.log"));
    }

    #[test]
    fn glob_could_match_nested_wildcards_are_conservative() {
        assert!(is_glob_match_possible(
            "/var/log/*test*.log",
            "/var/log/mytest_file.log"
        ));
    }

    #[test]
    fn glob_could_match_recursive_double_star_matches_nested_directories() {
        assert!(is_glob_match_possible(
            "/var/log/**/access.log",
            "/var/log/subdir/access.log"
        ));
        assert!(!is_glob_match_possible(
            "/var/log/**/access.log",
            "/var/log/subdir/error.log"
        ));
    }

    #[test]
    fn glob_could_match_recursive_double_star_directory_only_pattern() {
        assert!(is_glob_match_possible(
            "/var/log/**",
            "/var/log/subdir/app.log"
        ));
        assert!(is_glob_match_possible("/var/log/**", "/var/log/app.log"));
        assert!(!is_glob_match_possible("/var/log/**", "/srv/log/app.log"));
    }

    #[test]
    fn glob_could_match_directory_wildcards_are_conservative() {
        assert!(is_glob_match_possible("/var/*/app.log", "/var/log/app.log"));
        assert!(is_glob_match_possible(
            "/var/log[12]/*.log",
            "/var/log1/a.log"
        ));
        assert!(!is_glob_match_possible(
            "/var/*/app.log",
            "/srv/log/app.log"
        ));
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

    #[test]
    fn elasticsearch_index_prefix_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: http://localhost:9200
        index: "_bad-index"
"#;
        let err = Config::load_str(yaml).unwrap_err();
        assert!(
            err.to_string().contains("has illegal prefix '_'"),
            "expected prefix rejection: {err}"
        );
    }
}

#[cfg(test)]
mod validate_http_response_tests {
    use crate::types::Config;

    #[test]
    fn http_response_body_with_204_is_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: http
        listen: 127.0.0.1:8081
        format: json
        http:
          path: /ingest
          response_code: 204
          response_body: '{"ok":true}'
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).expect_err("204 + response_body must fail validation");
        assert!(
            err.to_string()
                .contains("http.response_body is not allowed when http.response_code is 204"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn http_max_drained_bytes_per_poll_zero_is_rejected() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: http
        listen: 127.0.0.1:8081
        format: json
        http:
          max_drained_bytes_per_poll: 0
    outputs:
      - type: "null"
"#;
        let err = Config::load_str(yaml).expect_err("zero drain cap must fail validation");
        assert!(
            err.to_string()
                .contains("http.max_drained_bytes_per_poll must be at least 1"),
            "unexpected error: {err}"
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

#[cfg(test)]
mod validate_otlp_protocol_compression_tests {
    use crate::types::Config;

    #[test]
    fn otlp_valid_protocol_accepted() {
        for proto in ["http", "grpc"] {
            let yaml = format!(
                "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        protocol: {proto}\n"
            );
            Config::load_str(&yaml)
                .unwrap_or_else(|e| panic!("protocol '{proto}' should be accepted: {e}"));
        }
    }

    #[test]
    fn otlp_invalid_protocol_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        protocol: websocket\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("websocket") && msg.contains("http") && msg.contains("grpc"),
            "expected protocol rejection for 'websocket': {msg}"
        );
    }

    #[test]
    fn otlp_valid_compression_accepted() {
        for comp in ["zstd", "gzip", "none"] {
            let yaml = format!(
                "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        compression: {comp}\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| {
                panic!("compression '{comp}' should be accepted for otlp: {e}")
            });
        }
    }

    #[test]
    fn otlp_invalid_compression_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: otlp\n        endpoint: http://localhost:4317\n        compression: lz4\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("lz4") && msg.contains("zstd") && msg.contains("gzip"),
            "expected compression rejection for 'lz4': {msg}"
        );
    }

    #[test]
    fn elasticsearch_invalid_compression_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: elasticsearch\n        endpoint: http://localhost:9200\n        compression: zstd\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("compression") && msg.contains("zstd"),
            "expected compression rejection for 'zstd' on elasticsearch: {msg}"
        );
    }

    #[test]
    fn elasticsearch_valid_compression_accepted() {
        for comp in ["gzip", "none"] {
            let yaml = format!(
                "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: elasticsearch\n        endpoint: http://localhost:9200\n        compression: {comp}\n"
            );
            Config::load_str(&yaml).unwrap_or_else(|e| {
                panic!("compression '{comp}' should be accepted for elasticsearch: {e}")
            });
        }
    }
}

#[cfg(test)]
mod validate_read_buf_size_tests {
    use crate::types::Config;

    #[test]
    fn read_buf_size_upper_bound_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n        read_buf_size: 5000000\n    outputs:\n      - type: stdout\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("read_buf_size") && msg.contains("4194304"),
            "expected read_buf_size upper bound rejection: {msg}"
        );
    }

    #[test]
    fn read_buf_size_at_max_accepted() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n        read_buf_size: 4194304\n    outputs:\n      - type: stdout\n";
        Config::load_str(yaml).expect("read_buf_size at exactly 4 MiB should be accepted");
    }

    #[test]
    fn read_buf_size_just_over_max_rejected() {
        let yaml = "pipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n        read_buf_size: 4194305\n    outputs:\n      - type: stdout\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("read_buf_size") && msg.contains("4194304"),
            "expected read_buf_size upper bound rejection: {msg}"
        );
    }
}

#[cfg(test)]
mod validate_metrics_endpoint_tests {
    use crate::types::Config;

    #[test]
    fn metrics_endpoint_valid_url_accepted() {
        let yaml = "server:\n  metrics_endpoint: http://localhost:4318/v1/metrics\npipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n";
        Config::load_str(yaml).expect("valid metrics_endpoint should be accepted");
    }

    #[test]
    fn metrics_endpoint_invalid_url_rejected() {
        let yaml = "server:\n  metrics_endpoint: not-a-url\npipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metrics_endpoint"),
            "expected metrics_endpoint rejection: {msg}"
        );
    }

    #[test]
    fn metrics_endpoint_ftp_scheme_rejected() {
        let yaml = "server:\n  metrics_endpoint: ftp://localhost:21/metrics\npipelines:\n  test:\n    inputs:\n      - type: file\n        path: /tmp/test.log\n    outputs:\n      - type: stdout\n";
        let err = Config::load_str(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metrics_endpoint") && msg.contains("scheme"),
            "expected metrics_endpoint scheme rejection: {msg}"
        );
    }
}

#[cfg(test)]
mod validate_otlp_options_tests {
    use crate::types::Config;

    #[test]
    fn otlp_accepts_new_options() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        retry_attempts: 3
        retry_initial_backoff_ms: 100
        retry_max_backoff_ms: 1000
        request_timeout_ms: 5000
        batch_size: 2048
        batch_timeout_ms: 1000
        headers:
          X-Custom: value
        tls:
          insecure_skip_verify: true
"#;
        Config::load_str(yaml).expect("otlp options should be accepted");
    }

    #[test]
    fn non_otlp_rejects_new_options() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: stdout
        retry_attempts: 3
"#;
        let err = Config::load_str(yaml).unwrap_err().to_string();
        assert!(err.contains("'retry_attempts' is only supported for otlp outputs"));
    }

    #[test]
    fn elasticsearch_accepts_tls_and_request_timeout_ms() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: https://localhost:9200
        request_timeout_ms: 5000
        tls:
          insecure_skip_verify: true
"#;
        Config::load_str(yaml).expect("elasticsearch should accept tls and request_timeout_ms");
    }

    #[test]
    fn loki_accepts_tls_and_request_timeout_ms() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: loki
        endpoint: https://localhost:3100
        request_timeout_ms: 5000
        tls:
          insecure_skip_verify: true
"#;
        Config::load_str(yaml).expect("loki should accept tls and request_timeout_ms");
    }

    #[test]
    fn elasticsearch_rejects_zero_request_timeout_ms() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: elasticsearch
        endpoint: https://localhost:9200
        request_timeout_ms: 0
"#;
        // Now rejected at parse time via PositiveMillis.
        let _ = Config::load_str(yaml).expect_err("zero request_timeout_ms should be rejected");
    }

    #[test]
    fn otlp_rejects_zero_request_timeout_ms() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        request_timeout_ms: 0
"#;
        // Now rejected at parse time via PositiveMillis.
        let _ = Config::load_str(yaml).expect_err("zero request_timeout_ms should be rejected");
    }

    #[test]
    fn otlp_rejects_zero_batch_timeout_ms() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        batch_timeout_ms: 0
"#;
        // Now rejected at parse time via PositiveMillis.
        let _ = Config::load_str(yaml).expect_err("zero batch_timeout_ms should be rejected");
    }

    #[test]
    fn otlp_rejects_initial_backoff_exceeding_max() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: otlp
        endpoint: http://localhost:4317
        retry_initial_backoff_ms: 5000
        retry_max_backoff_ms: 1000
"#;
        let err = Config::load_str(yaml).unwrap_err().to_string();
        assert!(
            err.contains("retry_initial_backoff_ms") && err.contains("retry_max_backoff_ms"),
            "expected backoff ordering rejection, got: {err}"
        );
    }

    #[test]
    fn arrow_ipc_accepts_batch_size() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: file
        path: /tmp/test.log
    outputs:
      - type: arrow_ipc
        endpoint: http://localhost:9000
        batch_size: 512
"#;
        Config::load_str(yaml).expect("arrow_ipc should accept batch_size");
    }

    #[test]
    fn tcp_tls_accepts_cert_and_key() {
        let yaml = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
    outputs:
      - type: null
"#;
        Config::load_str(yaml).expect("tcp tls cert+key should validate");
    }

    #[test]
    fn tcp_tls_rejects_partial_or_mtls_fields() {
        let partial = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
    outputs:
      - type: null
"#;
        let err = Config::load_str(partial).unwrap_err().to_string();
        assert!(
            err.contains("tls.cert_file") && err.contains("tls.key_file"),
            "expected cert/key pairing validation error, got: {err}"
        );

        let mtls = r#"
pipelines:
  test:
    inputs:
      - type: tcp
        listen: 127.0.0.1:5514
        tls:
          cert_file: /tmp/server.crt
          key_file: /tmp/server.key
          client_ca_file: /tmp/ca.crt
    outputs:
      - type: null
"#;
        let err = Config::load_str(mtls).unwrap_err().to_string();
        assert!(
            err.contains("client authentication is not supported"),
            "expected mTLS rejection, got: {err}"
        );
    }
}
