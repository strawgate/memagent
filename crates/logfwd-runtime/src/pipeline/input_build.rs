use std::path::PathBuf;
use std::sync::Arc;

use bytes::BytesMut;
use logfwd_config::{
    Format, GeneratorAttributeValueConfig, GeneratorComplexityConfig, GeneratorProfileConfig,
    HttpMethodConfig, InputConfig, InputType, PlatformSensorInputConfig,
};
use logfwd_diagnostics::diagnostics::ComponentStats;
use logfwd_io::format::FormatDecoder;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{FileInput, InputSource};
use logfwd_io::tail::TailConfig;

use super::InputState;

// ── File-input defaults ────────────────────────────────────────────────
const DEFAULT_FILE_POLL_INTERVAL_MS: u64 = 50;
const DEFAULT_READ_BUF_SIZE: usize = 256 * 1024;
const DEFAULT_PER_FILE_READ_BUDGET_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_OPEN_FILES: usize = 1024;

// ── Generator-input defaults ───────────────────────────────────────────
const DEFAULT_GENERATOR_BATCH_SIZE: usize = 1000;

// ── Platform-sensor defaults ───────────────────────────────────────────
const DEFAULT_SENSOR_POLL_INTERVAL_MS: u64 = 10_000;
const DEFAULT_SENSOR_CONTROL_RELOAD_INTERVAL_MS: u64 = 1_000;

/// Build a format processor from the config format.
fn make_format(
    name: &str,
    input_type: InputType,
    format: &Format,
    stats: &Arc<ComponentStats>,
) -> Result<FormatDecoder, String> {
    const CRI_MAX_MESSAGE: usize = 2 * 1024 * 1024;
    let proc = match format {
        Format::Cri => FormatDecoder::cri(CRI_MAX_MESSAGE, Arc::clone(stats)),
        Format::Auto => FormatDecoder::auto(CRI_MAX_MESSAGE, Arc::clone(stats)),
        Format::Json => FormatDecoder::passthrough_json(Arc::clone(stats)),
        Format::Raw => FormatDecoder::passthrough(Arc::clone(stats)),
        unsupported => {
            return Err(format!(
                "input '{name}': format {:?} is not supported for {:?} inputs",
                unsupported, input_type
            ));
        }
    };
    Ok(proc)
}

fn validate_input_format(name: &str, input_type: InputType, format: &Format) -> Result<(), String> {
    match input_type {
        InputType::Generator | InputType::Otlp | InputType::ArrowIpc => {
            if !matches!(format, Format::Json) {
                return Err(format!(
                    "input '{name}': format {:?} is not supported for {:?} inputs (expected json)",
                    format, input_type
                ));
            }
        }
        InputType::Http => {
            if !matches!(format, Format::Json | Format::Raw) {
                return Err(format!(
                    "input '{name}': format {:?} is not supported for {:?} inputs (expected json or raw)",
                    format, input_type
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn require_non_empty<'a>(
    name: &str,
    input_type: &str,
    field: &str,
    value: Option<&'a String>,
) -> Result<&'a str, String> {
    let value = value
        .map(String::as_str)
        .ok_or_else(|| format!("input '{name}': {input_type} input requires '{field}'"))?;
    if value.trim().is_empty() {
        return Err(format!(
            "input '{name}': {input_type} input requires non-empty '{field}'"
        ));
    }
    Ok(value)
}

/// Build the runtime input state (source, staging buffer, and metrics handle)
/// from a validated input config.
pub(super) fn build_input_state(
    name: &str,
    cfg: &InputConfig,
    stats: Arc<ComponentStats>,
) -> Result<InputState, String> {
    let (raw_source, format, buf_cap): (Box<dyn InputSource>, Format, usize) = match cfg.input_type
    {
        InputType::File => {
            let path = require_non_empty(name, "file", "path", cfg.path.as_ref())?;
            let format = cfg.format.clone().unwrap_or(Format::Auto);
            let mut tail_config = TailConfig {
                start_from_end: false,
                poll_interval_ms: cfg
                    .poll_interval_ms
                    .unwrap_or(DEFAULT_FILE_POLL_INTERVAL_MS),
                read_buf_size: cfg.read_buf_size.unwrap_or(DEFAULT_READ_BUF_SIZE),
                per_file_read_budget_bytes: cfg
                    .per_file_read_budget_bytes
                    .unwrap_or(DEFAULT_PER_FILE_READ_BUDGET_BYTES),
                max_open_files: cfg.max_open_files.unwrap_or(DEFAULT_MAX_OPEN_FILES),
                ..Default::default()
            };
            if let Some(interval) = cfg.glob_rescan_interval_ms {
                tail_config.glob_rescan_interval_ms = interval;
            }
            if let Some(max) = cfg.adaptive_fast_polls_max {
                tail_config.adaptive_fast_polls_max = max;
            }
            let is_glob = path.contains('*') || path.contains('?') || path.contains('[');
            let source = if is_glob {
                FileInput::new_with_globs(
                    name.to_string(),
                    &[path],
                    tail_config,
                    Arc::clone(&stats),
                )
            } else {
                FileInput::new(
                    name.to_string(),
                    &[PathBuf::from(path)],
                    tail_config,
                    Arc::clone(&stats),
                )
            }
            .map_err(|e| format!("input '{name}': failed to create tailer: {e}"))?;
            validate_input_format(name, InputType::File, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Generator => {
            use logfwd_io::generator::{
                GeneratorAttributeValue, GeneratorComplexity, GeneratorConfig,
                GeneratorGeneratedField, GeneratorInput, GeneratorProfile, GeneratorTimestamp,
                parse_iso8601_to_epoch_ms,
            };
            let generator_cfg = cfg.generator.as_ref();
            let config = GeneratorConfig {
                events_per_sec: generator_cfg.and_then(|c| c.events_per_sec).unwrap_or(0),
                batch_size: generator_cfg
                    .and_then(|c| c.batch_size)
                    .unwrap_or(DEFAULT_GENERATOR_BATCH_SIZE),
                total_events: generator_cfg.and_then(|c| c.total_events).unwrap_or(0),
                complexity: match generator_cfg.and_then(|c| c.complexity.clone()) {
                    Some(GeneratorComplexityConfig::Complex) => GeneratorComplexity::Complex,
                    Some(GeneratorComplexityConfig::Simple) | None => GeneratorComplexity::Simple,
                    // Non-exhaustive config enum: future variants default to
                    // Simple to preserve backward-compatible behavior.
                    Some(_) => GeneratorComplexity::Simple,
                },
                profile: match generator_cfg.and_then(|c| c.profile.clone()) {
                    Some(GeneratorProfileConfig::Record) => GeneratorProfile::Record,
                    Some(GeneratorProfileConfig::Logs) | None => GeneratorProfile::Logs,
                    // Non-exhaustive config enum: future variants default to
                    // Logs to preserve backward-compatible behavior.
                    Some(_) => GeneratorProfile::Logs,
                },
                attributes: generator_cfg
                    .map(|c| {
                        c.attributes
                            .iter()
                            .map(|(k, v)| {
                                let value = match v {
                                    GeneratorAttributeValueConfig::String(v) => {
                                        GeneratorAttributeValue::String(v.clone())
                                    }
                                    GeneratorAttributeValueConfig::Null => {
                                        GeneratorAttributeValue::Null
                                    }
                                    GeneratorAttributeValueConfig::Integer(v) => {
                                        GeneratorAttributeValue::Integer(*v)
                                    }
                                    GeneratorAttributeValueConfig::Float(v) => {
                                        GeneratorAttributeValue::Float(*v)
                                    }
                                    GeneratorAttributeValueConfig::Bool(v) => {
                                        GeneratorAttributeValue::Bool(*v)
                                    }
                                    // Non-exhaustive config enum: preserve current behavior
                                    // by mapping future variants to JSON null.
                                    _ => GeneratorAttributeValue::Null,
                                };
                                (k.clone(), value)
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                sequence: generator_cfg.and_then(|c| {
                    c.sequence.as_ref().map(|seq| GeneratorGeneratedField {
                        field: seq.field.clone(),
                        start: seq.start.unwrap_or(1),
                    })
                }),
                event_created_unix_nano_field: generator_cfg
                    .and_then(|c| c.event_created_unix_nano_field.clone()),
                timestamp: match generator_cfg.and_then(|c| c.timestamp.as_ref()) {
                    None => GeneratorTimestamp::default(),
                    Some(ts) => {
                        let start_epoch_ms = match ts.start.as_deref() {
                            None => GeneratorTimestamp::default().start_epoch_ms,
                            Some(s) if s.eq_ignore_ascii_case("now") => {
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map_err(|_| {
                                        format!("input '{name}': system clock is before Unix epoch, cannot resolve timestamp.start=\"now\"")
                                    })?
                                    .as_millis() as i64
                            }
                            Some(s) => parse_iso8601_to_epoch_ms(s).map_err(|e| {
                                format!("input '{name}': invalid timestamp.start: {e}")
                            })?,
                        };
                        GeneratorTimestamp {
                            start_epoch_ms,
                            step_ms: ts.step_ms.unwrap_or(1),
                        }
                    }
                },
            };
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Generator, &format)?;
            let source = GeneratorInput::new(name, config);
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Otlp => {
            let addr = require_non_empty(name, "otlp", "listen", cfg.listen.as_ref())?;
            let resource_prefix = cfg
                .resource_prefix
                .as_deref()
                .unwrap_or(logfwd_types::field_names::DEFAULT_RESOURCE_PREFIX);
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Otlp, &format)?;
            let source =
                logfwd_io::otlp_receiver::OtlpReceiverInput::new_with_stats_and_resource_prefix(
                    name,
                    addr,
                    Arc::clone(&stats),
                    resource_prefix,
                )
                .map_err(|e| format!("input '{name}': failed to start OTLP receiver: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::ArrowIpc => {
            let addr = require_non_empty(name, "arrow_ipc", "listen", cfg.listen.as_ref())?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::ArrowIpc, &format)?;
            let source = logfwd_io::arrow_ipc_receiver::ArrowIpcReceiver::new(name, addr)
                .map_err(|e| format!("input '{name}': failed to start Arrow IPC receiver: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Http => {
            let addr = require_non_empty(name, "http", "listen", cfg.listen.as_ref())?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Http, &format)?;
            let mut options = logfwd_io::http_input::HttpInputOptions::default();
            if let Some(http) = &cfg.http {
                if let Some(path) = &http.path {
                    if path.trim().is_empty() {
                        return Err(format!(
                            "input '{name}': http input requires non-empty 'http.path' when provided"
                        ));
                    }
                    options.path = path.clone();
                }
                if let Some(strict_path) = http.strict_path {
                    options.strict_path = strict_path;
                }
                if let Some(max_request_body_size) = http.max_request_body_size {
                    options.max_request_body_size = max_request_body_size;
                }
                if let Some(response_code) = http.response_code {
                    options.response_code = response_code;
                }
                if let Some(response_body) = &http.response_body {
                    options.response_body = Some(response_body.clone());
                }
                if let Some(method) = &http.method {
                    options.method = match method {
                        HttpMethodConfig::Get => logfwd_io::http_input::HttpInputMethod::Get,
                        HttpMethodConfig::Post => logfwd_io::http_input::HttpInputMethod::Post,
                        HttpMethodConfig::Put => logfwd_io::http_input::HttpInputMethod::Put,
                        HttpMethodConfig::Delete => logfwd_io::http_input::HttpInputMethod::Delete,
                        HttpMethodConfig::Patch => logfwd_io::http_input::HttpInputMethod::Patch,
                        HttpMethodConfig::Head => logfwd_io::http_input::HttpInputMethod::Head,
                        HttpMethodConfig::Options => {
                            logfwd_io::http_input::HttpInputMethod::Options
                        }
                    };
                }
            }
            let source = logfwd_io::http_input::HttpInput::new_with_options(name, addr, options)
                .map_err(|e| format!("input '{name}': failed to start HTTP input: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::Udp => {
            let addr = require_non_empty(name, "udp", "listen", cfg.listen.as_ref())?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for UDP inputs (CRI is a file-based container log format)"
                ));
            }
            let source = logfwd_io::udp_input::UdpInput::new(name, addr, Arc::clone(&stats))
                .map_err(|e| format!("input '{name}': failed to bind UDP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Udp, &format)?;
            (Box::new(source), format, 1024 * 1024)
        }
        InputType::Tcp => {
            let addr = require_non_empty(name, "tcp", "listen", cfg.listen.as_ref())?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for TCP inputs (CRI is a file-based container log format)"
                ));
            }
            let source = logfwd_io::tcp_input::TcpInput::new(name, addr, Arc::clone(&stats))
                .map_err(|e| format!("input '{name}': failed to bind TCP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Tcp, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputType::LinuxEbpfSensor | InputType::MacosEsSensor | InputType::WindowsEbpfSensor => {
            use logfwd_io::platform_sensor::{PlatformSensorInput, PlatformSensorTarget};

            let target = match cfg.input_type {
                InputType::LinuxEbpfSensor => PlatformSensorTarget::Linux,
                InputType::MacosEsSensor => PlatformSensorTarget::Macos,
                InputType::WindowsEbpfSensor => PlatformSensorTarget::Windows,
                _ => unreachable!("handled by outer match"),
            };

            if cfg.format.is_some() {
                return Err(format!(
                    "input '{name}': sensor inputs do not support 'format' (Arrow-native input)"
                ));
            }

            let sensor_cfg = build_platform_sensor_config(cfg.sensor.as_ref());
            let source = PlatformSensorInput::new(name, target, sensor_cfg).map_err(|e| {
                format!(
                    "input '{name}': failed to initialize {} input: {e}",
                    cfg.input_type
                )
            })?;
            return Ok(InputState {
                source: Box::new(source),
                buf: BytesMut::with_capacity(64 * 1024),
                stats,
            });
        }
        InputType::Journald => {
            use logfwd_io::journald_input::{JournaldBackendPref, JournaldConfig, JournaldInput};

            // Fix #14: Reject non-JSON formats for journald — it always emits JSON.
            if let Some(ref fmt) = cfg.format {
                if !matches!(fmt, Format::Json) {
                    return Err(format!(
                        "input '{name}': journald input only supports json format (got {fmt:?})"
                    ));
                }
            }

            let jd_cfg = cfg.journald.as_ref();
            // Map from config crate's JournaldBackendConfig to IO crate's JournaldBackendPref.
            let backend = match jd_cfg.map(|c| c.backend).unwrap_or_default() {
                logfwd_config::JournaldBackendConfig::Auto => JournaldBackendPref::Auto,
                logfwd_config::JournaldBackendConfig::Native => JournaldBackendPref::Native,
                logfwd_config::JournaldBackendConfig::Subprocess => JournaldBackendPref::Subprocess,
            };
            let config = JournaldConfig {
                include_units: jd_cfg.map(|c| c.include_units.clone()).unwrap_or_default(),
                exclude_units: jd_cfg.map(|c| c.exclude_units.clone()).unwrap_or_default(),
                current_boot_only: jd_cfg.is_none_or(|c| c.current_boot_only),
                since_now: jd_cfg.is_some_and(|c| c.since_now),
                journalctl_path: jd_cfg
                    .and_then(|c| c.journalctl_path.clone())
                    .unwrap_or_else(|| "journalctl".to_string()),
                journal_directory: jd_cfg.and_then(|c| c.journal_directory.clone()),
                journal_namespace: jd_cfg.and_then(|c| c.journal_namespace.clone()),
                backend,
            };
            let format = cfg.format.clone().unwrap_or(Format::Json);
            let source = JournaldInput::new(name, config, Arc::clone(&stats))
                .map_err(|e| format!("input '{name}': failed to start journald receiver: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        _ => {
            return Err(format!(
                "input '{name}': type {:?} not yet supported",
                cfg.input_type
            ));
        }
    };

    // Wrap the raw transport with framing + format processing.
    let format_proc = make_format(name, cfg.input_type.clone(), &format, &stats)?;
    let framed = FramedInput::new(raw_source, format_proc, Arc::clone(&stats));

    Ok(InputState {
        source: Box::new(framed),
        buf: BytesMut::with_capacity(buf_cap),
        stats,
    })
}

fn build_platform_sensor_config(
    cfg: Option<&PlatformSensorInputConfig>,
) -> logfwd_io::platform_sensor::PlatformSensorConfig {
    let poll_interval_ms = cfg
        .and_then(|c| c.poll_interval_ms)
        .unwrap_or(DEFAULT_SENSOR_POLL_INTERVAL_MS);
    let control_reload_interval_ms = cfg
        .and_then(|c| c.control_reload_interval_ms)
        .unwrap_or(DEFAULT_SENSOR_CONTROL_RELOAD_INTERVAL_MS);
    logfwd_io::platform_sensor::PlatformSensorConfig {
        poll_interval: std::time::Duration::from_millis(poll_interval_ms.max(1)),
        control_path: cfg.and_then(|c| c.control_path.clone()).map(PathBuf::from),
        control_reload_interval: std::time::Duration::from_millis(
            control_reload_interval_ms.max(1),
        ),
        enabled_families: cfg.and_then(|c| c.enabled_families.clone()),
        emit_signal_rows: cfg.and_then(|c| c.emit_signal_rows).unwrap_or(true),
    }
}

/// Returns whether OTLP input should use structured ingress mode.
///
/// Structured ingress preserves typed OTLP fields but bypasses the scanner.
/// If scanner line capture is required, use legacy scanner ingress.
#[cfg(test)]
pub(super) fn otlp_uses_structured_ingress(
    scan_config: &logfwd_core::scan_config::ScanConfig,
) -> bool {
    !scan_config.captures_line()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_input_accepts_json_and_raw_formats() {
        assert!(validate_input_format("http", InputType::Http, &Format::Json).is_ok());
        assert!(validate_input_format("http", InputType::Http, &Format::Raw).is_ok());
    }

    #[test]
    fn http_input_rejects_non_json_raw_formats() {
        let err = validate_input_format("http", InputType::Http, &Format::Cri)
            .expect_err("http input must reject cri format");
        assert!(
            err.contains("expected json or raw"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generator_otlp_and_arrow_ipc_require_json_format() {
        for input_type in [InputType::Generator, InputType::Otlp, InputType::ArrowIpc] {
            assert!(validate_input_format("in", input_type.clone(), &Format::Json).is_ok());
            let err = validate_input_format("in", input_type, &Format::Raw)
                .expect_err("non-json format must be rejected");
            assert!(err.contains("expected json"), "unexpected error: {err}");
        }
    }

    #[test]
    fn sensor_inputs_reject_format_configuration() {
        let mut pm = logfwd_diagnostics::diagnostics::PipelineMetrics::new(
            "p",
            "SELECT 1",
            &logfwd_test_utils::test_meter(),
        );
        let stats = pm.add_input("sensor", "test");
        let input_type = if cfg!(target_os = "linux") {
            InputType::LinuxEbpfSensor
        } else if cfg!(target_os = "macos") {
            InputType::MacosEsSensor
        } else {
            InputType::WindowsEbpfSensor
        };
        let cfg = InputConfig {
            name: Some("sensor".to_string()),
            input_type,
            path: None,
            listen: None,
            resource_prefix: None,
            format: Some(Format::Raw),
            poll_interval_ms: None,
            read_buf_size: None,
            per_file_read_budget_bytes: None,
            adaptive_fast_polls_max: None,
            max_open_files: None,
            glob_rescan_interval_ms: None,
            generator: None,
            http: None,
            sensor: Some(Default::default()),
            sql: None,
            tls: None,
            journald: None,
        };
        let err = match build_input_state("sensor", &cfg, stats) {
            Ok(_) => panic!("sensor format must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.contains("do not support 'format'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_input_state_file_tuning_knobs() {
        use logfwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);
        let stats = pm.add_input("test_in", "file");

        // Omitted / defaults
        let cfg_defaults = InputConfig {
            name: Some("test_in".into()),
            input_type: InputType::File,
            path: Some("/tmp/test.log".into()),
            listen: None,
            resource_prefix: None,
            format: None,
            poll_interval_ms: None,
            read_buf_size: None,
            per_file_read_budget_bytes: None,
            adaptive_fast_polls_max: None,
            max_open_files: None,
            glob_rescan_interval_ms: None,
            generator: None,
            sensor: None,
            http: None,
            sql: None,
            tls: None,
            journald: None,
        };

        // Note: build_input_state doesn't return the raw TailConfig directly in
        // InputState, so we just run it to ensure it successfully builds without
        // error. A more involved test requires exposing or inspecting the internal
        // file tailer state, but here we at least verify it parses and maps defaults
        // cleanly for a valid file input configuration.
        let state = build_input_state("test_in", &cfg_defaults, Arc::clone(&stats))
            .expect("default file input should build");
        assert_eq!(
            state.source.get_cadence().adaptive_fast_polls_max,
            8,
            "default adaptive_fast_polls_max should come from TailConfig default"
        );

        // Explicit tuning overrides
        let cfg_overrides = InputConfig {
            name: Some("test_in".into()),
            input_type: InputType::File,
            path: Some("/tmp/test.log".into()),
            listen: None,
            resource_prefix: None,
            format: None,
            poll_interval_ms: Some(123),
            read_buf_size: Some(456),
            per_file_read_budget_bytes: Some(789),
            adaptive_fast_polls_max: Some(11),
            max_open_files: Some(10),
            glob_rescan_interval_ms: None,
            generator: None,
            sensor: None,
            http: None,
            sql: None,
            tls: None,
            journald: None,
        };

        let state = build_input_state("test_in", &cfg_overrides, Arc::clone(&stats))
            .expect("overridden file input should build");
        assert_eq!(
            state.source.get_cadence().adaptive_fast_polls_max,
            11,
            "adaptive_fast_polls_max override should be forwarded"
        );
    }

    #[test]
    fn build_input_state_rejects_udp_tcp_cri_and_auto_formats() {
        use logfwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);

        for input_type in [InputType::Udp, InputType::Tcp] {
            for format in [Format::Cri, Format::Auto] {
                let cfg = InputConfig {
                    name: Some("in".to_string()),
                    input_type: input_type.clone(),
                    path: None,
                    listen: Some("127.0.0.1:0".to_string()),
                    resource_prefix: None,
                    format: Some(format),
                    poll_interval_ms: None,
                    read_buf_size: None,
                    per_file_read_budget_bytes: None,
                    adaptive_fast_polls_max: None,
                    max_open_files: None,
                    glob_rescan_interval_ms: None,
                    generator: None,
                    http: None,
                    sensor: None,
                    sql: None,
                    tls: None,
                    journald: None,
                };
                let stats = pm.add_input("in", "test");
                let err = match build_input_state("in", &cfg, stats) {
                    Ok(_) => panic!("CRI/auto must be rejected for UDP/TCP inputs"),
                    Err(err) => err,
                };
                assert!(
                    err.contains("not supported"),
                    "unexpected error for {:?}/{:?}: {err}",
                    input_type,
                    cfg.format
                );
            }
        }
    }

    #[test]
    fn build_input_state_rejects_empty_file_path_and_listen() {
        use logfwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);

        let file_cfg = InputConfig {
            name: Some("file-in".to_string()),
            input_type: InputType::File,
            path: Some("   ".to_string()),
            listen: None,
            resource_prefix: None,
            format: Some(Format::Json),
            poll_interval_ms: None,
            read_buf_size: None,
            per_file_read_budget_bytes: None,
            adaptive_fast_polls_max: None,
            max_open_files: None,
            glob_rescan_interval_ms: None,
            generator: None,
            http: None,
            sensor: None,
            sql: None,
            tls: None,
            journald: None,
        };
        let stats = pm.add_input("file-in", "file");
        let err = match build_input_state("file-in", &file_cfg, stats) {
            Ok(_) => panic!("empty file path should be rejected"),
            Err(err) => err,
        };
        assert!(err.contains("non-empty 'path'"), "unexpected error: {err}");

        for input_type in [
            InputType::Otlp,
            InputType::ArrowIpc,
            InputType::Http,
            InputType::Udp,
            InputType::Tcp,
        ] {
            let cfg = InputConfig {
                name: Some("net-in".to_string()),
                input_type,
                path: None,
                listen: Some("   ".to_string()),
                resource_prefix: None,
                format: Some(Format::Json),
                poll_interval_ms: None,
                read_buf_size: None,
                per_file_read_budget_bytes: None,
                adaptive_fast_polls_max: None,
                max_open_files: None,
                glob_rescan_interval_ms: None,
                generator: None,
                http: None,
                sensor: None,
                sql: None,
                tls: None,
                journald: None,
            };
            let stats = pm.add_input("net-in", "net");
            let err = match build_input_state("net-in", &cfg, stats) {
                Ok(_) => panic!("empty listen should be rejected"),
                Err(err) => err,
            };
            assert!(
                err.contains("non-empty 'listen'"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn build_input_state_rejects_empty_http_path_override() {
        use logfwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = logfwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);
        let stats = pm.add_input("http-in", "http");
        let cfg = InputConfig {
            name: Some("http-in".to_string()),
            input_type: InputType::Http,
            path: None,
            listen: Some("127.0.0.1:0".to_string()),
            resource_prefix: None,
            format: Some(Format::Json),
            poll_interval_ms: None,
            read_buf_size: None,
            per_file_read_budget_bytes: None,
            adaptive_fast_polls_max: None,
            max_open_files: None,
            glob_rescan_interval_ms: None,
            generator: None,
            http: Some(logfwd_config::HttpInputConfig {
                path: Some("   ".to_string()),
                ..Default::default()
            }),
            sensor: None,
            sql: None,
            tls: None,
            journald: None,
        };
        let err = match build_input_state("http-in", &cfg, stats) {
            Ok(_) => panic!("empty http.path override should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.contains("non-empty 'http.path'"),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn otlp_structured_ingress_tracks_line_capture_flag() {
        let mut scan = logfwd_core::scan_config::ScanConfig::default();
        scan.line_field_name = None;
        assert!(
            otlp_uses_structured_ingress(&scan),
            "line capture disabled should prefer structured OTLP ingress"
        );
        scan.line_field_name = Some(logfwd_types::field_names::BODY.to_string());
        assert!(
            !otlp_uses_structured_ingress(&scan),
            "line capture enabled should force legacy scanner ingress"
        );
    }
}
