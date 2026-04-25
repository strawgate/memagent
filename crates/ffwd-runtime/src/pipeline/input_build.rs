use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::BytesMut;
use ffwd_config::{
    Format, GeneratorAttributeValueConfig, GeneratorComplexityConfig, GeneratorProfileConfig,
    HostMetricsInputConfig, HttpMethodConfig, InputConfig, InputType, InputTypeConfig,
    OtlpProtobufDecodeModeConfig,
};
use ffwd_diagnostics::diagnostics::ComponentStats;
use ffwd_io::format::FormatDecoder;
use ffwd_io::framed::FramedInput;
use ffwd_io::input::{FileInput, InputSource, StdinInput};
use ffwd_io::tail::TailConfig;

use super::IngestState;

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
                "input '{name}': format {unsupported:?} is not supported for {input_type:?} inputs"
            ));
        }
    };
    Ok(proc)
}

fn validate_input_format(name: &str, input_type: InputType, format: &Format) -> Result<(), String> {
    match input_type {
        InputType::Generator | InputType::Otlp | InputType::ArrowIpc
            if !matches!(format, Format::Json) =>
        {
            return Err(format!(
                "input '{name}': format {format:?} is not supported for {input_type:?} inputs (expected json)"
            ));
        }
        InputType::Http if !matches!(format, Format::Json | Format::Raw) => {
            return Err(format!(
                "input '{name}': format {format:?} is not supported for {input_type:?} inputs (expected json or raw)"
            ));
        }
        InputType::Stdin
            if !matches!(
                format,
                Format::Cri | Format::Auto | Format::Json | Format::Raw
            ) =>
        {
            return Err(format!(
                "input '{name}': format {format:?} is not supported for {input_type:?} inputs (expected cri, auto, json, or raw)"
            ));
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

#[cfg_attr(
    feature = "otlp-research",
    allow(unused_variables, clippy::unnecessary_wraps)
)]
fn resolve_otlp_protobuf_decode_mode(
    name: &str,
    mode: Option<OtlpProtobufDecodeModeConfig>,
) -> Result<ffwd_io::otlp_receiver::OtlpProtobufDecodeMode, String> {
    match mode.unwrap_or_default() {
        OtlpProtobufDecodeModeConfig::Prost => {
            Ok(ffwd_io::otlp_receiver::OtlpProtobufDecodeMode::Prost)
        }
        OtlpProtobufDecodeModeConfig::ProjectedFallback => {
            #[cfg(feature = "otlp-research")]
            {
                Ok(ffwd_io::otlp_receiver::OtlpProtobufDecodeMode::ProjectedFallback)
            }
            #[cfg(not(feature = "otlp-research"))]
            {
                Err(format!(
                    "input '{name}': protobuf_decode_mode projected_fallback requires building with the otlp-research feature"
                ))
            }
        }
        OtlpProtobufDecodeModeConfig::ProjectedOnly => {
            #[cfg(feature = "otlp-research")]
            {
                Ok(ffwd_io::otlp_receiver::OtlpProtobufDecodeMode::ProjectedOnly)
            }
            #[cfg(not(feature = "otlp-research"))]
            {
                Err(format!(
                    "input '{name}': protobuf_decode_mode projected_only requires building with the otlp-research feature"
                ))
            }
        }
        _ => Err(format!(
            "input '{name}': unsupported protobuf_decode_mode value"
        )),
    }
}

/// Build the runtime input state (source, staging buffer, and metrics handle)
/// from a validated input config.
pub(super) fn build_input_state(
    name: &str,
    cfg: &InputConfig,
    stats: Arc<ComponentStats>,
) -> Result<IngestState, String> {
    let (raw_source, format, buf_cap): (Box<dyn InputSource>, Format, usize) = match &cfg
        .type_config
    {
        InputTypeConfig::File(f) => {
            let path = require_non_empty(name, "file", "path", Some(&f.path))?;
            let format = cfg.format.clone().unwrap_or(Format::Auto);
            let mut tail_config = TailConfig {
                start_from_end: false,
                poll_interval_ms: f.poll_interval_ms.map_or(
                    DEFAULT_FILE_POLL_INTERVAL_MS,
                    ffwd_config::PositiveMillis::get,
                ),
                read_buf_size: f.read_buf_size.unwrap_or(DEFAULT_READ_BUF_SIZE),
                per_file_read_budget_bytes: f
                    .per_file_read_budget_bytes
                    .unwrap_or(DEFAULT_PER_FILE_READ_BUDGET_BYTES),
                max_open_files: f.max_open_files.unwrap_or(DEFAULT_MAX_OPEN_FILES),
                ..Default::default()
            };
            if let Some(interval) = f.glob_rescan_interval_ms {
                tail_config.glob_rescan_interval_ms = interval;
            }
            if let Some(max) = f.adaptive_fast_polls_max {
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
        InputTypeConfig::Generator(g) => {
            use ffwd_io::generator::{
                GeneratorAttributeValue, GeneratorComplexity, GeneratorConfig,
                GeneratorGeneratedField, GeneratorInput, GeneratorProfile, GeneratorTimestamp,
                parse_iso8601_to_epoch_ms,
            };
            let generator_cfg = g.generator.as_ref();
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
                message_template: generator_cfg.and_then(|c| c.message_template.clone()),
                timestamp: match generator_cfg.and_then(|c| c.timestamp.as_ref()) {
                    None => GeneratorTimestamp::default(),
                    Some(ts) => {
                        let start_epoch_ms = match ts.start.as_deref() {
                                None => GeneratorTimestamp::default().start_epoch_ms,
                                Some(s) if s.eq_ignore_ascii_case("now") => {
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map_err(|_e| {
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
        InputTypeConfig::Otlp(o) => {
            let addr = require_non_empty(name, "otlp", "listen", Some(&o.listen))?;
            let protobuf_decode_mode =
                resolve_otlp_protobuf_decode_mode(name, o.protobuf_decode_mode)?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Otlp, &format)?;
            #[cfg(feature = "otlp-research")]
            let source = ffwd_io::otlp_receiver::OtlpReceiverInput::new_with_protobuf_decode_mode_experimental(
                name,
                addr,
                Some(Arc::clone(&stats)),
                protobuf_decode_mode,
                o.max_recv_message_size_bytes,
            )
            .map_err(|e| format!("input '{name}': failed to start OTLP receiver: {e}"))?;
            #[cfg(not(feature = "otlp-research"))]
            let source = ffwd_io::otlp_receiver::OtlpReceiverInput::new_with_stats_and_max_size(
                name,
                addr,
                Arc::clone(&stats),
                o.max_recv_message_size_bytes,
            )
            .map_err(|e| format!("input '{name}': failed to start OTLP receiver: {e}"))?;
            #[cfg(not(feature = "otlp-research"))]
            let _ = protobuf_decode_mode;
            return Ok(IngestState {
                source: Box::new(source),
                buf: BytesMut::with_capacity(4 * 1024 * 1024),
                row_origins: Vec::new(),
                source_paths: HashMap::new(),
                cri_metadata: ffwd_io::input::CriMetadata::default(),
                stats,
            });
        }
        InputTypeConfig::ArrowIpc(a) => {
            let addr = require_non_empty(name, "arrow_ipc", "listen", Some(&a.listen))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::ArrowIpc, &format)?;
            let mut options = ffwd_io::arrow_ipc_receiver::ArrowIpcReceiverOptions::default();
            if let Some(v) = a.max_connections {
                options.max_connections = v;
            }
            if let Some(v) = a.max_message_size_bytes {
                options.max_message_size_bytes = v;
            }
            let source = ffwd_io::arrow_ipc_receiver::ArrowIpcReceiver::new_with_stats(
                name,
                addr,
                options,
                Arc::clone(&stats),
            )
            .map_err(|e| format!("input '{name}': failed to start Arrow IPC receiver: {e}"))?;
            return Ok(IngestState {
                source: Box::new(source),
                buf: BytesMut::with_capacity(4 * 1024 * 1024),
                row_origins: Vec::new(),
                source_paths: HashMap::new(),
                cri_metadata: ffwd_io::input::CriMetadata::default(),
                stats,
            });
        }
        InputTypeConfig::Http(h) => {
            let addr = require_non_empty(name, "http", "listen", Some(&h.listen))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Http, &format)?;
            let mut options = ffwd_io::http_input::HttpInputOptions::default();
            if let Some(http) = &h.http {
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
                if let Some(max_drained_bytes_per_poll) = http.max_drained_bytes_per_poll {
                    options.max_drained_bytes_per_poll = max_drained_bytes_per_poll;
                }
                if let Some(response_code) = http.response_code {
                    options.response_code = response_code;
                }
                if let Some(response_body) = &http.response_body {
                    options.response_body = Some(response_body.clone());
                }
                if let Some(method) = &http.method {
                    options.method = match method {
                        HttpMethodConfig::Get => ffwd_io::http_input::HttpInputMethod::Get,
                        HttpMethodConfig::Post => ffwd_io::http_input::HttpInputMethod::Post,
                        HttpMethodConfig::Put => ffwd_io::http_input::HttpInputMethod::Put,
                        HttpMethodConfig::Delete => ffwd_io::http_input::HttpInputMethod::Delete,
                        HttpMethodConfig::Patch => ffwd_io::http_input::HttpInputMethod::Patch,
                        HttpMethodConfig::Head => ffwd_io::http_input::HttpInputMethod::Head,
                        HttpMethodConfig::Options => ffwd_io::http_input::HttpInputMethod::Options,
                        _ => return Err(format!("input '{name}': unsupported http.method value")),
                    };
                }
            }
            let source = ffwd_io::http_input::HttpInput::new_with_options(name, addr, options)
                .map_err(|e| format!("input '{name}': failed to start HTTP input: {e}"))?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputTypeConfig::Stdin(_) => {
            let format = cfg.format.clone().unwrap_or(Format::Auto);
            validate_input_format(name, InputType::Stdin, &format)?;
            let source = StdinInput::new(name);
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputTypeConfig::Udp(u) => {
            let addr = require_non_empty(name, "udp", "listen", Some(&u.listen))?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for UDP inputs (CRI is a file-based container log format)"
                ));
            }
            let mut options = ffwd_io::udp_input::UdpInputOptions::default();
            if let Some(v) = u.max_message_size_bytes {
                options.max_message_size_bytes = v;
            }
            if let Some(v) = u.so_rcvbuf {
                options.so_rcvbuf = v;
            }
            let source =
                ffwd_io::udp_input::UdpInput::with_options(name, addr, options, Arc::clone(&stats))
                    .map_err(|e| format!("input '{name}': failed to bind UDP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Udp, &format)?;
            (Box::new(source), format, 1024 * 1024)
        }
        InputTypeConfig::Tcp(t) => {
            let addr = require_non_empty(name, "tcp", "listen", Some(&t.listen))?;
            if matches!(cfg.format, Some(Format::Cri | Format::Auto)) {
                return Err(format!(
                    "input '{name}': CRI/auto format is not supported for TCP inputs (CRI is a file-based container log format)"
                ));
            }
            let mut options = ffwd_io::tcp_input::TcpInputOptions::default();
            if let Some(v) = t.max_clients {
                options.max_clients = Some(v);
            }
            if let Some(v) = t.connection_timeout_ms {
                options.connection_timeout_ms = v.get();
            }
            if let Some(v) = t.read_timeout_ms {
                options.read_timeout_ms = Some(v.get());
            }
            if let Some(tls) = &t.tls {
                let has_client_ca = tls
                    .client_ca_file
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|v| !v.is_empty());
                if tls.require_client_auth && !has_client_ca {
                    return Err(format!(
                        "input '{name}': tcp.tls.require_client_auth requires non-empty 'tcp.tls.client_ca_file'"
                    ));
                }
                if has_client_ca && !tls.require_client_auth {
                    return Err(format!(
                        "input '{name}': tcp.tls.client_ca_file requires 'tcp.tls.require_client_auth: true'"
                    ));
                }
                let cert_file =
                    require_non_empty(name, "tcp", "tcp.tls.cert_file", tls.cert_file.as_ref())?;
                let key_file =
                    require_non_empty(name, "tcp", "tcp.tls.key_file", tls.key_file.as_ref())?;
                options.tls = Some(ffwd_io::tcp_input::TcpInputTlsOptions {
                    cert_file: cert_file.to_string(),
                    key_file: key_file.to_string(),
                    client_ca_file: tls
                        .client_ca_file
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .map(str::to_string),
                    require_client_auth: tls.require_client_auth,
                });
            }
            let source =
                ffwd_io::tcp_input::TcpInput::with_options(name, addr, options, Arc::clone(&stats))
                    .map_err(|e| format!("input '{name}': failed to bind TCP {addr}: {e}"))?;
            let format = cfg.format.clone().unwrap_or(Format::Json);
            validate_input_format(name, InputType::Tcp, &format)?;
            (Box::new(source), format, 4 * 1024 * 1024)
        }
        InputTypeConfig::LinuxEbpfSensor(s) => {
            #[cfg(not(target_os = "linux"))]
            {
                let _ = s;
                return Err(format!("input '{name}': linux_ebpf_sensor requires Linux"));
            }
            #[cfg(target_os = "linux")]
            {
                use ffwd_io::platform_sensor::PlatformSensorInput;

                if cfg.format.is_some() {
                    return Err(format!(
                        "input '{name}': sensor inputs do not support 'format' (Arrow-native input)"
                    ));
                }

                // Warn on tuning fields that are ignored by the eBPF sensor.
                if let Some(sensor) = s.sensor.as_ref() {
                    if sensor.poll_interval_ms.is_some() {
                        tracing::warn!(
                            "input '{name}': sensor.poll_interval_ms is ignored for linux_ebpf_sensor (event-driven, not polled)"
                        );
                    }
                    if sensor.max_rows_per_poll.is_some() {
                        tracing::warn!(
                            "input '{name}': sensor.max_rows_per_poll is ignored for linux_ebpf_sensor; use sensor.max_events_per_poll instead"
                        );
                    }
                }

                let ebpf_path = s
                    .sensor
                    .as_ref()
                    .and_then(|c| c.ebpf_binary_path.clone())
                    .ok_or_else(|| {
                        format!(
                            "input '{name}': linux_ebpf_sensor requires 'sensor.ebpf_binary_path'"
                        )
                    })?;

                let max_events = s
                    .sensor
                    .as_ref()
                    .and_then(|c| c.max_events_per_poll)
                    .filter(|&n| n > 0)
                    .unwrap_or(1024);
                let sensor_cfg = ffwd_io::platform_sensor::PlatformSensorConfig {
                    ebpf_binary_path: ebpf_path.into(),
                    max_events_per_poll: max_events,
                    filter_self: true,
                    include_process_names: s
                        .sensor
                        .as_ref()
                        .and_then(|c| c.include_process_names.clone()),
                    exclude_process_names: s
                        .sensor
                        .as_ref()
                        .and_then(|c| c.exclude_process_names.clone()),
                    include_event_types: s
                        .sensor
                        .as_ref()
                        .and_then(|c| c.include_event_types.clone()),
                    exclude_event_types: s
                        .sensor
                        .as_ref()
                        .and_then(|c| c.exclude_event_types.clone()),
                    ring_buffer_size_kb: s.sensor.as_ref().and_then(|c| c.ring_buffer_size_kb),
                    poll_interval_ms: s
                        .sensor
                        .as_ref()
                        .and_then(|c| c.poll_interval_ms)
                        .map(ffwd_config::PositiveMillis::get),
                };

                let source = PlatformSensorInput::new(name, sensor_cfg, Arc::clone(&stats))
                    .map_err(|e| {
                        format!("input '{name}': failed to initialize eBPF sensor: {e}")
                    })?;
                return Ok(IngestState {
                    source: Box::new(source),
                    buf: BytesMut::with_capacity(64 * 1024),
                    row_origins: Vec::new(),
                    source_paths: HashMap::new(),
                    cri_metadata: ffwd_io::input::CriMetadata::default(),
                    stats,
                });
            }
        }
        InputTypeConfig::MacosEsSensor(s) | InputTypeConfig::WindowsEbpfSensor(s) => {
            use ffwd_io::host_metrics::{HostMetricsInput, HostMetricsTarget};

            let target = match &cfg.type_config {
                InputTypeConfig::MacosEsSensor(_) => HostMetricsTarget::Macos,
                InputTypeConfig::WindowsEbpfSensor(_) => HostMetricsTarget::Windows,
                _ => unreachable!("handled by outer match"),
            };

            if cfg.format.is_some() {
                return Err(format!(
                    "input '{name}': sensor inputs do not support 'format' (Arrow-native input)"
                ));
            }

            let sensor_cfg = build_host_metrics_config(s.sensor.as_ref());
            let source = HostMetricsInput::new(name, target, sensor_cfg).map_err(|e| {
                format!(
                    "input '{name}': failed to initialize {} input: {e}",
                    cfg.input_type()
                )
            })?;
            return Ok(IngestState {
                source: Box::new(source),
                buf: BytesMut::with_capacity(64 * 1024),
                row_origins: Vec::new(),
                source_paths: HashMap::new(),
                cri_metadata: ffwd_io::input::CriMetadata::default(),
                stats,
            });
        }
        InputTypeConfig::HostMetrics(s) => {
            use ffwd_io::host_metrics::{HostMetricsInput, HostMetricsTarget};

            #[cfg(target_os = "linux")]
            let target = HostMetricsTarget::Linux;
            #[cfg(target_os = "macos")]
            let target = HostMetricsTarget::Macos;
            #[cfg(target_os = "windows")]
            let target = HostMetricsTarget::Windows;
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            return Err(format!(
                "input '{name}': host_metrics is not supported on this platform"
            ));

            if cfg.format.is_some() {
                return Err(format!(
                    "input '{name}': host_metrics input does not support 'format' (Arrow-native input)"
                ));
            }

            let metrics_cfg = build_host_metrics_config(s.sensor.as_ref());
            let source = HostMetricsInput::new(name, target, metrics_cfg).map_err(|e| {
                format!("input '{name}': failed to initialize host_metrics input: {e}")
            })?;
            return Ok(IngestState {
                source: Box::new(source),
                buf: BytesMut::with_capacity(64 * 1024),
                row_origins: Vec::new(),
                source_paths: HashMap::new(),
                cri_metadata: ffwd_io::input::CriMetadata::default(),
                stats,
            });
        }
        InputTypeConfig::S3(s) => {
            let s3_cfg = &s.s3;

            // Validate required fields before feature-gated code.
            require_non_empty(name, "s3", "bucket", Some(&s3_cfg.bucket))?;

            #[cfg(not(feature = "s3"))]
            {
                let _ = s3_cfg;
                return Err(format!(
                    "input '{name}': S3 input requires the 's3' feature \
                     (rebuild with --features s3)"
                ));
            }

            #[cfg(feature = "s3")]
            {
                use ffwd_config::S3CompressionConfig;
                use ffwd_io::s3_input::decompress::Compression;
                use ffwd_io::s3_input::{S3Input, S3InputSettings};

                let compression_override: Option<Compression> = match s3_cfg.compression {
                    None => None,
                    Some(S3CompressionConfig::Auto) => None,
                    Some(S3CompressionConfig::Gzip) => Some(Compression::Gzip),
                    Some(S3CompressionConfig::Zstd) => Some(Compression::Zstd),
                    Some(S3CompressionConfig::Snappy) => Some(Compression::Snappy),
                    Some(S3CompressionConfig::None) => Some(Compression::None),
                    Some(_) => {
                        return Err(format!("input '{name}': unsupported S3 compression value"));
                    }
                };

                let settings = S3InputSettings::from_fields(
                    s3_cfg.bucket.clone(),
                    s3_cfg.region.clone(),
                    s3_cfg.endpoint.clone(),
                    s3_cfg.prefix.clone(),
                    s3_cfg.sqs_queue_url.clone(),
                    s3_cfg.start_after.clone(),
                    s3_cfg.access_key_id.clone(),
                    s3_cfg.secret_access_key.clone(),
                    s3_cfg.session_token.clone(),
                    s3_cfg.part_size_bytes,
                    s3_cfg.max_concurrent_fetches,
                    s3_cfg.max_concurrent_objects,
                    s3_cfg.visibility_timeout_secs,
                    compression_override,
                    s3_cfg
                        .poll_interval_ms
                        .map(ffwd_config::PositiveMillis::get),
                    super::source_metadata_style_needs_source_paths(cfg.source_metadata),
                )
                .map_err(|e| format!("input '{name}': {e}"))?;

                let source = S3Input::new(name, settings)
                    .map_err(|e| format!("input '{name}': failed to create S3 input: {e}"))?;
                let format = cfg.format.clone().unwrap_or(Format::Auto);
                let format_proc = make_format(name, InputType::S3, &format, &stats)?;
                let framed = FramedInput::new(Box::new(source), format_proc, Arc::clone(&stats));
                return Ok(IngestState {
                    source: Box::new(framed),
                    buf: BytesMut::with_capacity(4 * 1024 * 1024),
                    row_origins: Vec::new(),
                    source_paths: HashMap::new(),
                    cri_metadata: ffwd_io::input::CriMetadata::default(),
                    stats,
                });
            }
        }
        InputTypeConfig::MacosLog(s) => {
            let config = s.macos_log.as_ref();
            let level = config.and_then(|c| c.level.as_deref());
            let subsystem = config.and_then(|c| c.subsystem.as_deref());
            let process = config.and_then(|c| c.process.as_deref());

            let source = ffwd_io::macos_log_input::MacosLogInput::new(
                name,
                level,
                subsystem,
                process,
                Arc::clone(&stats),
            );

            (
                Box::new(source) as Box<dyn InputSource>,
                Format::Json,
                64 * 1024,
            )
        }
        InputTypeConfig::Journald(j) => {
            use ffwd_io::journald_input::{JournaldBackendPref, JournaldConfig, JournaldInput};

            // Fix #14: Reject non-JSON formats for journald — it always emits JSON.
            if let Some(ref fmt) = cfg.format
                && !matches!(fmt, Format::Json)
            {
                return Err(format!(
                    "input '{name}': journald input only supports json format (got {fmt:?})"
                ));
            }

            let jd_cfg = j.journald.as_ref();
            // Map from config crate's JournaldBackendConfig to IO crate's JournaldBackendPref.
            let backend = match jd_cfg.map(|c| c.backend).unwrap_or_default() {
                ffwd_config::JournaldBackendConfig::Auto => JournaldBackendPref::Auto,
                ffwd_config::JournaldBackendConfig::Native => JournaldBackendPref::Native,
                ffwd_config::JournaldBackendConfig::Subprocess => JournaldBackendPref::Subprocess,
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
    };

    // Wrap the raw transport with framing + format processing.
    let format_proc = make_format(name, cfg.input_type(), &format, &stats)?;
    let framed = FramedInput::new(raw_source, format_proc, Arc::clone(&stats));

    Ok(IngestState {
        source: Box::new(framed),
        buf: BytesMut::with_capacity(buf_cap),
        row_origins: Vec::new(),
        source_paths: HashMap::new(),
        cri_metadata: ffwd_io::input::CriMetadata::default(),
        stats,
    })
}

fn build_host_metrics_config(
    cfg: Option<&HostMetricsInputConfig>,
) -> ffwd_io::host_metrics::HostMetricsConfig {
    let poll_interval_ms = cfg.and_then(|c| c.poll_interval_ms).map_or(
        DEFAULT_SENSOR_POLL_INTERVAL_MS,
        ffwd_config::PositiveMillis::get,
    );
    let control_reload_interval_ms = cfg.and_then(|c| c.control_reload_interval_ms).map_or(
        DEFAULT_SENSOR_CONTROL_RELOAD_INTERVAL_MS,
        ffwd_config::PositiveMillis::get,
    );
    ffwd_io::host_metrics::HostMetricsConfig {
        poll_interval: std::time::Duration::from_millis(poll_interval_ms),
        control_path: cfg.and_then(|c| c.control_path.clone()).map(PathBuf::from),
        control_reload_interval: std::time::Duration::from_millis(control_reload_interval_ms),
        enabled_families: cfg.and_then(|c| c.enabled_families.clone()),
        emit_signal_rows: cfg.and_then(|c| c.emit_signal_rows).unwrap_or(true),
        max_rows_per_poll: cfg
            .and_then(|c| c.max_rows_per_poll)
            .filter(|&n| n > 0)
            .unwrap_or(256),
        max_process_rows_per_poll: cfg
            .and_then(|c| c.max_process_rows_per_poll)
            .and_then(std::num::NonZeroUsize::new),
    }
}

/// Returns whether OTLP input should use structured ingress mode.
///
/// Structured ingress preserves typed OTLP fields but bypasses the scanner.
/// If scanner line capture is required, route OTLP payloads through scanner
/// ingress so the configured line field is populated.
#[cfg(test)]
pub(super) fn otlp_uses_structured_ingress(
    scan_config: &ffwd_core::scan_config::ScanConfig,
) -> bool {
    !scan_config.captures_line()
}
#[cfg(test)]
mod tests {
    use super::*;
    use ffwd_config::SourceMetadataStyle;

    #[test]
    fn http_input_accepts_json_and_raw_formats() {
        assert!(validate_input_format("http", InputType::Http, &Format::Json).is_ok());
        assert!(validate_input_format("http", InputType::Http, &Format::Raw).is_ok());
    }

    #[test]
    fn stdin_input_accepts_line_or_raw_formats() {
        for format in [Format::Auto, Format::Cri, Format::Json, Format::Raw] {
            assert!(validate_input_format("stdin", InputType::Stdin, &format).is_ok());
        }
    }

    #[test]
    fn stdin_input_rejects_structured_formats() {
        for format in [
            Format::Logfmt,
            Format::Syslog,
            Format::Text,
            Format::Console,
        ] {
            let err = validate_input_format("stdin", InputType::Stdin, &format)
                .expect_err("stdin input must reject unsupported format");
            assert!(
                err.contains("expected cri, auto, json, or raw"),
                "unexpected error: {err}"
            );
        }
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
    #[cfg(not(feature = "otlp-research"))]
    fn projected_otlp_decode_mode_requires_research_feature() {
        let err = resolve_otlp_protobuf_decode_mode(
            "otlp-in",
            Some(OtlpProtobufDecodeModeConfig::ProjectedFallback),
        )
        .expect_err("projected mode should require otlp-research");
        assert!(
            err.contains("otlp-research"),
            "unexpected error for projected decode mode: {err}"
        );
    }

    #[test]
    #[cfg(feature = "otlp-research")]
    fn projected_otlp_decode_mode_maps_when_research_feature_enabled() {
        let mode = resolve_otlp_protobuf_decode_mode(
            "otlp-in",
            Some(OtlpProtobufDecodeModeConfig::ProjectedFallback),
        )
        .expect("projected mode should be available with otlp-research");
        assert_eq!(
            mode,
            ffwd_io::otlp_receiver::OtlpProtobufDecodeMode::ProjectedFallback
        );
    }

    #[test]
    fn sensor_inputs_reject_format_configuration() {
        let mut pm = ffwd_diagnostics::diagnostics::PipelineMetrics::new(
            "p",
            "SELECT 1",
            &ffwd_test_utils::test_meter(),
        );
        let stats = pm.add_input("sensor", "test");
        let (input_type_config, _input_type) = if cfg!(target_os = "linux") {
            (
                InputTypeConfig::LinuxEbpfSensor(Default::default()),
                InputType::LinuxEbpfSensor,
            )
        } else if cfg!(target_os = "macos") {
            (
                InputTypeConfig::MacosEsSensor(Default::default()),
                InputType::MacosEsSensor,
            )
        } else {
            (
                InputTypeConfig::WindowsEbpfSensor(Default::default()),
                InputType::WindowsEbpfSensor,
            )
        };
        let cfg = InputConfig {
            name: Some("sensor".to_string()),
            format: Some(Format::Raw),
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: input_type_config,
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
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);
        let stats = pm.add_input("test_in", "file");

        // Omitted / defaults
        let cfg_defaults = InputConfig {
            name: Some("test_in".into()),
            format: None,
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::File(ffwd_config::FileTypeConfig {
                path: "/tmp/test.log".into(),
                poll_interval_ms: None,
                read_buf_size: None,
                per_file_read_budget_bytes: None,
                adaptive_fast_polls_max: None,
                max_open_files: None,
                glob_rescan_interval_ms: None,
                start_at: None,
                encoding: None,
                follow_symlinks: None,
                ignore_older_secs: None,
                multiline: None,
                max_line_bytes: None,
            }),
        };

        // Note: build_input_state doesn't return the raw TailConfig directly in
        // IngestState, so we just run it to ensure it successfully builds without
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
            format: None,
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::File(ffwd_config::FileTypeConfig {
                path: "/tmp/test.log".into(),
                poll_interval_ms: ffwd_config::PositiveMillis::new(123),
                read_buf_size: Some(456),
                per_file_read_budget_bytes: Some(789),
                adaptive_fast_polls_max: Some(11),
                max_open_files: Some(10),
                glob_rescan_interval_ms: None,
                start_at: None,
                encoding: None,
                follow_symlinks: None,
                ignore_older_secs: None,
                multiline: None,
                max_line_bytes: None,
            }),
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
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);

        for (input_type, type_config_fn) in [
            (
                InputType::Udp,
                (|listen: &str| {
                    InputTypeConfig::Udp(ffwd_config::UdpTypeConfig {
                        listen: listen.to_string(),
                        max_message_size_bytes: None,
                        so_rcvbuf: None,
                    })
                }) as fn(&str) -> InputTypeConfig,
            ),
            (
                InputType::Tcp,
                (|listen: &str| {
                    InputTypeConfig::Tcp(ffwd_config::TcpTypeConfig {
                        listen: listen.to_string(),
                        tls: None,
                        max_clients: None,
                        connection_timeout_ms: None,
                        read_timeout_ms: None,
                    })
                }) as fn(&str) -> InputTypeConfig,
            ),
        ] {
            for format in [Format::Cri, Format::Auto] {
                let cfg = InputConfig {
                    name: Some("in".to_string()),
                    format: Some(format),
                    sql: None,
                    source_metadata: SourceMetadataStyle::None,
                    type_config: type_config_fn("127.0.0.1:0"),
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
    fn structured_batch_inputs_bypass_framed_poll_into_path() {
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);

        let configs = [
            InputConfig {
                name: Some("otlp-in".to_string()),
                format: None,
                sql: None,
                source_metadata: SourceMetadataStyle::None,
                type_config: InputTypeConfig::Otlp(ffwd_config::OtlpTypeConfig {
                    listen: "127.0.0.1:0".to_string(),
                    protobuf_decode_mode: None,
                    max_recv_message_size_bytes: None,
                    tls: None,
                    grpc_keepalive_time_ms: None,
                    grpc_max_concurrent_streams: None,
                }),
            },
            InputConfig {
                name: Some("arrow-in".to_string()),
                format: None,
                sql: None,
                source_metadata: SourceMetadataStyle::None,
                type_config: InputTypeConfig::ArrowIpc(ffwd_config::ArrowIpcTypeConfig {
                    listen: "127.0.0.1:0".to_string(),
                    max_connections: None,
                    max_message_size_bytes: None,
                }),
            },
        ];

        for cfg in configs {
            let name = cfg.name.as_deref().expect("test config has a name");
            let stats = pm.add_input(name, "structured");
            let mut state = build_input_state(name, &cfg, stats)
                .unwrap_or_else(|err| panic!("{name} should build: {err}"));
            assert!(
                state.source.poll_into(&mut state.buf).unwrap().is_none(),
                "{name} should use direct structured polling, not framed buffered polling"
            );
        }
    }

    #[test]
    fn build_host_metrics_config_uses_defaults() {
        let cfg = build_host_metrics_config(None);
        assert_eq!(cfg.max_rows_per_poll, 256);
        assert_eq!(cfg.max_process_rows_per_poll, None);
    }

    #[test]
    fn build_host_metrics_config_ignores_zero_caps() {
        let input = HostMetricsInputConfig {
            max_rows_per_poll: Some(0),
            max_process_rows_per_poll: Some(0),
            ..HostMetricsInputConfig::default()
        };
        let cfg = build_host_metrics_config(Some(&input));
        assert_eq!(cfg.max_rows_per_poll, 256);
        assert_eq!(cfg.max_process_rows_per_poll, None);
    }

    #[test]
    fn build_input_state_rejects_empty_file_path_and_listen() {
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);

        let file_cfg = InputConfig {
            name: Some("file-in".to_string()),
            format: Some(Format::Json),
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::File(ffwd_config::FileTypeConfig {
                path: "   ".to_string(),
                poll_interval_ms: None,
                read_buf_size: None,
                per_file_read_budget_bytes: None,
                adaptive_fast_polls_max: None,
                max_open_files: None,
                glob_rescan_interval_ms: None,
                start_at: None,
                encoding: None,
                follow_symlinks: None,
                ignore_older_secs: None,
                multiline: None,
                max_line_bytes: None,
            }),
        };
        let stats = pm.add_input("file-in", "file");
        let err = match build_input_state("file-in", &file_cfg, stats) {
            Ok(_) => panic!("empty file path should be rejected"),
            Err(err) => err,
        };
        assert!(err.contains("non-empty 'path'"), "unexpected error: {err}");

        let type_configs: Vec<(InputType, InputTypeConfig)> = vec![
            (
                InputType::Otlp,
                InputTypeConfig::Otlp(ffwd_config::OtlpTypeConfig {
                    listen: "   ".to_string(),
                    protobuf_decode_mode: None,
                    max_recv_message_size_bytes: None,
                    tls: None,
                    grpc_keepalive_time_ms: None,
                    grpc_max_concurrent_streams: None,
                }),
            ),
            (
                InputType::ArrowIpc,
                InputTypeConfig::ArrowIpc(ffwd_config::ArrowIpcTypeConfig {
                    listen: "   ".to_string(),
                    max_connections: None,
                    max_message_size_bytes: None,
                }),
            ),
            (
                InputType::Http,
                InputTypeConfig::Http(ffwd_config::HttpTypeConfig {
                    listen: "   ".to_string(),
                    http: None,
                }),
            ),
            (
                InputType::Udp,
                InputTypeConfig::Udp(ffwd_config::UdpTypeConfig {
                    listen: "   ".to_string(),
                    max_message_size_bytes: None,
                    so_rcvbuf: None,
                }),
            ),
            (
                InputType::Tcp,
                InputTypeConfig::Tcp(ffwd_config::TcpTypeConfig {
                    listen: "   ".to_string(),
                    tls: None,
                    max_clients: None,
                    connection_timeout_ms: None,
                    read_timeout_ms: None,
                }),
            ),
        ];

        for (_input_type, type_config) in type_configs {
            let cfg = InputConfig {
                name: Some("net-in".to_string()),
                format: Some(Format::Json),
                sql: None,
                source_metadata: SourceMetadataStyle::None,
                type_config,
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
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);
        let stats = pm.add_input("http-in", "http");
        let cfg = InputConfig {
            name: Some("http-in".to_string()),
            format: Some(Format::Json),
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::Http(ffwd_config::HttpTypeConfig {
                listen: "127.0.0.1:0".to_string(),
                http: Some(ffwd_config::HttpInputConfig {
                    path: Some("   ".to_string()),
                    ..Default::default()
                }),
            }),
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
    fn build_input_state_tcp_tls_reports_field_context_for_missing_files() {
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);
        let stats = pm.add_input("tcp-in", "tcp");
        let cfg = InputConfig {
            name: Some("tcp-in".to_string()),
            format: Some(Format::Json),
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::Tcp(ffwd_config::TcpTypeConfig {
                listen: "127.0.0.1:0".to_string(),
                tls: Some(ffwd_config::TlsServerConfig {
                    cert_file: Some("/definitely/missing/server.crt".to_string()),
                    key_file: Some("/definitely/missing/server.key".to_string()),
                    client_ca_file: None,
                    require_client_auth: false,
                }),
                max_clients: None,
                connection_timeout_ms: None,
                read_timeout_ms: None,
            }),
        };

        let err = match build_input_state("tcp-in", &cfg, stats) {
            Ok(_) => panic!("missing tls files should fail startup"),
            Err(err) => err,
        };
        assert!(
            err.contains("tcp.tls.cert_file") || err.contains("tcp.tls.key_file"),
            "expected tcp tls field context in startup error: {err}"
        );
    }

    #[test]
    fn build_input_state_tcp_mtls_requires_client_ca_file() {
        use ffwd_diagnostics::diagnostics::PipelineMetrics;

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("p", "SELECT 1", &meter);
        let stats = pm.add_input("tcp-in", "tcp");
        let cfg = InputConfig {
            name: Some("tcp-in".to_string()),
            format: Some(Format::Json),
            sql: None,
            source_metadata: SourceMetadataStyle::None,
            type_config: InputTypeConfig::Tcp(ffwd_config::TcpTypeConfig {
                listen: "127.0.0.1:0".to_string(),
                tls: Some(ffwd_config::TlsServerConfig {
                    cert_file: Some("/tmp/server.crt".to_string()),
                    key_file: Some("/tmp/server.key".to_string()),
                    client_ca_file: None,
                    require_client_auth: true,
                }),
                max_clients: None,
                connection_timeout_ms: None,
                read_timeout_ms: None,
            }),
        };

        let err = match build_input_state("tcp-in", &cfg, stats) {
            Ok(_) => panic!("mTLS without client_ca_file should fail startup"),
            Err(err) => err,
        };
        assert!(
            err.contains("tcp.tls.require_client_auth") && err.contains("tcp.tls.client_ca_file"),
            "expected mTLS client CA field context in startup error: {err}"
        );
    }
    #[test]
    fn otlp_structured_ingress_tracks_line_capture_flag() {
        let mut scan = ffwd_core::scan_config::ScanConfig::default();
        scan.line_field_name = None;
        assert!(
            otlp_uses_structured_ingress(&scan),
            "line capture disabled should prefer structured OTLP ingress"
        );
        scan.line_field_name = Some(ffwd_types::field_names::BODY.to_string());
        assert!(
            !otlp_uses_structured_ingress(&scan),
            "line capture enabled should force scanner ingress"
        );
    }
}
