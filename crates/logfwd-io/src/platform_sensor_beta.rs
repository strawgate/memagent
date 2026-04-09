//! Cross-platform beta sensor input.
//!
//! This source now emits Arrow `RecordBatch` rows directly. It includes a
//! lightweight runtime control plane (optional JSON file reload) and explicit
//! per-platform signal families so we can iterate toward production sensors
//! without routing synthetic JSON through text decoders.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_types::diagnostics::ComponentHealth;
use serde::Deserialize;

use crate::input::{InputEvent, InputSource};

/// Platform target for a beta sensor input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformSensorTarget {
    Linux,
    Macos,
    Windows,
}

impl PlatformSensorTarget {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Macos => "macos",
            Self::Windows => "windows",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SignalFamily {
    Process,
    File,
    Network,
    Dns,
    Module,
    Registry,
    Authz,
}

impl SignalFamily {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::File => "file",
            Self::Network => "network",
            Self::Dns => "dns",
            Self::Module => "module",
            Self::Registry => "registry",
            Self::Authz => "authz",
        }
    }

    fn parse(name: &str) -> Result<Self, String> {
        match name {
            "process" => Ok(Self::Process),
            "file" => Ok(Self::File),
            "network" => Ok(Self::Network),
            "dns" => Ok(Self::Dns),
            "module" => Ok(Self::Module),
            "registry" => Ok(Self::Registry),
            "authz" => Ok(Self::Authz),
            other => Err(format!(
                "unknown sensor family '{other}' (supported: process,file,network,dns,module,registry,authz)"
            )),
        }
    }
}

const LINUX_FAMILIES: &[SignalFamily] = &[
    SignalFamily::Process,
    SignalFamily::File,
    SignalFamily::Network,
    SignalFamily::Dns,
    SignalFamily::Authz,
];

const MACOS_FAMILIES: &[SignalFamily] = &[
    SignalFamily::Process,
    SignalFamily::File,
    SignalFamily::Network,
    SignalFamily::Dns,
    SignalFamily::Module,
    SignalFamily::Authz,
];

const WINDOWS_FAMILIES: &[SignalFamily] = &[
    SignalFamily::Process,
    SignalFamily::File,
    SignalFamily::Network,
    SignalFamily::Dns,
    SignalFamily::Module,
    SignalFamily::Registry,
    SignalFamily::Authz,
];

fn target_signal_families(target: PlatformSensorTarget) -> &'static [SignalFamily] {
    match target {
        PlatformSensorTarget::Linux => LINUX_FAMILIES,
        PlatformSensorTarget::Macos => MACOS_FAMILIES,
        PlatformSensorTarget::Windows => WINDOWS_FAMILIES,
    }
}

/// Runtime options for platform beta inputs.
#[derive(Debug, Clone)]
pub struct PlatformSensorBetaConfig {
    /// Emit periodic heartbeat rows when the source is idle.
    pub emit_heartbeat: bool,
    /// Heartbeat cadence.
    pub poll_interval: Duration,
    /// Optional JSON control-plane file for runtime sensor tuning.
    pub control_path: Option<PathBuf>,
    /// How often to probe `control_path` for updates.
    pub control_reload_interval: Duration,
    /// Optional explicit signal families to enable.
    ///
    /// `None` means "use platform defaults". `Some([])` means "disable all".
    pub enabled_families: Option<Vec<String>>,
    /// Emit periodic per-family sample rows.
    pub emit_signal_rows: bool,
}

impl Default for PlatformSensorBetaConfig {
    fn default() -> Self {
        Self {
            emit_heartbeat: true,
            poll_interval: Duration::from_millis(10_000),
            control_path: None,
            control_reload_interval: Duration::from_millis(1_000),
            enabled_families: None,
            emit_signal_rows: true,
        }
    }
}

/// Beta input source for per-platform sensor bring-up.
#[derive(Debug)]
pub struct PlatformSensorBetaInput {
    machine: Option<PlatformSensorMachine>,
}

#[derive(Debug)]
enum PlatformSensorMachine {
    Init(PlatformSensorState<InitState>),
    Running(PlatformSensorState<RunningState>),
}

#[derive(Debug)]
struct PlatformSensorCommon {
    name: String,
    target: PlatformSensorTarget,
    host_platform: &'static str,
    cfg: PlatformSensorBetaConfig,
    schema: Arc<Schema>,
}

#[derive(Debug)]
struct PlatformSensorState<S> {
    common: PlatformSensorCommon,
    state: S,
}

#[derive(Debug)]
struct InitState {
    control: ControlState,
}

#[derive(Debug)]
struct RunningState {
    last_emit: Instant,
    last_control_check: Instant,
    control: ControlState,
}

#[derive(Debug, Clone)]
struct ControlState {
    generation: u64,
    enabled_families: Vec<SignalFamily>,
    source: ControlSource,
    emit_heartbeat: bool,
    emit_signal_rows: bool,
}

#[derive(Debug, Clone, Copy)]
enum ControlSource {
    StaticConfig,
    ControlFile,
}

impl ControlSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::StaticConfig => "static_config",
            Self::ControlFile => "control_file",
        }
    }
}

#[derive(Debug)]
struct SensorRow {
    timestamp_unix_nano: u64,
    event_family: String,
    event_kind: String,
    signal_family: Option<String>,
    signal_status: String,
    control_generation: u64,
    control_source: String,
    control_path: Option<String>,
    enabled_families: Option<String>,
    effective_emit_heartbeat: Option<bool>,
    effective_emit_signal_rows: Option<bool>,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlFileConfig {
    generation: Option<u64>,
    enabled_families: Option<Vec<String>>,
    emit_heartbeat: Option<bool>,
    emit_signal_rows: Option<bool>,
}

impl PlatformSensorState<InitState> {
    fn start(self) -> io::Result<(PlatformSensorState<RunningState>, Vec<InputEvent>)> {
        let mut rows = vec![self.common.control_row(
            &self.state.control,
            "startup",
            "beta sensor startup complete",
            "ok",
        )];
        rows.extend(self.common.capability_rows(&self.state.control));
        rows.extend(self.common.signal_sample_rows(
            &self.state.control,
            "startup_sample",
            "initial signal snapshot",
        ));

        let event = self.common.build_batch_event(rows)?;
        let running = PlatformSensorState {
            common: self.common,
            state: RunningState {
                last_emit: Instant::now(),
                last_control_check: Instant::now(),
                control: self.state.control,
            },
        };
        Ok((running, vec![event]))
    }
}

impl PlatformSensorState<RunningState> {
    fn poll_rows(&mut self) -> Vec<SensorRow> {
        let mut rows = Vec::new();

        if let Some(reload_rows) = self.try_reload_control() {
            rows.extend(reload_rows);
        }

        if self.state.control.emit_heartbeat
            && self.state.last_emit.elapsed() >= self.common.cfg.poll_interval
        {
            rows.push(self.common.control_row(
                &self.state.control,
                "heartbeat",
                "beta sensor heartbeat",
                "ok",
            ));
            rows.extend(self.common.signal_sample_rows(
                &self.state.control,
                "heartbeat_sample",
                "periodic signal snapshot",
            ));
            self.state.last_emit = Instant::now();
        }

        rows
    }

    fn try_reload_control(&mut self) -> Option<Vec<SensorRow>> {
        let path = self.common.cfg.control_path.as_ref()?;
        if self.state.last_control_check.elapsed() < self.common.cfg.control_reload_interval {
            return None;
        }
        self.state.last_control_check = Instant::now();

        match read_control_file(path) {
            Ok(None) => None,
            Ok(Some(file_cfg)) => {
                let mut next = self.state.control.clone();
                if let Some(enabled) = file_cfg.enabled_families {
                    let parsed = match parse_enabled_families(Some(&enabled), self.common.target) {
                        Ok(v) => v,
                        Err(e) => {
                            return Some(vec![self.common.control_row(
                                &self.state.control,
                                "control_reload_failed",
                                &format!("invalid enabled_families in control file: {e}"),
                                "error",
                            )]);
                        }
                    };
                    next.enabled_families = parsed;
                }
                if let Some(v) = file_cfg.emit_heartbeat {
                    next.emit_heartbeat = v;
                }
                if let Some(v) = file_cfg.emit_signal_rows {
                    next.emit_signal_rows = v;
                }
                next.source = ControlSource::ControlFile;
                let generation_changed = file_cfg
                    .generation
                    .is_some_and(|generation| generation != self.state.control.generation);

                let changed = next.enabled_families != self.state.control.enabled_families
                    || next.emit_heartbeat != self.state.control.emit_heartbeat
                    || next.emit_signal_rows != self.state.control.emit_signal_rows
                    || generation_changed;

                if !changed {
                    return None;
                }

                next.generation = file_cfg
                    .generation
                    .unwrap_or_else(|| self.state.control.generation.saturating_add(1));
                self.state.control = next.clone();

                let mut rows = vec![self.common.control_row(
                    &next,
                    "control_reload_applied",
                    "applied control file settings",
                    "ok",
                )];
                rows.extend(self.common.capability_rows(&next));
                rows.extend(self.common.signal_sample_rows(
                    &next,
                    "control_reload_sample",
                    "signal snapshot after control reload",
                ));
                Some(rows)
            }
            Err(e) => Some(vec![self.common.control_row(
                &self.state.control,
                "control_reload_failed",
                &format!("failed to load control file: {e}"),
                "error",
            )]),
        }
    }
}

impl PlatformSensorCommon {
    fn control_row(
        &self,
        control: &ControlState,
        event_kind: &str,
        message: &str,
        signal_status: &str,
    ) -> SensorRow {
        SensorRow {
            timestamp_unix_nano: now_unix_nano(),
            event_family: "sensor_control".to_string(),
            event_kind: event_kind.to_string(),
            signal_family: None,
            signal_status: signal_status.to_string(),
            control_generation: control.generation,
            control_source: control.source.as_str().to_string(),
            control_path: self
                .cfg
                .control_path
                .as_ref()
                .map(|path| path.display().to_string()),
            enabled_families: Some(enabled_families_csv(control)),
            effective_emit_heartbeat: Some(control.emit_heartbeat),
            effective_emit_signal_rows: Some(control.emit_signal_rows),
            message: message.to_string(),
        }
    }

    fn capability_rows(&self, control: &ControlState) -> Vec<SensorRow> {
        let enabled: std::collections::HashSet<_> =
            control.enabled_families.iter().copied().collect();
        target_signal_families(self.target)
            .iter()
            .map(|family| {
                let is_enabled = enabled.contains(family);
                SensorRow {
                    timestamp_unix_nano: now_unix_nano(),
                    event_family: "sensor_control".to_string(),
                    event_kind: "capability".to_string(),
                    signal_family: Some(family.as_str().to_string()),
                    signal_status: if is_enabled {
                        "enabled".to_string()
                    } else {
                        "disabled".to_string()
                    },
                    control_generation: control.generation,
                    control_source: control.source.as_str().to_string(),
                    control_path: self
                        .cfg
                        .control_path
                        .as_ref()
                        .map(|path| path.display().to_string()),
                    enabled_families: Some(enabled_families_csv(control)),
                    effective_emit_heartbeat: Some(control.emit_heartbeat),
                    effective_emit_signal_rows: Some(control.emit_signal_rows),
                    message: if is_enabled {
                        format!("{} family enabled", family.as_str())
                    } else {
                        format!("{} family disabled", family.as_str())
                    },
                }
            })
            .collect()
    }

    fn signal_sample_rows(
        &self,
        control: &ControlState,
        event_kind: &str,
        message: &str,
    ) -> Vec<SensorRow> {
        if !control.emit_signal_rows {
            return Vec::new();
        }

        control
            .enabled_families
            .iter()
            .map(|family| SensorRow {
                timestamp_unix_nano: now_unix_nano(),
                event_family: family.as_str().to_string(),
                event_kind: event_kind.to_string(),
                signal_family: Some(family.as_str().to_string()),
                signal_status: "ok".to_string(),
                control_generation: control.generation,
                control_source: control.source.as_str().to_string(),
                control_path: self
                    .cfg
                    .control_path
                    .as_ref()
                    .map(|path| path.display().to_string()),
                enabled_families: Some(enabled_families_csv(control)),
                effective_emit_heartbeat: Some(control.emit_heartbeat),
                effective_emit_signal_rows: Some(control.emit_signal_rows),
                message: format!("{} ({})", message, family.as_str()),
            })
            .collect()
    }

    fn build_batch_event(&self, rows: Vec<SensorRow>) -> io::Result<InputEvent> {
        if rows.is_empty() {
            return Err(io::Error::other("cannot build sensor batch from zero rows"));
        }

        let mut timestamp_unix_nano = Vec::with_capacity(rows.len());
        let mut sensor_name = Vec::with_capacity(rows.len());
        let mut sensor_target_platform = Vec::with_capacity(rows.len());
        let mut sensor_host_platform = Vec::with_capacity(rows.len());
        let mut event_family = Vec::with_capacity(rows.len());
        let mut event_kind = Vec::with_capacity(rows.len());
        let mut signal_family = Vec::with_capacity(rows.len());
        let mut signal_status = Vec::with_capacity(rows.len());
        let mut control_generation = Vec::with_capacity(rows.len());
        let mut control_source = Vec::with_capacity(rows.len());
        let mut control_path = Vec::with_capacity(rows.len());
        let mut enabled_families = Vec::with_capacity(rows.len());
        let mut effective_emit_heartbeat = Vec::with_capacity(rows.len());
        let mut effective_emit_signal_rows = Vec::with_capacity(rows.len());
        let mut message = Vec::with_capacity(rows.len());
        let mut sensor_beta = Vec::with_capacity(rows.len());
        let mut accounted_bytes = 0_u64;

        for row in rows {
            timestamp_unix_nano.push(row.timestamp_unix_nano);
            sensor_name.push(self.name.clone());
            sensor_target_platform.push(self.target.as_str().to_string());
            sensor_host_platform.push(self.host_platform.to_string());
            event_family.push(row.event_family);
            event_kind.push(row.event_kind);
            signal_family.push(row.signal_family);
            signal_status.push(row.signal_status);
            control_generation.push(row.control_generation);
            control_source.push(row.control_source);
            control_path.push(row.control_path);
            enabled_families.push(row.enabled_families);
            effective_emit_heartbeat.push(row.effective_emit_heartbeat);
            effective_emit_signal_rows.push(row.effective_emit_signal_rows);
            accounted_bytes = accounted_bytes.saturating_add(row.message.len() as u64);
            message.push(row.message);
            sensor_beta.push(true);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(timestamp_unix_nano)),
            Arc::new(StringArray::from(sensor_name)),
            Arc::new(StringArray::from(sensor_target_platform)),
            Arc::new(StringArray::from(sensor_host_platform)),
            Arc::new(StringArray::from(event_family)),
            Arc::new(StringArray::from(event_kind)),
            Arc::new(StringArray::from(signal_family)),
            Arc::new(StringArray::from(signal_status)),
            Arc::new(UInt64Array::from(control_generation)),
            Arc::new(StringArray::from(control_source)),
            Arc::new(StringArray::from(control_path)),
            Arc::new(StringArray::from(enabled_families)),
            Arc::new(BooleanArray::from(effective_emit_heartbeat)),
            Arc::new(BooleanArray::from(effective_emit_signal_rows)),
            Arc::new(StringArray::from(message)),
            Arc::new(BooleanArray::from(sensor_beta)),
        ];

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|e| io::Error::other(format!("build sensor batch: {e}")))?;

        Ok(InputEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes,
        })
    }
}

impl PlatformSensorBetaInput {
    /// Create a beta platform sensor source.
    ///
    /// Returns an error when `target` does not match the current host platform.
    pub fn new(
        name: impl Into<String>,
        target: PlatformSensorTarget,
        cfg: PlatformSensorBetaConfig,
    ) -> io::Result<Self> {
        let host_platform = current_host_platform().as_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "platform sensor beta inputs are unsupported on this host",
            )
        })?;

        if target.as_str() != host_platform {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} sensor beta input can only run on {} hosts (current host: {})",
                    target.as_str(),
                    target.as_str(),
                    host_platform
                ),
            ));
        }

        let mut control = ControlState {
            generation: 1,
            enabled_families: parse_enabled_families(cfg.enabled_families.as_deref(), target)?,
            source: ControlSource::StaticConfig,
            emit_heartbeat: cfg.emit_heartbeat,
            emit_signal_rows: cfg.emit_signal_rows,
        };

        if let Some(path) = cfg.control_path.as_ref()
            && let Some(file_cfg) = read_control_file(path)?
        {
            control = apply_control_file(control, file_cfg, target)?;
        }

        Ok(Self {
            machine: Some(PlatformSensorMachine::Init(PlatformSensorState {
                common: PlatformSensorCommon {
                    name: name.into(),
                    target,
                    host_platform,
                    cfg,
                    schema: sensor_schema(),
                },
                state: InitState { control },
            })),
        })
    }
}

impl InputSource for PlatformSensorBetaInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let machine = self
            .machine
            .take()
            .ok_or_else(|| io::Error::other("platform sensor beta state missing"))?;

        match machine {
            PlatformSensorMachine::Init(init) => {
                let (running, events) = init.start()?;
                self.machine = Some(PlatformSensorMachine::Running(running));
                Ok(events)
            }
            PlatformSensorMachine::Running(mut running) => {
                let rows = running.poll_rows();
                let events = if rows.is_empty() {
                    Vec::new()
                } else {
                    vec![running.common.build_batch_event(rows)?]
                };
                self.machine = Some(PlatformSensorMachine::Running(running));
                Ok(events)
            }
        }
    }

    fn name(&self) -> &str {
        match self
            .machine
            .as_ref()
            .expect("platform sensor beta state should always be present")
        {
            PlatformSensorMachine::Init(init) => &init.common.name,
            PlatformSensorMachine::Running(running) => &running.common.name,
        }
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }
}

fn parse_enabled_families(
    configured: Option<&[String]>,
    target: PlatformSensorTarget,
) -> io::Result<Vec<SignalFamily>> {
    let mut out = Vec::new();
    let Some(configured) = configured else {
        out.extend_from_slice(target_signal_families(target));
        return Ok(out);
    };

    for name in configured {
        let normalized = name.trim();
        if normalized.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sensor family names must not be empty",
            ));
        }
        let family = SignalFamily::parse(normalized)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if !target_signal_families(target).contains(&family) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "family '{}' is not available for {} targets",
                    family.as_str(),
                    target.as_str()
                ),
            ));
        }
        if !out.contains(&family) {
            out.push(family);
        }
    }

    Ok(out)
}

fn read_control_file(path: &Path) -> io::Result<Option<ControlFileConfig>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let parsed = serde_json::from_slice::<ControlFileConfig>(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("control file '{}' is not valid JSON: {e}", path.display()),
                )
            })?;
            Ok(Some(parsed))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn apply_control_file(
    mut state: ControlState,
    file_cfg: ControlFileConfig,
    target: PlatformSensorTarget,
) -> io::Result<ControlState> {
    if let Some(enabled) = file_cfg.enabled_families {
        state.enabled_families = parse_enabled_families(Some(&enabled), target)?;
    }
    if let Some(v) = file_cfg.emit_heartbeat {
        state.emit_heartbeat = v;
    }
    if let Some(v) = file_cfg.emit_signal_rows {
        state.emit_signal_rows = v;
    }
    if let Some(generation) = file_cfg.generation {
        state.generation = generation;
    }
    state.source = ControlSource::ControlFile;
    Ok(state)
}

fn enabled_families_csv(control: &ControlState) -> String {
    control
        .enabled_families
        .iter()
        .map(|family| family.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn sensor_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_unix_nano", DataType::UInt64, false),
        Field::new("sensor_name", DataType::Utf8, false),
        Field::new("sensor_target_platform", DataType::Utf8, false),
        Field::new("sensor_host_platform", DataType::Utf8, false),
        Field::new("event_family", DataType::Utf8, false),
        Field::new("event_kind", DataType::Utf8, false),
        Field::new("signal_family", DataType::Utf8, true),
        Field::new("signal_status", DataType::Utf8, false),
        Field::new("control_generation", DataType::UInt64, false),
        Field::new("control_source", DataType::Utf8, false),
        Field::new("control_path", DataType::Utf8, true),
        Field::new("enabled_families", DataType::Utf8, true),
        Field::new("effective_emit_heartbeat", DataType::Boolean, true),
        Field::new("effective_emit_signal_rows", DataType::Boolean, true),
        Field::new("message", DataType::Utf8, false),
        Field::new("sensor_beta", DataType::Boolean, false),
    ]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostPlatform {
    Linux,
    Macos,
    Windows,
    Unsupported,
}

impl HostPlatform {
    const fn as_str(self) -> Option<&'static str> {
        match self {
            Self::Linux => Some("linux"),
            Self::Macos => Some("macos"),
            Self::Windows => Some("windows"),
            Self::Unsupported => None,
        }
    }
}

fn now_unix_nano() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn current_host_platform() -> HostPlatform {
    if cfg!(target_os = "linux") {
        HostPlatform::Linux
    } else if cfg!(target_os = "macos") {
        HostPlatform::Macos
    } else if cfg!(target_os = "windows") {
        HostPlatform::Windows
    } else {
        HostPlatform::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn host_target() -> PlatformSensorTarget {
        #[cfg(target_os = "linux")]
        {
            PlatformSensorTarget::Linux
        }
        #[cfg(target_os = "macos")]
        {
            PlatformSensorTarget::Macos
        }
        #[cfg(target_os = "windows")]
        {
            PlatformSensorTarget::Windows
        }
    }

    fn non_host_target() -> PlatformSensorTarget {
        #[cfg(target_os = "linux")]
        {
            PlatformSensorTarget::Macos
        }
        #[cfg(target_os = "macos")]
        {
            PlatformSensorTarget::Windows
        }
        #[cfg(target_os = "windows")]
        {
            PlatformSensorTarget::Linux
        }
    }

    fn first_batch(events: &[InputEvent]) -> &RecordBatch {
        match &events[0] {
            InputEvent::Batch { batch, .. } => batch,
            _ => panic!("expected batch event"),
        }
    }

    fn string_col(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    fn u64_col(batch: &RecordBatch, name: &str) -> Vec<u64> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64 array");
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    #[test]
    fn rejects_non_matching_platform_target() {
        let err = PlatformSensorBetaInput::new(
            "beta",
            non_host_target(),
            PlatformSensorBetaConfig::default(),
        )
        .expect_err("non-matching target must fail");
        assert!(err.to_string().contains("can only run on"));
    }

    #[test]
    fn rejects_unknown_enabled_family() {
        let err = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                enabled_families: Some(vec!["not_a_family".to_string()]),
                ..PlatformSensorBetaConfig::default()
            },
        )
        .expect_err("unknown family must fail");
        assert!(err.to_string().contains("unknown sensor family"));
    }

    #[test]
    fn emits_startup_batch_on_first_poll() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig::default(),
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        assert_eq!(events.len(), 1);

        let batch = first_batch(&events);
        assert!(batch.num_rows() >= 1);
        let kinds = string_col(batch, "event_kind");
        assert!(kinds.iter().any(|v| v.as_deref() == Some("startup")));
        assert!(
            string_col(batch, "event_family")
                .iter()
                .any(|v| v.as_deref() == Some("sensor_control"))
        );
    }

    #[test]
    fn heartbeat_disabled_emits_only_startup_batch() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                emit_heartbeat: false,
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                ..PlatformSensorBetaConfig::default()
            },
        )
        .expect("host target should be valid");

        let startup = input.poll().expect("startup poll");
        assert_eq!(startup.len(), 1);
        assert!(input.poll().expect("second poll").is_empty());
    }

    #[test]
    fn enabled_families_filter_signal_samples() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                enabled_families: Some(vec!["process".to_string(), "dns".to_string()]),
                ..PlatformSensorBetaConfig::default()
            },
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        let families = string_col(batch, "signal_family");

        let sample_families: Vec<String> = kinds
            .iter()
            .zip(families.iter())
            .filter_map(|(kind, family)| {
                if kind.as_deref() == Some("startup_sample") {
                    family.clone()
                } else {
                    None
                }
            })
            .collect();

        assert!(sample_families.iter().any(|f| f == "process"));
        assert!(sample_families.iter().any(|f| f == "dns"));
        assert!(!sample_families.iter().any(|f| f == "network"));
    }

    #[test]
    fn explicit_empty_enabled_families_disables_signal_samples() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                enabled_families: Some(Vec::new()),
                ..PlatformSensorBetaConfig::default()
            },
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            !kinds
                .iter()
                .any(|kind| kind.as_deref() == Some("startup_sample")),
            "empty enabled_families should disable signal sample rows",
        );
    }

    #[test]
    fn control_file_reload_updates_generation_and_families() {
        let dir = tempfile::tempdir().expect("tempdir");
        let control_path = dir.path().join("sensor-control.json");

        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                emit_heartbeat: false,
                enabled_families: Some(vec!["process".to_string()]),
                ..PlatformSensorBetaConfig::default()
            },
        )
        .expect("host target should be valid");

        // startup
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        std::fs::write(
            &control_path,
            r#"{"generation":42,"enabled_families":["dns"],"emit_signal_rows":true}"#,
        )
        .expect("write control file");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("reload poll succeeds");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);

        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds
                .iter()
                .any(|v| v.as_deref() == Some("control_reload_applied"))
        );

        let generations = u64_col(batch, "control_generation");
        assert!(generations.iter().all(|g| *g == 42));

        let families = string_col(batch, "signal_family");
        let reloaded_sample_families: Vec<String> = kinds
            .iter()
            .zip(families.iter())
            .filter_map(|(kind, family)| {
                if kind.as_deref() == Some("control_reload_sample") {
                    family.clone()
                } else {
                    None
                }
            })
            .collect();
        assert!(reloaded_sample_families.iter().any(|f| f == "dns"));
        assert!(!reloaded_sample_families.iter().any(|f| f == "process"));
    }

    #[test]
    fn control_reload_same_generation_and_values_is_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let control_path = dir.path().join("sensor-control.json");

        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                emit_heartbeat: false,
                enabled_families: Some(vec!["process".to_string()]),
                ..PlatformSensorBetaConfig::default()
            },
        )
        .expect("host target should be valid");

        // startup
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        std::fs::write(
            &control_path,
            r#"{"generation":1,"enabled_families":["process"],"emit_heartbeat":false,"emit_signal_rows":true}"#,
        )
        .expect("write control file");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("reload poll succeeds");
        assert!(
            events.is_empty(),
            "unchanged control file should not emit reload-applied rows"
        );
    }
}
