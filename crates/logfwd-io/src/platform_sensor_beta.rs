//! Cross-platform beta sensor input.
//!
//! This source emits platform-gated beta telemetry snapshots for core families
//! (`process`, `network`, `disk_io`) plus control-plane lifecycle events.

use std::ffi::OsStr;
use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use logfwd_types::diagnostics::ComponentHealth;
use serde::Serialize;
use serde_json::json;
use sysinfo::{Networks, Process, ProcessRefreshKind, ProcessesToUpdate, System};

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

/// Core beta sensor event families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformSensorFamily {
    Process,
    Network,
    DiskIo,
}

impl PlatformSensorFamily {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Network => "network",
            Self::DiskIo => "disk_io",
        }
    }
}

/// Runtime options for platform beta inputs.
#[derive(Debug, Clone)]
pub struct PlatformSensorBetaConfig {
    /// Emit periodic heartbeats when no data rows were emitted in a cycle.
    pub emit_heartbeat: bool,
    /// Snapshot cadence.
    pub poll_interval: Duration,
    /// Families to collect each cycle.
    pub families: Vec<PlatformSensorFamily>,
    /// Upper bound on data rows emitted per cycle across all families.
    pub max_rows_per_poll: usize,
}

impl Default for PlatformSensorBetaConfig {
    fn default() -> Self {
        Self {
            emit_heartbeat: true,
            poll_interval: Duration::from_millis(10_000),
            families: vec![
                PlatformSensorFamily::Process,
                PlatformSensorFamily::Network,
                PlatformSensorFamily::DiskIo,
            ],
            max_rows_per_poll: 256,
        }
    }
}

/// Beta input source for per-platform sensor bring-up.
#[derive(Debug)]
pub struct PlatformSensorBetaInput {
    machine: Option<PlatformSensorMachineState>,
}

#[derive(Debug)]
enum PlatformSensorMachineState {
    Init(PlatformSensorMachine<InitState>),
    Running(PlatformSensorMachine<RunningState>),
}

#[derive(Debug)]
struct PlatformSensorMachine<S> {
    common: PlatformSensorCommon,
    state: S,
}

#[derive(Debug)]
struct PlatformSensorCommon {
    name: String,
    target: PlatformSensorTarget,
    host_platform: &'static str,
    cfg: PlatformSensorBetaConfig,
    system: System,
    networks: Networks,
}

#[derive(Debug)]
struct InitState;

#[derive(Debug)]
struct RunningState {
    last_cycle: Instant,
}

impl PlatformSensorBetaInput {
    /// Create a beta platform sensor source.
    ///
    /// Returns an error when `target` does not match the current host platform.
    pub fn new(
        name: impl Into<String>,
        target: PlatformSensorTarget,
        mut cfg: PlatformSensorBetaConfig,
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

        if cfg.families.is_empty() {
            cfg.families = PlatformSensorBetaConfig::default().families;
        }
        if cfg.max_rows_per_poll == 0 {
            cfg.max_rows_per_poll = 1;
        }

        Ok(Self {
            machine: Some(PlatformSensorMachineState::Init(PlatformSensorMachine {
                common: PlatformSensorCommon {
                    name: name.into(),
                    target,
                    host_platform,
                    cfg,
                    system: System::new_all(),
                    networks: Networks::new_with_refreshed_list(),
                },
                state: InitState,
            })),
        })
    }
}

impl PlatformSensorCommon {
    fn run_collection_cycle(&mut self, out: &mut Vec<u8>) -> usize {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::everything(),
        );
        self.networks.refresh(true);

        let mut emitted = 0usize;
        let mut remaining = self.cfg.max_rows_per_poll;

        for family in &self.cfg.families {
            if remaining == 0 {
                break;
            }
            let count = match family {
                PlatformSensorFamily::Process => self.emit_process_rows(out, remaining),
                PlatformSensorFamily::Network => self.emit_network_rows(out, remaining),
                PlatformSensorFamily::DiskIo => self.emit_disk_io_rows(out, remaining),
            };
            emitted += count;
            remaining = remaining.saturating_sub(count);
        }

        emitted
    }

    fn emit_process_rows(&self, out: &mut Vec<u8>, limit: usize) -> usize {
        let mut rows: Vec<(&sysinfo::Pid, &Process)> = self.system.processes().iter().collect();
        rows.sort_unstable_by_key(|(pid, _)| pid.as_u32());

        let mut emitted = 0usize;
        for (pid, process) in rows.into_iter().take(limit) {
            let disk_usage = process.disk_usage();
            append_json_line(
                out,
                &json!({
                    "timestamp_unix_nano": now_unix_nano(),
                    "level": "INFO",
                    "event_family": "process",
                    "sensor_event": "snapshot",
                    "sensor_status": "beta",
                    "sensor_target_platform": self.target.as_str(),
                    "sensor_host_platform": self.host_platform,
                    "sensor_name": &self.name,
                    "sensor_beta": true,
                    "process_pid": pid.as_u32(),
                    "process_parent_pid": process.parent().map(sysinfo::Pid::as_u32),
                    "process_name": os_to_string(process.name()),
                    "process_cmd": os_vec_to_string(process.cmd()),
                    "process_exe": process.exe().map(|p| p.to_string_lossy().to_string()),
                    "process_status": format!("{:?}", process.status()),
                    "process_cpu_usage": process.cpu_usage(),
                    "process_memory_bytes": process.memory(),
                    "process_virtual_memory_bytes": process.virtual_memory(),
                    "process_start_time_unix_sec": process.start_time(),
                    "process_disk_read_delta_bytes": disk_usage.read_bytes,
                    "process_disk_write_delta_bytes": disk_usage.written_bytes,
                }),
            );
            emitted += 1;
        }

        emitted
    }

    fn emit_network_rows(&self, out: &mut Vec<u8>, limit: usize) -> usize {
        let mut rows: Vec<_> = self.networks.iter().collect();
        rows.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let mut emitted = 0usize;
        for (iface, data) in rows.into_iter().take(limit) {
            append_json_line(
                out,
                &json!({
                    "timestamp_unix_nano": now_unix_nano(),
                    "level": "INFO",
                    "event_family": "network",
                    "sensor_event": "snapshot",
                    "sensor_status": "beta",
                    "sensor_target_platform": self.target.as_str(),
                    "sensor_host_platform": self.host_platform,
                    "sensor_name": &self.name,
                    "sensor_beta": true,
                    "network_interface": iface,
                    "network_received_delta_bytes": data.received(),
                    "network_transmitted_delta_bytes": data.transmitted(),
                    "network_received_total_bytes": data.total_received(),
                    "network_transmitted_total_bytes": data.total_transmitted(),
                    "network_packets_received_delta": data.packets_received(),
                    "network_packets_transmitted_delta": data.packets_transmitted(),
                    "network_errors_received_delta": data.errors_on_received(),
                    "network_errors_transmitted_delta": data.errors_on_transmitted(),
                }),
            );
            emitted += 1;
        }

        emitted
    }

    fn emit_disk_io_rows(&self, out: &mut Vec<u8>, limit: usize) -> usize {
        let mut rows: Vec<(&sysinfo::Pid, &Process)> = self
            .system
            .processes()
            .iter()
            .filter(|(_, process)| {
                let usage = process.disk_usage();
                usage.read_bytes > 0 || usage.written_bytes > 0
            })
            .collect();
        rows.sort_unstable_by(|(a_pid, a_proc), (b_pid, b_proc)| {
            let a = a_proc.disk_usage().written_bytes + a_proc.disk_usage().read_bytes;
            let b = b_proc.disk_usage().written_bytes + b_proc.disk_usage().read_bytes;
            b.cmp(&a).then_with(|| a_pid.as_u32().cmp(&b_pid.as_u32()))
        });

        let mut emitted = 0usize;
        for (pid, process) in rows.into_iter().take(limit) {
            let usage = process.disk_usage();
            append_json_line(
                out,
                &json!({
                    "timestamp_unix_nano": now_unix_nano(),
                    "level": "INFO",
                    "event_family": "disk_io",
                    "sensor_event": "snapshot",
                    "sensor_status": "beta",
                    "sensor_target_platform": self.target.as_str(),
                    "sensor_host_platform": self.host_platform,
                    "sensor_name": &self.name,
                    "sensor_beta": true,
                    "process_pid": pid.as_u32(),
                    "process_name": os_to_string(process.name()),
                    "disk_io_read_delta_bytes": usage.read_bytes,
                    "disk_io_write_delta_bytes": usage.written_bytes,
                    "disk_io_read_total_bytes": usage.total_read_bytes,
                    "disk_io_write_total_bytes": usage.total_written_bytes,
                }),
            );
            emitted += 1;
        }

        emitted
    }

    fn emit_startup_control(&self, out: &mut Vec<u8>) {
        append_json_line(
            out,
            &SensorControlEvent {
                timestamp_unix_nano: now_unix_nano(),
                level: "INFO",
                message: "platform sensor beta startup",
                event_family: "sensor_control",
                sensor_event: "startup",
                sensor_status: "beta",
                sensor_target_platform: self.target.as_str(),
                sensor_host_platform: self.host_platform,
                sensor_name: &self.name,
                sensor_beta: true,
            },
        );

        for family in &self.cfg.families {
            append_json_line(
                out,
                &json!({
                    "timestamp_unix_nano": now_unix_nano(),
                    "level": "INFO",
                    "event_family": "sensor_control",
                    "sensor_event": "capability",
                    "sensor_status": "beta",
                    "sensor_target_platform": self.target.as_str(),
                    "sensor_host_platform": self.host_platform,
                    "sensor_name": &self.name,
                    "sensor_beta": true,
                    "sensor_family": family.as_str(),
                    "sensor_family_state": "beta_supported",
                }),
            );
        }
    }
}

impl PlatformSensorMachine<InitState> {
    fn start(self, out: &mut Vec<u8>) -> PlatformSensorMachine<RunningState> {
        self.common.emit_startup_control(out);
        PlatformSensorMachine {
            common: self.common,
            state: RunningState {
                last_cycle: Instant::now(),
            },
        }
    }
}

impl PlatformSensorMachine<RunningState> {
    fn poll_collection_if_due(&mut self, out: &mut Vec<u8>, force: bool) {
        if force || self.state.last_cycle.elapsed() >= self.common.cfg.poll_interval {
            let emitted_rows = self.common.run_collection_cycle(out);
            if emitted_rows == 0 && self.common.cfg.emit_heartbeat {
                append_json_line(
                    out,
                    &SensorControlEvent {
                        timestamp_unix_nano: now_unix_nano(),
                        level: "INFO",
                        message: "platform sensor beta heartbeat",
                        event_family: "sensor_control",
                        sensor_event: "heartbeat",
                        sensor_status: "beta",
                        sensor_target_platform: self.common.target.as_str(),
                        sensor_host_platform: self.common.host_platform,
                        sensor_name: &self.common.name,
                        sensor_beta: true,
                    },
                );
            }
            self.state.last_cycle = Instant::now();
        }
    }
}

impl InputSource for PlatformSensorBetaInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut bytes = Vec::with_capacity(16 * 1024);
        let machine = self
            .machine
            .take()
            .ok_or_else(|| io::Error::other("platform sensor beta state missing"))?;

        let next_machine = match machine {
            PlatformSensorMachineState::Init(init) => {
                let mut running = init.start(&mut bytes);
                running.poll_collection_if_due(&mut bytes, true);
                PlatformSensorMachineState::Running(running)
            }
            PlatformSensorMachineState::Running(mut running) => {
                running.poll_collection_if_due(&mut bytes, false);
                PlatformSensorMachineState::Running(running)
            }
        };
        self.machine = Some(next_machine);

        if bytes.is_empty() {
            return Ok(vec![]);
        }

        let accounted_bytes = bytes.len() as u64;
        Ok(vec![InputEvent::Data {
            bytes,
            source_id: None,
            accounted_bytes,
        }])
    }

    fn name(&self) -> &str {
        match self
            .machine
            .as_ref()
            .expect("platform sensor beta state should always be present")
        {
            PlatformSensorMachineState::Init(init) => &init.common.name,
            PlatformSensorMachineState::Running(running) => &running.common.name,
        }
    }

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }
}

#[derive(Serialize)]
struct SensorControlEvent<'a> {
    timestamp_unix_nano: u64,
    level: &'static str,
    message: &'static str,
    event_family: &'static str,
    sensor_event: &'static str,
    sensor_status: &'static str,
    sensor_target_platform: &'static str,
    sensor_host_platform: &'static str,
    sensor_name: &'a str,
    sensor_beta: bool,
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

fn append_json_line<T: Serialize>(out: &mut Vec<u8>, value: &T) {
    if serde_json::to_writer(&mut *out, value).is_ok() {
        out.push(b'\n');
    }
}

fn os_to_string(value: &OsStr) -> String {
    value.to_string_lossy().to_string()
}

fn os_vec_to_string(values: &[std::ffi::OsString]) -> String {
    values
        .iter()
        .map(|v| v.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
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
    fn emits_startup_and_snapshot_rows_on_first_poll() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                emit_heartbeat: false,
                poll_interval: Duration::from_secs(3600),
                families: vec![PlatformSensorFamily::Process],
                max_rows_per_poll: 1,
            },
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        assert_eq!(events.len(), 1);
        let payload = match &events[0] {
            InputEvent::Data { bytes, .. } => std::str::from_utf8(bytes).expect("utf8"),
            _ => panic!("expected Data event"),
        };
        assert!(payload.contains("\"sensor_event\":\"startup\""));
        assert!(payload.contains("\"sensor_event\":\"capability\""));
        assert!(payload.contains("\"event_family\":\"process\""));
    }

    #[test]
    fn heartbeat_disabled_emits_only_first_cycle_until_interval_elapses() {
        let mut input = PlatformSensorBetaInput::new(
            "beta",
            host_target(),
            PlatformSensorBetaConfig {
                emit_heartbeat: false,
                poll_interval: Duration::from_secs(3600),
                families: vec![PlatformSensorFamily::DiskIo],
                max_rows_per_poll: 1,
            },
        )
        .expect("host target should be valid");

        assert_eq!(input.poll().expect("startup poll").len(), 1);
        assert!(input.poll().expect("second poll").is_empty());
    }
}
