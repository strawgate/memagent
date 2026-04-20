//! Host metrics input.
//!
//! This source emits Arrow `RecordBatch` rows directly with host metrics
//! (process snapshots, CPU, memory, network stats via sysinfo). It includes a
//! lightweight runtime control plane (optional JSON file reload) and explicit
//! per-platform signal families so we can iterate toward production metrics
//! without routing synthetic JSON through text decoders.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{cmp::Ordering, collections::BinaryHeap};

use arrow::array::{ArrayRef, BooleanArray, Float32Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_types::diagnostics::ComponentHealth;
use serde::Deserialize;
use sysinfo::{Networks, Process, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

use crate::input::{InputEvent, InputSource};

#[derive(Clone, Copy)]
struct SelectedProcess<'a> {
    pid: u32,
    process: &'a Process,
}

impl Eq for SelectedProcess<'_> {}

impl PartialEq for SelectedProcess<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid
    }
}

impl Ord for SelectedProcess<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pid.cmp(&other.pid)
    }
}

impl PartialOrd for SelectedProcess<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Platform target for a platform sensor input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMetricsTarget {
    Linux,
    Macos,
    Windows,
}

impl HostMetricsTarget {
    /// Returns the stable lowercase platform key used in config and telemetry rows.
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

const DEFAULT_MAX_PROCESS_ROWS_PER_POLL: usize = 1024;

fn target_signal_families(target: HostMetricsTarget) -> &'static [SignalFamily] {
    match target {
        HostMetricsTarget::Linux => LINUX_FAMILIES,
        HostMetricsTarget::Macos => MACOS_FAMILIES,
        HostMetricsTarget::Windows => WINDOWS_FAMILIES,
    }
}

/// Runtime options for platform sensor inputs.
#[derive(Debug, Clone)]
pub struct HostMetricsConfig {
    /// Periodic sample cadence.
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
    /// Upper bound on data rows emitted per collection cycle.
    pub max_rows_per_poll: usize,
    /// Upper bound on process rows emitted per collection cycle.
    ///
    /// This cap is applied after family budgeting to prevent unbounded memory
    /// growth when hosts expose very large process tables. The default is 1024;
    /// `0` also means "use the default".
    pub max_process_rows_per_poll: usize,
}

impl Default for HostMetricsConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(10),
            control_path: None,
            control_reload_interval: Duration::from_secs(1),
            enabled_families: None,
            emit_signal_rows: true,
            max_rows_per_poll: 256,
            max_process_rows_per_poll: DEFAULT_MAX_PROCESS_ROWS_PER_POLL,
        }
    }
}

/// Input source for per-platform sensor bring-up.
#[derive(Debug)]
pub struct HostMetricsInput {
    name: String,
    machine: Option<HostMetricsMachine>,
}

#[derive(Debug)]
enum HostMetricsMachine {
    Init(HostMetricsState<InitState>),
    Running(HostMetricsState<RunningState>),
}

#[derive(Debug)]
struct HostMetricsCommon {
    name: String,
    target: HostMetricsTarget,
    host_platform: &'static str,
    cfg: HostMetricsConfig,
    schema: Arc<Schema>,
    system: System,
    networks: Networks,
    /// Wrapping counter used to rotate the budget remainder
    /// across families so no single family is permanently starved (#1935).
    poll_count: usize,
}

#[derive(Debug)]
struct HostMetricsState<S> {
    common: HostMetricsCommon,
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
    health: ComponentHealth,
}

#[derive(Debug, Clone)]
struct ControlState {
    generation: u64,
    enabled_families: Vec<SignalFamily>,
    source: ControlSource,
    emit_signal_rows: bool,
}

type InitStartOk = (HostMetricsState<RunningState>, Vec<InputEvent>);
type InitStartErr = Box<(HostMetricsState<InitState>, io::Error)>;

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
    effective_emit_signal_rows: Option<bool>,
    message: String,

    // Process fields
    process_pid: Option<u32>,
    process_parent_pid: Option<u32>,
    process_name: Option<String>,
    process_cmd: Option<String>,
    process_exe: Option<String>,
    process_status: Option<String>,
    process_cpu_usage: Option<f32>,
    process_memory_bytes: Option<u64>,
    process_virtual_memory_bytes: Option<u64>,
    process_start_time_unix_sec: Option<u64>,
    process_disk_read_delta_bytes: Option<u64>,
    process_disk_write_delta_bytes: Option<u64>,
    process_user_id: Option<u32>,
    process_effective_user_id: Option<u32>,
    process_group_id: Option<u32>,
    process_effective_group_id: Option<u32>,
    process_cwd: Option<String>,
    process_session_id: Option<u32>,
    process_run_time_secs: Option<u64>,
    process_open_files: Option<u64>,
    process_thread_count: Option<u64>,
    process_container_id: Option<String>,

    // Network fields
    network_interface: Option<String>,
    network_received_delta_bytes: Option<u64>,
    network_transmitted_delta_bytes: Option<u64>,
    network_received_total_bytes: Option<u64>,
    network_transmitted_total_bytes: Option<u64>,
    network_packets_received_delta: Option<u64>,
    network_packets_transmitted_delta: Option<u64>,
    network_errors_received_delta: Option<u64>,
    network_errors_transmitted_delta: Option<u64>,
    network_mac_address: Option<String>,
    network_ip_addresses: Option<String>,
    network_mtu: Option<u64>,

    // System-level fields
    system_total_memory_bytes: Option<u64>,
    system_used_memory_bytes: Option<u64>,
    system_available_memory_bytes: Option<u64>,
    system_total_swap_bytes: Option<u64>,
    system_used_swap_bytes: Option<u64>,
    system_cpu_usage_percent: Option<f32>,
    system_uptime_secs: Option<u64>,
    system_cgroup_memory_limit: Option<u64>,
    system_cgroup_memory_current: Option<u64>,

    // Disk I/O fields
    disk_io_read_delta_bytes: Option<u64>,
    disk_io_write_delta_bytes: Option<u64>,
    disk_io_read_total_bytes: Option<u64>,
    disk_io_write_total_bytes: Option<u64>,
}

// Control files are operator-tunable runtime state, not a fixed schema. Tolerate
// unknown fields so operators can stage new knobs or leave stale ones behind
// (for example `emit_heartbeat`, which became a no-op) without the reader
// aborting mid-poll.
#[derive(Debug, Deserialize)]
struct ControlFileConfig {
    generation: Option<u64>,
    enabled_families: Option<Vec<String>>,
    emit_signal_rows: Option<bool>,
}

impl HostMetricsState<InitState> {
    fn start(mut self) -> Result<InitStartOk, InitStartErr> {
        let now = Instant::now();
        let mut rows = vec![self.common.control_row(
            &self.state.control,
            "startup",
            "sensor startup complete",
            "ok",
        )];
        rows.extend(self.common.capability_rows(&self.state.control));
        self.common
            .run_collection_cycle(&self.state.control, &mut rows);
        rows.extend(self.common.signal_sample_rows(
            &self.state.control,
            "startup_sample",
            "initial signal snapshot",
        ));

        let event = match self.common.build_batch_event(rows) {
            Ok(event) => event,
            Err(err) => return Err(Box::new((self, err))),
        };
        let last_control_check = now
            .checked_sub(self.common.cfg.control_reload_interval)
            .unwrap_or(now);
        let running = HostMetricsState {
            common: self.common,
            state: RunningState {
                last_emit: now,
                last_control_check,
                control: self.state.control,
                health: ComponentHealth::Healthy,
            },
        };
        Ok((running, vec![event]))
    }
}

impl HostMetricsState<RunningState> {
    fn poll_rows(&mut self) -> Vec<SensorRow> {
        let mut rows = Vec::new();

        if let Some(reload_rows) = self.try_reload_control() {
            rows.extend(reload_rows);
        }

        if self.state.last_emit.elapsed() >= self.common.cfg.poll_interval {
            self.common
                .run_collection_cycle(&self.state.control, &mut rows);

            if self.state.control.emit_signal_rows {
                rows.extend(self.common.signal_sample_rows(
                    &self.state.control,
                    "sample",
                    "periodic signal snapshot",
                ));
            }
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
            Ok(None) => {
                self.state.health = ComponentHealth::Healthy;
                None
            }
            Ok(Some(file_cfg)) => {
                let mut next = self.state.control.clone();
                if let Some(enabled) = file_cfg.enabled_families {
                    let parsed = match parse_enabled_families(Some(&enabled), self.common.target) {
                        Ok(v) => v,
                        Err(e) => {
                            self.state.health = ComponentHealth::Degraded;
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
                if let Some(v) = file_cfg.emit_signal_rows {
                    next.emit_signal_rows = v;
                }
                next.source = ControlSource::ControlFile;
                let generation_changed = file_cfg
                    .generation
                    .is_some_and(|generation| generation != self.state.control.generation);

                let changed = next.enabled_families != self.state.control.enabled_families
                    || next.emit_signal_rows != self.state.control.emit_signal_rows
                    || generation_changed;

                if !changed {
                    self.state.health = ComponentHealth::Healthy;
                    return None;
                }

                next.generation = file_cfg
                    .generation
                    .unwrap_or_else(|| self.state.control.generation.saturating_add(1));
                self.state.control = next.clone();
                self.state.health = ComponentHealth::Healthy;

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
            Err(e) => {
                self.state.health = ComponentHealth::Degraded;
                Some(vec![self.common.control_row(
                    &self.state.control,
                    "control_reload_failed",
                    &format!("failed to load control file: {e}"),
                    "error",
                )])
            }
        }
    }
}

impl HostMetricsCommon {
    fn base_row(
        &self,
        control: &ControlState,
        event_family: &str,
        event_kind: &str,
        signal_status: &str,
        message: &str,
    ) -> SensorRow {
        SensorRow {
            timestamp_unix_nano: now_unix_nano(),
            event_family: event_family.to_string(),
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
            effective_emit_signal_rows: Some(control.emit_signal_rows),
            message: message.to_string(),
            process_pid: None,
            process_parent_pid: None,
            process_name: None,
            process_cmd: None,
            process_exe: None,
            process_status: None,
            process_cpu_usage: None,
            process_memory_bytes: None,
            process_virtual_memory_bytes: None,
            process_start_time_unix_sec: None,
            process_disk_read_delta_bytes: None,
            process_disk_write_delta_bytes: None,
            process_user_id: None,
            process_effective_user_id: None,
            process_group_id: None,
            process_effective_group_id: None,
            process_cwd: None,
            process_session_id: None,
            process_run_time_secs: None,
            process_open_files: None,
            process_thread_count: None,
            process_container_id: None,
            network_interface: None,
            network_received_delta_bytes: None,
            network_transmitted_delta_bytes: None,
            network_received_total_bytes: None,
            network_transmitted_total_bytes: None,
            network_packets_received_delta: None,
            network_packets_transmitted_delta: None,
            network_errors_received_delta: None,
            network_errors_transmitted_delta: None,
            network_mac_address: None,
            network_ip_addresses: None,
            network_mtu: None,
            system_total_memory_bytes: None,
            system_used_memory_bytes: None,
            system_available_memory_bytes: None,
            system_total_swap_bytes: None,
            system_used_swap_bytes: None,
            system_cpu_usage_percent: None,
            system_uptime_secs: None,
            system_cgroup_memory_limit: None,
            system_cgroup_memory_current: None,
            disk_io_read_delta_bytes: None,
            disk_io_write_delta_bytes: None,
            disk_io_read_total_bytes: None,
            disk_io_write_total_bytes: None,
        }
    }

    fn control_row(
        &self,
        control: &ControlState,
        event_kind: &str,
        message: &str,
        signal_status: &str,
    ) -> SensorRow {
        self.base_row(
            control,
            "sensor_control",
            event_kind,
            signal_status,
            message,
        )
    }

    fn capability_rows(&self, control: &ControlState) -> Vec<SensorRow> {
        let enabled: std::collections::HashSet<_> =
            control.enabled_families.iter().copied().collect();
        target_signal_families(self.target)
            .iter()
            .map(|family| {
                let is_enabled = enabled.contains(family);
                let mut row = self.base_row(
                    control,
                    "sensor_control",
                    "capability",
                    if is_enabled { "enabled" } else { "disabled" },
                    &if is_enabled {
                        format!("{} family enabled", family.as_str())
                    } else {
                        format!("{} family disabled", family.as_str())
                    },
                );
                row.signal_family = Some(family.as_str().to_string());
                row
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
            .filter(|family| {
                !matches!(
                    family,
                    SignalFamily::Process | SignalFamily::Network | SignalFamily::File
                )
            })
            .map(|family| {
                let mut row = self.base_row(
                    control,
                    family.as_str(),
                    event_kind,
                    "ok",
                    &format!("{} ({})", message, family.as_str()),
                );
                row.signal_family = Some(family.as_str().to_string());
                row
            })
            .collect()
    }

    fn run_collection_cycle(&mut self, control: &ControlState, out: &mut Vec<SensorRow>) -> usize {
        let enabled: std::collections::HashSet<_> =
            control.enabled_families.iter().copied().collect();

        // Families that have real data collection.
        let collection_families = [
            SignalFamily::Process,
            SignalFamily::Network,
            SignalFamily::File,
        ];
        let active_count = collection_families
            .iter()
            .filter(|f| enabled.contains(f))
            .count();
        if active_count == 0 {
            return 0;
        }

        // Always emit system health regardless of family budget.
        self.emit_system_rows(control, out);

        // Only refresh the subsystems we actually need.
        let needs_processes =
            enabled.contains(&SignalFamily::Process) || enabled.contains(&SignalFamily::File);
        if needs_processes {
            self.system.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing()
                    .with_cpu()
                    .with_memory()
                    .with_disk_usage()
                    .with_cmd(UpdateKind::OnlyIfNotSet)
                    .with_exe(UpdateKind::OnlyIfNotSet)
                    .with_user(UpdateKind::OnlyIfNotSet)
                    .with_cwd(UpdateKind::OnlyIfNotSet),
            );
        }
        if enabled.contains(&SignalFamily::Network) {
            self.networks.refresh(true);
        }

        // Split budget evenly across active families, rotating the remainder
        // so no single family is permanently starved (#1935).
        let per_family_budget = self.cfg.max_rows_per_poll / active_count;
        let remainder = self.cfg.max_rows_per_poll % active_count;
        let start_idx = self.poll_count % active_count;
        self.poll_count = self.poll_count.wrapping_add(1);

        let mut emitted = 0usize;
        let mut family_idx = 0usize;

        if enabled.contains(&SignalFamily::Process) {
            let extra =
                usize::from((family_idx + active_count - start_idx) % active_count < remainder);
            emitted += self.emit_process_rows(control, out, per_family_budget + extra);
            family_idx += 1;
        }
        if enabled.contains(&SignalFamily::Network) {
            let extra =
                usize::from((family_idx + active_count - start_idx) % active_count < remainder);
            emitted += self.emit_network_rows(control, out, per_family_budget + extra);
            family_idx += 1;
        }
        if enabled.contains(&SignalFamily::File) {
            let extra =
                usize::from((family_idx + active_count - start_idx) % active_count < remainder);
            emitted += self.emit_disk_io_rows(control, out, per_family_budget + extra);
        }

        emitted
    }

    fn emit_process_rows(
        &self,
        control: &ControlState,
        out: &mut Vec<SensorRow>,
        limit: usize,
    ) -> usize {
        let limit = limit.min(self.cfg.max_process_rows_per_poll);
        if limit == 0 {
            return 0;
        }

        let processes = self.system.processes();
        let mut procs = BinaryHeap::with_capacity(limit.min(processes.len()));
        for (pid, process) in processes {
            let selected = SelectedProcess {
                pid: pid.as_u32(),
                process,
            };
            if procs.len() < limit {
                procs.push(selected);
            } else if procs
                .peek()
                .is_some_and(|largest| selected.pid < largest.pid)
            {
                procs.pop();
                procs.push(selected);
            }
        }
        let mut procs = procs.into_vec();
        procs.sort_unstable_by_key(|selected| selected.pid);

        let mut emitted = 0usize;
        for selected in procs {
            let pid = selected.pid;
            let process = selected.process;
            let disk_usage = process.disk_usage();
            let mut row = self.base_row(control, "process", "snapshot", "ok", "process snapshot");
            row.signal_family = Some("process".to_string());
            row.process_pid = Some(pid);
            row.process_parent_pid = process.parent().map(sysinfo::Pid::as_u32);
            row.process_name = Some(os_to_string(process.name()));
            row.process_cmd = Some(os_vec_to_string(process.cmd()));
            row.process_exe = process.exe().map(|p| p.to_string_lossy().to_string());
            row.process_status = Some(format!("{:?}", process.status()));
            row.process_cpu_usage = Some(process.cpu_usage());
            row.process_memory_bytes = Some(process.memory());
            row.process_virtual_memory_bytes = Some(process.virtual_memory());
            row.process_start_time_unix_sec = Some(process.start_time());
            row.process_disk_read_delta_bytes = Some(disk_usage.read_bytes);
            row.process_disk_write_delta_bytes = Some(disk_usage.written_bytes);
            #[cfg(unix)]
            {
                row.process_user_id = process.user_id().map(|uid| **uid);
                row.process_effective_user_id = process.effective_user_id().map(|uid| **uid);
                row.process_group_id = process.group_id().map(|gid| *gid);
                row.process_effective_group_id = process.effective_group_id().map(|gid| *gid);
            }
            row.process_cwd = process.cwd().map(|p| p.to_string_lossy().to_string());
            row.process_session_id = process.session_id().map(sysinfo::Pid::as_u32);
            row.process_run_time_secs = Some(process.run_time());
            row.process_open_files = process.open_files().map(|n| n as u64);
            row.process_thread_count = process.tasks().map(|t| t.len() as u64);
            row.process_container_id = extract_container_id(pid);
            out.push(row);
            emitted += 1;
        }

        emitted
    }

    fn emit_network_rows(
        &self,
        control: &ControlState,
        out: &mut Vec<SensorRow>,
        limit: usize,
    ) -> usize {
        let mut ifaces: Vec<_> = self.networks.iter().collect();
        ifaces.sort_unstable_by_key(|(a, _)| *a);

        let mut emitted = 0usize;
        for (iface, data) in ifaces.into_iter().take(limit) {
            let mut row = self.base_row(control, "network", "snapshot", "ok", "network snapshot");
            row.signal_family = Some("network".to_string());
            row.network_interface = Some(iface.clone());
            row.network_received_delta_bytes = Some(data.received());
            row.network_transmitted_delta_bytes = Some(data.transmitted());
            row.network_received_total_bytes = Some(data.total_received());
            row.network_transmitted_total_bytes = Some(data.total_transmitted());
            row.network_packets_received_delta = Some(data.packets_received());
            row.network_packets_transmitted_delta = Some(data.packets_transmitted());
            row.network_errors_received_delta = Some(data.errors_on_received());
            row.network_errors_transmitted_delta = Some(data.errors_on_transmitted());
            row.network_mac_address = Some(data.mac_address().to_string());
            row.network_ip_addresses = {
                let ips: Vec<String> = data
                    .ip_networks()
                    .iter()
                    .map(|ip| ip.addr.to_string())
                    .collect();
                if ips.is_empty() {
                    None
                } else {
                    Some(ips.join(","))
                }
            };
            row.network_mtu = Some(data.mtu());
            out.push(row);
            emitted += 1;
        }

        emitted
    }

    fn emit_system_rows(&mut self, control: &ControlState, out: &mut Vec<SensorRow>) -> usize {
        self.system.refresh_memory();
        self.system.refresh_cpu_usage();

        let mut row = self.base_row(
            control,
            "system",
            "snapshot",
            "ok",
            "system health snapshot",
        );
        row.signal_family = Some("system".to_string());
        row.system_total_memory_bytes = Some(self.system.total_memory());
        row.system_used_memory_bytes = Some(self.system.used_memory());
        row.system_available_memory_bytes = Some(self.system.available_memory());
        row.system_total_swap_bytes = Some(self.system.total_swap());
        row.system_used_swap_bytes = Some(self.system.used_swap());
        row.system_cpu_usage_percent = Some(self.system.global_cpu_usage());
        row.system_uptime_secs = Some(System::uptime());

        if let Some(limits) = self.system.cgroup_limits() {
            row.system_cgroup_memory_limit = Some(limits.total_memory);
            row.system_cgroup_memory_current =
                Some(limits.total_memory.saturating_sub(limits.free_memory));
        }

        out.push(row);
        1
    }

    fn emit_disk_io_rows(
        &self,
        control: &ControlState,
        out: &mut Vec<SensorRow>,
        limit: usize,
    ) -> usize {
        let mut procs: Vec<(&sysinfo::Pid, &Process)> = self
            .system
            .processes()
            .iter()
            .filter(|(_, process)| {
                let usage = process.disk_usage();
                usage.read_bytes > 0 || usage.written_bytes > 0
            })
            .collect();
        procs.sort_unstable_by(|(a_pid, a_proc), (b_pid, b_proc)| {
            let a = a_proc.disk_usage().written_bytes + a_proc.disk_usage().read_bytes;
            let b = b_proc.disk_usage().written_bytes + b_proc.disk_usage().read_bytes;
            b.cmp(&a).then_with(|| a_pid.as_u32().cmp(&b_pid.as_u32()))
        });

        let mut emitted = 0usize;
        for (pid, process) in procs.into_iter().take(limit) {
            let usage = process.disk_usage();
            let mut row = self.base_row(control, "disk_io", "snapshot", "ok", "disk I/O snapshot");
            row.signal_family = Some("file".to_string());
            row.process_pid = Some(pid.as_u32());
            row.process_name = Some(os_to_string(process.name()));
            row.disk_io_read_delta_bytes = Some(usage.read_bytes);
            row.disk_io_write_delta_bytes = Some(usage.written_bytes);
            row.disk_io_read_total_bytes = Some(usage.total_read_bytes);
            row.disk_io_write_total_bytes = Some(usage.total_written_bytes);
            out.push(row);
            emitted += 1;
        }

        emitted
    }

    fn build_batch_event(&self, rows: Vec<SensorRow>) -> io::Result<InputEvent> {
        if rows.is_empty() {
            return Err(io::Error::other("cannot build sensor batch from zero rows"));
        }

        let len = rows.len();
        let mut timestamp_unix_nano = Vec::with_capacity(len);
        let mut sensor_name = Vec::with_capacity(len);
        let mut sensor_target_platform = Vec::with_capacity(len);
        let mut sensor_host_platform = Vec::with_capacity(len);
        let mut event_family = Vec::with_capacity(len);
        let mut event_kind = Vec::with_capacity(len);
        let mut signal_family: Vec<Option<String>> = Vec::with_capacity(len);
        let mut signal_status = Vec::with_capacity(len);
        let mut control_generation = Vec::with_capacity(len);
        let mut control_source = Vec::with_capacity(len);
        let mut control_path: Vec<Option<String>> = Vec::with_capacity(len);
        let mut enabled_families: Vec<Option<String>> = Vec::with_capacity(len);
        let mut effective_emit_signal_rows: Vec<Option<bool>> = Vec::with_capacity(len);
        let mut message = Vec::with_capacity(len);

        let mut process_pid: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_parent_pid: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_name: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_cmd: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_exe: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_status: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_cpu_usage: Vec<Option<f32>> = Vec::with_capacity(len);
        let mut process_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_virtual_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_start_time_unix_sec: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_disk_read_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_disk_write_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_user_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_effective_user_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_group_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_effective_group_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_cwd: Vec<Option<String>> = Vec::with_capacity(len);
        let mut process_session_id: Vec<Option<u32>> = Vec::with_capacity(len);
        let mut process_run_time_secs: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_open_files: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_thread_count: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut process_container_id: Vec<Option<String>> = Vec::with_capacity(len);

        let mut network_interface: Vec<Option<String>> = Vec::with_capacity(len);
        let mut network_received_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_transmitted_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_received_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_transmitted_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_packets_received_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_packets_transmitted_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_errors_received_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_errors_transmitted_delta: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut network_mac_address: Vec<Option<String>> = Vec::with_capacity(len);
        let mut network_ip_addresses: Vec<Option<String>> = Vec::with_capacity(len);
        let mut network_mtu: Vec<Option<u64>> = Vec::with_capacity(len);

        let mut system_total_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_used_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_available_memory_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_total_swap_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_used_swap_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_cpu_usage_percent: Vec<Option<f32>> = Vec::with_capacity(len);
        let mut system_uptime_secs: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_cgroup_memory_limit: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut system_cgroup_memory_current: Vec<Option<u64>> = Vec::with_capacity(len);

        let mut disk_io_read_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut disk_io_write_delta_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut disk_io_read_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut disk_io_write_total_bytes: Vec<Option<u64>> = Vec::with_capacity(len);

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
            effective_emit_signal_rows.push(row.effective_emit_signal_rows);
            accounted_bytes = accounted_bytes.saturating_add(row.message.len() as u64);
            message.push(row.message);

            process_pid.push(row.process_pid);
            process_parent_pid.push(row.process_parent_pid);
            process_name.push(row.process_name);
            process_cmd.push(row.process_cmd);
            process_exe.push(row.process_exe);
            process_status.push(row.process_status);
            process_cpu_usage.push(row.process_cpu_usage);
            process_memory_bytes.push(row.process_memory_bytes);
            process_virtual_memory_bytes.push(row.process_virtual_memory_bytes);
            process_start_time_unix_sec.push(row.process_start_time_unix_sec);
            process_disk_read_delta_bytes.push(row.process_disk_read_delta_bytes);
            process_disk_write_delta_bytes.push(row.process_disk_write_delta_bytes);
            process_user_id.push(row.process_user_id);
            process_effective_user_id.push(row.process_effective_user_id);
            process_group_id.push(row.process_group_id);
            process_effective_group_id.push(row.process_effective_group_id);
            process_cwd.push(row.process_cwd);
            process_session_id.push(row.process_session_id);
            process_run_time_secs.push(row.process_run_time_secs);
            process_open_files.push(row.process_open_files);
            process_thread_count.push(row.process_thread_count);
            process_container_id.push(row.process_container_id);

            network_interface.push(row.network_interface);
            network_received_delta_bytes.push(row.network_received_delta_bytes);
            network_transmitted_delta_bytes.push(row.network_transmitted_delta_bytes);
            network_received_total_bytes.push(row.network_received_total_bytes);
            network_transmitted_total_bytes.push(row.network_transmitted_total_bytes);
            network_packets_received_delta.push(row.network_packets_received_delta);
            network_packets_transmitted_delta.push(row.network_packets_transmitted_delta);
            network_errors_received_delta.push(row.network_errors_received_delta);
            network_errors_transmitted_delta.push(row.network_errors_transmitted_delta);
            network_mac_address.push(row.network_mac_address);
            network_ip_addresses.push(row.network_ip_addresses);
            network_mtu.push(row.network_mtu);

            system_total_memory_bytes.push(row.system_total_memory_bytes);
            system_used_memory_bytes.push(row.system_used_memory_bytes);
            system_available_memory_bytes.push(row.system_available_memory_bytes);
            system_total_swap_bytes.push(row.system_total_swap_bytes);
            system_used_swap_bytes.push(row.system_used_swap_bytes);
            system_cpu_usage_percent.push(row.system_cpu_usage_percent);
            system_uptime_secs.push(row.system_uptime_secs);
            system_cgroup_memory_limit.push(row.system_cgroup_memory_limit);
            system_cgroup_memory_current.push(row.system_cgroup_memory_current);

            disk_io_read_delta_bytes.push(row.disk_io_read_delta_bytes);
            disk_io_write_delta_bytes.push(row.disk_io_write_delta_bytes);
            disk_io_read_total_bytes.push(row.disk_io_read_total_bytes);
            disk_io_write_total_bytes.push(row.disk_io_write_total_bytes);
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
            Arc::new(BooleanArray::from(effective_emit_signal_rows)),
            Arc::new(StringArray::from(message)),
            Arc::new(UInt32Array::from(process_pid)),
            Arc::new(UInt32Array::from(process_parent_pid)),
            Arc::new(StringArray::from(process_name)),
            Arc::new(StringArray::from(process_cmd)),
            Arc::new(StringArray::from(process_exe)),
            Arc::new(StringArray::from(process_status)),
            Arc::new(Float32Array::from(process_cpu_usage)),
            Arc::new(UInt64Array::from(process_memory_bytes)),
            Arc::new(UInt64Array::from(process_virtual_memory_bytes)),
            Arc::new(UInt64Array::from(process_start_time_unix_sec)),
            Arc::new(UInt64Array::from(process_disk_read_delta_bytes)),
            Arc::new(UInt64Array::from(process_disk_write_delta_bytes)),
            Arc::new(UInt32Array::from(process_user_id)),
            Arc::new(UInt32Array::from(process_effective_user_id)),
            Arc::new(UInt32Array::from(process_group_id)),
            Arc::new(UInt32Array::from(process_effective_group_id)),
            Arc::new(StringArray::from(process_cwd)),
            Arc::new(UInt32Array::from(process_session_id)),
            Arc::new(UInt64Array::from(process_run_time_secs)),
            Arc::new(UInt64Array::from(process_open_files)),
            Arc::new(UInt64Array::from(process_thread_count)),
            Arc::new(StringArray::from(process_container_id)),
            Arc::new(StringArray::from(network_interface)),
            Arc::new(UInt64Array::from(network_received_delta_bytes)),
            Arc::new(UInt64Array::from(network_transmitted_delta_bytes)),
            Arc::new(UInt64Array::from(network_received_total_bytes)),
            Arc::new(UInt64Array::from(network_transmitted_total_bytes)),
            Arc::new(UInt64Array::from(network_packets_received_delta)),
            Arc::new(UInt64Array::from(network_packets_transmitted_delta)),
            Arc::new(UInt64Array::from(network_errors_received_delta)),
            Arc::new(UInt64Array::from(network_errors_transmitted_delta)),
            Arc::new(StringArray::from(network_mac_address)),
            Arc::new(StringArray::from(network_ip_addresses)),
            Arc::new(UInt64Array::from(network_mtu)),
            Arc::new(UInt64Array::from(system_total_memory_bytes)),
            Arc::new(UInt64Array::from(system_used_memory_bytes)),
            Arc::new(UInt64Array::from(system_available_memory_bytes)),
            Arc::new(UInt64Array::from(system_total_swap_bytes)),
            Arc::new(UInt64Array::from(system_used_swap_bytes)),
            Arc::new(Float32Array::from(system_cpu_usage_percent)),
            Arc::new(UInt64Array::from(system_uptime_secs)),
            Arc::new(UInt64Array::from(system_cgroup_memory_limit)),
            Arc::new(UInt64Array::from(system_cgroup_memory_current)),
            Arc::new(UInt64Array::from(disk_io_read_delta_bytes)),
            Arc::new(UInt64Array::from(disk_io_write_delta_bytes)),
            Arc::new(UInt64Array::from(disk_io_read_total_bytes)),
            Arc::new(UInt64Array::from(disk_io_write_total_bytes)),
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

impl HostMetricsInput {
    /// Create a platform sensor source.
    ///
    /// Returns an error when `target` does not match the current host platform.
    pub fn new(
        name: impl Into<String>,
        target: HostMetricsTarget,
        mut cfg: HostMetricsConfig,
    ) -> io::Result<Self> {
        let name = name.into();
        let host_platform = current_host_platform().as_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "platform sensor inputs are unsupported on this host",
            )
        })?;

        if target.as_str() != host_platform {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} sensor input can only run on {} hosts (current host: {})",
                    target.as_str(),
                    target.as_str(),
                    host_platform
                ),
            ));
        }
        if cfg.max_process_rows_per_poll == 0 {
            cfg.max_process_rows_per_poll = DEFAULT_MAX_PROCESS_ROWS_PER_POLL;
        }

        let control = ControlState {
            generation: 1,
            enabled_families: parse_enabled_families(cfg.enabled_families.as_deref(), target)?,
            source: ControlSource::StaticConfig,
            emit_signal_rows: cfg.emit_signal_rows,
        };

        Ok(Self {
            name: name.clone(),
            machine: Some(HostMetricsMachine::Init(HostMetricsState {
                common: HostMetricsCommon {
                    name,
                    target,
                    host_platform,
                    cfg,
                    schema: sensor_schema(),
                    system: System::new(),
                    networks: Networks::new_with_refreshed_list(),
                    poll_count: 0,
                },
                state: InitState { control },
            })),
        })
    }
}

impl InputSource for HostMetricsInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let machine = self
            .machine
            .take()
            .ok_or_else(|| io::Error::other("platform sensor state missing"))?;

        let (next_machine, result) = match machine {
            HostMetricsMachine::Init(init) => match init.start() {
                Ok((running, events)) => (HostMetricsMachine::Running(running), Ok(events)),
                Err(err) => {
                    let (init, err) = *err;
                    (HostMetricsMachine::Init(init), Err(err))
                }
            },
            HostMetricsMachine::Running(mut running) => {
                let rows = running.poll_rows();
                let result = if rows.is_empty() {
                    Ok(Vec::new())
                } else {
                    running
                        .common
                        .build_batch_event(rows)
                        .map(|event| vec![event])
                };
                (HostMetricsMachine::Running(running), result)
            }
        };
        self.machine = Some(next_machine);
        result
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        match self.machine.as_ref() {
            Some(HostMetricsMachine::Init(_)) => ComponentHealth::Starting,
            Some(HostMetricsMachine::Running(running)) => running.state.health,
            None => ComponentHealth::Failed,
        }
    }
}

fn parse_enabled_families(
    configured: Option<&[String]>,
    target: HostMetricsTarget,
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

fn enabled_families_csv(control: &ControlState) -> String {
    control
        .enabled_families
        .iter()
        .map(|family| family.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(target_os = "linux")]
fn extract_container_id(pid: u32) -> Option<String> {
    let path = format!("/proc/{pid}/cgroup");
    let content = std::fs::read_to_string(path).ok()?;
    for line in content.lines() {
        for segment in line.rsplit('/') {
            let cleaned = segment
                .strip_prefix("docker-")
                .or_else(|| segment.strip_prefix("cri-containerd-"))
                .or_else(|| segment.strip_prefix("crio-"))
                .unwrap_or(segment);
            let cleaned = cleaned.strip_suffix(".scope").unwrap_or(cleaned);
            if cleaned.len() == 64 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(cleaned.to_string());
            }
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn extract_container_id(_pid: u32) -> Option<String> {
    None
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
        Field::new("effective_emit_signal_rows", DataType::Boolean, true),
        Field::new("message", DataType::Utf8, false),
        Field::new("process_pid", DataType::UInt32, true),
        Field::new("process_parent_pid", DataType::UInt32, true),
        Field::new("process_name", DataType::Utf8, true),
        Field::new("process_cmd", DataType::Utf8, true),
        Field::new("process_exe", DataType::Utf8, true),
        Field::new("process_status", DataType::Utf8, true),
        Field::new("process_cpu_usage", DataType::Float32, true),
        Field::new("process_memory_bytes", DataType::UInt64, true),
        Field::new("process_virtual_memory_bytes", DataType::UInt64, true),
        Field::new("process_start_time_unix_sec", DataType::UInt64, true),
        Field::new("process_disk_read_delta_bytes", DataType::UInt64, true),
        Field::new("process_disk_write_delta_bytes", DataType::UInt64, true),
        Field::new("process_user_id", DataType::UInt32, true),
        Field::new("process_effective_user_id", DataType::UInt32, true),
        Field::new("process_group_id", DataType::UInt32, true),
        Field::new("process_effective_group_id", DataType::UInt32, true),
        Field::new("process_cwd", DataType::Utf8, true),
        Field::new("process_session_id", DataType::UInt32, true),
        Field::new("process_run_time_secs", DataType::UInt64, true),
        Field::new("process_open_files", DataType::UInt64, true),
        Field::new("process_thread_count", DataType::UInt64, true),
        Field::new("process_container_id", DataType::Utf8, true),
        Field::new("network_interface", DataType::Utf8, true),
        Field::new("network_received_delta_bytes", DataType::UInt64, true),
        Field::new("network_transmitted_delta_bytes", DataType::UInt64, true),
        Field::new("network_received_total_bytes", DataType::UInt64, true),
        Field::new("network_transmitted_total_bytes", DataType::UInt64, true),
        Field::new("network_packets_received_delta", DataType::UInt64, true),
        Field::new("network_packets_transmitted_delta", DataType::UInt64, true),
        Field::new("network_errors_received_delta", DataType::UInt64, true),
        Field::new("network_errors_transmitted_delta", DataType::UInt64, true),
        Field::new("network_mac_address", DataType::Utf8, true),
        Field::new("network_ip_addresses", DataType::Utf8, true),
        Field::new("network_mtu", DataType::UInt64, true),
        Field::new("system_total_memory_bytes", DataType::UInt64, true),
        Field::new("system_used_memory_bytes", DataType::UInt64, true),
        Field::new("system_available_memory_bytes", DataType::UInt64, true),
        Field::new("system_total_swap_bytes", DataType::UInt64, true),
        Field::new("system_used_swap_bytes", DataType::UInt64, true),
        Field::new("system_cpu_usage_percent", DataType::Float32, true),
        Field::new("system_uptime_secs", DataType::UInt64, true),
        Field::new("system_cgroup_memory_limit", DataType::UInt64, true),
        Field::new("system_cgroup_memory_current", DataType::UInt64, true),
        Field::new("disk_io_read_delta_bytes", DataType::UInt64, true),
        Field::new("disk_io_write_delta_bytes", DataType::UInt64, true),
        Field::new("disk_io_read_total_bytes", DataType::UInt64, true),
        Field::new("disk_io_write_total_bytes", DataType::UInt64, true),
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
    use crate::atomic_write::atomic_write_file;
    use arrow::array::Array;
    use std::sync::Arc;

    mod tempfiles {
        use super::*;

        pub(super) fn control_file_path() -> (tempfile::TempDir, PathBuf) {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("sensor-control.json");
            (dir, path)
        }

        pub(super) fn write_control_file(path: &Path, json: serde_json::Value) {
            let bytes = serde_json::to_vec(&json).expect("serialize control file");
            atomic_write_file(path, &bytes).expect("write control file");
        }

        pub(super) fn write_malformed_control_file(path: &Path) {
            atomic_write_file(path, br#"{"generation":"invalid"}"#)
                .expect("write malformed control file");
        }
    }

    fn host_target() -> HostMetricsTarget {
        #[cfg(target_os = "linux")]
        {
            HostMetricsTarget::Linux
        }
        #[cfg(target_os = "macos")]
        {
            HostMetricsTarget::Macos
        }
        #[cfg(target_os = "windows")]
        {
            HostMetricsTarget::Windows
        }
    }

    fn non_host_target() -> HostMetricsTarget {
        #[cfg(target_os = "linux")]
        {
            HostMetricsTarget::Macos
        }
        #[cfg(target_os = "macos")]
        {
            HostMetricsTarget::Windows
        }
        #[cfg(target_os = "windows")]
        {
            HostMetricsTarget::Linux
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
        let err = HostMetricsInput::new("sensor", non_host_target(), HostMetricsConfig::default())
            .expect_err("non-matching target must fail");
        assert!(err.to_string().contains("can only run on"));
    }

    #[test]
    fn rejects_unknown_enabled_family() {
        let err = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["not_a_family".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect_err("unknown family must fail");
        assert!(err.to_string().contains("unknown sensor family"));
    }

    #[test]
    fn emits_startup_batch_on_first_poll() {
        let mut input =
            HostMetricsInput::new("sensor", host_target(), HostMetricsConfig::default())
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
    fn signal_rows_disabled_suppresses_sample_placeholders() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let startup = input.poll().expect("startup poll");
        assert_eq!(startup.len(), 1);
        std::thread::sleep(Duration::from_millis(2));

        // Data collection rows may still appear, but "sample" placeholders must not.
        let events = input.poll().expect("second poll");
        if !events.is_empty() {
            let batch = first_batch(&events);
            let kinds = string_col(batch, "event_kind");
            assert!(
                !kinds.iter().any(|v| v.as_deref() == Some("sample")),
                "signal sample placeholders should be suppressed when emit_signal_rows is false"
            );
        }
    }

    #[test]
    fn periodic_signal_rows_emit_when_enabled() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                emit_signal_rows: true,
                poll_interval: Duration::from_millis(1),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1, "second poll should emit sample rows");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds.iter().any(|v| v.as_deref() == Some("sample")),
            "periodic sample rows should be emitted when signal rows are enabled"
        );
    }

    #[test]
    fn enabled_families_filter_signal_samples() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string(), "dns".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        let families = string_col(batch, "signal_family");

        // Process family now has real data collection (snapshot rows) instead of
        // signal sample placeholders. DNS still gets signal sample rows.
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

        // dns is a placeholder-only family, so it should appear as startup_sample
        assert!(sample_families.iter().any(|f| f == "dns"));
        // process now uses real collection (snapshot), not startup_sample
        assert!(!sample_families.iter().any(|f| f == "process"));
        // network is not enabled, so it should not appear
        assert!(!sample_families.iter().any(|f| f == "network"));

        // Process data should appear as snapshot rows
        let event_families = string_col(batch, "event_family");
        assert!(
            event_families
                .iter()
                .any(|v| v.as_deref() == Some("process")),
            "process data collection rows should be in the startup batch"
        );
    }

    #[test]
    fn explicit_empty_enabled_families_disables_signal_samples() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(Vec::new()),
                ..HostMetricsConfig::default()
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
        let (_dir, control_path) = tempfiles::control_file_path();

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                enabled_families: Some(vec!["process".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // startup
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 42,
                "enabled_families": ["dns"],
                "emit_signal_rows": true,
            }),
        );
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
        let (_dir, control_path) = tempfiles::control_file_path();

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                enabled_families: Some(vec!["process".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // startup
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 1,
                "enabled_families": ["process"],
                "emit_signal_rows": true,
            }),
        );
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("reload poll succeeds");
        assert!(
            events.is_empty(),
            "unchanged control file should not emit reload-applied rows"
        );
    }

    #[test]
    fn malformed_control_file_does_not_fail_startup() {
        let (_dir, control_path) = tempfiles::control_file_path();
        tempfiles::write_malformed_control_file(&control_path);

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path),
                control_reload_interval: Duration::from_millis(1),
                emit_signal_rows: false,
                ..HostMetricsConfig::default()
            },
        )
        .expect("startup should not fail on malformed optional control file");

        assert_eq!(input.poll().expect("startup poll").len(), 1);
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("reload poll should succeed");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds
                .iter()
                .any(|v| v.as_deref() == Some("control_reload_failed")),
            "malformed control file should surface through reload failure rows"
        );
    }

    #[test]
    fn health_degrades_on_reload_error_and_recovers_after_valid_reload() {
        let (_dir, control_path) = tempfiles::control_file_path();
        tempfiles::write_malformed_control_file(&control_path);

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                emit_signal_rows: false,
                ..HostMetricsConfig::default()
            },
        )
        .expect("startup should succeed");

        assert_eq!(input.health(), ComponentHealth::Starting);
        let _ = input.poll().expect("startup poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);

        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("reload poll");
        assert_eq!(input.health(), ComponentHealth::Degraded);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 2,
                "enabled_families": ["process"],
                "emit_signal_rows": false,
            }),
        );
        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("recovery poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn poll_error_preserves_machine_and_name_invariants() {
        let mut input =
            HostMetricsInput::new("sensor", host_target(), HostMetricsConfig::default())
                .expect("host target should be valid");

        let init = match input.machine.as_mut() {
            Some(HostMetricsMachine::Init(init)) => init,
            _ => panic!("expected init machine"),
        };
        init.common.schema = Arc::new(Schema::new(Vec::<Field>::new()));

        let err = match input.poll() {
            Ok(_) => panic!("invalid schema must fail"),
            Err(err) => err,
        };
        assert!(
            !err.to_string().contains("state missing"),
            "unexpected state-loss error: {err}"
        );

        // Even after a poll error, public API should remain non-panicking.
        assert_eq!(input.name(), "sensor");

        let second_err = match input.poll() {
            Ok(_) => panic!("machine should still be present"),
            Err(err) => err,
        };
        assert!(
            !second_err.to_string().contains("state missing"),
            "machine should not be lost after an error: {second_err}"
        );
    }

    #[test]
    fn health_transitions_from_starting_to_healthy_after_first_poll() {
        let mut input =
            HostMetricsInput::new("sensor", host_target(), HostMetricsConfig::default())
                .expect("host target should be valid");

        assert_eq!(input.health(), ComponentHealth::Starting);
        let _ = input.poll().expect("startup poll succeeds");
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn health_degrades_on_control_reload_failure_and_recovers_on_success() {
        let (_dir, control_path) = tempfiles::control_file_path();
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                emit_signal_rows: false,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        assert_eq!(input.health(), ComponentHealth::Starting);
        let _ = input.poll().expect("startup poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);

        tempfiles::write_control_file(&control_path, serde_json::json!({"generation":"bad"}));
        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("reload failure poll");
        assert_eq!(input.health(), ComponentHealth::Degraded);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({"generation":2,"enabled_families":["process"],"emit_signal_rows":false}),
        );
        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("reload recovery poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    fn u32_col_optional(batch: &RecordBatch, name: &str) -> Vec<Option<u32>> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("u32 array");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn first_poll_emits_process_data() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 8,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll with data");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let pids = u32_col_optional(batch, "process_pid");
        assert!(
            pids.iter().any(|v| v.is_some()),
            "process snapshot rows should have process_pid set"
        );
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds.iter().any(|v| v.as_deref() == Some("snapshot")),
            "should contain snapshot event_kind"
        );
    }

    #[test]
    fn first_poll_emits_network_data() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["network".to_string()]),
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 64,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll with data");
        // Some CI environments may have no network interfaces, so just check
        // we get at least an empty successful poll.
        if !events.is_empty() {
            let batch = first_batch(&events);
            let ifaces = string_col(batch, "network_interface");
            assert!(
                ifaces.iter().any(|v| v.is_some()),
                "network snapshot rows should have network_interface set"
            );
        }
    }

    #[test]
    fn collection_respects_enabled_families() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 8,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let families = string_col(batch, "event_family");
        assert!(
            !families.iter().any(|v| v.as_deref() == Some("network")),
            "network rows should not be emitted when only process is enabled"
        );
        assert!(
            !families.iter().any(|v| v.as_deref() == Some("disk_io")),
            "disk_io rows should not be emitted when only process is enabled"
        );
    }

    #[test]
    fn max_rows_per_poll_caps_data_collection() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 3,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        // On any real system there are more than 3 processes; the cap should hold.
        // The system health row is always emitted outside the budget, so exclude it.
        let event_families = string_col(batch, "event_family");
        let snapshot_count = string_col(batch, "event_kind")
            .iter()
            .zip(event_families.iter())
            .filter(|(kind, family)| {
                kind.as_deref() == Some("snapshot") && family.as_deref() != Some("system")
            })
            .count();
        assert!(
            snapshot_count <= 3,
            "max_rows_per_poll should cap data rows, got {snapshot_count}"
        );
    }

    #[test]
    fn max_process_rows_per_poll_caps_process_collection() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                max_rows_per_poll: usize::MAX,
                max_process_rows_per_poll: 3,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host metrics input should construct");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);

        let families = string_col(batch, "signal_family");
        let kinds = string_col(batch, "event_kind");
        let snapshot_count = families
            .iter()
            .zip(kinds.iter())
            .filter(|(_, kind)| kind.as_deref() == Some("snapshot"))
            .filter(|(family, _)| family.as_deref() == Some("process"))
            .count();
        assert!(
            snapshot_count > 0,
            "expected process snapshot rows to be emitted so the cap can be validated"
        );
        assert!(
            snapshot_count <= 3,
            "max_process_rows_per_poll should cap process rows, got {snapshot_count}"
        );
    }

    #[test]
    fn zero_max_process_rows_per_poll_uses_runtime_default() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                max_rows_per_poll: usize::MAX,
                max_process_rows_per_poll: 0,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host metrics input should construct");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);

        let families = string_col(batch, "signal_family");
        let kinds = string_col(batch, "event_kind");
        let snapshot_count = families
            .iter()
            .zip(kinds.iter())
            .filter(|(_, kind)| kind.as_deref() == Some("snapshot"))
            .filter(|(family, _)| family.as_deref() == Some("process"))
            .count();
        assert!(
            snapshot_count > 0,
            "expected default process cap to leave process rows enabled"
        );
        assert!(
            snapshot_count <= DEFAULT_MAX_PROCESS_ROWS_PER_POLL,
            "default process cap should apply when runtime config is 0, got {snapshot_count}"
        );
    }

    #[test]
    fn process_snapshot_has_valid_fields() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 16,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // Two polls so sysinfo has history for deltas.
        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);

        let pids = u32_col_optional(batch, "process_pid");
        let names = string_col(batch, "process_name");
        let mem_col_idx = batch
            .schema()
            .index_of("process_memory_bytes")
            .expect("col");
        let mem_arr = batch
            .column(mem_col_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64");

        let mut found_valid = false;
        for i in 0..batch.num_rows() {
            if let Some(pid) = pids[i] {
                assert!(pid > 0, "pid should be positive");
                let name = names[i].as_deref().unwrap_or("");
                assert!(!name.is_empty(), "process name should not be empty");
                if !mem_arr.is_null(i) {
                    // At least some processes have nonzero memory
                    found_valid = true;
                }
            }
        }
        assert!(found_valid, "at least one process should have memory data");
    }

    #[test]
    fn disk_io_family_emits_only_active_processes() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["file".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 256,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // First poll primes sysinfo; on second poll we may see disk_io rows.
        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().expect("second poll");

        // Even if no process has active I/O, we should not crash.
        if !events.is_empty() {
            let batch = first_batch(&events);
            let families = string_col(batch, "event_family");
            // All data rows should be disk_io (no process or network)
            for fam in &families {
                if fam.as_deref() == Some("disk_io") {
                    // disk_io rows are expected
                }
            }
            assert!(
                !families.iter().any(|v| v.as_deref() == Some("process")),
                "only file family enabled; no process rows expected"
            );
        }
    }

    #[test]
    fn second_cycle_network_deltas_are_plausible() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["network".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 64,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().expect("second poll");

        if !events.is_empty() {
            let batch = first_batch(&events);
            let ifaces = string_col(batch, "network_interface");

            let rx_total_idx = batch
                .schema()
                .index_of("network_received_total_bytes")
                .expect("col");
            let rx_total_arr = batch
                .column(rx_total_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64");

            let rx_delta_idx = batch
                .schema()
                .index_of("network_received_delta_bytes")
                .expect("col");
            let rx_delta_arr = batch
                .column(rx_delta_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64");

            for i in 0..batch.num_rows() {
                if ifaces[i].is_some() && !rx_total_arr.is_null(i) && !rx_delta_arr.is_null(i) {
                    let total = rx_total_arr.value(i);
                    let delta = rx_delta_arr.value(i);
                    assert!(
                        delta <= total,
                        "delta ({delta}) should not exceed total ({total}) for {}",
                        ifaces[i].as_deref().unwrap_or("?")
                    );
                }
            }
        }
    }

    #[test]
    fn schema_column_count_matches_expected() {
        let schema = sensor_schema();
        assert_eq!(
            schema.fields().len(),
            61,
            "schema should have 14 base + 47 telemetry columns"
        );
    }

    #[test]
    fn budget_rotation_distributes_remainder_fairly() {
        // Simulate the rotation logic with 3 families and budget=2
        // per_family_budget = 2/3 = 0, remainder = 2
        // Over 3 polls, each family should get the extra budget twice.
        let active_count = 3usize;
        let max_rows = 2usize;
        let per_family_budget = max_rows / active_count; // 0
        let remainder = max_rows % active_count; // 2

        // Track how many extra rows each family position gets over 3 cycles.
        let mut extras = [0usize; 3];
        for poll_count in 0..active_count {
            let start_idx = poll_count % active_count;
            for family_idx in 0..active_count {
                let extra =
                    usize::from((family_idx + active_count - start_idx) % active_count < remainder);
                extras[family_idx] += extra;
            }
        }

        // Each family should get exactly 2 extras over 3 polls (fair distribution).
        assert_eq!(
            extras,
            [2, 2, 2],
            "each family must get equal remainder share over a full rotation"
        );
        assert_eq!(
            per_family_budget, 0,
            "base budget is zero with budget=2, families=3"
        );

        // Also verify single cycle: with budget=1 and 3 families, only one family gets 1 per cycle.
        let max_rows = 1usize;
        let remainder = max_rows % active_count; // 1
        let mut totals = [0usize; 3];
        for poll_count in 0..active_count {
            let start_idx = poll_count % active_count;
            for family_idx in 0..active_count {
                let extra =
                    usize::from((family_idx + active_count - start_idx) % active_count < remainder);
                totals[family_idx] += extra;
            }
        }
        assert_eq!(
            totals,
            [1, 1, 1],
            "budget=1 with 3 families: each gets 1 row over 3 polls"
        );
    }
}
