use std::ffi::OsStr;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
};

use arrow::array::{ArrayRef, BooleanArray, Float32Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ffwd_types::diagnostics::ComponentHealth;
use serde::Deserialize;
use sysinfo::{Networks, Process, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

use crate::input::{SourceEvent, InputSource};

macro_rules! sensor_row_columns {
    ($callback:ident) => {
        $callback! {
            (timestamp_unix_nano, u64, UInt64Array, DataType::UInt64, false)
            (sensor_name, String, StringArray, DataType::Utf8, false)
            (sensor_target_platform, String, StringArray, DataType::Utf8, false)
            (sensor_host_platform, String, StringArray, DataType::Utf8, false)
            (event_family, String, StringArray, DataType::Utf8, false)
            (event_kind, String, StringArray, DataType::Utf8, false)
            (signal_family, Option<String>, StringArray, DataType::Utf8, true)
            (signal_status, String, StringArray, DataType::Utf8, false)
            (control_generation, u64, UInt64Array, DataType::UInt64, false)
            (control_source, String, StringArray, DataType::Utf8, false)
            (control_path, Option<String>, StringArray, DataType::Utf8, true)
            (enabled_families, Option<String>, StringArray, DataType::Utf8, true)
            (effective_emit_signal_rows, Option<bool>, BooleanArray, DataType::Boolean, true)
            (message, String, StringArray, DataType::Utf8, false)
            (process_pid, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_parent_pid, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_name, Option<String>, StringArray, DataType::Utf8, true)
            (process_cmd, Option<String>, StringArray, DataType::Utf8, true)
            (process_exe, Option<String>, StringArray, DataType::Utf8, true)
            (process_status, Option<String>, StringArray, DataType::Utf8, true)
            (process_cpu_usage, Option<f32>, Float32Array, DataType::Float32, true)
            (process_memory_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_virtual_memory_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_start_time_unix_sec, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_disk_read_delta_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_disk_write_delta_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_user_id, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_effective_user_id, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_group_id, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_effective_group_id, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_cwd, Option<String>, StringArray, DataType::Utf8, true)
            (process_session_id, Option<u32>, UInt32Array, DataType::UInt32, true)
            (process_run_time_secs, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_open_files, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_thread_count, Option<u64>, UInt64Array, DataType::UInt64, true)
            (process_container_id, Option<String>, StringArray, DataType::Utf8, true)
            (network_interface, Option<String>, StringArray, DataType::Utf8, true)
            (network_received_delta_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_transmitted_delta_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_received_total_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_transmitted_total_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_packets_received_delta, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_packets_transmitted_delta, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_errors_received_delta, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_errors_transmitted_delta, Option<u64>, UInt64Array, DataType::UInt64, true)
            (network_mac_address, Option<String>, StringArray, DataType::Utf8, true)
            (network_ip_addresses, Option<String>, StringArray, DataType::Utf8, true)
            (network_mtu, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_total_memory_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_used_memory_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_available_memory_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_total_swap_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_used_swap_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_cpu_usage_percent, Option<f32>, Float32Array, DataType::Float32, true)
            (system_uptime_secs, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_cgroup_memory_limit, Option<u64>, UInt64Array, DataType::UInt64, true)
            (system_cgroup_memory_current, Option<u64>, UInt64Array, DataType::UInt64, true)
            (disk_io_read_delta_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (disk_io_write_delta_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (disk_io_read_total_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
            (disk_io_write_total_bytes, Option<u64>, UInt64Array, DataType::UInt64, true)
        }
    };
}

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
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostMetricsTarget {
    /// Linux sensor collection.
    Linux,
    /// macOS sensor collection.
    Macos,
    /// Windows sensor collection.
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
    /// growth when hosts expose very large process tables. `None` uses the
    /// runtime default of 1024 rows per poll.
    pub max_process_rows_per_poll: Option<NonZeroUsize>,
    /// List of network interfaces to include. `None` means include all.
    ///
    /// Applied before `exclude`. If both include and exclude are set,
    /// an interface must match include to be considered, then excluded if
    /// it also matches exclude.
    pub network_include_interfaces: Option<Vec<String>>,
    /// List of network interfaces to exclude. `None` means exclude none.
    ///
    /// Applied after `include`. If an interface matches both include and
    /// exclude, it is excluded.
    pub network_exclude_interfaces: Option<Vec<String>>,
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
            max_process_rows_per_poll: None,
            network_include_interfaces: None,
            network_exclude_interfaces: None,
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
    /// Cached container IDs discovered from `/proc/<pid>/cgroup`, keyed by PID.
    ///
    /// Linux process snapshots can emit hundreds of rows per poll; caching
    /// avoids rereading procfs for the same process on every cycle.
    process_container_ids: HashMap<u32, CachedContainerId>,
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
struct CachedContainerId {
    process_start_time_unix_sec: u64,
    container_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ControlState {
    generation: u64,
    enabled_families: Vec<SignalFamily>,
    source: ControlSource,
    emit_signal_rows: bool,
}

type InitStartOk = (HostMetricsState<RunningState>, Vec<SourceEvent>);
type InitStartErr = Box<(HostMetricsState<InitState>, io::Error)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
