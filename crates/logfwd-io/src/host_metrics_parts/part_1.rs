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
