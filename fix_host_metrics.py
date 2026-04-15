import re

with open('crates/logfwd-io/src/host_metrics.rs', 'r') as f:
    content = f.read()

metrics_fields = """pub struct HostMetricsConfig {
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
    /// Glob patterns for process names to include.
    pub include_process_names: Option<Vec<String>>,
    /// Glob patterns for process names to exclude.
    pub exclude_process_names: Option<Vec<String>>,
    /// Specific event types to enable.
    pub include_event_types: Option<Vec<String>>,
    /// Specific event types to disable.
    pub exclude_event_types: Option<Vec<String>>,
    /// Ring buffer size in kilobytes.
    pub ring_buffer_size_kb: Option<usize>,
}

impl Default for HostMetricsConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(10_000),
            control_path: None,
            control_reload_interval: Duration::from_millis(1_000),
            enabled_families: None,
            emit_signal_rows: true,
            max_rows_per_poll: 256,
            include_process_names: None,
            exclude_process_names: None,
            include_event_types: None,
            exclude_event_types: None,
            ring_buffer_size_kb: None,
        }
    }
}"""

content = re.sub(
    r'pub struct HostMetricsConfig \{[^}]+\}\n\nimpl Default for HostMetricsConfig \{[^}]+\}\n\}',
    metrics_fields,
    content
)

with open('crates/logfwd-io/src/host_metrics.rs', 'w') as f:
    f.write(content)
