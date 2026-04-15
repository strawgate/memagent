import re

with open('crates/logfwd-config/src/types.rs', 'r') as f:
    content = f.read()

metrics_fields = """pub struct HostMetricsInputConfig {
    /// Sensor sample cadence. Defaults to 10_000 when omitted.
    pub poll_interval_ms: Option<u64>,
    /// Deprecated no-op retained for backward compatibility.
    ///
    /// Sensor inputs are Arrow-native and do not emit heartbeat rows.
    pub emit_heartbeat: Option<bool>,
    /// Optional path to a JSON control file for runtime sensor tuning.
    pub control_path: Option<String>,
    /// How often to check `control_path` for updates. Defaults to 1_000 when omitted.
    pub control_reload_interval_ms: Option<u64>,
    /// Optional explicit enabled families for this platform.
    ///
    /// `None` means "use platform defaults". `Some([])` means "disable all".
    pub enabled_families: Option<Vec<String>>,
    /// Emit periodic per-family sample rows. Defaults to true.
    pub emit_signal_rows: Option<bool>,
    /// Upper bound on data rows emitted per collection cycle. Defaults to 256.
    pub max_rows_per_poll: Option<usize>,
    /// Path to the compiled eBPF kernel binary (required for `linux_ebpf_sensor`).
    pub ebpf_binary_path: Option<String>,
    /// Maximum events to drain per poll cycle (default: 4096).
    pub max_events_per_poll: Option<usize>,
    /// Glob patterns for process names to include (e.g., `["nginx*", "python"]`).
    #[serde(default)]
    pub include_process_names: Option<Vec<String>>,
    /// Glob patterns for process names to exclude.
    #[serde(default)]
    pub exclude_process_names: Option<Vec<String>>,
    /// Specific event types to enable (e.g., `["process_exec", "tcp_connect"]`).
    #[serde(default)]
    pub include_event_types: Option<Vec<String>>,
    /// Specific event types to disable.
    #[serde(default)]
    pub exclude_event_types: Option<Vec<String>>,
    /// Ring buffer size in kilobytes.
    #[serde(default)]
    pub ring_buffer_size_kb: Option<usize>,
}"""

content = re.sub(
    r'pub struct HostMetricsInputConfig \{.*?\}',
    metrics_fields,
    content,
    flags=re.DOTALL
)

with open('crates/logfwd-config/src/types.rs', 'w') as f:
    f.write(content)
