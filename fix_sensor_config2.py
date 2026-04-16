import re

with open('crates/logfwd-io/src/platform_sensor.rs', 'r') as f:
    content = f.read()

sensor_fields = """pub struct PlatformSensorConfig {
    /// Path to the compiled eBPF kernel binary.
    pub ebpf_binary_path: PathBuf,
    /// Maximum events to drain per poll cycle.
    pub max_events_per_poll: usize,
    /// Whether to filter out events from our own process.
    pub filter_self: bool,
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
    /// Poll interval in milliseconds.
    pub poll_interval_ms: Option<u64>,
}"""

content = re.sub(
    r'pub struct PlatformSensorConfig \{[^}]+\}',
    sensor_fields,
    content
)

with open('crates/logfwd-io/src/platform_sensor.rs', 'w') as f:
    f.write(content)

with open('crates/logfwd-runtime/src/pipeline/input_build.rs', 'r') as f:
    content = f.read()

content = content.replace(
"""                let sensor_cfg = logfwd_io::platform_sensor::PlatformSensorConfig {
                    ebpf_binary_path: ebpf_path.into(),
                    max_events_per_poll: max_events,
                    filter_self: true,
                };""",
"""                let sensor_cfg = logfwd_io::platform_sensor::PlatformSensorConfig {
                    ebpf_binary_path: ebpf_path.into(),
                    max_events_per_poll: max_events,
                    filter_self: true,
                    include_process_names: s.sensor.as_ref().and_then(|c| c.include_process_names.clone()),
                    exclude_process_names: s.sensor.as_ref().and_then(|c| c.exclude_process_names.clone()),
                    include_event_types: s.sensor.as_ref().and_then(|c| c.include_event_types.clone()),
                    exclude_event_types: s.sensor.as_ref().and_then(|c| c.exclude_event_types.clone()),
                    ring_buffer_size_kb: s.sensor.as_ref().and_then(|c| c.ring_buffer_size_kb),
                    poll_interval_ms: s.sensor.as_ref().and_then(|c| c.poll_interval_ms),
                };"""
)

with open('crates/logfwd-runtime/src/pipeline/input_build.rs', 'w') as f:
    f.write(content)
