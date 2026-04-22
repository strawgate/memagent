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
