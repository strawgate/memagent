use crate::types::{ConfigError, InputType};

pub(super) fn sensor_supported_families(input_type: &InputType) -> &'static [&'static str] {
    match input_type {
        InputType::LinuxEbpfSensor => &["process", "file", "network", "dns", "authz"],
        InputType::HostMetrics => {
            #[cfg(target_os = "linux")]
            {
                &["process", "file", "network", "dns", "authz"]
            }
            #[cfg(target_os = "macos")]
            {
                &["process", "file", "network", "dns", "module", "authz"]
            }
            #[cfg(target_os = "windows")]
            {
                &[
                    "process", "file", "network", "dns", "module", "registry", "authz",
                ]
            }
            #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
            {
                &[]
            }
        }
        InputType::MacosEsSensor => &["process", "file", "network", "dns", "module", "authz"],
        InputType::WindowsEbpfSensor => &[
            "process", "file", "network", "dns", "module", "registry", "authz",
        ],
        _ => &[],
    }
}

pub(super) fn is_sensor_family_supported(input_type: &InputType, name: &str) -> bool {
    sensor_supported_families(input_type).contains(&name)
}

const PLATFORM_SENSOR_EVENT_TYPES: &[&str] = &[
    "exec",
    "exit",
    "tcp_connect",
    "tcp_accept",
    "file_open",
    "file_delete",
    "file_rename",
    "setuid",
    "setgid",
    "module_load",
    "ptrace",
    "memfd_create",
    "dns_query",
];

const PLATFORM_SENSOR_EVENT_TYPES_CSV: &str = "exec,exit,tcp_connect,tcp_accept,file_open,file_delete,file_rename,setuid,setgid,module_load,ptrace,memfd_create,dns_query";

pub(super) fn validate_sensor_event_type_filters(
    input_type: &InputType,
    pipeline_name: &str,
    input_label: &str,
    include_event_types: Option<&[String]>,
    exclude_event_types: Option<&[String]>,
) -> Result<(), ConfigError> {
    if include_event_types.is_none() && exclude_event_types.is_none() {
        return Ok(());
    }

    if *input_type != InputType::LinuxEbpfSensor {
        return Err(ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': sensor.include_event_types and sensor.exclude_event_types are only supported for linux_ebpf_sensor inputs"
        )));
    }

    validate_sensor_event_type_list(
        pipeline_name,
        input_label,
        "include_event_types",
        include_event_types,
    )?;
    validate_sensor_event_type_list(
        pipeline_name,
        input_label,
        "exclude_event_types",
        exclude_event_types,
    )?;
    Ok(())
}

fn validate_sensor_event_type_list(
    pipeline_name: &str,
    input_label: &str,
    field: &str,
    event_types: Option<&[String]>,
) -> Result<(), ConfigError> {
    let Some(event_types) = event_types else {
        return Ok(());
    };

    for event_type in event_types {
        let normalized = event_type.trim();
        if normalized.is_empty() {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' input '{input_label}': sensor.{field} entries must not be empty"
            )));
        }
        if event_type != normalized {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' input '{input_label}': sensor.{field} entry '{event_type}' has leading or trailing whitespace"
            )));
        }
        if !PLATFORM_SENSOR_EVENT_TYPES.contains(&normalized) {
            return Err(ConfigError::Validation(format!(
                "pipeline '{pipeline_name}' input '{input_label}': unknown sensor event type '{normalized}' for linux_ebpf_sensor input (supported: {PLATFORM_SENSOR_EVENT_TYPES_CSV})"
            )));
        }
    }

    Ok(())
}
