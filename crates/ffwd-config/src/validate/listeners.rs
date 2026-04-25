use crate::types::{ConfigError, InputType};
use std::collections::HashMap;

use super::common::{validation_error, validation_message};
use super::endpoints::{canonical_listen_host_key, validate_host_port};
use super::sensors::sensor_supported_families;

/// Ensures a systemd unit name has a suffix, appending `.service` when absent.
pub(super) fn normalize_unit_name(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{name}.service")
    }
}

pub(super) fn track_listen_addr_uniqueness(
    seen_listen_addrs: &mut HashMap<String, String>,
    transport: &str,
    pipeline_name: &str,
    input_label: &str,
    listen: &str,
) -> Result<(), ConfigError> {
    let Some(listen_key) = canonical_listen_addr_key(transport, listen).map_err(|err| {
        ConfigError::Validation(format!(
            "pipeline '{pipeline_name}' input '{input_label}': {}",
            validation_message(err)
        ))
    })?
    else {
        return Ok(());
    };
    let current_ref = format!("pipeline '{pipeline_name}' input '{input_label}'");
    if let Some(previous_ref) = seen_listen_addrs.get(&listen_key) {
        return Err(ConfigError::Validation(format!(
            "{current_ref}: listen address '{listen}' duplicates {previous_ref}"
        )));
    }
    seen_listen_addrs.insert(listen_key, current_ref);
    Ok(())
}

fn canonical_listen_addr_key(transport: &str, listen: &str) -> Result<Option<String>, ConfigError> {
    validate_host_port(listen)?;
    let (host, port_str) = if listen.starts_with('[') {
        let close_bracket = listen
            .find(']')
            .ok_or_else(|| validation_error(format!("'{listen}' has mismatched brackets")))?;
        if !listen[close_bracket..].starts_with("]:") {
            return Err(validation_error(format!(
                "'{listen}' is missing a port after IPv6 brackets"
            )));
        }
        (&listen[..=close_bracket], &listen[close_bracket + 2..])
    } else {
        listen.rsplit_once(':').ok_or_else(|| {
            validation_error(format!(
                "'{listen}' is missing a port (expected format host:port)"
            ))
        })?
    };
    let port = port_str
        .parse::<u16>()
        .map_err(|_e| validation_error(format!("'{listen}' has an invalid port '{port_str}'")))?;
    if port == 0 {
        return Ok(None);
    }
    Ok(Some(format!(
        "{transport}:{}:{port}",
        canonical_listen_host_key(host)
    )))
}

pub(super) fn sensor_supported_families_csv(input_type: &InputType) -> String {
    sensor_supported_families(input_type).join(",")
}
