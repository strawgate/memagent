// xtask-verify: allow(pub_module_needs_tests) // sanitize_identifier tested via validate/tests.rs

use crate::types::{ConfigError, OutputConfigV2};

pub(super) const MAX_READ_BUF_SIZE: usize = 4_194_304;

pub(super) fn validation_error(message: impl Into<String>) -> ConfigError {
    ConfigError::Validation(message.into())
}

pub(super) fn validate_bind_addr(addr: &str) -> Result<(), ConfigError> {
    validate_host_port(addr)
}

pub(super) fn validation_message(error: ConfigError) -> String {
    match error {
        ConfigError::Validation(message) => message,
        ConfigError::Io(error) => error.to_string(),
        ConfigError::Yaml(error) => error.to_string(),
    }
}

pub(super) fn output_label(output: &OutputConfigV2, index: usize) -> String {
    output
        .name()
        .map_or_else(|| format!("#{index}"), String::from)
}

pub(super) fn output_path_for_feedback_loop(output: &OutputConfigV2) -> Option<&str> {
    match output {
        OutputConfigV2::File(config) => config.path.as_deref(),
        _ => None,
    }
}

pub fn sanitize_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len().max(1));
    for (idx, ch) in name.chars().enumerate() {
        let valid = if idx == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        };
        out.push(if valid { ch } else { '_' });
    }
    if out.is_empty() { "_".to_string() } else { out }
}

/// Validate a `host:port` endpoint without a URL scheme.
///
/// Accepts DNS names, IPv4 addresses, and bracketed IPv6 addresses. URL-style
/// values are rejected so callers can distinguish bind addresses from HTTP
/// endpoints.
///
/// # Examples
///
/// ```
/// use ffwd_config::validate::validate_host_port;
///
/// assert!(validate_host_port("localhost:4317").is_ok());
/// assert!(validate_host_port("[::1]:4317").is_ok());
/// assert!(validate_host_port("http://localhost:4317").is_err());
/// ```
pub fn validate_host_port(addr: &str) -> Result<(), ConfigError> {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        return Err(validation_error(format!(
            "'{addr}' is a URL, expected host:port"
        )));
    }

    let (host, port_str) = if addr.starts_with('[') {
        let close_bracket = addr
            .find(']')
            .ok_or_else(|| validation_error(format!("'{addr}' has mismatched brackets")))?;
        let inner = &addr[1..close_bracket];
        if inner.is_empty() {
            return Err(validation_error(format!(
                "'{addr}' has an empty IPv6 address inside brackets"
            )));
        }
        inner.parse::<std::net::Ipv6Addr>().map_err(|_e| {
            validation_error(format!(
                "'{addr}' contains a non-IPv6 value inside brackets"
            ))
        })?;
        if !addr[close_bracket..].starts_with("]:") {
            return Err(validation_error(format!(
                "'{addr}' is missing a port after IPv6 brackets"
            )));
        }
        let port_str = &addr[close_bracket + 2..];
        (&addr[..=close_bracket], port_str)
    } else {
        addr.rsplit_once(':').ok_or_else(|| {
            validation_error(format!(
                "'{addr}' is missing a port (expected format host:port)"
            ))
        })?
    };

if host.is_empty() {
        return Err(validation_error(format!("'{addr}' has an empty host")));
    }

    if host.trim() != host {
        return Err(validation_error(format!(
            "'{addr}' host must not contain whitespace"
        )));
    }

    if host.contains('/') {
        return Err(validation_error(format!(
            "'{addr}' host contains a '/' (expected host:port, not a URL path)"
        )));
    }

    if !addr.starts_with('[') && host.contains(']') {
        return Err(validation_error(format!(
            "'{addr}' has an unmatched ']' in the host"
        )));
    }
    if !addr.starts_with('[') && host.contains('[') {
        return Err(validation_error(format!(
            "'{addr}' has an unmatched '[' in the host"
        )));
    }

    if !addr.starts_with('[') && host.contains(':') {
        return Err(validation_error(format!(
            "'{addr}' has multiple colons without IPv6 brackets"
        )));
    }

    port_str
        .parse::<u16>()
        .map_err(|_e| validation_error(format!("'{addr}' has an invalid port '{port_str}'")))?;
    Ok(())
}
