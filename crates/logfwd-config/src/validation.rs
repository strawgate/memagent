//! Validation helpers for configuration values.
//!
//! Contains pure validation functions for host:port addresses, bind addresses,
//! endpoint URLs, log levels, and the output-endpoint HTTPS policy.

// ---------------------------------------------------------------------------
// Bind address / host:port validation
// ---------------------------------------------------------------------------

/// Validate that a bind address is a parseable `host:port` socket address.
pub(crate) fn validate_bind_addr(addr: &str) -> Result<(), String> {
    validate_host_port(addr)
}

/// Validate that a string has a valid `host:port` format where port is a u16.
///
/// Accepts IP addresses (v4 and v6) as well as hostnames, consistent with the
/// runtime `TcpListener::bind` behaviour.  Use this function anywhere an
/// address is validated so that CLI and config validation remain in sync.
pub fn validate_host_port(addr: &str) -> Result<(), String> {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        return Err(format!("'{addr}' is a URL, expected host:port"));
    }

    let (host, port_str) = if addr.starts_with('[') {
        // Use find (first ']') not rfind (last ']') so that inputs like
        // "[::1]]:4317" are rejected rather than treating "[::1]]" as the host.
        let close_bracket = addr
            .find(']')
            .ok_or_else(|| format!("'{addr}' has mismatched brackets"))?;
        let inner = &addr[1..close_bracket];
        if inner.is_empty() {
            return Err(format!(
                "'{addr}' has an empty IPv6 address inside brackets"
            ));
        }
        inner
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| format!("'{addr}' contains a non-IPv6 value inside brackets"))?;
        if !addr[close_bracket..].starts_with("]:") {
            return Err(format!("'{addr}' is missing a port after IPv6 brackets"));
        }
        let port_str = &addr[close_bracket + 2..];
        (&addr[..=close_bracket], port_str)
    } else {
        addr.rsplit_once(':')
            .ok_or_else(|| format!("'{addr}' is missing a port (expected format host:port)"))?
    };

    if host.is_empty() {
        return Err(format!("'{addr}' has an empty host"));
    }

    // Reject path-like hosts (e.g. "host/path:80") — these are likely
    // malformed URLs rather than intentional host:port values. (#1461)
    if host.contains('/') {
        return Err(format!(
            "'{addr}' host contains a '/' (expected host:port, not a URL path)"
        ));
    }

    // Reject unmatched closing bracket outside of IPv6 brackets (e.g. "host]:80").
    if !addr.starts_with('[') && host.contains(']') {
        return Err(format!("'{addr}' has an unmatched ']' in the host"));
    }

    if !addr.starts_with('[') && host.contains(':') {
        return Err(format!(
            "'{addr}' has multiple colons without IPv6 brackets"
        ));
    }

    port_str
        .parse::<u16>()
        .map_err(|_| format!("'{addr}' has an invalid port '{port_str}'"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Log level validation
// ---------------------------------------------------------------------------

/// Validate that a log level string is a recognised tracing level.
///
/// Accepted values (case-insensitive): `trace`, `debug`, `info`, `warn`, `error`.
pub(crate) fn validate_log_level(level: &str) -> Result<(), String> {
    match level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(format!(
            "'{level}' is not a recognised log level; expected one of: trace, debug, info, warn, error"
        )),
    }
}

// ---------------------------------------------------------------------------
// Endpoint URL validation
// ---------------------------------------------------------------------------

/// Validate that an endpoint URL has a recognised scheme and a non-empty host.
///
/// Accepts `http://` or `https://` followed by at least one character.
/// Returns the parsed [`url::Url`] on success so callers can inspect it
/// without re-parsing.
fn validate_endpoint_url(endpoint: &str) -> Result<url::Url, String> {
    let url = endpoint
        .parse::<url::Url>()
        .map_err(|e| format!("endpoint '{endpoint}' is not a valid URL: {e}"))?;

    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(format!(
                "endpoint '{endpoint}' has unsupported scheme '{other}'; expected 'http' or 'https'"
            ));
        }
    }

    if url.host_str().is_none() {
        return Err(format!(
            "endpoint '{endpoint}' has no host (expected e.g. https://collector:4318)"
        ));
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(
            "endpoint must not include credentials in the URL; use output.auth instead".to_string(),
        );
    }

    Ok(url)
}

fn is_loopback_url(url: &url::Url) -> bool {
    let Some(host) = url.host() else {
        return false;
    };
    match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(ip) => ip.is_loopback(),
        url::Host::Ipv6(ip) => ip.is_loopback(),
    }
}

/// Output endpoint validation policy:
/// - URL must be parseable and use http/https.
/// - URL must not include embedded credentials.
/// - Non-loopback endpoints must use HTTPS.
pub fn validate_output_endpoint_url(endpoint: &str) -> Result<(), String> {
    let url = validate_endpoint_url(endpoint)?;
    if url.scheme() != "https" && !is_loopback_url(&url) {
        return Err(format!(
            "endpoint '{endpoint}' uses insecure HTTP; use 'https://' for non-loopback output endpoints"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_host_port_works() {
        assert!(validate_host_port("127.0.0.1:4317").is_ok());
        assert!(validate_host_port("localhost:4317").is_ok());
        assert!(validate_host_port("my-host.internal:8080").is_ok());
        assert!(validate_host_port("[::1]:4317").is_ok());
        assert!(validate_host_port("[2001:db8::1]:80").is_ok());

        assert!(
            validate_host_port(":4317")
                .unwrap_err()
                .contains("empty host")
        );
        assert!(
            validate_host_port("http://localhost:4317")
                .unwrap_err()
                .contains("URL")
        );
        assert!(
            validate_host_port("https://localhost:4317")
                .unwrap_err()
                .contains("URL")
        );
        assert!(
            validate_host_port("foo:bar:4317")
                .unwrap_err()
                .contains("multiple colons")
        );
        assert!(
            validate_host_port("localhost")
                .unwrap_err()
                .contains("missing a port")
        );
        assert!(
            validate_host_port("localhost:")
                .unwrap_err()
                .contains("invalid port")
        );
        assert!(
            validate_host_port("localhost:999999")
                .unwrap_err()
                .contains("invalid port")
        );
        assert!(
            validate_host_port("[::1]")
                .unwrap_err()
                .contains("missing a port")
        );
        assert!(
            validate_host_port("[::1]:")
                .unwrap_err()
                .contains("invalid port")
        );
        // Empty IPv6 brackets — []:8080 has no host
        assert!(validate_host_port("[]:8080").unwrap_err().contains("empty"));
        // Double closing bracket — [::1]]:4317 is malformed
        assert!(
            validate_host_port("[::1]]:4317")
                .unwrap_err()
                .contains("missing a port")
        );
        // Path-like host rejected (#1461)
        assert!(
            validate_host_port("foo/bar:4317")
                .unwrap_err()
                .contains("/")
        );
        // Unmatched closing bracket rejected (#1461)
        assert!(validate_host_port("foo]:4317").unwrap_err().contains("]"));
    }

    #[test]
    fn validate_bind_addr_works() {
        assert!(validate_bind_addr("127.0.0.1:4317").is_ok());
        assert!(validate_bind_addr("localhost:4317").is_ok());
        assert!(validate_bind_addr("[::1]:4317").is_ok());
        assert!(validate_bind_addr("http://localhost:4317").is_err());
    }
}
