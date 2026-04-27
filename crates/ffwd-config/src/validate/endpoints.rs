//! Endpoint validation helpers.
// xtask-verify: allow(pub_module_needs_tests) // validate_host_port tested via validate/tests.rs

use crate::types::ConfigError;
use std::net::IpAddr;
use url::Url;

use super::common::validation_error;

const REDACTED_URL_USERINFO: &str = "***redacted***";

fn redact_url(endpoint: &str) -> String {
    if let Ok(mut parsed) = Url::parse(endpoint) {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            let _ = parsed.set_username(REDACTED_URL_USERINFO);
            let _ = parsed.set_password(None);
        }
        parsed.to_string()
    } else if let Some(scheme_end) = endpoint.find("://") {
        let after_scheme = &endpoint[scheme_end + 3..];
        if let Some(at) = after_scheme.find('@') {
            format!(
                "{}://{}@{}",
                &endpoint[..scheme_end],
                REDACTED_URL_USERINFO,
                &after_scheme[at + 1..]
            )
        } else {
            endpoint.to_string()
        }
    } else if let Some(at) = endpoint.rfind('@') {
        let authority_start = endpoint.find("://").map_or(0, |idx| idx + 3);
        let prefix = &endpoint[..authority_start];
        let suffix = &endpoint[at + 1..];
        format!("{prefix}{REDACTED_URL_USERINFO}@{suffix}")
    } else {
        endpoint.to_string()
    }
}

pub(super) fn validate_endpoint_url(endpoint: &str) -> Result<(), ConfigError> {
    let safe = redact_url(endpoint);

    let parsed = Url::parse(endpoint)
        .map_err(|_e| validation_error(format!("endpoint '{safe}' is not a valid URL")))?;

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(validation_error(format!(
            "endpoint '{safe}' must not include credentials in the URL; use output.auth instead"
        )));
    }

    let rest = if endpoint
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
    {
        &endpoint[8..]
    } else if endpoint
        .get(..7)
        .is_some_and(|p| p.eq_ignore_ascii_case("http://"))
    {
        &endpoint[7..]
    } else {
        return Err(validation_error(format!(
            "endpoint '{safe}' has no recognised scheme; expected 'http://' or 'https://'"
        )));
    };

    if rest.is_empty() || rest.starts_with('/') || rest.starts_with('?') || rest.starts_with('#') {
        return Err(validation_error(format!(
            "endpoint '{safe}' has no host after the scheme"
        )));
    }

    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(validation_error(format!(
            "endpoint '{safe}' has no host after the scheme"
        )));
    }

    Ok(())
}

pub(super) fn validate_log_level(level: &str) -> Result<(), ConfigError> {
    match level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(validation_error(format!(
            "'{level}' is not a recognised log level; expected one of: trace, debug, info, warn, error"
        ))),
    }
}

pub(super) fn validate_iso8601_timestamp(s: &str) -> Result<(), ConfigError> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return Err(validation_error(format!(
            "must be \"now\" or YYYY-MM-DDTHH:MM:SSZ format, got {s:?}"
        )));
    }
    let digits = |off: usize, n: usize| -> Result<u32, ConfigError> {
        let mut v = 0u32;
        for i in 0..n {
            let c = b[off + i];
            if !c.is_ascii_digit() {
                return Err(validation_error(format!("non-digit character in {s:?}")));
            }
            v = v * 10 + (c - b'0') as u32;
        }
        Ok(v)
    };
    let year = digits(0, 4)? as i32;
    let month = digits(5, 2)?;
    let day = digits(8, 2)?;
    let hour = digits(11, 2)?;
    let min = digits(14, 2)?;
    let sec = digits(17, 2)?;

    if !(1..=12).contains(&month) {
        return Err(validation_error(format!(
            "month {month} out of range 1-12 in {s:?}"
        )));
    }
    if hour > 23 || min > 59 || sec > 59 {
        return Err(validation_error(format!(
            "time component out of range in {s:?}"
        )));
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => unreachable!(),
    };
    if day < 1 || day > max_day {
        return Err(validation_error(format!(
            "day {day} out of range for {year:04}-{month:02} (max {max_day}) in {s:?}"
        )));
    }
    Ok(())
}

pub(super) fn canonical_listen_host_key(host: &str) -> String {
    let bare_host = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    bare_host
        .parse::<IpAddr>()
        .map_or_else(|_| bare_host.to_lowercase(), |addr| addr.to_string())
}
