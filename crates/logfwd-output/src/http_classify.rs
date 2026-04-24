//! Shared HTTP response classification for all HTTP-based output sinks.
//!
//! Provides [`classify_http_status`] which maps HTTP status codes to
//! [`SendResult`] values, and [`parse_retry_after`]
//! which handles both delta-seconds and HTTP-date `Retry-After` headers
//! (RFC 9110 §10.2.3).

use std::io;
use std::time::Duration;

use super::sink::SendResult;

/// Default retry-after delay when the server provides no `Retry-After` header.
pub const DEFAULT_RETRY_AFTER_SECS: u64 = 5;

/// Parse a `Retry-After` header value (RFC 9110 §10.2.3).
///
/// Supports both:
/// - delta-seconds: `"120"` → `Duration::from_secs(120)`
/// - HTTP-date: `"Wed, 21 Oct 2015 07:28:00 GMT"` → seconds until that time
///
/// Returns `None` if the value is absent, unparsable, or in the past.
pub fn parse_retry_after(header: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    let value = header.and_then(|v| v.to_str().ok())?;
    // delta-seconds first (most common for APIs)
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // HTTP-date fallback
    if let Ok(target) = httpdate::parse_http_date(value.trim()) {
        return target.duration_since(std::time::SystemTime::now()).ok();
    }
    None
}

/// Classify an HTTP response status into a [`SendResult`].
///
/// Standard classification shared by all HTTP-based output sinks:
///
/// | Status      | Result                                                     |
/// |-------------|------------------------------------------------------------|
/// | 2xx         | `None` — success, caller handles metrics                   |
/// | 429         | `RetryAfter` (from header, or default 5s)                  |
/// | 408         | `RetryAfter` (from header, or default 5s)                  |
/// | 4xx (other) | `Rejected` (permanent, do not retry)                       |
/// | 5xx         | `IoError` if no Retry-After, `RetryAfter` if parseable and not in the past |
/// | Other       | `IoError` (transient)                                      |
///
/// # Arguments
/// - `status`: HTTP status code as `u16`
/// - `retry_after_header`: the `Retry-After` header value from the response
/// - `detail`: human-readable error detail (response body snippet, sink name, etc.)
pub fn classify_http_status(
    status: u16,
    retry_after_header: Option<&reqwest::header::HeaderValue>,
    detail: &str,
) -> Option<SendResult> {
    if (200..300).contains(&status) {
        return None;
    }

    // 429 Too Many Requests — always retry
    if status == 429 {
        let delay = parse_retry_after(retry_after_header)
            .unwrap_or(Duration::from_secs(DEFAULT_RETRY_AFTER_SECS));
        return Some(SendResult::RetryAfter(delay));
    }

    // 408 Request Timeout — server says it timed out waiting for us
    if status == 408 {
        let delay = parse_retry_after(retry_after_header)
            .unwrap_or(Duration::from_secs(DEFAULT_RETRY_AFTER_SECS));
        return Some(SendResult::RetryAfter(delay));
    }

    // 5xx Server Error — retry; honour Retry-After if present
    if (500..600).contains(&status) {
        if let Some(delay) = parse_retry_after(retry_after_header) {
            return Some(SendResult::RetryAfter(delay));
        }
        return Some(SendResult::IoError(io::Error::other(format!(
            "server error (HTTP {status}): {detail}"
        ))));
    }

    // 4xx Client Error (not 429/408) — permanent rejection
    if (400..500).contains(&status) {
        return Some(SendResult::Rejected(format!("HTTP {status}: {detail}")));
    }

    // 3xx Redirection — permanent configuration or destination change
    if (300..400).contains(&status) {
        return Some(SendResult::Rejected(format!("HTTP {status}: {detail}")));
    }

    // 1xx, etc. — treat as transient
    Some(SendResult::IoError(io::Error::other(format!(
        "unexpected HTTP {status}: {detail}"
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_returns_none() {
        assert!(classify_http_status(200, None, "ok").is_none());
        assert!(classify_http_status(204, None, "no content").is_none());
    }

    #[test]
    fn rate_limited_returns_retry_after() {
        let result = classify_http_status(429, None, "rate limited");
        assert!(
            matches!(result, Some(SendResult::RetryAfter(d)) if d == Duration::from_secs(DEFAULT_RETRY_AFTER_SECS)),
            "expected RetryAfter({DEFAULT_RETRY_AFTER_SECS}s), got: {result:?}"
        );
    }

    #[test]
    fn rate_limited_with_header() {
        let hv = reqwest::header::HeaderValue::from_static("30");
        let result = classify_http_status(429, Some(&hv), "rate limited");
        assert!(
            matches!(result, Some(SendResult::RetryAfter(d)) if d == Duration::from_secs(30)),
            "expected RetryAfter(30s), got: {result:?}"
        );
    }

    #[test]
    fn request_timeout_returns_retry_after() {
        let result = classify_http_status(408, None, "timeout");
        assert!(
            matches!(result, Some(SendResult::RetryAfter(d)) if d == Duration::from_secs(DEFAULT_RETRY_AFTER_SECS)),
            "expected RetryAfter({DEFAULT_RETRY_AFTER_SECS}s), got: {result:?}"
        );
    }

    #[test]
    fn server_error_without_header_is_io_error() {
        let result = classify_http_status(500, None, "internal");
        assert!(
            matches!(result, Some(SendResult::IoError(_))),
            "expected IoError, got: {result:?}"
        );
    }

    #[test]
    fn server_error_with_retry_after() {
        let hv = reqwest::header::HeaderValue::from_static("10");
        let result = classify_http_status(503, Some(&hv), "unavailable");
        assert!(
            matches!(result, Some(SendResult::RetryAfter(d)) if d == Duration::from_secs(10)),
            "expected RetryAfter(10s), got: {result:?}"
        );
    }

    #[test]
    fn client_error_is_rejected() {
        let result = classify_http_status(400, None, "bad request");
        assert!(
            matches!(result, Some(SendResult::Rejected(ref r)) if r.contains("400")),
            "expected Rejected containing '400', got: {result:?}"
        );
    }

    #[test]
    fn auth_error_is_rejected() {
        let result = classify_http_status(401, None, "unauthorized");
        assert!(
            matches!(result, Some(SendResult::Rejected(ref r)) if r.contains("401")),
            "expected Rejected containing '401', got: {result:?}"
        );
    }

    #[test]
    fn redirect_is_rejected() {
        let result = classify_http_status(301, None, "redirect");
        assert!(
            matches!(result, Some(SendResult::Rejected(ref r)) if r.contains("301")),
            "expected Rejected containing '301', got: {result:?}"
        );
    }

    #[test]
    fn unexpected_status_is_io_error() {
        let result = classify_http_status(100, None, "continue");
        assert!(
            matches!(result, Some(SendResult::IoError(_))),
            "expected IoError, got: {result:?}"
        );
    }

    #[test]
    fn parse_retry_after_integer() {
        let hv = reqwest::header::HeaderValue::from_static("120");
        assert_eq!(parse_retry_after(Some(&hv)), Some(Duration::from_secs(120)));
    }

    #[test]
    fn parse_retry_after_none() {
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn parse_retry_after_invalid() {
        let hv = reqwest::header::HeaderValue::from_static("not-a-number");
        assert_eq!(parse_retry_after(Some(&hv)), None);
    }

    #[test]
    fn parse_retry_after_http_date_with_whitespace() {
        let hv = reqwest::header::HeaderValue::from_static(" Wed, 21 Oct 2099 07:28:00 GMT ");
        let parsed = parse_retry_after(Some(&hv));
        assert!(
            parsed.is_some(),
            "expected HTTP-date Retry-After with surrounding whitespace to parse"
        );
    }
}
