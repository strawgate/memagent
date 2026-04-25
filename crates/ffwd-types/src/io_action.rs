//! Unified I/O action classification.
//!
//! Every I/O operation result is one of three outcomes:
//! - **Retry**: transient failure, try again with backoff
//! - **Reject**: permanent failure, do not retry
//! - **Fatal**: component must shut down
//!
//! This shared vocabulary lets retry engines, fanout aggregation, and metrics
//! all operate on the same classification without each component reinventing
//! the taxonomy.

use core::fmt;
use core::time::Duration;

/// Classification of an I/O operation outcome.
///
/// Used at every stage boundary (receiver → pipeline, pipeline → sink) so
/// that retry engines, fanout aggregation, and health checks share one
/// vocabulary.
#[derive(Debug)]
pub enum IoAction {
    /// Transient failure — retry with backoff.
    ///
    /// `after` is a hint from the server (e.g., `Retry-After` header).
    /// The retry engine may choose a longer delay based on its own policy.
    Retry {
        /// Human-readable reason for the failure.
        reason: String,
        /// Optional delay hint (e.g., from a `Retry-After` header).
        after: Option<Duration>,
    },

    /// Permanent failure — do not retry this operation.
    ///
    /// Examples: 400 Bad Request, schema mismatch, invalid credentials.
    Reject {
        /// Human-readable reason for rejection.
        reason: String,
    },

    /// Fatal failure — the component must shut down.
    ///
    /// Examples: unrecoverable connection loss, panic recovery, resource
    /// exhaustion that can't be remedied by retrying.
    Fatal {
        /// Human-readable reason for the fatal error.
        reason: String,
    },
}

impl IoAction {
    /// Create a transient-retry action with no server-supplied delay hint.
    pub fn retry(reason: impl Into<String>) -> Self {
        IoAction::Retry {
            reason: reason.into(),
            after: None,
        }
    }

    /// Create a transient-retry action with a server-supplied delay hint.
    pub fn retry_after(reason: impl Into<String>, delay: Duration) -> Self {
        IoAction::Retry {
            reason: reason.into(),
            after: Some(delay),
        }
    }

    /// Create a permanent-rejection action.
    pub fn reject(reason: impl Into<String>) -> Self {
        IoAction::Reject {
            reason: reason.into(),
        }
    }

    /// Create a fatal action (component must shut down).
    pub fn fatal(reason: impl Into<String>) -> Self {
        IoAction::Fatal {
            reason: reason.into(),
        }
    }

    /// Returns `true` if this action is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, IoAction::Retry { .. })
    }

    /// Returns `true` if this is a permanent rejection.
    pub fn is_rejected(&self) -> bool {
        matches!(self, IoAction::Reject { .. })
    }

    /// Returns `true` if this is a fatal error.
    pub fn is_fatal(&self) -> bool {
        matches!(self, IoAction::Fatal { .. })
    }

    /// Numeric severity for fanout aggregation: Fatal(3) > Reject(2) > Retry(1).
    ///
    /// When fanning out to multiple sinks, the worst (highest severity) result
    /// determines the aggregate outcome.
    pub fn severity(&self) -> u8 {
        match self {
            IoAction::Retry { .. } => 1,
            IoAction::Reject { .. } => 2,
            IoAction::Fatal { .. } => 3,
        }
    }

    /// Pick the worse of two actions (higher severity wins).
    ///
    /// Used by fanout to aggregate results from multiple sinks.
    /// On a severity tie, the action with the larger `Retry-After` hint wins
    /// so callers respect the most conservative delay.
    #[must_use]
    pub fn worse(self, other: Self) -> Self {
        match other.severity().cmp(&self.severity()) {
            core::cmp::Ordering::Greater => other,
            core::cmp::Ordering::Less => self,
            core::cmp::Ordering::Equal => {
                // Equal severity — prefer the larger Retry-After hint.
                if other.retry_after_hint() > self.retry_after_hint() {
                    other
                } else {
                    self
                }
            }
        }
    }

    /// Returns the `Retry-After` hint, or `None` for non-retry actions.
    pub fn retry_after_hint(&self) -> Option<Duration> {
        match self {
            IoAction::Retry { after, .. } => *after,
            _ => None,
        }
    }
}

impl fmt::Display for IoAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IoAction::Retry { reason, after } => {
                write!(f, "retry: {reason}")?;
                if let Some(d) = after {
                    write!(f, " (after {}ms)", d.as_millis())?;
                }
                Ok(())
            }
            IoAction::Reject { reason } => write!(f, "reject: {reason}"),
            IoAction::Fatal { reason } => write!(f, "fatal: {reason}"),
        }
    }
}

/// Classify an HTTP status code into an [`IoAction`].
///
/// This implements the standard classification shared by all HTTP-based sinks:
///
/// | Status        | Classification          |
/// |---------------|-------------------------|
/// | 2xx           | Success (not an error)  |
/// | 429           | Retry with delay hint   |
/// | 408           | Retry (request timeout) |
/// | 5xx           | Retry (server error)    |
/// | 4xx (other)   | Reject (client error)   |
///
/// **Design note on 401 (Unauthorized):** classified as `Reject` because
/// retrying with the same credentials is futile. Callers that support token
/// refresh should catch 401 before calling this classifier, refresh the
/// token, then retry with new credentials.
///
/// Returns `None` for success (2xx) since success is not an error action.
pub fn classify_http_status(
    status: u16,
    retry_after_header: Option<&str>,
    detail: &str,
) -> Option<IoAction> {
    if (200..300).contains(&status) {
        return None; // success
    }

    let delay = parse_retry_after(retry_after_header);

    if status == 429 {
        let reason = format!("rate limited (HTTP 429): {detail}");
        return Some(match delay {
            Some(d) => IoAction::retry_after(reason, d),
            None => IoAction::retry(reason),
        });
    }

    // 408 Request Timeout — server explicitly says it timed out waiting.
    if status == 408 {
        let reason = format!("request timeout (HTTP 408): {detail}");
        return Some(match delay {
            Some(d) => IoAction::retry_after(reason, d),
            None => IoAction::retry(reason),
        });
    }

    if (500..600).contains(&status) {
        let reason = format!("server error (HTTP {status}): {detail}");
        return Some(match delay {
            Some(d) => IoAction::retry_after(reason, d),
            None => IoAction::retry(reason),
        });
    }

    if (400..500).contains(&status) {
        return Some(IoAction::reject(format!(
            "client error (HTTP {status}): {detail}"
        )));
    }

    // 3xx Redirection — permanent configuration or destination change
    if (300..400).contains(&status) {
        return Some(IoAction::reject(format!(
            "redirect (HTTP {status}): {detail}"
        )));
    }

    // Unexpected status codes (1xx, etc.) — treat as transient
    Some(IoAction::retry(format!(
        "unexpected HTTP {status}: {detail}"
    )))
}

/// Parse a `Retry-After` header value into a [`Duration`].
///
/// Supports integer seconds (RFC 9110 §10.2.3). HTTP-date format is not
/// currently supported — returns `None` for unparsable values.
pub fn parse_retry_after(header_value: Option<&str>) -> Option<Duration> {
    header_value
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_2xx_is_none() {
        assert!(classify_http_status(200, None, "ok").is_none());
        assert!(classify_http_status(204, None, "no content").is_none());
    }

    #[test]
    fn classify_429_is_retry() {
        let action = classify_http_status(429, None, "rate limited").unwrap();
        assert!(action.is_retryable());
        assert_eq!(action.retry_after_hint(), None);
    }

    #[test]
    fn classify_429_with_retry_after() {
        let action = classify_http_status(429, Some("30"), "rate limited").unwrap();
        assert!(action.is_retryable());
        assert_eq!(action.retry_after_hint(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn classify_408_is_retry() {
        let action = classify_http_status(408, None, "request timeout").unwrap();
        assert!(action.is_retryable());
    }

    #[test]
    fn classify_401_is_reject() {
        let action = classify_http_status(401, None, "unauthorized").unwrap();
        assert!(action.is_rejected());
    }

    #[test]
    fn classify_5xx_is_retry() {
        let action = classify_http_status(500, None, "internal error").unwrap();
        assert!(action.is_retryable());
        let action = classify_http_status(503, None, "unavailable").unwrap();
        assert!(action.is_retryable());
    }

    #[test]
    fn classify_503_with_retry_after() {
        let action = classify_http_status(503, Some("10"), "unavailable").unwrap();
        assert!(action.is_retryable());
        assert_eq!(action.retry_after_hint(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn classify_4xx_is_reject() {
        let action = classify_http_status(400, None, "bad request").unwrap();
        assert!(action.is_rejected());
        let action = classify_http_status(404, None, "not found").unwrap();
        assert!(action.is_rejected());
    }

    #[test]
    fn classify_3xx_is_reject() {
        let action = classify_http_status(301, None, "redirect").unwrap();
        assert!(action.is_rejected());
        let action = classify_http_status(302, None, "found").unwrap();
        assert!(action.is_rejected());
    }

    #[test]
    fn severity_ordering() {
        let retry = IoAction::retry("a");
        let reject = IoAction::reject("b");
        let fatal = IoAction::fatal("c");
        assert!(retry.severity() < reject.severity());
        assert!(reject.severity() < fatal.severity());
    }

    #[test]
    fn worse_picks_higher_severity() {
        let retry = IoAction::retry("transient");
        let reject = IoAction::reject("permanent");
        let result = retry.worse(reject);
        assert!(result.is_rejected());
    }

    #[test]
    fn worse_tie_prefers_larger_retry_after() {
        let short = IoAction::retry_after("a", Duration::from_secs(1));
        let long = IoAction::retry_after("b", Duration::from_secs(10));
        let result = short.worse(long);
        assert_eq!(result.retry_after_hint(), Some(Duration::from_secs(10)));

        // Reverse order should still pick the longer delay.
        let short2 = IoAction::retry_after("a", Duration::from_secs(1));
        let long2 = IoAction::retry_after("b", Duration::from_secs(10));
        let result2 = long2.worse(short2);
        assert_eq!(result2.retry_after_hint(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn worse_tie_some_beats_none_retry_after() {
        let no_hint = IoAction::retry("a");
        let with_hint = IoAction::retry_after("b", Duration::from_secs(5));
        let result = no_hint.worse(with_hint);
        assert_eq!(result.retry_after_hint(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn parse_retry_after_integer() {
        assert_eq!(parse_retry_after(Some("5")), Some(Duration::from_secs(5)));
        assert_eq!(
            parse_retry_after(Some(" 30 ")),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn parse_retry_after_none_for_invalid() {
        assert_eq!(parse_retry_after(None), None);
        assert_eq!(parse_retry_after(Some("not-a-number")), None);
        assert_eq!(parse_retry_after(Some("Thu, 01 Dec 2024")), None);
    }
}
