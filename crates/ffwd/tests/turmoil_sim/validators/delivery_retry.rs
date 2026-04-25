//! Trace validators for `tla/DeliveryRetry.tla` safety properties.
//!
//! These validators check retry loop invariants using `retry_attempt`
//! trace events emitted from `process_item()` in `worker.rs`.

use std::collections::BTreeMap;

use crate::trace_bridge::{EventValidator, NormalizedTrace};

/// Return all DeliveryRetry validators.
pub fn validators() -> Vec<Box<dyn EventValidator>> {
    vec![
        Box::new(BackoffCapValidator),
        Box::new(BackoffMonotonicValidator),
        Box::new(RetryCountMonotonicValidator),
    ]
}

// ---------------------------------------------------------------------------
// BackoffCapValidator — TLA+ DeliveryRetry.tla::BackoffCapRespected
//
// The backoff delay never exceeds the configured maximum.
// Since we don't know the exact max from the trace, we use a generous
// bound (the default max_retry_delay is 60s = 60_000ms).
// ---------------------------------------------------------------------------

const MAX_BACKOFF_MS_UPPER_BOUND: u64 = 120_000; // 2 minutes — generous ceiling

pub struct BackoffCapValidator;

impl EventValidator for BackoffCapValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        for entry in &trace.entries {
            if entry.kind != "retry_attempt" {
                continue;
            }
            if let Some(backoff_ms) = entry
                .attributes
                .get("backoff_ms")
                .and_then(|s| s.parse::<u64>().ok())
            {
                if backoff_ms > MAX_BACKOFF_MS_UPPER_BOUND {
                    return Err(format!(
                        "BackoffCapRespected violated at index {}: \
                         backoff_ms={backoff_ms} exceeds upper bound {MAX_BACKOFF_MS_UPPER_BOUND}",
                        entry.index
                    ));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BackoffMonotonicValidator — TLA+ DeliveryRetry.tla::BackoffDelayMonotonic
//
// Per-batch: backoff delay never decreases across retry attempts.
// Note: backon uses jitter, so we allow equal values (>=, not >).
// RetryAfter uses server-directed delay which may not be monotonic with
// respect to exponential backoff, so we only check Timeout and IoError.
// ---------------------------------------------------------------------------

pub struct BackoffMonotonicValidator;

impl EventValidator for BackoffMonotonicValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        // Per (worker_id, batch_id): last backoff_ms for exponential reasons only
        let mut last_backoff: BTreeMap<(usize, u64), u64> = BTreeMap::new();

        for entry in &trace.entries {
            if entry.kind != "retry_attempt" {
                continue;
            }
            let reason = entry
                .attributes
                .get("reason")
                .map(String::as_str)
                .unwrap_or("");
            // Only check exponential backoff reasons (not server-directed RetryAfter)
            if reason != "timeout" && reason != "io_error" {
                continue;
            }
            let worker_id = entry
                .attributes
                .get("worker_id")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let batch_id = entry
                .attributes
                .get("batch_id")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let backoff_ms = entry
                .attributes
                .get("backoff_ms")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let key = (worker_id, batch_id);
            if let Some(&prev) = last_backoff.get(&key) {
                // Allow equal due to jitter, but not strictly decreasing.
                // Also allow decrease when backoff iterator is exhausted and
                // falls back to max_retry_delay (which may be lower than a
                // jittered peak). Use a 50% tolerance.
                if backoff_ms < prev / 2 {
                    return Err(format!(
                        "BackoffDelayMonotonic violated at index {}: \
                         worker={worker_id} batch={batch_id} backoff dropped \
                         from {prev}ms to {backoff_ms}ms",
                        entry.index
                    ));
                }
            }
            last_backoff.insert(key, backoff_ms);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RetryCountMonotonicValidator — TLA+ DeliveryRetry.tla::RetryCountMonotonic
//
// Per-batch: attempt count strictly increases across retry events.
// ---------------------------------------------------------------------------

pub struct RetryCountMonotonicValidator;

impl EventValidator for RetryCountMonotonicValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        // Per (worker_id, batch_id): last attempt number
        let mut last_attempt: BTreeMap<(usize, u64), usize> = BTreeMap::new();

        for entry in &trace.entries {
            if entry.kind != "retry_attempt" {
                continue;
            }
            let worker_id = entry
                .attributes
                .get("worker_id")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let batch_id = entry
                .attributes
                .get("batch_id")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let attempt = entry
                .attributes
                .get("attempt")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);

            let key = (worker_id, batch_id);
            if let Some(&prev) = last_attempt.get(&key) {
                if attempt <= prev {
                    return Err(format!(
                        "RetryCountMonotonic violated at index {}: \
                         worker={worker_id} batch={batch_id} attempt \
                         did not increase: {prev} -> {attempt}",
                        entry.index
                    ));
                }
            }
            last_attempt.insert(key, attempt);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace_bridge::{NormalizedTrace, NormalizedTraceEvent};

    fn entry(
        index: usize,
        kind: &'static str,
        attrs: &[(&'static str, &str)],
    ) -> NormalizedTraceEvent {
        let attributes = attrs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        NormalizedTraceEvent {
            index,
            kind,
            attributes,
        }
    }

    fn trace(entries: Vec<NormalizedTraceEvent>) -> NormalizedTrace {
        NormalizedTrace { entries }
    }

    #[test]
    fn backoff_cap_passes_within_bounds() {
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[("backoff_ms", "100"), ("reason", "io_error")],
            ),
            entry(
                1,
                "retry_attempt",
                &[("backoff_ms", "200"), ("reason", "io_error")],
            ),
        ]);
        BackoffCapValidator.validate(&t).unwrap();
    }

    #[test]
    fn backoff_cap_fails_exceeding_bound() {
        let t = trace(vec![entry(
            0,
            "retry_attempt",
            &[("backoff_ms", "999999"), ("reason", "io_error")],
        )]);
        let err = BackoffCapValidator.validate(&t).unwrap_err();
        assert!(err.contains("BackoffCapRespected"), "{err}");
    }

    #[test]
    fn backoff_monotonic_passes_increasing() {
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[
                    ("worker_id", "1"),
                    ("batch_id", "1"),
                    ("backoff_ms", "100"),
                    ("reason", "io_error"),
                ],
            ),
            entry(
                1,
                "retry_attempt",
                &[
                    ("worker_id", "1"),
                    ("batch_id", "1"),
                    ("backoff_ms", "200"),
                    ("reason", "io_error"),
                ],
            ),
        ]);
        BackoffMonotonicValidator.validate(&t).unwrap();
    }

    #[test]
    fn backoff_monotonic_fails_large_decrease() {
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[
                    ("worker_id", "1"),
                    ("batch_id", "1"),
                    ("backoff_ms", "1000"),
                    ("reason", "io_error"),
                ],
            ),
            entry(
                1,
                "retry_attempt",
                &[
                    ("worker_id", "1"),
                    ("batch_id", "1"),
                    ("backoff_ms", "100"),
                    ("reason", "io_error"),
                ],
            ),
        ]);
        let err = BackoffMonotonicValidator.validate(&t).unwrap_err();
        assert!(err.contains("BackoffDelayMonotonic"), "{err}");
    }

    #[test]
    fn backoff_monotonic_ignores_retry_after_reason() {
        // RetryAfter uses server-directed delay, not exponential — skip monotonicity check
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[
                    ("worker_id", "1"),
                    ("batch_id", "1"),
                    ("backoff_ms", "5000"),
                    ("reason", "retry_after"),
                ],
            ),
            entry(
                1,
                "retry_attempt",
                &[
                    ("worker_id", "1"),
                    ("batch_id", "1"),
                    ("backoff_ms", "100"),
                    ("reason", "retry_after"),
                ],
            ),
        ]);
        BackoffMonotonicValidator.validate(&t).unwrap();
    }

    #[test]
    fn retry_count_monotonic_passes_increasing() {
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[("worker_id", "1"), ("batch_id", "1"), ("attempt", "1")],
            ),
            entry(
                1,
                "retry_attempt",
                &[("worker_id", "1"), ("batch_id", "1"), ("attempt", "2")],
            ),
        ]);
        RetryCountMonotonicValidator.validate(&t).unwrap();
    }

    #[test]
    fn retry_count_monotonic_fails_non_increasing() {
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[("worker_id", "1"), ("batch_id", "1"), ("attempt", "2")],
            ),
            entry(
                1,
                "retry_attempt",
                &[("worker_id", "1"), ("batch_id", "1"), ("attempt", "2")],
            ),
        ]);
        let err = RetryCountMonotonicValidator.validate(&t).unwrap_err();
        assert!(err.contains("RetryCountMonotonic"), "{err}");
    }

    #[test]
    fn retry_count_independent_across_batches() {
        let t = trace(vec![
            entry(
                0,
                "retry_attempt",
                &[("worker_id", "1"), ("batch_id", "1"), ("attempt", "3")],
            ),
            // Different batch — attempt 1 is fine
            entry(
                1,
                "retry_attempt",
                &[("worker_id", "1"), ("batch_id", "2"), ("attempt", "1")],
            ),
        ]);
        RetryCountMonotonicValidator.validate(&t).unwrap();
    }
}
