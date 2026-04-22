//! Trace validators for `tla/WorkerPoolDispatch.tla` safety properties.

use std::collections::BTreeSet;

use crate::trace_bridge::{EventValidator, NormalizedTrace};

/// Return all WorkerPoolDispatch validators.
pub fn validators() -> Vec<Box<dyn EventValidator>> {
    vec![
        Box::new(PoolStoppedImpliesNoInFlightValidator),
        Box::new(ForceAbortAccountsForAllValidator),
    ]
}

// ---------------------------------------------------------------------------
// PoolStoppedImpliesNoInFlight — TLA+ WorkerPoolDispatch.tla::StoppedImpliesNoInFlight
//
// After pool_drain_complete, no dispatched batch is missing a terminal.
// ---------------------------------------------------------------------------

pub struct PoolStoppedImpliesNoInFlightValidator;

impl EventValidator for PoolStoppedImpliesNoInFlightValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut began: BTreeSet<u64> = BTreeSet::new();
        let mut terminalized: BTreeSet<u64> = BTreeSet::new();
        let mut pool_drained = false;

        for entry in &trace.entries {
            match entry.kind {
                "batch_begin" => {
                    if let Some(id) = entry.attributes.get("batch_id").and_then(|s| s.parse().ok())
                    {
                        began.insert(id);
                    }
                }
                "batch_terminal" => {
                    if let Some(id) = entry.attributes.get("batch_id").and_then(|s| s.parse().ok())
                    {
                        terminalized.insert(id);
                    }
                }
                "pool_drain_complete" => {
                    pool_drained = true;
                }
                _ => {}
            }
        }

        if !pool_drained {
            return Ok(());
        }

        let in_flight: BTreeSet<_> = began.difference(&terminalized).collect();
        if !in_flight.is_empty() {
            return Err(format!(
                "StoppedImpliesNoInFlight violated: pool drained but {} batch(es) \
                 still in-flight: {:?}",
                in_flight.len(),
                in_flight
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ForceAbortAccountsForAll — TLA+ WorkerPoolDispatch.tla::ForceAbortAccountsForAll
//
// When pool_drain_complete fires with forced_abort=true, all pending and
// in-flight batches must be accounted for (delivered, rejected, or abandoned).
// ---------------------------------------------------------------------------

pub struct ForceAbortAccountsForAllValidator;

impl EventValidator for ForceAbortAccountsForAllValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut began: BTreeSet<u64> = BTreeSet::new();
        let mut terminalized: BTreeSet<u64> = BTreeSet::new();
        let mut forced_abort = false;

        for entry in &trace.entries {
            match entry.kind {
                "batch_begin" => {
                    if let Some(id) = entry.attributes.get("batch_id").and_then(|s| s.parse().ok())
                    {
                        began.insert(id);
                    }
                }
                "batch_terminal" => {
                    if let Some(id) = entry.attributes.get("batch_id").and_then(|s| s.parse().ok())
                    {
                        terminalized.insert(id);
                    }
                }
                "pool_drain_complete" => {
                    if entry
                        .attributes
                        .get("forced_abort")
                        .map(String::as_str)
                        == Some("true")
                    {
                        forced_abort = true;
                    }
                }
                _ => {}
            }
        }

        if !forced_abort {
            return Ok(());
        }

        let unaccounted: BTreeSet<_> = began.difference(&terminalized).collect();
        if !unaccounted.is_empty() {
            return Err(format!(
                "ForceAbortAccountsForAll violated: forced abort but {} batch(es) \
                 not accounted for: {:?}",
                unaccounted.len(),
                unaccounted
            ));
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
    fn pool_stopped_passes_when_all_batches_terminalized() {
        let t = trace(vec![
            entry(0, "batch_begin", &[("batch_id", "1")]),
            entry(1, "batch_terminal", &[("batch_id", "1"), ("terminal", "acked")]),
            entry(2, "pool_drain_complete", &[("forced_abort", "false")]),
        ]);
        PoolStoppedImpliesNoInFlightValidator.validate(&t).unwrap();
    }

    #[test]
    fn pool_stopped_fails_when_batch_missing_terminal() {
        let t = trace(vec![
            entry(0, "batch_begin", &[("batch_id", "1")]),
            entry(1, "pool_drain_complete", &[("forced_abort", "false")]),
        ]);
        let err = PoolStoppedImpliesNoInFlightValidator
            .validate(&t)
            .unwrap_err();
        assert!(err.contains("StoppedImpliesNoInFlight"), "{err}");
    }

    #[test]
    fn force_abort_passes_when_all_accounted() {
        let t = trace(vec![
            entry(0, "batch_begin", &[("batch_id", "1")]),
            entry(
                1,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "abandoned")],
            ),
            entry(2, "pool_drain_complete", &[("forced_abort", "true")]),
        ]);
        ForceAbortAccountsForAllValidator.validate(&t).unwrap();
    }

    #[test]
    fn force_abort_fails_when_batch_unaccounted() {
        let t = trace(vec![
            entry(0, "batch_begin", &[("batch_id", "1")]),
            entry(1, "pool_drain_complete", &[("forced_abort", "true")]),
        ]);
        let err = ForceAbortAccountsForAllValidator
            .validate(&t)
            .unwrap_err();
        assert!(err.contains("ForceAbortAccountsForAll"), "{err}");
    }

    #[test]
    fn force_abort_skips_when_not_forced() {
        let t = trace(vec![
            entry(0, "batch_begin", &[("batch_id", "1")]),
            entry(1, "pool_drain_complete", &[("forced_abort", "false")]),
        ]);
        // Graceful drain — ForceAbort property is vacuously true.
        ForceAbortAccountsForAllValidator.validate(&t).unwrap();
    }
}
