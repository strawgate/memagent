//! Trace validators for `tla/WorkerPoolDispatch.tla` safety properties.

use std::collections::BTreeSet;

use crate::trace_bridge::{EventValidator, NormalizedTrace};

/// Return all WorkerPoolDispatch validators.
pub fn validators() -> Vec<Box<dyn EventValidator>> {
    vec![
        Box::new(PoolStoppedImpliesNoInFlightValidator),
        Box::new(ForceAbortAccountsForAllValidator),
        Box::new(FailureIsStickyValidator),
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
                    if let Some(id) = entry
                        .attributes
                        .get("batch_id")
                        .and_then(|s| s.parse().ok())
                    {
                        began.insert(id);
                    }
                }
                "batch_terminal" => {
                    if let Some(id) = entry
                        .attributes
                        .get("batch_id")
                        .and_then(|s| s.parse().ok())
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
                    if let Some(id) = entry
                        .attributes
                        .get("batch_id")
                        .and_then(|s| s.parse().ok())
                    {
                        began.insert(id);
                    }
                }
                "batch_terminal" => {
                    if let Some(id) = entry
                        .attributes
                        .get("batch_id")
                        .and_then(|s| s.parse().ok())
                    {
                        terminalized.insert(id);
                    }
                }
                "pool_drain_complete" => {
                    if entry.attributes.get("forced_abort").map(String::as_str) == Some("true") {
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

// ---------------------------------------------------------------------------
// FailureIsStickyValidator — TLA+ WorkerPoolDispatch.tla::FailureIsStickyTemporal
//
// Once a worker produces a Rejected or Panic outcome (indicating Failed health),
// it must not produce Ok outcomes afterward without an intervening worker
// restart (tracked via worker_id changes in the barrier-sourced events).
//
// Only examines sink_result events with worker_id > 0 (barrier-sourced).
// InstrumentedSink self-reports use worker_id=0 and are skipped.
// ---------------------------------------------------------------------------

pub struct FailureIsStickyValidator;

impl EventValidator for FailureIsStickyValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        // Per-worker: has this worker entered Failed state?
        let mut failed_workers: BTreeSet<usize> = BTreeSet::new();

        for entry in &trace.entries {
            if entry.kind != "sink_result" {
                continue;
            }
            let Some(wid_str) = entry.attributes.get("worker_id") else {
                continue;
            };
            let Ok(worker_id) = wid_str.parse::<usize>() else {
                continue;
            };
            if worker_id == 0 {
                continue; // skip InstrumentedSink self-reports
            }

            let outcome = entry
                .attributes
                .get("outcome")
                .map(String::as_str)
                .unwrap_or("");

            match outcome {
                "rejected" | "panic" => {
                    failed_workers.insert(worker_id);
                }
                "ok" if failed_workers.contains(&worker_id) => {
                    return Err(format!(
                        "FailureIsStickyTemporal violated at index {}: \
                         worker {worker_id} produced Ok outcome after being Failed",
                        entry.index
                    ));
                }
                _ => {}
            }
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
            entry(
                1,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
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
        let err = ForceAbortAccountsForAllValidator.validate(&t).unwrap_err();
        assert!(err.contains("ForceAbortAccountsForAll"), "{err}");
    }

    #[test]
    fn failure_sticky_passes_when_no_ok_after_failed() {
        let t = trace(vec![
            entry(
                0,
                "sink_result",
                &[("worker_id", "1"), ("outcome", "ok"), ("rows", "10")],
            ),
            entry(
                1,
                "sink_result",
                &[("worker_id", "1"), ("outcome", "rejected"), ("rows", "10")],
            ),
            // Worker 1 is now Failed — no more Ok events for worker 1
        ]);
        FailureIsStickyValidator.validate(&t).unwrap();
    }

    #[test]
    fn failure_sticky_fails_when_ok_after_rejected() {
        let t = trace(vec![
            entry(
                0,
                "sink_result",
                &[("worker_id", "1"), ("outcome", "rejected"), ("rows", "10")],
            ),
            entry(
                1,
                "sink_result",
                &[("worker_id", "1"), ("outcome", "ok"), ("rows", "10")],
            ),
        ]);
        let err = FailureIsStickyValidator.validate(&t).unwrap_err();
        assert!(err.contains("FailureIsStickyTemporal"), "{err}");
    }

    #[test]
    fn failure_sticky_allows_different_worker_ok_after_other_failed() {
        let t = trace(vec![
            entry(
                0,
                "sink_result",
                &[("worker_id", "1"), ("outcome", "rejected"), ("rows", "10")],
            ),
            // Worker 2 is independent — can still deliver Ok
            entry(
                1,
                "sink_result",
                &[("worker_id", "2"), ("outcome", "ok"), ("rows", "10")],
            ),
        ]);
        FailureIsStickyValidator.validate(&t).unwrap();
    }

    #[test]
    fn failure_sticky_ignores_worker_id_zero() {
        // worker_id=0 means InstrumentedSink self-report — not barrier-sourced
        let t = trace(vec![
            entry(
                0,
                "sink_result",
                &[("worker_id", "0"), ("outcome", "rejected"), ("rows", "10")],
            ),
            entry(
                1,
                "sink_result",
                &[("worker_id", "0"), ("outcome", "ok"), ("rows", "10")],
            ),
        ]);
        FailureIsStickyValidator.validate(&t).unwrap();
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
