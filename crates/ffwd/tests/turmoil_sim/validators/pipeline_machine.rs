//! Trace validators for `tla/PipelineMachine.tla` safety properties.
//!
//! Each struct implements [`EventValidator`] and checks a specific TLA+
//! invariant or temporal property against a [`NormalizedTrace`] produced
//! by the turmoil trace bridge.

use std::collections::{BTreeMap, BTreeSet};

use crate::trace_bridge::{EventValidator, NormalizedTrace};

/// Return all PipelineMachine validators.
pub fn validators() -> Vec<Box<dyn EventValidator>> {
    vec![
        Box::new(NoDoubleCompleteValidator),
        Box::new(DrainCompletenessValidator),
        Box::new(NoCreateAfterDrainValidator),
        Box::new(CheckpointOrderingValidator),
        Box::new(CommittedMonotonicValidator),
    ]
}

// ---------------------------------------------------------------------------
// NoDoubleComplete — TLA+ PipelineMachine.tla::NoDoubleComplete
//
// No batch appears in two different terminal sets simultaneously.
// In trace terms: no batch_id has two batch_terminal events with
// different terminal states.
// ---------------------------------------------------------------------------

pub struct NoDoubleCompleteValidator;

impl EventValidator for NoDoubleCompleteValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        // batch_id → first terminal state observed
        let mut terminals: BTreeMap<u64, &str> = BTreeMap::new();

        for entry in &trace.entries {
            if entry.kind != "batch_terminal" {
                continue;
            }
            let Some(batch_id_str) = entry.attributes.get("batch_id") else {
                continue;
            };
            let Ok(batch_id) = batch_id_str.parse::<u64>() else {
                continue;
            };
            let Some(terminal) = entry.attributes.get("terminal") else {
                continue;
            };

            if let Some(prev) = terminals.get(&batch_id) {
                if *prev != terminal.as_str() {
                    return Err(format!(
                        "NoDoubleComplete violated at index {}: batch {batch_id} \
                         terminalized as '{terminal}' but was already '{prev}'",
                        entry.index
                    ));
                }
            } else {
                terminals.insert(batch_id, terminal.as_str());
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DrainCompleteness — TLA+ PipelineMachine.tla::DrainCompleteness
//                     + QuiescenceHasNoSilentStrandedWork
//
// When the pipeline reaches Stopped, every batch that had a batch_begin
// must also have a batch_terminal (acked, rejected, or abandoned).
// No batch is silently lost.
// ---------------------------------------------------------------------------

pub struct DrainCompletenessValidator;

impl EventValidator for DrainCompletenessValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut began: BTreeSet<u64> = BTreeSet::new();
        let mut terminalized: BTreeSet<u64> = BTreeSet::new();
        let mut saw_stopped = false;

        for entry in &trace.entries {
            match entry.kind {
                "batch_begin" => {
                    if let Some(id) = entry.attributes.get("batch_id") {
                        if let Ok(id) = id.parse::<u64>() {
                            began.insert(id);
                        }
                    }
                }
                "batch_terminal" => {
                    if let Some(id) = entry.attributes.get("batch_id") {
                        if let Ok(id) = id.parse::<u64>() {
                            terminalized.insert(id);
                        }
                    }
                }
                "phase" => {
                    if entry.attributes.get("phase").map(String::as_str) == Some("stopped") {
                        saw_stopped = true;
                    }
                }
                _ => {}
            }
        }

        if !saw_stopped {
            // If the pipeline didn't reach Stopped, this property is vacuously true.
            return Ok(());
        }

        let missing: BTreeSet<_> = began.difference(&terminalized).collect();
        if !missing.is_empty() {
            return Err(format!(
                "DrainCompleteness violated: {} batch(es) began but never terminalized: {:?}",
                missing.len(),
                missing
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// NoCreateAfterDrain — TLA+ PipelineMachine.tla::NoCreateAfterDrain
//
// After the pipeline enters Draining phase, no new batches may be created.
// ---------------------------------------------------------------------------

pub struct NoCreateAfterDrainValidator;

impl EventValidator for NoCreateAfterDrainValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut draining = false;

        for entry in &trace.entries {
            match entry.kind {
                "phase" => {
                    if entry.attributes.get("phase").map(String::as_str) == Some("draining") {
                        draining = true;
                    }
                }
                "batch_begin" if draining => {
                    let batch_id = entry
                        .attributes
                        .get("batch_id")
                        .map(String::as_str)
                        .unwrap_or("?");
                    return Err(format!(
                        "NoCreateAfterDrain violated at index {}: \
                         batch_begin for batch {batch_id} after draining phase",
                        entry.index
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CheckpointOrderingInvariant — TLA+ PipelineMachine.tla::CheckpointOrderingInvariant
//
// When committed[s] = n, all batches for source s with checkpoint <= n
// must have reached a terminal state (acked or rejected).
// ---------------------------------------------------------------------------

pub struct CheckpointOrderingValidator;

impl EventValidator for CheckpointOrderingValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        // Per-source: batch_id → checkpoint offset from batch_begin
        let mut source_batches: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
        // Per-batch: terminal state (if any)
        let mut terminalized: BTreeSet<u64> = BTreeSet::new();
        // Per-source: latest committed offset
        let mut committed: BTreeMap<u64, u64> = BTreeMap::new();

        for entry in &trace.entries {
            match entry.kind {
                "batch_begin" => {
                    if let (Some(bid), Some(sid), Some(ckpt)) = (
                        entry
                            .attributes
                            .get("batch_id")
                            .and_then(|s| s.parse::<u64>().ok()),
                        entry
                            .attributes
                            .get("source_id")
                            .and_then(|s| s.parse::<u64>().ok()),
                        entry
                            .attributes
                            .get("checkpoint")
                            .and_then(|s| s.parse::<u64>().ok()),
                    ) {
                        source_batches.entry(sid).or_default().push((bid, ckpt));
                    }
                }
                "batch_terminal" => {
                    if let Some(bid) = entry
                        .attributes
                        .get("batch_id")
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        terminalized.insert(bid);
                    }
                }
                "checkpoint_update" => {
                    if let (Some(sid), Some(offset)) = (
                        entry
                            .attributes
                            .get("source_id")
                            .and_then(|s| s.parse::<u64>().ok()),
                        entry
                            .attributes
                            .get("offset")
                            .and_then(|s| s.parse::<u64>().ok()),
                    ) {
                        committed.insert(sid, offset);

                        // Check invariant: all batches for this source with
                        // checkpoint <= offset must be terminalized.
                        if let Some(batches) = source_batches.get(&sid) {
                            for &(bid, ckpt) in batches {
                                if ckpt <= offset && !terminalized.contains(&bid) {
                                    return Err(format!(
                                        "CheckpointOrderingInvariant violated at index {}: \
                                         source {sid} committed to {offset} but batch {bid} \
                                         (checkpoint {ckpt}) is not terminalized",
                                        entry.index
                                    ));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CommittedMonotonic — TLA+ PipelineMachine.tla::CommittedMonotonic
//
// Per-source committed checkpoint never decreases.
// (This was partially in TransitionValidator; extracted here as a
// pluggable EventValidator for composability.)
// ---------------------------------------------------------------------------

pub struct CommittedMonotonicValidator;

impl EventValidator for CommittedMonotonicValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut last: BTreeMap<u64, u64> = BTreeMap::new();

        for entry in &trace.entries {
            if entry.kind != "checkpoint_update" {
                continue;
            }
            if let (Some(sid), Some(offset)) = (
                entry
                    .attributes
                    .get("source_id")
                    .and_then(|s| s.parse::<u64>().ok()),
                entry
                    .attributes
                    .get("offset")
                    .and_then(|s| s.parse::<u64>().ok()),
            ) {
                let prev = last.get(&sid).copied().unwrap_or(0);
                if offset < prev {
                    return Err(format!(
                        "CommittedMonotonic violated at index {}: \
                         source {sid} checkpoint regressed from {prev} to {offset}",
                        entry.index
                    ));
                }
                last.insert(sid, offset);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace_bridge::NormalizedTraceEvent;

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
    fn no_double_complete_passes_on_unique_terminals() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(
                1,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
            entry(
                2,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
            entry(3, "phase", &[("phase", "draining")]),
            entry(4, "phase", &[("phase", "stopped")]),
        ]);
        NoDoubleCompleteValidator.validate(&t).unwrap();
    }

    #[test]
    fn no_double_complete_rejects_conflicting_terminals() {
        let t = trace(vec![
            entry(
                0,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
            entry(
                1,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "rejected")],
            ),
        ]);
        let err = NoDoubleCompleteValidator.validate(&t).unwrap_err();
        assert!(err.contains("NoDoubleComplete"), "{err}");
    }

    #[test]
    fn drain_completeness_passes_when_all_batches_terminalized() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(
                1,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
            entry(
                2,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
            entry(3, "phase", &[("phase", "draining")]),
            entry(4, "phase", &[("phase", "stopped")]),
        ]);
        DrainCompletenessValidator.validate(&t).unwrap();
    }

    #[test]
    fn drain_completeness_fails_when_batch_not_terminalized() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(
                1,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
            entry(2, "phase", &[("phase", "draining")]),
            entry(3, "phase", &[("phase", "stopped")]),
        ]);
        let err = DrainCompletenessValidator.validate(&t).unwrap_err();
        assert!(err.contains("DrainCompleteness"), "{err}");
    }

    #[test]
    fn no_create_after_drain_passes_when_no_batch_after_draining() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(
                1,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
            entry(2, "phase", &[("phase", "draining")]),
            entry(3, "phase", &[("phase", "stopped")]),
        ]);
        NoCreateAfterDrainValidator.validate(&t).unwrap();
    }

    #[test]
    fn no_create_after_drain_rejects_batch_after_draining() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(1, "phase", &[("phase", "draining")]),
            entry(
                2,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
        ]);
        let err = NoCreateAfterDrainValidator.validate(&t).unwrap_err();
        assert!(err.contains("NoCreateAfterDrain"), "{err}");
    }

    #[test]
    fn checkpoint_ordering_passes_when_all_prior_batches_terminalized() {
        let t = trace(vec![
            entry(
                0,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
            entry(
                1,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
            entry(
                2,
                "checkpoint_update",
                &[("source_id", "0"), ("offset", "10")],
            ),
        ]);
        CheckpointOrderingValidator.validate(&t).unwrap();
    }

    #[test]
    fn checkpoint_ordering_fails_when_prior_batch_not_terminalized() {
        let t = trace(vec![
            entry(
                0,
                "batch_begin",
                &[("batch_id", "1"), ("source_id", "0"), ("checkpoint", "10")],
            ),
            // batch 1 NOT terminalized
            entry(
                1,
                "checkpoint_update",
                &[("source_id", "0"), ("offset", "10")],
            ),
        ]);
        let err = CheckpointOrderingValidator.validate(&t).unwrap_err();
        assert!(err.contains("CheckpointOrderingInvariant"), "{err}");
    }

    #[test]
    fn committed_monotonic_passes_on_increasing_offsets() {
        let t = trace(vec![
            entry(
                0,
                "checkpoint_update",
                &[("source_id", "0"), ("offset", "10")],
            ),
            entry(
                1,
                "checkpoint_update",
                &[("source_id", "0"), ("offset", "20")],
            ),
        ]);
        CommittedMonotonicValidator.validate(&t).unwrap();
    }

    #[test]
    fn committed_monotonic_rejects_regression() {
        let t = trace(vec![
            entry(
                0,
                "checkpoint_update",
                &[("source_id", "0"), ("offset", "20")],
            ),
            entry(
                1,
                "checkpoint_update",
                &[("source_id", "0"), ("offset", "10")],
            ),
        ]);
        let err = CommittedMonotonicValidator.validate(&t).unwrap_err();
        assert!(err.contains("CommittedMonotonic"), "{err}");
    }
}
