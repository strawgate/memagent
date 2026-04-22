//! Trace validators for `tla/ShutdownProtocol.tla` safety properties.
//!
//! These validators check shutdown cascade ordering and data conservation
//! using the existing trace event stream.

use crate::trace_bridge::{EventValidator, NormalizedTrace};

/// Return all ShutdownProtocol validators.
pub fn validators() -> Vec<Box<dyn EventValidator>> {
    vec![
        Box::new(ShutdownOrderingValidator),
        Box::new(ConservationValidator),
    ]
}

// ---------------------------------------------------------------------------
// ShutdownOrderingValidator — TLA+ ShutdownProtocol.tla ordering properties
//
// Verifies the shutdown cascade:
//   phase:draining → pool_drain_begin → pool_drain_complete → phase:stopped
//
// Covers: NoCpuStopBeforeIoDrain, NoJoinBeforePipelineDrain, NoStopBeforeJoin
// (mapped to the simplified turmoil shutdown model).
// ---------------------------------------------------------------------------

pub struct ShutdownOrderingValidator;

impl EventValidator for ShutdownOrderingValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        // Track ordering by index of first occurrence.
        let mut draining_idx: Option<usize> = None;
        let mut pool_drain_begin_idx: Option<usize> = None;
        let mut pool_drain_complete_idx: Option<usize> = None;
        let mut stopped_idx: Option<usize> = None;

        for entry in &trace.entries {
            match entry.kind {
                "phase" => match entry.attributes.get("phase").map(String::as_str) {
                    Some("draining") if draining_idx.is_none() => {
                        draining_idx = Some(entry.index);
                    }
                    Some("stopped") if stopped_idx.is_none() => {
                        stopped_idx = Some(entry.index);
                    }
                    _ => {}
                },
                "pool_drain_begin" if pool_drain_begin_idx.is_none() => {
                    pool_drain_begin_idx = Some(entry.index);
                }
                "pool_drain_complete" if pool_drain_complete_idx.is_none() => {
                    pool_drain_complete_idx = Some(entry.index);
                }
                _ => {}
            }
        }

        // All four events must be present for a complete shutdown.
        let (Some(d), Some(pdb), Some(pdc), Some(s)) =
            (draining_idx, pool_drain_begin_idx, pool_drain_complete_idx, stopped_idx)
        else {
            // Incomplete shutdown — skip (other validators catch missing phases).
            return Ok(());
        };

        if d > pdb {
            return Err(format!(
                "ShutdownOrdering violated: draining (idx {d}) must precede \
                 pool_drain_begin (idx {pdb})"
            ));
        }
        if pdb > pdc {
            return Err(format!(
                "ShutdownOrdering violated: pool_drain_begin (idx {pdb}) must precede \
                 pool_drain_complete (idx {pdc})"
            ));
        }
        if pdc > s {
            return Err(format!(
                "ShutdownOrdering violated: pool_drain_complete (idx {pdc}) must precede \
                 stopped (idx {s})"
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConservationValidator — TLA+ ShutdownProtocol.tla::IoConservation + PipelineConservation
//
// Total rows entering the pipeline (sum of batch_begin events) must equal
// total rows leaving (sum of batch_terminal events' corresponding row counts).
//
// Since the trace carries row counts on sink_result but not on batch_terminal,
// we use a simpler formulation: every batch_begin batch_id must have a
// corresponding batch_terminal. This is conservation at the batch level
// (no batches silently lost in transit).
// ---------------------------------------------------------------------------

pub struct ConservationValidator;

impl EventValidator for ConservationValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut began: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        let mut terminalized: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        let mut saw_stopped = false;

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
                "phase" => {
                    if entry.attributes.get("phase").map(String::as_str) == Some("stopped") {
                        saw_stopped = true;
                    }
                }
                _ => {}
            }
        }

        if !saw_stopped {
            return Ok(());
        }

        // Every batch that entered must have exited.
        let lost: std::collections::BTreeSet<_> = began.difference(&terminalized).collect();
        if !lost.is_empty() {
            return Err(format!(
                "Conservation violated: {} batch(es) entered pipeline but never \
                 terminalized (data loss): {:?}",
                lost.len(),
                lost
            ));
        }

        // No batch should appear in terminal without a begin (phantom batches).
        let phantom: std::collections::BTreeSet<_> = terminalized.difference(&began).collect();
        if !phantom.is_empty() {
            return Err(format!(
                "Conservation violated: {} batch(es) terminalized without \
                 batch_begin (phantom): {:?}",
                phantom.len(),
                phantom
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
    fn shutdown_ordering_passes_correct_cascade() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(1, "phase", &[("phase", "draining")]),
            entry(2, "pool_drain_begin", &[]),
            entry(3, "pool_drain_complete", &[("forced_abort", "false")]),
            entry(4, "phase", &[("phase", "stopped")]),
        ]);
        ShutdownOrderingValidator.validate(&t).unwrap();
    }

    #[test]
    fn shutdown_ordering_fails_when_pool_drain_before_draining() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(1, "pool_drain_begin", &[]),
            entry(2, "phase", &[("phase", "draining")]),
            entry(3, "pool_drain_complete", &[("forced_abort", "false")]),
            entry(4, "phase", &[("phase", "stopped")]),
        ]);
        let err = ShutdownOrderingValidator.validate(&t).unwrap_err();
        assert!(err.contains("ShutdownOrdering"), "{err}");
    }

    #[test]
    fn conservation_passes_when_all_batches_accounted() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(1, "batch_begin", &[("batch_id", "1")]),
            entry(2, "batch_begin", &[("batch_id", "2")]),
            entry(
                3,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
            entry(
                4,
                "batch_terminal",
                &[("batch_id", "2"), ("terminal", "rejected")],
            ),
            entry(5, "phase", &[("phase", "draining")]),
            entry(6, "phase", &[("phase", "stopped")]),
        ]);
        ConservationValidator.validate(&t).unwrap();
    }

    #[test]
    fn conservation_fails_on_data_loss() {
        let t = trace(vec![
            entry(0, "phase", &[("phase", "running")]),
            entry(1, "batch_begin", &[("batch_id", "1")]),
            entry(2, "batch_begin", &[("batch_id", "2")]),
            entry(
                3,
                "batch_terminal",
                &[("batch_id", "1"), ("terminal", "acked")],
            ),
            // batch 2 lost — no terminal
            entry(4, "phase", &[("phase", "draining")]),
            entry(5, "phase", &[("phase", "stopped")]),
        ]);
        let err = ConservationValidator.validate(&t).unwrap_err();
        assert!(err.contains("Conservation"), "{err}");
    }
}
