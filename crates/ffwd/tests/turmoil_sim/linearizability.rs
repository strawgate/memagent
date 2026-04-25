//! Linearizability history extraction and Porcupine checker integration.
//!
//! This module converts runtime-emitted turmoil seam events into an operation
//! history consumable by an external Porcupine checker.

use std::collections::{BTreeMap, btree_map::Entry};
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::NamedTempFile;

use super::fault_harness::{
    FaultScenario, NetworkFault, NetworkFaultAction, RuntimeEventRecord, ScenarioProfile,
};
use super::instrumented_sink::FailureAction;
use super::observable_checkpoint::CheckpointHandle;
use ffwd_runtime::turmoil_barriers::RuntimeBarrierEvent;

#[derive(Debug, Clone, Eq, PartialEq)]
struct SubmitInfo {
    call_ts: u64,
    submit_seq: u64,
    source_id: u64,
    offset: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ResolveInfo {
    ts: u64,
    outcome: String,
    advanced_to: Option<u64>,
    advanced_sources: Vec<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct FlushInfo {
    ts: u64,
    success: bool,
}

fn is_ackable_outcome(outcome: &str) -> bool {
    matches!(outcome, "delivered" | "rejected")
}

fn outcome_name(outcome: &ffwd_runtime::worker_pool::DeliveryOutcome) -> &'static str {
    use ffwd_runtime::worker_pool::DeliveryOutcome;

    match outcome {
        DeliveryOutcome::Delivered => "delivered",
        DeliveryOutcome::Rejected { .. } => "rejected",
        DeliveryOutcome::RetryExhausted => "retry_exhausted",
        DeliveryOutcome::TimedOut => "timed_out",
        DeliveryOutcome::PoolClosed => "pool_closed",
        DeliveryOutcome::WorkerChannelClosed => "worker_channel_closed",
        DeliveryOutcome::NoWorkersAvailable => "no_workers_available",
        DeliveryOutcome::InternalFailure => "internal_failure",
    }
}

/// Build a JSON history for Porcupine from runtime barrier events.
///
/// Current scope is intentionally narrow:
/// - exactly one checkpoint tuple per submitted batch
/// - single-source checkpoint linearization
fn build_porcupine_history(
    records: &[RuntimeEventRecord],
    checkpoint: Option<&CheckpointHandle>,
) -> Result<Value, String> {
    let mut submits: BTreeMap<u64, SubmitInfo> = BTreeMap::new();
    let mut resolves: BTreeMap<u64, ResolveInfo> = BTreeMap::new();
    let mut flushes: Vec<FlushInfo> = Vec::new();
    let mut submit_seq = 0_u64;
    let mut max_ts = 0_u64;

    for record in records {
        max_ts = max_ts.max(record.ts);
        match &record.event {
            RuntimeBarrierEvent::BatchSubmitted {
                batch_id,
                checkpoints,
            } => {
                if checkpoints.len() != 1 {
                    return Err(format!(
                        "batch {batch_id} has {} checkpoints; linearizability v1 expects exactly 1",
                        checkpoints.len()
                    ));
                }
                let (source_id, offset) = checkpoints[0];
                submit_seq += 1;
                match submits.entry(*batch_id) {
                    Entry::Vacant(slot) => {
                        slot.insert(SubmitInfo {
                            call_ts: record.ts,
                            submit_seq,
                            source_id,
                            offset,
                        });
                    }
                    Entry::Occupied(_) => {
                        return Err(format!("duplicate BatchSubmitted for batch_id={batch_id}"));
                    }
                }
            }
            RuntimeBarrierEvent::AckApplied {
                batch_id,
                outcome,
                checkpoint_advances,
            } => {
                let advanced_to = checkpoint_advances
                    .iter()
                    .max_by_key(|(_, offset)| *offset)
                    .map(|(_, offset)| *offset);
                let mut advanced_sources: Vec<u64> = checkpoint_advances
                    .iter()
                    .map(|(source, _)| *source)
                    .collect();
                advanced_sources.sort_unstable();
                advanced_sources.dedup();
                let resolve = ResolveInfo {
                    ts: record.ts,
                    outcome: outcome_name(outcome).to_string(),
                    advanced_to,
                    advanced_sources,
                };
                match resolves.entry(*batch_id) {
                    Entry::Vacant(slot) => {
                        slot.insert(resolve);
                    }
                    Entry::Occupied(_) => {
                        return Err(format!("duplicate AckApplied for batch_id={batch_id}"));
                    }
                }
            }
            RuntimeBarrierEvent::CheckpointFlush { success } => {
                flushes.push(FlushInfo {
                    ts: record.ts,
                    success: *success,
                });
            }
            _ => {}
        }
    }

    let distinct_sources: std::collections::BTreeSet<u64> =
        submits.values().map(|s| s.source_id).collect();
    if distinct_sources.len() > 1 {
        return Err(format!(
            "linearizability v1 expects single source_id, found {}",
            distinct_sources.len()
        ));
    }
    let maybe_source = distinct_sources.iter().next().copied();

    let mut operations = Vec::new();
    for (batch_id, submit) in &submits {
        // `submit.call_ts.saturating_mul(2)` leaves odd timestamp slots for
        // interleaved resolve operations before the JSON history entry below.
        // Saturation keeps max-value simulated timestamps from overflowing.
        let call = submit.call_ts.saturating_mul(2);
        operations.push(json!({
            "client_id": submit.source_id,
            "call": call,
            "return": call + 1,
            "input": {
                "op": "submit",
                "batch_id": batch_id,
                "submit_seq": submit.submit_seq,
                "source_id": submit.source_id,
                "offset": submit.offset
            },
            "output": {}
        }));
    }
    for (batch_id, resolve) in &resolves {
        let Some(submit) = submits.get(batch_id) else {
            return Err(format!("AckApplied for unknown batch_id={batch_id}"));
        };
        if resolve
            .advanced_sources
            .iter()
            .any(|source| *source != submit.source_id)
        {
            return Err(format!(
                "AckApplied checkpoint_advances include mismatched source(s) for batch_id={batch_id}: submit_source={}, advances={:?}",
                submit.source_id, resolve.advanced_sources
            ));
        }
        if resolve.ts < submit.call_ts {
            return Err(format!(
                "AckApplied ts={} precedes BatchSubmitted ts={} for batch_id={batch_id}",
                resolve.ts, submit.call_ts
            ));
        }
        let call = submit.call_ts.saturating_mul(2).saturating_add(1);
        let ret = resolve.ts.saturating_mul(2).saturating_add(2);
        if ret <= call {
            return Err(format!(
                "invalid resolve interval for batch_id={batch_id}: call={call}, return={ret}"
            ));
        }
        operations.push(json!({
            "client_id": submit.source_id,
            "call": call,
            "return": ret,
            "input": {
                "op": "resolve",
                "batch_id": batch_id,
                "source_id": submit.source_id
            },
            "output": {
                "outcome": resolve.outcome,
                "ackable": is_ackable_outcome(&resolve.outcome),
                "advanced_to": resolve.advanced_to
            }
        }));
    }
    for flush in &flushes {
        let call = flush.ts.saturating_mul(2);
        operations.push(json!({
            "client_id": 0,
            "call": call,
            "return": call + 1,
            "input": {
                "op": "flush"
            },
            "output": {
                "success": flush.success
            }
        }));
    }
    if let Some(source_id) = maybe_source {
        let call = max_ts.saturating_add(1).saturating_mul(2);
        let durable = checkpoint.and_then(|h| h.durable_offset(source_id));
        operations.push(json!({
            "client_id": source_id,
            "call": call,
            "return": call + 1,
            "input": {
                "op": "read_durable",
                "source_id": source_id
            },
            "output": {
                "durable": durable
            }
        }));
    }
    operations.sort_by(|a, b| {
        let ac = a.get("call").and_then(Value::as_u64).unwrap_or_default();
        let bc = b.get("call").and_then(Value::as_u64).unwrap_or_default();
        ac.cmp(&bc)
    });

    Ok(json!({ "operations": operations }))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should exist")
}

fn run_porcupine_checker(history_path: &std::path::Path) -> Result<(), String> {
    let checker_dir = workspace_root().join("scripts/linearizability");
    let output = Command::new("go")
        .arg("run")
        .arg(".")
        .arg("--history")
        .arg(history_path)
        .current_dir(&checker_dir)
        .output()
        .map_err(|e| format!("failed to run go checker: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "porcupine checker failed with exit status {}.\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn parse_seed_matrix_from_env(var: &str, fallback: &[u64]) -> Vec<u64> {
    let Ok(raw) = env::var(var) else {
        return fallback.to_vec();
    };
    let parsed: Vec<u64> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<u64>().ok())
        .collect();
    if parsed.is_empty() {
        fallback.to_vec()
    } else {
        parsed
    }
}

#[test]
fn porcupine_checker_accepts_runtime_history() {
    // Allow local dev without Go toolchain; CI sets this up explicitly.
    let go_available = Command::new("go")
        .arg("version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !go_available {
        return;
    }

    let outcome = FaultScenario::builder("porcupine-history")
        .with_seed(20260441)
        .with_line_count(120)
        .with_batch_target_bytes(256)
        .with_sink_script(vec![
            FailureAction::RetryAfter(Duration::from_millis(5)),
            FailureAction::Succeed,
        ])
        .with_checkpoint_flush_interval(Duration::from_millis(20))
        .with_shutdown_after(Duration::from_secs(3))
        .run();

    let history = build_porcupine_history(outcome.runtime_events(), outcome.checkpoint())
        .expect("runtime events should map to porcupine history");
    let operations = history
        .get("operations")
        .and_then(Value::as_array)
        .expect("history operations array");
    assert!(
        operations.len() >= 2,
        "expected multiple operations for linearizability coverage, got {}",
        operations.len()
    );
    let has_flush = operations.iter().any(|op| {
        op.get("input")
            .and_then(Value::as_object)
            .and_then(|i| i.get("op"))
            .and_then(Value::as_str)
            == Some("flush")
    });
    let has_read = operations.iter().any(|op| {
        op.get("input")
            .and_then(Value::as_object)
            .and_then(|i| i.get("op"))
            .and_then(Value::as_str)
            == Some("read_durable")
    });
    assert!(
        has_flush,
        "expected at least one flush operation in history"
    );
    assert!(
        has_read,
        "expected terminal read_durable operation in history"
    );
    let history_file = NamedTempFile::new().expect("create history temp file");
    serde_json::to_writer_pretty(&history_file, &history)
        .expect("serialize porcupine history json");
    if let Err(err) = run_porcupine_checker(history_file.path()) {
        let rendered = serde_json::to_string_pretty(&history).expect("pretty print history");
        panic!("porcupine checker rejected history: {err}\nhistory:\n{rendered}");
    }
}

#[test]
fn history_builder_requires_single_checkpoint_per_batch() {
    let records = vec![RuntimeEventRecord {
        ts: 1,
        event: RuntimeBarrierEvent::BatchSubmitted {
            batch_id: 1,
            checkpoints: vec![(1, 10), (2, 20)],
        },
    }];
    let err =
        build_porcupine_history(&records, None).expect_err("expected single-checkpoint error");
    assert!(
        err.contains("expects exactly 1"),
        "unexpected error message: {err}"
    );
}

#[test]
fn history_builder_requires_single_source_for_v1_model() {
    let records = vec![
        RuntimeEventRecord {
            ts: 1,
            event: RuntimeBarrierEvent::BatchSubmitted {
                batch_id: 1,
                checkpoints: vec![(1, 10)],
            },
        },
        RuntimeEventRecord {
            ts: 2,
            event: RuntimeBarrierEvent::BatchSubmitted {
                batch_id: 2,
                checkpoints: vec![(2, 20)],
            },
        },
    ];
    let err = build_porcupine_history(&records, None)
        .expect_err("expected single-source model guard error");
    assert!(
        err.contains("expects single source_id"),
        "unexpected error message: {err}"
    );
}

#[test]
fn history_builder_rejects_ack_before_submit() {
    let records = vec![
        RuntimeEventRecord {
            ts: 1,
            event: RuntimeBarrierEvent::AckApplied {
                batch_id: 9,
                outcome: ffwd_runtime::worker_pool::DeliveryOutcome::Delivered,
                checkpoint_advances: vec![(1, 10)],
            },
        },
        RuntimeEventRecord {
            ts: 2,
            event: RuntimeBarrierEvent::BatchSubmitted {
                batch_id: 9,
                checkpoints: vec![(1, 10)],
            },
        },
    ];
    let err = build_porcupine_history(&records, None)
        .expect_err("expected malformed-order guard when AckApplied precedes submit");
    assert!(
        err.contains("precedes BatchSubmitted"),
        "unexpected error message: {err}"
    );
}

#[test]
fn history_builder_rejects_mismatched_advance_source() {
    let records = vec![
        RuntimeEventRecord {
            ts: 1,
            event: RuntimeBarrierEvent::BatchSubmitted {
                batch_id: 7,
                checkpoints: vec![(1, 10)],
            },
        },
        RuntimeEventRecord {
            ts: 2,
            event: RuntimeBarrierEvent::AckApplied {
                batch_id: 7,
                outcome: ffwd_runtime::worker_pool::DeliveryOutcome::Delivered,
                checkpoint_advances: vec![(99, 10)],
            },
        },
    ];
    let err = build_porcupine_history(&records, None)
        .expect_err("expected mismatched-advance-source guard to fail");
    assert!(
        err.contains("mismatched source"),
        "unexpected error message: {err}"
    );
}

#[test]
fn porcupine_checker_rejects_corrupted_durable_read() {
    let go_available = Command::new("go")
        .arg("version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !go_available {
        return;
    }

    let outcome = FaultScenario::builder("porcupine-history-corrupt")
        .with_seed(20260442)
        .with_line_count(80)
        .with_batch_target_bytes(256)
        .with_sink_script(vec![FailureAction::Succeed])
        .with_checkpoint_flush_interval(Duration::from_millis(20))
        .with_shutdown_after(Duration::from_secs(3))
        .run();

    let mut history = build_porcupine_history(outcome.runtime_events(), outcome.checkpoint())
        .expect("runtime events should map to porcupine history");
    let operations = history
        .get_mut("operations")
        .and_then(Value::as_array_mut)
        .expect("history operations array");
    let read_op = operations
        .iter_mut()
        .find(|op| {
            op.get("input")
                .and_then(Value::as_object)
                .and_then(|i| i.get("op"))
                .and_then(Value::as_str)
                == Some("read_durable")
        })
        .expect("expected read_durable op");
    let output = read_op
        .get_mut("output")
        .and_then(Value::as_object_mut)
        .expect("read_durable output object");
    output.insert("durable".to_string(), json!(0));

    let history_file = NamedTempFile::new().expect("create history temp file");
    serde_json::to_writer_pretty(&history_file, &history)
        .expect("serialize porcupine history json");
    let err = run_porcupine_checker(history_file.path())
        .expect_err("checker should reject corrupted durable read");
    assert!(
        err.contains("not linearizable") || err.contains("sequential precheck failed"),
        "unexpected checker error: {err}"
    );
}

#[test]
fn porcupine_checker_rejects_corrupted_resolve_source() {
    let go_available = Command::new("go")
        .arg("version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !go_available {
        return;
    }

    let outcome = FaultScenario::builder("porcupine-history-corrupt-source")
        .with_seed(20260443)
        .with_line_count(80)
        .with_batch_target_bytes(256)
        .with_sink_script(vec![FailureAction::Succeed])
        .with_checkpoint_flush_interval(Duration::from_millis(20))
        .with_shutdown_after(Duration::from_secs(3))
        .run();

    let mut history = build_porcupine_history(outcome.runtime_events(), outcome.checkpoint())
        .expect("runtime events should map to porcupine history");
    let operations = history
        .get_mut("operations")
        .and_then(Value::as_array_mut)
        .expect("history operations array");
    let resolve_op = operations
        .iter_mut()
        .find(|op| {
            op.get("input")
                .and_then(Value::as_object)
                .and_then(|i| i.get("op"))
                .and_then(Value::as_str)
                == Some("resolve")
        })
        .expect("expected resolve op");
    let input = resolve_op
        .get_mut("input")
        .and_then(Value::as_object_mut)
        .expect("resolve input object");
    input.insert("source_id".to_string(), json!(999_u64));

    let history_file = NamedTempFile::new().expect("create history temp file");
    serde_json::to_writer_pretty(&history_file, &history)
        .expect("serialize porcupine history json");
    let err = run_porcupine_checker(history_file.path())
        .expect_err("checker should reject corrupted resolve source");
    assert!(
        err.contains("not linearizable") || err.contains("sequential precheck failed"),
        "unexpected checker error: {err}"
    );
}

#[test]
fn porcupine_checker_accepts_runtime_history_seed_matrix() {
    let go_available = Command::new("go")
        .arg("version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !go_available {
        return;
    }

    let seeds = parse_seed_matrix_from_env(
        "PORCUPINE_SEED_MATRIX",
        &[20260444_u64, 20260445, 20260446, 20260447],
    );
    for seed in seeds {
        let outcome = FaultScenario::builder("porcupine-history-matrix")
            .with_seed(seed)
            .with_line_count(96)
            .with_batch_target_bytes(256)
            .with_sink_script(vec![
                FailureAction::RetryAfter(Duration::from_millis(5)),
                FailureAction::Succeed,
            ])
            .with_checkpoint_flush_interval(Duration::from_millis(20))
            .with_shutdown_after(Duration::from_secs(3))
            .run();

        let history = build_porcupine_history(outcome.runtime_events(), outcome.checkpoint())
            .unwrap_or_else(|err| panic!("seed {seed} history build failed: {err}"));
        let history_file = NamedTempFile::new().expect("create history temp file");
        serde_json::to_writer_pretty(&history_file, &history)
            .expect("serialize porcupine history json");

        if let Err(err) = run_porcupine_checker(history_file.path()) {
            let rendered = serde_json::to_string_pretty(&history).expect("pretty print history");
            panic!("seed {seed} rejected by porcupine checker: {err}\nhistory:\n{rendered}");
        }
    }
}

#[test]
#[ignore = "manual bug-hunting sweep; run explicitly"]
fn porcupine_checker_accepts_chaos_runtime_history_seed_sweep() {
    let go_available = Command::new("go")
        .arg("version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !go_available {
        return;
    }

    let default: Vec<u64> = (20260600_u64..20260640_u64).collect();
    let seeds = parse_seed_matrix_from_env("PORCUPINE_CHAOS_SEED_MATRIX", &default);
    for seed in seeds {
        let outcome = FaultScenario::builder("porcupine-chaos-sweep")
            .with_seed(seed)
            .with_profile(ScenarioProfile::network_chaos())
            .with_turmoil_tcp_sink()
            .with_line_count(120)
            .with_batch_target_bytes(256)
            .with_checkpoint_flush_interval(Duration::from_millis(30))
            .with_shutdown_after(Duration::from_secs(12))
            .with_network_fault(NetworkFault::at_step(80, NetworkFaultAction::Hold))
            .with_network_fault(NetworkFault::at_step(150, NetworkFaultAction::Release))
            .with_network_fault(NetworkFault::at_step(190, NetworkFaultAction::Partition))
            .with_network_fault(NetworkFault::at_step(270, NetworkFaultAction::Repair))
            .run();

        if let Some(err) = outcome.sim_error() {
            panic!("seed {seed} simulation failed before history build: {err}");
        }

        let history = build_porcupine_history(outcome.runtime_events(), outcome.checkpoint())
            .unwrap_or_else(|err| panic!("seed {seed} history build failed: {err}"));
        let history_file = NamedTempFile::new().expect("create history temp file");
        serde_json::to_writer_pretty(&history_file, &history)
            .expect("serialize porcupine history json");

        if let Err(err) = run_porcupine_checker(history_file.path()) {
            let rendered = serde_json::to_string_pretty(&history).expect("pretty print history");
            panic!("seed {seed} rejected by porcupine checker: {err}\nhistory:\n{rendered}");
        }
    }
}
