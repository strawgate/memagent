//! Scenario-driven tests using the fault harness MVP.

use std::io;
use std::time::Duration;

use super::fault_harness::{
    FaultScenario, InvariantSet, NetworkFault, NetworkFaultAction, ScenarioProfile,
    TypedInvariantBundle,
};
use super::instrumented_sink::FailureAction;
use super::trace_bridge::{SinkOutcome, TraceEvent, validate_replay_history_equivalence};

#[test]
fn checkpoint_crash_scenario_keeps_delivery_and_monotonic_updates() {
    let outcome = FaultScenario::builder("checkpoint-crash")
        .with_seed(20260410)
        .with_line_count(20)
        .with_counting_sink()
        .with_batch_timeout(Duration::from_millis(10))
        .with_checkpoint_flush_interval(Duration::from_millis(50))
        .with_checkpoint_crash_after(Duration::from_millis(200))
        .with_shutdown_after(Duration::from_secs(5))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .delivered_eq(20)
        .checkpoint_crash_count_ge(1)
        .checkpoint_updates_ge(1, 1)
        .checkpoint_monotonic(1)
        .verify(&outcome);
}

#[test]
fn pre_durability_flush_failure_never_persists_checkpoint_and_keeps_delivery_contract() {
    let outcome = FaultScenario::builder("pre-durability-flush-failure")
        .with_seed(20260417)
        .with_line_count(20)
        .with_counting_sink()
        .with_batch_timeout(Duration::from_millis(10))
        .with_checkpoint_flush_interval(Duration::from_millis(50))
        .with_checkpoint_crash_on_nth_flush(1)
        .with_shutdown_after(Duration::from_secs(3))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .delivered_eq(20)
        .checkpoint_crash_count_ge(1)
        .checkpoint_updates_ge(1, 1)
        .checkpoint_monotonic(1)
        .checkpoint_durable_absent(1)
        .trace_contract_valid()
        .verify(&outcome);
}

#[test]
fn post_durability_flush_failure_preserves_valid_checkpoint_progress_and_delivery_contract() {
    // Use a slow sink (20ms per batch) to ensure simulated time passes
    // between acks, giving the checkpoint flush interval (50ms) time to
    // fire at least once before the crash is armed at 500ms.
    let mut script = Vec::new();
    for _ in 0..40 {
        script.push(FailureAction::Delay(Duration::from_millis(20)));
    }
    let outcome = FaultScenario::builder("post-durability-flush-failure")
        .with_seed(20260418)
        .with_line_count(40)
        .with_sink_script(script)
        .with_batch_target_bytes(128)
        .with_batch_timeout(Duration::from_millis(10))
        .with_checkpoint_flush_interval(Duration::from_millis(50))
        .with_checkpoint_crash_after(Duration::from_millis(500))
        .with_shutdown_after(Duration::from_secs(10))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .delivered_eq(40)
        .checkpoint_crash_count_ge(1)
        .checkpoint_flush_count_ge(1)
        .checkpoint_updates_ge(1, 1)
        .checkpoint_monotonic(1)
        .checkpoint_durable_not_ahead_of_updates(1)
        .trace_contract_valid()
        .verify(&outcome);
}

/// Issue #1314 / #895 contract mapping:
/// - #1314 Turmoil fault simulation coverage for checkpoint protocol behavior.
/// - #895 Phase 7 terminalization invariant: run must reach terminal stopped phase.
///
/// This scenario forces a deterministic pre-boundary fault by crashing exactly on
/// the first checkpoint flush attempt. Since no durability/ack boundary is crossed,
/// durable checkpoint state must remain absent even though checkpoint updates were
/// observed in memory.
#[test]
fn issue_1314_895_pre_boundary_crash_holds_checkpoint_before_durability_ack_boundary() {
    let outcome = FaultScenario::builder("issue-1314-895-pre-boundary-crash")
        .with_seed(20260434)
        .with_line_count(24)
        .with_counting_sink()
        .with_batch_target_bytes(128)
        .with_batch_timeout(Duration::from_millis(10))
        .with_checkpoint_flush_interval(Duration::from_millis(40))
        .with_checkpoint_crash_on_nth_flush(1)
        .with_shutdown_after(Duration::from_secs(4))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .delivered_eq(24)
        .checkpoint_crash_count_ge(1)
        .checkpoint_updates_ge(1, 1)
        .checkpoint_monotonic(1)
        .checkpoint_durable_absent(1)
        .trace_contract_valid()
        .verify(&outcome);

    let checkpoint = outcome
        .checkpoint()
        .expect("pre-boundary scenario should expose checkpoint handle");
    assert!(
        checkpoint.flush_count() <= 1,
        "pre-boundary crash should not permit sustained durable advancement; flush_count={}",
        checkpoint.flush_count()
    );
}

/// Issue #1314 / #895 contract mapping:
/// - #1314 checkpoint protocol: durable checkpoint must never jump ahead of
///   acknowledged/update history under faulted flushes.
/// - #895 terminalization: even with late crash, scenario must terminate cleanly.
///
/// This scenario creates a deterministic post-boundary fault by crashing on the
/// second checkpoint flush. The first flush crosses durability/ack boundary, then
/// later updates occur before the crash. Durable checkpoint must remain <= observed
/// updates and must not advance to the latest in-memory update after the crash.
#[test]
fn issue_1314_895_post_boundary_crash_preserves_checkpoint_and_terminalization_invariants() {
    let mut script = Vec::new();
    for _ in 0..64 {
        script.push(FailureAction::Delay(Duration::from_millis(20)));
    }
    let outcome = FaultScenario::builder("issue-1314-895-post-boundary-crash")
        .with_seed(20260435)
        .with_line_count(64)
        .with_sink_script(script)
        .with_batch_target_bytes(128)
        .with_batch_timeout(Duration::from_millis(10))
        .with_checkpoint_flush_interval(Duration::from_millis(40))
        .with_checkpoint_crash_on_nth_flush(2)
        .with_shutdown_after(Duration::from_secs(10))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .delivered_eq(64)
        .checkpoint_crash_count_ge(1)
        .checkpoint_flush_count_ge(1)
        .checkpoint_updates_ge(1, 2)
        .checkpoint_monotonic(1)
        .checkpoint_durable_not_ahead_of_updates(1)
        .trace_contract_valid()
        .verify(&outcome);

    let checkpoint = outcome
        .checkpoint()
        .expect("post-boundary scenario should expose checkpoint handle");
    let durable = checkpoint
        .durable_offset(1)
        .expect("post-boundary crash should keep previously durable checkpoint");
    let max_update = checkpoint
        .max_update_offset(1)
        .expect("post-boundary scenario must emit checkpoint updates");
    assert!(
        durable <= max_update,
        "durable checkpoint must never advance beyond observed updates: durable={durable}, max_update={max_update}"
    );
}

#[test]
fn retry_exhaustion_scenario_tracks_send_attempts() {
    let outcome = FaultScenario::builder("retry-exhaustion")
        .with_seed(20260411)
        .with_profile(ScenarioProfile::retry_stress())
        .with_line_count(10)
        .with_sink_script(vec![FailureAction::RepeatIoError(
            io::ErrorKind::ConnectionRefused,
        )])
        .with_pool_drain_timeout(Duration::from_secs(2))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .delivered_eq(0)
        .calls_ge(4)
        .verify(&outcome);
}

#[test]
fn network_partition_repair_scenario_recovers_delivery() {
    let outcome = FaultScenario::builder("network-partition-repair")
        .with_seed(20260412)
        .with_profile(ScenarioProfile::network_chaos())
        .with_turmoil_tcp_sink()
        .with_line_count(80)
        .with_network_fault(NetworkFault::at_step(100, NetworkFaultAction::Partition))
        .with_network_fault(NetworkFault::at_step(260, NetworkFaultAction::Repair))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .server_received_ge(40)
        .verify(&outcome);
}

#[test]
fn turmoil_tcp_checkpoint_crash_scenario_captures_checkpoint_activity() {
    let outcome = FaultScenario::builder("turmoil-tcp-checkpoint-crash")
        .with_seed(20260415)
        .with_turmoil_tcp_sink()
        .with_line_count(40)
        .with_batch_timeout(Duration::from_millis(20))
        .with_checkpoint_flush_interval(Duration::from_millis(50))
        .with_checkpoint_crash_after(Duration::from_millis(200))
        .with_shutdown_after(Duration::from_secs(10))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .server_received_ge(1)
        .checkpoint_crash_count_ge(1)
        .checkpoint_updates_ge(1, 1)
        .checkpoint_monotonic(1)
        .verify(&outcome);
}

#[test]
fn step_zero_network_fault_is_applied_and_recovery_is_possible() {
    let outcome = FaultScenario::builder("network-step-zero-fault")
        .with_seed(20260414)
        .with_turmoil_tcp_sink()
        .with_line_count(20)
        .with_batch_timeout(Duration::from_millis(20))
        .with_shutdown_after(Duration::from_secs(10))
        .with_network_fault(NetworkFault::at_step(0, NetworkFaultAction::Partition))
        .with_network_fault(NetworkFault::at_step(120, NetworkFaultAction::Repair))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .server_received_ge(1)
        .verify(&outcome);
}

#[test]
fn seed_replay_produces_stable_outcome_for_fail_rate_scenario() {
    let seed = 20260413;
    let baseline = FaultScenario::builder("seed-replay-baseline")
        .with_seed(seed)
        .with_turmoil_tcp_sink()
        .with_fail_rate(0.005)
        .with_line_count(30)
        .with_shutdown_after(Duration::from_secs(12))
        .run();

    let replay = FaultScenario::builder("seed-replay-replay")
        .with_seed(seed)
        .with_turmoil_tcp_sink()
        .with_fail_rate(0.005)
        .with_line_count(30)
        .with_shutdown_after(Duration::from_secs(12))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .verify(&baseline);
    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .verify(&replay);
    assert_eq!(
        baseline.server_received(),
        replay.server_received(),
        "same seed should reproduce same delivery count"
    );
}

#[test]
fn panic_unwind_scenario_reports_panic_contract() {
    let outcome = FaultScenario::builder("panic-unwind")
        .with_seed(20260420)
        .with_line_count(4)
        .with_sink_script(vec![FailureAction::Panic])
        .with_shutdown_after(Duration::from_millis(250))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .trace_contract_valid()
        .verify(&outcome);
    assert!(
        outcome.trace_events().iter().any(|event| matches!(
            event,
            TraceEvent::SinkResult {
                outcome: SinkOutcome::Panic,
                ..
            }
        )),
        "panic scenario must emit a sink panic outcome event"
    );
}

#[test]
fn checkpoint_flush_crash_can_hold_durable_checkpoint_progress() {
    let outcome = FaultScenario::builder("checkpoint-hold-no-advance")
        .with_seed(20260421)
        .with_line_count(20)
        .with_counting_sink()
        .with_batch_timeout(Duration::from_millis(10))
        .with_checkpoint_flush_interval(Duration::from_millis(50))
        .with_checkpoint_crash_after(Duration::from_millis(1))
        .with_shutdown_after(Duration::from_millis(70))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .checkpoint_crash_count_ge(1)
        .checkpoint_updates_ge(1, 1)
        .checkpoint_durable_eq(1, None)
        .verify(&outcome);
}

#[test]
fn post_stop_trace_contract_disallows_further_side_effects() {
    let outcome = FaultScenario::builder("post-stop-contract")
        .with_seed(20260422)
        .with_line_count(15)
        .with_sink_script(vec![FailureAction::Succeed])
        .with_batch_timeout(Duration::from_millis(10))
        .with_shutdown_after(Duration::from_secs(2))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .trace_contract_valid()
        .verify(&outcome);
}

#[test]
fn replay_equivalence_same_seed_produces_same_normalized_contract_trace() {
    let seed = 20260423;
    let baseline = FaultScenario::builder("replay-equivalence-a")
        .with_seed(seed)
        .with_line_count(12)
        .with_sink_script(vec![
            FailureAction::RetryAfter(Duration::from_millis(10)),
            FailureAction::Succeed,
        ])
        .with_checkpoint_flush_interval(Duration::from_millis(40))
        .with_shutdown_after(Duration::from_secs(3))
        .with_typed_contract(TypedInvariantBundle::trace_contract())
        .run();
    let replay = FaultScenario::builder("replay-equivalence-b")
        .with_seed(seed)
        .with_line_count(12)
        .with_sink_script(vec![
            FailureAction::RetryAfter(Duration::from_millis(10)),
            FailureAction::Succeed,
        ])
        .with_checkpoint_flush_interval(Duration::from_millis(40))
        .with_shutdown_after(Duration::from_secs(3))
        .with_typed_contract(TypedInvariantBundle::trace_contract())
        .run();

    InvariantSet::new()
        .no_sim_error()
        .trace_contract_valid()
        .verify(&baseline);
    InvariantSet::new()
        .no_sim_error()
        .trace_contract_valid()
        .verify(&replay);
    assert_eq!(
        baseline.normalized_contract_trace(),
        replay.normalized_contract_trace(),
        "same seed should produce same normalized contract trace"
    );
}

#[test]
fn replay_equivalence_seed_matrix_keeps_history_equivalent() {
    let seeds = [20260430_u64, 20260431, 20260432, 20260433];
    for seed in seeds {
        let run_a = FaultScenario::builder("replay-matrix-a")
            .with_seed(seed)
            .with_line_count(16)
            .with_sink_script(vec![
                FailureAction::RetryAfter(Duration::from_millis(5)),
                FailureAction::Succeed,
            ])
            .with_checkpoint_flush_interval(Duration::from_millis(30))
            .with_shutdown_after(Duration::from_secs(3))
            .run();
        let run_b = FaultScenario::builder("replay-matrix-b")
            .with_seed(seed)
            .with_line_count(16)
            .with_sink_script(vec![
                FailureAction::RetryAfter(Duration::from_millis(5)),
                FailureAction::Succeed,
            ])
            .with_checkpoint_flush_interval(Duration::from_millis(30))
            .with_shutdown_after(Duration::from_secs(3))
            .run();

        InvariantSet::new()
            .no_sim_error()
            .trace_contract_valid()
            .verify(&run_a);
        InvariantSet::new()
            .no_sim_error()
            .trace_contract_valid()
            .verify(&run_b);

        validate_replay_history_equivalence(&[
            run_a.trace_events().to_vec(),
            run_b.trace_events().to_vec(),
        ])
        .unwrap_or_else(|err| panic!("seed {seed} replay equivalence failed: {err}"));
    }
}

#[test]
fn hold_release_schedule_restores_delivery() {
    let outcome = FaultScenario::builder("hold-release")
        .with_seed(20260424)
        .with_profile(ScenarioProfile::network_chaos())
        .with_turmoil_tcp_sink()
        .with_line_count(50)
        .with_network_fault(NetworkFault::at_step(80, NetworkFaultAction::Hold))
        .with_network_fault(NetworkFault::at_step(160, NetworkFaultAction::Release))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .server_received_ge(1)
        .verify(&outcome);
}

#[test]
fn chaos_schedule_converges_after_network_stabilizes() {
    let line_count = 120_u64;
    let outcome = FaultScenario::builder("chaos-then-stable")
        .with_seed(20260434)
        .with_profile(ScenarioProfile::network_chaos())
        .with_turmoil_tcp_sink()
        .with_line_count(line_count as usize)
        .with_network_fault(NetworkFault::at_step(40, NetworkFaultAction::Hold))
        .with_network_fault(NetworkFault::at_step(110, NetworkFaultAction::Release))
        .with_network_fault(NetworkFault::at_step(150, NetworkFaultAction::Partition))
        .with_network_fault(NetworkFault::at_step(210, NetworkFaultAction::Repair))
        .with_network_fault(NetworkFault::at_step(240, NetworkFaultAction::Hold))
        .with_network_fault(NetworkFault::at_step(300, NetworkFaultAction::Release))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .trace_contract_valid()
        .verify(&outcome);
    assert_eq!(
        outcome.server_received(),
        Some(line_count),
        "chaos phase should eventually converge to full delivery after stable tail"
    );
}

#[test]
fn one_way_fault_schedule_recovers_after_directional_repair() {
    let line_count = 80_u64;
    let outcome = FaultScenario::builder("one-way-fault-schedule")
        .with_seed(20260436)
        .with_profile(ScenarioProfile::network_chaos())
        .with_turmoil_tcp_sink()
        .with_line_count(line_count as usize)
        .with_network_fault(NetworkFault::at_step(
            0,
            NetworkFaultAction::PartitionOneWay,
        ))
        .with_network_fault(NetworkFault::at_step(150, NetworkFaultAction::RepairOneWay))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .trace_contract_valid()
        .verify(&outcome);
    assert_eq!(
        outcome.server_received(),
        Some(line_count),
        "directional repair should restore full delivery after one-way partition"
    );
}

fn replay_equivalence_run(seed: u64, name: &str) -> Vec<TraceEvent> {
    let outcome = FaultScenario::builder(name)
        .with_seed(seed)
        .with_line_count(16)
        .with_sink_script(vec![
            FailureAction::RetryAfter(Duration::from_millis(5)),
            FailureAction::Succeed,
        ])
        .with_checkpoint_flush_interval(Duration::from_millis(30))
        .with_shutdown_after(Duration::from_secs(3))
        .run();
    InvariantSet::new()
        .no_sim_error()
        .trace_contract_valid()
        .verify(&outcome);
    outcome.trace_events().to_vec()
}

#[test]
fn replay_equivalence_seed_supports_three_identical_replays() {
    let seed = 20260435_u64;
    let runs = vec![
        replay_equivalence_run(seed, "replay-3x-a"),
        replay_equivalence_run(seed, "replay-3x-b"),
        replay_equivalence_run(seed, "replay-3x-c"),
    ];
    validate_replay_history_equivalence(&runs)
        .unwrap_or_else(|err| panic!("seed {seed} replay equivalence (3x) failed: {err}"));
}
