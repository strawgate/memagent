//! Scenario-driven tests using the fault harness MVP.

use std::io;
use std::time::Duration;

use super::fault_harness::{
    FaultScenario, InvariantSet, NetworkFault, NetworkFaultAction, TypedInvariantBundle,
};
use super::instrumented_sink::FailureAction;
use super::trace_bridge::{SinkOutcome, TraceEvent};

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
fn retry_exhaustion_scenario_tracks_send_attempts() {
    let outcome = FaultScenario::builder("retry-exhaustion")
        .with_seed(20260411)
        .with_line_count(10)
        .with_sink_script(vec![
            FailureAction::IoError(io::ErrorKind::ConnectionRefused),
            FailureAction::IoError(io::ErrorKind::ConnectionRefused),
            FailureAction::IoError(io::ErrorKind::ConnectionRefused),
            FailureAction::IoError(io::ErrorKind::ConnectionRefused),
            FailureAction::IoError(io::ErrorKind::ConnectionRefused),
        ])
        .with_shutdown_after(Duration::from_secs(5))
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
        .with_turmoil_tcp_sink()
        .with_line_count(80)
        .with_batch_timeout(Duration::from_millis(20))
        .with_shutdown_after(Duration::from_secs(15))
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
fn hold_release_schedule_restores_delivery() {
    let outcome = FaultScenario::builder("hold-release")
        .with_seed(20260424)
        .with_turmoil_tcp_sink()
        .with_line_count(50)
        .with_batch_timeout(Duration::from_millis(20))
        .with_shutdown_after(Duration::from_secs(10))
        .with_network_fault(NetworkFault::at_step(80, NetworkFaultAction::Hold))
        .with_network_fault(NetworkFault::at_step(160, NetworkFaultAction::Release))
        .run();

    InvariantSet::new()
        .no_sim_error()
        .server_connections_ge(1)
        .server_received_ge(1)
        .verify(&outcome);
}
