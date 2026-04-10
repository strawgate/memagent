//! Scenario-driven tests using the fault harness MVP.

use std::io;
use std::time::Duration;

use super::fault_harness::{FaultScenario, InvariantSet, NetworkFault, NetworkFaultAction};
use super::instrumented_sink::FailureAction;

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
