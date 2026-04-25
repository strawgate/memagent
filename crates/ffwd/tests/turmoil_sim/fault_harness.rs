//! MVP fault scenario harness for turmoil simulations.
//!
//! The harness composes fault injection inputs (sink behavior scripts,
//! checkpoint flush crashes, and turmoil network events) as data and runs
//! invariant checks against a structured `TestOutcome`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ffwd::pipeline::Pipeline;
use ffwd_runtime::turmoil_barriers::RuntimeBarrierEvent;
use ffwd_test_utils::sinks::CountingSink;
use ffwd_types::pipeline::SourceId;
use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;
use turmoil::barriers::Barrier;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSink};
use super::observable_checkpoint::{CheckpointHandle, ObservableCheckpointStore};
use super::tcp_server::{TcpServerHandle, run_tcp_server};
use super::trace_bridge::{
    TraceEvent, TransitionValidator, normalized_contract_trace, trace_event_from_runtime_barrier,
};
use super::turmoil_tcp_sink::TurmoilTcpSink;

const DEFAULT_SIM_DURATION_SECS: u64 = 60;
const DEFAULT_TICK_MS: u64 = 1;
const DEFAULT_BATCH_TIMEOUT_MS: u64 = 20;
const DEFAULT_TCP_PORT: u16 = 9137;

/// Shared scenario profile values for turmoil fault harness tests.
#[derive(Clone, Copy, Debug)]
pub struct ScenarioProfile {
    duration_secs: u64,
    tick_ms: u64,
    tcp_capacity: Option<usize>,
    batch_timeout_ms: u64,
    shutdown_after: Duration,
    pool_drain_timeout: Duration,
}

impl ScenarioProfile {
    /// Balanced default profile used by most scenario tests.
    pub const fn balanced() -> Self {
        Self {
            duration_secs: DEFAULT_SIM_DURATION_SECS,
            tick_ms: DEFAULT_TICK_MS,
            tcp_capacity: None,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
            shutdown_after: Duration::from_secs(5),
            pool_drain_timeout: Duration::from_secs(60),
        }
    }

    /// Profile tuned for network-chaos scenarios with longer convergence windows.
    pub const fn network_chaos() -> Self {
        Self {
            duration_secs: 90,
            tick_ms: 1,
            tcp_capacity: Some(1024 * 1024),
            batch_timeout_ms: 20,
            shutdown_after: Duration::from_secs(20),
            pool_drain_timeout: Duration::from_secs(60),
        }
    }

    /// Profile tuned for aggressive retry-shutdown interactions.
    pub const fn retry_stress() -> Self {
        Self {
            duration_secs: 60,
            tick_ms: 1,
            tcp_capacity: None,
            batch_timeout_ms: 20,
            shutdown_after: Duration::from_secs(5),
            pool_drain_timeout: Duration::from_secs(2),
        }
    }
}

/// One runtime barrier event captured with a monotonic logical timestamp.
#[derive(Clone, Debug)]
pub struct RuntimeEventRecord {
    /// Monotonic logical timestamp assigned by the fault harness.
    pub ts: u64,
    /// Raw runtime barrier event observed at the simulation seam.
    pub event: RuntimeBarrierEvent,
}

/// A network fault operation the scenario can inject into the simulation.
///
/// `Partition` isolates the pipeline host from the server host for the
/// configured step, and `Repair` restores connectivity so later steps can
/// observe the recovered path.
#[derive(Clone, Debug)]
pub enum NetworkFaultAction {
    /// Partition the pipeline and server hosts.
    Partition,
    /// Repair the pipeline and server hosts.
    Repair,
    /// Partition the pipeline -> server direction only.
    PartitionOneWay,
    /// Repair the pipeline -> server direction only.
    RepairOneWay,
    /// Hold all in-flight messages between the hosts.
    Hold,
    /// Release held in-flight messages between the hosts.
    Release,
}

/// A network fault scheduled for a particular turmoil simulation step.
///
/// The `step` is the deterministic turmoil tick at which the fault is
/// applied. Use [`NetworkFaultAction::Partition`] to cut connectivity and
/// [`NetworkFaultAction::Repair`] to restore it.
#[derive(Clone, Debug)]
pub struct NetworkFault {
    step: usize,
    action: NetworkFaultAction,
}

impl NetworkFault {
    /// Create a fault that should fire at the given simulation step.
    ///
    /// Steps are the turmoil simulation ticks produced by the harness, so a
    /// lower `step` fires earlier in the same run.
    pub fn at_step(step: usize, action: NetworkFaultAction) -> Self {
        Self { step, action }
    }
}

/// Sink selection for the fault scenario.
///
/// The harness uses this to choose between a scripted sink, the real TCP
/// sink path, or a lightweight counting sink when only delivery accounting
/// matters.
#[derive(Clone, Debug)]
pub enum SinkMode {
    /// Use the scripted, traceable sink implementation.
    Instrumented { script: Vec<FailureAction> },
    /// Exercise the real TCP sink against a turmoil TCP server.
    TurmoilTcp,
    /// Count delivered rows without any scripted failure behavior.
    Counting,
}

/// Configurable turmoil scenario used by the verification harness.
///
/// A scenario bundles input shape, sink mode, checkpoint behavior, network
/// faults, and simulation timing into one reusable verification run.
#[derive(Clone, Debug)]
pub struct FaultScenario {
    name: String,
    seed: u64,
    source_id: SourceId,
    lines: usize,
    sink_mode: SinkMode,
    duration_secs: u64,
    tick_ms: u64,
    batch_timeout_ms: u64,
    tcp_capacity: Option<usize>,
    batch_target_bytes: Option<usize>,
    shutdown_after: Duration,
    pool_drain_timeout: Duration,
    checkpoint_flush_interval: Option<Duration>,
    arm_checkpoint_crash_after: Option<Duration>,
    crash_on_nth_flush: Option<u64>,
    network_faults: Vec<NetworkFault>,
    fail_rate: Option<f64>,
    typed_contract: Option<TypedInvariantBundle>,
}

impl FaultScenario {
    /// Build a scenario with the default harness settings.
    pub fn builder(name: &str) -> Self {
        let profile = ScenarioProfile::balanced();
        Self {
            name: name.to_string(),
            seed: super::turmoil_seed(),
            source_id: SourceId(1),
            lines: 10,
            sink_mode: SinkMode::Counting,
            duration_secs: profile.duration_secs,
            tick_ms: profile.tick_ms,
            tcp_capacity: profile.tcp_capacity,
            batch_timeout_ms: profile.batch_timeout_ms,
            batch_target_bytes: None,
            shutdown_after: profile.shutdown_after,
            pool_drain_timeout: profile.pool_drain_timeout,
            checkpoint_flush_interval: None,
            arm_checkpoint_crash_after: None,
            crash_on_nth_flush: None,
            network_faults: Vec::new(),
            fail_rate: None,
            typed_contract: None,
        }
    }

    /// Override the turmoil RNG seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Apply a shared simulation profile to reduce per-test magic numbers.
    pub fn with_profile(mut self, profile: ScenarioProfile) -> Self {
        self.duration_secs = profile.duration_secs;
        self.tick_ms = profile.tick_ms;
        self.batch_timeout_ms = profile.batch_timeout_ms;
        self.tcp_capacity = profile.tcp_capacity;
        self.shutdown_after = profile.shutdown_after;
        self.pool_drain_timeout = profile.pool_drain_timeout;
        self
    }

    /// Set how many JSON lines the scenario should feed to the pipeline.
    pub fn with_line_count(mut self, lines: usize) -> Self {
        self.lines = lines;
        self
    }

    /// Select the scripted sink mode for the scenario.
    pub fn with_sink_script(mut self, script: Vec<FailureAction>) -> Self {
        self.sink_mode = SinkMode::Instrumented { script };
        self
    }

    /// Route output through the turmoil TCP sink/server pair.
    pub fn with_turmoil_tcp_sink(mut self) -> Self {
        self.sink_mode = SinkMode::TurmoilTcp;
        self
    }

    /// Use the counter-only sink mode.
    pub fn with_counting_sink(mut self) -> Self {
        self.sink_mode = SinkMode::Counting;
        self
    }

    /// Override the simulation shutdown deadline.
    pub fn with_shutdown_after(mut self, shutdown_after: Duration) -> Self {
        self.shutdown_after = shutdown_after;
        self
    }

    /// Override worker-pool drain timeout for this scenario.
    pub fn with_pool_drain_timeout(mut self, timeout: Duration) -> Self {
        self.pool_drain_timeout = timeout;
        self
    }

    /// Override the pipeline batch timeout.
    pub fn with_batch_timeout(mut self, timeout: Duration) -> Self {
        self.batch_timeout_ms = timeout.as_millis() as u64;
        self
    }

    /// Override the pipeline batch target bytes.
    pub fn with_batch_target_bytes(mut self, batch_target_bytes: usize) -> Self {
        self.batch_target_bytes = Some(batch_target_bytes);
        self
    }

    /// Enable checkpoint flushing at a fixed interval.
    pub fn with_checkpoint_flush_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_flush_interval = Some(interval);
        self
    }

    /// Arm a simulated checkpoint crash after the given delay.
    pub fn with_checkpoint_crash_after(mut self, crash_after: Duration) -> Self {
        self.arm_checkpoint_crash_after = Some(crash_after);
        self
    }

    /// Simulate a crash exactly on the Nth checkpoint flush attempt.
    pub fn with_checkpoint_crash_on_nth_flush(mut self, n: u64) -> Self {
        assert!(n > 0, "crash trigger is 1-indexed; 0 disables crash path");
        self.crash_on_nth_flush = Some(n);
        self
    }

    /// Schedule an additional network fault for the simulation.
    pub fn with_network_fault(mut self, fault: NetworkFault) -> Self {
        self.network_faults.push(fault);
        self
    }

    /// Set the turmoil fail rate.
    pub fn with_fail_rate(mut self, fail_rate: f64) -> Self {
        self.fail_rate = Some(fail_rate);
        self
    }

    /// Prototype Shape A lane: attach a typed invariant bundle.
    pub fn with_typed_contract(mut self, typed_contract: TypedInvariantBundle) -> Self {
        self.typed_contract = Some(typed_contract);
        self
    }

    /// Execute the scenario and return the captured outcome.
    pub fn run(self) -> TestOutcome {
        let scenario_name = self.name.clone();
        let seed = self.seed;
        let mut builder = super::sim_builder_with_profile(
            super::SimProfile::DEFAULT
                .with_duration(self.duration_secs)
                .with_tick(self.tick_ms),
        );
        builder.rng_seed(seed);
        if let Some(tcp_capacity) = self.tcp_capacity {
            builder.tcp_capacity(tcp_capacity);
        }
        if let Some(fail_rate) = self.fail_rate {
            builder.fail_rate(fail_rate);
        }
        let mut sim = builder.build();

        let mut delivered_counter = Arc::new(AtomicU64::new(0));
        let mut call_counter = Arc::new(AtomicU64::new(0));
        let mut checkpoint_handle: Option<CheckpointHandle> = None;
        let mut tcp_server_handle: Option<TcpServerHandle> = None;
        let mut runtime_events_barrier = Barrier::new(|_: &RuntimeBarrierEvent| true);
        let mut trace_events = Vec::new();
        let mut runtime_events = Vec::new();
        let mut runtime_event_ts = 0_u64;

        match &self.sink_mode {
            SinkMode::TurmoilTcp => {
                let server = TcpServerHandle::new();
                let sh = server.clone();
                sim.host("server", move || {
                    let host_handle = sh.clone();
                    async move {
                        run_tcp_server(DEFAULT_TCP_PORT, host_handle).await?;
                        Ok(())
                    }
                });
                delivered_counter = server.received_lines.clone();
                tcp_server_handle = Some(server);

                let scenario = self.clone();
                let maybe_checkpoint = if scenario.checkpoint_flush_interval.is_some()
                    || scenario.arm_checkpoint_crash_after.is_some()
                    || scenario.crash_on_nth_flush.is_some()
                {
                    let (mut store, handle) = ObservableCheckpointStore::new();
                    if let Some(n) = self.crash_on_nth_flush {
                        store = store.with_crash_on_nth_flush(n);
                    }
                    checkpoint_handle = Some(handle.clone());
                    Some((store, handle))
                } else {
                    None
                };

                sim.client("pipeline", async move {
                    let lines = generate_json_lines(scenario.lines);
                    let input = ChannelInputSource::new("scenario", scenario.source_id, lines);

                    let sink = TurmoilTcpSink::new("server", DEFAULT_TCP_PORT);
                    let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
                    pipeline.set_pool_drain_timeout(scenario.pool_drain_timeout);
                    pipeline.set_batch_timeout(Duration::from_millis(scenario.batch_timeout_ms));
                    if let Some(batch_target_bytes) = scenario.batch_target_bytes {
                        pipeline.set_batch_target_bytes(batch_target_bytes);
                    }
                    let mut pipeline = pipeline.with_input("scenario", Box::new(input));

                    if let Some(interval) = scenario.checkpoint_flush_interval {
                        pipeline.set_checkpoint_flush_interval(interval);
                    }

                    if let Some((store, handle)) = maybe_checkpoint {
                        pipeline = pipeline.with_checkpoint_store(Box::new(store));
                        if let Some(crash_after) = scenario.arm_checkpoint_crash_after {
                            tokio::spawn(async move {
                                tokio::time::sleep(crash_after).await;
                                handle.arm_crash();
                            });
                        }
                    }

                    let shutdown = CancellationToken::new();
                    let sd = shutdown.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(scenario.shutdown_after).await;
                        sd.cancel();
                    });

                    let run_result = pipeline.run_async(&shutdown).await;
                    run_result?;
                    Ok(())
                });
            }
            SinkMode::Instrumented { script } => {
                let sink = InstrumentedSink::new(script.clone());
                delivered_counter = sink.delivered_counter();
                call_counter = sink.call_counter();

                let scenario = self.clone();
                let maybe_checkpoint = if scenario.checkpoint_flush_interval.is_some()
                    || scenario.arm_checkpoint_crash_after.is_some()
                    || scenario.crash_on_nth_flush.is_some()
                {
                    let (mut store, handle) = ObservableCheckpointStore::new();
                    if let Some(n) = self.crash_on_nth_flush {
                        store = store.with_crash_on_nth_flush(n);
                    }
                    checkpoint_handle = Some(handle.clone());
                    Some((store, handle))
                } else {
                    None
                };

                sim.client("pipeline", async move {
                    let lines = generate_json_lines(scenario.lines);
                    let input = ChannelInputSource::new("scenario", scenario.source_id, lines);
                    let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
                    pipeline.set_pool_drain_timeout(scenario.pool_drain_timeout);
                    pipeline.set_batch_timeout(Duration::from_millis(scenario.batch_timeout_ms));
                    if let Some(batch_target_bytes) = scenario.batch_target_bytes {
                        pipeline.set_batch_target_bytes(batch_target_bytes);
                    }

                    let mut pipeline = pipeline.with_input("scenario", Box::new(input));

                    if let Some(interval) = scenario.checkpoint_flush_interval {
                        pipeline.set_checkpoint_flush_interval(interval);
                    }

                    if let Some((store, handle)) = maybe_checkpoint {
                        pipeline = pipeline.with_checkpoint_store(Box::new(store));
                        if let Some(crash_after) = scenario.arm_checkpoint_crash_after {
                            tokio::spawn(async move {
                                tokio::time::sleep(crash_after).await;
                                handle.arm_crash();
                            });
                        }
                    }

                    let shutdown = CancellationToken::new();
                    let sd = shutdown.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(scenario.shutdown_after).await;
                        sd.cancel();
                    });

                    let run_result = pipeline.run_async(&shutdown).await;
                    run_result?;
                    Ok(())
                });
            }
            SinkMode::Counting => {
                let sink = CountingSink::new(delivered_counter.clone());
                let scenario = self.clone();

                let maybe_checkpoint = if scenario.checkpoint_flush_interval.is_some()
                    || scenario.arm_checkpoint_crash_after.is_some()
                    || scenario.crash_on_nth_flush.is_some()
                {
                    let (mut store, handle) = ObservableCheckpointStore::new();
                    if let Some(n) = self.crash_on_nth_flush {
                        store = store.with_crash_on_nth_flush(n);
                    }
                    checkpoint_handle = Some(handle.clone());
                    Some((store, handle))
                } else {
                    None
                };

                sim.client("pipeline", async move {
                    let lines = generate_json_lines(scenario.lines);
                    let input = ChannelInputSource::new("scenario", scenario.source_id, lines);
                    let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
                    pipeline.set_pool_drain_timeout(scenario.pool_drain_timeout);
                    pipeline.set_batch_timeout(Duration::from_millis(scenario.batch_timeout_ms));
                    if let Some(batch_target_bytes) = scenario.batch_target_bytes {
                        pipeline.set_batch_target_bytes(batch_target_bytes);
                    }

                    let mut pipeline = pipeline.with_input("scenario", Box::new(input));

                    if let Some(interval) = scenario.checkpoint_flush_interval {
                        pipeline.set_checkpoint_flush_interval(interval);
                    }

                    if let Some((store, handle)) = maybe_checkpoint {
                        pipeline = pipeline.with_checkpoint_store(Box::new(store));
                        if let Some(crash_after) = scenario.arm_checkpoint_crash_after {
                            tokio::spawn(async move {
                                tokio::time::sleep(crash_after).await;
                                handle.arm_crash();
                            });
                        }
                    }

                    let shutdown = CancellationToken::new();
                    let sd = shutdown.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(scenario.shutdown_after).await;
                        sd.cancel();
                    });

                    let run_result = pipeline.run_async(&shutdown).await;
                    run_result?;
                    Ok(())
                });
            }
        }

        let mut network_faults = self.network_faults.clone();
        network_faults.sort_by_key(|f| f.step);

        let mut applied_network_fault_steps = Vec::new();
        let run_outcome = catch_unwind(AssertUnwindSafe(|| {
            let mut faults = network_faults.iter().peekable();
            let mut step = 0usize;

            while let Some(fault) = faults.peek() {
                if fault.step != 0 {
                    break;
                }
                applied_network_fault_steps.push(fault.step);
                apply_network_fault(&mut sim, fault);
                faults.next();
            }

            loop {
                if sim.step()? {
                    break Ok::<(), Box<dyn std::error::Error>>(());
                }
                super::maybe_trace_sim_step(&scenario_name, step + 1);
                drain_runtime_events(
                    &mut runtime_events_barrier,
                    &mut trace_events,
                    &mut runtime_events,
                    &mut runtime_event_ts,
                );
                step += 1;
                while let Some(fault) = faults.peek() {
                    if fault.step != step {
                        break;
                    }
                    applied_network_fault_steps.push(fault.step);
                    apply_network_fault(&mut sim, fault);
                    faults.next();
                }
            }
        }));
        drain_runtime_events(
            &mut runtime_events_barrier,
            &mut trace_events,
            &mut runtime_events,
            &mut runtime_event_ts,
        );

        let (panicked, sim_error) = match run_outcome {
            Ok(Ok(())) => (false, None),
            Ok(Err(err)) => (false, Some(err.to_string())),
            Err(_) => (true, None),
        };
        let trace_validation_error = TransitionValidator::default().validate(&trace_events).err();
        let normalized_trace = normalized_contract_trace(&trace_events);

        let outcome = TestOutcome {
            scenario_name,
            seed,
            delivered_rows: delivered_counter.load(Ordering::Relaxed),
            send_calls: call_counter.load(Ordering::Relaxed),
            panicked,
            sim_error,
            applied_network_fault_steps,
            checkpoint: checkpoint_handle,
            tcp_server: tcp_server_handle,
            trace_events,
            runtime_events,
            trace_validation_error,
            normalized_trace,
        };
        if let Some(typed_contract) = self.typed_contract {
            typed_contract.verify(&outcome);
        }
        outcome
    }
}

fn drain_runtime_events(
    barrier: &mut Barrier<RuntimeBarrierEvent>,
    events: &mut Vec<TraceEvent>,
    runtime_events: &mut Vec<RuntimeEventRecord>,
    runtime_event_ts: &mut u64,
) {
    loop {
        let mut wait = Box::pin(barrier.wait());
        match wait.as_mut().now_or_never() {
            Some(Some(triggered)) => {
                *runtime_event_ts += 1;
                runtime_events.push(RuntimeEventRecord {
                    ts: *runtime_event_ts,
                    event: (*triggered).clone(),
                });
                events.extend(trace_event_from_runtime_barrier(&triggered));
            }
            Some(None) | None => break,
        }
    }
}

fn apply_network_fault(sim: &mut turmoil::Sim<'_>, fault: &NetworkFault) {
    match fault.action {
        NetworkFaultAction::Partition => sim.partition("pipeline", "server"),
        NetworkFaultAction::Repair => sim.repair("pipeline", "server"),
        NetworkFaultAction::PartitionOneWay => sim.partition_oneway("pipeline", "server"),
        NetworkFaultAction::RepairOneWay => sim.repair_oneway("pipeline", "server"),
        NetworkFaultAction::Hold => sim.hold("pipeline", "server"),
        NetworkFaultAction::Release => sim.release("pipeline", "server"),
    }
}

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

/// Captured results from a single fault-scenario run.
///
/// The outcome stores the runtime effects observed by the harness so tests
/// can make post-run assertions about delivery counts, checkpoint behavior,
/// panic paths, and TCP server activity.
pub struct TestOutcome {
    scenario_name: String,
    seed: u64,
    delivered_rows: u64,
    send_calls: u64,
    panicked: bool,
    sim_error: Option<String>,
    applied_network_fault_steps: Vec<usize>,
    checkpoint: Option<CheckpointHandle>,
    tcp_server: Option<TcpServerHandle>,
    trace_events: Vec<TraceEvent>,
    runtime_events: Vec<RuntimeEventRecord>,
    trace_validation_error: Option<String>,
    normalized_trace: Vec<String>,
}

impl TestOutcome {
    /// Return the number of rows the sink ultimately delivered.
    pub fn delivered_rows(&self) -> u64 {
        self.delivered_rows
    }

    /// Return the number of sink send attempts made by the scenario.
    pub fn send_calls(&self) -> u64 {
        self.send_calls
    }

    /// Return whether the turmoil simulation panicked.
    pub fn panicked(&self) -> bool {
        self.panicked
    }

    /// Return the simulation error, if any.
    pub fn sim_error(&self) -> Option<&str> {
        self.sim_error.as_deref()
    }

    /// Return the checkpoint handle for post-run assertions, if one exists.
    pub fn checkpoint(&self) -> Option<&CheckpointHandle> {
        self.checkpoint.as_ref()
    }

    /// Return the TCP server row count captured by the scenario, if any.
    pub fn server_received(&self) -> Option<u64> {
        self.tcp_server
            .as_ref()
            .map(|h| h.received_lines.load(Ordering::Relaxed))
    }

    /// Return the TCP server connection count captured by the scenario, if any.
    pub fn server_connections(&self) -> Option<u64> {
        self.tcp_server
            .as_ref()
            .map(|h| h.connection_count.load(Ordering::Relaxed))
    }

    /// Return a replay hint for reproducing the same turmoil seed.
    pub fn replay_hint(&self) -> String {
        format!(
            "replay with TURMOIL_SEED={} cargo test -p ffwd --features turmoil --test turmoil_sim",
            self.seed
        )
    }

    /// Normalized deterministic contract trace for replay comparison.
    pub fn normalized_contract_trace(&self) -> &[String] {
        &self.normalized_trace
    }

    /// Raw trace events captured during the scenario run.
    pub fn trace_events(&self) -> &[TraceEvent] {
        &self.trace_events
    }

    /// Raw runtime barrier events captured during the scenario run.
    pub fn runtime_events(&self) -> &[RuntimeEventRecord] {
        &self.runtime_events
    }
}

#[derive(Clone, Debug)]
enum Invariant {
    NoSimError,
    DeliveredEq(u64),
    CallsGe(u64),
    CheckpointMonotonic {
        source_id: u64,
    },
    CheckpointCrashCountGe(u64),
    CheckpointFlushCountGe(u64),
    CheckpointUpdatesGe {
        source_id: u64,
        min: usize,
    },
    ServerReceivedGe(u64),
    ServerConnectionsGe(u64),
    CheckpointDurableEq {
        source_id: u64,
        expected: Option<u64>,
    },
    CheckpointDurableAbsent {
        source_id: u64,
    },
    CheckpointDurableNotAheadOfUpdates {
        source_id: u64,
    },
    TraceContractValid,
}

/// Builder-style collection of invariants to assert against a `TestOutcome`.
///
/// Each invariant is evaluated after the scenario completes; combine the
/// builder methods to express the postconditions a particular fault run must
/// satisfy.
#[derive(Clone, Debug, Default)]
pub struct InvariantSet {
    invariants: Vec<Invariant>,
}

impl InvariantSet {
    /// Create an empty invariant set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Require that the scenario completes without a simulation error.
    pub fn no_sim_error(mut self) -> Self {
        self.invariants.push(Invariant::NoSimError);
        self
    }

    /// Require an exact delivered-row count.
    pub fn delivered_eq(mut self, expected: u64) -> Self {
        self.invariants.push(Invariant::DeliveredEq(expected));
        self
    }

    /// Require at least the given number of send attempts.
    pub fn calls_ge(mut self, minimum: u64) -> Self {
        self.invariants.push(Invariant::CallsGe(minimum));
        self
    }

    /// Require monotonic checkpoint progress for a source.
    pub fn checkpoint_monotonic(mut self, source_id: u64) -> Self {
        self.invariants
            .push(Invariant::CheckpointMonotonic { source_id });
        self
    }

    /// Require that at least one checkpoint flush crash occurred.
    pub fn checkpoint_crash_count_ge(mut self, min: u64) -> Self {
        self.invariants.push(Invariant::CheckpointCrashCountGe(min));
        self
    }

    /// Require that at least `min` checkpoint flushes succeeded.
    pub fn checkpoint_flush_count_ge(mut self, min: u64) -> Self {
        self.invariants.push(Invariant::CheckpointFlushCountGe(min));
        self
    }

    /// Require a minimum number of checkpoint updates for a source.
    pub fn checkpoint_updates_ge(mut self, source_id: u64, min: usize) -> Self {
        self.invariants
            .push(Invariant::CheckpointUpdatesGe { source_id, min });
        self
    }

    /// Require at least the given number of TCP rows received by the server.
    pub fn server_received_ge(mut self, minimum: u64) -> Self {
        self.invariants.push(Invariant::ServerReceivedGe(minimum));
        self
    }

    /// Require at least the given number of TCP connections.
    pub fn server_connections_ge(mut self, minimum: u64) -> Self {
        self.invariants
            .push(Invariant::ServerConnectionsGe(minimum));
        self
    }

    /// Require an exact durable checkpoint value (or no checkpoint).
    pub fn checkpoint_durable_eq(mut self, source_id: u64, expected: Option<u64>) -> Self {
        self.invariants.push(Invariant::CheckpointDurableEq {
            source_id,
            expected,
        });
        self
    }

    /// Require that durable checkpoint is absent for a source.
    pub fn checkpoint_durable_absent(mut self, source_id: u64) -> Self {
        self.invariants
            .push(Invariant::CheckpointDurableAbsent { source_id });
        self
    }

    /// Require durable checkpoint not to exceed observed checkpoint updates.
    pub fn checkpoint_durable_not_ahead_of_updates(mut self, source_id: u64) -> Self {
        self.invariants
            .push(Invariant::CheckpointDurableNotAheadOfUpdates { source_id });
        self
    }

    /// Require the transition-contract validator to accept trace output.
    pub fn trace_contract_valid(mut self) -> Self {
        self.invariants.push(Invariant::TraceContractValid);
        self
    }

    /// Verify every configured invariant against the captured outcome.
    pub fn verify(&self, outcome: &TestOutcome) {
        for invariant in &self.invariants {
            match invariant {
                Invariant::NoSimError => {
                    assert!(
                        outcome.sim_error().is_none(),
                        "scenario '{}' failed: sim error {:?} ({})",
                        outcome.scenario_name,
                        outcome.sim_error(),
                        outcome.replay_hint()
                    );
                    assert!(
                        !outcome.panicked(),
                        "scenario '{}' failed: panic observed ({})",
                        outcome.scenario_name,
                        outcome.replay_hint()
                    );
                }
                Invariant::DeliveredEq(expected) => {
                    assert_eq!(
                        outcome.delivered_rows(),
                        *expected,
                        "scenario '{}' delivered_rows mismatch ({})",
                        outcome.scenario_name,
                        outcome.replay_hint()
                    );
                }
                Invariant::CallsGe(minimum) => {
                    assert!(
                        outcome.send_calls() >= *minimum,
                        "scenario '{}' expected send_calls >= {}, got {} ({})",
                        outcome.scenario_name,
                        minimum,
                        outcome.send_calls(),
                        outcome.replay_hint()
                    );
                }
                Invariant::CheckpointMonotonic { source_id } => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    checkpoint.assert_monotonic(*source_id);
                }
                Invariant::CheckpointCrashCountGe(minimum) => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    assert!(
                        checkpoint.crash_count() >= *minimum,
                        "scenario '{}' expected crash_count >= {}, got {} ({})",
                        outcome.scenario_name,
                        minimum,
                        checkpoint.crash_count(),
                        outcome.replay_hint()
                    );
                }
                Invariant::CheckpointFlushCountGe(minimum) => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    assert!(
                        checkpoint.flush_count() >= *minimum,
                        "scenario '{}' expected flush_count >= {}, got {} ({})",
                        outcome.scenario_name,
                        minimum,
                        checkpoint.flush_count(),
                        outcome.replay_hint()
                    );
                }
                Invariant::CheckpointUpdatesGe { source_id, min } => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    assert!(
                        checkpoint.update_count(*source_id) >= *min,
                        "scenario '{}' expected checkpoint updates for source {} >= {}, got {} ({})",
                        outcome.scenario_name,
                        source_id,
                        min,
                        checkpoint.update_count(*source_id),
                        outcome.replay_hint()
                    );
                }
                Invariant::ServerReceivedGe(minimum) => {
                    let received = outcome
                        .server_received()
                        .expect("server invariant requested without server handle");
                    assert!(
                        received >= *minimum,
                        "scenario '{}' expected server_received >= {}, got {} ({})",
                        outcome.scenario_name,
                        minimum,
                        received,
                        outcome.replay_hint()
                    );
                }
                Invariant::ServerConnectionsGe(minimum) => {
                    let connections = outcome
                        .server_connections()
                        .expect("server invariant requested without server handle");
                    assert!(
                        connections >= *minimum,
                        "scenario '{}' expected server_connections >= {}, got {} ({})",
                        outcome.scenario_name,
                        minimum,
                        connections,
                        outcome.replay_hint()
                    );
                }
                Invariant::CheckpointDurableEq {
                    source_id,
                    expected,
                } => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    let durable = checkpoint.durable_offset(*source_id);
                    assert_eq!(
                        durable,
                        *expected,
                        "scenario '{}' expected durable checkpoint {:?} for source {}, got {:?} ({})",
                        outcome.scenario_name,
                        expected,
                        source_id,
                        durable,
                        outcome.replay_hint()
                    );
                }
                Invariant::CheckpointDurableAbsent { source_id } => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    let durable = checkpoint.durable_offset(*source_id);
                    assert_eq!(
                        durable,
                        None,
                        "scenario '{}' expected absent durable checkpoint for source {}, got {:?} ({})",
                        outcome.scenario_name,
                        source_id,
                        durable,
                        outcome.replay_hint()
                    );
                }
                Invariant::CheckpointDurableNotAheadOfUpdates { source_id } => {
                    let checkpoint = outcome
                        .checkpoint()
                        .expect("checkpoint invariant requested without checkpoint handle");
                    checkpoint.assert_durable_not_ahead_of_updates(*source_id);
                }
                Invariant::TraceContractValid => {
                    if let Some(err) = &outcome.trace_validation_error {
                        panic!(
                            "scenario '{}' produced invalid transition trace: {err} ({})",
                            outcome.scenario_name,
                            outcome.replay_hint()
                        );
                    }
                }
            }
        }
    }
}

/// Prototype for Shape A: typed phase + invariant bundle contract.
#[derive(Clone, Debug)]
pub struct TypedInvariantBundle {
    requires_trace_contract: bool,
}

impl TypedInvariantBundle {
    /// Build a minimal typed contract prototype that requires valid traces.
    pub fn trace_contract() -> Self {
        Self {
            requires_trace_contract: true,
        }
    }

    fn verify(&self, outcome: &TestOutcome) {
        if self.requires_trace_contract {
            assert!(
                outcome.trace_validation_error.is_none(),
                "typed invariant bundle rejected outcome '{}': {:?}",
                outcome.scenario_name,
                outcome.trace_validation_error
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{FaultScenario, InvariantSet, NetworkFault, NetworkFaultAction};

    #[test]
    fn late_faults_are_not_injected_after_sim_completion() {
        let outcome = FaultScenario::builder("late-faults-skip-after-completion")
            .with_seed(20260416)
            .with_turmoil_tcp_sink()
            .with_line_count(0)
            .with_shutdown_after(Duration::from_millis(5))
            .with_network_fault(NetworkFault::at_step(0, NetworkFaultAction::Partition))
            .with_network_fault(NetworkFault::at_step(10_000, NetworkFaultAction::Repair))
            .run();

        InvariantSet::new().no_sim_error().verify(&outcome);
        assert_eq!(
            outcome.applied_network_fault_steps,
            vec![0],
            "late faults must not be injected after Turmoil reports completion"
        );
    }
}
