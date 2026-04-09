# Fault Injection Framework — Design


## Problem

logfwd must recover from arbitrary failures at every layer — input, pipeline,
and output — without data loss (at-least-once delivery). Today we have ad-hoc
turmoil simulation tests that cover specific scenarios, but no systematic
framework for composing faults, verifying recovery invariants, or running
randomized fault campaigns.

We need a framework that can:
1. Inject arbitrary faults at input, output, and pipeline worker layers
2. Compose multiple faults into realistic failure scenarios
3. Verify recovery invariants deterministically
4. Run randomized fault campaigns for nightly CI

## Design Principles

Drawn from FoundationDB's simulation testing, TiKV's `fail-rs`, Netflix chaos
engineering, and our own turmoil experience:

1. **Deterministic replay** — every test uses seeded RNG via turmoil; failing
   seeds become permanent regression tests
2. **Composable faults** — faults are data (enums), not code; they compose via
   scenario builders
3. **Separation of injection from verification** — faults are injected via
   instrumented components; invariants are checked by independent observers
4. **Zero production cost** — all fault infrastructure lives behind
   `#[cfg(feature = "turmoil")]` or in test crates
5. **Steady-state hypothesis** — every test defines what "correct" looks like
   before injecting faults (Netflix chaos engineering principle)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Test Harness                          │
│  FaultScenario::builder()                               │
│    .input_fault(InputFault::Corrupt { ... })             │
│    .output_fault(OutputFault::Intermittent { ... })      │
│    .pipeline_fault(PipelineFault::SlowScan { ... })      │
│    .invariants(Invariants::at_least_once())              │
│    .build()                                              │
└────────────┬──────────────┬──────────────┬──────────────┘
             │              │              │
    ┌────────▼───────┐ ┌───▼────────┐ ┌───▼─────────────┐
    │ FaultInput     │ │ FaultSink  │ │ FaultCheckpoint  │
    │ (InputSource)  │ │ (Sink)     │ │ (CheckpointStore)│
    └────────┬───────┘ └───┬────────┘ └───┬─────────────┘
             │              │              │
    ┌────────▼──────────────▼──────────────▼──────────────┐
    │              Pipeline::for_simulation                │
    │    (real pipeline loop, real state machine)           │
    └────────────────────────┬────────────────────────────┘
                             │
    ┌────────────────────────▼────────────────────────────┐
    │           InvariantVerifier (post-test)              │
    │  - delivery completeness                             │
    │  - checkpoint monotonicity                           │
    │  - ordered ACK correctness                           │
    │  - shutdown completeness                             │
    │  - no checkpoint regression                          │
    └─────────────────────────────────────────────────────┘
```

## Fault Taxonomy

### 1. Input Faults (`InputFault`)

Faults injected via a `FaultInputSource` wrapper around `ChannelInputSource`.

| Fault | Description | Injection point |
|-------|-------------|-----------------|
| `Corrupt` | Inject invalid JSON, partial lines, binary garbage | Mutates bytes in `poll()` return |
| `Truncate` | Emit `InputEvent::Truncated` mid-stream | Inserts event after N polls |
| `Rotate` | Emit `InputEvent::Rotated` mid-stream | Inserts event after N polls |
| `PollError` | Return `io::Error` from `poll()` | Replaces poll result |
| `SlowPoll` | Add delay to `poll()` (simulates NFS latency) | Wraps poll with sleep |
| `Stall` | Stop returning data for a duration | Suppresses Data events |
| `BackpressureSpike` | Emit large chunks to fill bounded channel | Increases chunk size |
| `PermissionDenied` | Return EACCES from `poll()` | Specific io::ErrorKind |
| `SourcePanic` | Panic inside `poll()` | Panic after N calls |

```rust
#[derive(Clone, Debug)]
pub enum InputFault {
    /// Corrupt N bytes at random positions in the Kth poll result.
    Corrupt { after_polls: usize, corrupt_bytes: usize },
    /// Emit a Truncated event after N polls.
    Truncate { after_polls: usize },
    /// Emit a Rotated event after N polls.
    Rotate { after_polls: usize },
    /// Return io::Error from poll() at call N.
    PollError { at_poll: usize, kind: io::ErrorKind },
    /// Delay poll() by the given duration starting at call N.
    SlowPoll { after_polls: usize, delay: Duration },
    /// Stop returning data for the given duration after N polls.
    Stall { after_polls: usize, duration: Duration },
    /// Emit oversized chunks to pressure bounded channels.
    BackpressureSpike { after_polls: usize, chunk_bytes: usize },
    /// Return EACCES from poll() after N polls.
    PermissionDenied { after_polls: usize },
    /// Panic at poll N.
    SourcePanic { at_poll: usize },
}
```

### 2. Output Faults (`OutputFault`)

Faults injected via `InstrumentedSink` (already exists) extended with new
modes.

Existing `FailureAction` modes (`IoError`, `RetryAfter`, `Reject`, `Delay`,
`Panic`) remain unchanged. `OutputFault` adds the following modes:

| Fault | Description | How injected |
|-------|-------------|--------------|
| `Intermittent` | **New:** probabilistic failure with configurable rate | RNG-driven per call |
| `CorrelatedFailure` | **New:** all workers fail simultaneously for a window | Shared failure flag |
| `PartialAccept` | **New:** accept some rows, reject rest | Split batch mid-send |
| `SlowFlush` | **New:** flush() takes a long time | Delay in flush impl |
| `ShutdownHang` | **New:** shutdown() blocks indefinitely | Never-completing future |
| `ConnectionCycle` | **New:** succeed N times, fail M times, repeat | Cyclic script |

```rust
/// New failure actions extending the existing FailureAction enum.
#[derive(Clone, Debug)]
pub enum OutputFault {
    /// Probabilistic failure: fail with the given io::ErrorKind
    /// at the specified rate (0.0 = never, 1.0 = always).
    Intermittent { rate: f64, kind: io::ErrorKind },
    /// All sinks created by the factory fail simultaneously for
    /// the given duration when triggered.
    CorrelatedFailure { duration: Duration },
    /// Cycle: succeed for N calls, then fail for M calls, repeat.
    ConnectionCycle { succeed_count: usize, fail_count: usize, kind: io::ErrorKind },
    /// Accept only the first N rows in a batch; reject the rest.
    PartialAccept { accept_rows: usize },
    /// flush() delays for the given duration.
    SlowFlush { delay: Duration },
    /// shutdown() never completes (tests drain timeout).
    ShutdownHang,
}
```

### 3. Pipeline Faults (`PipelineFault`)

Faults that affect the pipeline loop itself. These are harder to inject
because the pipeline loop is internal. Two approaches:

**Approach A: Turmoil-native** (preferred where possible) — use turmoil's
network simulation to create conditions that stress the pipeline (partitions,
holds, latency spikes).

**Approach B: Instrumented wrappers** — wrap internal components with
fault-injecting delegates.

| Fault | Description | Approach |
|-------|-------------|----------|
| `NetworkPartition` | Block traffic between hosts | Turmoil `partition/repair` |
| `NetworkHold` | Buffer traffic, release as burst | Turmoil `hold/release` |
| `LatencySpike` | Increase link latency temporarily | Turmoil `set_link_latency` |
| `ServerCrash` | Kill and restart server host | Turmoil `crash/bounce` |
| `SlowScan` | Scanner takes longer than expected | Injected delay in scan wrapper |
| `TransformError` | SQL transform returns error | Error-returning transform |
| `CheckpointCorrupt` | Checkpoint data is garbled on load | Wrapper around CheckpointStore |
| `CheckpointFlushFail` | flush() fails (disk full) | Already exists in ObservableCheckpointStore |
| `ChannelClose` | Internal channel drops unexpectedly | Cancel token on timer |

```rust
#[derive(Clone, Debug)]
pub enum PipelineFault {
    /// Partition two turmoil hosts for a duration.
    NetworkPartition { from: String, to: String, duration: Duration, at: Duration },
    /// Hold traffic, then release.
    NetworkHold { from: String, to: String, hold_duration: Duration, at: Duration },
    /// Spike link latency.
    LatencySpike { from: String, to: String, latency: Duration, duration: Duration, at: Duration },
    /// Crash and bounce a server host.
    ServerCrash { host: String, at: Duration, restart_after: Duration },
    /// Inject scan delay in the scanner adapter.
    SlowScan { delay: Duration, at_batch: usize },
    /// Return an error from transform execution.
    TransformError { at_batch: usize, message: String },
    /// Checkpoint flush failure at the Nth flush.
    CheckpointFlushFail { at_flush: usize },
    /// Corrupt loaded checkpoint data.
    CheckpointCorrupt,
    /// Force-close an internal channel at a scheduled time.
    ChannelClose { at: Duration },
}
```

### 4. Compound Faults (`FaultScenario`)

The real power is combining faults. A `FaultScenario` is a collection of
faults with a timeline:

```rust
pub struct FaultScenario {
    /// Faults applied to the input source.
    pub input_faults: Vec<InputFault>,
    /// Per-worker output failure scripts.
    pub output_faults: Vec<OutputFault>,
    /// FailureAction scripts for the InstrumentedSink (existing mechanism).
    pub output_scripts: Vec<Vec<FailureAction>>,
    /// Number of output workers.
    pub worker_count: usize,
    /// Pipeline/network faults applied during simulation.
    pub pipeline_faults: Vec<PipelineFault>,
    /// Input data configuration.
    pub input_lines: usize,
    /// Pipeline tuning.
    pub batch_timeout: Duration,
    pub batch_target_bytes: Option<usize>,
    pub checkpoint_flush_interval: Duration,
    /// Simulation parameters.
    pub sim_duration: Duration,
    pub shutdown_after: Duration,
    /// Invariants to verify after the test.
    pub invariants: InvariantSet,
}
```

## Invariant Verification

Post-test verification is the core of the framework. Every test defines a set
of invariants that must hold after the simulation completes:

### `InvariantSet`

```rust
pub struct InvariantSet {
    /// Every input line identity (offset/hash) must appear in sink output
    /// at least once. Counts alone are insufficient.
    pub at_least_once_delivery: bool,
    /// Durable checkpoint offset must never decrease per source.
    pub checkpoint_monotonicity: bool,
    /// Checkpoints must not advance past unacknowledged batches.
    pub ordered_ack: bool,
    /// Pipeline must reach Stopped state without hanging.
    pub shutdown_completeness: bool,
    /// Retry-exhausted/timed-out (transient) batches must not advance
    /// checkpoints; durable offset stays at the pre-batch value.
    pub hold_on_transient_failure: bool,
    /// Rejected batches DO advance checkpoints (accept data loss for
    /// permanent rejects to avoid infinite replay loops).
    pub reject_advances_checkpoint: bool,
    /// Custom assertion function.
    pub custom: Vec<Box<dyn Fn(&TestOutcome) -> Result<(), String>>>,
}

impl InvariantSet {
    /// Standard invariants for at-least-once delivery.
    pub fn at_least_once() -> Self {
        Self {
            at_least_once_delivery: true,
            checkpoint_monotonicity: true,
            ordered_ack: true,
            shutdown_completeness: true,
            hold_on_transient_failure: true,
            reject_advances_checkpoint: true,
            custom: vec![],
        }
    }

    /// Relaxed invariants for scenarios where data loss is acceptable
    /// (e.g., testing crash recovery, not full delivery).
    pub fn crash_recovery() -> Self {
        Self {
            at_least_once_delivery: false,   // some data may be lost in crash window
            checkpoint_monotonicity: true,
            ordered_ack: true,
            shutdown_completeness: true,
            hold_on_transient_failure: true,
            reject_advances_checkpoint: true,
            custom: vec![],
        }
    }
}
```

### `TestOutcome` — collected by observers during the test

```rust
pub struct TestOutcome {
    /// Total input lines generated.
    pub input_lines: usize,
    /// Stable record identities generated by the source (offset/hash/id).
    pub input_ids: HashSet<RecordId>,
    /// Lines delivered to sink (from delivered_counter).
    pub delivered_lines: u64,
    /// Stable record identities observed at sink output.
    pub delivered_ids: HashSet<RecordId>,
    /// Lines rejected by sink.
    pub rejected_lines: u64,
    /// Per-source checkpoint history (from ObservableCheckpointStore).
    pub checkpoint_handle: CheckpointHandle,
    /// Total send_batch calls.
    pub send_calls: u64,
    /// Whether the pipeline completed without hanging.
    pub completed: bool,
    /// Whether force_stop was used.
    pub force_stopped: bool,
    /// Turmoil seed used.
    pub seed: u64,
}
```

### Verification Logic

```rust
impl InvariantSet {
    pub fn verify(&self, outcome: &TestOutcome) -> Result<(), Vec<String>> {
        let mut failures = vec![];

        if self.at_least_once_delivery {
            if !outcome.delivered_ids.is_superset(&outcome.input_ids) {
                failures.push(format!(
                    "at-least-once violation: missing {} input ids",
                    outcome.input_ids.difference(&outcome.delivered_ids).count()
                ));
            }
        }

        if self.checkpoint_monotonicity {
            // Check all sources for monotonicity via checkpoint_handle
            // (uses existing assert_monotonic logic but collects errors
            // instead of panicking)
        }

        if self.shutdown_completeness && !outcome.completed {
            failures.push("shutdown did not complete".to_string());
        }

        for custom in &self.custom {
            if let Err(msg) = custom(outcome) {
                failures.push(msg);
            }
        }

        if failures.is_empty() { Ok(()) } else { Err(failures) }
    }
}
```

## Scenario Builder API

```rust
let scenario = FaultScenario::builder()
    .input_lines(100)
    .worker_count(2)
    .batch_timeout(Duration::from_millis(20))
    // Inject a corrupt chunk after 10 polls
    .input_fault(InputFault::Corrupt { after_polls: 10, corrupt_bytes: 5 })
    // Output: first worker gets intermittent 10% failures
    .output_fault(OutputFault::Intermittent { rate: 0.1, kind: io::ErrorKind::ConnectionReset })
    // Network partition at T+2s for 1s
    .pipeline_fault(PipelineFault::NetworkPartition {
        from: "pipeline".into(),
        to: "server".into(),
        duration: Duration::from_secs(1),
        at: Duration::from_secs(2),
    })
    // Checkpoint crash at 3rd flush
    .pipeline_fault(PipelineFault::CheckpointFlushFail { at_flush: 3 })
    // Verify recovery
    .invariants(InvariantSet::at_least_once())
    .sim_duration(Duration::from_secs(60))
    .shutdown_after(Duration::from_secs(30))
    .build();

scenario.run_with_seed(42).unwrap();
```

## Randomized Fault Campaigns

For nightly CI, generate random fault scenarios using proptest or seeded RNG:

```rust
/// Generate a random fault scenario from a seed.
pub fn random_scenario(seed: u64) -> FaultScenario {
    let mut rng = StdRng::seed_from_u64(seed);

    let mut builder = FaultScenario::builder()
        .input_lines(rng.gen_range(10..500))
        .worker_count(rng.gen_range(1..4))
        .batch_timeout(Duration::from_millis(rng.gen_range(10..200)));

    // Random input faults (0-3)
    let n_input = rng.gen_range(0..4);
    for _ in 0..n_input {
        builder = builder.input_fault(random_input_fault(&mut rng));
    }

    // Random output faults (0-3)
    let n_output = rng.gen_range(0..4);
    for _ in 0..n_output {
        builder = builder.output_fault(random_output_fault(&mut rng));
    }

    // Random pipeline faults (0-2)
    let n_pipeline = rng.gen_range(0..3);
    for _ in 0..n_pipeline {
        builder = builder.pipeline_fault(random_pipeline_fault(&mut rng));
    }

    builder
        .invariants(InvariantSet::crash_recovery())
        .sim_duration(Duration::from_secs(120))
        .shutdown_after(Duration::from_secs(60))
        .build()
}

/// Nightly CI: run N random seeds, report all failures.
#[test]
#[ignore = "nightly CI only; run with -- --ignored"]
fn nightly_fault_campaign() {
    let base_seed: u64 = std::env::var("CAMPAIGN_SEED")
        .ok().and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let n: usize = std::env::var("CAMPAIGN_RUNS")
        .ok().and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let mut failures = vec![];
    for i in 0..n {
        let seed = base_seed.wrapping_add(i as u64);
        let scenario = random_scenario(seed);
        if let Err(errs) = scenario.run_with_seed(seed) {
            failures.push((seed, errs));
        }
    }

    if !failures.is_empty() {
        for (seed, errs) in &failures {
            eprintln!("SEED {seed} FAILED:");
            for e in errs { eprintln!("  - {e}"); }
        }
        panic!("{} of {n} seeds failed", failures.len());
    }
}
```

## Implementation Plan

### Phase 1: Core Framework (follow-up implementation PR)

This design PR is documentation-only; no runtime or test code is added here.

Planned new files in `crates/logfwd/tests/turmoil_sim/`:

| File | Contents |
|------|----------|
| `fault_types.rs` | `InputFault`, `OutputFault`, `PipelineFault` enums |
| `fault_input.rs` | `FaultInputSource` wrapping `ChannelInputSource` |
| `fault_sink.rs` | Extended `InstrumentedSink` with `Intermittent`, `ConnectionCycle`, `CorrelatedFailure` |
| `invariants.rs` | `InvariantSet`, `TestOutcome`, verification logic |
| `scenario.rs` | `FaultScenario` builder, `run_with_seed()` |
| `random_faults.rs` | `random_scenario()`, `random_input_fault()`, etc. |

Modifications:

| File | Change |
|------|--------|
| `main.rs` | Add `mod fault_types; mod fault_input; ...` |
| `instrumented_sink.rs` | Add `Intermittent`, `ConnectionCycle` to `FailureAction` |
| `observable_checkpoint.rs` | Add `at_flush` crash trigger, `corrupt_on_load` flag |

Documentation updates required in the same implementation PR:

- `dev-docs/VERIFICATION.md` — describe where scenario-based fault injection fits relative to TLA+, Kani, and proptest
- `dev-docs/CHANGE_MAP.md` — add cross-links for fault-injection changes that affect invariants and simulation coverage
- `dev-docs/ARCHITECTURE.md` — document new simulation harness seams if pipeline orchestration hooks are introduced

### Phase 2: Named Scenarios (follow-up)

Pre-built scenarios for common failure patterns:

```rust
impl FaultScenario {
    /// Network blip: 500ms partition during active sending.
    pub fn network_blip() -> Self { ... }

    /// Flaky sink: 10% failure rate with retry recovery.
    pub fn flaky_sink() -> Self { ... }

    /// Disk full: checkpoint flush fails mid-run.
    pub fn disk_full() -> Self { ... }

    /// Thundering herd: all workers fail then recover simultaneously.
    pub fn thundering_herd() -> Self { ... }

    /// Cascading failure: input corrupt + output retry + checkpoint crash.
    pub fn cascading_failure() -> Self { ... }

    /// Long partition: server down longer than retry window.
    pub fn long_partition() -> Self { ... }

    /// Split brain: asymmetric partition (pipeline→server broken, server→pipeline ok).
    pub fn split_brain() -> Self { ... }

    /// Slow consumer: sink delay exceeds batch timeout.
    pub fn slow_consumer() -> Self { ... }
}
```

### Phase 3: Nightly Campaign (follow-up)

- `random_faults.rs` with randomized scenario generation
- CI workflow addition in `.github/workflows/nightly-testing.yml`
- Seed database for regression tests (failing seeds become `#[test]` cases)

### Phase 4: Input Layer Faults (follow-up)

Requires `FaultInputSource` that wraps the real `InputSource` trait:
- Intercepts `poll()` results and applies `InputFault` schedule
- Needs integration with turmoil's simulated time for `SlowPoll` and `Stall`

## Relationship to Existing Infrastructure

The framework **extends, not replaces** the existing turmoil_sim tests:

- `InstrumentedSink` gains new `FailureAction` variants
- `ObservableCheckpointStore` gains new crash/corruption modes
- `ChannelInputSource` gets wrapped by `FaultInputSource`
- Existing tests (`network_sim`, `crash_sim`, `bug_hunt`) remain unchanged
- New `FaultScenario`-based tests live alongside them

## Invariants Reference

| Invariant | Description | TLA+ coverage | Runtime oracle |
|-----------|-------------|---------------|---------------|
| **ALO** | At-least-once: every input record identity eventually delivered | Partial (protocol-level ordering/liveness only) | `delivered_ids ⊇ input_ids` |
| **CM** | Checkpoint monotonicity: offsets never decrease | Yes (`tla/PipelineMachine.tla`) | `assert_monotonic(source_id)` |
| **OA** | Ordered ACK: checkpoint doesn't skip ahead | Yes (`tla/PipelineMachine.tla`) | `BatchId` ordering + checkpoint trace checks |
| **SC** | Shutdown completeness: pipeline reaches Stopped | Yes (liveness config) | `sim.run()` returns `Ok` and no forced stop |
| **HTF** | Hold on transient failure: retry-exhausted/timed-out batches do not advance checkpoint | Partial (retry policy details are implementation-level) | `durable_offset == pre_batch_offset` after all transient retries fail |
| **RAC** | Reject advances checkpoint: permanent rejects may advance | No (policy choice, implementation-level) | checkpoint advances only for explicit permanent reject paths |
| **NR** | No regression: durable offset never goes backward across restarts | Partial (restart persistence modeled abstractly) | Cross-run checkpoint comparison |


## Relationship to TLA+ and proptest

The invariant checks in this framework are runtime test oracles, not replacements for formal models:

- **TLA+ remains canonical** for temporal protocol properties (ordering, liveness, eventual drain) in `tla/PipelineMachine.tla`.
- **Fault-injection invariants operationalize those properties** in executable scenarios to catch regressions in wiring, retries, and integration behavior.
- **proptest remains required** for async/stateful permutations and heap-heavy paths where randomized input generation plus shrinking provides better counterexamples than scenario scripts.

Planned split of responsibility:

- Use **TLA+** to define protocol-level invariants and allowed transitions.
- Use **proptest** for component-level stochastic exploration (e.g., scanner/SIMD equivalence, reducer transition sequences).
- Use **fault scenarios** to validate end-to-end recovery under composed infrastructure failures.

Each new fault scenario category should map to at least one existing verification artifact (TLA+ property name or proptest target) in the follow-up implementation PR.

## Future Consideration: `fail-rs` Failpoints

The `fail` crate (tikv/fail-rs) provides compile-time failpoints for injecting
faults at arbitrary internal code points that turmoil cannot reach (checkpoint
writes, scanner internals, config reload). Key properties:

- **Zero production overhead** — `fail_point!` macro expands to nothing when
  the `failpoints` feature is disabled
- **Action DSL** — `"20%3*print(alive)->panic"` means 20% chance to print
  (max 3 times), then always panic
- **Probabilistic/conditional** — `p%` prefix, `cnt*` prefix, boolean guards

**Critical async limitation**: `sleep`/`pause`/`delay` actions use
`std::thread::sleep`, which blocks the tokio runtime thread. In logfwd's async
code paths, **only use `return`-type failpoints** (`fail_point!("name", |_| Err(...))`)
and `panic` failpoints. Never use `sleep`/`pause`/`delay` in async contexts.

**Integration approach** (if adopted in a later phase):
- Add `fail` as optional dep with `failpoints` feature in `logfwd-runtime` and `logfwd-io`
- NOT in `logfwd-core` (which is `no_std` + `forbid(unsafe)`)
- Place at I/O boundaries: checkpoint write, file open, OTLP connect, sink create
- Complements turmoil (network-level) with internal code-path injection
- Global state requires `FailScenario` mutex — failpoint tests cannot run in parallel

## Open Questions

1. **Should `FaultInputSource` live in `logfwd-test-utils` or in the test module?**
   Pro test-utils: reusable across crates. Pro test module: simpler, co-located.
   **Recommendation:** Start in test module, promote to test-utils if reused.

2. **Should randomized campaigns use proptest or custom RNG?**
   proptest gives shrinking but adds complexity. Custom RNG + seed replay is
   simpler and sufficient for scenario-level tests.
   **Recommendation:** Custom RNG with seed replay. proptest for property-level
   tests within individual faults.

3. **How do we handle the known ack-starvation bug?**
   In certain retry-heavy schedules, ACK processing can be delayed long enough
   that checkpoints do not advance until much later in the run (starvation under
   sustained retry pressure). This can produce false negatives if scenarios
   require immediate intermediate checkpoint progress.

   **Temporary test posture:** use `InvariantSet::crash_recovery()` for campaign
   scenarios that intentionally induce prolonged retry pressure.

   **Exit criterion:** once the starvation bug is fixed and covered by regression
   tests, move those campaigns back to `InvariantSet::at_least_once()` and
   require intermediate checkpoint advancement assertions.

4. **Should pipeline faults (network partition, crash) be part of the scenario
   builder or applied via `sim.step()` sequences?**
   Both. The builder handles timed faults. Complex choreographies use manual
   `sim.step()` with the fault enums as documentation.
   **Recommendation:** Builder for common patterns, manual for edge cases.
