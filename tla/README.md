# TLA+ Formal Specifications

Formal models for the logfwd pipeline design. These specs capture
properties that Kani (bounded model checker) cannot express — temporal
logic, liveness, and protocol-level design invariants.

## Contributor Quickstart (CI parity)

Run these TLC commands locally for parity with CI coverage:

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.liveness.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCShutdownProtocol.tla -config tla/ShutdownProtocol.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCShutdownProtocol.tla -config tla/ShutdownProtocol.liveness.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineBatch.tla -config tla/PipelineBatch.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineBatch.tla -config tla/PipelineBatch.liveness.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCDeliveryRetry.tla -config tla/DeliveryRetry.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCDeliveryRetry.tla -config tla/DeliveryRetry.liveness.cfg
```

## PipelineMachine.tla

Models `PipelineMachine<S, C>` from
`crates/logfwd-core/src/pipeline/lifecycle.rs`.

### What it proves

| Property | Type | Description |
|----------|------|-------------|
| `DrainCompleteness` | Safety | `stop()` only reachable when all in_flight batches are resolved |
| `NoHeldWorkAfterStop` | Safety | `Stopped` never leaves non-terminal held work behind |
| `QuiescenceHasNoSilentStrandedWork` | Safety | At `Stopped`, no in-flight batch is left without explicit terminal outcome |
| `NoUnresolvedSentAtQuiescence` | Safety | At `Stopped`, every sent batch is terminalized (`acked`/`rejected`/`abandoned`) |
| `StopMetadataConsistent` | Safety | `forced`/`stop_reason` remain phase-consistent (`Stopped` iff reason is not `none`) |
| `CheckpointOrderingInvariant` | Safety | committed[s]=n implies all sent batches `<= n` are terminalized for commit (`acked` or `rejected`), none in_flight |
| `UnresolvedWorkNotCommittedPast` | Safety | active or held in-flight work cannot be silently committed past |
| `CheckpointNeverAheadOfTerminalizedPrefix` | Safety | committed checkpoint is never ahead of the ack/reject terminalized prefix |
| `CommittedNeverAheadOfCreated` | Safety | committed[s] never exceeds highest created batch ID |
| `NoDoubleComplete` | Safety | batch cannot be both in_flight and any terminal set |
| `InFlightImpliesCreated` | Safety | structural: in_flight ⊆ created |
| `HeldImpliesInFlight` | Safety | structural: non-terminal held work remains in_flight |
| `AckedImpliesCreated` | Safety | structural: acked ⊆ created |
| `CommittedMonotonic` | Safety (temporal) | checkpoint never goes backwards |
| `HeldTransitionsDoNotCommit` | Safety (temporal) | hold/retry/force-stop held-state transitions do not advance checkpoints |
| `ForceStopAbandonsAllInFlight` | Safety (temporal) | force-stop explicitly moves all unresolved in-flight work to `abandoned` |
| `NoCreateAfterDrain` | Safety (temporal) | no new batches after begin_drain |
| `DrainMeansNoNewSending` | Safety (temporal) | in_flight cannot grow once phase ≠ Running |
| `FailureTerminalizationPreservesCheckpoint` | Safety (temporal) | force/crash terminalization does not advance checkpoints |
| `FailureClassMustTerminalizePrototype` | Safety (temporal) | force/crash transition class preserves terminalization completeness |
| `EventualDrain` | Liveness | every started drain eventually reaches Stopped |
| `NoBatchLeftBehind` | Liveness | every in_flight batch eventually terminalizes (ack/reject/abandon) |
| `HeldBatchEventuallyReleased` | Liveness | every non-terminal hold is eventually retried/released |
| `PanickedBatchEventuallyAccountedFor` | Liveness | panic-held work eventually reaches ack/reject/abandon |
| `StoppedIsStable` | Liveness | once Stopped, stays Stopped |
| `AllCreatedBatchesEventuallyAccountedFor` | Liveness | every created batch is committed or machine is Stopped |
| `BeginDrainReachable` | Reachability (invariant ~P) | Draining phase is reachable (vacuity guard) |
| `StopReachable` | Reachability (invariant ~P) | Stopped phase is reachable (vacuity guard) |
| `AckOccurs` | Reachability (invariant ~P) | at least one batch is acked (AckBatch fires) |
| `CheckpointAdvances` | Reachability (invariant ~P) | committed checkpoint advances at least once |
| `ForcedReachable` | Reachability (invariant ~P) | ForceStop path is reachable (vacuity guard) |
| `RejectOccurs` | Reachability (invariant ~P) | Reject path is reachable |
| `HoldOccurs` | Reachability (invariant ~P) | non-terminal hold/failure path is reachable |
| `RetryOccurs` | Reachability (invariant ~P) | held retry/release path is reachable |
| `PanicHoldOccurs` | Reachability (invariant ~P) | panic-driven hold path is reachable |
| `AbandonOccurs` | Reachability (invariant ~P) | ForceStop abandonment path is reachable |
| `CrashReachable` | Reachability (invariant ~P) | panic/unwind-equivalent crash-stop path is reachable |
| `HeldAbandonOccurs` | Reachability (invariant ~P) | ForceStop can explicitly abandon previously held work |

### File structure (two-file pattern)

This spec follows the industry-standard two-file pattern used by etcd-io/raft
(`MCetcdraft.tla`), PingCAP/tla-plus, and Jack Vanlightly's Kafka verification:

```text
tla/
  # Lifecycle state machine (ordered ACK, checkpoint ordering, drain guarantee)
  PipelineMachine.tla           — clean algorithm spec
  MCPipelineMachine.tla         — TLC config: symmetry sets, model constants
  PipelineMachine.cfg           — safety model (~10.8M distinct states locally)
  PipelineMachine.liveness.cfg  — liveness model (smaller constants, no SYMMETRY)
  PipelineMachine.thorough.cfg  — PR-CI thorough safety model (3 sources, 3 batches)
  PipelineMachine.nightly.thorough.cfg — nightly deep safety model (3 sources, 4 batches)
  PipelineMachine.coverage.cfg  — reachability / vacuity guards

  # Shutdown coordination (two-tier I/O+CPU worker drain protocol)
  ShutdownProtocol.tla          — N inputs with I/O+CPU workers, per-input io channels, shared pipeline channel, and pool drain
  MCShutdownProtocol.tla        — TLC config (small capacities: IoChannel=2, Pipeline=3)
  ShutdownProtocol.cfg          — safety model (ordering + conservation invariants)
  ShutdownProtocol.liveness.cfg — liveness model (shutdown completion, no deadlock)
  ShutdownProtocol.coverage.cfg — reachability guards

  # Batching protocol (multi-source, checkpoint merge, reject handling)
  PipelineBatch.tla             — batch accumulation + flush + ack/reject
  MCPipelineBatch.tla           — TLC config
  PipelineBatch.cfg             — safety model
  PipelineBatch.liveness.cfg    — liveness model
  PipelineBatch.coverage.cfg    — reachability guards

  # Delivery retry loop (exponential backoff, batch terminalization liveness)
  DeliveryRetry.tla             — worker retry loop with backoff + cancel
  MCDeliveryRetry.tla           — TLC config
  DeliveryRetry.cfg             — safety model (backoff invariants)
  DeliveryRetry.liveness.cfg    — liveness model (terminal reachable under fairness)
  DeliveryRetry.coverage.cfg    — reachability guards

  README.md                     — this file
```

### Four models to run

**Model 1 — Safety (normal + ForceStop paths):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.cfg
# Sources={"s1","s2"}, MaxBatchesPerSource=3, MaxNonTerminalHolds=1
# ~10.8M distinct states locally with one TLC worker, < 10 min. Checks all
# INVARIANTS + temporal action properties.
```

**Model 2 — Liveness (smaller constants, no SYMMETRY):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.liveness.cfg
# Sources={"s1","s2"}, MaxBatchesPerSource=2, MaxNonTerminalHolds=1
# ~77K distinct states locally with one TLC worker, < 5 min. Checks drain,
# terminalization, held-release, and stopped-stability liveness.
```

> **Warning:** Never use `CONSTRAINT` to bound state space for liveness
> checking — it silently breaks liveness by cutting off infinite behaviors
> before they reach the convergent state. Use model constants instead.
>
> **Warning:** Never use `SYMMETRY` in liveness models. TLC may collapse states
> that must be distinct for temporal reasoning, silently producing unsound results.
> SYMMETRY is safe only for safety (INVARIANT) checks.

**Model 3 — Coverage / reachability (vacuity guards):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.coverage.cfg
# TLC will report INVARIANT VIOLATIONS for BeginDrainReachable, StopReachable,
# AckOccurs, RejectOccurs, HoldOccurs, RetryOccurs, PanicHoldOccurs,
# CheckpointAdvances, ForcedReachable, AbandonOccurs, HeldAbandonOccurs —
# each violation is a witness trace proving the state IS reachable.
# No violation = state unreachable = bug.
```

Each reachability assertion is defined as `~P` (negation of the target state).
As an INVARIANT, a violation means TLC found a state where P holds — the trace
IS the witness. Using `<>(P)` as a PROPERTY would have inverted semantics:
a violation would mean P is *never* reached (counterexample), not that it IS.

This is the TLA+ equivalent of `kani::cover!()`. If you add a new invariant, add
a corresponding reachability assertion to verify its precondition is not vacuously
impossible.

**Model 4 — Thorough safety sweep (optional, slower):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.thorough.cfg
# PR CI default thorough depth: Sources={"s1","s2","s3"}, MaxBatchesPerSource=3
```

**Model 5 — Nightly deep safety sweep (slowest):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.nightly.thorough.cfg
# Nightly CI depth: Sources={"s1","s2","s3"}, MaxBatchesPerSource=4
```

**Sabotage test** — verify no invariant is vacuously true:
temporarily replace an invariant's consequent with `FALSE`. TLC must find a
counterexample. If it reports "No error found," the precondition is unreachable
and the invariant was trivially satisfied.

### Running TLC

```bash
# CLI (requires tla2tools.jar)
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.cfg

# With coverage stats (verify every action fires):
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineMachine.tla -config tla/PipelineMachine.cfg -coverage 1

# Via TLA+ Toolbox:
# File -> Open Spec -> MCPipelineMachine.tla
# TLC Model Checker -> New Model -> Load from PipelineMachine.cfg
```

---

## ShutdownProtocol.tla

Models the two-tier shutdown cascade from `feat/io-compute-separation` (PR #1512).
Per input: I/O worker -> bounded io_cpu channel -> CPU worker -> shared pipeline channel.

### Model parameters

| Config | NumInputs | IoChannelCapacity | PipelineChannelCapacity | MaxItems |
|--------|-----------|-------------------|-------------------------|----------|
| Safety | 2 | 2 | 3 | 3 |
| Liveness | 2 | 2 | 3 | 2 |
| Coverage | 2 | 2 | 3 | 3 |

Production uses IoChannelCapacity=4, PipelineChannelCapacity=16. The protocol is
capacity-independent so small values suffice for model checking.

### What it proves

| Property | Type | Description |
|----------|------|-------------|
| `NoCpuStopBeforeIoDrain` | Safety | cpu_workers_stopped implies io_channels_drained |
| `NoJoinBeforePipelineDrain` | Safety | workers_joined implies pipeline_channel_drained |
| `NoStopBeforeJoin` | Safety | machine_stopped implies workers_joined |
| `MachineStoppedImpliesOutputTerminal` | Safety | machine_stopped implies output health is terminal (`Stopped` or `Failed`) |
| `NormalStopImpliesPoolDrained` | Safety | normal stop (not forced) implies pool fully drained |
| `ForcedStopImpliesOutputFailed` | Safety | forced stop reports terminal output failure |
| `DrainFlagsConsistent` | Safety | latched shutdown milestones imply their underlying worker/channel state |
| `IoConservation` | Safety | per-input: produced = in_io_channel + cpu_forwarded (no dup/loss) |
| `PipelineConservation` | Safety | total forwarded = in_pipeline_channel + consumed (no dup/loss) |
| `ShutdownCompletes` | Liveness | shutdown signal leads to machine_stopped |
| `NoCpuWorkerDeadlock` | Liveness | io_channels_drained leads to cpu_workers_stopped |
| `IoCpuChannelEventuallyDrained` | Liveness | all I/O workers stopped leads to io_channels_drained |
| `CpuWorkersEventuallyStop` | Liveness | all I/O workers stopped leads to all CPU workers stopped |
| `EventualStop` | Liveness | machine eventually reaches stopped state permanently |
| `OutputFailureSticky` | Liveness | once output health is failed, it remains failed |
| `ShutdownReachable` | Reachability | shutdown_signaled is reachable (vacuity guard) |
| `IoChannelsDrainedReachable` | Reachability | io_channels_drained is reachable |
| `CpuWorkersStoppedReachable` | Reachability | cpu_workers_stopped is reachable |
| `PipelineChannelDrainedReachable` | Reachability | pipeline_channel_drained is reachable |
| `WorkersJoinedReachable` | Reachability | workers_joined is reachable |
| `PoolDrainedReachable` | Reachability | pool_drained is reachable |
| `NormalStopReachable` | Reachability | normal stop is reachable |
| `ForceStopReachable` | Reachability | force stop is reachable |
| `OutputFailedReachable` | Reachability | output failure path is reachable |
| `IoChannelFullReachable` | Reachability | at least one io channel reaches capacity (backpressure) |
| `PipelineChannelFullReachable` | Reachability | pipeline channel reaches capacity (backpressure) |

### Key design: per-input CPU worker stop

Each CPU worker independently decides to exit when its own I/O worker is dead
and its own io_cpu channel is empty (`~io_alive[i] /\ Len(io_channels[i]) = 0`).
This matches the implementation where each `cpu_worker`'s `io_rx.recv()` returns
`None` independently. No global barrier is needed for individual CPU workers to exit.

The global `io_channels_drained` flag can be set by two transitions:
`MarkIoChannelsDrained` (when all I/O workers are down and all per-input channels
are empty) or `MarkCpuWorkersStopped` (as a derived consistency observation when
all CPU workers have exited). Neither is a precondition for `CpuWorkerStop`.

---

## Relationship to Kani proofs and proptest

Use TLA+, Kani, and proptest as a layered verification stack:

| Layer | Tool | File | Scope |
|-------|------|------|-------|
| Design | TLA+ (this dir) | `tla/*.tla` | Temporal logic, liveness, protocol invariants |
| Implementation | Kani | `pipeline/batch.rs`, `pipeline/lifecycle.rs` | Memory safety, overflow, type transitions |
| Property-based | proptest | `pipeline.rs` tests | State sequence correctness under arbitrary inputs |

TLA+ proves the **design** is correct (ordering, drainability, eventual stop).
Kani proves bounded implementation properties (no panic/overflow in pure logic).
proptest stresses larger input/state spaces and integration behavior.

## PipelineBatch.tla

Models multi-source batch accumulation, flush, checkpoint merge behavior, and
ack/reject handling at the batching seam.

Run:

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineBatch.tla -config tla/PipelineBatch.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineBatch.tla -config tla/PipelineBatch.liveness.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCPipelineBatch.tla -config tla/PipelineBatch.coverage.cfg
```

---

## DeliveryRetry.tla

Models the worker delivery retry loop from
`crates/logfwd-runtime/src/worker_pool/worker.rs` (`process_item`). This is the
highest-priority spec because PipelineMachine.tla assumes `WF(AckBatch)` (batches
eventually terminalize) but nothing formally verified that assumption until now.

The retry loop sends a batch to a sink, and on transient failure (IoError,
RetryAfter, timeout) retries indefinitely with exponential backoff capped at
`MaxBackoffMs`. Terminal exits are: Ok (delivered), Rejected (permanent failure),
or Cancel (shutdown). Liveness depends on sink recovery OR shutdown cancellation.

### Model parameters

| Config | InitialBackoffMs | MaxBackoffMs | MaxRetries |
|--------|------------------|--------------|------------|
| Safety | 100 | 1600 | 5 |
| Liveness | 100 | 800 | 3 |
| Coverage | 100 | 1600 | 5 |

### What it proves

| Property | Type | Description |
|----------|------|-------------|
| `BackoffCapRespected` | Safety | backoff delay never exceeds MaxBackoffMs |
| `BackoffMonotonic` | Safety | in WaitingBackoff, delay is at least InitialBackoffMs |
| `TerminalImpliesOutcome` | Safety | Terminal state has a definite outcome |
| `NonTerminalImpliesNoOutcome` | Safety | non-terminal state has no outcome yet |
| `IdleImpliesNoRetries` | Safety | Idle state has zero retries |
| `CancelledImpliesCancelledOutcome` | Safety | cancelled flag implies Cancelled outcome |
| `BackoffZeroOnlyInitially` | Safety | zero backoff only before first transient failure |
| `RetryCountMonotonic` | Safety (temporal) | retry count never decreases |
| `BackoffDelayMonotonic` | Safety (temporal) | backoff delay never decreases |
| `TerminalIsAbsorbing` | Safety (temporal) | Terminal state is permanent |
| `OutcomeIsStable` | Safety (temporal) | outcome never changes once set |
| `TerminalReachable` | Liveness | every delivery eventually reaches Terminal (under fairness) |
| `HealthySinkDelivers` | Liveness | permanently healthy sink implies eventual Ok |
| `CancelTerminates` | Liveness | cancellation leads to Terminal |
| `TerminalIsStable` | Liveness | Terminal is eventually stable |
| `BackoffEventuallyResolves` | Liveness | WaitingBackoff always resolves |
| `SendingReachable` | Reachability | Sending state is reachable |
| `OkReachable` | Reachability | Ok outcome is reachable |
| `RejectedReachable` | Reachability | Rejected outcome is reachable |
| `CancelledReachable` | Reachability | Cancelled outcome is reachable |
| `BackoffReachable` | Reachability | WaitingBackoff is reachable |
| `RetryOccurs` | Reachability | at least one retry occurs |
| `BackoffCapReached` | Reachability | backoff reaches MaxBackoffMs |
| `MultipleRetriesOccur` | Reachability | multiple retries occur |
| `SinkRecoveryReachable` | Reachability | sink recovery after failure is reachable |

### Run

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCDeliveryRetry.tla -config tla/DeliveryRetry.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCDeliveryRetry.tla -config tla/DeliveryRetry.liveness.cfg
java -cp /path/to/tla2tools.jar tlc2.TLC tla/MCDeliveryRetry.tla -config tla/DeliveryRetry.coverage.cfg
```

### Key design: retry forever, terminate on cancel

The code deliberately retries forever for transient errors (Filebeat-style delivery
model). The worker blocks on its current batch, propagating backpressure through
bounded channels to inputs. The only terminal exits are:

- **Ok** — sink accepted the batch
- **Rejected** — sink permanently rejected (4xx, schema error)
- **Cancelled** — shutdown token fired

This means `TerminalReachable` requires WF(Cancel) in the fairness assumption.
Without cancellation, a permanently-failing sink would block the worker forever
by design. This is the correct behavior: backpressure, not data loss.

### Relationship to PipelineMachine.tla

PipelineMachine.tla's `WF(AckBatch)` assumes that once a batch is in Sending
state, it eventually receives an explicit terminal outcome (ack/reject/abandon).
DeliveryRetry.tla's `TerminalReachable` property formally justifies this
assumption by proving that the retry loop in `process_item` always terminates
under weak fairness (with sink recovery or shutdown cancellation).

---

## TailLifecycle.tla

Models the pure tail reducer behavior extracted in `crates/logfwd-io/src/tail/state.rs`:

- EOF emission thresholding (`eof_emitted` + idle streak)
- graceful-shutdown EOF gating (`fileOffset >= fileSize`)
- EOF reset on data/truncate paths
- error backoff growth/cap/reset (`consecutive_error_polls`, `backoff_ms`)

### What it proves

| Property | Type | Description |
|----------|------|-------------|
| `EofEmissionRequiresThreshold` | Safety | EOF emit transition only occurs once idle threshold is reached |
| `DataResetsEofState` | Safety | data transition always clears EOF state and idle streak |
| `ShutdownEofRequiresCaughtUp` | Safety | shutdown EOF may bypass idle threshold only when the tracked offset has caught up to current file size |
| `ShutdownBehindSuppressesEof` | Safety | shutdown must suppress terminal EOF while unread file bytes remain |
| `BackoffZeroIffNoErrors` | Safety | backoff state is cleared exactly when error streak is zero |
| `BackoffDelayConsistent` | Safety | backoff delay follows the bounded exponential schedule |

### Run

```bash
just tlc-tail
```

---

## Comparison with production systems

| Feature | Our Design | Vector | Filebeat | Fluent Bit | OTel |
|---------|-----------|--------|----------|------------|------|
| Typestate enforcement | ✅ (PhantomData) | ❌ | ❌ | ❌ | ❌ |
| Explicit ordered-ack | ✅ (BTreeMap) | FuturesOrdered | activeCursorOps | ❌ | ❌ |
| Unbypassable stop guard | ✅ (Err(self)) | ❌ (timeout kill) | ❌ | ❌ | ❌ |
| Formal verification | ✅ (Kani + TLA+) | ❌ | ❌ | ❌ | ❌ |
| Force-drain escape | ✅ (ForceStop) | ✅ | ❌ | ✅ (grace) | ✅ (ctx timeout) |

Our design is closest to Vector's `OrderedFinalizer` + Filebeat's cursor model.
Key difference: `FuturesOrdered` (Vector) vs explicit `BTreeMap` (us). The BTreeMap
approach is more inspectable (`is_drained()` queries, `in_flight_count()`) at the
cost of more explicit management.

---

## Key design decisions captured in this spec

### 1. Non-terminal hold/retry is explicit

`HoldBatch` models fail(), retry exhaustion, dispatch failure, timeout, and
similar control-plane outcomes that must not advance checkpoints. A held batch
remains in `in_flight`, so it blocks normal `Stop` and cannot be committed past.
`RetryHeldBatch` releases the hold without committing or terminalizing. Panic is
modeled as `PanicHoldBatch`: the same non-terminal hold plus an audit marker so
TLC can prove panic-held work is later terminalized or explicitly abandoned.

`MaxNonTerminalHolds` bounds retry/failure churn for TLC. This is a model bound,
not a production retry budget.

### 2. Rejected batches advance the checkpoint

`RejectBatch` is a distinct transition from `AckBatch`, but both are explicit
terminal outcomes that can advance ordered commit. Permanently-undeliverable
data must not block checkpoint progress forever; that would stall drain
indefinitely. At-least-once is weakened to at-most-once only for rejected
batches. This matches Filebeat's behavior (advance past malformed records) and
differs from Fluent Bit (drops the route, retries via backlog).

**Implication:** if a batch is rejected, the data in that batch is lost. This
is the correct behavior for a log forwarder where corrupted or oversized data
cannot be retried, but it must be explicitly documented and metered.

### 3. `pending_acks` is correctly abstracted away

The Rust implementation uses an explicit `pending_acks: BTreeMap<SourceId,
BTreeMap<BatchId, C>>` to handle out-of-order acks. In this spec, `pending_acks`
is implicit: `NewCommitted` directly computes the committed value from the
`acked` set. The two are equivalent:

- When `in_flight[s]` becomes empty after `apply_ack(last_batch)`, `NewCommitted`
  advances to cover ALL acked batches (no lower-ID blocker exists), matching the
  Rust behavior where the pending_acks loop drains completely.
- The `is_drained()` check (Rust: `in_flight.all_empty() && pending_acks.all_empty()`)
  is equivalent to the TLA+ Stop guard (`\A s: in_flight[s] = {}`) in all
  reachable states, because empty in_flight implies empty pending_acks.

### 4. Per-source independent checkpoints

Each source `s` has its own `committed[s]`. A slow source doesn't block a fast
source from committing. This is the right design for a log forwarder (equivalent
to per-partition independent offsets in Kafka). It differs from Flink's global
checkpoint barrier, which is required for stateful stream processing but adds
blocking that a stateless forwarder should not need.

### 5. ForceStop and the liveness assumption

`ForceStop` is modeled to reflect that every production system has a hard-kill
escape hatch. Under normal operation (no ForceStop), the spec proves that drain
always eventually completes (`EventualDrain`). With `ForceStop`, in-flight work
including held and panic-held work is explicitly terminalized into `abandoned`,
so `DrainCompleteness` still holds (`Stopped => in_flight = {}`). The explicit
`abandoned` set captures the policy decision to accept data loss for liveness.

**Fairness assumption for `WF(Stop)`:** Stop's enabledness is stable once
reached during Draining, because `NoCreateAfterDrain` (verified invariant)
prevents new BeginSend calls from growing `in_flight` during the Draining
phase. Therefore WF (weak fairness) suffices; SF (strong fairness, required
when enabledness oscillates) is not needed.

---

## Known gaps (not modeled here, documented for future work)

**Gap detection:** Vector's `OrderedAcknowledgements` in `acks.rs` detects gaps
in marker ID sequences (disk corruption / dropped records). Our BTreeMap-based
design will stall if a BatchId is never acked (bug). The invariant is: all
BatchIds in `[0, next_batch_id)` will eventually receive `apply_ack`. This is
enforced by the Rust type system (`#[must_use]` on `BatchTicket`) but not
formally proven in TLA+ here. A future spec extension could add a `GapFreeIds`
safety invariant.

**Source identity re-use:** Vector's `Checkpointer::update_key(old, new)` handles
file fingerprint changes (log rotation). Our `SourceRegistry::upsert()` re-
activates Committed sources, but the interaction with in-flight batches from the
old identity is not modeled here.

**Sink liveness during drain:** The Rust caller must ensure sinks remain alive
until all Sending tickets are resolved. If sinks are torn down early, `is_drained()`
will never become true and drain will never complete. OTel enforces this via
topological shutdown order (receivers stop before exporters). This is a caller
constraint on the pipeline, not a property of the machine itself.

**Retry timing and payload retention:** `HoldBatch`/`RetryHeldBatch` model
lifecycle effects, not backoff timers, retry jitter, or retained batch payload
storage. The backoff-level retry loop is now modeled in `DeliveryRetry.tla`,
which proves terminalization liveness. Runtime fault timing (jitter, wall-clock
delay accuracy) remains covered by Turmoil/proptest rather than this finite TLA
model.

---

## Resources for learning TLA+

- [Learn TLA+](https://learntla.com) — the best introductory resource
- [Hillel Wayne: Weak and Strong Fairness](https://www.hillelwayne.com/post/fairness/) — when to use WF vs SF
- [Jack Vanlightly: Verifying Kafka Transactions](https://jack-vanlightly.com/analyses/2024/12/3/verifying-kafka-transactions-diary-entry-2-writing-an-initial-tla-spec) — real-world pipeline verification
- [AWS: How Formal Methods Are Used at Amazon](https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/) — the DynamoDB 35-step bug story
- [TLA+ Examples repository](https://github.com/tlaplus/Examples) — reference specs
- [PingCAP/tla-plus](https://github.com/pingcap/tla-plus) — Raft, Percolator, 2PC
- [spacejam/tla-rust](https://github.com/spacejam/tla-rust) — TLA+ + Rust workflow reference
