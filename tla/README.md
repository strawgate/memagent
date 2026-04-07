# TLA+ Formal Specifications

Formal models for the logfwd pipeline design. These specs capture
properties that Kani (bounded model checker) cannot express — temporal
logic, liveness, and protocol-level design invariants.

## PipelineMachine.tla

Models `PipelineMachine<S, C>` from
`crates/logfwd-core/src/pipeline/lifecycle.rs`.

### What it proves

| Property | Type | Description |
|----------|------|-------------|
| `DrainCompleteness` | Safety | `stop()` only reachable when all in_flight batches are resolved |
| `CheckpointOrderingInvariant` | Safety | committed[s]=n implies all batches 1..n are acked, none in_flight |
| `CommittedNeverAheadOfAcked` | Safety | committed[s] never exceeds count of acked batches |
| `NoDoubleComplete` | Safety | batch cannot be both in_flight and acked |
| `InFlightImpliesCreated` | Safety | structural: in_flight ⊆ created |
| `AckedImpliesCreated` | Safety | structural: acked ⊆ created |
| `CommittedMonotonic` | Safety (temporal) | checkpoint never goes backwards |
| `NoCreateAfterDrain` | Safety (temporal) | no new batches after begin_drain |
| `DrainMeansNoNewSending` | Safety (temporal) | in_flight cannot grow once phase ≠ Running |
| `EventualDrain` | Liveness | every started drain eventually reaches Stopped |
| `NoBatchLeftBehind` | Liveness | every in_flight batch eventually leaves in_flight |
| `StoppedIsStable` | Liveness | once Stopped, stays Stopped |
| `AllCreatedBatchesEventuallyAccountedFor` | Liveness | every created batch is committed or machine is Stopped |
| `BeginDrainReachable` | Reachability (invariant ~P) | Draining phase is reachable (vacuity guard) |
| `StopReachable` | Reachability (invariant ~P) | Stopped phase is reachable (vacuity guard) |
| `AckOccurs` | Reachability (invariant ~P) | at least one batch is acked (AckBatch fires) |
| `CheckpointAdvances` | Reachability (invariant ~P) | committed checkpoint advances at least once |
| `ForcedReachable` | Reachability (invariant ~P) | ForceStop path is reachable (vacuity guard) |

### File structure (two-file pattern)

This spec follows the industry-standard two-file pattern used by etcd-io/raft
(`MCetcdraft.tla`), PingCAP/tla-plus, and Jack Vanlightly's Kafka verification:

```text
tla/
  # Lifecycle state machine (ordered ACK, checkpoint ordering, drain guarantee)
  PipelineMachine.tla           — clean algorithm spec
  MCPipelineMachine.tla         — TLC config: symmetry sets, model constants
  PipelineMachine.cfg           — safety model (~50K states)
  PipelineMachine.liveness.cfg  — liveness model (smaller constants, no SYMMETRY)
  PipelineMachine.thorough.cfg  — thorough safety model (3 sources, 4 batches)
  PipelineMachine.coverage.cfg  — reachability / vacuity guards

  # Shutdown coordination (multi-process drain protocol)
  ShutdownProtocol.tla          — N inputs + channel + consumer + pool
  MCShutdownProtocol.tla        — TLC config
  ShutdownProtocol.cfg          — safety model
  ShutdownProtocol.liveness.cfg — liveness model
  ShutdownProtocol.coverage.cfg — reachability guards

  # Batching protocol (multi-source, checkpoint merge, reject handling)
  PipelineBatch.tla             — batch accumulation + flush + ack/reject
  MCPipelineBatch.tla           — TLC config
  PipelineBatch.cfg             — safety model
  PipelineBatch.liveness.cfg    — liveness model
  PipelineBatch.coverage.cfg    — reachability guards

  README.md                     — this file
```

### Four models to run

**Model 1 — Safety (normal path, EnableForceStop=FALSE):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.cfg
# Sources={"s1","s2"}, MaxBatchesPerSource=3, symmetry on Sources
# ~50K states, < 30s. Checks all INVARIANTS + temporal action properties.
```

**Model 2 — Liveness (smaller constants, no SYMMETRY):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.liveness.cfg
# Sources={"s1","s2"}, MaxBatchesPerSource=2 — liveness needs small constants
# ~5K states, < 5 min. Checks EventualDrain, NoBatchLeftBehind, StoppedIsStable
```

> **Warning:** Never use `CONSTRAINT` to bound state space for liveness
> checking — it silently breaks liveness by cutting off infinite behaviors
> before they reach the convergent state. Use model constants instead.
>
> **Warning:** Never use `SYMMETRY` in liveness models. TLC may collapse states
> that must be distinct for temporal reasoning, silently producing unsound results.
> SYMMETRY is safe only for safety (INVARIANT) checks.

**Model 3 — Safety with ForceStop:**

ForceStop is always in `Next` — no separate config needed. The `forced` flag
records when it fired, and `DrainCompleteness` is conditioned on `~forced`, so
all configs check it unconditionally. To verify ForceStop-specific behavior, run
the safety config and inspect the `forced=TRUE` traces in TLC's error output.

**Model 4 — Coverage / reachability (vacuity guards):**

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.coverage.cfg
# TLC will report INVARIANT VIOLATIONS for BeginDrainReachable, StopReachable,
# AckOccurs, CheckpointAdvances, ForcedReachable — each violation is a witness
# trace proving the state IS reachable. No violation = state unreachable = bug.
```

Each reachability assertion is defined as `~P` (negation of the target state).
As an INVARIANT, a violation means TLC found a state where P holds — the trace
IS the witness. Using `<>(P)` as a PROPERTY would have inverted semantics:
a violation would mean P is *never* reached (counterexample), not that it IS.

This is the TLA+ equivalent of `kani::cover!()`. If you add a new invariant, add
a corresponding reachability assertion to verify its precondition is not vacuously
impossible.

**Sabotage test** — verify no invariant is vacuously true:
temporarily replace an invariant's consequent with `FALSE`. TLC must find a
counterexample. If it reports "No error found," the precondition is unreachable
and the invariant was trivially satisfied.

### Running TLC

```bash
# CLI (requires tla2tools.jar)
cd tla/
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.cfg

# With coverage stats (verify every action fires):
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.cfg -coverage 1

# Via TLA+ Toolbox:
# File -> Open Spec -> MCPipelineMachine.tla
# TLC Model Checker -> New Model -> Load from PipelineMachine.cfg
```

---

## Relationship to Kani proofs and proptest

| Layer | Tool | File | Scope |
|-------|------|------|-------|
| Design | TLA+ (this dir) | `tla/*.tla` | Temporal logic, liveness, protocol invariants |
| Implementation | Kani | `pipeline/batch.rs`, `pipeline/lifecycle.rs` | Memory safety, overflow, type transitions |
| Property-based | proptest | `pipeline.rs` tests | State sequence correctness under arbitrary inputs |

TLA+ proves the **design** is correct — no race conditions, correct ordering, drain is eventually possible. Kani proves the **Rust implementation** doesn't panic or overflow. They are complementary: a design bug is caught here; an implementation bug is caught by Kani.

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

### 1. `fail()` is invisible to the machine

A `fail()`ed batch returns its ticket to Queued state but the machine still
tracks it as in_flight (it was already `begin_send`'d). The TLA+ model has no
`FailBatch` action because fail() changes no machine state. This models the
Rust code exactly: `fail()` returns `BatchTicket<Queued, C>` but the BTreeMap
entry in `in_flight[source]` is not removed until `apply_ack` is called.

### 2. Explicit permanent rejects advance the checkpoint

`RejectBatch` is aliased to `AckBatch` — same state transition. Permanently-
undeliverable data must not block checkpoint progress forever; that would
stall drain indefinitely. At-least-once is weakened to at-most-once only for
rejected batches. This matches Filebeat's behavior (advance past malformed
records) and differs from Fluent Bit (drops the route, retries via backlog).

**Implication:** if a batch is rejected, the data in that batch is lost. This
is the correct behavior for a log forwarder where corrupted or oversized data
cannot be retried, but it must be explicitly documented and metered.

Control-plane and retry-exhaustion failures are a different category. They
must not be modeled as `RejectBatch`. In the current Rust rollout those
outcomes are held unresolved so checkpoints do not advance past undelivered
data; graceful drain may therefore fall back to `ForceStop`, with replay on
restart covering the unresolved batches.

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
always eventually completes (`EventualDrain`). With `ForceStop` enabled,
`DrainCompleteness` no longer holds — this is intentional and correct: force-
stopping is explicitly the policy decision to accept data loss for liveness.

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

---

## Resources for learning TLA+

- [Learn TLA+](https://learntla.com) — the best introductory resource
- [Hillel Wayne: Weak and Strong Fairness](https://www.hillelwayne.com/post/fairness/) — when to use WF vs SF
- [Jack Vanlightly: Verifying Kafka Transactions](https://jack-vanlightly.com/analyses/2024/12/3/verifying-kafka-transactions-diary-entry-2-writing-an-initial-tla-spec) — real-world pipeline verification
- [AWS: How Formal Methods Are Used at Amazon](https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/) — the DynamoDB 35-step bug story
- [TLA+ Examples repository](https://github.com/tlaplus/Examples) — reference specs
- [PingCAP/tla-plus](https://github.com/pingcap/tla-plus) — Raft, Percolator, 2PC
- [spacejam/tla-rust](https://github.com/spacejam/tla-rust) — TLA+ + Rust workflow reference
