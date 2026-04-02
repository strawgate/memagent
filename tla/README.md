# TLA+ Formal Specifications

Formal models for the logfwd pipeline design. These specs capture
properties that Kani (bounded model checker) cannot express: temporal
logic, liveness, and protocol-level design invariants.

## PipelineMachine.tla

Models `PipelineMachine<S, C>` from
`crates/logfwd-core/src/pipeline/lifecycle.rs`.

### What it proves

**Drain guarantee (safety):** `stop()` is only reachable when every
in-flight batch for every source has been acked or rejected. The
pipeline cannot shut down with data in flight.

**Checkpoint ordering (safety):** `committed[s] = n` implies that all
batches 1..n for source `s` are acknowledged and none are still
in-flight. This is the at-least-once delivery invariant — safe to
restart from `committed[s]` because all prior data was delivered.

**Liveness (under fairness):** Every in-flight batch eventually leaves
in-flight. Every started drain eventually reaches Stopped. No batch is
stuck forever.

### Relationship to Kani proofs

| Layer | Tool | Scope |
|-------|------|-------|
| Design | TLA+ (this spec) | Temporal logic, liveness, protocol invariants |
| Implementation | Kani (`batch.rs`, `lifecycle.rs`) | Memory safety, overflow, type transitions |
| Property-based | proptest (`pipeline.rs`) | State sequence correctness |

TLA+ proves the design is correct. Kani proves the Rust implementation
doesn't panic or overflow. They are complementary — a design bug would
be caught here; an implementation bug would be caught by Kani.

### Running the model checker

**Prerequisites:** [TLA+ Toolbox](https://lamport.azurewebsites.net/tla/toolbox.html)
or the TLC CLI.

```bash
# CLI (requires tla2tools.jar on classpath)
cd tla/
java -jar /path/to/tla2tools.jar -config PipelineMachine.cfg PipelineMachine.tla

# Or via the Toolbox IDE:
# File -> Open Spec -> PipelineMachine.tla
# TLC Model Checker -> New Model -> Load from .cfg
```

**Expected output:** All invariants satisfied, all temporal properties
verified. State space should be ~50K states for the fast configuration
(MaxSources=2, MaxBatchesPerSource=3).

### Model bounds

The `.cfg` file has two preset bounds:

| Config | MaxSources | MaxBatchesPerSource | States | Time |
|--------|-----------|---------------------|--------|------|
| FAST | 2 | 3 | ~50K | < 30s |
| THOROUGH | 3 | 4 | ~2M | ~5 min |

The THOROUGH configuration should be run before merging changes to
`crates/logfwd-core/src/pipeline/lifecycle.rs` or `batch.rs`.

### Key design decisions captured in this spec

1. **`fail()` is invisible to the machine**: A `fail()`ed batch
   stays in `in_flight` until `apply_ack` is called. The TLA+ model
   reflects this — there is no `FailBatch` action because it doesn't
   change machine state.

2. **Rejected batches advance the checkpoint**: `RejectBatch` has the
   same state transition as `AckBatch`. Permanently-undeliverable data
   must not block checkpoint progress forever (it would stall drain
   indefinitely). At-least-once is weakened to at-most-once only for
   rejected batches.

3. **Committed is per-source, not global**: Each source has an
   independent committed checkpoint. A slow source doesn't block a
   fast source from committing. This is verified by the
   `CheckpointOrderingInvariant` holding per-source.

4. **Contiguous prefix, not arbitrary subset**: The `NewCommitted`
   operator advances only when all of batches 1..n are acked — gaps
   are not allowed. This matches the Rust `record_ack_and_advance`
   implementation which walks the pending_acks BTreeMap in order.
