# Verification

logfwd uses four complementary verification techniques. Choose based on what you need to prove.

## Tool selection

| Technique | Layer | Best for | Limitation |
|-----------|-------|----------|------------|
| **TLA+** | Design | Temporal properties: liveness, drain, protocol ordering | Not tied to code; models must be maintained separately |
| **Kani** | Implementation | Exhaustive bounded inputs; pure functions; unsafe code | Slow on wide types; no async; no IO |
| **proptest** | Integration | Heap-intensive, stateful, or async code | Incomplete coverage |
| **Miri** | Runtime | UB detection; allocator behavior | Not exhaustive |
| **Rust types** | Compile-time | Illegal state transitions; silent data loss | Only what the type system can encode |

If the property is temporal ("eventually," "always," "never after X") → TLA+.  
If the function is pure, bounded, and critical → Kani.  
If it's stateful, heap-heavy, or async → proptest.  
For unsafe code, use Kani and proptest both.

---

## TLA+ (Tier 0 — design level)

TLA+ proves protocol-level properties that bounded model checkers cannot express: liveness,
fairness, and ordering invariants that hold across infinite behaviors.

### When to write a TLA+ spec

Write a TLA+ spec when:
- A correctness property is temporal: "eventually," "once X, always X," "never Y after Z"
- A state machine has multiple cooperating actors where Kani can only check one step at a time
- You need to prove a drain or shutdown protocol terminates under all interleavings
- The question is "is the design correct?" not "is the implementation correct?"

### What's in `tla/`

> `tla/` lives in this repository on `main`. CI runs the checked-in TLC models
> according to `.github/workflows/ci.yml`.

`tla/PipelineMachine.tla` models the `PipelineMachine<S, C>` lifecycle. It proves:

| Property | Type | Description |
|----------|------|-------------|
| `DrainCompleteness` | Safety | `stop()` only reachable when all in-flight batches are resolved |
| `CheckpointOrderingInvariant` | Safety | `committed[s]=n` implies all batches `1..n` are acked, none in-flight |
| `CommittedMonotonic` | Safety | Checkpoint never goes backwards |
| `NoCreateAfterDrain` | Safety | No new batches after `begin_drain` |
| `NoDoubleComplete` | Safety | Batch cannot be both in-flight and acked |
| `EventualDrain` | Liveness | Every started drain eventually reaches Stopped |
| `NoBatchLeftBehind` | Liveness | Every in-flight batch eventually leaves in-flight |
| `StoppedIsStable` | Liveness | Once Stopped, stays Stopped |

`tla/MCPipelineMachine.tla` is the TLC model checker configuration (symmetry sets, model
constants). `tla/PipelineMachine.cfg` is the TLC configuration file (three models
documented inside). See `tla/README.md` for full documentation, design decisions captured
in the spec, and known gaps.

### Running TLC

```bash
cd tla/
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.cfg
```

Three models:
- **Model 1 — Safety** (comment out `ForceStop` in `Next`): `Sources={"s1","s2"}`,
  `MaxBatchesPerSource=3`, ~50K states, < 30s
- **Model 2 — Liveness**: `MaxBatchesPerSource=2`, ~5K states, < 5 min
- **Model 3 — ForceStop**: enable `ForceStop`, remove `DrainCompleteness` from invariants

> Do NOT use `CONSTRAINT` to bound state space for liveness — it silently breaks liveness
> by cutting off infinite behaviors before they converge. Use model constants instead.

---

## Kani (Tiers 1–2 — exhaustive and bounded)

Kani is a bounded model checker for Rust. `kani::any()` is a universal quantifier, not
random — the SAT solver considers ALL possible values simultaneously.

### When Kani proofs are required

Add Kani proofs when implementing or modifying:

1. **Parsers** — Any code handling raw bytes (`framer.rs`, `cri.rs`, `json_scanner.rs`)
   - MUST have `verify_<function>_no_panic` proof
   - SHOULD have correctness proof against an oracle

2. **Wire formats** — Protobuf encoding, varint encoding, size calculations
   - MUST prove no panic on all inputs within bounded size
   - MUST verify size limits are respected

3. **Bitmask operations** — SIMD structural character detection (`structural.rs`)
   - MUST have oracle-based correctness proof

4. **Byte search primitives** — Low-level search operations (`byte_search.rs`)
   - MUST prove correctness matches naive implementation

5. **State machines** — Protocol state transitions (P/F flags, pipeline lifecycle)
   - MUST prove state invariants hold across all transitions

Kani is NOT required for: I/O operations, async runtime logic, complex state machines
> 8-10 transitions (use proptest), heap-heavy `Vec`/`HashMap` code (use proptest + Miri),
simple getters/setters.

If a review tool or human review finds a state-machine bug in mixed async/runtime code,
do not stop at patching the shell. Extract the transition policy into a local pure
reducer or state module when feasible, then add Kani proofs for single-step invariants
and proptest sequence coverage for multi-step behavior.

### Running proofs

```bash
cargo kani -p logfwd-core              # All proofs in a crate
cargo kani --harness verify_my_fn      # Specific harness
cargo kani -p logfwd-core --tests      # Kani + unit tests
cargo build -p logfwd-core \
  --target thumbv6m-none-eabi          # Verify no_std compliance
```

### Non-core pure seam boundary contract

Non-core Kani scope is explicitly tracked in:

- `dev-docs/verification/kani-boundary-contract.toml` as the source of truth
- `scripts/verify_kani_boundary_contract.py` as the fast validator

Status values in the contract:

- `required` — this seam must include in-file Kani harnesses (`#[cfg(kani)]` + `#[kani::proof]`)
- `recommended` — pure seam where Kani is encouraged, but not a hard gate
- `exempt` — intentionally not a Kani target, usually because the file is async/IO-heavy or still mixes shell code with policy

Run locally:

```bash
python3 scripts/verify_kani_boundary_contract.py
just kani-boundary
```

CI runs this check in the dedicated `Verification guardrail` job, and the
required `CI conclusion` status depends on that guardrail passing. CI also runs
`cargo kani` for the required production crates (`logfwd-core`, `logfwd-arrow`,
`logfwd-io`, `logfwd-output`) under the following conditions (from the `kani`
job in `.github/workflows/ci.yml`):

- **any push** to the repository, OR
- a pull request carrying the **`ci:full` label**, OR
- a **non-draft pull request** where the path-filter outputs
  `kani_required == 'true'` — i.e. at least one changed file matches one of:
  - `crates/logfwd-core/**`
  - `crates/logfwd-arrow/**`
  - `crates/logfwd-io/**`
  - `crates/logfwd-output/**`
  - `dev-docs/verification/kani-boundary-contract.toml`
  - `scripts/verify_kani_boundary_contract.py`
  - `.github/workflows/ci.yml`

A matching path change in a **draft** PR does not trigger the job; the PR must
be marked ready for review first.

### Proof quality requirements

Every Kani proof MUST:
1. Use `#[cfg(kani)]` for proof modules; use `#[cfg_attr(kani, kani::requires(...))]` /
   `#[cfg_attr(kani, kani::ensures(...))]` for function contracts on production code
2. Follow naming: `verify_<function>_<property>`
3. Add `#[kani::unwind(N)]` for any loops (N = max iterations + 1 or 2)
4. Use appropriate input sizes (8–32 bytes for parsing, full range for bitmasks)
5. Add `kani::cover!()` to guard against vacuous proofs

### Best practices

**Guard against vacuous proofs.** Every proof with `kani::assume()` or constrained inputs
needs cover statements confirming interesting paths are reachable:
```rust
kani::cover!(count > 0, "at least one match found");
kani::cover!(count == 0, "no matches (empty case exercised)");
```
If a cover reports UNSATISFIABLE, the proof may be vacuously true.

**Use symbolic exploration, not hardcoded orderings.** For order-independent properties
(e.g., ack permutations), use `kani::any_where()` to let Kani explore all orderings:
```rust
let order: u8 = kani::any_where(|&o: &u8| o < 6);
let (first, second, third) = match order { /* all 6 permutations */ };
```

**Build independent oracles.** Don't trust internal state as the oracle. Compute expected
values from the raw input independently. For iterators, count expected items by scanning
the input buffer directly rather than trusting the iterator's bitmask.

**Select solvers for complex proofs.** Add `#[kani::solver(kissat)]` to proofs that take
> 10 seconds. Kissat is fastest for ~47% of slow harnesses, with speedups up to 265x.
For arithmetic-heavy proofs, try `z3` or `bitwuzla`.

**Compositional proofs scale; monolithic proofs don't.** For deep call chains, add
`#[kani::requires]` / `#[kani::ensures]` contracts to leaf functions, verify with
`proof_for_contract`, then use `stub_verified` in callers. This bounds state-space
explosion to N independent proofs over slices of the call graph.

**Prefer `kani::any_where()` over `any()` + `assume()`.** Keep constraints co-located
with value generation. Use `assume()` only when constraints span multiple variables or
depend on computed state.

---

## proptest (Tier 3 — statistical)

Use proptest for end-to-end integration, heap-intensive code, and async logic that Kani
cannot reach.

```rust
use proptest::prelude::*;
proptest! {
    #[test]
    fn scanner_never_panics(input in any::<Vec<u8>>()) {
        let _ = scanner.scan(&input);
    }
}
```

Oracle tests compare against an independent reference implementation. Our scanner uses
first-writer-wins for duplicate keys; sonic-rs uses last-writer-wins — oracle tests skip
duplicate-key inputs.

Run with `PROPTEST_CASES=2000` minimum for the scanner. proptest finds bugs on: escapes
crossing 64-byte boundaries, fields in different orders, duplicate keys with different
types.

---

## Compile-time (Tier 4)

The Rust type system enforces invariants that no runtime verification can match:

- `BatchTicket<S, C>` — `#[must_use]` and consume-on-transition prevent silent ticket
  drops. Illegal transitions (e.g., acking a Queued ticket) are compile errors.
- `AckReceipt<C>` — `#[must_use]` ensures every receipt reaches `apply_ack`.
- `#![forbid(unsafe_code)]` in logfwd-core — cannot be bypassed with `#[allow]`.
- `#![no_std]` — compiler removes the IO APIs structurally.

---

## logfwd-core rules

logfwd-core is the proven kernel. All rules are CI-enforced.

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` + alloc | Compiler. CI: `cargo build --target thumbv6m-none-eabi` |
| `#![forbid(unsafe_code)]` | Compiler. Cannot be overridden with `#[allow]`. |
| Only deps: memchr + wide | CI dependency allowlist check |
| No panics | `clippy::unwrap_used`, `clippy::panic`, `clippy::indexing_slicing` = deny |
| Every public fn has a proof | CI proof coverage script |
| No IO, threads, async | Structural (no_std removes the APIs) |

---

## Per-module verification status

| Module | What it does | Verification |
|--------|-------------|-------------|
| `structural.rs` | Escape detection, quote classification, SIMD structural detection | Kani exhaustive (12 proofs) + proptest (SIMD ≡ scalar) |
| `structural_iter.rs` | Streaming structural position iterator | Kani exhaustive (3 proofs) |
| `framer.rs` | Newline framing, line boundary detection | Kani exhaustive + oracle (4 proofs) |
| `reassembler.rs` | CRI partial line reassembly (P/F merging) | Kani exhaustive (8 proofs) |
| `byte_search.rs` | Proven byte search (find_byte, rfind_byte) | Kani exhaustive + oracle (3 proofs) |
| `scanner.rs` | JSON field extraction (ScanBuilder trait) | Kani bounded (1 proof) + proptest oracle |
| `json_scanner.rs` | Streaming JSON field scanner via bitmask iteration | Kani bounded (5 proofs) + proptest oracle |
| `scan_config.rs` | `parse_int_fast`, `parse_float_fast`, `ScanConfig` | Kani exhaustive (2 proofs) |
| `cri.rs` | CRI log parsing + partial line reassembly | Kani exhaustive (8 proofs) |
| `otlp.rs` | Protobuf wire format + OTLP encoding + timestamp parsing | Kani mixed exhaustive + bounded (30 proofs incl. 3 contract verifications) |
| `pipeline/lifecycle.rs` | Pipeline state machine (ordered ACK, drain, shutdown) | Kani exhaustive (6 proofs) + proptest + **TLA+** |
| `logfwd/pipeline/health.rs` | Pipeline component-health transition reducer (`observed`, poll failure, shutdown) | Kani exhaustive (4 proofs) + unit tests + proptest sequence checks |
| `pipeline/batch.rs` | BatchTicket typestate (ack/nack/fail/reject) | Kani exhaustive (5 proofs) + compile-time |
| `logfwd-types/diagnostics/health.rs` | `ComponentHealth` lattice (`combine`, readiness, storage repr) | Kani exhaustive (4 proofs) + unit tests |
| `logfwd-output/lib.rs` | Conflict struct detection, ColVariant priority ordering | Kani (8 proofs: ColVariant field preservation, variant_dt, is_conflict_struct, json/str priority contracts) |
| `logfwd-output/sink.rs` | Fanout helper correctness: `merge_child_send_result` pending-signal tracking and `finalize_fanout_outcome` outcome precedence | Kani (2 proofs: `verify_merge_child_send_result_tracks_pending_signals`, `verify_finalize_fanout_outcome_precedence`) |
| `logfwd-output/sink/health.rs` | Output health reducer + fanout roll-up semantics | Kani (7 proofs: retrying terminal preservation, shutdown completion, startup recovery, delivery recovery, shutdown request semantics, fanout commutativity, fatal-failure drain preservation) + unit tests + proptest sequence/aggregation checks |
| `logfwd-io/diagnostics/policy.rs` | Control-plane readiness/status policy mapping (`health_reason`, `readiness_impact`, readiness snapshot consistency) | Kani exhaustive (4 proofs) + unit tests + proptest mapping checks |
| `logfwd-io/otlp_receiver.rs` | OTLP proto→JSON transcoding helpers inside a mixed runtime receiver shell | Kani recommended (1 proof: hex encoding) + unit tests; future Kani targets include protojson numeric parsing, integer/float writers, and JSON escaping helpers |
| `logfwd-io/polling_input_health.rs` | Polling-input source health reducer for tail/TCP/UDP (`healthy`, backpressure, error-backoff) | Kani exhaustive (3 proofs) + unit tests + proptest sequence checks |
| `logfwd-io/receiver_health.rs` | Standalone receiver health reducer (`noop`, backpressure, fatal, shutdown) | Kani exhaustive (6 proofs) + unit tests + proptest sequence checks |
| `logfwd-io/format.rs` | CRI metadata injection, Auto-mode fallthrough to passthrough | Kani (4 proofs: inject_cri_metadata output structure, JSON vs plain-text path dispatch) |
| `logfwd-io/tail.rs` | File tailer EOF emission state machine (eof_emitted flag) | Kani (4 proofs: at-most-once emission per streak, data-reset invariant, two-poll sequence, reset-cycle) |
| `logfwd/pipeline/checkpoint_policy.rs` | Typed delivery outcome -> checkpoint disposition mapping (`Ack`, `Reject`, `Hold`) | Kani exhaustive (3 proofs) + unit tests |
| `logfwd/worker_pool/health.rs` | Pool idle-phase insertion + worker-slot aggregation policy | Kani exhaustive (3 proofs) + unit tests + proptest aggregation checks |
| `logfwd/worker_pool.rs` | MRU dispatch decision + typed delivery outcome helpers | Kani (8 dispatch/outcome proofs) + unit tests for worker-slot aggregation, drain-phase stickiness, and create-failure behavior |
| `logfwd-arrow/storage_builder.rs` | StructArray conflict column assembly | Kani (2 proofs: duplicate name guard, row count invariant) + unit tests |
| `logfwd-arrow/streaming_builder.rs` | StructArray conflict column assembly (StringView) | Kani (2 proofs: duplicate name guard, row count invariant) + unit tests |
| `logfwd-arrow/conflict_schema.rs` | Conflict-struct detection + row-level precedence selection | Kani recommended (2 proofs) + unit tests |
| `scanner_conformance.rs` (accumulation) | BytesMut accumulation → Bytes → Scanner equivalence | proptest (3 tests × 256 cases: random split, single chunk, per-line split; full value comparison) |

### Verification tiers

**Tier 0 — Design (TLA+):** temporal and liveness properties over the protocol. Proves the
design is correct before implementation.

**Tier 1 — Exhaustive (Kani proves for ALL inputs within type bounds):**
u64 bitmask operations, integer parsing, varint roundtrip, pipeline state machine
(all state×event pairs), BatchTicket transitions, severity parsing.

**Tier 2 — Bounded (Kani proves for inputs up to size N):**
`parse_cri_line` (≤32 bytes), `NewlineFramer` (all 32-byte inputs), `skip_nested`
(all 16-byte inputs), `CriReassembler` P+P+F (8-byte messages, max_size ≤32).

**Tier 3 — Statistical (proptest, high confidence):**
SIMD ≡ scalar structural detection, scanner oracle vs sonic-rs, pipeline event sequences,
pipeline ack ordering.

**Tier 4 — Compile-time (Rust type system):**
BatchTicket `#[must_use]` + consume-on-transition, AckReceipt `#[must_use]`.

---

## Adding a function to logfwd-core

1. Write the function. Pure logic only — no IO, no allocation-heavy patterns, no panics.

2. Choose verification tier:
   - Can Kani verify ALL inputs? (small fixed-width types) → Tier 1
   - Can Kani verify bounded inputs? (byte slices ≤256) → Tier 2
   - Neither? → Tier 3 (proptest)

3. Write the proof:

```rust
// Tier 1: Kani exhaustive
#[cfg(kani)]
mod verification {
    use super::*;
    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_my_function_property() {
        let input: u64 = kani::any();
        let result = my_function(input);
        assert!(/* postcondition */);
        kani::cover!(/* interesting case */);
    }
}

// Tier 3: proptest
#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    proptest! {
        fn my_function_never_panics(input in any::<Vec<u8>>()) {
            let _ = my_function(&input);
        }
    }
}
```

4. Verify locally before submitting:
```bash
cargo kani -p logfwd-core --harness verify_my_function_property
cargo test -p logfwd-core
cargo build -p logfwd-core --target thumbv6m-none-eabi
```
