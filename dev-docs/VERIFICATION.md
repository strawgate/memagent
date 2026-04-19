# Verification

logfwd uses five complementary verification techniques. Choose based on what you need to prove.

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
Canonical property definitions and TLC model commands live in `tla/README.md`.
This section defines policy and trigger conditions for when TLA+ updates are required.

### When to write a TLA+ spec

Write a TLA+ spec when:
- A correctness property is temporal: "eventually," "once X, always X," "never Y after Z"
- A state machine has multiple cooperating actors where Kani can only check one step at a time
- You need to prove a drain or shutdown protocol terminates under all interleavings
- The question is "is the design correct?" not "is the implementation correct?"

Update existing TLA+ specs when:
- State-transition rules change (`create/send/ack/reject/abandon`, drain/shutdown, checkpoint advance)
- Liveness assumptions or fairness conditions change
- A new terminal outcome or protocol phase is added

### What's in `tla/`

> `tla/` lives in this repository on `main`. CI runs the checked-in TLC models
> according to `.github/workflows/ci.yml`.

`tla/PipelineMachine.tla` models the `PipelineMachine<S, C>` lifecycle. It proves:

| Property | Type | Description |
|----------|------|-------------|
| `DrainCompleteness` | Safety | `stop()` only reachable when all in-flight batches are resolved |
| `NoHeldWorkAfterStop` | Safety | `Stopped` never leaves non-terminal held work behind |
| `QuiescenceHasNoSilentStrandedWork` | Safety | `Stopped` never leaves sent batches without a terminal `acked`/`rejected`/`abandoned` outcome |
| `NoUnresolvedSentAtQuiescence` | Safety | `Stopped` implies `sent \ terminal = {}` for every source (no stranded sent work) |
| `StopMetadataConsistent` | Safety | `forced` and `stop_reason` stay phase-consistent (`Stopped` iff reason is non-`none`) |
| `CheckpointOrderingInvariant` | Safety | `committed[s]=n` implies every sent batch `<= n` is commit-terminal via `acked`/`rejected` and none are in-flight |
| `UnresolvedWorkNotCommittedPast` | Safety | active or held in-flight work cannot be silently committed past |
| `CheckpointNeverAheadOfTerminalizedPrefix` | Safety | committed checkpoint is never ahead of the ack/reject terminalized prefix |
| `CommittedMonotonic` | Safety | Checkpoint never goes backwards |
| `FailureTerminalizationPreservesCheckpoint` | Safety | Force/crash terminalization cannot advance checkpoints |
| `FailureClassMustTerminalizePrototype` | Safety | Force/crash transitions must leave no sent-but-unterminalized batches |
| `HeldTransitionsDoNotCommit` | Safety | hold/retry/force-stop held-state transitions do not advance checkpoints |
| `ForceStopAbandonsAllInFlight` | Safety | force-stop explicitly moves every unresolved in-flight batch to `abandoned` |
| `NoCreateAfterDrain` | Safety | No new batches after `begin_drain` |
| `NoDoubleComplete` | Safety | In-flight batches are disjoint from `acked`, `rejected`, and `abandoned` terminal sets |
| `EventualDrain` | Liveness | Every started drain eventually reaches Stopped |
| `NoBatchLeftBehind` | Liveness | Every in-flight batch eventually terminalizes (`ack`/`reject`/`abandon`) |
| `HeldBatchEventuallyReleased` | Liveness | Every non-terminal hold is eventually retried/released |
| `PanickedBatchEventuallyAccountedFor` | Liveness | Panic-held work eventually reaches `ack`/`reject`/`abandon` |
| `StoppedIsStable` | Liveness | Once Stopped, stays Stopped |

`tla/MCPipelineMachine.tla` is the TLC model checker configuration (symmetry sets, model
constants). `tla/PipelineMachine.cfg` is the TLC configuration file (three models
documented inside). See `tla/README.md` for full documentation, design decisions captured
in the spec, and known gaps.

`tla/ShutdownProtocol.tla` models split I/O/CPU worker shutdown plus output
terminal health. Clean shutdown reaches output health `Stopped`; forced stop
and worker-panic paths reach `Failed`. Safety checks include
`MachineStoppedImpliesOutputTerminal` and `ForcedStopImpliesOutputFailed`;
liveness checks include `OutputFailureSticky`.

### Running TLC

```bash
cd tla/
java -cp /path/to/tla2tools.jar tlc2.TLC MCPipelineMachine.tla -config PipelineMachine.cfg
```

Three models:
- **Model 1 — Safety** (normal + ForceStop paths): `Sources={"s1","s2"}`,
  `MaxBatchesPerSource=3`, `MaxNonTerminalHolds=1`, ~10.8M distinct states
  locally with one TLC worker, < 10 min
- **Model 2 — Liveness**: `MaxBatchesPerSource=2`, `MaxNonTerminalHolds=1`,
  ~77K distinct states locally with one TLC worker, < 5 min
- **Model 3 — Coverage**: reachability/vacuity witnesses via
  `PipelineMachine.coverage.cfg` (expected invariant violations as witnesses)

The PipelineMachine model explicitly includes bounded non-terminal
hold/fail/retry/panic behavior. `HoldBatch` keeps a batch in `in_flight` without
advancing checkpoints; `RetryHeldBatch` releases the hold; `PanicHoldBatch`
records a panic-originated hold; `ForceStop` explicitly abandons unresolved
work. The model still abstracts away retry backoff timing and payload retention.

> Do NOT use `CONSTRAINT` to bound state space for liveness — it silently breaks liveness
> by cutting off infinite behaviors before they converge. Use model constants instead.

---

## Kani (Tiers 1–2 — exhaustive and bounded)

Kani is a bounded model checker for Rust. `kani::any()` is a universal quantifier, not
random — the SAT solver considers ALL possible values simultaneously.

### Kani quick path (10 minutes)

1. Assume Kani is required for any changed pure/bounded `logfwd-core` logic.
2. For new public `logfwd-core` functions, add or update a Kani proof.
3. Put harnesses in `#[cfg(kani)] mod verification` in the same file.
4. Use `verify_<function>_<property>` naming and add `kani::cover!`.
5. Add unwind bounds for loops.
6. Run targeted harness, then run `just kani`.
7. Run `just kani-boundary` when touching non-core pure seam boundaries.
8. If Kani is infeasible, extract a pure reducer seam or document a bounded exemption with proptest/TLA+ coverage.

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

Kani is not required for: I/O operations, async runtime logic, and heap-heavy
`Vec`/`HashMap` code (use proptest + Miri), plus trivial getters/setters.
For complex state machines, extract pure single-step reducers when possible and
prove those with Kani; cover multi-step temporal behavior with proptest/TLA+.

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
MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test -p logfwd-core --lib
MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test -p logfwd-types --lib
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
`cargo kani` for the required proof-bearing crates (`logfwd-core`,
`logfwd-arrow`, `logfwd-types`, `logfwd-io`, `logfwd-output`,
`logfwd-runtime`, `logfwd-diagnostics`, and `logfwd`) under the following
conditions (from the `kani` job in `.github/workflows/ci.yml`):

- **any push** to the repository, OR
- a pull request carrying the **`ci:full` label**, OR
- a **non-draft pull request** where the path-filter outputs
  `kani_required == 'true'` — i.e. at least one changed file matches one of:
  - `crates/logfwd-core/**`
  - `crates/logfwd-arrow/**`
  - `crates/logfwd-types/**`
  - `crates/logfwd-io/**`
  - `crates/logfwd-output/**`
  - `crates/logfwd-runtime/**`
  - `crates/logfwd-diagnostics/**`
  - `crates/logfwd/**`
  - `Cargo.toml`
  - `Cargo.lock`
  - `dev-docs/verification/kani-boundary-contract.toml`
  - `scripts/verify_kani_boundary_contract.py`
  - `.github/workflows/ci.yml`

A matching path change in a **draft** PR does not trigger the job; the PR must
be marked ready for review first.

### Verification guardrail checks

The CI `Verification guardrail` job runs these lightweight anti-drift checks:

- `scripts/verify_kani_boundary_contract.py` — validates non-core Kani seam policy
- `scripts/verify_tlc_matrix_contract.py` — validates TLC matrix coverage for expected non-coverage/non-thorough `tla/*.cfg` models
- `scripts/verify_proptest_regressions.py` — validates persisted proptest regression-file policy and approved `failure_persistence: None` usage
- `scripts/verify_verification_trigger_contract.py` — validates CI path filters, Kani crate scopes, and guardrail wiring stay aligned

Run them locally:

```bash
just verification-guardrail
```

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
| Every public fn has a proof | Required policy + review checks + CI Kani job with per-module inventory below |
| No IO, threads, async | Structural (no_std removes the APIs) |

---

## Per-module verification status

| Module | What it does | Verification |
|--------|-------------|-------------|
| `structural.rs` | Escape detection, quote classification, SIMD structural detection | Kani exhaustive (9 proofs) + proptest (SIMD ≡ scalar) |
| `structural_iter.rs` | Streaming structural position iterator, including space-only `next_non_space` semantics where tab/CR remain non-space | Kani exhaustive (3 proofs) |
| `framer.rs` | Newline framing, line boundary detection | Kani exhaustive + oracle (4 proofs) |
| `reassembler.rs` | CRI partial line reassembly (P/F merging) | Kani exhaustive (8 proofs) |
| `byte_search.rs` | Proven byte search (find_byte, rfind_byte) | Kani exhaustive + oracle (3 proofs) |
| `scanner.rs` | Scanner-to-builder protocol (`ScanBuilder`, `BuilderState`) | Kani bounded (4 proofs) + property-based protocol coverage |
| `json_scanner.rs` | Streaming JSON field scanner via bitmask iteration, including escaped key/value decoding, CRLF normalization, and JSON whitespace boundary regressions | Kani bounded (5 proofs) + proptest oracle + compliance regressions |
| `scan_config.rs` | `parse_int_fast`, `parse_float_fast`, `ScanConfig` | Kani exhaustive (2 proofs) |
| `cri.rs` | CRI log parsing + partial line reassembly | Kani exhaustive (8 proofs) |
| `otlp.rs` | Protobuf wire format + OTLP encoding + timestamp parsing | Kani mixed exhaustive + bounded (30 proofs incl. 3 contract verifications) |
| `pipeline/lifecycle.rs` | Pipeline state machine (ordered ACK, drain, shutdown) | Kani exhaustive (16 proofs) + proptest + **TLA+** |
| `logfwd-runtime/pipeline/health.rs` | Pipeline component-health transition reducer (`observed`, bounded startup poll failure escalation, shutdown) | Kani exhaustive (6 proofs) + unit tests + proptest sequence checks |
| `pipeline/batch.rs` | BatchTicket typestate (ack/nack/fail/reject) | Kani exhaustive (5 proofs) + compile-time |
| `logfwd-types/diagnostics/health.rs` | `ComponentHealth` lattice (`combine`, readiness, storage repr) | Kani exhaustive (4 proofs) + unit tests |
| `logfwd-output/lib.rs` | Conflict struct detection, ColVariant priority ordering | Kani (8 proofs: ColVariant field preservation, variant_dt, is_conflict_struct, json/str priority contracts) |
| `logfwd-output/sink.rs` | Fanout helper correctness: `merge_child_send_result` pending-signal tracking and `finalize_fanout_outcome` outcome precedence | Kani (2 proofs: `verify_merge_child_send_result_tracks_pending_signals`, `verify_finalize_fanout_outcome_precedence`) |
| `logfwd-output/sink/health.rs` | Output health reducer + fanout roll-up semantics | Kani (7 proofs: retrying terminal preservation, shutdown completion, startup recovery, delivery recovery, shutdown request semantics, fanout commutativity, fatal-failure drain preservation) + unit tests + proptest sequence/aggregation checks |
| `logfwd-diagnostics/stderr_capture.rs` | Stderr capture buffering + ANSI stripping helpers | Kani exhaustive (2 proofs) + unit tests |
| `logfwd-diagnostics/metric_history.rs` | Tiered diagnostics metric history retention/serialization helpers | Unit tests |
| `logfwd-diagnostics/span_exporter.rs` | In-process OTel span snapshot buffering/export helpers | Unit tests |
| `logfwd-diagnostics/diagnostics/policy.rs` | Control-plane readiness/status policy mapping (`health_reason`, `readiness_impact`, readiness snapshot consistency) | Kani exhaustive (4 proofs) + unit tests + proptest mapping checks |
| `logfwd-io/otlp_receiver/convert.rs` | OTLP proto→JSON/Arrow conversion helpers extracted from the runtime shell | Kani recommended (1 proof: hex encoding) + unit tests + proptest oracle checks (writer helpers and request→NDJSON fast-vs-simple equivalence) + ignored release microbench guardrails |
| `logfwd-io/otlp_receiver/projection.rs` | Experimental OTLP wire projection into Arrow (bypasses prost object graph) | 56 tests: prost-reference oracle parity (primitive, multi-resource/scope, randomized), malformed wire rejection (truncated varint/fixed64/fixed32/len, invalid UTF-8 in key/value/body, group mismatch, group depth, field zero, oversized field, wrong wire type), nested unknown-field interleavings, intentionally ignored metadata fields, high-cardinality dynamic attributes, unsupported-but-valid AnyValue fallback (array/kvlist in body/attrs, mixed primitive+unsupported), unsupported+malformed fallback rejection, ProjectedFallback mode parity. Proptest: 128-case arbitrary-byte ProjectedFallback-vs-prost classification, 64-case single-container randomized parity, and 32-case multi-container randomized parity. Projected decode remains non-default pending production-path benchmark gates. |
| `logfwd-io/otlp_receiver.rs` | Receiver channel-drain boundedness, projected decoder pool sharding, HTTP decode backpressure, and decompression expansion limits | Unit tests (`drain_receiver_payloads` limit, queue carry-over, event order + `accounted_bytes` preservation, projected decoder shard/round-robin behavior, 429 on protobuf decode permit exhaustion, gzip/zstd expansion-limit rejection) |
| `logfwd-io/tcp_input.rs` | TCP per-client bounded-read predicate (`should_stop_client_read`) and noisy-neighbor progress envelope | Kani (3 proofs: zero counters, independent caps, predicate equivalence) + unit tests + proptest predicate equivalence + noisy-vs-quiet bounded-progress regression |
| `logfwd-io/udp_input.rs` | UDP bounded-drain predicate (`should_stop_udp_drain`), per-poll datagram work cap, and sender-scoped source identity | Kani (3 proofs: zero counters, independent caps, predicate equivalence) + unit tests + proptest predicate equivalence + bounded-drain/recovery regression + IPv4/IPv6 `SourceId` attribution tests |
| `logfwd-output/elasticsearch.rs` | Elasticsearch bulk NDJSON serialization + timestamp suffix writer | Unit tests + proptest oracle checks (serialize_batch and timestamp suffix fast-vs-simple equivalence) + ignored release microbench guardrails |
| `logfwd-output/otlp_sink.rs` | OTLP sink row encoder and generated-fast encoder parity | Unit tests + proptest oracle check (generated-fast vs handwritten encoder equivalence on random UTF-8 rows) |
| `logfwd-transform/enrichment.rs` | CSV enrichment parsing, header validation, nullable `Utf8View` batch production, and DataFusion join integration | Unit tests for CSV edge cases and legacy-value/null parity + integration test for `Utf8View` join output. Kani is not required because this path is heap-heavy CSV/DataFusion integration rather than a bounded pure seam. |
| `logfwd-io/polling_input_health.rs` | Polling-input source health reducer for tail/TCP/UDP (`healthy`, backpressure, error-backoff) | Kani exhaustive (3 proofs) + unit tests + proptest sequence checks |
| `logfwd-io/receiver_health.rs` | Standalone receiver health reducer (`noop`, backpressure, fatal, shutdown) | Kani exhaustive (6 proofs) + unit tests + proptest sequence checks |
| `logfwd-io/format.rs` | CRI metadata injection, Auto-mode fallthrough to passthrough | Kani (4 proofs: inject_cri_metadata output structure, JSON vs plain-text path dispatch) |
| `logfwd-io/framed.rs` | Per-source framing/remainder checkpoint boundary (`overflow_tainted`, EOF remainder flush, source-scoped EOF semantics, idle source-state reclamation) | Unit tests (remainder carry/overflow, per-source isolation, EOF flush + checkpoint advancement contracts, complete-state reclamation and CRI partial retention) |
| `logfwd-io/tail/verification.rs` | File tailer pure reducers for EOF emission + shutdown EOF gating + error backoff (`eof_model_transition`, `shutdown_should_emit_eof`, `backoff_transition`) | Kani (8 proofs: EOF at-most-once thresholding, reset semantics, two-poll/repeat cycle, shutdown caught-up gating, backoff cap/reset/monotonicity) + proptest (state reducer invariants in `tail/state.rs`) + **TLA+** (`tla/TailLifecycle.tla`) |
| `logfwd-io/segment.rs` | Checkpoint segment envelope read/write/recovery (`LCHK` header/footer, checksum, replay plan) | Unit tests for panic/OOM/silent-mask hardening (unsupported-version, permission-denied I/O surfacing, write/read size-bound parity, recovery error semantics) |
| `logfwd-runtime/pipeline/checkpoint_policy.rs` | Typed delivery outcome -> checkpoint disposition mapping plus bounded ordered-commit seam model (`Ack`, `Reject`, `Hold`) | Kani exhaustive/bounded (6 proofs: outcome mapping, hold no-advance, ack/reject terminal equivalence, mixed-sequence monotonicity/no-gap jumps) + unit tests + proptest ordered sequence invariants |
| `logfwd-runtime/pipeline/checkpoint_io.rs` | Final checkpoint flush retry window (`MAX_ATTEMPTS`, retry/no-retry boundary, retryable error classification, stop-on-success behavior) | Kani (3 proofs: zero/one-attempt no-retry, retry-window equivalence, retryable error classification) + unit tests + proptest retry-window equivalence checks + async retry behavior tests |
| `logfwd-runtime/pipeline/input_poll.rs` | Turmoil input-loop flush predicate (`should_flush_buffer`) for byte-batch/timeout emission policy | Kani (3 proofs: empty-buffer no-flush, timeout gate semantics, predicate equivalence) + unit tests + proptest policy equivalence |
| `logfwd-runtime/worker_pool/health.rs` | Pool idle-phase insertion + worker-slot aggregation policy | Kani exhaustive (3 proofs) + unit tests + proptest aggregation checks |
| `logfwd-runtime/worker_pool.rs` | MRU dispatch decision + typed delivery outcome helpers | Kani (8 dispatch/outcome proofs) + unit tests for worker-slot aggregation, drain-phase stickiness, and create-failure behavior |
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
   - New public `logfwd-core` function: Kani proof required (Tier 1 or Tier 2).
   - If Kani is infeasible in current shape: extract a pure seam and prove it,
     or document an explicit exemption with proptest/TLA+ coverage.

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
