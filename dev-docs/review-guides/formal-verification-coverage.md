# Formal Verification Coverage — Full Review Guidance

You are reviewing formal verification coverage for logfwd, a project that
uses a four-tier verification strategy as a core differentiator. The project
targets 80+ Kani proofs in `logfwd-core`, TLA+ for temporal protocol properties,
proptest for statistical coverage of complex inputs, and Rust's type system for
compile-time invariants. Fail this check if required verification is absent,
of insufficient quality, or if `VERIFICATION.md` has not been updated to reflect
new or changed proofs.


## Kani Proofs — When Required

Any new public function added to `logfwd-core` requires at least one Kani proof.
Three categories are exempt from Kani and require proptest or type-level
verification instead:
1. Async functions (Kani does not support async runtimes; use proptest for event-sequence properties instead).
2. Heap-heavy code that creates `Vec`, `HashMap`, or `BTreeMap` proportional to input size (Kani state-space explodes; use proptest).
3. Trivial getters or setters with no logic beyond a field read/write (no invariant to prove).

These are the ONLY exemptions. Any function with logic — parsing, encoding, bit
manipulation, state transitions, arithmetic — is NOT exempt. For any non-exempt
function, the absence of a proof is a hard fail. These exemptions mirror the ones
in `CRATE_RULES.md` and are the same exemptions referenced by the Crate Boundary
check's Rule 5.

By function category the requirements are:

### Parsers

Any code that handles raw bytes: `framer.rs`, `cri.rs`, `json_scanner.rs`,
`scan_config.rs`, `aggregator.rs`. MUST have a `verify_<fn>_no_panic` proof using
`kani::any()` inputs within a bounded slice size (8–32 bytes for most parsers,
up to 256 bytes where larger inputs change behavior). SHOULD have a correctness
proof comparing output against an independently-computed oracle. The oracle must
be a simple, obviously-correct reference implementation, not the production code
rewritten — an oracle that calls the same function it is testing is not an oracle.

`aggregator.rs` additionally requires explicit state-machine invariant proofs: the
`P`/`F` flags and partial-line accumulation state must have a
`verify_aggregator_state_invariants` proof using a bounded symbolic trace (bounded
sequence of `push` and `finish` calls) that checks: `P` is set iff a partial line
is currently buffered, `F` is set iff the previous line was a partial that was
continued, and the output line count is non-decreasing. Use a bounded trace length
(e.g. 4 operations) and `kani::any::<[u8; 8]>()` for each chunk input.

### Wire Format Encoders

`otlp.rs`, varint encoding, protobuf size calculation. MUST prove no panic on all
inputs within bounded message size. MUST verify that the encoded byte length equals
the predicted size (this class of bug was the source of protobuf framing errors
found during development). MUST prove varint encoding roundtrips:
`decode(encode(x)) == x` for all `x` in the type's range.

### Bitmask Operations

`structural.rs`, `structural_iter.rs`, `chunk_classify.rs`. MUST have oracle-based
correctness proofs. The oracle for structural detection is the scalar byte-by-byte
reference implementation. The proof must verify that for ALL inputs
(`kani::any::<u64>()` bitmasks, `kani::any::<[u8; 64]>()` chunks), the
SIMD-equivalent and scalar paths produce identical results. `kani::cover!()` must
confirm that both "some bits set" and "zero bits" cases are reachable — an
all-zeros input is trivially correct and does not prove the interesting paths.

### Byte Search Primitives

`byte_search.rs`. MUST prove correctness against a naive implementation (linear
scan). Both `find_byte` and `rfind_byte` must have oracle proofs. The proof must
verify: found index is within bounds, byte at that index equals target, all
preceding bytes (for `find`) or following bytes (for `rfind`) do not equal target.

### State Machine Transitions

`pipeline/lifecycle.rs`, `pipeline/batch.rs`, `pipeline/registry.rs`. MUST prove
all valid state transitions preserve invariants. For lifecycle: no batch creation
after drain begins, committed checkpoint is monotonically non-decreasing, stopped
state is terminal. For batch typestate: `Queued → Sending` is the only allowed
first transition, `Sending → Acked` and `Sending → Rejected` are the only allowed
completions. For registry: sequence numbers are strictly monotonic, no duplicate
batch IDs.


## Kani Proof Quality Requirements — Every Proof Must

1. Live in a `#[cfg(kani)] mod verification {}` block at the bottom of the module
   file, NOT in `#[cfg(test)]`. Function contracts (`#[cfg_attr(kani, kani::requires(...))]`
   and `#[cfg_attr(kani, kani::ensures(...))]`) annotate production code directly,
   but the proof harnesses themselves are in `#[cfg(kani)]`.
2. Follow the naming convention `verify_<function_name>_<property_being_proved>`.
   Examples: `verify_encode_varint_roundtrip`, `verify_parse_cri_line_no_panic`,
   `verify_prefix_xor_matches_scalar`.
3. Carry `#[kani::unwind(N)]` for EVERY loop in the proof or in functions called
   by the proof. N = maximum number of loop iterations + 1 or 2. A proof with an
   unbounded loop will either time out or be unsound. Reject proofs with loops but
   no unwind annotation.
4. Include `kani::cover!()` statements to guard against vacuous proofs. Every proof
   that uses `kani::assume()` or constrains inputs must have at least two `cover!`
   calls confirming that interesting paths satisfying the constraints are actually
   reachable. If a `cover!` statement reports UNSATISFIABLE, the proof may be
   trivially true because no input satisfies the constraints — this is a vacuous
   proof and must be fixed before merging.
5. Build oracles independently. The oracle must compute the expected result from the
   raw input without calling through to the function under test. For iterator-based
   proofs, count expected items by scanning the input buffer directly rather than
   trusting an iterator's bitmask output.
6. Prefer `kani::any_where()` over `kani::any()` + `kani::assume()` when constraints
   can be expressed at value generation time. Reserve `assume()` for constraints that
   span multiple variables or depend on computed intermediate state.


## Solver Selection

Proofs that run longer than 10 seconds must carry `#[kani::solver(kissat)]` with
a comment explaining why: "kissat chosen: default cadical times out on this
bitmask-heavy proof". For arithmetic-heavy proofs (varint, integer parsing with
overflow), try `z3` or `bitwuzla` if cadical is slow. Document the solver choice.
The CI Kani job has a 30-minute timeout; any proof expected to approach this limit
must use kissat or another fast solver and carry a comment.


## Compositional Proofs for Deep Call Chains

For functions in `logfwd-core` that are called from multiple other functions in
the hot path (e.g., `scan_string`, `skip_nested`, byte search primitives), add
`#[cfg_attr(kani, kani::requires(...))]` and `#[cfg_attr(kani, kani::ensures(...))]`
contracts to the leaf function. Verify these contracts with `proof_for_contract`.
Then callers can use `stub_verified` to replace the callee with its contract —
bounding state-space explosion to N independent proofs over slices of the call
graph rather than one exponentially expensive monolithic proof. Reject PRs that
add deeply nested pure logic to `logfwd-core` hot-path functions without contracts
on the leaf functions, when those functions are called from multiple sites.


## TLA+ Requirements

Any PR that modifies `pipeline/lifecycle.rs`, `PipelineMachine` state transitions,
the drain protocol, checkpoint ordering, batch sequence numbering, or the
ordered-ACK mechanism MUST address TLA+ coverage. The spec lives at
`tla/PipelineMachine.tla` in this repository. The TLC configurations are
`PipelineMachine.cfg` (safety model, ~50K states) and
`PipelineMachine.liveness.cfg` (liveness model, ~5K states).

Safety invariants checked in `PipelineMachine.cfg` (INVARIANTS block):
`TypeOK` (well-typed state), `NoDoubleComplete` (a batch cannot be both in-flight
and acked simultaneously), `DrainCompleteness` (stop() only reachable when all
in-flight batches are resolved), `CheckpointOrderingInvariant`
(committed[s]=n implies all batches 1..n are acked),
`CommittedNeverAheadOfAcked` (committed never exceeds the acked frontier),
`InFlightImpliesCreated` (in-flight batches were created),
`AckedImpliesCreated` (acked batches were created).

Temporal action properties also checked in `PipelineMachine.cfg` (PROPERTIES block,
safe to check at full model size): `NoCreateAfterDrain` (no `CreateBatch` action
while Draining or Stopped), `CommittedMonotonic` (committed never decreases),
`DrainMeansNoNewSending` (no `BeginSend` while Draining).

Liveness properties checked in `PipelineMachine.liveness.cfg` (PROPERTIES block,
require Fairness in Spec): `EventualDrain` (Draining always leads to Stopped),
`NoBatchLeftBehind` (every in-flight batch eventually leaves in-flight),
`StoppedIsStable` (once Stopped, stays Stopped),
`AllCreatedBatchesEventuallyAccountedFor` (every created batch eventually acked
or rejected).

For any PR touching this area, require either: (a) explicit confirmation that
the change does not affect any of the above properties with reasoning, OR (b)
updated TLA+ spec and TLC output showing all properties still hold under both
cfg files. NEVER use CONSTRAINT to bound state space for liveness checking —
it silently breaks liveness by cutting off infinite behaviors before they reach
the convergent state. Use model constants (`MaxBatchesPerSource`, `Sources` set
size) instead.


## Proptest Requirements

New SIMD code in `logfwd-arrow` MUST have a proptest oracle test comparing SIMD
output == scalar output for random inputs. `PROPTEST_CASES=2000` minimum. This is
non-negotiable — it is the conformance guarantee that makes SIMD safe to deploy.
New scanner behavior (field extraction, type detection, duplicate key handling,
CRI parsing) MUST have proptest coverage for the following known bug-triggering
cases: escape sequences crossing 64-byte SIMD block boundaries, fields appearing
in different orders across records, duplicate keys with same type (first-write-wins
must be preserved), duplicate keys with different types (must produce two separate
suffixed columns for the current phase, or single promoted-type column after Phase
10). New async pipeline code MUST have proptest tests for: event sequences arriving
in arbitrary order, acks arriving in a different order than submits (ordered-ACK
must still advance checkpoint correctly), drain requested while batches in-flight.
Oracle tests for the scanner compare against sonic-rs as ground truth. Our scanner
uses first-writer-wins for duplicate keys; sonic-rs uses last-writer-wins. Oracle
tests must skip duplicate-key inputs or explicitly document the deviation.


## VERIFICATION.md Updates

Any PR that adds, removes, or modifies Kani proofs MUST update the per-module
verification status table in `dev-docs/VERIFICATION.md`. The table columns are:
Module, What it does, Verification. The proof count in the Verification column
must be accurate — count the proofs in the changed module and verify the table
entry matches. PRs that add proofs without updating this table will confuse
future reviewers about coverage completeness. If a module moves from Tier 2
(bounded) to Tier 1 (exhaustive), update the tier classification and the tier
descriptions at the bottom of the file.
