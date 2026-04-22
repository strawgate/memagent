# Verification Gap Analysis: TLA+ Spec and Kani Proofs

> **Status:** Active
> **Date:** 2026-03-31
> **Context:** gap analysis of TLA+ spec and Kani proof coverage with high-priority findings on RotateFile and invariants.
> **Scope:** `dev-docs/research/FileCheckpoint.tla` (archived), `crates/logfwd-core/src/checkpoint_tracker.rs`

---

## Part 1: TLA+ Specification Audit

### 1. Completeness: Variable Priming

Each of the 12 actions was checked for whether all 20 state variables
are either primed or listed in UNCHANGED.

| # | Action | Verdict | Notes |
|---|--------|---------|-------|
| 1 | AppendLine | PASS | All 20 vars accounted for |
| 2 | RotateFile | **FAIL** | See finding T-1 |
| 3 | CopyTruncate | PASS | All 20 vars accounted for |
| 4 | TailerRead | PASS | All 20 vars accounted for |
| 5 | TailerReadRotated | PASS | All 20 vars accounted for |
| 6 | TailerFinishRotated | PASS | All 20 vars accounted for |
| 7 | FormBatch | PASS | All 20 vars accounted for |
| 8 | FlushPartialBatch | PASS | All 20 vars accounted for |
| 9 | SendBatch | PASS | All 20 vars accounted for |
| 10 | AckBatch | PASS | All 20 vars accounted for |
| 11 | Crash | PASS | All 20 vars accounted for |
| 12 | Restart | PASS | All 20 vars accounted for |

#### Finding T-1: RotateFile does not clear framer_buf on source change [MEDIUM]

RotateFile includes `framer_buf` and `framer_source` in UNCHANGED, with
a comment saying "the tailer will re-read the rotated file's unread portion
separately." But consider: the framer_buf may contain lines from the OLD
file identity. After rotation, `TailerRead` on the NEW file will either
overwrite or append depending on `framer_source`. The ELSE branch of
`TailerRead` overwrites `framer_buf` entirely, discarding the old data.

In contrast, `CopyTruncate` explicitly clears `framer_buf` and `framer_source`
to 0. The asymmetry is intentional (rotation preserves the old file as
`rotated_content` for draining), but there is a subtle issue: if
`TailerReadRotated` fires before `TailerRead`, the framer already has old-file
data. Then `TailerRead` fires on the new file and hits the ELSE branch,
silently dropping the old-file lines that were never batched.

This is not necessarily a bug -- the rotated file drain mechanism is supposed
to handle those lines -- but the model allows an interleaving where
`TailerReadRotated` loads lines into the framer, `TailerRead` overwrites them,
and they are never emitted. The model accepts this because `TailerReadRotated`
advances `rotated_offset` so those lines won't be re-read, yet they were never
sent downstream.

**Impact**: Possible silent data loss on rotation when the tailer reads from
both old and new files before batching the old file's lines.

**Recommendation**: Either (a) add a guard to `TailerRead` that prevents it
from firing while `framer_source = rotated_identity` and `framer_buf` is
non-empty, or (b) model per-source framer buffers (which matches the real
implementation's per-source remainder design).

---

### 2. Property Coverage

#### Finding T-2: NoCorruption is too weak -- does not check ordering or completeness [HIGH]

`NoCorruption` only checks:
```tla
\A i \in 1..Len(emitted) :
    emitted[i] \in LineValues /\ emitted[i] < next_line_id
```

This verifies that every emitted line was actually written (no fabrication).
It does NOT verify:

1. **No duplicates**: The same line could appear multiple times in `emitted`.
   After crash+restart, at-least-once delivery means duplicates are expected,
   but the spec should document this explicitly and ideally have a property
   that bounds duplicates (e.g., a line appears at most twice -- once before
   crash, once after restart).

2. **Ordering**: Lines could be emitted out of order. For a single-file model
   this may be acceptable, but the property name "NoCorruption" suggests
   stronger guarantees than it delivers.

3. **Completeness**: There is no safety invariant that checks "every line
   written to the file eventually appears in emitted." That is left to the
   liveness property `EventualEmission`, but liveness is only checked with
   `MaxRotations=0`. With rotations, lines in the old file that were not
   checkpointed before a crash are lost (by design), but the spec has no
   property that captures "lines are lost only in these specific scenarios."

**Recommendation**: Add a `NoDuplicateEmission` invariant for the no-crash
case, and document the at-least-once semantics explicitly in the spec header.
Add an `OrderPreserved` invariant (emitted is a subsequence of the
concatenation of all file generations' contents).

#### Finding T-3: CheckpointBounded has a gap when file identity is reused [LOW]

`CheckpointBounded` checks:
```tla
/\ alive => checkpoints[file_identity] <= Len(file_content)
```

The spec assigns fresh identities on every rotation/truncation
(`next_identity` increments), so identity reuse never occurs within the
model's bounds. This is correct. However, in the real system, fingerprints
are computed from file content -- if two different files happen to have the
same fingerprint (collision), the model would not catch the resulting bug.
This is an inherent limitation of the identity abstraction.

**Impact**: The model faithfully represents the design. The real-system risk
is fingerprint collision, which is outside the TLA+ model's scope.

#### Finding T-4: CheckpointMonotonicity is correct but specified as a temporal property [LOW]

`CheckpointMonotonicity` uses the `[]` (always) temporal operator:
```tla
CheckpointMonotonicity ==
    [][\A s \in SourceIds : checkpoints'[s] >= checkpoints[s]]_vars
```

This is correctly formulated as a property (not an invariant) and is
included in both the safety and liveness configs. No issue.

---

### 3. Fairness

#### Finding T-5: Missing WF on AppendLine allows vacuous liveness [HIGH]

The fairness condition includes WF for all internal actions (TailerRead,
FormBatch, SendBatch, AckBatch, etc.) and Restart. However, `AppendLine`
has no fairness constraint.

This means TLC can satisfy `EventualEmission` vacuously:
```
EventualEmission ==
    \A v \in LineValues :
        (v \in SeqToSet(file_content) /\ alive /\ next_line_id > MaxLines)
            ~> (v \in SeqToSet(emitted))
```

The antecedent requires `next_line_id > MaxLines`, meaning all lines must
have been written. But without fairness on `AppendLine`, the model can
stutter forever without writing all lines, and the leads-to is vacuously
satisfied (the antecedent is never true).

However, `AppendLine` is an environment action (not under the system's
control), so WF is not appropriate -- the environment is not obligated to
write lines. The real question is: does the spec prove anything useful
about liveness?

With `MaxLines=3, MaxCrashes=1, MaxRotations=0, BatchSize=1`, TLC will
explore states where all 3 lines are written (since it explores all
reachable states including those where AppendLine fires). The liveness
check is not vacuous in practice -- TLC finds the relevant states. But
the semantic argument is fragile: it depends on TLC's exhaustive search,
not on the fairness specification.

**Recommendation**: Add `SF_vars(AppendLine)` (strong fairness) or at
minimum a constraint `<>(next_line_id > MaxLines)` as an assumption to
make the liveness proof non-vacuous by construction, not just by TLC
exploration strategy.

#### Finding T-6: No fairness on Crash allows infinite crashing [LOW]

`Crash` has no fairness constraint, which is correct -- the environment
should not be forced to crash. However, `Restart` has WF, meaning that
after a crash, the system eventually restarts. This is the right model
for a supervised process.

Note that with `MaxCrashes` bounded, the liveness model is safe from
infinite crash loops. No action needed.

---

### 4. Model Abstraction

#### Finding T-7: Line-level abstraction misses byte-level framing bugs [MEDIUM]

The spec models lines as atomic units. The `framer_buf` is a sequence of
complete lines. The comment in the spec acknowledges: "In line-level model,
lines are complete, so this is a buffer of lines awaiting batching."

Byte-level bugs the abstraction misses:

1. **Partial lines spanning multiple reads**: In the real system, a line
   may be split across two read() calls. The framer's remainder buffer
   accumulates bytes until a newline is found. The TLA+ spec cannot model
   this because it treats lines as indivisible. The Kani proofs on
   `CheckpointTracker` partially fill this gap (they model byte offsets
   and newline positions), but the Kani proofs do not model the framer's
   buffer content, only the offset arithmetic.

2. **Empty lines**: An empty line (`\n` at the start of a chunk) is a
   valid line in the real system. The TLA+ model uses `LineValues == 1..MaxLines`
   (positive naturals), so empty lines cannot be represented. Empty lines
   would still advance `processed_offset` in the byte model but produce
   no "line value" in the TLA+ model. This gap is unlikely to cause a
   checkpoint bug but could mask framing edge cases.

3. **Lines longer than read buffer**: If a single line exceeds the read
   buffer size, multiple reads accumulate remainder before the newline is
   found. The Kani `verify_no_data_loss_on_crash` harness partially models
   this (reads without newlines grow remainder), but the TLA+ spec does
   not model this scenario at all.

**Recommendation**: The Kani proofs on `CheckpointTracker` cover the byte-level
offset arithmetic. The remaining gap is the framer's buffer management
(concatenation of partial lines across reads), which has no formal model.
Consider adding a Kani proof for the actual `Framer` that checks: for any
sequence of byte chunks with newlines at known positions, the framer emits
exactly the expected complete lines.

---

### 5. Liveness

#### Finding T-8: EventualEmission may be too strong with crashes + rotations [MEDIUM]

`EventualEmission` claims that after all writes stop, every line in the
current file is eventually emitted. But:

1. With `MaxRotations > 0`: A line written to generation 1's file, then
   rotation happens, then crash. On restart, the rotated file is lost
   (restart sets `rotated_active = FALSE`). The line is in `file_content`
   of the old generation but the current `file_content` is the new (empty)
   file. So the antecedent `v \in SeqToSet(file_content)` refers to the
   *current* file's content at the time of evaluation, not all historical
   content. This makes the property weaker than it appears -- it only
   promises emission of lines in the *current* file.

2. The liveness config sets `MaxRotations=0`, sidestepping the rotation
   issue entirely. The property is never tested against rotation scenarios.

**Recommendation**: Either (a) test liveness with `MaxRotations >= 1`
(will likely fail, revealing the expected data loss on crash+rotation),
or (b) weaken `EventualEmission` to explicitly exclude the rotation+crash
case, and add a separate property: "without crashes, all lines in all
file generations are eventually emitted."

#### Finding T-9: Progress property references stale state [LOW]

```tla
Progress ==
    \A v \in LineValues :
        (alive /\ v \in SeqToSet(file_content) /\ read_offset < Len(file_content))
            ~> (v \in SeqToSet(emitted) \/ ~alive)
```

The clause `read_offset < Len(file_content)` makes the antecedent false
once the tailer catches up, so the property is vacuously true for most
states. This is not wrong, but it means Progress only asserts something
during the window between a write and the tailer catching up.

---

### 6. State Space

#### Finding T-10: Safety config constants may be too small for multi-rotation bugs [MEDIUM]

Safety config: `MaxLines=4, MaxCrashes=1, MaxRotations=1, BatchSize=2`.

This means at most 2 file generations (initial + 1 rotation), 4 total
lines, and batches of 2. Some scenarios that require larger constants:

1. **Two rotations without full drain**: `MaxRotations=1` means only one
   rotation can occur. A bug where the second rotation arrives before the
   first rotated file is fully drained cannot be found.

2. **BatchSize > lines available**: With `MaxLines=4` and `BatchSize=2`,
   at most 2 full batches can form. Interactions between 3+ batches
   and checkpoint advancement are not explored.

3. **Multiple crashes**: `MaxCrashes=1` means only one crash+restart cycle.
   Bugs that require crash-restart-crash (e.g., checkpoint from pre-first-crash
   batch being used after second restart) cannot be found.

**Recommendation**: Add a "stress" config with `MaxLines=6, MaxCrashes=2,
MaxRotations=2, BatchSize=2`. The state space will be larger but still
tractable for TLC (likely under 1M states).

#### Finding T-11: Coverage config has stale constant MaxBatches [LOW]

The coverage config `FileCheckpoint.coverage.cfg` includes:
```
MaxBatches = 2
```

But the TLA+ spec does not declare a `MaxBatches` constant. TLC may
silently ignore this or report an error. Either way, it is dead
configuration.

**Recommendation**: Remove `MaxBatches = 2` from the coverage config.

---

## Part 2: Kani Proofs Audit

### 1. Tautological Proofs

#### Finding K-1: verify_overflow_safety silently skips overflowing inputs [HIGH]

```rust
if tracker.read_offset.checked_add(n_bytes).is_some() {
    tracker.apply_read(n_bytes, last_newline_pos);
}
```

The harness skips reads that would overflow. This means Kani never actually
tests the overflow path -- it just proves "if we skip overflows, nothing
overflows." The comment says "In production code, we'd use checked_add"
but the actual `apply_read` implementation uses plain `+` arithmetic:

```rust
self.read_offset = old_read + n_bytes;
```

If production code is compiled in release mode (no overflow checks), a
near-u64::MAX `read_offset` with a large `n_bytes` would silently wrap,
causing `read_offset` to become smaller than `processed_offset` and
violating all invariants.

The proof should either:
1. Add overflow guards to the production code (`checked_add` + error
   handling), matching the Kani precondition, or
2. Remove the `if` guard in the Kani harness and let Kani find the
   overflow, proving it cannot happen under real-world constraints.

As written, the proof gives false confidence about overflow safety.

**Recommendation**: Add `checked_add` to `apply_read` in production code
and return an error on overflow. Then the Kani proof becomes meaningful.

### 2. Missing Properties

#### Finding K-2: No Kani proof links checkpoint_offset to actual line boundaries [MEDIUM]

The Kani proofs verify offset arithmetic invariants:
- `checkpoint_offset <= processed_offset <= read_offset`
- `processed_offset + remainder_len == read_offset`
- checkpoint monotonicity
- processed advances only on newline

But there is no proof that `processed_offset` actually corresponds to a
newline boundary in the input data. The `apply_read` API takes
`last_newline_pos` as a parameter and trusts the caller to provide the
correct value. If the caller reports a wrong newline position, the tracker
will happily compute a wrong `processed_offset` and nobody will catch it.

**Recommendation**: This is an interface trust boundary. Consider adding a
Kani proof for the framer that verifies: given arbitrary byte input, the
reported `last_newline_pos` is correct (i.e., the byte at that position
is actually `\n`).

#### Finding K-3: No proof for the at-least-once delivery guarantee [MEDIUM]

The TLA+ spec models the full pipeline (read -> frame -> batch -> send ->
ack -> checkpoint -> crash -> restart -> re-read). The Kani proofs only
model the `CheckpointTracker` state machine. There is no Kani proof that
verifies: "after restart, re-reading from `checkpoint_offset` will
re-process all lines that were not yet acknowledged."

This is the central correctness property. It is proven in TLA+ at the
line level but not at the byte level.

**Recommendation**: Add an integration-level Kani harness (or proptest)
that combines the `Framer` and `CheckpointTracker`: apply a sequence of
byte reads, checkpoint, restart, re-read from checkpoint, and verify that
all previously-incomplete lines are re-emitted.

### 3. Assume Coverage

#### Finding K-4: Resume offset range is unnecessarily restrictive [LOW]

In `verify_invariants_hold`, `verify_checkpoint_monotonicity`, and
`verify_no_data_loss_on_crash`:
```rust
let resume: u64 = kani::any_where(|&r: &u64| r <= 1_000_000);
```

This restricts the initial offset to [0, 1M]. The arithmetic is all
relative (additions/subtractions), so the starting offset should not
matter unless overflow is involved. For overflow, `verify_overflow_safety`
uses unconstrained `kani::any()`. The restriction is harmless but
unnecessary -- Kani's SAT solver would handle the full u64 range.

**Impact**: No loss of soundness. The restriction reduces solving time
at the cost of a slightly less general proof.

#### Finding K-5: Read size capped at 4096 bytes [LOW]

```rust
let n_bytes: u64 = kani::any_where(|&n: &u64| n > 0 && n <= 4096);
```

The cap at 4096 is reasonable (matches typical read buffer sizes) and
the arithmetic is linear, so the SAT solver's results generalize. No
issue.

### 4. Unwind Bounds

#### Finding K-6: Unwind bounds are correct for the loop structures [PASS]

| Harness | Unwind | Loop iterations | Sufficient? |
|---------|--------|-----------------|-------------|
| verify_invariants_hold | 7 | while i < 6 | Yes (7 = 6+1) |
| verify_checkpoint_monotonicity | 7 | while i < 6 | Yes |
| verify_no_data_loss_on_crash | 9 | while i < 4 + inner if | Yes (4 iters * 2 paths + 1) |
| verify_processed_advances_on_newline | 7 | while i < 6 | Yes |
| verify_overflow_safety | 5 | while i < 4 | Yes (5 = 4+1) |

All unwind bounds are at least loop-count + 1, which is the minimum
required for Kani to prove full loop coverage.

### 5. Connection to TLA+ Spec

#### Finding K-7: CheckpointTracker models a subset of the TLA+ state machine [HIGH]

The TLA+ spec has 12 actions over 20 state variables modeling the full
pipeline: file writes, rotation, truncation, tailer reads, framing,
batching, sending, acking, checkpointing, crash, and restart.

The `CheckpointTracker` Kani proofs model only 3 actions: Read, Checkpoint,
and Restart. It tracks 4 state variables: `read_offset`, `processed_offset`,
`remainder_len`, `checkpoint_offset`.

**Missing from the Kani model**:

1. **Source identity / per-source checkpoints**: The TLA+ spec's central
   insight (Finding 001) is that checkpoints must be per-source-identity.
   The `CheckpointTracker` has no concept of source identity -- it tracks
   a single file's offsets. The per-source coordination (which source's
   checkpoint to advance on ack) is not proven at the byte level.

2. **In-flight batches and ack**: The TLA+ spec models the `SendBatch`
   and `AckBatch` actions where the checkpoint only advances on
   acknowledgment. The `CheckpointTracker.apply_checkpoint()` advances
   immediately -- it does not model the in-flight window. If
   `apply_checkpoint` is called at the wrong time (before the output
   acks), the checkpoint would be too far ahead.

3. **File rotation / truncation**: The TLA+ spec models how file identity
   changes on rotation and truncation. The `CheckpointTracker` does not
   model these events at all -- it assumes a single stable file.

4. **The framer buffer subtraction (Finding 002)**: The TLA+ spec's
   `SendBatch` action computes `in_flight_offset = read_offset - Len(framer_buf)`.
   The `CheckpointTracker` directly maintains `processed_offset` which
   accounts for the remainder. These are equivalent concepts but the
   equivalence is not formally verified.

**Recommendation**: Add a refinement-mapping proof or proptest that
demonstrates: for any sequence of TLA+ actions restricted to a single
source, the `CheckpointTracker`'s state transitions produce the same
checkpoint values as the TLA+ spec. This would formally bridge the
two verification layers.

---

## Summary

| ID | Severity | Component | Finding |
|----|----------|-----------|---------|
| T-1 | MEDIUM | TLA+ | RotateFile does not clear framer_buf; possible silent data loss in model |
| T-2 | HIGH | TLA+ | NoCorruption does not check ordering, completeness, or duplicates |
| T-3 | LOW | TLA+ | CheckpointBounded gap on identity reuse (inherent abstraction limit) |
| T-4 | LOW | TLA+ | CheckpointMonotonicity correctly specified |
| T-5 | HIGH | TLA+ | No fairness on AppendLine; EventualEmission may be vacuously satisfied |
| T-6 | LOW | TLA+ | No fairness on Crash is correct by design |
| T-7 | MEDIUM | TLA+ | Line-level abstraction misses byte-level framing bugs |
| T-8 | MEDIUM | TLA+ | EventualEmission not tested with rotations; may be too strong |
| T-9 | LOW | TLA+ | Progress property vacuously true once tailer catches up |
| T-10 | MEDIUM | TLA+ | Safety constants too small for multi-rotation or multi-crash bugs |
| T-11 | LOW | TLA+ | Coverage config has stale MaxBatches constant |
| K-1 | HIGH | Kani | verify_overflow_safety skips overflows instead of proving absence |
| K-2 | MEDIUM | Kani | No proof that processed_offset corresponds to actual newline boundary |
| K-3 | MEDIUM | Kani | No proof for at-least-once delivery at byte level |
| K-4 | LOW | Kani | Resume offset unnecessarily restricted to 1M |
| K-5 | LOW | Kani | Read size cap at 4096 is reasonable |
| K-6 | PASS | Kani | Unwind bounds are correct |
| K-7 | HIGH | Kani | CheckpointTracker models only a subset of TLA+ state machine; no refinement mapping |

### Critical/High findings requiring action:

1. **T-2 (HIGH)**: Strengthen `NoCorruption` to check ordering and add `NoDuplicateEmission`.
2. **T-5 (HIGH)**: Add fairness or antecedent constraint to make `EventualEmission` non-vacuous by construction.
3. **K-1 (HIGH)**: Either add `checked_add` to production code or remove the guard in the Kani harness.
4. **K-7 (HIGH)**: Bridge the TLA+ and Kani verification layers with a refinement mapping or equivalence test.
