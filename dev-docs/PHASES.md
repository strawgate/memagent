# Implementation Phases

Current work tracked in GitHub epic #262.

## Phase 0: Kani prototype — GO/NO-GO (#263)

Add Kani to workspace. Prove `prefix_xor` and `compute_real_quotes`
exhaustively. Validate tooling works, CI works, developer experience
is acceptable.

**If Kani fails:** fall back to proptest-only. Still do the crate
restructuring for discipline benefits.

## Phase 1: Crate restructuring (#264, #265, #266, #267)

Split logfwd-core into proven core + satellite crates.

```
1a: Create logfwd-arrow     (move builders + SIMD)      → Copilot
1b: Create logfwd-input     (move file tailer + IO)     → Copilot
1c: Move remaining impure   (enrichment, compress, etc) → Copilot
1d: Tighten core to no_std  (forbid unsafe, strict CI)  → Us
```

1a and 1b run in parallel. 1c depends on both. 1d depends on all.

## Phase 2: Kani Tier 1 proofs (#268)

Exhaustive proofs for all bitmask operations, varint roundtrip,
parse_int_fast, pipeline state machine single-step. Function
contracts for compositional verification. Proof coverage CI.

## Phase 3: Scanner restructure + FieldSink (#269)

Rename ScanBuilder → FieldSink. Ensure scan loop works in no_std.
Add bounded Kani proof for scan_line. proptest oracle vs serde_json.

## Phase 4: Pipeline state machine + BatchToken (#270)

Extract run_async decisions into pure state machine in core.
Kani exhaustive single-step. proptest random event sequences.
BatchToken #[must_use] linear type. Wire into run_async.

## Phase 5: proptest state machines + CI hardening (#271)

proptest-state-machine for FormatParser and CriReassembler.
Proof coverage enforcement. cargo-mutants weekly. cargo-vet.

## Phase 6: TLA+ pipeline specification (#272)

Model batching/timeout/shutdown protocol. Prove liveness (data is
never abandoned) and fairness (no input starved). Design-level
artifact, separate from code.

## Parallel work (not blocked by phases)

| Issue | What | Assignee |
|-------|------|----------|
| #273 | Fix offset_of u32 truncation | Copilot |
| #274 | Fix row_count u32 overflow | Copilot |
| #275 | Fix CRI silent truncation | Design needed |
| #285 | Fix OTLP type suffix assumption | Copilot |
| #279 | Arrow version upgrade | Us |
| #252 | Async HTTP client (reqwest) | After #221 merges |
| #205 | UTF-8 validate once | Independent |
