# WS07 — #1363 Processor Architecture Remainder (2026-04-09)

## Scope and framing

This document re-baselines issue **#1363** against the current `main` branch by:

1. inventorying what is already landed versus still unresolved,
2. proposing a minimal 2–4 slice implementation plan with dependencies,
3. comparing at least two sequencing options, and
4. validating one concrete seam extraction with code in this branch.

The baseline intent for #1363 is inferred from repository artifacts tied to that issue and adjacent runtime architecture docs, especially the per-input SQL design analysis and the runtime pipeline implementation.

---

## 1) Re-baseline inventory: landed vs open

## 1.1 Landed on main (already done)

### A. Per-input scan/transform boundary exists

`Pipeline` now carries one `InputTransform` per input (per-input scanner + SQL transform), and `from_config` resolves per-input SQL (`input.sql` with pipeline-level fallback). This is a major architecture intent of #1363 that is already implemented and should not be re-proposed.

- `Pipeline` stores `input_transforms: Vec<InputTransform>` aligned with inputs.
- `InputTransform` contains `Scanner`, `SqlTransform`, and input identity.
- `from_config` computes `input_sql = input_cfg.sql.or(pipeline_sql)` and builds scanner config per input.

**Implication:** #1363 is no longer at “single shared scanner/SQL” stage; any next planning should assume split per-input transform is baseline.

### B. Input I/O and compute are split into dedicated worker threads

`input_pipeline.rs` now owns the per-input two-thread model:

- I/O worker: polls source + accumulates bytes,
- CPU worker: scanner + SQL execution,
- bounded I/O→CPU channel (capacity 4),
- shutdown/drain cascade explicitly documented in code comments.

**Implication:** this was a large unknown/risk area in older architecture discussions and is now landed.

### C. Runtime crate owns processor chain between SQL and output

Processor semantics are centralized in `logfwd-runtime` and output semantics in `logfwd-output`, consistent with the architecture layer split.

- Processor contract (`process`, `flush`, `is_stateful`) and chain execution helpers exist.
- Pipeline submission path enforces checkpoint dispositions (ack/hold/reject) based on processor/output outcomes.

**Implication:** next work should refine reducer seams and stateful processor readiness, not re-open crate ownership.

### D. Fanout output correctness hardening landed

`logfwd-output` already includes explicit fanout reduction logic with behavior coverage (result precedence, health rollups, tests/Kani references in docs).

**Implication:** processor architecture remainder should target runtime orchestration seams rather than fanout sink fundamentals.

---

## 1.2 Still open / unresolved on main

### 1) Stateful processors remain intentionally blocked in production path

`Pipeline::with_processor` and `with_processors` explicitly panic on stateful processors (`#1404`), while partial heartbeat/flush scaffolding exists behind TODOs in the run loop.

- Heartbeat pass-through code path exists for stateful processors.
- Cascading flush path exists.
- Both paths currently log TODOs that emitted rows are not submitted to output worker pool.

**Why this matters for #1363 remainder:** processor architecture is still incomplete for stateful behavior because checkpoint semantics and output submission integration are not finished.

### 2) Submission logic still couples async shell + decision reducer concerns

Although functionally correct, `submit_batch` has been carrying decision logic (processor error mapping, concat semantics, ticket disposition triggers) tightly coupled with async flow. This makes future stateful semantics and policy changes harder to iterate safely.

### 3) Schema divergence behavior across processor outputs is fail-fast only

If processors return multiple batches with incompatible schemas, submission rejects the batch. This is safe, but the policy/options for schema reconciliation or explicit contract enforcement are not yet promoted to a first-class architecture decision.

### 4) #1363 scope needs narrow “next slices” to avoid umbrella drift

The codebase has moved past broad restructuring; the unresolved value now lies in small seam-oriented increments with explicit test/verification updates.

---

## 2) Validating prototype implemented in this branch

## Prototype: extract processor-stage reducer seam from submit path

I implemented a concrete seam extraction in runtime pipeline code:

- Added `pipeline/processor_stage.rs` with pure-ish reducer function:
  - `run_processor_stage(processors, batch, metadata) -> ProcessorStageResult`
  - result variants: `Forward`, `Hold`, `AckDrop`, `Reject`
- Updated `pipeline/submit.rs` to consume `ProcessorStageResult` and perform checkpoint disposition + metrics updates in one place.
- Added targeted unit tests for seam behavior:
  - empty chain forwards,
  - transient error → hold,
  - permanent error → ack-drop,
  - incompatible multi-batch schemas → reject.

## Why this is useful as a validating prototype

This extraction is intentionally small but demonstrates the expected architecture direction for the remainder of #1363:

- **separates pure policy reduction from async orchestration**,
- **creates a stable integration seam for stateful processor follow-up**, and
- **lets future policy changes be unit-tested without end-to-end pipeline harnesses**.

---

## 3) Next minimal slices (2–4), with dependencies

Below are four concrete slices; execute top 2–3 as minimum depending on capacity.

## Slice 1 — Processor stage reducer extraction (DONE in this branch)

**Goal:** isolate processor stage decisions from async submit loop.

- Deliverables:
  - `run_processor_stage` seam + tests.
  - `submit_batch` simplified to orchestration + ticket/metrics transitions.
- Dependencies: none.
- Risk: low (behavior-preserving refactor).
- Verification:
  - unit tests in seam module,
  - targeted runtime crate tests.

## Slice 2 — Stateful processor output submission path (the #1404 bridge)

**Goal:** complete heartbeat/cascading-flush output submission when stateful processors emit rows.

- Deliverables:
  - route heartbeat-emitted batches through the same submission/ack path,
  - route cascading flush emissions through output pool before final drain,
  - remove/replace existing TODO logging-only behavior.
- Dependencies:
  - Slice 1 seam (to reuse reducer behavior for emitted batches).
- Risk: medium-high (checkpoint correctness + shutdown ordering).
- Verification:
  - integration tests for heartbeat emission and shutdown flush delivery,
  - checkpoint advancement tests (no duplicate re-read after restart),
  - docs update in `dev-docs/ARCHITECTURE.md` and `dev-docs/VERIFICATION.md` if invariants change.

## Slice 3 — Explicit processor output schema contract

**Goal:** make schema guarantees explicit between processor chain and output submission.

- Deliverables (choose one):
  - Option A: enforce “single schema per logical batch” contract at processor boundary with clearer error surfaces and metrics, or
  - Option B: add deterministic union/padding adapter for processor outputs (if cross-schema fanout is desired).
- Dependencies:
  - independent of Slice 2 for Option A,
  - depends on clear checkpoint semantics if Option B changes batch semantics.
- Risk: medium (data shape compatibility and perf impacts).
- Verification:
  - targeted tests for mixed-schema processor outputs,
  - transform/output compatibility tests,
  - perf spot check if padding path is introduced.

## Slice 4 — Processor architecture docs and change-map sync

**Goal:** prevent future drift by documenting the settled seam and stateful path expectations.

- Deliverables:
  - update `dev-docs/ARCHITECTURE.md` processor stage diagram,
  - if invariants changed, update `dev-docs/VERIFICATION.md` expectations,
  - if crate boundary expectations changed, update `dev-docs/CRATE_RULES.md` / crate AGENTS as needed.
- Dependencies:
  - after Slice 2 and/or 3 lands.
- Risk: low.
- Verification:
  - doc review checklist from `dev-docs/CHANGE_MAP.md`.

---

## 4) Sequencing options (compare at least two)

## Option A (recommended): Reliability-first

1. Slice 1 (seam extraction) ✅
2. Slice 2 (stateful output submission/ack correctness)
3. Slice 3 (schema contract hardening)
4. Slice 4 (docs sync)

### Pros

- Addresses highest correctness gap (stateful processor behavior) earlier.
- Builds on seam extraction to reduce regression surface in async paths.
- Better aligns with existing TODOs and panic guardrails.

### Cons

- More integration-heavy sooner (checkpoint + shutdown interactions).

## Option B: Contract-first

1. Slice 1 (seam extraction) ✅
2. Slice 3 (schema contract first)
3. Slice 2 (stateful submission)
4. Slice 4 (docs sync)

### Pros

- Clarifies data-shape behavior early for downstream teams.
- Might reduce ambiguity in processor implementer expectations.

### Cons

- Leaves known stateful processor production gap open longer.
- Could force rework in schema decisions once stateful flush/heartbeat semantics are fully wired.

## Recommendation

Choose **Option A** unless there is an immediate external dependency on processor multi-schema behavior. The unresolved correctness risk is dominated by stateful processor submission/checkpointing, not by schema policy expressiveness.

---

## 5) Regression risk and verification needs for the immediate next slice (Slice 2)

## Primary regression risks

1. **Checkpoint drift**: emitted heartbeat/flush rows may be delivered without correct ticket lifecycle transitions.
2. **Duplicate delivery or data loss**: shutdown ordering bugs can duplicate or drop stateful buffered rows.
3. **Drain deadlocks/backpressure**: routing extra emitted batches could stall shutdown if not integrated with existing pool/drain loop.

## Verification plan

- Add focused runtime integration tests:
  - stateful processor emits on heartbeat → output lines increase and checkpoints advance as expected,
  - stateful processor emits on cascading flush during shutdown → data delivered before pipeline exits,
  - transient output error during flush path → hold/retry semantics preserved.
- Add invariant assertions:
  - no `Sending` ticket left unresolved at end of run,
  - no final drain path that bypasses output pool submission.
- Update contributor docs if semantics change:
  - architecture flow text,
  - verification expectations around processor lifecycle.

---

## 6) Minimal execution checklist mapping

- ✅ Inventory landed vs open completed.
- ✅ Identified 2–4 implementation slices with dependencies.
- ✅ Prototyped one concrete seam extraction (processor stage reducer).
- ✅ Assessed regression risk + verification needs for next slice.

