# Architecture Research Synthesis

> Five workstreams, 16 Codex Cloud attempts, one recommendation memo.
> Fan-in from `fanout-manifest.json` (2026-04-19).

---

## Inputs

| # | Workstream | Issue | Attempts | Status | Deliverable |
|---|-----------|-------|----------|--------|-------------|
| 1 | Delivery contract state machine | #1312 | 4 | ✅ READY | `delivery-contract-state-machine.md` (408 lines) |
| 2 | Processor chain architecture | #1363 | 3 | ✅ READY | `processor-chain-architecture-evaluation.md` (387 lines) |
| 3 | Tagged enum config migration | #1101 | 3 | ✅ READY | `tagged-enum-config-migration-plan.md` (544 lines) |
| 4 | File tailer typestate model | #1310 | 4 | ✅ READY | `file-tailer-typestate-model.md` (492 lines) |
| 5 | OTLP projection gap analysis | #823 | 2 | ✅ READY | `otlp-projection-gap-analysis.md` (234 lines, from diff) |

All tasks completed. Task 5 was rate-limited during collection but successfully extracted from diff.

---

## Convergence

Strong consensus across attempts on all five workstreams:

1. **Delivery contract** — All 4 attempts identified the same core bug: mixed `Ok + Rejected` fanout outcomes collapse to global `Ok`, causing checkpoint advancement past undelivered data. All recommend a rich `ContractOutcome` enum preserving per-sink evidence until checkpoint policy is applied.

2. **Processor chain** — All attempts recommend **Hybrid topology (Option 3)**: keep processors inline with scan+SQL, separate only output I/O. Full channel-per-stage (Option 2) deferred until #1409 pipeline compiler contracts mature. Channel overhead analysis shows <2.6% of one core even at worst-case batch sizes.

3. **Tagged enum config** — All attempts confirm `InputConfig` is already migrated to `#[serde(tag = "type")]`. All recommend **dual-parse** (`untagged` wrapper containing tagged V2 + legacy V1) for `OutputConfig` migration.

4. **File tailer typestate** — All attempts found 11–15 implicit states in `FileReader`. All recommend a phased extraction: state types first (no behavior change), then consuming transition methods, then proofs. Attempts 1 and 4 converged on 15 states; attempts 2 and 3 found 11 (smaller scope, merged substates).

5. **OTLP projection** — Both attempts agree projection is **not ready for default**. Blocking gaps: AnyValue array/kvlist not directly handled, still behind experimental cfg, no committed benchmark artifacts.

---

## Disagreements

Minor divergences, none changing the direction:

| Area | Divergence | Resolution |
|------|-----------|------------|
| Delivery contract enum naming | `ContractOutcome` vs `DeliveryClassification` | Name is cosmetic; adopt `ContractOutcome` (clearest) |
| Tailer state count | 15 vs 11 states | 15 is more granular (separates truncation substates); use 15 for typestate design, group to ~8 for TLA+ abstraction |
| Processor: where to put enrichment tables | Inline vs separate enrichment stage | Keep inline per-batch; network enrichment (HTTP callout) is the only case for separate stage (not yet needed) |
| Config: dual-parse vs hard cutover | One attempt suggested pure `serde(tag)` now | Dual-parse is strictly safer given active feature PRs (#2194–#2200); adopt dual-parse |

---

## Repo fit

Cross-checked each deliverable against actual source:

### Delivery contract
- **Verified**: `SendResult` enum exists in `logfwd-output/src/sink/mod.rs` — `Ok`, `Rejected(String)`, `RetryAfter(Duration)`, `IoError(anyhow::Error)`.
- **Verified**: `AsyncFanoutSink` reduces per-child outcomes to single `SendResult` via `finalize_fanout_outcome` — collapse logic confirmed.
- **Verified**: `default_ticket_disposition` in pipeline maps `is_delivered()` → Ack, `is_permanent_reject()` → Reject, else → Hold.
- **Gap confirmed**: mixed `Ok + Rejected` returns `Ok`. This is a real correctness bug for multi-output configs.

### Processor chain
- **Verified**: I/O → CPU worker split already exists with bounded channel (capacity 4 in pipeline config).
- **Verified**: Output worker pool dispatches via `WorkerPool::dispatch_batch`.
- **Verified**: No separate processor stage channel exists today — processors run inline in pipeline submit path.
- **Fit**: Hybrid topology matches the existing architecture's grain. No structural leap required.

### Tagged enum config
- **Verified**: `InputConfig` uses `#[serde(tag = "type")]` with per-variant structs and `deny_unknown_fields`.
- **Verified**: `OutputConfig` is flat — 26 optional fields, type-dispatched via manual `validate.rs` matrix.
- **Verified**: Active PRs #2194–#2200 add more fields to the flat struct, increasing urgency.
- **Fit**: Dual-parse is the right call; hard cutover would break existing user configs.

### File tailer typestate
- **Verified**: `FileReader` in `crates/logfwd-io/src/tail/reader.rs` manages open/read/truncate/eof/evict/offset mutation in one large struct.
- **Verified**: `EofState` reducer exists with `idle_since` + `emitted` + `idle_polls`.
- **Verified**: `TailLifecycle.tla` models only EOF + error backoff reducers, not per-file lifecycle.
- **Gap confirmed**: 5 TLA+ divergences identified are real — the TLA+ spec needs extension.

### OTLP projection
- **Verified**: Projection is behind `otlp-research` feature flag.
- **Verified**: `ProjectedFallback` mode exists — falls back to Prost structured decode for unsupported AnyValue types.
- **Verified**: No committed benchmark result artifacts in-tree for projection vs structured decode.
- **Fit**: Correct to not promote to default yet. Blocking gaps are real.

---

## Evidence quality

| Workstream | Confidence | Grade | Notes |
|-----------|-----------|-------|-------|
| Delivery contract | **High** | Decision-grade | Bug is code-grounded; fix direction is clear; TLA+ sketch provided |
| Processor chain | **High** | Decision-grade | Channel overhead quantified; checkpoint boundary implications mapped |
| Tagged enum config | **High** | Decision-grade | Full field usage matrix by output type; PR breakdown provided |
| File tailer typestate | **Medium-High** | Decision-grade for Phase 1; directional for Phase 3 | State catalog is thorough; TLA+ extension plan is sketch-level |
| OTLP projection | **Medium** | Directional | Correct go/no-go assessment, but missing quantitative benchmark data to set cutover thresholds |

---

## Recommendations

### Adopt now (create issues / begin implementation)

1. **Delivery contract `ContractOutcome`** — The mixed-fanout collapse is a correctness bug (#1312). Phase 1 (add `ContractOutcome` in parallel with existing enums + telemetry counters) is low-risk and should ship soon.

2. **Tagged enum `OutputConfig` migration** — The flat struct is actively accumulating technical debt from #2194–#2200. Phase 1 (introduce `OutputConfigV2` + compat parser, no runtime changes) can ship independently and unblocks cleaner feature PRs.

3. **File tailer typestate Phase 1** — Extract explicit state types with no behavior change. This is a refactor-only PR that improves readability and sets up proofs.

### Benchmark first

4. **OTLP projection promotion** — Before changing defaults, need committed benchmark artifacts showing decode improvement on primitive-dominant payloads and regression bounds on complex payloads. Create a benchmark tracking issue.

### Defer (dependent on prerequisites)

5. **Processor chain topology change** — The Hybrid topology is correct direction, but implementation is blocked on delivery contract (#1312) and pipeline compiler (#1409) maturity. Phase A (stateful processor seam completion) can start now; Phases B–C wait.

6. **File tailer typestate Phase 3 (proofs)** — Depends on Phase 1 completion and TLA+ spec extension. Worth planning but not starting immediately.

---

## Cross-cutting dependency graph

```
Tagged enum OutputConfig Phase 1        Delivery contract Phase 1
        │                                        │
        ▼                                        ▼
Output feature PRs (#2194-#2200)        Delivery contract Phase 2
                                                 │
                                                 ▼
                                        Processor chain Phase B
                                         (output separation)
                                                 │
                                                 ▼
                                        Pipeline compiler (#1409)
                                                 │
                                                 ▼
                                        Processor chain Phase C
                                         (compiler-oriented)

File tailer Phase 1 (refactor) ──────► File tailer Phase 2 (transitions)
                                                 │
                                                 ▼
                                        File tailer Phase 3 (proofs + TLA+)

OTLP benchmark issue ──────────────────► OTLP projection promotion decision
```

---

## Recommended next steps

1. **Create 3 implementation issues** from this synthesis:
   - `feat: introduce ContractOutcome enum for delivery contract (#1312)` — Phase 1 scope from deliverable §5–§6
   - `refactor: introduce OutputConfigV2 tagged enum with compat parser (#1101)` — Phase 1 scope from deliverable §4–§5
   - `refactor: extract file tailer lifecycle state types (#1310)` — Phase 1 scope from deliverable §6

2. **Create 1 benchmark tracking issue:**
   - `bench: OTLP projection decode benchmarks and cutover gates (#823)` — gate criteria from deliverable §9

3. **Update architecture docs:**
   - Add delivery contract state diagram to `dev-docs/ARCHITECTURE.md`
   - Update `dev-docs/VERIFICATION.md` with tailer TLA+ gap analysis
   - Reference this synthesis from the 5 parent research issues

4. **Commit this synthesis** to the research directory and link from issue comments.

---

## Appendix: deliverable locations

All deliverables are in `dev-docs/research/fanout-2026-04-19-architecture-research/cloud-artifacts/`:

| Task | Path |
|------|------|
| 1 | `output-delivery-contract-…/extracted/dev-docs/research/delivery-contract-state-machine.md` |
| 2 | `processor-chain-…/extracted/dev-docs/research/processor-chain-architecture-evaluation.md` |
| 3 | `tagged-enum-config-…/extracted/dev-docs/research/tagged-enum-config-migration-plan.md` |
| 4 | `file-tailer-typestate-…/extracted/dev-docs/research/file-tailer-typestate-model.md` |
| 5 | OTLP projection gap analysis — extracted from Codex Cloud task diff output |
