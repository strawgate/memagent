# Architecture Research Synthesis

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Architecture research fanout — workstream synthesis
> **Sources:** Five parallel workstreams evaluated via AI-assisted analysis

---

## Inputs

| # | Workstream | Issue | Status | Key Finding |
|---|-----------|-------|--------|-------------|
| 1 | Delivery contract state machine | #1312 | ✅ READY | Mixed `Ok + Rejected` fanout outcomes collapse to global `Ok`, causing checkpoint advancement past undelivered data |
| 2 | Processor chain architecture | #1363 | ✅ READY | Hybrid topology (Option 3) recommended: keep processors inline with scan+SQL, separate only output I/O |
| 3 | Tagged enum config migration | #1101 | ✅ READY | Dual-parse approach (tagged V2 + legacy V1) recommended for `OutputConfig` |
| 4 | File tailer typestate model | #1310 | ✅ READY | 11–15 implicit states found in `FileReader`; phased extraction recommended |
| 5 | OTLP projection gap analysis | #823 | ✅ READY | Projection not ready for default; blocking gaps remain |

---

## Convergence

### 1. Delivery Contract

All analysis attempts identified the same core bug: `AsyncFanoutSink::finalize_fanout_outcome` collapses mixed `Ok + Rejected` fanout outcomes to a single `Ok`, causing checkpoint advancement past undelivered data.

**Recommendation:** Introduce a `ContractOutcome` enum preserving per-sink evidence (`Ok`, `Rejected`, `RetryAfter`, `IoError`) until checkpoint policy is applied.

**Verification:** `SendResult` enum exists in `logfwd-output/src/sink/mod.rs` — bug is code-grounded.

### 2. Processor Chain

Channel overhead analysis shows <2.6% of one core even at worst-case batch sizes. No structural leap required.

**Recommendation:** Hybrid topology — keep processors inline with scan+SQL, separate only output I/O. Full channel-per-stage deferred until #1409 matures.

**Verification:** I/O → CPU worker split already exists with bounded channel (capacity 4 in pipeline config).

### 3. Tagged Enum Config

`InputConfig` already uses `#[serde(tag = "type")]`. `OutputConfig` remains flat — 26 optional fields with manual dispatch matrix. Active PRs #2194–#2200 are adding more fields, increasing urgency.

**Recommendation:** Dual-parse approach: introduce `OutputConfigV2` tagged enum with compat parser. Hard cutover would break existing user configs.

**Verification:** Field usage matrix confirms flat struct is actively accumulating technical debt.

### 4. File Tailer Typestate

`FileReader` in `crates/logfwd-io/src/tail/reader.rs` manages open/read/truncate/eof/evit/offset mutation in one large struct. `EofState` reducer exists but models only EOF + error backoff, not full per-file lifecycle.

**Recommendation:** Phased extraction — state types first (no behavior change), then consuming transition methods, then proofs. 15-state model is more granular than the 11-state alternative.

**Verification:** Gap analysis confirms TLA+ spec needs extension to cover full lifecycle.

### 5. OTLP Projection

Projection remains behind `otlp-research` feature flag. `ProjectedFallback` mode exists but `complex-anyvalue` payloads still fail fallback gate.

**Recommendation:** Do not switch defaults yet. Keep `prost` as default, keep projected modes explicit. Optimize fallback classification before revisiting.

**Verification:** No committed benchmark artifacts in-tree; gate criteria require quantitative data.

---

## Recommendations

### Adopt Now

1. **Delivery contract `ContractOutcome`** — Phase 1 (add enum + telemetry counters) is low-risk; correctness bug for multi-output configs.

2. **Tagged enum `OutputConfigV2` migration** — Phase 1 can ship independently; unblocks cleaner feature PRs.

3. **File tailer typestate Phase 1** — Extract explicit state types; refactor-only PR improves readability and sets up proofs.

### Benchmark First

4. **OTLP projection promotion** — Need committed benchmark artifacts before changing defaults. Create tracking issue with gate criteria.

### Defer

5. **Processor chain topology change** — Blocked on delivery contract (#1312) and pipeline compiler (#1409) maturity.

6. **File tailer typestate Phase 3 (proofs)** — Depends on Phase 1 completion and TLA+ spec extension.

---

## Dependency Graph

```
Tagged enum OutputConfig Phase 1        Delivery contract Phase 1
        │                                        │
        ▼                                        ▼
Output feature PRs (#2194-#2200)        Delivery contract Phase 2
                                                 │
                                                 ▼
                                        Processor chain Phase B
                                                 │
                                                 ▼
                                        Pipeline compiler (#1409)

File tailer Phase 1 ──────────────► File tailer Phase 2 (transitions)
                                                 │
                                                 ▼
                                        File tailer Phase 3 (proofs)

OTLP benchmark issue ──────────────► OTLP projection promotion decision