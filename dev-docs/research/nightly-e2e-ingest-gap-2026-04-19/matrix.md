# Nightly E2E / Ingest Gap Failure-Class Matrix (2026-04-19)

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Nightly E2E ingest gap analysis

## Scope and guardrails

- Parent issue: #1500 (`work-unit: nightly e2e stability — durable tracker + ingest gap`).
- This artifact is investigation-first and reproduction-first; **no production behavior changes** are proposed here.
- Focused areas inspected:
  - Nightly workflow lanes and reporting behavior.
  - `tests/e2e` harness timing, gating, and failure categorization.
  - File-tail / source-id / framing / checkpoint boundaries in `logfwd-io` and `logfwd` compliance tests.

---

## Symptoms and linked issue classes

> Note on #1559 / #1578 mapping: no direct references were discoverable in-repo by text search (`git log --grep`, `rg`), so this matrix maps observed failure shapes to issue *classes* rather than asserting exact issue content.

| Symptom shape | Likely issue class mapping | Where observed |
|---|---|---|
| Nightly/e2e run reports ingest shortfall (`VERIFY_TIMEOUT`, low `lines`) despite generator completion. | #1559/#1578-style “ingest gap under orchestration race” class (inference). | `tests/e2e/run.sh` verify loop and gating tokens. |
| File compliance suite intermittently times out waiting for initial ingest in multi-test run, while each failing test passes in isolation. | Harness race / test interference class (high confidence). | `cargo test -p logfwd --test it compliance_file` vs individual `--exact` runs. |
| Rotation/truncate paths explicitly tolerate duplication or ignore copytruncate due to known race. | File-tail transition and checkpoint/framing coupling class. | `crates/logfwd/tests/it/compliance_file.rs` comments and assertions. |
| Partial-line + EOF + multi-source correctness is contract-tested and generally strong. | Evidence *against* primary framing/source-mix root cause for current gap reports. | `crates/logfwd-io/tests/it/file_boundary_contract.rs`. |

---

## Failure-class taxonomy and evidence

### 1) Harness race

**Hypothesis:** nightly/e2e ingest gaps are at least partly driven by orchestration timing races, not only tailer correctness.

- Evidence supporting:
  - `tests/e2e/run.sh` contains explicit race mitigations (generator-running gate, marker-emitted gate, port-forward readiness gate, bounded retry loop with backoff), indicating prior known harness-ordering risk.
  - `run.sh` still uses bounded timeout with a line-count range (`10..15`), which can still fail under environmental slowness.
  - In this run, the unit smoke harness (`bash tests/e2e/run_smoke_test.sh`) passed, validating gate ordering behavior but not real KIND delivery.
  - `cargo test -p logfwd --test it compliance_file` failed in aggregate with two timeout failures:
    - `compliance_file_rotate_create`: timeout waiting for first 5000 lines.
    - `compliance_glob_new_files`: timeout waiting for first glob file.
  - Re-running those same tests individually with `--exact` passed quickly.
- Evidence weakening:
  - No deterministic local repro yet of `tests/e2e/run.sh` against real KIND in this environment.
- Confidence: **High** (for test-harness/interference component), **Medium** (for nightly KIND ingest-gap contribution).

### 2) File-tail state transition

**Hypothesis:** rotate/truncate/delete-recreate transitions can under/over-deliver around state edges.

- Evidence supporting:
  - `compliance_file.rs` documents non-exactly-once expectations in truncation/delete-recreate flows and allows over-delivery bounds.
  - `copytruncate` test is `#[ignore]` with explicit known data-loss race note.
  - `discovery.rs` rotation decision combines inode/device/fingerprint/delete-handle checks; edge handling is complex and sensitive to filesystem semantics.
  - `reader.rs` emits `Truncated`, `Rotated`, and data events based on offset and identity/fingerprint reconciliation, with special-case empty sentinel logic.
- Evidence weakening:
  - Focused tailer unit test (`tail::tests::test_tail_truncation`) passed locally.
- Confidence: **Medium-High**.

### 3) Framing/remainder coupling

**Hypothesis:** ingest gaps may stem from newline boundary bookkeeping, EOF flush, or partial remainder coupling with checkpoints.

- Evidence supporting:
  - Historically plausible class for line-loss bugs.
- Evidence weakening:
  - `file_boundary_contract.rs` has targeted replay-style contract tests for:
    - checkpoint only advancing at real newline boundaries,
    - per-source remainder isolation for globs,
    - EOF remainder flush preserving per-source identity,
    - restart/restore preserving complete vs partial source isolation.
  - Entire `file_boundary_contract` integration target passed locally.
- Confidence: **Low-Medium** as primary current root cause.

### 4) Source identity / source metadata

**Hypothesis:** source-id instability or collisions cause wrong checkpoint application or cross-file contamination.

- Evidence supporting:
  - `reader.rs` includes source-id/path dual restoration logic for evicted offsets; this is subtle and can regress.
  - `SourceId(0)` sentinel handling means certain paths may temporarily carry unknown identity.
- Evidence weakening:
  - Contract tests assert source isolation and checkpoint independence across glob files.
  - No direct failing signal in current focused runs.
- Confidence: **Medium**.

### 5) Checkpoint advancement

**Hypothesis:** checkpoint offset moves ahead/behind durable-safe boundary causing apparent ingest gaps after restarts or transitions.

- Evidence supporting:
  - `reader.rs` has multiple clamp/reset behaviors when saved offset > file size (path and source variants), indicating known edge conditions.
  - Compliance tests use line-count waits only; they do not assert per-source checkpoint transitions in those scenarios.
- Evidence weakening:
  - Dedicated `file_boundary_contract` checkpoint assertions passed.
- Confidence: **Medium** (especially for restart/rotation interactions not fully covered by current compliance tests).

### 6) E2E environment / KIND / collector behavior

**Hypothesis:** environment-level factors (cluster startup, DaemonSet rollout, port-forward stability, collector responsiveness) produce false ingest-gap failures.

- Evidence supporting:
  - `run.sh` has explicit fail tokens for readiness and connectivity (`RECEIVER_NOT_READY`, `FF_ROLLOUT_TIMEOUT`, `PORT_FORWARD_*`, `VERIFY_TIMEOUT`).
  - Existing checked-in e2e result artifacts contain only `error: context "kind-kind-cri-smoke" does not exist`, demonstrating environment fragility for result capture/replay.
  - `run_smoke_test.sh` validates ordering via stubs, not real K8s/data-plane behavior.
- Evidence weakening:
  - No fresh full KIND e2e run was executed here (cost/time + environment prerequisites).
- Confidence: **Medium-High** for contribution to nightly instability.

### 7) Unknown / needs reproduction

- Missing direct issue-body mapping for #1559/#1578 in local repo context.
- Missing deterministic reproducer that isolates one failure class while freezing others.
- Confidence: **High that gap remains**, by definition.

---

## Exact code/test evidence inspected

- Workflow and harness:
  - `.github/workflows/nightly-testing.yml`
  - `tests/e2e/run.sh`
  - `tests/e2e/run_smoke_test.sh`
- File compliance and tail internals:
  - `crates/logfwd/tests/it/compliance_file.rs`
  - `crates/logfwd-io/src/tail/tailer.rs`
  - `crates/logfwd-io/src/tail/reader.rs`
  - `crates/logfwd-io/src/tail/state.rs`
  - `crates/logfwd-io/src/tail/discovery.rs`
  - `crates/logfwd-io/src/tail/tests.rs`
  - `crates/logfwd-io/tests/it/file_boundary_contract.rs`
- Existing e2e artifacts:
  - `tests/e2e/results/kind-cri-smoke/*.txt|*.log` (all currently show missing context message).

---

## Reproduction plan (by class)

| Class | Command(s) | Required env | Expected failure signal | Preserve artifacts |
|---|---|---|---|---|
| Harness race in compliance suite | `cargo test -p logfwd --test it compliance_file -- --nocapture` | Rust toolchain; local filesystem | Intermittent timeout in multi-test run (e.g., rotate_create/glob_new_files) while isolated tests pass | Full stdout/stderr; `RUST_BACKTRACE=1`; timing of each test |
| Isolate suspected flakes | `cargo test -p logfwd --test it compliance_file::compliance_file_rotate_create -- --exact --nocapture` and `...compliance_glob_new_files...` | Same | Usually pass; contrasts with aggregate failure | Per-test wall time and pass/fail matrix across repetitions |
| Tail transition correctness | `cargo test -p logfwd-io tail::tests::test_tail_truncation -- --nocapture` | Same | Should pass; use as baseline while adding deterministic stress tests later | Test output + any tracing logs |
| Framing/checkpoint boundary | `cargo test -p logfwd-io --test it file_boundary_contract -- --nocapture` | Same | Should pass; if fails, points to source/remainder/checkpoint coupling | Emitted-by-source payload snapshots |
| E2E harness ordering (stub) | `bash tests/e2e/run_smoke_test.sh` | POSIX shell | PASS with marker-gated ordering | Invocation log generated by smoke script |
| Real KIND ingest-gap | `bash tests/e2e/run.sh` | Docker + kind + kubectl + cluster permissions + network | Categorized token via `E2E_FAIL_CATEGORY=*` or PASS line-range success | `kubectl describe/logs/events`, `/stats` polling trace, fail category token |

Recommended deterministic loop for flaky class detection:

```bash
for i in $(seq 1 20); do
  echo "=== run $i ==="
  cargo test -p logfwd --test it compliance_file -- --nocapture || break
done
```

---

## Proposed implementation slices (one bug class per PR)

### Slice A — Harness determinism in `compliance_file` (no tail behavior changes)

- Goal: remove inter-test timing interference and turn current timeouts into deterministic signals.
- Likely file footprint:
  - `crates/logfwd/tests/it/compliance_file.rs`
  - possibly `crates/logfwd/tests/it/main.rs` (ordering/serial strategy) if needed.
- Regression test shape:
  - deterministic helper that captures elapsed milestones and asserts bounded ingestion-stage completion with explicit diagnostics per stage.
  - optional serial annotation for known-interfering tests only.
- Why low-discretion: directly targets observed aggregate-vs-isolated mismatch.

### Slice B — Rotate/glob startup readiness invariants in file-tail compliance tests

- Goal: enforce explicit “tailer attached + first read observed” preconditions before transition actions.
- Likely file footprint:
  - `crates/logfwd/tests/it/compliance_file.rs` only.
- Regression test shape:
  - refactor wait helpers to report per-phase metric deltas and fail with phase-specific message.
- Why low-discretion: test-only hardening; avoids production semantics changes.

### Slice C — E2E failure artifact standardization

- Goal: ensure every `E2E_FAIL_CATEGORY` path captures consistent triage bundle.
- Likely file footprint:
  - `tests/e2e/run.sh`
  - optional docs note under `dev-docs/research/...` follow-up.
- Regression test shape:
  - extend `run_smoke_test.sh` stubs to assert artifact/diagnostic commands were called on each failure branch.
- Why low-discretion: harness observability improvement without changing ingest behavior.

### Slice D — Source/checkpoint transition targeted regression (only if class reproduces)

- Goal: if reproduction shows real source-id/checkpoint coupling bug, add minimal failing contract test first.
- Likely file footprint:
  - `crates/logfwd-io/tests/it/file_boundary_contract.rs`
  - then minimal production path in `tail/reader.rs` or `tail/discovery.rs`.
- Regression test shape:
  - two-file glob + rotate/truncate + restart sequence asserting per-source checkpoint monotonicity and no cross-source replay.
- Why low-discretion: code change gated by first reproducing deterministic failing test.

---

## What not to touch yet

- Do **not** refactor scanner internals.
- Do **not** rewrite the file tailer architecture.
- Do **not** batch nightly workflow hygiene + tail behavior + framing semantics in one PR.
- Do **not** change e2e expected line range semantics before reproducer confirms necessity.
- Do **not** unignore copytruncate by default without a deterministic contract for tolerated loss window.

---

## Current confidence-ranked findings

1. **Highest confidence:** aggregate compliance-file timeouts are at least partly harness/test-interference driven (failed in suite, passed individually).
2. **High confidence:** e2e harness already encodes race-aware gating and categorized failure tokens, so remaining nightly instability likely includes environment/data-plane factors.
3. **Medium confidence:** file-tail rotation/truncation transitions remain a legitimate correctness risk area due to nuanced identity/offset logic and known copytruncate caveat.
4. **Lower confidence as primary root cause:** framing/remainder/source-isolation core contracts, because targeted `file_boundary_contract` checks currently pass.

