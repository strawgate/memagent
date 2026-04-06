# OTLP + File IO Correctness POC (Worker: codex, 2026-04-05)

## Why this exists

This worker doc proposes a **contract-first reliability plan** for OTLP input/output and file input/output so we stop relying on whack-a-mole bug fixes.

It includes:

1. A concrete correctness architecture (what invariants we enforce).
2. A historical bug-pattern review from this repository.
3. A comparison baseline from other collector projects already present in our competitive bench harness.
4. A POC test added in this branch that validates **OTLP encoder -> OTLP receiver loopback compatibility** end-to-end.

---

## 1) Problem framing

The recurring failure mode in observability pipelines is not single-bug coding mistakes; it's **contract drift across components**:

- Output encoder evolves, input decoder assumptions lag.
- File tailing edge cases (rotate/truncate/partial line) are fixed one scenario at a time.
- High-level tests check throughput/counts but don't always check semantic equivalence.

If we don't freeze explicit contracts and test them continuously, correctness decays as features land.

---

## 2) What bug history in this repo is telling us

From recent history touching OTLP and IO components (see `git log` in this branch):

- OTLP receiver lifecycle/shutdown regressions and fix/re-fix cycles.
- OTLP protocol configuration and error handling defects.
- Tail/checkpoint safety defects and TOCTOU/truncation behavior fixes.

Recent commits of note include:

- `121e99e` — receiver lifecycle regression reversal.
- `e8ca674` — Drop impls for HTTP receiver shutdown.
- `364d965` — OTLP gRPC protocol behavior fix (HTTP/2).
- `cb23727` — OTLP sink error handling/panic hardening.
- `7ba9fd9` and `650baa6` — file tail/checkpoint/truncation safety fixes.

Interpretation: we are repeatedly repairing **integration boundaries**, not just internals.

---

## 3) External project baseline we should emulate

The workspace already tracks competing collectors in `logfwd-competitive-bench`:

- OpenTelemetry Collector (`otelcol`)
- Vector
- Fluent Bit
- Filebeat

For our use-case, the key pattern from mature collectors is:

- **Strict protocol envelopes** (OTLP wire compliance/golden fixtures)
- **Durability and replay semantics at source boundaries** (file tail/checkpoint discipline)
- **Layered verification** (unit + property + integration + stress)

We should copy the *method* (contract suites + compatibility fixtures), not only the runtime features.

---

## 4) Proposed architecture: “Correctness by construction”

### 4.1 Contract layers

1. **Wire contract (OTLP):**
   - Canonical fixtures for protobuf + JSON OTLP requests/responses.
   - Required fields map (severity/message/timestamp/trace/span/resource attrs).
   - Negative fixtures (invalid hex IDs, malformed payloads, oversized body, wrong path/method).

2. **Source durability contract (file input):**
   - At-least-once no-loss invariant under rotate/truncate/reopen.
   - Checkpoint monotonicity constraints.
   - Explicit duplicate-allowed windows (copytruncate race) documented + tested.

3. **Semantic contract (cross-component):**
   - OTLP output -> OTLP input loopback preserving semantic keys.
   - File input -> output sink path preserving line identity sequence.

### 4.2 Verification ladder

- **Tier A:** Kani/low-level proofs for pure codecs/parsers.
- **Tier B:** property tests for boundary fuzzing.
- **Tier C:** deterministic integration fixtures (golden vectors).
- **Tier D:** long-run chaos/rotation stress tests.

No layer substitutes for another; each catches a different bug class.

---

## 5) POC implemented in this branch

### New test

- `crates/logfwd-io/tests/it/otlp_loopback_contract.rs`

What it does:

1. Build a real Arrow `RecordBatch` with canonical OTLP fields.
2. Send it through production `OtlpSink::send_batch` (which includes `encode_batch` + HTTP transport).
3. Deliver it to a live `OtlpReceiverInput` over HTTP.
4. Poll emitted JSON lines and assert semantic field preservation:
   - level
   - message
   - trace_id
   - span_id
   - resource attribute (`service.name`)

Why this matters:

- Existing tests heavily validated encoder and receiver independently.
- This POC adds a **cross-crate compatibility contract** that fails if either side drifts.

---

## 6) Why this approach is better than patch-by-patch fixes

This design changes defect economics:

- **Before:** bug appears in production path -> add narrow regression test.
- **After:** contract suite fails at PR time when any incompatible change lands.

Expected outcomes:

- Faster detection of protocol drift.
- Fewer regressions in OTLP/file edge handling.
- Lower long-term maintenance cost because invariants are explicit and executable.

---

## 7) Next steps (recommended)

1. Add fixture directories:
   - `tests/fixtures/otlp/valid/`
   - `tests/fixtures/otlp/invalid/`
   - `tests/fixtures/file-io/rotation/`
2. Add differential tests comparing our OTLP behavior with at least one external reference implementation for fixture corpus.
3. Promote this loopback test into CI-required “contract suite” stage.
4. Add benchmark counters for correctness overhead (latency delta with contract checks enabled/disabled).
