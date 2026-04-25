# Verification Traceability Matrix

This matrix maps high-risk seams to verification ownership and current evidence across **TLA+**, **Kani**, **proptest**, **turmoil**, and integration tests.

Scope used to build this matrix:

- `dev-docs/VERIFICATION.md`
- `dev-docs/verification/kani-boundary-contract.toml`
- `tla/README.md` and `tla/*.tla` / `tla/*.cfg`
- `crates/ffwd/tests/turmoil_sim/`

> Status meanings: `required` = hard verification ownership; `recommended` = preferred but not gate-enforced; `exempt` = currently out of scope for that primary mode until seam extraction/refactor.

## Matrix

| Module / seam path | Contract statement (what must hold) | Primary verification mode | Enforcement status | Current evidence (test/spec/doc/PR/issue reference) | Gap owner issue when evidence is missing |
|---|---|---|---|---|---|
| `crates/ffwd-io/src/tail/state.rs` + `crates/ffwd-io/src/tail/verification.rs` + `tla/TailLifecycle.tla` | Tail EOF and error-backoff reducers must be monotonic/threshold-consistent: normal EOF emits only after idle threshold, shutdown EOF emits only when the tracked offset has caught up to current file size, data resets EOF state, and backoff matches bounded exponential policy. | Kani | required | Listed as a Kani-required seam in `dev-docs/verification/kani-boundary-contract.toml`; tail row ownership in `dev-docs/VERIFICATION.md`; supporting temporal evidence in `tla/TailLifecycle.tla` invariants + `tla/TailLifecycle.cfg`. | N/A (evidence present) |
| `crates/ffwd-io/src/tail/identity.rs` + `crates/ffwd-types/src/pipeline/registry.rs` | File-source identity must derive stable non-zero `SourceId` values for identifiable files and registry updates must preserve per-source lifecycle/accounting without cross-source drift. | Kani | required | `crates/ffwd-types/src/pipeline/registry.rs` is listed as a required Kani seam in `dev-docs/verification/kani-boundary-contract.toml`; source-id sentinel and file identity behavior are covered by `crates/ffwd-io/src/tail/identity.rs`, `crates/ffwd-io/src/tail/reader.rs`, and tail integration tests. | N/A (evidence present) |
| `crates/ffwd-runtime/src/pipeline/checkpoint_policy.rs` | Delivery outcomes must map to checkpoint disposition without advancing past unresolved prefixes (ordered commit and monotonic advancement). | Kani | required | Listed as Kani-owned seam in `dev-docs/verification/kani-boundary-contract.toml`; detailed proof ownership in `dev-docs/VERIFICATION.md` checkpoint policy row; temporal design counterpart in `tla/PipelineMachine.tla`. | N/A (evidence present) |
| `crates/ffwd-runtime/src/pipeline/checkpoint_io.rs` | Final checkpoint flush retry window must classify retryable errors correctly and stop on success without invalid advancement. | Kani | recommended | `dev-docs/VERIFICATION.md` checkpoint IO row records Kani as the primary proof evidence, with unit + proptest + async retry tests as supporting coverage; seam is marked `recommended` in boundary contract TOML. | N/A (evidence present) |
| `crates/ffwd-types/src/pipeline/lifecycle.rs` + `tla/PipelineMachine.tla` | Pipeline lifecycle must drain to terminal state with no stranded in-flight/sent work; stop metadata and checkpoint ordering invariants must hold. | Kani | required | Listed as a required Kani seam in `dev-docs/verification/kani-boundary-contract.toml`; `dev-docs/VERIFICATION.md` lifecycle row records Kani exhaustive + proptest + TLA+ coverage; complementary temporal-model evidence in `tla/README.md` PipelineMachine property inventory and `tla/PipelineMachine.cfg` + `.liveness.cfg` + `.coverage.cfg`. | N/A (evidence present) |
| `tla/ShutdownProtocol.tla` + runtime shutdown seams | Shutdown terminalization must preserve ordering (`io→cpu→pipeline→join→stop`) and force-stop must move output health to terminal failed state. | TLA | required | `tla/README.md` ShutdownProtocol property list and TLC configs; safety/liveness specs in `tla/ShutdownProtocol.tla` + `.cfg` + `.liveness.cfg`. | N/A (evidence present) |
| `crates/ffwd/tests/turmoil_sim/trace_validation.rs` + `crates/ffwd/tests/turmoil_sim/fault_scenario_sim.rs` | Post-stop contract: after `Stopped`, no sink or checkpoint side effects; trace phase ordering and checkpoint monotonicity must hold under simulation. | turmoil | required | `trace_validation` tests enforce phase and side-effect contracts; `fault_scenario_sim` scenarios validate post-stop behavior and crash/retry traces. | N/A (evidence present) |
| `crates/ffwd/tests/turmoil_sim/network_sim.rs` + `fault_scenario_sim.rs` | Fault simulation must preserve recoverability and delivery accounting across partitions/repairs, retries, and slow in-flight work. | turmoil | required | Network partition/repair, retry exhaustion, and out-of-order ack checkpoint tests in `network_sim.rs`; scenario coverage in `fault_scenario_sim.rs`. | N/A (evidence present) |
| `crates/ffwd/tests/turmoil_sim/fs_crash_sim.rs` + `crash_sim.rs` + `observable_checkpoint.rs` | Delivery/checkpoint boundary under crash: delivery may succeed while durable checkpoint lags; durable state must reflect sync semantics and per-source monotonic updates. | turmoil | required | Filesystem crash durability tests in `fs_crash_sim.rs`; checkpoint crash invariants in `crash_sim.rs` with `ObservableCheckpointStore` inspection helpers. | N/A (evidence present) |
| `crates/ffwd-io/src/otlp_receiver/convert.rs` | OTLP input conversion helpers (proto/JSON normalization + writer helpers) must preserve semantic parity and bounded encoding correctness. | Kani | recommended | Boundary contract marks seam as `recommended` with issue linkage; `dev-docs/VERIFICATION.md` cites Kani proof + proptest oracle checks + unit coverage for convert/projection path. | #1484 (seam currently recommended, not required) |
| `crates/ffwd-output/src/otlp_sink.rs` | OTLP output row encoding must remain equivalent between generated-fast and handwritten encoders for random UTF-8 rows. | proptest | required | `dev-docs/VERIFICATION.md` OTLP sink row documents proptest oracle equivalence plus unit coverage; output serialization ownership is explicit in verification status table. | N/A (evidence present) |
| `crates/ffwd-core/src/{framer.rs,cri.rs,json_scanner.rs,scanner.rs,otlp.rs}` | Core parser/scanner/protocol/OTLP wire surfaces must remain panic-free and spec-correct within bounded/exhaustive domains. | Kani | required | `dev-docs/VERIFICATION.md` per-module table lists Kani proof ownership counts for framer/CRI/scanner protocol/json scanner/otlp (with proptest oracle where applicable). | N/A (evidence present) |
| `crates/ffwd-runtime/src/pipeline/mod.rs` | Async orchestration shell should be represented by extracted pure seams; until extracted, ownership remains documentation + integration coverage instead of required Kani. | integration | exempt | Boundary contract explicitly marks this seam `exempt`; extracted sub-seams (`checkpoint_policy`, `health`, `input_poll`) carry proofs/tests. | #1449 |
| `crates/ffwd-diagnostics/src/diagnostics/server.rs` | Diagnostics server should separate pure policy from HTTP/runtime shell before Kani can be required. | integration | exempt | Boundary contract marks seam `exempt` and points to extraction tracking issue. | #1438 |

## Noted gaps / risk concentrations

1. **Recommended OTLP receiver conversion seam (`convert.rs`) is not yet a required Kani gate.** Current evidence exists (Kani + proptest + unit tests), but enforcement remains soft pending extraction/completion work. Gap owner: **#1484**.
2. **Some async/runtime shells are intentionally exempt from required Kani** (`pipeline/mod.rs`, diagnostics server). Coverage currently relies on extracted pure reducers + integration/turmoil, with extraction tracked in **#1449** and **#1438**.
3. **No new issue numbers were invented in this matrix.** When no clear owner issue existed, the rule is to use `TBD: open gap issue`; this run did not require any such placeholder because boundary contract entries already provide owner issues for listed gaps.

## Maintenance notes

- Keep this file aligned with `dev-docs/verification/kani-boundary-contract.toml` status/issue fields and `dev-docs/VERIFICATION.md` per-module table.
- When adding a new critical seam, add a matrix row in the same PR that adds/changes verification ownership.
- For TLA-owned rows, include both spec file and cfg model evidence so traceability captures executable model intent.
