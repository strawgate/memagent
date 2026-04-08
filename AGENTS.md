# Agent Guide

## Start here

- `README.md` — what logfwd does, performance targets
- `DEVELOPING.md` — build/test/bench commands, hard-won lessons
- `dev-docs/ARCHITECTURE.md` — pipeline data flow, scanner stages, crate map
- `dev-docs/DESIGN.md` — vision, target architecture, architecture decisions
- `dev-docs/VERIFICATION.md` — TLA+, Kani, proptest — when to use each, proof requirements, per-module status

## Reference docs

| Doc | When to read |
|-----|-------------|
| [`dev-docs/CRATE_RULES.md`](dev-docs/CRATE_RULES.md) | Per-crate rules enforced by CI |
| [`dev-docs/CODE_STYLE.md`](dev-docs/CODE_STYLE.md) | Naming, error handling, hot path rules |
| [`dev-docs/ADAPTER_CONTRACT.md`](dev-docs/ADAPTER_CONTRACT.md) | Receiver/pipeline/sink contracts, checkpoint rules, duplicate/loss windows, diagnostics/control-plane contract |
| [`dev-docs/SCANNER_CONTRACT.md`](dev-docs/SCANNER_CONTRACT.md) | Scanner input requirements, output guarantees, limitations |
| [`dev-docs/PR_PROCESS.md`](dev-docs/PR_PROCESS.md) | Copilot assignment, PR triage, review criteria, merge process |
| [`dev-docs/CHANGE_MAP.md`](dev-docs/CHANGE_MAP.md) | What must change together for config, pipeline, verification, and crate-boundary edits |
| [Roadmap (GitHub issue #889)](https://github.com/strawgate/memagent/issues/889) | What's done, what's in progress, what's next |
| [`dev-docs/references/arrow.md`](dev-docs/references/arrow.md) | RecordBatch, StringViewArray, schema evolution |
| [`dev-docs/references/datafusion.md`](dev-docs/references/datafusion.md) | SessionContext, MemTable, UDF creation, SQL execution |
| [`dev-docs/references/tokio-async-patterns.md`](dev-docs/references/tokio-async-patterns.md) | Runtime, bounded channels, CancellationToken, select! safety |
| [`dev-docs/references/opentelemetry-otlp.md`](dev-docs/references/opentelemetry-otlp.md) | OTLP protobuf nesting, HTTP vs gRPC |
| [`dev-docs/references/kani-verification.md`](dev-docs/references/kani-verification.md) | Kani proofs, function contracts, solver selection |
| [`dev-docs/DOCS_STANDARDS.md`](dev-docs/DOCS_STANDARDS.md) | Doc taxonomy, lifecycle rules, anti-drift measures |
| [`tla/README.md`](tla/README.md) | TLA+ specs: PipelineMachine, ShutdownProtocol, PipelineBatch |

## User docs

User-facing documentation lives in `book/src/`. See `book/src/SUMMARY.md` for the full table of contents.

## Issue labels

See [`CONTRIBUTING.md`](CONTRIBUTING.md#issues-and-labels) for the full label taxonomy, triage process, and work-unit rules.

Quick reference — every issue needs **type + priority + component(s)**:

| Type | Priority |
|------|----------|
| `bug`, `enhancement`, `performance`, `architecture`, `research`, `documentation`, `work-unit` | `P0` critical, `P1` high, `P2` medium, `P3` low |

## Before submitting

- `just ci` must pass
- See `dev-docs/VERIFICATION.md` for proof requirements (Kani, TLA+, proptest)
- For control-plane or state-machine fixes in mixed async/runtime code, prefer extracting a local pure reducer/state module and add Kani + proptest coverage there when feasible
- See `dev-docs/CODE_STYLE.md` for style rules
