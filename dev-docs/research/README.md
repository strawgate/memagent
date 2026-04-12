# Research Index

Point-in-time investigations that informed architecture decisions.

## Rules

- Research docs are not canonical behavior docs.
- Every research note file (excluding `README.md`) must start with:

```markdown
> **Status:** Active | Completed | Historical
> **Date:** YYYY-MM-DD
> **Context:** one-line purpose
```

- `Active` means unresolved engineering decisions remain.
- `Completed` means action items were closed.
- `Historical` means superseded but kept for context.

## Active Research

- [checkpoint-snapshot-design.md](checkpoint-snapshot-design.md)
- [crate-restructure-plan.md](crate-restructure-plan.md)
- [file-tailing-audit.md](file-tailing-audit.md)
- [linearizability-porcupine-plan.md](linearizability-porcupine-plan.md)
- [per-input-sql-analysis.md](per-input-sql-analysis.md)
- [tla-gap-analysis.md](tla-gap-analysis.md)
- [turmoil-concurrency-bugs-report-2026-04.md](turmoil-concurrency-bugs-report-2026-04.md)
- [type-suffix-redesign.md](type-suffix-redesign.md)
- [verification-audit.md](verification-audit.md)

## Completed Research

- [completed-rollup-2026-04.md](completed-rollup-2026-04.md)
- [columnar-batch-builder.md](columnar-batch-builder.md)

## Historical / Superseded

- [offset-checkpoint-research.md](offset-checkpoint-research.md)
- Additional removed artifacts are preserved in git history.
