# Research Index

Point-in-time investigations that informed architecture decisions.

## Rules

- Research docs are not canonical behavior docs.
- Every research note file (excluding `README.md`) must start with a Markdown
  title, followed by lifecycle metadata near the top:

```markdown
# Research Note Title

> **Status:** Active | Completed | Historical
> **Date:** YYYY-MM-DD
> **Context:** one-line purpose
```

- `Active` means unresolved engineering decisions remain.
- `Completed` means action items were closed.
- `Historical` means superseded but kept for context.

## Active Research

- [checkpoint-snapshot-design.md](checkpoint-snapshot-design.md)
- [columnar-batch-builder.md](columnar-batch-builder.md)
- [config-library-poc-2026-04-19.md](config-library-poc-2026-04-19.md)
- [crate-restructure-plan.md](crate-restructure-plan.md)
- [elasticsearch-retry-contract-2026-04-18.md](elasticsearch-retry-contract-2026-04-18.md)
- [file-tailing-audit.md](file-tailing-audit.md)
- [linearizability-porcupine-plan.md](linearizability-porcupine-plan.md)
- [per-input-sql-analysis.md](per-input-sql-analysis.md)
- [source-metadata-zero-copy-plan-2026-04-19.md](source-metadata-zero-copy-plan-2026-04-19.md)
- [source-metadata-attachment-perf-2026-04-19.md](source-metadata-attachment-perf-2026-04-19.md)
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
