# Research Index

Point-in-time investigations that informed architecture decisions.

This index is curated. It is not intended to enumerate every audit, prompt,
artifact, or generated file under `dev-docs/research/`.

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
- [config-library-poc.md](config-library-poc.md)
- [crate-restructure-plan.md](crate-restructure-plan.md)
- [elasticsearch-retry-contract.md](elasticsearch-retry-contract.md)
- [file-tailing-design.md](file-tailing-design.md)
- [linearizability-porcupine-plan.md](linearizability-porcupine-plan.md)
- [per-input-sql-analysis.md](per-input-sql-analysis.md)
- [predicate-pushdown-design.md](predicate-pushdown-design.md)
- [source-metadata-attachment-plan.md](source-metadata-attachment-plan.md)
- [tla-gap-analysis.md](tla-gap-analysis.md)
- [turmoil-concurrency-analysis.md](turmoil-concurrency-analysis.md)
- [type-suffix-redesign.md](type-suffix-redesign.md)
- [verification-gap-analysis.md](verification-gap-analysis.md)
- [zero-copy-pipeline-design.md](zero-copy-pipeline-design.md)

## Durable Design Notes

- [columnar-batch-builder.md](columnar-batch-builder.md)
