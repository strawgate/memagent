> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS1 core builder architecture.

# WS1 Prompt: Core Columnar Builder Architecture

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Read first:

- `AGENTS.md`
- `DEVELOPING.md`
- `CONTRIBUTING.md`
- `dev-docs/research/columnar-builder/architecture.md`
- `dev-docs/research/columnar-builder/experiment-contract.md`

Task: propose and, if useful, prototype the shared `ColumnarBatchBuilder` core
that should replace or subsume the current `StreamingBuilder` internals. Backward
compatibility with the current `StreamingBuilder` API is not required. Produced
RecordBatches must remain compatible with transforms and outputs.

Hard constraints:

- Do not put OTLP semantics in `logfwd-arrow`.
- Prefer one shared implementation of nulls, conflicts, string/binary backing,
  metadata, and Arrow materialization.
- Dynamic scanner fields and planned protocol fields must both be first-class.
- Use batch-local plans/caches only unless you explicitly justify otherwise.

Investigate:

- Current `crates/logfwd-arrow/src/streaming_builder.rs` responsibilities.
- Whether to extract `ColumnarBatchBuilder` beneath `StreamingBuilder`, rewrite
  `StreamingBuilder`, or replace it outright.
- How `BlockStore`, `FieldHandle`, `SchemaPlan`, `ColumnWriter`, and
  `ConflictPolicy` should fit together.
- Whether a prototype clarifies the design. If yes, write code.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws1-core-builder.md` with the
sections required by `experiment-contract.md`.

Recommendation labels: `adopt`, `reject`, or `investigate-further`.
