> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS2 planned fields and dense writers.

# WS2 Prompt: Planned Fields And Dense Writers

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Read `AGENTS.md`, `DEVELOPING.md`, `CONTRIBUTING.md`,
`dev-docs/research/columnar-builder/architecture.md`, and
`dev-docs/research/columnar-builder/experiment-contract.md`.

Task: determine whether known/planned fields can avoid `StreamingBuilder`'s
sparse row/value storage and expensive `finish_batch()` materialization.

Prototype if feasible:

- `FieldHandle` or equivalent stable handles.
- Dense writers for at least `Int64`, `Bool`, and `Utf8View`.
- Row-count hints and null bitmap handling.
- A path that an OTLP decoder could use for canonical fields without repeated
  dynamic field lookup.

Compare against current projected-view decode. Use the benchmark contract. If a
full prototype is too large, build a narrow proof-of-concept and clearly state
what remains.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws2-planned-dense-writers.md`.
Include before/after timings, allocation numbers, and a recommendation.
