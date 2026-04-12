> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS6 OTLP output encode architecture.

# WS6 Prompt: OTLP Output Encode Architecture

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Task: optimize or redesign `OtlpSink` around planned batches and structured OTLP
input without leaking input-specific semantics into `logfwd-arrow`.

Investigate/prototype:

- Single resource/scope fast path.
- Per-batch encode plans for static column names and protobuf key/value sizes.
- Direct binary trace/span encode if native binary columns exist or are proposed.
- Avoiding row grouping when batch structure already provides grouping.
- Interaction with SQL transforms that may reorder/drop/change columns.

Run output and E2E benchmarks per the experiment contract.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws6-output-encode.md` with a
recommendation and benchmark evidence.
