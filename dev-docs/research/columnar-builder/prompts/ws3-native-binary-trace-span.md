> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS3 native binary trace/span representation.

# WS3 Prompt: Native Binary Trace/Span Path

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Read the foundation docs in `dev-docs/research/columnar-builder/`.

Task: evaluate and prototype replacing OTLP trace/span hex string columns with a
native binary Arrow representation.

Questions to answer:

- Should trace ID be `FixedSizeBinary(16)`, `BinaryView`, or something else?
- Should span ID be `FixedSizeBinary(8)`, `BinaryView`, or something else?
- How does this affect `OtlpSink` output encoding?
- How does this affect JSON/file outputs and DataFusion transforms?
- Can OTLP input -> OTLP output avoid any hex conversion?

Prototype if feasible:

- Add builder support for fixed-size binary or binary views.
- Use it in projected OTLP decode for trace/span.
- Teach OTLP output to encode direct binary columns.
- Preserve compatibility or edge rendering for non-OTLP outputs where practical.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws3-native-binary-trace-span.md`
with correctness tests, benchmark evidence, and a recommendation.
