> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS5 descriptor/codegen OTLP decoder research.

# WS5 Prompt: Descriptor Or Codegen-Guided OTLP Decoder

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Task: find a maintainable way to reduce handwritten OTLP wire decoder code while
preserving direct-to-Arrow performance.

Investigate:

- Protobuf descriptors available from existing OTLP proto crates/build scripts.
- Generating field walkers or typed callbacks into a builder.
- Generating validation/parity tests from descriptors.
- Whether generated code is readable and reviewable enough for this repo.
- Whether codegen can support only the projected subset while falling back for
  unsupported shapes.

Prototype if feasible:

- A tiny generated or descriptor-driven decoder slice for one OTLP message.
- Compare its performance and code size against current hand parsing.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws5-descriptor-codegen.md`.
Recommendation must say `adopt-now`, `adopt-later`, or `reject`.
