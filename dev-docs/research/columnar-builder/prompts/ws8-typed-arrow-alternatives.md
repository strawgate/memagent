> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS8 typed-arrow and alternative libraries.

# WS8 Prompt: typed-arrow And Alternatives

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Task: challenge the custom-builder premise. Research and, if useful, prototype
whether existing typed Arrow tooling can reduce boilerplate or replace pieces of
our planned builder architecture.

Must investigate:

- `typed-arrow` compile-time Arrow schema/builder mapping and zero-copy read
  views. Could it reduce builder boilerplate?
- Arrow-RS native builder APIs we are underusing.
- Other crates or codegen patterns for typed Arrow construction.
- Whether these tools support dynamic attrs, conflicts, string views, binary
  views, and fixed-size binary trace/span IDs.

Use web research only for primary docs/source when needed. Prefer code
inspection and small prototypes over summary-only conclusions.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws8-typed-arrow-alternatives.md`
with a recommendation: adopt, partially adopt, or reject.
