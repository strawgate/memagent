> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS7 real payload corpus and fuzz oracle.

# WS7 Prompt: Real Payload Corpus And Correctness Oracle

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Task: design and prototype the correctness layer required before trusting a
projected OTLP wire decoder.

Investigate/prototype:

- Realistic OTLP payload corpus format and fixture loader.
- Synthetic corpus cases: arrays, kvlists, bytes, duplicate attrs, repeated
  resource/scope logs, malformed wire fields, invalid UTF-8.
- Prost/reference parity checks for supported cases.
- Explicit fallback checks for unsupported cases.
- Fuzz target structure for projected decoder vs prost/reference behavior.
- Minimal CI-friendly command set.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws7-corpus-fuzz-oracle.md` with
an implementation plan and any prototype tests/fixtures you add.
