> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Cloud fanout prompt for WS4 EventTape architecture.

# WS4 Prompt: EventTape

You are working in `strawgate/memagent` on the branch that contains
`dev-docs/research/columnar-builder/`. Assume you can see only the committed repo
on this branch and this prompt.

Task: evaluate whether a compact typed EventTape layer should sit between OTLP
wire parsing and Arrow construction.

Potential shape:

```text
BeginRow
I64(field, value)
Utf8View(field, block, offset, len)
FixedBinary(field, block, offset, len)
DynamicAttr(position, key_view, value)
EndRow
```

Investigate:

- Can prost/reference conversion emit the same tape for parity tests?
- Can direct wire decode emit the tape cheaply?
- Can the tape replay into a shared columnar builder?
- Should EventTape be production, tests-only, debug-only, or rejected?
- Does tape improve fuzzing/malformed-wire diagnosis enough to justify cost?

Prototype if feasible. Benchmark if production tape is proposed.

Required deliverable:

Write `dev-docs/research/columnar-builder/results/ws4-event-tape.md` with a clear
recommendation and evidence.
