> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Proposed large fanout plan for the columnar builder architecture decision.

# Fanout Plan

## Strategy

Build experiments against this foundation branch, not against `main`, so agents
share the same benchmark harness, architecture vocabulary, and correctness
contract.

The fanout should ask agents to disagree productively. We want multiple credible
architecture bets tested with code, measurements, and maintainability analysis.

## Workstreams

| ID | Workstream | Attempts | Primary Question |
|---|---|---:|---|
| WS1 | Core builder architecture | 4 | What shared builder shape should replace or subsume `StreamingBuilder`? |
| WS2 | Planned fields + dense writers | 4 | Can known fields avoid sparse materialization and dynamic lookup? |
| WS3 | Native binary trace/span | 3 | Can we remove OTLP trace/span hex roundtrip cleanly? |
| WS4 | EventTape | 4 | Does a parser-first typed tape improve correctness and maintainability enough to justify it? |
| WS5 | Descriptor/codegen decoder | 4 | Can descriptors/codegen reduce handwritten OTLP wire decoder risk without losing performance? |
| WS6 | Output encode architecture | 3 | Can `OtlpSink` use batch plans and protocol structure to skip grouping/repeated encoding work? |
| WS7 | Real corpus + fuzz oracle | 3 | What corpus/fuzz/parity layer is needed before trusting the projected decoder? |
| WS8 | typed-arrow and alternatives | 2 | Can existing typed Arrow tooling reduce builder boilerplate or make our plan unnecessary? |

Total recommended cloud tasks: `27`.

## Fan-In Rubric

Score each attempt across these axes:

| Axis | Weight | Notes |
|---|---:|---|
| Correctness confidence | 30% | prost parity, malformed wire behavior, fuzz/corpus coverage |
| Performance evidence | 25% | decode/materialize, E2E, allocation, flamegraphs |
| Maintainability | 20% | semantic duplication, code size, testability, 10-year cost |
| Crate boundaries | 10% | keeps OTLP semantics out of `logfwd-arrow` |
| Migration path | 10% | incremental delivery, limited blast radius |
| Surprise value | 5% | ideas that change the decision space |

## Fan-In Questions

During fan-in, answer these:

1. Should we build a new `ColumnarBatchBuilder` core or rewrite
   `StreamingBuilder` directly?
2. Which field writer representation wins: Arrow builders, dense vectors, sparse
   vectors, or hybrid?
3. Should trace/span IDs be `FixedSizeBinary`, `BinaryView`, or remain strings?
4. Is EventTape worth keeping for production, tests only, or not at all?
5. Is descriptor/codegen useful now, later, or a distraction?
6. What is the smallest PR sequence that gets benchmark wins without locking in
   the wrong architecture?

## Expected PR Sequence After Fan-In

A plausible sequence, subject to evidence:

1. Extract `BlockStore`, `FieldHandle`, and builder planning types.
2. Add planned-field handles while preserving current dynamic behavior.
3. Add dense typed writers for canonical fields.
4. Move OTLP canonical fields to planned handles.
5. Add native binary/fixed-size binary trace/span columns.
6. Optimize `OtlpSink` for binary trace/span and single resource/scope groups.
7. Add corpus/fuzz/parity gates before expanding projected decode coverage.
