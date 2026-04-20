# Research: OTLP Receiver Projection — Measure Remaining Overhead and Cutover Criteria

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Architecture research fanout — workstream synthesis

You are handling one workstream inside a larger architecture research fanout for the FastForward (formerly logfwd) log forwarder.

## Objective

Measure how much of the original proto→JSON→scan 2× CPU overhead (issue #823) remains after the OTLP projection work, determine the cutover criteria for retiring the legacy decode path, and produce a recommendation on next steps.

## Why this workstream exists

Issue #823 identified that the OTLP receiver decodes protobuf into JSON strings, sends through a channel, then json_scanner re-parses them — roughly 2× CPU overhead vs direct proto-to-Arrow.

Since then, significant projection work has landed:
- PR #1837: Experimental OTLP columnar projection
- PR #2215: Migrate OTLP projection to ColumnarBatchBuilder (#1845)
- PR #2257: Stabilize OTLP projection metadata
- PR #2261: Differential fuzz coverage for projection
- PR #2264: Wire projection opt-in metrics
- PR #2269: Offload OTLP protobuf decode to spawn_blocking

The projection path exists as opt-in, but the legacy proto→JSON→scan path is still the default. Key questions:
1. How much overhead remains with the projection path enabled?
2. What's missing before projection can become the default?
3. Can the legacy path be retired, or must both coexist?

## Mode

research

## Required execution checklist

- You MUST read these files before writing anything:
  - `crates/logfwd-io/src/otlp_receiver.rs` (receiver entry point)
  - `crates/logfwd-io/src/otlp_receiver/decode.rs` (protobuf decode)
  - `crates/logfwd-io/src/otlp_receiver/projection.rs` (columnar projection path)
  - `crates/logfwd-io/src/otlp_receiver/server.rs` (HTTP server and routing)
  - `crates/logfwd-io/src/otlp_receiver/tests.rs` (test coverage)
  - `crates/logfwd-arrow/src/columnar/builder.rs` (ColumnarBatchBuilder)
  - `crates/logfwd-bench/benches/otlp_io.rs` (OTLP I/O benchmarks)
  - `crates/logfwd-bench/src/generators/otlp.rs` (OTLP test data generators)
- You MUST catalog the two decode paths:
  1. Legacy: protobuf → JSON string → channel → json_scanner → Arrow RecordBatch
  2. Projection: protobuf → ColumnarBatchBuilder → Arrow RecordBatch (direct)
- You MUST identify what triggers each path (config flag, feature gate, auto-detection)
- You MUST catalog what the projection path does NOT yet handle that the legacy path does:
  - Field types
  - Edge cases
  - SQL transform compatibility
  - Enrichment compatibility
- You MUST analyze the existing benchmark results in `crates/logfwd-bench/benches/otlp_io.rs`:
  - What scenarios are benchmarked?
  - Do benchmarks compare legacy vs projection?
  - What throughput numbers exist?
- You MUST produce a gap analysis: what's missing before projection can be default
- You MUST define concrete cutover criteria (what tests must pass, what benchmarks must show)
- You MUST end with a decisive recommendation

After completing the required work, use your judgment to explore:
- Whether the proto→JSON path serves any use case that projection fundamentally cannot
- Whether the spawn_blocking offload (PR #2269) changes the overhead picture
- Whether OTLP JSON input (as opposed to protobuf) has a separate optimization path needed
- The proptest regression files for projection correctness

## Required repo context

Read at least these:
- `crates/logfwd-io/src/otlp_receiver/` (all files)
- `crates/logfwd-arrow/src/columnar/builder.rs`
- `crates/logfwd-bench/benches/otlp_io.rs`
- `crates/logfwd-bench/src/generators/otlp.rs`
- `crates/logfwd-io/proptest-regressions/otlp_receiver/projection.txt` (if exists)

## Deliverable

Write one repo-local output at:

`dev-docs/research/otlp-projection-gap-analysis.md`

Include:
1. Two-path architecture diagram (legacy vs projection)
2. Feature parity matrix (what each path handles)
3. Benchmark analysis (existing numbers, gaps in benchmark coverage)
4. Gap list: what projection doesn't handle yet
5. Cutover criteria checklist
6. Risk analysis: what could go wrong if projection becomes default
7. Recommendation: retire legacy, keep both, or other
8. Relationship to #823, SQL transform compatibility, enrichment compatibility

## Constraints

- Ground everything in the actual codebase
- Do not implement code changes — this is a research deliverable
- If benchmark numbers exist in the repo, report them; do not run new benchmarks
- The projection path must be compatible with SQL transforms (DataFusion) — this is non-negotiable
- Do not propose removing the JSON input format support (OTLP supports both protobuf and JSON wire formats)

## Success criteria

- A reader understands exactly what the projection path can and cannot do today
- The gap list is concrete and actionable (not "needs more testing")
- The cutover criteria are specific enough to become a checklist in a follow-up issue
- The recommendation is decisive

## Decision style

End with: "The projection path is {ready / not ready} for default. The blocking gaps are {list}. The legacy path {can / cannot} be retired because {reason}. Next steps: {concrete actions}."
