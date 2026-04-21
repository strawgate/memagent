# Research: Pipeline Topology Compiler Design (Issue #1363)

## Context

The logfwd pipeline currently uses imperative wiring: `input_pipeline.rs` creates an I/O worker and CPU worker, connects them with channels, and runs them. The I/O/compute split is done (`io_worker.rs`, `cpu_worker.rs`). But there is no declarative topology — the pipeline shape is hardcoded.

Issue #1363 envisions: processor chain architecture with I/O/compute separation, pipeline compiler, and multi-pipeline topology.

## What to investigate

1. Read the current pipeline implementation:
   - `crates/logfwd-runtime/src/pipeline/mod.rs` — Pipeline struct, construction
   - `crates/logfwd-runtime/src/pipeline/run.rs` — run loop, heartbeat, drain
   - `crates/logfwd-runtime/src/pipeline/io_worker.rs` — I/O worker
   - `crates/logfwd-runtime/src/pipeline/cpu_worker.rs` — CPU worker
   - `crates/logfwd-runtime/src/pipeline/input_pipeline.rs` — InputPipelineManager
   - `crates/logfwd-runtime/src/pipeline/submit.rs` — batch submission
   - `crates/logfwd-runtime/src/pipeline/checkpoint_policy.rs` — checkpoint disposition

2. Read the config and architecture docs:
   - `crates/logfwd-config/src/types.rs` — pipeline config structure
   - `dev-docs/ARCHITECTURE.md` — current pipeline data flow
   - `dev-docs/DESIGN.md` — architecture decision records

3. Design the compiler:
   - What is a "topology"? (DAG of stages: input → scan → transform → output)
   - How does the compiler infer checkpoint boundaries?
   - How does --dry-run validate without running?
   - How does multi-pipeline work? (shared inputs? independent pipelines?)
   - What is the compilation output? (a struct that `run()` consumes?)

4. Consider processor chain design:
   - Simple processors: AddField, Filter, Rename, Dedup, RateLimit (#1411)
   - How do processors compose between scan and SQL transform?
   - Where do stateful processors (tail sampling) fit?

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws02-pipeline-topology-compiler-report.md` containing:

1. Architecture proposal: what the compiler produces, what it consumes
2. Topology graph model: nodes, edges, checkpoint boundaries
3. --dry-run design: what it validates, how it reports
4. Multi-pipeline design: shared vs independent resources
5. Processor chain composition model
6. Implementation plan: ordered phases with file paths
7. Risks, open questions, and dependencies

Do NOT implement code changes. Research and design only.
