# Research: Processor Chain Architecture — I/O/Compute Separation Evaluation

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Architecture research fanout — workstream synthesis

You are handling one workstream inside a larger architecture research fanout for the FastForward (formerly logfwd) log forwarder.

## Objective

Evaluate the processor chain architecture proposed in issue #1363: replacing the monolithic pipeline select loop with a channel-per-stage topology that separates I/O workers from compute workers. Produce a concrete evaluation with measured or estimated overhead, a recommended topology, and a phased migration plan.

## Why this workstream exists

Issue #1363 (P2, research) proposes splitting the pipeline into:
```
Input Pipeline (per input) → Processor Pipeline → Output Pipeline (per output)
```
with bounded channels between stages and separate I/O vs compute worker pools.

The current architecture is a single `tokio::select!` loop in `pipeline/input_pipeline.rs` that handles input, scanning, transform, and output dispatch in one async task. This works but has limitations:
- One slow output blocks all processing
- No natural place for processor chains (blocklist, enrichment, sampling)
- Checkpoint boundaries are implicit rather than compiler-enforced
- Multi-pipeline contention shares the same tokio runtime without isolation

Related issues:
- #1409 (Phase 4: Pipeline compiler) depends on this research
- #1408 (Phase 3: I/O/compute separation for outputs) is a subset
- #233 (control/data channel separation) is a prerequisite concept
- #319 (AsyncFanout) overlaps with the output pipeline design

Key question: Is channel-per-stage worth the overhead, or should we use a simpler approach like a processor trait chain within the existing loop?

## Mode

research

## Required execution checklist

- You MUST read these files before writing anything:
  - `dev-docs/ARCHITECTURE.md` (current pipeline data flow)
  - `dev-docs/DESIGN.md` (target architecture vision)
  - `crates/logfwd-runtime/src/pipeline/input_pipeline.rs` (the main select loop)
  - `crates/logfwd-runtime/src/pipeline/mod.rs` (pipeline module structure)
  - `crates/logfwd-runtime/src/pipeline/processor_stage.rs` (current processor stage)
  - `crates/logfwd-runtime/src/worker_pool/` (all files — current worker pool design)
  - `crates/logfwd-runtime/src/pipeline/submit.rs` (output submission)
  - `crates/logfwd-runtime/src/transform.rs` (SQL transform integration)
  - `crates/logfwd-runtime/src/processor/` (blocklist, http_enrich processors)
- You MUST catalog what the current select loop does step-by-step (input → scan → transform → submit → checkpoint)
- You MUST evaluate at least 3 topology options:
  1. **Status quo+**: processor trait chain inside the existing loop (minimal change)
  2. **Channel-per-stage**: full I/O/compute separation as proposed in #1363
  3. **Hybrid**: I/O separation for outputs only (Phase 3 scope), processors stay inline
- You MUST estimate channel crossing overhead for each option using:
  - RecordBatch sizes (typical: 1-8MB, 1K-64K rows)
  - Channel types available (tokio mpsc, crossbeam, flume)
  - Number of channel hops per batch
- You MUST analyze the checkpoint boundary implications of each topology
- You MUST produce a comparison matrix with: latency, throughput ceiling, isolation, complexity, migration cost
- You MUST end with a decisive recommendation and phased migration plan

After completing the required work, use your judgment to explore:
- Whether DataFusion's execution engine could serve as the processor chain runtime
- How tail-based sampling (#1555) would fit into each topology
- Whether per-input pipelines should have independent runtimes or share one

## Required repo context

Read at least these:
- `dev-docs/ARCHITECTURE.md`
- `dev-docs/DESIGN.md`
- `crates/logfwd-runtime/src/pipeline/input_pipeline.rs`
- `crates/logfwd-runtime/src/pipeline/mod.rs`
- `crates/logfwd-runtime/src/pipeline/processor_stage.rs`
- `crates/logfwd-runtime/src/worker_pool/`
- `crates/logfwd-runtime/src/pipeline/submit.rs`
- `crates/logfwd-runtime/src/transform.rs`
- `crates/logfwd-runtime/src/processor/`

Also read the benchmark infrastructure to understand current throughput:
- `crates/logfwd-bench/benches/pipeline.rs`
- `crates/logfwd-bench/benches/full_chain.rs`

## Deliverable

Write one repo-local output at:

`dev-docs/research/processor-chain-architecture-evaluation.md`

Include:
1. Current pipeline loop anatomy (what happens step by step)
2. Three topology options with architecture diagrams
3. Channel overhead analysis with estimated costs
4. Checkpoint boundary analysis for each option
5. Comparison matrix (latency, throughput, isolation, complexity, migration cost)
6. How tail-based sampling, blocklist, and enrichment processors fit in each option
7. Recommended topology with phased migration plan
8. Relationship to #1409 (pipeline compiler), #319 (AsyncFanout), #1408 (Phase 3)

## Constraints

- Ground everything in the actual codebase — do not speculate about code you haven't read
- Do not implement code changes — this is a research deliverable
- RecordBatch is the unit of work — all analysis should use batch-level granularity
- The hot path target is 1.7M lines/sec single-core ARM64 — any proposal that can't meet this is rejected
- Assume DataFusion SQL transforms remain optional (not all pipelines use SQL)
- Do not propose changes to logfwd-core — the scanner is not part of the processor chain

## Success criteria

- A reader can compare the 3 options and understand the concrete tradeoffs
- The channel overhead estimate is grounded in actual batch sizes and Rust channel benchmarks
- The checkpoint boundary analysis identifies exactly where durability guarantees change
- The migration plan has concrete phases that can land incrementally

## Decision style

End with: "I recommend topology {X} because {reasons}. The migration order is: Phase A ({scope}), Phase B ({scope}), Phase C ({scope})."
