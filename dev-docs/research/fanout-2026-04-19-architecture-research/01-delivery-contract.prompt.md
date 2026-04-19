# Research: Output Delivery Contract — State Machine and Formal Specification

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Architecture research fanout — workstream synthesis

You are handling one workstream inside a larger architecture research fanout for the FastForward (formerly logfwd) log forwarder.

## Objective

Produce a concrete delivery state machine and TLA+ spec sketch for the output delivery contract described in issue #1312. The goal is to formalize exactly which delivery outcomes (retry, reject, partial-ack, checkpoint-advance) are valid at each boundary in the output path, and what the checkpoint advancement rules should be.

## Why this workstream exists

Issue #1312 (P0, architecture) is the highest-priority architecture issue. The output path currently has these pieces:
- Worker pool dispatches batches to sink workers
- Each sink can return success, retryable error, or terminal error
- Fanout dispatches to multiple sinks, aggregates results
- Checkpoint advancement depends on all outputs acknowledging the batch
- But "richer sink/worker outcomes still collapse too early at the pipeline checkpoint seam"

Recent merged PRs have incrementally improved this:
- PR #1505: Fix fanout delivery semantics
- PR #1480: Tighten sink health recovery semantics  
- PR #1720: Harden worker panic path with failure acks
- PR #2116: Fix SIGUSR1/2, pool submit deadlock, fanout misclassification
- PR #2267: Continue ES split after terminal rejection
- PR #2274 (open): Share output sink clients, classify terminal errors

But no one has written the full delivery state machine as a specification. The incremental fixes keep patching symptoms without a formal contract.

## Mode

research

## Required execution checklist

- You MUST read these files before writing anything:
  - `dev-docs/ARCHITECTURE.md` (pipeline data flow, buffer lifecycle)
  - `dev-docs/ADAPTER_CONTRACT.md` (receiver/pipeline/sink contracts)
  - `crates/logfwd-output/src/sink.rs` (current sink trait and health model)
  - `crates/logfwd-output/src/sink/health.rs` (health state machine)
  - `crates/logfwd-runtime/src/worker_pool/worker.rs` (worker dispatch/ack loop)
  - `crates/logfwd-runtime/src/worker_pool/dispatch.rs` (fanout dispatch)
  - `crates/logfwd-runtime/src/pipeline/submit.rs` (checkpoint advancement)
  - `crates/logfwd-runtime/src/pipeline/checkpoint_policy.rs` (checkpoint policy)
  - `crates/logfwd-types/src/pipeline/batch.rs` (batch types and ack semantics)
  - `tla/PipelineBatch.tla` (existing batch spec)
  - `tla/PipelineMachine.tla` (existing pipeline spec)
- You MUST catalog every distinct delivery outcome the current code can produce (success, retryable, terminal, panic, timeout, partial).
- You MUST identify where outcome information is lost (collapsed to a simpler type) between sink → worker → fanout → pipeline → checkpoint.
- You MUST produce a state machine diagram (as ASCII or mermaid) showing the full delivery lifecycle from batch-created to checkpoint-advanced.
- You MUST identify at least 3 concrete scenarios where the current outcome collapse causes incorrect behavior (data loss, duplicate delivery, or checkpoint advance past undelivered data).
- You MUST draft a TLA+ module sketch (or pseudocode spec) for the delivery contract with safety properties.
- You MUST end with a concrete recommendation: what types/enums need to change, where the state machine boundary should live, and what the migration path looks like.

After completing the required work, use your judgment to explore:
- Whether the existing TLA+ PipelineBatch spec can be extended or needs a separate DeliveryContract spec
- Whether partial-batch acknowledgment (some sinks succeed, some fail) should be supported
- What AsyncFanout (#319) would look like built on top of this contract

## Required repo context

Read at least these:
- `dev-docs/ARCHITECTURE.md`
- `dev-docs/ADAPTER_CONTRACT.md`
- `crates/logfwd-output/src/sink.rs`
- `crates/logfwd-output/src/sink/health.rs`
- `crates/logfwd-runtime/src/worker_pool/worker.rs`
- `crates/logfwd-runtime/src/worker_pool/dispatch.rs`
- `crates/logfwd-runtime/src/pipeline/submit.rs`
- `crates/logfwd-runtime/src/pipeline/checkpoint_policy.rs`
- `crates/logfwd-types/src/pipeline/batch.rs`
- `tla/PipelineBatch.tla`
- `tla/PipelineMachine.tla`

## Deliverable

Write one repo-local output at:

`dev-docs/research/delivery-contract-state-machine.md`

Include:
1. Current delivery outcome taxonomy (what the code actually does today)
2. State machine diagram (ASCII/mermaid)
3. Outcome collapse points with concrete bug scenarios
4. Draft TLA+ module or formal pseudocode
5. Recommended type/enum changes
6. Migration path (what can land incrementally vs what requires a coordinated change)
7. Relationship to AsyncFanout (#319) and Phase 3 (#1408)

## Constraints

- Ground everything in the actual codebase — do not speculate about code you haven't read
- Separate the formal contract (what SHOULD happen) from the current behavior (what DOES happen)
- Do not implement code changes — this is a research deliverable
- Do not modify existing TLA+ specs — draft new content in the deliverable doc
- Focus on the output→checkpoint boundary, not the input→pipeline boundary

## Success criteria

- A reader can look at the state machine and immediately know what delivery outcome is possible at each boundary
- At least 3 concrete scenarios demonstrate why the current outcome collapse is wrong
- The TLA+ sketch is specific enough that a follow-up task could turn it into a model-checkable spec
- The migration path is realistic (not "rewrite everything")

## Decision style

End with a decisive recommendation: "The delivery contract should be {X}, the type changes are {Y}, and the migration order is {Z}."
