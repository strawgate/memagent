# Research: File Tailer Typestate Model and Refactor Plan

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Architecture research fanout — workstream synthesis

You are handling one workstream inside a larger architecture research fanout for the FastForward (formerly logfwd) log forwarder.

## Objective

Catalog the implicit state machine in the file tailer subsystem, propose an explicit typestate model, and produce a refactor plan that makes per-file lifecycle invariants verifiable with Kani/proptest. This directly advances issue #1310 (P1, architecture).

## Why this workstream exists

Issue #1310 describes the file tailer (`crates/logfwd-io/src/tail/`) as "the clearest example of why we want the full correct-by-construction architecture." It's a large stateful subsystem with:
- File lifecycle transitions (discovered → opened → reading → truncated → rotated → evicted)
- Event ordering invariants (checkpoint after read, not before)
- Identity rules (fingerprint-based file identity, inode reuse detection)
- Bounded reads (budget per poll cycle)
- Checkpoint semantics (which offset is safe to checkpoint)

The tailer was partially refactored (PR #939 split it, PR #1764 did further work), and the current code lives in `crates/logfwd-io/src/tail/` with modules for discovery, reader, identity, state, tailer, and glob. But the state model is still implicit — state transitions are scattered across match arms and conditional logic rather than enforced by types.

Related issues:
- #1519 (tail/framed lifecycle and checkpoint boundary semantics)
- #1258 (adaptive polling — needs clear state model to know when to speed up/slow down)
- #1559 (missing event in profile-smoke — likely a state transition bug)
- #1315 (I/O correctness umbrella)

## Mode

research

## Required execution checklist

- You MUST read ALL files in the tailer module before writing anything:
  - `crates/logfwd-io/src/tail/mod.rs`
  - `crates/logfwd-io/src/tail/tailer.rs`
  - `crates/logfwd-io/src/tail/reader.rs`
  - `crates/logfwd-io/src/tail/state.rs`
  - `crates/logfwd-io/src/tail/discovery.rs`
  - `crates/logfwd-io/src/tail/identity.rs`
  - `crates/logfwd-io/src/tail/glob.rs`
  - `crates/logfwd-io/src/tail/tests.rs`
  - `crates/logfwd-io/src/tail/verification.rs`
- You MUST also read:
  - `dev-docs/ARCHITECTURE.md` (pipeline data flow)
  - `dev-docs/SCANNER_CONTRACT.md` (scanner input requirements)
  - `crates/logfwd-io/src/checkpoint.rs` (checkpoint storage)
  - `tla/TailLifecycle.tla` (existing TLA+ spec for tail lifecycle)
  - `tla/MCTailLifecycle.tla` (model checker config)
- You MUST catalog every implicit state the code uses for a per-file lifecycle:
  - What fields/flags represent the current state?
  - What transitions are possible from each state?
  - What invariants should hold at each state? (e.g., "file handle is open", "offset ≤ file size")
  - Where are transitions guarded by runtime checks vs enforced by types?
- You MUST produce a state diagram (ASCII or mermaid) of the per-file lifecycle
- You MUST compare the implicit state model against the existing TLA+ TailLifecycle spec — identify gaps
- You MUST propose a concrete typestate design:
  - Rust types/structs for each state
  - Transition methods that consume one state and produce the next
  - What data is available in each state
  - How errors/panics are handled at invalid transitions
- You MUST identify which invariants are provable with Kani vs proptest
- You MUST end with a phased refactor plan

After completing the required work, use your judgment to explore:
- Whether the discovery/glob module has its own implicit state machine
- How truncation detection interacts with checkpoint advancement
- Whether the framer (CRI reassembly) should be part of the tailer state or separate

## Required repo context

Read at least these:
- `crates/logfwd-io/src/tail/` (all files)
- `crates/logfwd-io/src/checkpoint.rs`
- `dev-docs/ARCHITECTURE.md`
- `dev-docs/SCANNER_CONTRACT.md`
- `tla/TailLifecycle.tla`
- `tla/MCTailLifecycle.tla`

## Deliverable

Write one repo-local output at:

`dev-docs/research/file-tailer-typestate-model.md`

Include:
1. Current implicit state catalog (every state, its representation, transitions, invariants)
2. Per-file lifecycle state diagram
3. Gap analysis: implicit model vs TLA+ TailLifecycle spec
4. Proposed typestate design with Rust type signatures
5. Verification plan: which invariants for Kani, which for proptest, which for TLA+
6. Phased refactor plan (Phase 1: extract state types, Phase 2: enforce transitions, Phase 3: proofs)
7. Relationship to #1519, #1258, #1559

## Constraints

- Ground everything in the actual codebase — read every file in the tailer module
- Do not implement code changes — this is a research deliverable
- Do not modify existing TLA+ specs — reference them for gap analysis
- The tailer runs on OS threads (blocking I/O) — the typestate design must work outside of async
- Checkpoint correctness is the highest priority invariant

## Success criteria

- Every implicit state in the current code is identified and named
- The typestate proposal is concrete enough that a follow-up PR could implement Phase 1
- The gap analysis between code and TLA+ spec identifies at least 2 divergences
- The verification plan assigns each invariant to the right tool (Kani/proptest/TLA+)

## Decision style

End with: "The tailer has {N} implicit states. The typestate refactor should start with {specific module} because {reason}. The critical invariant to prove first is {X}."
