# Linearizability Fanout Wave 1

> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Fanout plan for linearizability/Porcupine evaluation and implementation.

Date: 2026-04-11  
Branch target for cloud tasks: `main`

## Goal

Produce decision-grade evidence and implementation proposals for adding linearizability checking over runtime-emitted turmoil histories, with Porcupine as primary candidate.

## Workstreams

1. `ws01-model-contract`  
   Define minimal legal sequential model for checkpoint/ack/flush semantics.
2. `ws02-history-capture`  
   Ensure runtime-emitted events are sufficient and stable for checker input.
3. `ws03-porcupine-checker`  
   Implement a small checker CLI over exported histories.
4. `ws04-ci-integration`  
   Propose and prototype low-cost CI wiring + regression corpus strategy.
5. `ws05-alternatives-and-risks`  
   Compare Porcupine vs lighter in-repo checker strategy and recommend default lane.

## Fan-in rubric

1. Soundness of model vs runtime semantics
2. Counterexample quality and debuggability
3. Determinism and reproducibility in CI
4. Integration complexity and maintenance cost
5. Incremental adoption plan (ship-now vs gated vs follow-up)

## Required fan-in outputs

- Per-workstream winner with rationale
- Integrated PR sequence proposal
- Ranked recommendation:
  - ship now
  - ship gated
  - follow-up research
