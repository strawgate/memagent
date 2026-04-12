# Columnar Builder Foundation

This directory is the foundation for the OTLP direct-to-Arrow architecture fanout.
It frames the shared builder direction, benchmark contract, correctness oracle,
and workstream prompts that cloud agents should use when prototyping alternatives.

Read these in order:

1. [architecture.md](architecture.md) - design target and open architecture decisions.
2. [experiment-contract.md](experiment-contract.md) - required benchmark, correctness, and reporting rules.
3. [fanout-plan.md](fanout-plan.md) - proposed workstreams and fan-in rubric.
4. [prompts/](prompts/) - self-contained cloud-agent prompts for each workstream.

The current working theory is that `StreamingBuilder` should stop being the only
column construction abstraction. Instead, `logfwd-arrow` should expose a shared
columnar construction engine that supports both dynamic scanner-discovered fields
and planned/typed protocol fields. Existing scanner-facing APIs can become an
adapter over that engine; OTLP should use planned handles and native binary types
without leaking OTLP semantics into `logfwd-arrow`.
