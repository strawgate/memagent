# Architecture Decisions

Decisions made, with reasoning. Reference this before reopening a
settled question.

## no_std for logfwd-core (not std + clippy)

**Decision:** `#![no_std]` with `extern crate alloc`.

**Why:** Structural enforcement. The compiler blocks IO — not lints
that can be `#[allow]`'d. This is the rustls/s2n-quic pattern. Four
deep research studies unanimously recommended this over std + clippy.

**Cost:** hashbrown for HashMap (if needed), `core::`/`alloc::` import
paths, memchr needs `default-features = false`.

**Benefit:** Impossible to accidentally add IO to the proven core.

## FieldSink trait boundary (not type return)

**Decision:** Generic trait (serde Visitor pattern), not Vec<ParsedField>.

**Why:** Zero allocation at the boundary. Full inlining via
monomorphization. Kani can verify for a concrete mock type. This is
how serde achieves zero-cost serialization — the data flows directly
from parser to builder without intermediate representation.

**Alternative considered:** SmallVec<[ParsedField; 16]> return type.
Research showed Vec is often faster than SmallVec in benchmarks, and
the callback pattern eliminates allocation entirely.

## Kani for exhaustive, proptest for unbounded

**Decision:** Kani for functions with small fixed-width inputs (u64
bitmask ops, varint, state machines). proptest for everything else.

**Why:** Kani's practical limit is ~16-32 bytes for complex parsing.
Our JSON parser handles arbitrary-length input — Kani can't prove it.
But Kani CAN prove bitmask operations for ALL u64 inputs (2^64 states)
in seconds. Use each tool where it's strongest.

**Composition:** Kani function contracts + `stub_verified` lets us
prove sub-components and compose them. Parser sub-functions proven
individually; top-level parser uses proven stubs.

## TLA+ for pipeline liveness (not Kani)

**Decision:** TLA+ specification for "data is never abandoned."

**Why:** Kani is a bounded model checker — it can't prove temporal
properties like "eventually." TLA+ can. AWS, CockroachDB, and Datadog
use TLA+ for their critical protocol designs.

## Stay independent from OTAP (don't depend on their crates)

**Decision:** Implement OTAP wire protocol from proto spec.

**Why:** All otap-dataflow Rust crates have `publish = false`, use
Arrow 58.1 (we're on 54), and bring a massive dependency tree
(DataFusion, prost, tonic, roaring, ciborium). The protocol is still
experimental. We'd couple to their upgrade schedule and architecture.

**When to revisit:** If they publish stable crates to crates.io
with semver guarantees.

## Flat schema (not OTAP star schema)

**Decision:** Single RecordBatch with all fields as columns.
`_resource_*` prefix for resource attributes.

**Why:** Directly queryable by DuckDB, Polars, DataFusion with zero
schema knowledge. OTAP's star schema (4+ tables with foreign keys)
is optimized for wire efficiency, not queryability.

**OTAP compatibility:** Convert at the boundary. Star-to-flat for
receiving, flat-to-star for sending.

## Two-stage architecture: SIMD detects, scalar consumes

**Decision:** Stage 1 (SIMD character detection) and Stage 2
(parsing/field extraction) are separate. SIMD produces bitmasks.
A sequential scanner consumes them. They never mix.

**Why:** SIMD is embarrassingly parallel WITHIN a 64-byte block —
every byte compared independently against 9 needles. But parsing
is inherently sequential — the meaning of a comma depends on
whether we're inside a string. These are fundamentally different
parallelism profiles.

**How it works:**
- Stage 1 runs SIMD on the whole buffer (or per-block streaming),
  producing u64 bitmasks. Cross-block state: only 2 u64 values
  (escape carry, string interior carry).
- Stage 2 walks through structural positions sequentially using
  `trailing_zeros` + clear-lowest-bit. The scanner state machine
  tracks "what token am I expecting next?" Parser state carries
  across blocks but has no effect on SIMD.

**No SIMD is lost by going sequential.** The current scanner is
already sequential — it calls `next_quote(pos)` one token at a
time. Sequential bitmask iteration just replaces the stored-vector
lookup with a stack-local `trailing_zeros` operation.

**This is the proven simdjson architecture.** Stage 1 (SIMD) +
Stage 2 (sequential scalar) achieves >2 GB/s in simdjson.

## Scalar SIMD fallback in core (SIMD in logfwd-arrow)

**Decision:** Core has a safe scalar `find_char_mask`. SIMD impls
live in logfwd-arrow behind a `CharDetector` trait.

**Why:** `#![forbid(unsafe_code)]` in core. SIMD intrinsics require
unsafe. Kani proves the scalar path. proptest verifies SIMD matches
scalar. Performance comes from SIMD at runtime; correctness is proven
on the scalar path.

## Pipeline state machine over linear BatchToken

**Decision:** Prove pipeline correctness via a pure state machine
in core (Kani single-step + TLA+ liveness), not a linear type.

**Why:** Strictly linear `BatchToken` is impossible with async
cancellation. Tokio can cancel a future at any `.await` point,
which means a `#[must_use]` token can be dropped without being
consumed. The type system cannot prevent this.

**Strategy:** The state machine in core is proven correct: no state
exists where a batch is "forgotten." The async shell handles
best-effort delivery (At-Least-Once) using the Arrow IPC segment
store for durability. Logic correctness is proven; delivery
guarantees are operational.

**Related:** #270 (pipeline state machine), #272 (TLA+ liveness).

## Opaque checkpoints (not u64 offsets)

**Decision:** `BatchTicket<S, C>` and `PipelineMachine<S, C>` are generic
over checkpoint type `C`. The pipeline stores and forwards checkpoints
without interpreting them.

**Why:** Research across Filebeat, Vector, Fluent Bit, and OTel Collector
shows that "offset" means fundamentally different things per input:
- File tail: `u64` byte position (contiguous)
- Kafka: `i64` partition offset (gaps from compaction/txn aborts)
- Journald: opaque cursor string
- Windows Event Log: opaque bookmark XML
- Push sources (OTLP, syslog): no checkpoint at all

A `u64` offset with contiguity enforcement would only work for file
inputs. Making the pipeline generic lets each input own its checkpoint
semantics while the pipeline provides ordering guarantees via `BatchId`.

**Cost:** `C: Clone` bound on `PipelineMachine` methods. Generic type
parameter propagates to `AckReceipt<C>`, `CommitAdvance<C>`.

**Benefit:** Pipeline works for all input types without adaptation.
Checkpoint validation (contiguity for files, gap tolerance for Kafka)
stays in the input plugin where it belongs.

**Research:** `dev-docs/research/offset-checkpoint-research.md`
**Related:** #270 (pipeline state machine).
