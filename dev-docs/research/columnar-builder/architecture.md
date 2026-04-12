# Columnar Builder Architecture Brief

> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Foundation brief for evolving `StreamingBuilder` into a long-lived shared columnar construction architecture.

## Problem

The OTLP receiver needs to ingest protobuf log payloads directly into Arrow
RecordBatches at very high throughput. The current prost path builds a large
object graph and then converts to Arrow; the experimental projected wire decoder
shows that bypassing that object graph is materially faster and allocates orders
of magnitude fewer objects per row.

The remaining bottleneck is no longer just protobuf parsing. Profiles show that
Arrow materialization, generated string handling, dynamic field lookup, and OTLP
output re-encoding dominate the projected path.

This is a 10-year maintainability decision. A fully separate OTLP-only Arrow
builder might be fastest in the short term, but it risks duplicating schema,
conflict, nullability, string-backing, and output compatibility semantics that the
rest of the pipeline also needs.

## Decision Target

Choose an architecture that gives structured protocols the performance of typed
builders while preserving a single shared implementation of columnar construction
semantics.

The preferred direction to investigate is:

```text
scanner / protocol decoder
        -> schema plan + field handles
        -> shared columnar batch builder
        -> Arrow RecordBatch
```

`StreamingBuilder` should either become:

- an adapter over this lower-level engine, or
- be replaced by a clearer builder split if that is simpler and more maintainable.

Backward compatibility with the current `StreamingBuilder` API is not a goal.
Compatibility of produced RecordBatches with transforms and outputs is a goal.
That compatibility includes the existing mixed-type conflict contract:
`StreamingBuilder` emits one `StructArray` under the original field name when a
field contains multiple primitive types in a batch. The shared columnar builder
must preserve that representation, or change transforms and outputs in the same
PR with explicit migration evidence.

## Current Evidence

The experimental OTLP projected-view path at foundation baseline commit
`a622416d8f54686dc31c4a75b5c6877e7d57471c` has already shown large wins over
`main`/prost on synthetic but decision-grade fixtures:

| Fixture | Path | `main` prost | projected-view | Result |
|---|---:|---:|---:|---:|
| attrs-heavy | decode | 4175.6 ns/row | ~1734 ns/row | ~2.4x faster |
| attrs-heavy | decode->encode | 4677.5 ns/row | ~2435 ns/row | ~1.9x faster |
| wide-10k | decode | 2400.7 ns/row | ~1077 ns/row | ~2.2x faster |
| wide-10k | decode->encode | 2931.1 ns/row | ~1580 ns/row | ~1.9x faster |

Projected-view decode allocation after the current foundation work:

| Fixture | Bytes/row | Allocs/row |
|---|---:|---:|
| attrs-heavy | ~2142 | ~0.443 |
| wide-10k | ~1555 | ~0.055 |

The prost baseline allocated roughly `39-71` objects per row on decode.

## Current Hotspots

Representative projected-view profiles show these remaining areas:

- `StreamingBuilder::finish_batch`: sparse-to-dense Arrow materialization.
- Trace/span ID handling: binary OTLP IDs are represented as hex strings.
- Dynamic field lookup: repeated hash lookup for record attributes.
- `OtlpSink::encode_batch`: row grouping and repeated protobuf key/value encoding.
- Arrow `StringViewBuilder::with_capacity` and dense null/value allocation.

The multi-block StringView change removed a large full-input copy when decoded
strings exist. That is evidence for making backing-buffer/block management a
first-class builder concept.

## Architectural Principles

1. **No OTLP semantics in `logfwd-arrow`.**
   `logfwd-arrow` may know about `FieldKind::FixedBinary(16)`. It must not know
   that the field is an OTLP trace ID.

2. **One shared columnar semantics implementation.**
   Null behavior, duplicate handling, conflict handling, field metadata, string
   backing, and Arrow materialization should not be reimplemented per protocol.

3. **Planned/typed protocols should not pay dynamic-scanner costs.**
   OTLP canonical fields are known. They should use handles and dense typed
   writers, not repeated dynamic `resolve_field` calls and sparse row/value
   vectors.

4. **Dynamic data remains first-class.**
   Generic JSON logs and dynamic OTLP attributes still need schema discovery,
   conflict handling, and sparse storage or a similarly flexible representation.

5. **No cross-batch caches by default.**
   Batch-local plans/caches are acceptable. Cross-event or cross-batch caches
   require explicit justification and measurement.

6. **Correctness is reference-driven.**
   Projected OTLP decode must be checked against prost/reference conversion and
   malformed protobuf tests. Performance cannot replace parity.

## Proposed Core Concepts

### `ColumnarBatchBuilder`

The shared engine that owns rows, column writers, backing blocks, and final Arrow
RecordBatch materialization.

Potential API shape:

```rust
builder.begin_batch(input_bytes, row_hint);
builder.begin_row();
builder.append_i64(timestamp, value);
builder.append_f64(latency, value);
builder.append_bool(cache_hit, value);
builder.append_utf8_view(body, bytes);
builder.append_decoded_utf8(message, bytes);
builder.append_binary_view(payload, bytes);
builder.append_fixed_binary(trace_id, bytes);
builder.end_row();
let batch = builder.finish()?;
```

### `FieldHandle`

A stable field handle returned by a plan or resolver. Hot loops should use
handles, not names.

```rust
let timestamp = plan.known("timestamp", FieldKind::Int64);
builder.append_i64(timestamp, ts);
```

### `SchemaPlan` / `BatchPlan`

A batch-local plan that defines known fields, dynamic field policy, conflict
policy, metadata, constants, and row hints.

Dynamic APIs should support position-aware resolution for common structured
payloads:

```rust
let attr = plan.resolve_dynamic_at_position(position, key, observed_kind);
```

### `ColumnWriter`

Writers should be chosen by field kind and usage pattern:

| Writer | Purpose |
|---|---|
| dense `Int64` / `Float64` / `Bool` | known typed fields |
| dense `Utf8View` | known string fields backed by input/decoded blocks |
| `BinaryView` / `FixedSizeBinary` | raw trace/span/body bytes |
| sparse dynamic writer | generic scanner fields and dynamic attrs |
| conflict writer | mixed observed types |
| constant writer | resource attrs and metadata-derived columns |

### `BlockStore`

Backing memory should be modeled as multiple blocks, not fake combined offsets:

```rust
BlockId::Input
BlockId::Decoded
BlockId::Generated
BlockId::External(...)
```

A view is `(block, offset, len)`. This supports zero-copy input views and decoded
strings without copying the full input buffer.

## Key Open Decisions

1. Is a shared `ColumnarBatchBuilder` simpler than replacing `StreamingBuilder`
   outright?
2. Should planned fields use dense Arrow builders directly, dense vectors plus
   null bitmaps, or a hybrid?
3. How should dynamic conflicts be represented for OTLP attributes and generic
   JSON fields?
4. Should trace/span IDs become `FixedSizeBinary(16/8)`, `BinaryView`, or both?
5. Is an EventTape layer useful for correctness and fuzzing, or too expensive?
6. Can protobuf descriptors/codegen reduce handwritten wire-decoder risk without
   sacrificing performance?
7. How much OTLP output optimization belongs in the builder plan versus
   `logfwd-output` encode planning?
8. What real-world payload corpus is sufficient to decide this architecture?

## Non-Goals For The Foundation Branch

- Finishing the new builder architecture.
- Preserving old `StreamingBuilder` method compatibility.
- Merging an OTLP-only builder without comparison.
- Adding cross-batch caches.
- Declaring the synthetic benchmark fixtures sufficient for final decisions.

## Success Criteria For Fan-In

A proposal is strong if it:

- reduces decode/materialize and E2E time on the OTLP suite,
- reduces allocation bytes/row or allocation count,
- keeps prost/reference parity,
- reduces or contains handwritten protocol-specific code,
- preserves Arrow/transform/output interoperability,
- keeps OTLP semantics outside `logfwd-arrow`, and
- has a plausible migration path that can be delivered incrementally.
