# ColumnarBatchBuilder Design Intent

> **Status:** Completed
> **Date:** 2026-04-12
> **Context:** Recovered design intent for the shared `ColumnarBatchBuilder` direction, landing independently of OTLP projection PR #1837.

This note captures the current design stance for the shared column construction engine.

`ColumnarBatchBuilder` is the reusable column construction engine for structured inputs. It is **not** an OTLP-specific builder.

The intended flow is:

```text
producer/parser
  -> row/value facts or typed field writes
  -> shared ColumnarBatchBuilder
  -> Arrow RecordBatch
```

## Recovered design intent

The repo already has the core correctness model in `StreamingBuilder`:

- direct incremental Arrow builder null-padding already failed in `IndexedBatchBuilder`
- proptest found column-length mismatches when fields appeared sparsely across batches
- `StreamingBuilder` instead records row/value facts and bulk-builds columns at `finish_batch`
- gaps become nulls and columns cannot drift out of alignment

That deferred sparse-row model is the invariant worth preserving.

## Responsibilities split

### Producers own source semantics

Producers and parsers decide how source data becomes field observations.

Examples:

- JSON scanner
- CRI parser
- CSV parser
- OTLP prost conversion
- OTLP wire projection

Their job is to normalize source-specific syntax, detect fields, and emit typed appends or row/value facts.

### Builder owns column construction

`ColumnarBatchBuilder` owns the shared mechanics:

- row/column alignment
- sparse padding
- conflict columns for mixed-type JSON fields
- backing blocks for input bytes, decoded strings, generated strings, and future external buffers
- detach/view finalization
- schema metadata
- Arrow materialization

The builder should stay source-agnostic. It should not know OTLP field semantics.

## StreamingBuilder role

`StreamingBuilder` should become the scanner-facing `ScanBuilder` adapter over the shared engine.

That means:

- scanner code continues to talk to the existing `ScanBuilder` boundary
- `StreamingBuilder` remains the adapter that satisfies scanner lifecycle and `RecordBatch` output expectations
- the reusable engine underneath can be shared by structured producers that are not part of the scanner loop

`ScanBuilder` is the contract boundary, not the source of protocol meaning.

## Type and conflict policy

Typed inputs must stay clean.

- OTLP, CSV, Arrow IPC, and other schema-fixed inputs should use typed append paths and should not inherit JSON-style mixed-type conflict mechanics.
- JSON-style conflict handling only activates when a field genuinely changes type within a batch.
- Clean typed inputs must not pay conflict overhead they do not need.

The output schema can use generic Arrow kinds such as:

- `FixedBinary(16)`
- `BinaryView`
- `Utf8View`
- `Int64`
- `Bool`

Those are builder-level kinds. OTLP semantics stay outside `logfwd-arrow`.

## Validation boundaries

The shared engine still has to enforce the same hard limits the current builder relies on:

- buffer lengths must fit the relevant offset representation
- batch sizes must stay within sane memory limits
- view-backed string buffers must remain valid until batch finalization
- detached batches must remain self-contained

This is where `StreamingBuilder`-style offset handling and `StringView` ownership rules matter.

## Relationship to OTLP projection work

The experimental OTLP wire projection is evidence for the builder direction, not the final builder architecture.

It is useful because it shows:

- whether direct typed appends are worth it
- where view-backed strings help
- what append/storage surface the shared engine needs

It does **not** mean OTLP semantics belong in `logfwd-arrow`.

## Canonical stance

The durable architecture is:

1. parsers/producers own source semantics
2. the shared builder owns deferred sparse column construction
3. `StreamingBuilder` remains the scanner adapter
4. OTLP-specific meaning stays in OTLP crates
5. typed inputs keep typed fast paths
6. JSON conflict handling remains a dynamic-input concern only

## Next work units

- [#1838](https://github.com/strawgate/memagent/issues/1838) architecture: shared ColumnarBatchBuilder for structured inputs
- [#1839](https://github.com/strawgate/memagent/issues/1839) work-unit: columnar docs - land recovered design intent on main

## References

- [DEVELOPING.md](../../DEVELOPING.md) for the deferred builder rationale that recovered this design
- [SCANNER_CONTRACT.md](../SCANNER_CONTRACT.md) for row count, conflict, nullability, and builder contract expectations
- [PR #1837](https://github.com/strawgate/memagent/pull/1837) for the OTLP projection evidence that informed this note
