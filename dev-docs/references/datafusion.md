# DataFusion Notes (Repo-Scoped)

How we use DataFusion in ffwd, without generic tutorial material.

## Role in Pipeline

- Input bytes become Arrow `RecordBatch`.
- DataFusion runs SQL transforms over `logs` batches.
- Output sinks receive transformed batches.

## Repo Rules

- `ffwd-transform` owns SQL behavior.
- Keep query semantics deterministic across batches.
- Avoid config-driven magic outside canonical SQL behavior.
- Treat DataFusion as engine, not place for business policy.

## Change Checklist

When changing transform behavior:

- Update `book/src/content/docs/configuration/sql-transforms.md`.
- Update `book/src/content/docs/configuration/reference.mdx` for any schema/UDF changes.
- Update `dev-docs/ARCHITECTURE.md` if data flow changes.
- Add tests for null handling, type conflicts, and empty batch behavior.

## Performance Guardrails

- Do not add avoidable serialization between Arrow and SQL layers.
- Keep schema normalization behavior explicit and tested.
- Benchmark transform-heavy changes (`just bench` when relevant).

## Canonical Docs

- Architecture contract: `dev-docs/ARCHITECTURE.md`
- SQL user docs: `book/src/content/docs/configuration/sql-transforms.md`
- Crate rules: `dev-docs/CRATE_RULES.md`

## Upstream

- https://datafusion.apache.org/
