# Arrow Notes (Repo-Scoped)

How Arrow is used in logfwd.

## Role

- Scanner and builders produce Arrow-native columns.
- SQL transform consumes/produces `RecordBatch`.
- Output adapters serialize Arrow-derived data.

## Invariants

- Keep zero/low-copy paths when practical.
- Preserve row-count and nullability alignment across columns.
- Keep type-conflict behavior stable and documented.

## Change Checklist

If Arrow schema or builder behavior changes:

- Update `book/src/config/reference.md` (operator-facing impact).
- Update `dev-docs/SCANNER_CONTRACT.md` when scanner output contract changes.
- Add regression tests for mixed types and sparse fields.
- Re-run performance checks for hot-path changes.

## High-Risk Areas

- Conflict struct normalization.
- String view/buffer lifetime assumptions.
- Mid-batch field appearance and null backfill behavior.

## Canonical Docs

- Scanner contract: `dev-docs/SCANNER_CONTRACT.md`
- Architecture: `dev-docs/ARCHITECTURE.md`
- Verification policy: `dev-docs/VERIFICATION.md`

## Upstream

- https://arrow.apache.org/docs/
