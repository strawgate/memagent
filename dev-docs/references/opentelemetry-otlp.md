# OTLP Notes (Repo-Scoped)

Boundary expectations for OTLP ingest and egress in ffwd.

## Role

- OTLP receiver is an input adapter.
- OTLP sink is an output adapter.
- Core semantics should not depend on transport details.

## Contract Highlights

- Reject malformed payloads predictably.
- Keep diagnostics counters aligned with accepted/rejected behavior.
- Preserve checkpoint semantics across retries and failures.

## Change Checklist

When touching OTLP paths:

- Update `dev-docs/ADAPTER_CONTRACT.md`.
- Update user-facing config docs if fields or behavior changed:
  `book/src/content/docs/configuration/reference.mdx`.
- Add integration tests for malformed payloads, backpressure, and retry behavior.

## Canonical Docs

- Adapter correctness: `dev-docs/ADAPTER_CONTRACT.md`
- Pipeline boundaries: `dev-docs/ARCHITECTURE.md`

## Upstream

- https://opentelemetry.io/docs/specs/otlp/
