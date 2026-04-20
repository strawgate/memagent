# High-Quality Rust Practices

Rust review checklist for correctness and performance-sensitive code.

## Hard Fails

Reject new production-path code that introduces:

- `.unwrap()` / panic-driven control flow.
- Unchecked indexing where bounds safety is not guaranteed.
- Per-record allocations in hot-path loops without evidence.
- `unsafe` without clear and specific `// SAFETY:` justification.

## Hot Path Checklist

For reader → framer → scanner → builders → OTLP encoder → compress paths:

- Avoid per-line/per-record allocations when a borrowed or reused buffer works.
- Prefer borrowing/views over accidental copies in hot paths.
- Keep formatting/string construction out of tight loops.
- Benchmarks provided when performance risk is non-trivial.

## API and Type Quality

- Public APIs return `Result`/`Option` on user-input failure.
- Naming and ownership are explicit and consistent.
- State-machine logic prefers compile-time constraints or clearly tested transitions.

## Test Expectations

- New behavior includes focused tests.
- Regression-prone edge cases are covered.
- Verification requirements are respected for core logic.

## Canonical References

- [`dev-docs/CODE_STYLE.md`](../CODE_STYLE.md)
- [`dev-docs/VERIFICATION.md`](../VERIFICATION.md)
