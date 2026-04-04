# logfwd-arrow

Arrow integration layer. Implements logfwd-core's ScanBuilder trait to produce RecordBatch.

## Constraints

| Rule | Enforcement |
|------|-------------|
| Implements core's ScanBuilder trait | Compilation |
| proptest: SIMD output == scalar output | CI test suite |
| Deps: core + arrow + bytes | Cargo.toml |

## Key modules

- `streaming_builder.rs` — Zero-copy StringViewArray builder (hot path)
- `storage_builder.rs` — Owned-buffer builder for persistence
- `conflict_schema.rs` — StructArray conflict column detection + normalization
- `scanner.rs` — CopyScanner and ZeroCopyScanner wrappers
