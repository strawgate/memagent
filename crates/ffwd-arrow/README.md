# ffwd-arrow

Arrow integration layer. Implements ffwd-core's ScanBuilder trait to produce RecordBatch.

## Constraints

| Rule | Enforcement |
|------|-------------|
| Implements core's ScanBuilder trait | Compilation |
| proptest: SIMD output == scalar output | CI test suite |
| Deps: core + arrow + bytes | Cargo.toml |

## Key modules

- `streaming_builder.rs` — Zero-copy StringViewArray builder (hot path)
- `conflict_schema.rs` — StructArray conflict column detection + normalization
- `scanner.rs` — Scanner wrapper (zero-copy + detached output modes)
