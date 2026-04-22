# Large Rust File Audit and Split Plan

This document captures a baseline audit of Rust files above 1000 lines and a concrete split target of keeping each module file below 600 lines.

## Audit command

```bash
python scripts/audit_large_rust_files.py --root . --threshold 1000 --target 600 --top 10
```

## Top 10 oversized files and target split counts

| Rank | File | Lines | Suggested parts | Estimated max lines/part |
|---|---|---:|---:|---:|
| 1 | `crates/logfwd-core/src/json_scanner.rs` | 2797 | 5 | 560 |
| 2 | `crates/logfwd-config/src/validate.rs` | 2794 | 5 | 559 |
| 3 | `crates/logfwd-transform/src/query_analyzer.rs` | 2694 | 5 | 539 |
| 4 | `crates/logfwd-diagnostics/src/diagnostics/server.rs` | 2648 | 5 | 530 |
| 5 | `crates/logfwd-transform/src/enrichment.rs` | 2543 | 5 | 509 |
| 6 | `crates/logfwd-io/src/host_metrics.rs` | 2402 | 5 | 481 |
| 7 | `crates/logfwd-io/src/generator.rs` | 2386 | 4 | 597 |
| 8 | `crates/logfwd-io/src/otlp_receiver/projection/tests.rs` | 2288 | 4 | 572 |
| 9 | `crates/logfwd-io/src/tcp_input.rs` | 2276 | 4 | 569 |
| 10 | `crates/logfwd-io/src/otlp_receiver/projection/generated.rs` | 2272 | 4 | 568 |

## Refactor pattern to apply per file

Use a `mod.rs` plus thematic submodules, preserving public API and behavior:

1. Convert `<module>.rs` into `<module>/mod.rs` with only:
   - imports shared across submodules,
   - public types and public re-exports,
   - submodule declarations.
2. Split logic into submodules by cohesive concern (parser, builder, state, io, tests).
3. Keep each submodule below 600 lines; if one grows, split again by function family.
4. Keep tests close to changed code unless test-only files are already established.
5. Run crate-targeted tests first, then workspace-level checks.

## Proposed split skeletons

### `json_scanner`
- `json_scanner/mod.rs`
- `json_scanner/tokenize.rs`
- `json_scanner/parse.rs`
- `json_scanner/scan_state.rs`
- `json_scanner/builders.rs`
- `json_scanner/tests.rs`

### `validate`
- `validate/mod.rs`
- `validate/schema.rs`
- `validate/rules.rs`
- `validate/errors.rs`
- `validate/normalization.rs`
- `validate/tests.rs`

### `query_analyzer`
- `query_analyzer/mod.rs`
- `query_analyzer/ast.rs`
- `query_analyzer/rewrite.rs`
- `query_analyzer/validation.rs`
- `query_analyzer/planning.rs`
- `query_analyzer/tests.rs`

### `diagnostics/server`
- `diagnostics/server/mod.rs`
- `diagnostics/server/router.rs`
- `diagnostics/server/handlers.rs`
- `diagnostics/server/state.rs`
- `diagnostics/server/transport.rs`
- `diagnostics/server/tests.rs`

### `enrichment`
- `enrichment/mod.rs`
- `enrichment/catalog.rs`
- `enrichment/lookup.rs`
- `enrichment/record_batch.rs`
- `enrichment/source.rs`
- `enrichment/tests.rs`

### `host_metrics`
- `host_metrics/mod.rs`
- `host_metrics/cpu.rs`
- `host_metrics/memory.rs`
- `host_metrics/disk.rs`
- `host_metrics/net.rs`
- `host_metrics/tests.rs`

### `generator`
- `generator/mod.rs`
- `generator/config.rs`
- `generator/sources.rs`
- `generator/scheduling.rs`
- `generator/tests.rs`

### `projection/tests`
- `projection/tests/mod.rs`
- `projection/tests/roundtrip.rs`
- `projection/tests/type_coercion.rs`
- `projection/tests/error_paths.rs`
- `projection/tests/edge_cases.rs`

### `tcp_input`
- `tcp_input/mod.rs`
- `tcp_input/listener.rs`
- `tcp_input/connection.rs`
- `tcp_input/framing.rs`
- `tcp_input/tests.rs`

### `projection/generated`
- keep generated origin path unchanged if required by codegen, but emit segmented includes:
  - `projection/generated.rs` (wrapper + include declarations)
  - `projection/generated/part_1.rs`
  - `projection/generated/part_2.rs`
  - `projection/generated/part_3.rs`
  - `projection/generated/part_4.rs`

## Recommended execution order

1. Start with test-only/generated-heavy files (#8, #10) to validate structure and import patterns.
2. Refactor runtime/IO modules (#6, #7, #9).
3. Refactor transform/config modules (#2, #3, #5).
4. Refactor diagnostics/core modules (#1, #4) last due to broad cross-crate impact.

## Verification checklist

- `just fmt`
- `just clippy`
- `just test`
- plus crate-focused checks for each touched crate (for faster iteration)

