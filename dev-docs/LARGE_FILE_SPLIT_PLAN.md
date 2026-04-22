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

Prefer the least disruptive split that materially improves navigability while
preserving the public API:

1. Keep the existing wrapper file (`<module>.rs`) as the public entrypoint when
   callers already import that path and the split is primarily about file size.
2. Shard large implementations into thematic companion files (`<module>_parts/`,
   `generated_parts/`, `tests_parts/`) and have the wrapper file include or
   delegate to them.
3. Promote to a full `<module>/mod.rs` tree only when the split exposes stable
   internal seams that benefit from real submodule visibility boundaries.
4. Keep each shard below ~600 lines; if one grows, split again by function
   family or test theme.
5. Keep tests close to the wrapper/module they validate unless an existing
   generated/test-only layout already exists.
6. Run crate-targeted tests first, then workspace-level checks.

## Proposed split skeletons

### `json_scanner`
- `json_scanner.rs` wrapper or `json_scanner/mod.rs`
- `json_scanner/tokenize.rs`
- `json_scanner/parse.rs`
- `json_scanner/scan_state.rs`
- `json_scanner/builders.rs`
- `json_scanner/tests.rs`

### `validate`
- `validate.rs` wrapper or `validate/mod.rs`
- `validate/schema.rs`
- `validate/rules.rs`
- `validate/errors.rs`
- `validate/normalization.rs`
- `validate/tests.rs`

### `query_analyzer`
- `query_analyzer.rs` wrapper or `query_analyzer/mod.rs`
- `query_analyzer/ast.rs`
- `query_analyzer/rewrite.rs`
- `query_analyzer/validation.rs`
- `query_analyzer/planning.rs`
- `query_analyzer/tests.rs`

### `diagnostics/server`
- `diagnostics/server.rs` wrapper or `diagnostics/server/mod.rs`
- `diagnostics/server/router.rs`
- `diagnostics/server/handlers.rs`
- `diagnostics/server/state.rs`
- `diagnostics/server/transport.rs`
- `diagnostics/server/tests.rs`

### `enrichment`
- `enrichment.rs` wrapper or `enrichment/mod.rs`
- `enrichment/catalog.rs`
- `enrichment/lookup.rs`
- `enrichment/record_batch.rs`
- `enrichment/source.rs`
- `enrichment/tests.rs`

### `host_metrics`
- `host_metrics.rs` wrapper or `host_metrics/mod.rs`
- `host_metrics/cpu.rs`
- `host_metrics/memory.rs`
- `host_metrics/disk.rs`
- `host_metrics/net.rs`
- `host_metrics/tests.rs`

### `generator`
- `generator.rs` wrapper
- `generator_parts/part_*.rs`
- `generator_parts/tests_*.rs`

### `projection/tests`
- `projection/tests.rs` wrapper
- `projection/tests_parts/part_*.rs`

### `tcp_input`
- `tcp_input.rs` wrapper
- `tcp_input_parts/part_*.rs`
- `tcp_input_parts/tests_*.rs`

### `projection/generated`
- keep generated origin path unchanged if required by codegen, but emit segmented shards behind the wrapper:
  - `projection/generated.rs` (wrapper + shard declarations)
  - `projection/generated_parts/part_1.rs`
  - `projection/generated_parts/part_2.rs`
  - `projection/generated_parts/part_3.rs`
  - `projection/generated_parts/part_4.rs`

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
