# Workstream 2 Result: `logfwd-output` Core Module Split

## Recommendation label

**Recommend: Adopted split (test extraction + thin crate root) and merge as-is.**

This split materially reduces `lib.rs` size and review surface while preserving exports and runtime behavior.

## Goal recap

Refactor `crates/logfwd-output/src/lib.rs` into smaller internal modules while preserving behavior/API and keeping hot-path behavior neutral.

## Selected module boundaries

### Boundary chosen

1. **Crate root / wiring module (`lib.rs`)**
   - Keeps only crate-level module declarations, re-exports, and the tiny retry helper (`is_transient_error`) used by unit tests.
   - Retains all existing `pub use` surface and `pub(crate)` re-exports.

2. **Large crate-level unit-test module (`tests.rs`)**
   - Extracted the previous inline `#[cfg(test)] mod tests { ... }` block from `lib.rs` into `crates/logfwd-output/src/tests.rs` via `#[cfg(test)] mod tests;`.
   - Keeps the same test helpers and test cases; no behavioral change in production code paths.

### Why this boundary

- **High impact with low risk:** the largest concentration in `lib.rs` was test code. Moving it out shrinks the file from 1759 lines to 86 lines while avoiding churn in sink implementations.
- **Behavior preservation:** no production sink logic moved; no function signatures, trait shapes, or exported names changed.
- **Review ergonomics:** crate wiring is now readable in one screen; tests are still unit tests (inside `src/`) and keep private access.

## Before vs After module map

### Before

- `lib.rs` contained:
  - module declarations + re-exports
  - test-only helper (`is_transient_error`)
  - **large inline `mod tests` (~1600+ lines)**

### After

- `lib.rs` contains:
  - module declarations + re-exports
  - test-only helper (`is_transient_error`)
  - `#[cfg(test)] mod tests;`
- `tests.rs` contains:
  - extracted crate-level unit tests and shared test helpers

## External behavior/API verification

- Preserved all existing `pub use` exports from `lib.rs`.
- Did not change sink factory behavior, sink serialization logic, OTLP/Loki/JSON lines runtime code paths, or diagnostics semantics.
- No crate boundary changes and no new dependencies.

## Changed file list

- `crates/logfwd-output/src/lib.rs`
- `crates/logfwd-output/src/tests.rs` (new)

## Required checks run

1. `cargo check -p logfwd-output` ✅
2. `NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost cargo test -p logfwd-output` ✅

Notes:
- In this environment, localhost-targeted HTTP tests can be intercepted by proxy policy unless `NO_PROXY`/`no_proxy` are set; with explicit no-proxy settings, the crate test suite passes.

## Credible alternative split considered

### Alternative: Extract internal helper modules from runtime code immediately

Example candidates:
- `lib.rs` helper into dedicated `retry.rs`.
- Additional sink-adjacent utility extraction (e.g., further split row/column JSON helpers or metadata utility grouping) into nested modules.

### Why not chosen in this pass

- Current runtime logic is already mostly separated into dedicated files (`row_json.rs`, `conflict_columns.rs`, `metadata.rs`, etc.).
- Further moving runtime helpers now would increase mechanical churn for smaller immediate payoff than removing the oversized inline test block.
- The selected split de-risks the workstream and still satisfies the “materially smaller lib.rs” success target.

## Evidence that would change my mind

I would prefer the alternative split (more runtime helper extraction) if one or more of the following appears:

1. Review feedback indicates remaining crate-root responsibilities are still unclear.
2. New helper logic starts re-accumulating in `lib.rs` (trend back to large mixed-responsibility root).
3. Upcoming changes require clearer sub-boundaries for test ownership (e.g., sink-specific test files under `src/tests/`).
4. Performance or maintenance profiling shows specific helper hotspots whose locality would benefit from dedicated modules.

## Risk assessment

- **Functional risk:** low (test extraction only, no runtime logic changes).
- **Compatibility risk:** low (export surface preserved).
- **Performance risk:** neutral (no hot-path code changed).
