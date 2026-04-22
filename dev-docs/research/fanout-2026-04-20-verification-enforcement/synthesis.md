# Verification Enforcement Fanout — Synthesis

## Status: All 4 workstreams completed successfully

| Workstream | Status | Output | Quality |
|------------|--------|--------|---------|
| A: xtask verify | READY | 741 lines, 15 files, full xtask binary | High — 6 checks implemented, justfile + CI integration |
| B: verified attrs | READY | 314 lines, 7 files, proc-macro crate | High — `#[verified(kani)]` with compile-time linkage |
| C: fuzz targets | READY | 3342 lines, 22 files, 6 new fuzz targets | High — corpus seeds, `#[cfg(fuzzing)]` re-exports, manifest |
| D: proptest gaps | READY | 427 lines, 5 files, shared generator | High — row_json + json_lines proptests, arb_record_batch |

## Convergence

All 4 workstreams converge on the same architecture:

1. **`xtask verify`** is the CI enforcement backbone — runs 6 structural checks against source
2. **`#[verified(kani)]`** provides compile-time fast feedback for proof linkage
3. **Fuzz manifest** (`dev-docs/verification/fuzz-manifest.toml`) is the trust boundary source of truth
4. **Shared test generators** (`logfwd-test-utils/src/arrow.rs`) enable consistent property testing

## Integration Plan

### Phase 1: Land independently (no conflicts between workstreams)

The 4 workstreams touch different files:
- A: `xtask/`, `justfile`, `.github/workflows/ci.yml`, `dev-docs/VERIFICATION.md`
- B: `crates/logfwd-lint-attrs/src/lib.rs`, `crates/logfwd-core/src/otlp.rs`
- C: `crates/logfwd-io/fuzz/`, `crates/logfwd-io/src/{otlp_receiver,otap_receiver,arrow_ipc_receiver}.rs`
- D: `crates/logfwd-output/src/{row_json,json_lines}.rs`, `crates/logfwd-test-utils/src/arrow.rs`

**Overlap:** A and C both create `dev-docs/verification/fuzz-manifest.toml` — merge A's version (xtask checks it) with C's version (populated with targets).

### Phase 2: Wire together

After all 4 land:
1. `xtask verify` reads the fuzz manifest from C and checks targets exist
2. `xtask verify` reads `#[verified(kani)]` annotations from B and checks proof existence
3. CI runs `just verify` which calls `cargo xtask verify`
4. The proptest from D ensures row_json.rs is no longer a zero-test module (check 3 passes)

### Phase 3: Expand coverage

- Annotate remaining 50+ functions in logfwd-core with `#[verified(kani)]`
- Add `#[trust_boundary]` annotations to the 16 functions identified in research
- Add more fuzz targets from the trust boundary research (CSV enrichment, segment recovery)

## Key Decisions

### xtask verify (Workstream A)
- **6 checks implemented:** pub_fn_needs_proof, unsafe_needs_safety_comment, pub_module_needs_tests, encode_decode_roundtrip, trust_boundary_manifest, proof_count_manifest
- **Exemption mechanism:** `// xtask-verify: allow(check_name) reason: ...` comments
- **Integration:** `just verify` recipe, CI workflow step

### Verified attrs (Workstream B)
- **Compile-time linkage:** `#[verified(kani = "verify_foo")]` → `#[cfg(kani)] const _: () = { let _ = verification::verify_foo as fn(); };`
- **Error message:** Points at annotation site with missing proof name
- **20 functions annotated** in otlp.rs as proof of concept

### Fuzz targets (Workstream C)
- **6 targets added:** fuzz_otlp_protobuf_decode, fuzz_otlp_json_decode, fuzz_decompress, fuzz_otap_decode, fuzz_arrow_ipc_decode, fuzz_protojson_numbers
- **Corpus seeds** for valid protocol messages
- **`#[cfg(fuzzing)]` re-exports** for pub(super) functions
- **Fuzz manifest** with 14 total targets (8 existing + 6 new)

### Proptest (Workstream D)
- **Shared `arb_record_batch` generator** in logfwd-test-utils supporting 8+ Arrow types
- **5 proptests for row_json.rs:** valid JSON output, field count, null handling, special floats, path agreement
- **2 proptests for json_lines.rs:** valid JSON per line, no embedded newlines
- **row_json.rs goes from 0 to 7 tests**

## Risks

1. **Branch staleness:** The workstreams branched from `research/dylint-verification-lints` which is now far behind main. All diffs need rebasing.
2. **CI.yml conflicts:** Workstreams A and C both modify CI workflow. Need careful merge.
3. **logfwd-lint-attrs already exists on main** (landed in a separate PR). Workstream B needs to extend it, not recreate.

## Recommendation

**Land in order: D → C → B → A**

- D (proptest) is lowest conflict risk, highest immediate value (fixes the zero-test gap)
- C (fuzz targets) is independent of the others
- B (verified attrs) depends on logfwd-lint-attrs already on main
- A (xtask verify) depends on the fuzz manifest from C being in place

Each workstream's diff should be rebased onto main, conflict-resolved, and PR'd independently.
