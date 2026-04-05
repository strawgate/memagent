# Copilot Instructions for logfwd

Read `AGENTS.md` in the repository root for full project context, crate structure, reference docs, and code quality requirements.

## Quick Reference

- **Language:** Rust (stable toolchain, no hand-written unsafe — SIMD via wide crate)
- **Performance target:** 1.7M lines/sec CRI parsing + OTLP encoding, single-core ARM64
- **Build:** `just ci` (lint + test — run before any PR)
- **Test:** `just test`
- **Lint:** `just lint` (clippy + rustfmt + taplo + cargo-deny)

## Required Before Each Commit

Run these commands and fix any errors before pushing:

```bash
cargo fmt --all                                    # format all Rust code
cargo clippy --all-targets -- -D warnings          # lint — zero warnings allowed
cargo test -p <crate-you-changed>                  # test the crate you modified
```

Or run the full CI gate (slower but comprehensive):

```bash
just ci                                            # fmt + clippy + test + deny
```

If `just ci` takes too long (>5 min), at minimum run `cargo fmt --all && cargo clippy --all-targets -- -D warnings` before every commit, then run `just test` before the final push.

## PR Descriptions

- Use `Closes #N` for every issue the PR fully resolves (auto-closes on merge)
- Use `Relates to #N` for partial fixes
- Fill in the PR template — summary, closing keywords, test plan

## What NOT to Do

- Do not add dependencies without justification
- Do not refactor code unrelated to your task
- Do not introduce allocations in hot paths without benchmarking
- Do not use `.unwrap()` in production code paths
