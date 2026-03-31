# Copilot Instructions for logfwd

Read `AGENTS.md` in the repository root for full project context, crate structure, reference docs, and code quality requirements.

## Quick Reference

- **Language:** Rust (stable toolchain, no hand-written unsafe — SIMD via wide crate)
- **Performance target:** 1.7M lines/sec CRI parsing + OTLP encoding, single-core ARM64
- **Build:** `just ci` (lint + test — run before any PR)
- **Test:** `just test`
- **Lint:** `just lint` (clippy + rustfmt + taplo + cargo-deny)

## What NOT to Do

- Do not add dependencies without justification
- Do not refactor code unrelated to your task
- Do not skip `just ci` before committing
- Do not introduce allocations in hot paths without benchmarking
- Do not use `.unwrap()` in production code paths
