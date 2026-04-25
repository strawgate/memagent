# Copilot Instructions for ffwd

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
just fmt                                           # format all Rust code
just clippy                                        # lint changed crates — zero warnings
just test                                          # test changed crates
```

Or run the full CI gate (slower but comprehensive):

```bash
just ci                                            # fmt + clippy + test + deny
```

If `just ci` takes too long, at minimum run `just fmt && just clippy` before every commit.

## PR Descriptions

- Use `Closes #N` for every issue the PR fully resolves (auto-closes on merge)
- Use `Relates to #N` for partial fixes
- Fill in the PR template — summary, closing keywords, test plan

## What NOT to Do

- Do not add dependencies without justification
- Do not refactor code unrelated to your task
- Do not introduce allocations in hot paths without benchmarking
- Do not use `.unwrap()` in production code paths
