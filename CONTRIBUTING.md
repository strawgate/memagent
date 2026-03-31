# Contributing to logfwd

## Before You Start

1. Read `AGENTS.md` for project context and architecture decisions
2. Read `DEVELOPING.md` for build commands and crate structure
3. Read the source code you plan to modify, including its tests

## Pre-Commit Checklist

Run these before every commit:

```bash
# Format code
just fmt

# Run clippy (zero warnings required)
just clippy

# Run tests
just test

# Or run everything at once:
just ci
```

## Pre-Push Checklist

Before pushing to a branch or creating a PR:

```bash
# Full CI suite (format + clippy + TOML + deny + test)
just ci

# If you changed benchmarked code, verify no regression:
just bench
```

## Pull Request Process

1. **One concern per PR.** Don't mix bug fixes with refactors.
2. **Title format:** `type: concise description` (e.g., `fix: scanner panics on empty input`)
3. **PR body:** explain what changed and why. Include benchmark results if relevant.
4. **Tests required.** Every PR must include tests for the change.
5. **CI must pass.** All checks green before requesting review.
6. **Address all review feedback.** Don't leave threads unresolved.

## Code Style

See `CODE_STYLE.md` for style preferences enforced during review.

## Architecture

See `dev-docs/DECISIONS.md` for settled architecture decisions.
Don't reopen a settled question without new evidence.

## Key Commands

```bash
just ci           # Full CI: lint + test
just test         # Run all tests
just lint         # Format + clippy + TOML + deny
just clippy       # Clippy only
just fmt          # Format code
just bench        # Criterion benchmarks
cargo kani -p logfwd-core  # Run Kani proofs
```

## Getting Help

- File an issue at https://github.com/strawgate/memagent/issues
- Read `docs/references/` for library-specific guides
- Read `dev-docs/research/` for design research
