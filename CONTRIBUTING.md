# Contributing to logfwd

## Before You Start

1. Read `AGENTS.md` for project context and architecture decisions
2. Read `DEVELOPING.md` for build commands and crate structure
3. Read the source code you plan to modify, including its tests

## Pre-Commit Checklist

We use [pre-commit](https://pre-commit.com/) to automatically run fast checks (formatting and linting) before every commit. 

To set it up:
1. Install pre-commit: `pip install pre-commit` (or your preferred manager)
2. Install the hooks: `pre-commit install`

If you prefer manual checks, run these before every commit:

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

Before pushing to a branch or creating a PR, you **MUST** ensure the full CI suite passes locally:

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

See `dev-docs/CODE_STYLE.md` for style preferences enforced during review.

## Architecture

See `dev-docs/DECISIONS.md` for settled architecture decisions.
Don't reopen a settled question without new evidence.

## Key Commands

Always use `just` recipes instead of bare `cargo` for linting. CI runs
`cargo clippy -- -D warnings` (all warnings are errors), so bare
`cargo clippy` will miss failures that CI catches. The `just` recipes
match CI exactly.

```bash
just ci           # Full CI: lint + test — run this before pushing
just test         # Run all tests
just lint         # Format + clippy + TOML + deny
just clippy       # Clippy with -D warnings (same as CI)
just fmt          # Format code
just bench        # Criterion benchmarks
cargo kani -p logfwd-core  # Run Kani proofs (no just recipe)
cargo test -p logfwd-core  # Fast single-crate iteration
```

## Getting Help

- File an issue at https://github.com/strawgate/memagent/issues
- Read `dev-docs/references/` for library-specific guides
- Read `dev-docs/research/` for design research
