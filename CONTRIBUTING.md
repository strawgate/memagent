# Contributing to logfwd

## First 20 Minutes

1. Open `dev-docs/README.md` and choose the task route.
2. Run `just ci` to verify a clean fast baseline.
3. Run the smallest crate-local loop for your change (for example `cargo test -p logfwd-core`).
4. Read only the canonical docs for your change type:
   - config/schema: `dev-docs/CHANGE_MAP.md`
   - scanner/core logic: `dev-docs/SCANNER_CONTRACT.md` + `dev-docs/VERIFICATION.md`
   - runtime/pipeline: `dev-docs/ARCHITECTURE.md`

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

Before pushing to a branch or creating a PR, run the fast CI tier locally and use full CI when your changes touch cross-workspace behavior:

```bash
# Fast tier (default-members, good for most PRs)
just ci

# Full workspace parity (slower, includes datafusion and extended lint/test)
just ci-all

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

See `dev-docs/DESIGN.md` for settled architecture decisions.
Don't reopen a settled question without new evidence.

## Key Commands

Always use `just` recipes instead of bare `cargo` for linting. CI runs
`cargo clippy -- -D warnings` (all warnings are errors), so bare
`cargo clippy` will miss failures that CI catches. The `just` recipes
match CI exactly.

```bash
just ci           # Fast CI gate (default-members)
just ci-all       # Full workspace CI parity
just test         # Default-members tests
just test-all     # Full workspace tests
just lint         # Fast lint gate
just lint-all     # Full lint gate
just clippy       # Clippy with -D warnings (same as CI default tier)
just fmt          # Format code
just bench        # Criterion benchmarks
just kani         # Run Kani proofs (requires: cargo install --locked kani-verifier && cargo kani setup)
cargo test -p logfwd-core  # Fast single-crate iteration
```

## Issues and Labels

### Filing issues

- **One problem per issue.** Don't combine unrelated bugs or features.
- **Title format:** `type(scope): description` — e.g., `bug(tail): glob pattern fails for bare *.log`
- **Body:** describe current behavior, expected behavior, and reproduction steps. For features, describe the user-visible outcome.
- **Link related issues.** Reference parent metas, duplicates, and blocking issues.

### Label taxonomy

Every issue should have exactly one **type label** and one **priority label**. Add **component labels** when applicable.

#### Type labels

| Label | When to use |
|-------|-------------|
| `bug` | Broken behavior — something that worked and doesn't, or violates documented behavior |
| `enhancement` | New feature or improvement to existing functionality |
| `performance` | Performance optimization (throughput, latency, memory, CPU) |
| `architecture` | Systemic design change that eliminates classes of bugs |
| `research` / `research-needed` | Needs investigation before implementation can start |
| `documentation` | Docs-only change (user guide, dev docs, API docs) |
| `work-unit` | Scheduling ticket for one agent run — bundles low-discretion tasks |

#### Priority labels

| Label | Criteria | Response |
|-------|----------|----------|
| `P0` | Data loss, crash, security vulnerability | Fix immediately |
| `P1` | Significant user-visible bug, major UX breakage | Fix this week |
| `P2` | Minor bug, missing validation, metric inaccuracy | Fix when convenient |
| `P3` | Polish, refactor, research, tooling, minor UX | Backlog |

#### Component labels

Use `component:` prefixed labels to identify the affected subsystem. The full list:

- **Inputs:** `component:input/file`, `component:input/tcp`, `component:input/udp`, `component:input/otlp`, `component:input/generator`, `component:input/common`
- **Processing:** `component:processor/framing`, `component:processor/scanner`, `component:processor/transform-sql`, `component:processor/checkpoint`, `component:processor/enrichment`
- **Outputs:** `component:output/http`, `component:output/otlp`, `component:output/elasticsearch`, `component:output/loki`, `component:output/stdout`, `component:output/file`, `component:output/common`
- **Infrastructure:** `component:pipeline`, `component:config`, `component:diagnostics`

#### Special labels

| Label | Purpose |
|-------|---------|
| `production` | Required for production readiness — blocks GA |
| `copilot` | Assigned to GitHub Copilot for automated fix |
| `ci:full` | PR label: run the slow lanes too (macOS, Miri, deeper TLA/TLC sweeps, and any other optional CI jobs gated by the label) |
| `DO NOT MERGE` | PR label: prototype or blocked, must not be merged |
| `benchmark` | Nightly benchmark results (automated) |

### Issue triage

When triaging issues:

1. **Apply labels** — every issue needs type + priority + component(s)
2. **Check for duplicates** — search existing issues by title keywords and component
3. **Link to parent metas** — if the issue is part of a larger initiative, reference the tracking issue
4. **Close resolved issues** — when a PR fixes an issue, verify the fix in the codebase before closing. Reference the fixing PR in the close comment.
5. **Close stale issues** — if the described component was rewritten, the bug no longer exists, or the feature was superseded, close with an explanation

### Work-unit issues

Work-unit issues (`work-unit` label) are scheduling tickets that bundle small, low-discretion tasks for a single agent run. Rules:

- Each work-unit should be completable in one agent session
- List concrete, verifiable tasks (not vague goals)
- Reference specific files and functions where changes are needed
- Don't create duplicate work-units — check existing ones first

## Getting Help

- File an issue at https://github.com/strawgate/fastforward/issues
- Read `dev-docs/README.md` for architecture/constraint entrypoints
- Read `dev-docs/references/README.md` for concise library-specific notes
- Read `dev-docs/research/README.md` for active investigations
