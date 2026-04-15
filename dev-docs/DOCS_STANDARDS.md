# Documentation Standards

Rules for what documentation exists, where it lives, and how to keep it current.

---

## Doc surfaces

logfwd has five documentation surfaces. Each has a single purpose. Content belongs
in exactly one surface — if you find the same information in two places, one is
wrong and should be deleted.

| Surface | Path | Audience | Purpose |
|---------|------|----------|---------|
| **GitHub root** | `/*.md` | Contributors hitting the repo for the first time | Orientation: what this is, how to build it, how to contribute |
| **User book** | `book/src/` | Operators deploying and configuring logfwd | Config reference, deployment guides, troubleshooting, architecture overview |
| **Dev docs** | `dev-docs/` | Engineers modifying the codebase | Architecture decisions, code style, crate rules, verification, scanner contract |
| **Review guides** | `docs/ci/` | Code reviewers (human and AI) | PR review checklists, one per review dimension |
| **Spec docs** | `tla/` | Engineers modifying formal specs | TLA+ specs, TLC configs, running instructions |

### What goes where

**GitHub root files** follow convention — GitHub and tooling look for these names:

| File | Content |
|------|---------|
| `README.md` | What logfwd is, quick start, feature overview, links to book |
| `DEVELOPING.md` | Build/test/bench commands, workspace layout, hard-won lessons |
| `CONTRIBUTING.md` | PR workflow, commit conventions, where to read before contributing |
| `CHANGELOG.md` | Release notes in Keep a Changelog format |
| `AGENTS.md` / `CLAUDE.md` | Agent entry point: start-here list, reference doc table, issue labels |

No other `.md` files should live at the repo root. Completed audits, reviews, and
one-off reports belong in `dev-docs/research/`.

**User book** (`book/src/`) is the canonical user-facing documentation. It is built
with mdbook and published. If a user needs to know it to deploy or operate logfwd,
it goes here. Do NOT maintain parallel user docs in `docs/` — that path is reserved
for CI review guides.

**Dev docs** (`dev-docs/`) are for engineers working on the code. They describe
_why_ the code is shaped a certain way, not _how to use_ the product. Subcategories:

| Subdirectory | Content |
|-------------|---------|
| `dev-docs/` (root) | Living standards and specs: ARCHITECTURE, DESIGN, CODE_STYLE, CRATE_RULES, VERIFICATION, SCANNER_CONTRACT |
| `dev-docs/references/` | Pinned API references for specific library versions (Arrow, DataFusion, etc.) |
| `dev-docs/research/` | Research artifacts with lifecycle headers (see below) |
| `tla/` | TLA+ specs (PipelineMachine, ShutdownProtocol, PipelineBatch) |

**Review guides** (`docs/ci/`) are self-contained review checklists loaded by CI
review tooling. Each guide covers one review dimension (Rust practices, verification
coverage, crate boundaries, documentation, maintainer fitness). These reference
dev-docs by path but do not duplicate them.

---

## Document lifecycle

Every document has a lifecycle stage. Living documents and research artifacts track
this differently.

### Living documents (dev-docs root)

These are always current. If the code changes, the doc changes in the same PR.
They do NOT need lifecycle headers — being in `dev-docs/` root means they're living.

Examples: ARCHITECTURE.md, DESIGN.md, VERIFICATION.md

**Update trigger:** Any PR that changes behavior described in a living doc MUST
update that doc. The `docs/ci/documentation-thoroughly-updated.md` review guide
enforces this.

### Research artifacts (dev-docs/research/)

Research docs are point-in-time artifacts. They capture investigation results,
design explorations, audits, and benchmarks. Every research doc MUST have a
status header as the first content after the title:

```markdown
# Title

> **Status:** Historical | Active | Completed
> **Date:** YYYY-MM-DD
> **Context:** One line explaining why this was written
```

| Status | Meaning | Action |
|--------|---------|--------|
| **Historical** | Superseded by implementation or later research. Kept for context. | Read for background only. Do not act on recommendations. |
| **Active** | Contains open findings or gaps that drive current/future work. | Check findings before closing. |
| **Completed** | Audit or investigation that finished with all actions resolved. | Reference only. |

When a research doc's findings are fully addressed, update its status to
Completed or Historical. Never delete research docs — they provide decision
context that git log cannot.

### Reference docs (dev-docs/references/)

Pinned to a specific library version. The filename includes a version note inside
the document. When upgrading a dependency:

1. Update the existing reference doc with new version info
2. Keep the old content if it's still relevant (migration context)
3. Update `AGENTS.md` if the filename changed

Reference docs that cover unversioned patterns (e.g., `tokio-async-patterns.md`)
are living documents — update them when patterns change.
They do not carry version numbers in filenames but should note the library
versions they were last verified against.

---

## Structure rules

### Required sections by doc type

**Living dev-docs** (ARCHITECTURE, DESIGN, etc.):
- Title (`# Name`) — what this document covers
- Body — organized by topic, not chronologically
- No date headers (these are always-current documents)

**Research docs**:
- Title
- Status header (see above)
- Body — findings, analysis, recommendations
- Optional: Summary table, action items

**User book pages**:
- Title
- Brief intro (1-2 sentences)
- Content with examples
- No internal jargon — write for an operator who has never seen the codebase

**Review guides** (`docs/ci/`):
- Title with "Full Review Guidance" suffix
- Role statement ("You are reviewing...")
- Checklist items with pass/fail criteria
- References to canonical docs (by path)

### Formatting

- Use ATX headers (`#`, not underlines)
- One sentence per line in source (makes diffs readable)
- Code blocks with language tags (` ```rust `, ` ```yaml `, ` ```bash `)
- Tables for structured comparisons
- No emoji unless the user explicitly requests them
- Relative links between docs (`[ARCHITECTURE](ARCHITECTURE.md)`, not absolute paths)

### Cross-referencing

- Always link to the canonical location. Never say "see the config docs" — say
  `[Configuration Reference](../book/src/config/reference.md)` or
  `[ARCHITECTURE](dev-docs/ARCHITECTURE.md)`.
- When a dev-doc references a user-book page, link to the book path.
- When a user-book page references internals, DON'T — user docs should be
  self-contained. Link to other book pages instead.

---

## Anti-drift rules

Documentation drift is the #1 doc quality problem. These rules prevent it:

### 1. Same-PR updates

Any PR that changes behavior documented in a living doc MUST update that doc
in the same PR. "I'll update the docs later" is not acceptable — it never
happens. The CI review guide `documentation-thoroughly-updated.md` enforces
this during review.

### 2. Single source of truth

Every fact has exactly one canonical location. Other documents may _link_ to
it but must not _restate_ it. If you find the same information in two places,
delete one and replace it with a link.

**Canonical locations:**

| Fact | Canonical doc |
|------|---------------|
| Config YAML fields | `book/src/config/reference.md` |
| Column naming rules | `book/src/config/sql-transforms.md` |
| Deployment manifests | `book/src/deployment/` |
| Troubleshooting | `book/src/troubleshooting.md` |
| Crate boundaries and deps | `dev-docs/CRATE_RULES.md` |
| Architecture and data flow | `dev-docs/ARCHITECTURE.md` |
| Design decisions | `dev-docs/DESIGN.md` |
| Verification strategy | `dev-docs/VERIFICATION.md` |
| Scanner input/output contract | `dev-docs/SCANNER_CONTRACT.md` |
| Build/test/bench commands | `DEVELOPING.md` |
| PR workflow | `dev-docs/PR_PROCESS.md` |

### 3. No orphan docs

Every doc must be reachable from at least one index:
- Root `.md` files → linked from `README.md` or `AGENTS.md`
- `book/src/` pages → listed in `book/src/SUMMARY.md`
- `dev-docs/` files → listed in `AGENTS.md` reference table
- `dev-docs/research/` files → discoverable by convention (status headers)
- `docs/ci/` guides → referenced by CI tooling configuration

### 4. Stale content markers

If you discover stale content but can't fix it now, add a visible warning:

```markdown
> **Stale:** This section describes the old approach.
> See `book/src/config/sql-transforms.md` for the current behavior.
```

This is better than leaving wrong information unmarked. But fix it soon — stale
markers that linger become invisible.

---

## When NOT to write documentation

- **Don't document what the code says.** If the function signature and name are
  clear, a doc comment restating them adds noise. Document _why_, not _what_.
- **Don't write a doc for a single PR.** PR descriptions cover this. Research
  docs are for investigations that span multiple PRs or inform future work.
- **Don't duplicate git log.** "What changed and when" is in the commit history
  and CHANGELOG. Dev-docs capture _decisions_, not _events_.
- **Don't write tutorials in dev-docs.** Tutorials are user docs and belong in
  `book/src/`. Dev-docs assume the reader is already a contributor.
