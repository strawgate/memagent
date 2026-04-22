# Documentation Standards

Rules for keeping docs dense, current, and useful for both humans and agents.

## Canonical Surfaces

| Surface | Path | Audience | Canonical Use |
|---|---|---|---|
| Root docs | `/*.md` | First-time contributors | project orientation and contribution entrypoints |
| User book | `book/src/content/docs/` | Operators/users | install, config, deployment, troubleshooting, conceptual product docs |
| Developer docs | `dev-docs/` | Engineers/agents | architecture, constraints, contracts, verification |
| CI review guides | `dev-docs/review-guides/` | Reviewers | short pass/fail review checklists |
| TLA specs | `tla/` | Formal/spec contributors | temporal properties and model configs |

Do not duplicate canonical facts across surfaces.

## Mandatory Entry Points

- Root entrypoint: `README.md`
- Developer entrypoint: `dev-docs/README.md`
- Research index: `dev-docs/research/README.md` (curated, not exhaustive)
- References index: `dev-docs/references/README.md`
- User-book navigation: `book/astro.config.mjs` (Starlight sidebar config)

If you add or remove docs, update the corresponding index in the same PR when
that surface uses an index as a curated entrypoint. The research index is
intentionally selective and should not become a full file manifest.

## Documentation Categories

The repository uses three documentation categories: living docs, research docs,
and reference notes.

### Living docs (`dev-docs/*.md`, excluding subdirs: `research/`, `references/`, `review-guides/`, `verification/`)

- Represent current behavior and current policy.
- Must be updated in the same PR as behavior/policy changes.
- Must remain concise and task-oriented.

### Research docs (`dev-docs/research/**/*.md`)

Research docs are point-in-time artifacts and are never canonical runtime behavior docs.

Required lifecycle header directly under title:

```markdown
> **Status:** Active | Completed | Historical
> **Date:** YYYY-MM-DD
> **Context:** one-line purpose
```

Allowed status meanings:

- `Active`: unresolved decisions or implementation work remain.
- `Completed`: findings resolved or merged into living docs.
- `Historical`: superseded; retained only for context.

Pruning policy:

- Delete low-value or redundant research docs rather than keeping clutter.
- Prefer one synthesis doc over many per-workstream artifacts.
- Rely on git history for removed artifacts.
- Do not index every audit, prompt, benchmark artifact, or generated file in `research/README.md`.
- Index active investigations and durable syntheses that contributors should actually discover.

### Reference notes (`dev-docs/references/`)

Reference notes are repo-scoped guidance, not generic tutorials.
Depth should be proportional to topic complexity and risk.

- Simple/low-risk topics should stay short and implementation-specific.
- Complex/high-risk topics (for example Kani, TLA+, scanner invariants) should be in-depth.
- Deep references should be layered: quick orientation first, then detailed patterns, failure modes, and review checklists.
- Link upstream docs for general education.
- Keep all sections tied to repository decisions and implementation behavior.

## Root Markdown Allowlist

Only these root markdown files are allowed:

- `AGENTS.md`
- `CLAUDE.md` (symlink to `AGENTS.md` for Claude compatibility)
- `CHANGELOG.md`
- `CONTRIBUTING.md`
- `DEVELOPING.md`
- `README.md`
- Optional governance docs: `SECURITY.md`, `CODE_OF_CONDUCT.md`, `SUPPORT.md`

One-off reports belong in `dev-docs/research/`.

## Filename Conventions

- Root entrypoint and governance docs use conventional uppercase names:
  `README.md`, `CONTRIBUTING.md`, `DEVELOPING.md`, `CHANGELOG.md`, `AGENTS.md`.
- Existing top-level `dev-docs/*.md` living docs also use uppercase names.
  Keep that surface internally consistent unless a dedicated rename pass
  converts the whole layer.
- Subdirectory entrypoints may use `README.md`.
- Research, references, review guides, book docs, and new non-root docs should
  use lowercase kebab-case filenames.
- Avoid dates in filenames for durable design, reference, or policy docs.
- Dates are acceptable only for temporary research artifacts that are expected
  to be pruned rather than maintained long-term.

## Canonical Fact Mapping

| Fact | Canonical doc |
|---|---|
| Config schema and options | `book/src/content/docs/configuration/reference.mdx` |
| SQL transform behavior | `book/src/content/docs/configuration/sql-transforms.md` |
| Pipeline data flow and layer boundaries | `dev-docs/ARCHITECTURE.md` |
| Crate constraints and boundaries | `dev-docs/CRATE_RULES.md` |
| Verification requirements | `dev-docs/VERIFICATION.md` |
| Scanner contract | `dev-docs/SCANNER_CONTRACT.md` |
| Adapter/boundary contract | `dev-docs/ADAPTER_CONTRACT.md` |
| Co-change rules | `dev-docs/CHANGE_MAP.md` |
| PR mechanics | `dev-docs/PR_PROCESS.md` |

## Change Expectations

When behavior changes, docs must change in the same PR:

- User-visible behavior -> update `book/src/content/docs/` docs.
- Contributor-visible constraints -> update `dev-docs/` docs.
- Review criteria changes -> update `dev-docs/review-guides/` checklist docs.
- Architecture/invariant changes -> update `ARCHITECTURE.md`, `DESIGN.md`, and/or `VERIFICATION.md`.

Do not add contributor workflow pages under `book/src/content/docs/`.
Link to repo-local contributor docs instead.

Use `dev-docs/CHANGE_MAP.md` before coding.

## CI Validation

Docs CI must run:

- `scripts/docs/validate_operational_sections.py`
- `scripts/docs/validate_research_metadata.py`
- `scripts/docs/validate_root_doc_allowlist.py`
- `scripts/docs/report_stale_docs.py`

If a docs governance rule is added, add a validator in the same PR.
