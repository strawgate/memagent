# Completed Research Rollup (2026-04)

> **Status:** Completed
> **Date:** 2026-04-10
> **Context:** Consolidated summary of completed April 2026 research work to reduce research-doc sprawl.

This rollup replaces multiple standalone completed artifacts.

## Included Sources

- `documentation-experience-strategy-2026-04-05.md`
- `dependency-structure-audit-2026-04-08.md`

## Outcome: Documentation Experience Strategy (2026-04-05)

### Problem

Documentation surfaces were discoverable but still noisy and duplicative for contributors and agents.

### Completed outcomes

- Added explicit docs governance and canonical surface mapping.
- Added task-routing entrypoints for developer docs.
- Added CI-backed docs validation for operational sections, research metadata, and root-doc allowlist.
- Reduced low-value research/reference sprawl and moved toward concise repo-scoped references.

### Canonical docs that now carry this policy

- `dev-docs/DOCS_STANDARDS.md`
- `dev-docs/README.md`
- `dev-docs/research/README.md`
- `dev-docs/references/README.md`
- `.github/workflows/docs.yml`
- `scripts/docs/validate_operational_sections.py`
- `scripts/docs/validate_research_metadata.py`
- `scripts/docs/validate_root_doc_allowlist.py`

## Outcome: Dependency and Structure Audit (2026-04-08)

### Problem

Workspace dependency and documentation/tooling semantics required drift checks.

### Completed outcomes

- Yanked lockfile issue (`fastrand`) was addressed.
- Docs semantics were aligned with actual `just` command behavior.
- Boundary guidance drift was corrected.

### Follow-up policy retained

- Continue CI enforcement for dependency safety (`cargo deny check`).
- Keep docs/manifests synchronized in the same PR when semantics change.
- Keep dependency policy linting as an incremental improvement area.

### Canonical docs for ongoing enforcement

- `dev-docs/CRATE_RULES.md`
- `dev-docs/CHANGE_MAP.md`
- `dev-docs/PR_PROCESS.md`
- `dev-docs/review-guides/crate-boundary-and-dependency-integrity.md`
- `dev-docs/review-guides/documentation-thoroughly-updated.md`

## Notes

Detailed historical text for superseded standalone docs remains in git history.
