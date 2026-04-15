# Documentation Thoroughly Updated

Use this checklist to ensure docs stay synchronized with behavior.

## Hard Rule

If user-visible or contributor-visible behavior changed, docs must change in the same PR.

## What Must Be Updated

- Config schema/semantics: `book/src/content/docs/configuration/reference.mdx`
- SQL/UDF behavior: `book/src/content/docs/configuration/sql-transforms.md`
- Pipeline architecture/flow: `dev-docs/ARCHITECTURE.md`
- Verification requirements/status: `dev-docs/VERIFICATION.md`
- Crate responsibilities: `dev-docs/CRATE_RULES.md`
- Contributor workflow/commands: `DEVELOPING.md`, `CONTRIBUTING.md` when relevant

## Reviewer Questions

- Is there any behavior change not reflected in canonical docs?
- Are examples still correct after this change?
- Is there duplicate guidance that now disagrees with canonical docs?

## Reject / Warn Criteria

- Reject: behavior changed and canonical docs were not updated.
- Warn: docs changed but links, commands, or examples are stale/inconsistent.

## Required Companion References

- `dev-docs/CHANGE_MAP.md`
- `dev-docs/DOCS_STANDARDS.md`
