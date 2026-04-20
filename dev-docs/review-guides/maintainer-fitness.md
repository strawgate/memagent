# Maintainer Fitness

Maintainer-level triage checklist for merge readiness.

## Scope and Focus

- One logical concern per PR.
- Large diffs must have clear justification.
- Refactors are acceptable only when directly enabling the change.

## Roadmap Fit

- Change aligns with current roadmap priorities.
- Deferred/future-scope work is not being pulled in accidentally.

## Risk

- Hot-path changes include performance evidence.
- Behavior-changing changes include rollback/impact clarity.
- Architectural changes include corresponding docs updates.

## Merge Readiness Signals

- Tests and required checks pass.
- Review threads are resolved.
- Docs are updated where behavior changed.
- Risks and tradeoffs are explicit in the PR body.

## Canonical References

- `dev-docs/PR_PROCESS.md`
- `dev-docs/CHANGE_MAP.md`
- Roadmap: https://github.com/strawgate/fastforward/issues/889
