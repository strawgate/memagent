# PR Process

Repo-local mechanics for taking a change from issue to merged PR.

This document is intentionally about durable workflow, not bot-specific IDs,
model names, or one-off automation details that drift quickly.

## Overview

```text
Issue scoped -> branch or PR opened -> implementation + docs + verification ->
review -> follow-up fixes -> merge or close
```

## Pre-PR Checklist

Before opening or marking a PR ready, confirm all of these:

- The PR description states exactly what behavior changed and why.
- Docs changed in the same PR when behavior or policy changed.
- Config changes updated `book/src/content/docs/configuration/reference.mdx`.
- Architecture changes updated `dev-docs/ARCHITECTURE.md` and/or `dev-docs/DESIGN.md`.
- Invariant or proof changes updated `dev-docs/VERIFICATION.md` and related evidence.
- Commands in the PR body and docs were actually run in the target environment.

Use [CHANGE_MAP.md](CHANGE_MAP.md) before coding when the change crosses boundaries.

## 1. Scope The Work First

Every issue or PR should have one clear concern:

- one bug
- one feature slice
- one refactor with one goal
- one docs or verification cleanup

If a branch has mixed concerns, split it before review.

Good issue scope includes:

- concrete behavior to change
- touched files or modules
- invariants or contracts that must stay true
- links to parent metas or superseded work

## 2. Open The PR Early

Prefer an early draft PR over a large surprise diff.

Required conventions:

- PR title: `type: concise description`
- one concern per PR
- include validation commands in the body
- include benchmark deltas when claiming performance changes

Useful commands:

```bash
gh pr create --draft
gh pr view PR_NUMBER
gh pr ready PR_NUMBER
```

## 3. Keep The Branch Healthy

Before requesting review:

- run the smallest relevant local loop during development
- run `just ci` at minimum before pushing
- run `just ci-all` when the change touches cross-workspace behavior
- keep the branch current with `main` if CI or review context drifted

Useful commands:

```bash
just ci
just ci-all
gh pr checks PR_NUMBER
gh pr update-branch PR_NUMBER
```

If the CLI branch update is unavailable or branch protection requires it, merge
or rebase `origin/main` manually using normal non-interactive git commands.

## 4. Review Expectations

Every PR is reviewed for:

1. Behavior: what changed, and is it correct?
2. Scope: is the diff focused and appropriately sized?
3. Risk: does it touch core contracts, runtime invariants, or hot paths?
4. Validation: were the right tests, proofs, and docs updated?
5. Merge fitness: would we be comfortable owning this on `main` tomorrow?

Use the checklist docs in [`review-guides/`](review-guides/) during review.

## 5. AI-Agent PRs

Agent-authored PRs are held to the same bar as human-authored PRs.

Common failure modes:

- stale base branch
- docs not updated
- lint or test failures
- dead config or partially wired behavior
- plausible-looking code that violates crate boundaries or invariants

If a PR is easier to replace than salvage:

- close it
- file or refine a focused issue
- restart from current `main`

Do not merge weak code just because the diff is small or mostly generated.

## 6. Address Review Feedback Deliberately

When fixing review feedback:

- push the exact fix for the comment
- say what changed when the fix is non-obvious
- leave architectural questions open until resolved
- resolve threads only when the concern is actually addressed

Useful commands:

```bash
gh pr view PR_NUMBER --comments
gh pr comment PR_NUMBER --body "Addressed in <commit-or-summary>."
gh pr review PR_NUMBER --approve
gh pr review PR_NUMBER --request-changes --body "..."
```

If thread-resolution state matters and the regular CLI is insufficient, use
`gh api graphql` against the current repository. Keep those commands local to
the task at hand instead of hard-coding them into this doc.

## 7. Merge Or Close

Default merge mode is squash unless there is a strong reason not to.

```bash
gh pr merge PR_NUMBER --squash
gh pr merge PR_NUMBER --squash --auto
gh pr close PR_NUMBER --comment "Closing — superseded or stale; follow-up tracked in #NNNN."
```

Close a PR when it is:

- fully superseded
- based on outdated assumptions
- too broad to review safely
- missing enough context that a focused restart is cheaper

## 8. After Merge

After merging a batch or risky change:

- confirm the latest `main` CI run is healthy
- check whether follow-up issues or docs cleanup are still needed
- close or update linked issues accordingly

Useful commands:

```bash
gh run list --branch main --workflow CI --limit 1
gh issue list --state open
gh issue list --label work-unit --state open
```

## Review Bar

The default question is not "can this merge?"

It is "is this the shape we want to maintain?"

If the answer is no, either fix it or narrow the scope until the answer is yes.
