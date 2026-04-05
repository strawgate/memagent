# Amazing Docs Plan (Open Source, No Fluff)

> **Status:** Active
> **Date:** 2026-04-05
> **Context:** Replace abstract doc strategy language with a concrete execution plan that can be completed in normal open-source contributor time.

## Blunt answer

If this plan is only discussion, it is consultant nonsense.
If this plan becomes a prioritized backlog with owners, templates, CI checks, and a weekly ship cadence, it is real.
This document is the second kind.

## What “amazing docs” means for this project

Amazing docs are not “comprehensive.”
Amazing docs help users succeed quickly and help contributors change code safely.
For this repo, that means:

1. New user can run the project quickly.
2. Operator can deploy and troubleshoot without asking maintainers.
3. Contributor can find architecture constraints before opening a PR.
4. Every frequent support question has a canonical page.
5. Docs stay current through CI and ownership, not hero effort.

## Scope boundaries (so we do not boil the ocean)

We only produce and maintain these documentation surfaces:

- `README.md` and root onboarding docs for first contact.
- `book/src/` for user and operator documentation.
- `dev-docs/` for contributor architecture and engineering standards.
- `docs/ci/` for review checklists.

We explicitly avoid duplicating the same facts across surfaces.

## Execution status

### Completed in this PR

- Added a start-here navigation block to `book/src/SUMMARY.md` for faster path selection.
- Added `book/src/getting-started/which-guide.md` as a goal-based chooser page.
- Updated `README.md` documentation section to route users by goal before deep reference links.
- Reworked `book/src/troubleshooting.md` into a symptom-first triage guide with concrete checks and expected outcomes.
- Added deployment safe defaults, validation, and rollback procedures in `book/src/deployment/kubernetes.md` and `book/src/deployment/docker.md`.
- Added `dev-docs/CHANGE_MAP.md` to show companion updates required for config, pipeline, verification, and crate-boundary edits.
- Added a required pre-PR checklist to `dev-docs/PR_PROCESS.md` and concrete good/bad boundary examples in `dev-docs/CRATE_RULES.md`.
- Added CI automation for markdown link checking, operational-doc required-section validation, and stale operational-doc reporting.

### Manual verification log (2026-04-05)

- Ran `python3 scripts/docs/validate_operational_sections.py` successfully.
- Ran `python3 scripts/docs/report_stale_docs.py` successfully.
- Ran `python3 -m py_compile` for both docs scripts successfully.
- Installed `mdbook` via `cargo install --locked mdbook` and successfully ran `mdbook build book`.
- Attempted local `lychee` binary install, but did not complete within this iteration; link checks remain enforced in CI workflow via `lycheeverse/lychee-action`.

### Next execution target

- Week 1 through Week 4 baseline is now implemented.
- Next iteration should focus on measuring impact and refining weak pages based on issue/support signal.

## 30-day execution plan

### Week 1: Fix discoverability and first-run experience

**Deliverables:**

- Rewrite top section of `README.md` around “Install → Run → Verify output” in under 10 minutes.
- Ensure quick-start links to exactly one canonical config example.
- Add a “Start here” block at the top of `book/src/SUMMARY.md`.
- Add a short “Which doc should I read?” chooser page in the user book.

**Definition of done:**

- A new contributor can run the quick-start from a clean environment and copy/paste commands without guessing.
- Two maintainers verify steps manually.

### Week 2: Ship high-impact operational docs

**Deliverables:**

- Create a troubleshooting page organized by symptom, not subsystem.
- Add a deployment “safe defaults” page and “production hardening” checklist.
- Add a rollback section to deployment and upgrade pages.
- Add 5 common failure scenarios with diagnosis commands and expected outputs.

**Definition of done:**

- Existing open issues/support questions map to at least one troubleshooting entry.
- On-call maintainer approves that rollback instructions are operationally safe.

### Week 3: Contributor docs and architecture clarity

**Deliverables:**

- Add a “change map” page in `dev-docs/`:
  - where config changes go,
  - where pipeline behavior changes go,
  - where verification updates are required.
- Add “before opening a PR” checklist with links to required docs and commands.
- Tighten crate-boundary guidance with concrete bad/good examples.

**Definition of done:**

- A contributor can answer “what files must change for feature X?” from docs alone.
- Reviewers report fewer “missing context” comments.

### Week 4: Automation and anti-drift

**Deliverables:**

- Add CI link checker for markdown.
- Add docs lint for required sections on operational pages:
  - prerequisites,
  - validation steps,
  - rollback.
- Add snippet verification for shell commands used in quick-start examples (where feasible).
- Add stale-page report (last reviewed > 90 days) for operational pages.

**Definition of done:**

- CI fails on broken links.
- CI flags missing critical sections.
- Maintainers get a generated stale-doc report.

## Priority backlog (ordered)

Do these in order.
Do not start lower-priority work until higher-priority items are merged.

1. README first-run rewrite.
2. Troubleshooting by symptom.
3. Deployment safe defaults + rollback guidance.
4. Book navigation improvements.
5. Contributor change map.
6. CI link checking.
7. CI required-section linting.
8. Snippet verification.

## What we automate vs what we write manually

### Automate now

- Internal link validation.
- Presence checks for required sections in operational pages.
- Snippet validation for core quick-start commands.
- Stale-doc age reporting.

### Manual by maintainers

- Troubleshooting diagnosis trees.
- Architecture explanations and tradeoffs.
- Upgrade and rollback runbooks.
- Decision rationale and design constraints.

### Rule of thumb

If content requires judgment, keep it human-authored.
If content is repetitive and structurally testable, automate it.

## Templates to standardize quality

### Template: Task page (operator-facing)

1. What this page helps you do.
2. Prerequisites.
3. Exact commands.
4. Expected output.
5. Failure modes.
6. Rollback / safe exit.
7. Related docs.

### Template: Reference page

1. Canonical schema or option list.
2. Defaults and allowed values.
3. Version notes.
4. Examples.
5. Links to task pages.

### Template: Contributor page

1. Problem area and scope.
2. Invariants and constraints.
3. Files/modules to edit.
4. Required tests/verification.
5. Common mistakes.

## Minimal metrics (only what helps decisions)

Track just these four metrics monthly:

1. Quick-start success rate (maintainer smoke test pass rate).
2. Docs-linked issue resolution rate (issues closed via docs updates).
3. Broken links on main (must stay zero).
4. Pages not reviewed for 90+ days (must trend downward).

If a metric does not drive action, remove it.

## Ownership and cadence

- One docs maintainer on weekly rotation owns triage and backlog ordering.
- Every merged feature PR must include either:
  - docs update, or
  - explicit “no doc impact” justification in PR body.
- Spend the first 30 minutes of weekly maintainer sync on docs backlog and stale pages.

## Anti-patterns we will reject in review

- Huge conceptual docs with no runnable steps.
- Duplicate reference data in multiple files.
- Task guides without expected output.
- Deployment docs without rollback procedure.
- PRs changing behavior but deferring doc updates.

## Immediate next 5 PRs to open

1. `docs(readme): rewrite quick-start for 10-minute first run`
2. `docs(book): add start-here chooser and improve navigation`
3. `docs(troubleshooting): add symptom-first triage and fixes`
4. `docs(deploy): add safe defaults, validation, and rollback`
5. `ci(docs): enforce links and required operational sections`

## Reality check

Open source wins with consistency, not a giant one-time rewrite.
If we ship one meaningful docs PR each week for 8–12 weeks, quality will compound fast.
This is a practical plan only if we treat docs work as normal product work with backlog priority and merge discipline.
