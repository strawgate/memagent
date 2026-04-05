# PR Process: Issue → PR → Review → Merge

This document describes the mechanics of our issue-to-merge workflow, covering both Copilot-generated and human-authored PRs.

## Overview

```
Issue filed → Assign to Copilot → Copilot creates draft PR → Mark ready →
  Review (architecture + code quality) → Fix lint/bugs → Merge or Close
```

## Pre-PR Checklist (Required)

Before opening a PR, confirm all items:

- [ ] The PR description states exactly what behavior changed.
- [ ] If behavior changed, docs changed in the same PR (user docs in `book/src/`, contributor docs in `dev-docs/` as needed).
- [ ] If config semantics changed, `book/src/config/reference.md` was updated.
- [ ] If pipeline/architecture behavior changed, update `dev-docs/ARCHITECTURE.md` and/or `dev-docs/DESIGN.md`.
- [ ] If invariants/proofs changed, update `dev-docs/VERIFICATION.md` and related harnesses.
- [ ] Commands in docs were copy/paste verified in the target environment.

Use [CHANGE_MAP](CHANGE_MAP.md) to identify all required companion updates.

## 1. Filing Issues for Copilot

Write focused, well-scoped issues. Include:
- What to change (1-2 sentences)
- Which files are involved
- Any constraints or patterns to follow
- Reference to closed PRs if this replaces stale work

## 2. Assigning Issues to Copilot

Use the GraphQL API with model and custom agent selection:

```bash
# Required: get the issue's GraphQL ID
gh api graphql -f query='
{
  repository(owner: "strawgate", name: "memagent") {
    issue(number: ISSUE_NUMBER) { id }
  }
}'

# Assign with model + custom agent
gh api graphql \
  -H "GraphQL-Features: issues_copilot_assignment_api_support" \
  -H "GraphQL-Features: coding_agent_model_selection" \
  -f query='
mutation {
  updateIssue(input: {
    id: "ISSUE_GRAPHQL_ID"
    assigneeIds: ["BOT_kgDOC9w8XQ"]
    agentAssignment: {
      targetRepositoryId: "R_kgDORzg8fA"
      customAgent: "issue-worker"
      model: "claude-opus-4.6"
    }
  }) {
    issue { title assignees(first: 5) { nodes { login } } }
  }
}'
```

**Key IDs (this repo):**
- Copilot bot ID: `BOT_kgDOC9w8XQ`
- Repository ID: `R_kgDORzg8fA`
- Custom agent: `issue-worker` (defined in `.github/agents/issue-worker.agent.md`)
- Model: `claude-opus-4.6` (or `gpt-4o`, `claude-sonnet-4.6`, etc.)

**Simple REST fallback** (no model/agent selection):
```bash
gh api --method PATCH "/repos/strawgate/memagent/issues/ISSUE_NUMBER" \
  -f 'assignees[]=copilot-swe-agent[bot]'
```

## 3. PR Triage Loop

When Copilot PRs arrive, process them in batches:

### a. Inventory
```bash
# List all open Copilot PRs
gh pr list --author "app/copilot-swe-agent" --state open \
  --json number,title,isDraft,mergeable \
  --jq '.[] | "#\(.number) draft=\(.isDraft) mergeable=\(.mergeable) \(.title)"'
```

### b. Skip WIP PRs
PRs with `[WIP]` in the title are still being worked on by Copilot. Leave them as drafts.

### c. Mark non-WIP drafts as ready
```bash
gh pr ready ISSUE_NUMBER
```

### d. Update branches with master
```bash
gh api repos/{owner}/{repo}/pulls/ISSUE_NUMBER/update-branch \
  -X PUT -f update_method=merge
```

### e. Check CI
```bash
gh pr checks ISSUE_NUMBER
```

## 4. Review Criteria

Every PR gets reviewed for:

1. **What it changes** — 1-2 sentence summary
2. **Size/scope** — files changed, lines added/removed
3. **Risk** — safe and isolated vs touches core architecture
4. **Code quality** — bugs, missing error handling, questionable patterns
5. **Verdict** — one of:
   - **Safe to merge** — fix lint if needed, then merge
   - **Needs minor fixes** — specific actionable items, then merge
   - **Needs architectural review** — design questions to resolve first
   - **Close** — superseded, too stale, or fundamentally flawed

### Common Copilot issues to watch for
- **Lint failures** — Copilot doesn't run `cargo fmt` or `cargo clippy`. Every PR needs lint fixes.
- **Approx constant test values** — using `3.14` (too close to PI) triggers `clippy::approx_constant`
- **API drift** — Copilot branches from old commits, uses removed methods (e.g., `execute` → `execute_blocking`)
- **Dead config** — adding config fields that are parsed but never used at runtime
- **Formatting noise** — 50%+ of diff is `rustfmt` changes to unrelated files

## 5. Fixing PRs

Fix lint and minor issues directly on the PR branch using worktree agents:

```bash
# Launch a worktree agent to fix lint on a PR
# (from Claude Code — the agent checks out the branch, fixes clippy/fmt, runs tests, pushes)
```

For conflict resolution:
```bash
# Merge master into the PR branch
cd worktree-path
git fetch origin master
git merge origin/master
# Resolve conflicts (usually approx_constant values — accept master's version)
git checkout --theirs conflicted-file.rs
git add conflicted-file.rs
git commit --no-edit
git push origin HEAD:branch-name
```

## 6. Resolving Review Comments

After fixing review feedback, resolve the addressed threads via GraphQL (the REST API doesn't support review threads):

```bash
# List all unresolved review threads on a PR
gh api graphql -f query='
{
  repository(owner: "strawgate", name: "memagent") {
    pullRequest(number: PR_NUMBER) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          comments(first: 1) {
            nodes {
              body
              author { login }
            }
          }
        }
      }
    }
  }
}'

# Resolve a specific thread by ID
gh api graphql -f query='
mutation {
  resolveReviewThread(input: {
    threadId: "THREAD_NODE_ID"
  }) {
    thread { isResolved }
  }
}'
```

**When to resolve:**
- The fix is pushed and addresses the reviewer's exact concern
- The thread is outdated (code was refactored/removed)
- The comment was informational (no action needed)

**When NOT to resolve:**
- The fix is partial or a workaround — leave open for the reviewer
- The comment raises an architectural question that needs discussion

**Bulk resolve** — after pushing fixes, resolve all clearly-addressed threads in one pass:

```bash
# Get all unresolved thread IDs
THREADS=$(gh api graphql -f query='
{
  repository(owner: "strawgate", name: "memagent") {
    pullRequest(number: PR_NUMBER) {
      reviewThreads(first: 100) {
        nodes { id isResolved }
      }
    }
  }
}' --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false) | .id')

# Review what will be resolved
echo "Unresolved threads:"
echo "$THREADS"

# Resolve each one
for tid in $THREADS; do
  gh api graphql -f query="mutation { resolveReviewThread(input: { threadId: \"$tid\" }) { thread { isResolved } } }"
done
```

## 7. Merging

```bash
# Squash merge (preferred for Copilot PRs)
gh pr merge ISSUE_NUMBER --squash

# If branch protection requires CI:
gh pr merge ISSUE_NUMBER --squash --auto
```

## 8. Closing Stale PRs

When PRs are superseded or too stale to salvage:
```bash
gh pr close ISSUE_NUMBER --comment "Closing — [reason]. [what to do instead]."
```

If the underlying work is still needed, file a focused issue and assign to Copilot.

## 9. Stale PR Triage

Periodically check if WIP PRs are still relevant:

1. List all WIP PRs
2. For each, check if the feature already exists on master (search git log, grep source)
3. **Fully superseded** → close with explanation
4. **Partially superseded** → close, file focused issue for remaining work
5. **Still needed** → leave open or close and refile with updated scope

## 10. Master CI Health

After merging batches, verify master CI is green:
```bash
gh run list --branch master --workflow CI --limit 1 \
  --json conclusion -q '.[0].conclusion'
```

Common fixes:
- **Lint failure** → `cargo fmt --all` on master
- **ARM64 cross-compile** → `apt-get update` before package install
- **Flaky tests** → investigate timing-dependent assertions
