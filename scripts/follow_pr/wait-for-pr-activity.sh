#!/usr/bin/env bash
set -euo pipefail

## Follow-PR watcher (minimal Bash implementation)
## Usage: wait-for-pr-activity.sh OWNER/REPO PR_NUMBER [--interval SECONDS]
##        or: wait-for-pr-activity.sh https://github.com/OWNER/REPO/pull/PR_NUMBER [--interval SECONDS]

OWNER_REPO=""
PR_NUMBER=""
INTERVAL=${2:-300}

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 OWNER/REPO PR_NUMBER [--interval SECONDS]" >&2
  exit 64
fi

INPUT1="$1"
SHIFTED=0
if [[ "$INPUT1" == http*://*github.com* ]]; then
  # URL form: https://github.com/OWNER/REPO/pull/PR
  if [[ "$INPUT1" =~ github.com/([^/]+)/([^/]+)/pull/([0-9]+) ]]; then
    OWNER_REPO="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    PR_NUMBER="${BASH_REMATCH[3]}"
  else
    echo "Could not parse PR URL: $INPUT1" >&2
    exit 64
  fi
  SHIFTED=2
elif [[ "$INPUT1" =~ ^([^/]+)/([^/]+)$ ]]; then
  OWNER_REPO="$BASH_REMATCH[1]/${BASH_REMATCH[2]}"
  if [[ $# -ge 2 ]]; then
    PR_NUMBER="$2"
    SHIFTED=1
  else
    echo "Missing PR_NUMBER" >&2
    exit 64
  fi
else
  echo "Invalid input. Provide OWNER/REPO or a PR URL." >&2
  exit 64
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval)
      INTERVAL="$2"; shift 2;;
    *)
      echo "Unknown argument: $1" >&2; exit 64;;
  esac
done

BRANCH_STATE_DIR="/tmp/follow-pr/${OWNER_REPO//\//__}/pr-${PR_NUMBER}"
SNAPSHOT_FILE="$BRANCH_STATE_DIR/snapshot.json"
CONTEXT_DIR="$BRANCH_STATE_DIR/context"

mkdir -p "$BRANCH_STATE_DIR" "$CONTEXT_DIR"

echo "Watching PR $OWNER_REPO#$PR_NUMBER (interval ${INTERVAL}s)" >&2

fetch_snapshot() {
  if ! command -v gh >/dev/null 2>&1; then
    echo "gh CLI not found; cannot fetch PR data" >&2
    exit 1
  fi
  gh pr view "$PR_NUMBER" --repo "$OWNER_REPO" --json number,state,mergeable,headRefName,headOid,baseRefName,comments --jq '.' > "$SNAPSHOT_FILE"
}

start_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if [[ -f "$SNAPSHOT_FILE" ]]; then
  rm -f "$SNAPSHOT_FILE"
fi
fetch_snapshot

while true; do
  sleep "$INTERVAL"
  if ! gh pr view "$PR_NUMBER" --repo "$OWNER_REPO" --json number,state,mergeable,headRefName,headOid,baseRefName,comments --jq '.' > "$BRANCH_STATE_DIR/current.json"; then
    echo "Failed to fetch PR context" >&2
    continue
  fi
  # Simple change detection: compare with previous snapshot
  if ! diff -u "$SNAPSHOT_FILE" "$BRANCH_STATE_DIR/current.json" >/dev/null; then
    # Activation: something changed
    echo "activation: change detected" >&2
    mv "$BRANCH_STATE_DIR/current.json" "$SNAPSHOT_FILE"
    # Refresh context bundle if available
    if command -v /bin/true >/dev/null; then
      if [ -d "$CONTEXT_DIR" ]; then
        # ensure a refreshed bundle is produced via fetch-pr-context if available
        if [ -x "scripts/follow_pr/fetch-pr-context.sh" ]; then
          bash scripts/follow_pr/fetch-pr-context.sh "$OWNER_REPO" "$PR_NUMBER" > "$CONTEXT_DIR/pr-context.json" 2>&1 || true
        fi
      fi
    fi
    echo "Activation event at $(date -u +%Y-%m-%dT%H:%M:%SZ)"; break
  else
    # No change; continue polling
    continue
  fi
done
