#!/usr/bin/env bash

set -euo pipefail

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

if [[ -z "${GH_TOKEN:-}" ]]; then
    echo "GH_TOKEN must be set" >&2
    exit 2
fi

if [[ -z "${ISSUE_TITLE_BASE:-}" ]]; then
    echo "ISSUE_TITLE_BASE must be set" >&2
    exit 2
fi

if [[ -z "${ISSUE_BODY_FILE:-}" ]]; then
    echo "ISSUE_BODY_FILE must be set" >&2
    exit 2
fi

if [[ ! -f "${ISSUE_BODY_FILE}" ]]; then
    echo "Issue body file not found: ${ISSUE_BODY_FILE}" >&2
    exit 2
fi

if [[ -z "${ISSUE_SUITE_KEY:-}" ]]; then
    echo "ISSUE_SUITE_KEY must be set" >&2
    exit 2
fi

summary_file="${ISSUE_SUMMARY_JSON_FILE:-}"
issue_status="UNKNOWN"
issue_detail=""

if [[ -n "${summary_file}" ]]; then
    if [[ ! -f "${summary_file}" ]]; then
        echo "Issue summary file not found: ${summary_file}" >&2
        exit 2
    fi
    parsed_summary="$(
        python3 - "${summary_file}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
failed = payload.get("failed_count")
total = payload.get("scenario_count")
if total is None:
    total = payload.get("benchmark_count")
if total is None:
    total = payload.get("total_count")

status = "UNKNOWN"
detail = ""
if isinstance(failed, int):
    if failed > 0:
        status = "FAIL"
        if isinstance(total, int) and total > 0:
            detail = f"{failed}/{total} failing"
        else:
            detail = f"{failed} failing"
    else:
        if isinstance(total, int) and total > 0:
            status = "PASS"
            detail = f"all {total} passing"
        else:
            status = "FAIL"
            detail = "no results"

print(status)
print(detail)
PY
    )"
    parsed_lines=()
    while IFS= read -r line; do
        parsed_lines+=("${line}")
    done <<<"${parsed_summary}"
    if [[ "${#parsed_lines[@]}" -ge 1 ]]; then
        issue_status="$(trim "${parsed_lines[0]}")"
    fi
    if [[ "${#parsed_lines[@]}" -ge 2 ]]; then
        issue_detail="$(trim "${parsed_lines[1]}")"
    fi
fi

issue_title="[${issue_status}] ${ISSUE_TITLE_BASE}"
if [[ -n "${issue_detail}" ]]; then
    issue_title="${issue_title} (${issue_detail})"
fi

label_csv="$(trim "${ISSUE_LABELS:-live-suite-report}")"
IFS=',' read -r -a raw_labels <<<"${label_csv}"
labels=()
for raw_label in "${raw_labels[@]}"; do
    label="$(trim "${raw_label}")"
    if [[ -n "${label}" ]]; then
        labels+=("${label}")
        gh label create "${label}" \
            --repo "${GITHUB_REPOSITORY}" \
            --color "0E8A16" \
            --description "Live suite reporting issue" \
            --force >/dev/null 2>&1 || true
    fi
done

marker="live-suite-key:${ISSUE_SUITE_KEY}"
existing_issue_number="$(
    gh issue list \
        --repo "${GITHUB_REPOSITORY}" \
        --state open \
        --search "\"${marker}\" in:body" \
        --json number \
        --jq '.[0].number // empty'
)"

issue_body_with_meta="$(mktemp)"
trap 'rm -f "${issue_body_with_meta}"' EXIT

{
    echo "<!-- ${marker} -->"
    echo "<!-- live-suite-status:${issue_status} -->"
    echo "<!-- live-suite-updated:${GITHUB_RUN_ID:-unknown} -->"
    echo ""
    cat "${ISSUE_BODY_FILE}"
} >"${issue_body_with_meta}"

label_args=()
for label in "${labels[@]}"; do
    label_args+=(--label "${label}")
done

if [[ -n "${existing_issue_number}" ]]; then
    echo "Updating existing issue #${existing_issue_number}..."
    gh issue edit "${existing_issue_number}" \
        --repo "${GITHUB_REPOSITORY}" \
        --title "${issue_title}" \
        --body-file "${issue_body_with_meta}" \
        "${label_args[@]}" >/dev/null
    echo "Updated live issue #${existing_issue_number}"
else
    echo "Creating new issue..."
    new_issue_url="$(
        gh issue create \
            --repo "${GITHUB_REPOSITORY}" \
            --title "${issue_title}" \
            --body-file "${issue_body_with_meta}" \
            "${label_args[@]}"
    )"
    echo "Created live issue: ${new_issue_url}"
fi
