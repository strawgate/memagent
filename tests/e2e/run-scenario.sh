#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <scenario-id>" >&2
    exit 2
fi

SCENARIO_ID="$1"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCENARIO_DIR="$REPO_ROOT/tests/e2e/scenarios/$SCENARIO_ID"
E2E_RESULTS_DIR="${E2E_RESULTS_DIR:-$REPO_ROOT/tests/e2e/results/$SCENARIO_ID}"

if [[ ! -d "$SCENARIO_DIR" ]]; then
    echo "unknown e2e scenario: $SCENARIO_ID" >&2
    exit 2
fi

export REPO_ROOT
export SCENARIO_ID
export SCENARIO_DIR
export E2E_RESULTS_DIR

source "$REPO_ROOT/tests/e2e/lib/common.sh"

mkdir -p "$E2E_RESULTS_DIR"

run_phase() {
    local phase="$1"
    local script_path="$SCENARIO_DIR/${phase}.sh"

    if [[ -x "$script_path" ]]; then
        "$script_path"
        return 0
    fi

    run_default_phase "$phase"
}

cleanup() {
    run_phase collect || true
    run_phase down || true
}
trap cleanup EXIT

run_phase up
"$SCENARIO_DIR/run_workload.sh"
run_phase verify
