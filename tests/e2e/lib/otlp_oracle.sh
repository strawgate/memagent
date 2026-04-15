#!/usr/bin/env bash

set -euo pipefail

run_otlp_oracle_test() {
    local test_name="$1"
    set +e
    cargo test -p logfwd-io --test it "$test_name" --manifest-path "$MEMAGENT_REPO_ROOT/Cargo.toml" -- --ignored --exact --nocapture \
        >"$E2E_RESULTS_DIR/test.log" 2>&1
    local status=$?
    set -e
    printf '%s\n' "$status" >"$E2E_RESULTS_DIR/test.exitcode"
    printf '[]\n' >"$E2E_RESULTS_DIR/expected_rows.json"
}

verify_otlp_oracle_result() {
    local scenario_id="$1"
    local status
    status="$(cat "$E2E_RESULTS_DIR/test.exitcode")"
    printf '[]\n' >"$E2E_RESULTS_DIR/actual_rows.json"
    python3 - <<'PY' "$E2E_RESULTS_DIR" "$status" "$scenario_id"
import json
import pathlib
import sys

results = pathlib.Path(sys.argv[1])
status = int(sys.argv[2])
scenario = sys.argv[3]
result = {
    "scenario": scenario,
    "policy": "external-oracle",
    "passed": status == 0,
    "expected_count": 1,
    "actual_count": 1 if status == 0 else 0,
}
(results / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
summary = [
    f"## {scenario}",
    "",
    f"- Status: `{'PASS' if status == 0 else 'FAIL'}`",
    "- Mode: `cargo test ignored external oracle`",
    f"- Log: `{results / 'test.log'}`",
]
(results / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")
sys.exit(0 if status == 0 else 1)
PY
}
