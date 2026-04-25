#!/usr/bin/env bash
#
# Stub-based regression tests for tests/e2e/run.sh.
# Verifies marker gating and failure artifact capture branches.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SH="$SCRIPT_DIR/run.sh"

make_stubs() {
    local stub_dir="$1"

    cat >"$stub_dir/kind" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
echo "kind $*" >>"$LOG_FILE"
case "${1:-}" in
  get|create|load|delete) exit 0 ;;
esac
exit 0
STUB

    cat >"$stub_dir/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
echo "docker $*" >>"$LOG_FILE"
exit 0
STUB

    cat >"$stub_dir/curl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
STATE_DIR="${STATE_DIR:?}"
SCENARIO="${SCENARIO:?}"
echo "curl $*" >>"$LOG_FILE"
case "$*" in
  *"/stats"*)
    if [ "$SCENARIO" = "too_many_lines" ]; then
      printf '{"lines":99}'
      exit 0
    fi

    test -f "$STATE_DIR/port_forward_started"
    printf '{"lines":12}'
    exit 0
    ;;
esac
exit 1
STUB

    cat >"$stub_dir/kubectl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
STATE_DIR="${STATE_DIR:?}"
SCENARIO="${SCENARIO:?}"
echo "kubectl $*" >>"$LOG_FILE"

if [ "${1:-}" = "--context" ]; then
  shift 2
fi

case "${1:-}" in
  create|apply|delete)
    exit 0
    ;;
  wait)
    exit 0
    ;;
  rollout)
    if [ "${2:-}" = "status" ] && [ "$SCENARIO" = "rollout_timeout" ] && [ "${5:-}" = "daemonset/ff" ]; then
      exit 1
    fi
    exit 0
    ;;
  get)
    if [ "${2:-}" = "pod" ] && [ "${3:-}" = "-n" ] && [ "${4:-}" = "e2e-ff" ] && [ "${5:-}" = "log-generator" ] && [ "${6:-}" = "-o" ] && [[ "${7:-}" == jsonpath=* ]]; then
      counter_file="$STATE_DIR/log_generator_polls"
      count=0
      if [ -f "$counter_file" ]; then
        count="$(cat "$counter_file")"
      fi
      count=$((count + 1))
      printf '%s' "$count" >"$counter_file"
      if [ "$count" -ge 2 ]; then
        printf 'Running'
        printf 'running' >"$STATE_DIR/log_generator_running"
      else
        printf 'Pending'
      fi
      exit 0
    fi
    exit 0
    ;;
  logs)
    if [ "${2:-}" = "-n" ] && [ "${3:-}" = "e2e-ff" ] && [ "${4:-}" = "log-generator" ]; then
      if [ -f "$STATE_DIR/log_generator_running" ]; then
        printf '%s\n' \
          'FFWD_E2E_MARKER_001' \
          'FFWD_E2E_MARKER_010' \
          'log-generator: all 10 markers emitted'
        printf 'ready' >"$STATE_DIR/marker_ready"
      fi
      exit 0
    fi
    exit 0
    ;;
  port-forward)
    test -f "$STATE_DIR/marker_ready"
    printf 'started' >"$STATE_DIR/port_forward_started"
    exec sleep 300
    ;;
esac

exit 0
STUB

    chmod +x "$stub_dir"/*
}

assert_contains() {
    local file="$1"
    local pattern="$2"
    local message="$3"
    if ! grep -q "$pattern" "$file"; then
        echo "FAIL: $message"
        echo "Pattern: $pattern"
        echo "File: $file"
        exit 1
    fi
}

run_case() {
    local scenario="$1"
    local expected_exit="$2"
    local expected_category="$3"

    local tmp_root
    tmp_root="$(mktemp -d)"
    local stub_dir="$tmp_root/bin"
    local state_dir="$tmp_root/state"
    local log_file="$tmp_root/invocations.log"
    local out_file="$tmp_root/output.log"
    local artifact_root="$tmp_root/artifacts"

    mkdir -p "$stub_dir" "$state_dir" "$artifact_root"
    make_stubs "$stub_dir"

    set +e
    PATH="$stub_dir:$PATH" \
    LOG_FILE="$log_file" \
    STATE_DIR="$state_dir" \
    SCENARIO="$scenario" \
    E2E_CLUSTER_NAME="smoke" \
    E2E_RUN_ID="$scenario" \
    E2E_ARTIFACT_ROOT="$artifact_root" \
    NO_DESTROY=1 \
    bash "$RUN_SH" >"$out_file" 2>&1
    local status=$?
    set -e

    if [ "$status" -ne "$expected_exit" ]; then
        echo "FAIL: scenario '$scenario' exit code $status (expected $expected_exit)"
        cat "$out_file"
        exit 1
    fi

    if [ "$expected_exit" -eq 0 ]; then
        assert_contains "$out_file" 'PASS: blackhole received' "scenario '$scenario' did not pass"

        local logs_line
        local pf_line
        logs_line="$(grep -n 'logs -n e2e-ff log-generator --tail=40' "$log_file" | head -n1 | cut -d: -f1 || true)"
        pf_line="$(grep -n 'port-forward -n e2e-ff svc/blackhole-receiver 14318:4318' "$log_file" | head -n1 | cut -d: -f1 || true)"

        if [ -z "$logs_line" ] || [ -z "$pf_line" ] || [ "$logs_line" -ge "$pf_line" ]; then
            echo "FAIL: scenario '$scenario' marker gating order invalid"
            exit 1
        fi
    else
        assert_contains "$out_file" "E2E_FAIL_CATEGORY=$expected_category" "scenario '$scenario' missing fail category"
        assert_contains "$out_file" 'Artifacts captured in:' "scenario '$scenario' did not capture artifacts"
        assert_contains "$log_file" "kubectl --context kind-smoke get events -n e2e-ff --sort-by=.lastTimestamp" \
            "scenario '$scenario' did not run events diagnostics"
        assert_contains "$log_file" 'kubectl --context kind-smoke describe pods -n e2e-ff -l app=ff' \
            "scenario '$scenario' did not run ff describe diagnostics"
        assert_contains "$log_file" 'kubectl --context kind-smoke logs -n e2e-ff -l app=blackhole-receiver --tail=120' \
            "scenario '$scenario' did not run blackhole logs diagnostics"

        local artifact_dir="$artifact_root/$scenario"
        assert_contains "$artifact_dir/failure_summary.txt" "category=$expected_category" \
            "scenario '$scenario' missing failure summary category"
        test -f "$artifact_dir/phase_timeline.log" || {
            echo "FAIL: scenario '$scenario' missing phase timeline"
            exit 1
        }
        test -f "$artifact_dir/stats_trace.log" || {
            echo "FAIL: scenario '$scenario' missing stats trace"
            exit 1
        }
    fi

    rm -rf "$tmp_root"
}

run_case "happy" 0 ""
run_case "rollout_timeout" 1 "FF_ROLLOUT_TIMEOUT"
run_case "too_many_lines" 1 "TOO_MANY_LINES"

echo "PASS: run.sh smoke tests completed (happy + failure artifact branches)"
