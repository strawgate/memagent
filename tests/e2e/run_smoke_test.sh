#!/usr/bin/env bash
#
# Stub-based regression test for tests/e2e/run.sh.
# Verifies marker emission is gated before port-forward/verification.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SH="$SCRIPT_DIR/run.sh"
TMP_ROOT="$(mktemp -d)"
STUB_DIR="$TMP_ROOT/bin"
STATE_DIR="$TMP_ROOT/state"
LOG_FILE="$TMP_ROOT/invocations.log"
mkdir -p "$STUB_DIR" "$STATE_DIR"

cleanup() {
    rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

cat >"$STUB_DIR/kind" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
echo "kind $*" >>"$LOG_FILE"
case "$1" in
  get|create|load|delete) exit 0 ;;
esac
exit 0
EOF

cat >"$STUB_DIR/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
echo "docker $*" >>"$LOG_FILE"
exit 0
EOF

cat >"$STUB_DIR/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
STATE_DIR="${STATE_DIR:?}"
echo "curl $*" >>"$LOG_FILE"
case "$*" in
  *"/stats"*)
    test -f "$STATE_DIR/port_forward_started"
    printf '{"lines":12}'
    exit 0
    ;;
esac
exit 1
EOF

cat >"$STUB_DIR/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${LOG_FILE:?}"
STATE_DIR="${STATE_DIR:?}"
echo "kubectl $*" >>"$LOG_FILE"

if [ "${1:-}" = "--context" ]; then
  shift 2
fi

case "${1:-}" in
  create|apply|wait|rollout|delete)
    exit 0
    ;;
  get)
    if [ "${2:-}" = "pod" ] && [ "${3:-}" = "-n" ] && [ "${4:-}" = "e2e-logfwd" ] && [ "${5:-}" = "log-generator" ] && [ "${6:-}" = "-o" ] && [[ "${7:-}" == jsonpath=* ]]; then
      counter_file="$STATE_DIR/log_generator_polls"
      count=0
      if [ -f "$counter_file" ]; then
        count="$(cat "$counter_file")"
      fi
      count=$((count + 1))
      printf '%s' "$count" >"$counter_file"
      if [ "$count" -ge 3 ]; then
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
    if [ "${2:-}" = "-n" ] && [ "${3:-}" = "e2e-logfwd" ] && [ "${4:-}" = "log-generator" ]; then
      if [ -f "$STATE_DIR/log_generator_running" ]; then
        printf '%s\n' \
          'LOGFWD_E2E_MARKER_001' \
          'LOGFWD_E2E_MARKER_010' \
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
EOF

chmod +x "$STUB_DIR"/*

PATH="$STUB_DIR:$PATH" \
LOG_FILE="$LOG_FILE" \
STATE_DIR="$STATE_DIR" \
E2E_CLUSTER_NAME="smoke" \
NO_DESTROY=1 \
bash "$RUN_SH"

logs_line="$(grep -n 'logs -n e2e-logfwd log-generator --tail=40' "$LOG_FILE" | head -n1 | cut -d: -f1 || true)"
pf_line="$(grep -n 'port-forward -n e2e-logfwd svc/blackhole-receiver 14318:4318' "$LOG_FILE" | head -n1 | cut -d: -f1 || true)"

if [ -z "$logs_line" ]; then
  echo "FAIL: smoke test never waited for generator markers"
  exit 1
fi

if [ -z "$pf_line" ]; then
  echo "FAIL: smoke test never reached port-forward"
  exit 1
fi

if [ "$logs_line" -ge "$pf_line" ]; then
  echo "FAIL: generator markers were not gated before port-forward"
  exit 1
fi

echo "PASS: run.sh smoke test completed with generator gating and marker ordering"
