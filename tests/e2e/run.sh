#!/usr/bin/env bash
#
# KIND e2e test for logfwd.
# Validates the full pipeline: file tail → CRI parse → SQL transform → HTTP output.
#
# shellcheck disable=SC2329
set -euo pipefail

CLUSTER_NAME="${E2E_CLUSTER_NAME:-logfwd-e2e}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"
IMAGE="logfwd:e2e"
TIMEOUT=120
# Accept a range to tolerate benign duplicates from container restarts
# or CRI partial-line reassembly edge cases.
MIN_EXPECTED_LINES=10
MAX_EXPECTED_LINES=15
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NO_DESTROY="${NO_DESTROY:-}"
PF_PID=""
CLUSTER_CREATED=0
LOG_GENERATOR_MARKER='log-generator: all 10 markers emitted'
DEPLOY_TIMEOUT="${E2E_DEPLOY_TIMEOUT:-120s}"

k() {
    kubectl --context "$KUBE_CONTEXT" "$@"
}

fail() {
    local token="$1"
    shift
    echo "E2E_FAIL_CATEGORY=$token"
    echo "FAIL[$token]: $*"
}

# Poll a condition with backoff. Usage: wait_for <description> <timeout_s> <command...>
wait_for() {
    local desc="$1" timeout="$2"; shift 2
    local deadline=$((SECONDS + timeout))
    local delay=1
    while [ $SECONDS -lt $deadline ]; do
        if "$@" 2>/dev/null; then
            return 0
        fi
        sleep "$delay"
        delay=$(( delay < 5 ? delay + 1 : 5 ))
    done
    if "$@" 2>/dev/null; then
        return 0
    fi
    echo "FAIL: timed out waiting for: $desc (${timeout}s)"
    return 1
}

log_generator_is_running() {
    local phase
    phase="$(k get pod -n "$NAMESPACE" log-generator -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$phase" = "Running" ]
}

log_generator_markers_emitted() {
    k logs -n "$NAMESPACE" log-generator --tail=40 2>/dev/null | grep -q "$LOG_GENERATOR_MARKER"
}

cleanup() {
    if [ -n "$PF_PID" ]; then
        kill "$PF_PID" 2>/dev/null || true
    fi
    if [ -z "$NO_DESTROY" ]; then
        echo "--- Cleaning up ---"
        k delete namespace "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
        if [ "$CLUSTER_CREATED" -eq 1 ]; then
            kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
        else
            echo "Keeping pre-existing cluster: $CLUSTER_NAME"
        fi
    else
        echo "--- NO_DESTROY set, skipping cleanup ---"
    fi
}
trap cleanup EXIT

echo "=== Phase 1: KIND cluster ==="
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Reusing existing cluster: $CLUSTER_NAME"
else
    echo "Creating cluster: $CLUSTER_NAME"
    kind create cluster --name "$CLUSTER_NAME" --wait 60s
    CLUSTER_CREATED=1
fi

echo "=== Phase 2: Build and load image ==="
docker build -t "$IMAGE" "$REPO_ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

echo "=== Phase 3: Deploy ==="
k create namespace "$NAMESPACE" 2>/dev/null || true
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/logfwd-config.yaml"
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/blackhole-receiver.yaml"
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/logfwd-daemonset.yaml"

echo "=== Phase 4: Wait for readiness ==="
if ! k wait -n "$NAMESPACE" deployment/blackhole-receiver \
    --for=condition=available --timeout="$DEPLOY_TIMEOUT"; then
    echo ""
    fail "RECEIVER_NOT_READY" "blackhole-receiver deployment not available"
    k describe pods -n "$NAMESPACE" -l app=blackhole-receiver 2>&1 || true
    k logs -n "$NAMESPACE" -l app=blackhole-receiver --tail=50 2>&1 || true
    exit 1
fi
if ! k rollout status -n "$NAMESPACE" daemonset/logfwd --timeout="$DEPLOY_TIMEOUT"; then
    echo ""
    fail "LOGFWD_ROLLOUT_TIMEOUT" "logfwd daemonset rollout timed out"
    k get pods -n "$NAMESPACE" -l app=logfwd -o wide 2>&1 || true
    k describe pods -n "$NAMESPACE" -l app=logfwd 2>&1 || true
    k logs -n "$NAMESPACE" -l app=logfwd --tail=80 2>&1 || true
    k get events -n "$NAMESPACE" --sort-by='.lastTimestamp' 2>&1 || true
    exit 1
fi

echo "=== Phase 5: Generate logs ==="
k delete pod -n "$NAMESPACE" log-generator --ignore-not-found 2>/dev/null
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/log-generator.yaml"

# Strictly gate marker emission before verification. This prevents races where
# the verify loop starts before the generator has actually emitted the marker set.
if ! wait_for "log-generator pod running" 45 log_generator_is_running; then
    echo ""
    fail "GENERATOR_NOT_RUNNING" "log-generator pod never reached Running"
    k get pod -n "$NAMESPACE" log-generator -o wide 2>&1 || true
    k describe pod -n "$NAMESPACE" log-generator 2>&1 || true
    k logs -n "$NAMESPACE" log-generator --tail=40 2>&1 || true
    exit 1
fi

if ! wait_for "log-generator marker emission" 60 log_generator_markers_emitted; then
    echo ""
    fail "GENERATOR_MARKERS_MISSING" "log-generator never emitted the final marker batch"
    k get pod -n "$NAMESPACE" log-generator -o wide 2>&1 || true
    k describe pod -n "$NAMESPACE" log-generator 2>&1 || true
    k logs -n "$NAMESPACE" log-generator --tail=80 2>&1 || true
    exit 1
fi

echo "=== Phase 6: Port-forward ==="
k port-forward -n "$NAMESPACE" svc/blackhole-receiver 14318:4318 &
PF_PID=$!

# Wait for port-forward to accept connections instead of hardcoded sleep.
if ! wait_for "port-forward accepting connections" 15 \
    curl --connect-timeout 1 --max-time 1 -sf http://localhost:14318/stats; then
    echo ""
    fail "PORT_FORWARD_NOT_READY" "port-forward did not start serving /stats in time"
    k get pods -n "$NAMESPACE" -l app=blackhole-receiver -o wide 2>&1 || true
    k logs -n "$NAMESPACE" -l app=blackhole-receiver --tail=50 2>&1 || true
    exit 1
fi

echo "=== Phase 7: Verify ==="
DEADLINE=$((SECONDS + TIMEOUT))
DELAY=2
while [ $SECONDS -lt $DEADLINE ]; do
    if ! kill -0 "$PF_PID" 2>/dev/null; then
        echo ""
        fail "PORT_FORWARD_DROPPED" "port-forward exited during verification (pid=$PF_PID)"
        k get pods -n "$NAMESPACE" -l app=blackhole-receiver -o wide 2>&1 || true
        exit 1
    fi

    STATS=$(curl --connect-timeout 1 --max-time 2 -sf http://localhost:14318/stats 2>/dev/null || echo '{}')
    LINES=$(echo "$STATS" | sed -n 's/.*"lines":\([0-9]*\).*/\1/p')
    LINES="${LINES:-0}"

    echo "  blackhole /stats: lines=$LINES (want ${MIN_EXPECTED_LINES}..${MAX_EXPECTED_LINES})"

    if [ "$LINES" -ge "$MIN_EXPECTED_LINES" ] && [ "$LINES" -le "$MAX_EXPECTED_LINES" ]; then
        echo ""
        echo "PASS: blackhole received $LINES lines (expected ${MIN_EXPECTED_LINES}..${MAX_EXPECTED_LINES})"
        exit 0
    elif [ "$LINES" -gt "$MAX_EXPECTED_LINES" ]; then
        echo ""
        fail "TOO_MANY_LINES" "blackhole received $LINES lines (> max expected $MAX_EXPECTED_LINES)"
        echo "--- logfwd logs ---"
        k logs -n "$NAMESPACE" -l app=logfwd --tail=40 2>&1 || true
        exit 1
    fi

    # Exponential backoff: 2s → 3s → 4s → 5s (capped)
    sleep "$DELAY"
    DELAY=$(( DELAY < 5 ? DELAY + 1 : 5 ))
done

echo ""
fail "VERIFY_TIMEOUT" "timed out after ${TIMEOUT}s — blackhole received $LINES lines, expected ${MIN_EXPECTED_LINES}..${MAX_EXPECTED_LINES}"
echo ""
echo "--- logfwd DaemonSet logs ---"
k logs -n "$NAMESPACE" -l app=logfwd --tail=80 2>&1 || true
echo ""
echo "--- blackhole-receiver logs ---"
k logs -n "$NAMESPACE" -l app=blackhole-receiver --tail=30 2>&1 || true
echo ""
echo "--- log-generator logs ---"
k logs -n "$NAMESPACE" log-generator --tail=20 2>&1 || true
exit 1
