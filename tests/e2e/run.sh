#!/usr/bin/env bash
#
# KIND e2e test for logfwd.
# Validates the full pipeline: file tail → CRI parse → SQL transform → HTTP output.
#
set -euo pipefail

CLUSTER_NAME="${E2E_CLUSTER_NAME:-logfwd-e2e}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"
IMAGE="logfwd:e2e"
TIMEOUT=120
EXPECTED_LINES=10
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NO_DESTROY="${NO_DESTROY:-}"
PF_PID=""
CLUSTER_CREATED=0

k() {
    kubectl --context "$KUBE_CONTEXT" "$@"
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
k wait -n "$NAMESPACE" deployment/blackhole-receiver \
    --for=condition=available --timeout=90s
k rollout status -n "$NAMESPACE" daemonset/logfwd --timeout=90s

echo "=== Phase 5: Generate logs ==="
k delete pod -n "$NAMESPACE" log-generator --ignore-not-found 2>/dev/null
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/log-generator.yaml"
# Don't wait for Ready — busybox pod may not have readiness probe.
# The container will start and begin emitting logs after its 5s sleep.
sleep 8

echo "=== Phase 6: Verify ==="
k port-forward -n "$NAMESPACE" svc/blackhole-receiver 14318:4318 &
PF_PID=$!
sleep 2
if ! kill -0 "$PF_PID" 2>/dev/null; then
    echo "FAIL: port-forward failed to stay running"
    echo "--- blackhole-receiver pods ---"
    k get pods -n "$NAMESPACE" -l app=blackhole-receiver -o wide 2>&1 || true
    exit 1
fi

DEADLINE=$((SECONDS + TIMEOUT))
while [ $SECONDS -lt $DEADLINE ]; do
    if ! kill -0 "$PF_PID" 2>/dev/null; then
        echo ""
        echo "FAIL: port-forward exited during verification loop (pid=$PF_PID)"
        echo "--- blackhole-receiver pods ---"
        k get pods -n "$NAMESPACE" -l app=blackhole-receiver -o wide 2>&1 || true
        exit 1
    fi

    STATS=$(curl --connect-timeout 1 --max-time 2 -sf http://localhost:14318/stats 2>/dev/null || echo '{}')
    LINES=$(echo "$STATS" | sed -n 's/.*"lines":\([0-9]*\).*/\1/p')
    LINES="${LINES:-0}"

    echo "  blackhole /stats: $STATS (want lines == $EXPECTED_LINES)"

    if [ "$LINES" -eq "$EXPECTED_LINES" ]; then
        echo ""
        echo "PASS: blackhole received $LINES lines (expected $EXPECTED_LINES)"
        exit 0
    elif [ "$LINES" -gt "$EXPECTED_LINES" ]; then
        echo ""
        echo "FAIL: blackhole received $LINES lines (> expected $EXPECTED_LINES)"
        exit 1
    fi
    sleep 5
done

echo ""
echo "FAIL: timed out after ${TIMEOUT}s — blackhole received $LINES lines, expected $EXPECTED_LINES"
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
