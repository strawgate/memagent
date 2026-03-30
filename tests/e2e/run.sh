#!/usr/bin/env bash
#
# KIND e2e test for logfwd.
# Validates the full pipeline: file tail → CRI parse → SQL transform → HTTP output.
#
set -euo pipefail

CLUSTER_NAME="${E2E_CLUSTER_NAME:-logfwd-e2e}"
NAMESPACE="e2e-logfwd"
IMAGE="logfwd:e2e"
TIMEOUT=120
EXPECTED_LINES=10
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NO_DESTROY="${NO_DESTROY:-}"
PF_PID=""

cleanup() {
    if [ -n "$PF_PID" ]; then
        kill "$PF_PID" 2>/dev/null || true
    fi
    if [ -z "$NO_DESTROY" ]; then
        echo "--- Cleaning up ---"
        kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
        kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
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
fi

echo "=== Phase 2: Build and load image ==="
docker build -t "$IMAGE" "$REPO_ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

echo "=== Phase 3: Deploy ==="
kubectl create namespace "$NAMESPACE" 2>/dev/null || true
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/logfwd-config.yaml"
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/blackhole-receiver.yaml"
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/logfwd-daemonset.yaml"

echo "=== Phase 4: Wait for readiness ==="
kubectl wait -n "$NAMESPACE" deployment/blackhole-receiver \
    --for=condition=available --timeout=90s
kubectl rollout status -n "$NAMESPACE" daemonset/logfwd --timeout=90s

echo "=== Phase 5: Generate logs ==="
kubectl delete pod -n "$NAMESPACE" log-generator --ignore-not-found 2>/dev/null
kubectl apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/log-generator.yaml"
# Don't wait for Ready — busybox pod may not have readiness probe.
# The container will start and begin emitting logs after its 5s sleep.
sleep 8

echo "=== Phase 6: Verify ==="
kubectl port-forward -n "$NAMESPACE" svc/blackhole-receiver 14318:4318 &
PF_PID=$!
sleep 2

DEADLINE=$((SECONDS + TIMEOUT))
while [ $SECONDS -lt $DEADLINE ]; do
    STATS=$(curl -sf http://localhost:14318/stats 2>/dev/null || echo '{}')
    LINES=$(echo "$STATS" | sed -n 's/.*"lines":\([0-9]*\).*/\1/p')
    LINES="${LINES:-0}"

    echo "  blackhole /stats: $STATS (want lines == $EXPECTED_LINES)"

    if [ "$LINES" -ge "$EXPECTED_LINES" ]; then
        echo ""
        echo "PASS: blackhole received $LINES lines (expected $EXPECTED_LINES)"
        exit 0
    fi
    sleep 5
done

echo ""
echo "FAIL: timed out after ${TIMEOUT}s — blackhole received $LINES lines, expected $EXPECTED_LINES"
echo ""
echo "--- logfwd DaemonSet logs ---"
kubectl logs -n "$NAMESPACE" -l app=logfwd --tail=80 2>&1 || true
echo ""
echo "--- blackhole-receiver logs ---"
kubectl logs -n "$NAMESPACE" -l app=blackhole-receiver --tail=30 2>&1 || true
echo ""
echo "--- log-generator logs ---"
kubectl logs -n "$NAMESPACE" log-generator --tail=20 2>&1 || true
exit 1
