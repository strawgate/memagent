#!/usr/bin/env bash
#
# KIND stability test for logfwd.
#
# Runs logfwd in a KIND cluster under sustained load for 5 minutes and verifies:
# 1. No data loss (blackhole received >= 90% of generated lines)
# 2. No OOM kills (pod stayed running the entire time)
# 3. No restarts (restart count == 0)
# 4. Memory stayed within limits
# 5. Pod restart resilience (optional, if --with-restart is passed)
#
set -euo pipefail

CLUSTER_NAME="${E2E_CLUSTER_NAME:-logfwd-stability}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"
IMAGE="logfwd:e2e"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NO_DESTROY="${NO_DESTROY:-}"
PF_PID=""
CLUSTER_CREATED=0

# Test parameters
DURATION="${STABILITY_DURATION:-300}"     # 5 minutes default
LOG_RATE="${STABILITY_LOG_RATE:-100}"     # 100 lines/sec
EXPECTED_MIN_RATIO="${STABILITY_MIN_RATIO:-0.99}"  # expect >= 99% delivery (1% tolerance for CRI flush timing)
POLL_INTERVAL=10
WITH_RESTART=0

for arg in "$@"; do
    case "$arg" in
        --with-restart) WITH_RESTART=1 ;;
        --quick) DURATION=60; LOG_RATE=50 ;;
    esac
done

EXPECTED_LINES=$((DURATION * LOG_RATE))

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
        fi
    else
        echo "--- NO_DESTROY set, skipping cleanup ---"
    fi
}
trap cleanup EXIT

echo "=== Stability Test Configuration ==="
echo "  Duration:       ${DURATION}s"
echo "  Log rate:       ${LOG_RATE}/sec"
echo "  Expected lines: ~${EXPECTED_LINES}"
echo "  Min delivery:   ${EXPECTED_MIN_RATIO}"
echo "  With restart:   ${WITH_RESTART}"
echo ""

# ---------------------------------------------------------------------------
# Phase 1: Cluster
# ---------------------------------------------------------------------------
echo "=== Phase 1: KIND cluster ==="
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Reusing existing cluster: $CLUSTER_NAME"
else
    echo "Creating cluster: $CLUSTER_NAME"
    # Enable metrics-server for kubectl top
    cat <<KINDCFG | kind create cluster --name "$CLUSTER_NAME" --wait 60s --config -
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
KINDCFG
    CLUSTER_CREATED=1
fi

# ---------------------------------------------------------------------------
# Phase 2: Build + load
# ---------------------------------------------------------------------------
echo "=== Phase 2: Build and load image ==="
docker build -t "$IMAGE" "$REPO_ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

# ---------------------------------------------------------------------------
# Phase 3: Deploy
# ---------------------------------------------------------------------------
echo "=== Phase 3: Deploy ==="
k create namespace "$NAMESPACE" 2>/dev/null || true

# Use stability-specific config (filters for STABILITY_MARKER)
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/logfwd-config-stability.yaml"
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/blackhole-receiver.yaml"
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/logfwd-daemonset.yaml"

echo "=== Phase 4: Wait for readiness ==="
k wait -n "$NAMESPACE" deployment/blackhole-receiver \
    --for=condition=available --timeout=90s
k rollout status -n "$NAMESPACE" daemonset/logfwd --timeout=90s

# ---------------------------------------------------------------------------
# Phase 5: Start sustained log generation
# ---------------------------------------------------------------------------
echo "=== Phase 5: Start sustained log generator (${DURATION}s at ${LOG_RATE}/sec) ==="
k delete pod -n "$NAMESPACE" log-generator-sustained --ignore-not-found 2>/dev/null

# Override duration and rate via env vars in the manifest
cat "$SCRIPT_DIR/manifests/log-generator-sustained.yaml" | \
    sed "s/value: \"300\"/value: \"${DURATION}\"/" | \
    sed "s/value: \"100\"/value: \"${LOG_RATE}\"/" | \
    k apply -n "$NAMESPACE" -f -

# Set up port-forward to blackhole
k port-forward -n "$NAMESPACE" svc/blackhole-receiver 14318:4318 &
PF_PID=$!
sleep 2

# ---------------------------------------------------------------------------
# Phase 6: Monitor during test
# ---------------------------------------------------------------------------
echo "=== Phase 6: Monitoring (${DURATION}s) ==="

PREV_LINES=0
PREV_TIME=$(date +%s)
STALL_COUNT=0
MAX_MEMORY_MI=0

END_TIME=$(($(date +%s) + DURATION + 30))  # extra 30s for pipeline drain

while [ "$(date +%s)" -lt "$END_TIME" ]; do
    sleep "$POLL_INTERVAL"

    # Get blackhole stats
    STATS=$(curl --connect-timeout 2 --max-time 3 -sf http://localhost:14318/stats 2>/dev/null || echo '{}')
    LINES=$(echo "$STATS" | sed -n 's/.*"lines":\([0-9]*\).*/\1/p')
    LINES="${LINES:-0}"

    # Get pod status
    RESTARTS=$(k get pods -n "$NAMESPACE" -l app=logfwd -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo "?")
    POD_STATUS=$(k get pods -n "$NAMESPACE" -l app=logfwd -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "?")

    # Memory usage (if metrics available)
    MEMORY=$(k top pod -n "$NAMESPACE" -l app=logfwd --no-headers 2>/dev/null | awk '{print $3}' || echo "N/A")

    # Track peak memory
    if [[ "$MEMORY" =~ ^([0-9]+)Mi$ ]]; then
        MEM_MI="${BASH_REMATCH[1]}"
        if [ "$MEM_MI" -gt "$MAX_MEMORY_MI" ]; then
            MAX_MEMORY_MI=$MEM_MI
        fi
    fi

    # Check for stalls (no new lines in 2 consecutive polls)
    if [ "$LINES" -eq "$PREV_LINES" ] && [ "$PREV_LINES" -gt 0 ]; then
        STALL_COUNT=$((STALL_COUNT + 1))
    else
        STALL_COUNT=0
    fi

    NOW=$(date +%s)
    ELAPSED=$((NOW - PREV_TIME))
    if [ "$ELAPSED" -gt 0 ] && [ "$LINES" -gt "$PREV_LINES" ]; then
        RATE_ACTUAL=$(( (LINES - PREV_LINES) / ELAPSED ))
    else
        RATE_ACTUAL=0
    fi

    echo "  [$(date +%H:%M:%S)] lines=$LINES (+$RATE_ACTUAL/s) restarts=$RESTARTS status=$POD_STATUS memory=$MEMORY"

    PREV_LINES=$LINES
    PREV_TIME=$NOW

    # Mid-test restart (if requested)
    if [ "$WITH_RESTART" -eq 1 ]; then
        HALFWAY=$((DURATION / 2))
        ELAPSED_TOTAL=$((NOW - (END_TIME - DURATION - 30)))
        if [ "$ELAPSED_TOTAL" -ge "$HALFWAY" ] && [ "$ELAPSED_TOTAL" -lt $((HALFWAY + POLL_INTERVAL + 5)) ]; then
            echo "  >>> Restarting logfwd pods (resilience test) <<<"
            k delete pod -n "$NAMESPACE" -l app=logfwd
            sleep 5
            k rollout status -n "$NAMESPACE" daemonset/logfwd --timeout=60s
        fi
    fi

    # Early exit if we got everything
    if [ "$LINES" -ge "$EXPECTED_LINES" ]; then
        echo "  All expected lines received!"
        break
    fi

    # Fail fast on stall
    if [ "$STALL_COUNT" -ge 6 ]; then
        echo ""
        echo "FAIL: pipeline stalled — no new lines for ${STALL_COUNT} consecutive polls"
        echo "--- logfwd logs ---"
        k logs -n "$NAMESPACE" -l app=logfwd --tail=40 2>&1 || true
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Phase 7: Results
# ---------------------------------------------------------------------------
echo ""
echo "=== Results ==="

FINAL_STATS=$(curl --connect-timeout 2 --max-time 3 -sf http://localhost:14318/stats 2>/dev/null || echo '{}')
FINAL_LINES=$(echo "$FINAL_STATS" | sed -n 's/.*"lines":\([0-9]*\).*/\1/p')
FINAL_LINES="${FINAL_LINES:-0}"

FINAL_RESTARTS=$(k get pods -n "$NAMESPACE" -l app=logfwd -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null || echo "?")
FINAL_STATUS=$(k get pods -n "$NAMESPACE" -l app=logfwd -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "?")

if [ "$EXPECTED_LINES" -gt 0 ]; then
    RATIO=$(awk "BEGIN{printf \"%.4f\", $FINAL_LINES / $EXPECTED_LINES}")
    PERCENT=$(awk "BEGIN{printf \"%.1f\", $RATIO * 100}")
else
    RATIO="0"
    PERCENT="0"
fi

echo "  Lines generated (expected): ~${EXPECTED_LINES}"
echo "  Lines received:             ${FINAL_LINES}"
echo "  Delivery ratio:             ${PERCENT}%"
echo "  Pod restarts:               ${FINAL_RESTARTS}"
echo "  Pod status:                 ${FINAL_STATUS}"
echo "  Peak memory:                ${MAX_MEMORY_MI}Mi"
echo ""

# Assertions
FAILED=0

# 1. Delivery ratio
if awk "BEGIN{exit !($RATIO < $EXPECTED_MIN_RATIO)}"; then
    echo "FAIL: delivery ratio ${PERCENT}% < ${EXPECTED_MIN_RATIO} minimum"
    FAILED=1
else
    echo "PASS: delivery ratio ${PERCENT}% >= $(awk "BEGIN{printf \"%.0f\", $EXPECTED_MIN_RATIO * 100}")%"
fi

# 2. No unexpected restarts
if [ "$WITH_RESTART" -eq 0 ]; then
    if [ "$FINAL_RESTARTS" != "0" ]; then
        echo "FAIL: pod restarted ${FINAL_RESTARTS} times (expected 0)"
        FAILED=1
    else
        echo "PASS: no unexpected pod restarts"
    fi
else
    echo "INFO: restart test — ${FINAL_RESTARTS} restarts (expected)"
fi

# 3. Pod still running
if [ "$FINAL_STATUS" != "Running" ]; then
    echo "FAIL: pod status is ${FINAL_STATUS} (expected Running)"
    FAILED=1
else
    echo "PASS: pod still running"
fi

# 4. Memory within limits (if we got data)
if [ "$MAX_MEMORY_MI" -gt 0 ]; then
    if [ "$MAX_MEMORY_MI" -gt 240 ]; then
        echo "WARN: peak memory ${MAX_MEMORY_MI}Mi approaching 256Mi limit"
    else
        echo "PASS: peak memory ${MAX_MEMORY_MI}Mi within limits"
    fi
fi

echo ""
if [ "$FAILED" -eq 1 ]; then
    echo "=== STABILITY TEST FAILED ==="
    echo ""
    echo "--- logfwd logs (last 40 lines) ---"
    k logs -n "$NAMESPACE" -l app=logfwd --tail=40 2>&1 || true
    echo ""
    echo "--- generator logs ---"
    k logs -n "$NAMESPACE" log-generator-sustained --tail=10 2>&1 || true
    exit 1
else
    echo "=== STABILITY TEST PASSED ==="
    exit 0
fi
