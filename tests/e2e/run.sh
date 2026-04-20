#!/usr/bin/env bash
#
# KIND e2e test for ff.
# Validates the full pipeline: file tail → CRI parse → SQL transform → HTTP output.
#
# shellcheck disable=SC2329
set -euo pipefail

CLUSTER_NAME="${E2E_CLUSTER_NAME:-ff-e2e}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-ff"
IMAGE="ff:e2e"
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
ARTIFACT_ROOT="${E2E_ARTIFACT_ROOT:-$SCRIPT_DIR/logs/results}"
RUN_ID="${E2E_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="$ARTIFACT_ROOT/$RUN_ID"
PHASE_LOG="$ARTIFACT_DIR/phase_timeline.log"
STATS_TRACE_FILE="$ARTIFACT_DIR/stats_trace.log"
CURRENT_PHASE="startup"
LAST_STATS='{}'
LAST_LINES=0

mkdir -p "$ARTIFACT_DIR"
: >"$PHASE_LOG"
: >"$STATS_TRACE_FILE"

k() {
    kubectl --context "$KUBE_CONTEXT" "$@"
}

fail() {
    local token="$1"
    shift
    echo "E2E_FAIL_CATEGORY=$token"
    echo "FAIL[$token]: $*"
}

phase() {
    local name="$1"
    CURRENT_PHASE="$name"
    echo "=== $name ==="
    printf '%s phase=%s elapsed_seconds=%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$name" "$SECONDS" >>"$PHASE_LOG"
}

record_stats_trace() {
    printf '%s lines=%s stats=%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LAST_LINES" "$LAST_STATS" >>"$STATS_TRACE_FILE"
}

capture_cmd() {
    local file="$1"
    shift
    {
        echo "$ $*"
        "$@"
    } >"$ARTIFACT_DIR/$file" 2>&1 || true
}

capture_failure_bundle() {
    local token="$1"
    local message="$2"

    {
        echo "category=$token"
        echo "message=$message"
        echo "phase=$CURRENT_PHASE"
        echo "elapsed_seconds=$SECONDS"
        echo "pf_pid=${PF_PID:-}"
        echo "last_lines=$LAST_LINES"
        echo "artifact_dir=$ARTIFACT_DIR"
    } >"$ARTIFACT_DIR/failure_summary.txt"

    capture_cmd "kubectl_get_pods.txt" k get pods -n "$NAMESPACE" -o wide
    capture_cmd "kubectl_get_ff_daemonset.txt" k get daemonset -n "$NAMESPACE" ff -o wide
    capture_cmd "kubectl_get_blackhole_deployment.txt" k get deployment -n "$NAMESPACE" blackhole-receiver -o wide
    capture_cmd "kubectl_describe_ff_pods.txt" k describe pods -n "$NAMESPACE" -l app=ff
    capture_cmd "kubectl_describe_blackhole_pods.txt" k describe pods -n "$NAMESPACE" -l app=blackhole-receiver
    capture_cmd "kubectl_describe_log_generator.txt" k describe pod -n "$NAMESPACE" log-generator
    capture_cmd "kubectl_logs_ff.txt" k logs -n "$NAMESPACE" -l app=ff --tail=120
    capture_cmd "kubectl_logs_blackhole.txt" k logs -n "$NAMESPACE" -l app=blackhole-receiver --tail=120
    capture_cmd "kubectl_logs_log_generator.txt" k logs -n "$NAMESPACE" log-generator --tail=120
    capture_cmd "kubectl_events.txt" k get events -n "$NAMESPACE" --sort-by='.lastTimestamp'
    capture_cmd "kubectl_rollout_ff.txt" k rollout status -n "$NAMESPACE" daemonset/ff --timeout=5s
    capture_cmd "kubectl_rollout_blackhole.txt" k rollout status -n "$NAMESPACE" deployment/blackhole-receiver --timeout=5s
    capture_cmd "stats_snapshot.json" curl --connect-timeout 1 --max-time 2 -sf http://localhost:14318/stats

    if [ -n "$PF_PID" ] && kill -0 "$PF_PID" 2>/dev/null; then
        echo "port_forward_running=true" >>"$ARTIFACT_DIR/failure_summary.txt"
    else
        echo "port_forward_running=false" >>"$ARTIFACT_DIR/failure_summary.txt"
    fi

    echo "Artifacts captured in: $ARTIFACT_DIR"
}

fail_and_exit() {
    local token="$1"
    local message="$2"

    echo ""
    fail "$token" "$message"
    capture_failure_bundle "$token" "$message"
    exit 1
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
    local phase_value
    phase_value="$(k get pod -n "$NAMESPACE" log-generator -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$phase_value" = "Running" ]
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

phase "Phase 1: KIND cluster"
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Reusing existing cluster: $CLUSTER_NAME"
else
    echo "Creating cluster: $CLUSTER_NAME"
    kind create cluster --name "$CLUSTER_NAME" --wait 60s
    CLUSTER_CREATED=1
fi

phase "Phase 2: Build and load image"
docker build -t "$IMAGE" "$REPO_ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

phase "Phase 3: Deploy"
k create namespace "$NAMESPACE" 2>/dev/null || true
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/ff-config.yaml"
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/blackhole-receiver.yaml"
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/ff-daemonset.yaml"

phase "Phase 4: Wait for readiness"
if ! k wait -n "$NAMESPACE" deployment/blackhole-receiver \
    --for=condition=available --timeout="$DEPLOY_TIMEOUT"; then
    fail_and_exit "RECEIVER_NOT_READY" "blackhole-receiver deployment not available"
fi

if ! k rollout status -n "$NAMESPACE" daemonset/ff --timeout="$DEPLOY_TIMEOUT"; then
    fail_and_exit "FF_ROLLOUT_TIMEOUT" "ff daemonset rollout timed out"
fi

phase "Phase 5: Generate logs"
k delete pod -n "$NAMESPACE" log-generator --ignore-not-found 2>/dev/null
k apply -n "$NAMESPACE" -f "$SCRIPT_DIR/manifests/log-generator.yaml"

# Strictly gate marker emission before verification. This prevents races where
# the verify loop starts before the generator has actually emitted the marker set.
if ! wait_for "log-generator pod running" 45 log_generator_is_running; then
    fail_and_exit "GENERATOR_NOT_RUNNING" "log-generator pod never reached Running"
fi

if ! wait_for "log-generator marker emission" 60 log_generator_markers_emitted; then
    fail_and_exit "GENERATOR_MARKERS_MISSING" "log-generator never emitted the final marker batch"
fi

phase "Phase 6: Port-forward"
k port-forward -n "$NAMESPACE" svc/blackhole-receiver 14318:4318 &
PF_PID=$!

# Wait for port-forward to accept connections instead of hardcoded sleep.
if ! wait_for "port-forward accepting connections" 15 \
    curl --connect-timeout 1 --max-time 1 -sf http://localhost:14318/stats; then
    fail_and_exit "PORT_FORWARD_NOT_READY" "port-forward did not start serving /stats in time"
fi

phase "Phase 7: Verify"
DEADLINE=$((SECONDS + TIMEOUT))
DELAY=2
while [ $SECONDS -lt $DEADLINE ]; do
    if ! kill -0 "$PF_PID" 2>/dev/null; then
        fail_and_exit "PORT_FORWARD_DROPPED" "port-forward exited during verification (pid=$PF_PID)"
    fi

    LAST_STATS="$(curl --connect-timeout 1 --max-time 2 -sf http://localhost:14318/stats 2>/dev/null || echo '{}')"
    LAST_LINES="$(echo "$LAST_STATS" | sed -n 's/.*"lines":\([0-9]*\).*/\1/p')"
    LAST_LINES="${LAST_LINES:-0}"
    record_stats_trace

    echo "  blackhole /stats: lines=$LAST_LINES (want ${MIN_EXPECTED_LINES}..${MAX_EXPECTED_LINES})"

    if [ "$LAST_LINES" -ge "$MIN_EXPECTED_LINES" ] && [ "$LAST_LINES" -le "$MAX_EXPECTED_LINES" ]; then
        echo ""
        echo "PASS: blackhole received $LAST_LINES lines (expected ${MIN_EXPECTED_LINES}..${MAX_EXPECTED_LINES})"
        exit 0
    elif [ "$LAST_LINES" -gt "$MAX_EXPECTED_LINES" ]; then
        fail_and_exit "TOO_MANY_LINES" "blackhole received $LAST_LINES lines (> max expected $MAX_EXPECTED_LINES)"
    fi

    # Exponential backoff: 2s → 3s → 4s → 5s (capped)
    sleep "$DELAY"
    DELAY=$(( DELAY < 5 ? DELAY + 1 : 5 ))
done

fail_and_exit "VERIFY_TIMEOUT" "timed out after ${TIMEOUT}s — blackhole received $LAST_LINES lines, expected ${MIN_EXPECTED_LINES}..${MAX_EXPECTED_LINES}"
