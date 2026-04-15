#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"

kubectl --context "$KUBE_CONTEXT" get pods -n "$NAMESPACE" -o wide >"$E2E_RESULTS_DIR/pods.txt" 2>&1 || true
kubectl --context "$KUBE_CONTEXT" get events -n "$NAMESPACE" --sort-by='.lastTimestamp' >"$E2E_RESULTS_DIR/events.txt" 2>&1 || true
kubectl --context "$KUBE_CONTEXT" logs -n "$NAMESPACE" -l app=logfwd --tail=-1 >"$E2E_RESULTS_DIR/logfwd.log" 2>&1 || true
kubectl --context "$KUBE_CONTEXT" logs -n "$NAMESPACE" -l app=capture-receiver --tail=-1 >"$E2E_RESULTS_DIR/capture.log" 2>&1 || true
kubectl --context "$KUBE_CONTEXT" logs -n "$NAMESPACE" log-generator --tail=-1 >"$E2E_RESULTS_DIR/log-generator.log" 2>&1 || true
