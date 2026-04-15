#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"

if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    kind create cluster --name "$CLUSTER_NAME" --wait 60s
    printf 'created\n' >"$E2E_RESULTS_DIR/cluster-created"
fi

kind load docker-image logfwd:e2e --name "$CLUSTER_NAME"

kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
kubectl --context "$KUBE_CONTEXT" create namespace "$NAMESPACE"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" create configmap capture-tcp-script \
    --from-file=capture_tcp.py="$REPO_ROOT/tests/e2e/lib/capture_tcp.py"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f "$SCENARIO_DIR/manifests/capture-receiver.yaml"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f "$SCENARIO_DIR/manifests/logfwd-config.yaml"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f "$SCENARIO_DIR/manifests/logfwd-daemonset.yaml"

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait deployment/capture-receiver --for=condition=available --timeout=120s
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status daemonset/logfwd --timeout=120s
