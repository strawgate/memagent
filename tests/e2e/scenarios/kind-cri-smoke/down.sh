#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"

kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
if [[ -f "$E2E_RESULTS_DIR/cluster-created" ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
fi
