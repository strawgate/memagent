#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"
CAPTURE_POD="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods -l app=capture-receiver -o jsonpath='{.items[0].metadata.name}')"
CAPTURE_PATH="/artifacts/captured.ndjson"

deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
    if kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$CAPTURE_POD" -- test -s "$CAPTURE_PATH" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$CAPTURE_POD" -- cat "$CAPTURE_PATH" >"$E2E_RESULTS_DIR/captured.ndjson"

python3 - "$E2E_RESULTS_DIR/captured.ndjson" >"$E2E_RESULTS_DIR/stats.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()
print(json.dumps({"lines": len(lines), "bytes": path.stat().st_size}, indent=2))
PY

run_oracle_verify "$E2E_RESULTS_DIR/captured.ndjson"
