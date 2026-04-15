#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {
        "scenario": "kind-cri-smoke",
        "source_id": "log-generator",
        "event_id": f"kind-cri-smoke:{i:04d}",
        "seq": i,
        "level": level,
        "message": f"KIND_E2E_MARKER_{i:03d}",
    }
    for i, level in enumerate(["INFO", "INFO", "WARN", "INFO", "ERROR", "INFO", "DEBUG", "INFO", "WARN", "INFO"], start=1)
]
print(json.dumps(rows, indent=2))
PY

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete pod log-generator --ignore-not-found >/dev/null 2>&1 || true
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" apply -f "$SCENARIO_DIR/manifests/log-generator.yaml"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait pod/log-generator --for=condition=Ready --timeout=60s
sleep 5
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" logs log-generator >"$E2E_RESULTS_DIR/log-generator-source.ndjson"
python3 "$REPO_ROOT/tests/e2e/lib/source_evidence.py" \
    --mode json-lines \
    --input "$E2E_RESULTS_DIR/log-generator-source.ndjson" \
    --output "$E2E_RESULTS_DIR/source_rows.json" \
    --scenario "$SCENARIO_ID" \
    --source-id "log-generator"
