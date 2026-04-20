---
title: "Kubernetes Deployment"
description: "Deploy FastForward as a DaemonSet with OTLP forwarding"
---

This page is the Kubernetes-specific production deployment guide for FastForward.
For standalone container usage, see [Docker deployment](/deployment/docker/).

:::tip[Production safe defaults]
Use these defaults unless you have measured reasons to change them:

- Run as a DaemonSet with one pod per node.
- Read host logs from `/var/log` as `readOnly: true`.
- Set OTLP compression to `zstd` for lower egress cost.
- Enable diagnostics endpoint (`server.diagnostics`) for triage.
- Start with resource requests of `cpu: 250m`, `memory: 128Mi` and tune from observed load.
- Keep transform filters conservative first, then tighten once observability confirms behavior.

These defaults prioritize stability and debuggability over premature optimization.
:::

## DaemonSet

A DaemonSet is the recommended way to deploy FastForward in a Kubernetes cluster. Each
node runs one FastForward pod that reads container logs from `/var/log` on the host.

A ready-to-use manifest is provided at `deploy/daemonset.yml`.

:::note[CRI field requirement]
In the file-input example below, `_stream` is only present when the input is
parsed as CRI (`format: cri`). The `_timestamp` column is present here because
CRI parsing injects it for this example, but `_timestamp` may also be provided
by other inputs or formats. If you switch to a different input format and these
columns are not available, remove them or update the query accordingly.
:::

### Minimal DaemonSet

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: collectors
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: logfwd
  namespace: collectors
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: logfwd-config
  namespace: collectors
data:
  config.yaml: |
    input:
      type: file
      path: /var/log/pods/**/*.log
      format: cri

    transform: |
      SELECT
        level,
        message,
        _timestamp,
        _stream
      FROM logs
      WHERE level != 'DEBUG'

    output:
      type: otlp
      endpoint: ${OTEL_ENDPOINT}
      protocol: grpc
      compression: zstd

    server:
      diagnostics: 0.0.0.0:9090
      log_level: info
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: logfwd
  namespace: collectors
  labels:
    app: logfwd
spec:
  selector:
    matchLabels:
      app: logfwd
  template:
    metadata:
      labels:
        app: logfwd
    spec:
      serviceAccountName: logfwd
      tolerations:
        - operator: Exists  # run on all nodes including control-plane
      containers:
        - name: logfwd
          image: ghcr.io/strawgate/memagent:latest
          imagePullPolicy: IfNotPresent
          args:
            - run
            - --config
            - /etc/logfwd/config.yaml
          env:
            - name: OTEL_ENDPOINT
              value: http://otel-collector.monitoring.svc.cluster.local:4317
          ports:
            - name: diagnostics
              containerPort: 9090
          resources:
            requests:
              cpu: "250m"
              memory: "128Mi"
            limits:
              cpu: "1"
              memory: "512Mi"
          volumeMounts:
            - name: varlog
              mountPath: /var/log
              readOnly: true
            - name: config
              mountPath: /etc/logfwd
              readOnly: true
      volumes:
        - name: varlog
          hostPath:
            path: /var/log
        - name: config
          configMap:
            name: logfwd-config
```

Apply it:

```bash
kubectl apply -f deploy/daemonset.yml
kubectl -n collectors rollout status daemonset/logfwd
```

### Validate rollout

```bash
# Pod health
kubectl -n collectors get pods -l app=logfwd -o wide

# Runtime logs
kubectl -n collectors logs daemonset/logfwd --tail=100

# Diagnostics endpoint (port-forward one pod)
POD=$(kubectl -n collectors get pods -l app=logfwd -o jsonpath='{.items[0].metadata.name}')
kubectl -n collectors port-forward "$POD" 9090:9090
curl -s http://localhost:9090/admin/v1/status | jq .
```

### Rollback

If a new deployment causes dropped logs or sustained output errors, revert quickly:

```bash
# Roll back DaemonSet to previous revision
kubectl -n collectors rollout undo daemonset/logfwd

# Verify rollback completion
kubectl -n collectors rollout status daemonset/logfwd

# Confirm forwarding resumes
kubectl -n collectors logs daemonset/logfwd --tail=100
```


### Kubernetes metadata enrichment

Use the `k8s_path` enrichment table to attach namespace, pod, and container labels to
every log record:

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: ecs

enrichment:
  - type: k8s_path
    table_name: k8s

transform: |
  SELECT
    l.level,
    l.message,
    k.namespace,
    k.pod_name,
    k.container_name
  FROM logs l
  LEFT JOIN k8s k ON l."file.path" = k.log_path_prefix
```

### Namespace filtering

To collect logs only from specific namespaces, filter in the transform:

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: ecs

transform: |
  SELECT l.*, k.namespace, k.pod_name, k.container_name
  FROM logs l
  LEFT JOIN k8s k ON l."file.path" = k.log_path_prefix
  WHERE k.namespace IN ('production', 'staging')
```

### Scraping the diagnostics endpoint with Prometheus

Expose port 9090 in the pod spec to make the diagnostics API reachable from
within the cluster.

To scrape `/admin/v1/status`, configure a Prometheus adapter (such as
`json_exporter`) that converts the JSON response into Prometheus metrics, or
query the endpoint directly in your monitoring stack.

---

## OTLP collector integration

FastForward sends log records as OTLP protobuf. Any OpenTelemetry-compatible collector
can receive them.

### OpenTelemetry Collector

Add a `otlp` receiver to your collector config and enable the `logs` pipeline:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  debug:
    verbosity: normal
  otlphttp/loki:
    endpoint: http://loki:3100/otlp

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [debug, otlphttp/loki]
```

Point FastForward at the collector:

```yaml
output:
  type: otlp
  endpoint: http://otel-collector:4317
  protocol: grpc
  compression: zstd
```

### Grafana Alloy / Agent

Grafana Alloy can receive OTLP logs and forward them to Loki or Tempo. Configure an
`otelcol.receiver.otlp` component and connect it to your exporter pipeline.

---

## Resource sizing guidelines

FastForward is designed to process logs on a single CPU core. The pipeline runs as a set
of blocking OS threads — one per input plus shared coordinator threads.

### Baseline

| Scenario | CPU | Memory |
|----------|-----|--------|
| Quiet node (< 1 MB/s) | 50 m | 64 Mi |
| Typical node (1–10 MB/s) | 250 m | 128 Mi |
| High-throughput node (> 10 MB/s) | 500 m – 1 CPU | 256 Mi |

### Memory breakdown

| Component | Typical size |
|-----------|-------------|
| Arrow RecordBatch (per batch, 8 KB read) | ~512 Ki |
| DataFusion query plan | ~4 Mi |
| OTLP request buffer | ~2 Mi |
| Enrichment tables | < 1 Mi |
| Per-pipeline overhead | ~16 Mi |

### Tuning tips

- **Reduce memory**: avoid enabling input line capture unless needed (`line_field`) — it stores the full
  JSON line and accounts for up to 65 % of table memory.
- **Reduce CPU**: use a `WHERE` clause in the transform to drop unwanted records early.
- **Multiple pipelines**: each pipeline occupies its own thread. Add CPU budget
  proportionally.

### Kubernetes resource example

```yaml
resources:
  requests:
    cpu: "250m"
    memory: "128Mi"
  limits:
    cpu: "1"
    memory: "512Mi"
```

Set the limit higher than the request so FastForward can burst during log spikes without
being OOM-killed.

---

## Validating before deploy

Use `validate` to parse and validate the config without starting the pipeline:

```bash
logfwd validate --config config.yaml
```

Use `dry-run` to build all pipeline objects without starting them (catches errors
such as SQL syntax issues):

```bash
logfwd dry-run --config config.yaml
```

Both commands exit 0 on success and print an error to stderr on failure.
