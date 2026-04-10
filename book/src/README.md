# logfwd

A high-performance log forwarder built in Rust. Tails log files, parses JSON and Kubernetes CRI format with portable SIMD, transforms every batch with DataFusion SQL, and ships to OTLP, Elasticsearch, Loki, file, or stdout.

```
log files → SIMD parse → Arrow RecordBatch → SQL transform → output
```

logfwd is a single static binary with no runtime dependencies. Point it at log files, write a SQL query to filter and reshape the data, and forward the results to any OTLP-compatible collector — or directly to Elasticsearch, Loki, file, or stdout. SQL transforms are the core idea: instead of learning a vendor-specific DSL, you write standard SQL to control exactly what gets shipped.

## Get started

1. **[Installation](./getting-started/installation.md)** — Binary download, Docker, or build from source
2. **[Quick Start](./getting-started/quickstart.md)** — Working pipeline in about 10 minutes, no external dependencies
3. **[Your First Pipeline](./getting-started/first-pipeline.md)** — Production config with monitoring and validation

## Configure

- **[Configuration Reference](./config/reference.md)** — All YAML fields, input/output types, enrichment
- **[Input Types](./config/inputs.md)** — File, TCP, UDP, OTLP receiver, HTTP ingest, generator
- **[Output Types](./config/outputs.md)** — OTLP, Elasticsearch, Loki, file, stdout, and development sinks
- **[SQL Transforms](./config/sql-transforms.md)** — Filter, reshape, extract — full DataFusion SQL

## Deploy

- **[Kubernetes DaemonSet](./deployment/kubernetes.md)** — Manifest, resource sizing, CRI format
- **[Docker](./deployment/docker.md)** — Container images and compose files
- **[Monitoring](./deployment/monitoring.md)** — Diagnostics API, Prometheus metrics, health checks

## Learn more

- **[Pipeline Design](./architecture/pipeline.md)** — How data flows from input to output
- **[Scanner](./architecture/scanner.md)** — How the parser works
- **[Performance](./architecture/performance.md)** — Benchmarks and tuning
- **[Troubleshooting](./troubleshooting.md)** — Common errors and how to fix them
