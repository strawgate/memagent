# logfwd

A high-performance log forwarder that tails log files, parses JSON and CRI container format, transforms with SQL, and ships to OTLP, HTTP, or stdout.

> **Target: 1M+ lines/sec** on a single core with zero-copy SIMD scanning, Apache Arrow columnar processing, and DataFusion SQL transforms.

## Quick start

```bash
cargo build --release -p logfwd
./target/release/logfwd --config config.yaml
```

```yaml
pipelines:
  errors:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    transform: "SELECT * FROM logs WHERE level_str = 'ERROR'"
    outputs:
      - type: otlp
        endpoint: otel-collector:4317
```

## What makes logfwd different

- **SQL transforms** — filter, rename, aggregate with full DataFusion SQL instead of custom DSLs
- **SIMD scanning** — zero-copy JSON-to-Arrow parsing using SIMD instructions
- **Arrow columnar** — Apache Arrow RecordBatch as the universal pipeline format
- **Kubernetes native** — CRI log format, pod metadata enrichment via path parsing

## Documentation

| Section | What you'll find |
|---------|-----------------|
| [Getting Started](./getting-started/installation.md) | Install, configure, run your first pipeline |
| [Configuration](./config/reference.md) | All YAML fields, input/output types, SQL transforms, UDFs |
| [Deployment](./deployment/kubernetes.md) | Kubernetes DaemonSet, Docker, monitoring |
| [Architecture](./architecture/pipeline.md) | Pipeline internals, SIMD scanner, performance design |
| [Development](./development/contributing.md) | Build, test, benchmark, contribute |

## Links

- [GitHub Repository](https://github.com/strawgate/memagent)
- [Benchmark Dashboard](https://strawgate.com/memagent/)
- [API Documentation](./api/logfwd/index.html)
