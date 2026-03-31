# logfwd

A high-performance log forwarder. Tails log files, parses JSON and CRI container format, transforms with SQL, and ships to OTLP, HTTP, or stdout.

## Quick Start (Interactive Benchmark)

You can try `logfwd` locally using its built-in test data generator and "blackhole" collector.

1. **Build the binary**:
   ```bash
   cargo build --release -p logfwd
   ```

2. **Generate test logs**:
   ```bash
   ./target/release/logfwd --generate-json 100000 logs.json
   ```

3. **Start the OTLP blackhole** (collector that discards data):
   ```bash
   ./target/release/logfwd --blackhole &
   ```

4. **Run a filtered pipeline**:
   Create `config.yaml`:
   ```yaml
   input:
     type: file
     path: logs.json  # paths may be relative to this config file
     format: json

   transform: |
     SELECT * FROM logs WHERE duration_ms_int > 50

   output:
     type: otlp
     endpoint: http://127.0.0.1:4318
     compression: zstd
   ```

   Run the forwarder:
   ```bash
   ./target/release/logfwd --config config.yaml
   ```

## Configuration

logfwd uses YAML configuration with two modes:

**Simple** (single pipeline):
```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

transform: SELECT level_str, msg_str, status_int FROM logs WHERE status_int >= 400

output:
  type: otlp
  endpoint: http://otel-collector:4318
```

**Advanced** (multiple pipelines):
```yaml
pipelines:
  errors:
    input:
      type: file
      path: /var/log/pods/**/*.log
      format: cri
    transform: SELECT * FROM logs WHERE level_str = 'ERROR'
    output:
      type: otlp
      endpoint: http://otel-collector:4318

  all-logs:
    input:
      type: file
      path: /var/log/pods/**/*.log
      format: cri
    output:
      type: stdout
      format: json
```

## SQL Column Naming

All JSON fields are automatically suffixed with their type (`_str`, `_int`, `_float`) to ensure schema stability. 

```sql
-- Filter by level
SELECT * FROM logs WHERE level_str = 'ERROR'

-- Extract integer fields
SELECT status_int, duration_ms_int FROM logs
```

For more details, see [docs/COLUMN_NAMING.md](docs/COLUMN_NAMING.md).

## CLI

```
logfwd --config <config.yaml>        Run pipeline
logfwd --config <config.yaml> --check      Validate config without running
logfwd --blackhole [bind_addr]       OTLP collector for benchmarks
logfwd --generate-json <n> <file>    Generate synthetic test data
logfwd --version                     Print version
```

## Documentation

| Guide | Description |
|-------|-------------|
| [docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md) | All YAML fields, input/output types, SQL transforms, UDFs, enrichment |
| [docs/COLUMN_NAMING.md](docs/COLUMN_NAMING.md) | How JSON fields map to SQL columns (suffixes) |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Docker, Kubernetes DaemonSet, resource sizing, OTLP integration |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common errors, absolute paths, /api/pipelines, debug mode |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Pipeline data flow, crate map, scanner internals |
| [DEVELOPING.md](DEVELOPING.md) | Build, test, lint, bench commands; hard-won implementation notes |
