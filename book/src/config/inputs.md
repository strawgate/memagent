# Input Types

## File

Tail one or more log files with glob pattern support.

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri      # cri | json | raw | auto
```

- **Glob re-scanning**: New files matching the pattern are discovered automatically (every 5s).
- **Rotation handling**: Detects file rotation (rename + create) and switches to the new file. Drains remaining data from the old file before switching.
- **Formats**: CRI (Kubernetes container runtime), JSON (newline-delimited), raw (plain text, each line becomes `{"_raw": "..."}`)

## Generator

Emit synthetic JSON log lines for benchmarking and pipeline testing. No external
data source is required.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `listen` | string | No | unlimited | Target events per second, e.g. `"50000"`. |

```yaml
input:
  type: generator
  listen: "50000"   # ~50,000 events/sec; omit for unlimited
```

Use `--generate-json <n> <file>` on the CLI to write a fixed number of lines to a file instead.

## UDP / TCP / OTLP (planned)

Network input sources are defined in the config schema but not yet implemented.
