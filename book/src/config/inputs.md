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
| `listen` | string | No | (unlimited) | Target events per second, e.g. `"50000"`. Omit for maximum throughput. |

```yaml
input:
  type: generator
  listen: "50000"   # ~50,000 events/sec; omit for unlimited
```

Use `--generate-json <num_lines> <output_file>` on the CLI to write a fixed number of lines to a file instead.

## UDP

Receive log lines on a UDP socket.

```yaml
input:
  type: udp
  listen: 0.0.0.0:514
  format: json
```

## TCP

Accept log lines on a TCP socket.

```yaml
input:
  type: tcp
  listen: 0.0.0.0:5140
  format: json
```

## OTLP

Receive OTLP log records from another agent or SDK.

```yaml
input:
  type: otlp
```
