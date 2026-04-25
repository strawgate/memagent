---
title: "Docker Deployment"
description: "Run FastForward as a standalone container"
---

Use this page when you want to run FastForward as a standalone container on a single host.
For Kubernetes clusters, see [Kubernetes deployment](/deployment/kubernetes/).

:::tip[Safe defaults]
Start with these settings for predictable behavior:

- Mount `/var/log` read-only.
- Mount `config.yaml` read-only.
- Enable diagnostics (`server.diagnostics: 0.0.0.0:9090`).
- Use OTLP compression (`compression: zstd`) for remote collectors.
- Mount a named volume for checkpoint persistence (see below).
- Set resource constraints so FastForward cannot starve the host.
:::

## Quick start

```bash
docker run -d \
  --name ff \
  -v /var/log:/var/log:ro \
  -v ./config.yaml:/etc/ff/config.yaml:ro \
  -v ff-data:/var/lib/ffwd \
  -p 9090:9090 \
  --cpus 1.0 \
  --memory 256m \
  ghcr.io/strawgate/fastforward:latest \
  run --config /etc/ff/config.yaml
```

### Flag breakdown

| Flag | Purpose |
|------|---------|
| `-v /var/log:/var/log:ro` | Gives FastForward read-only access to host log files |
| `-v ./config.yaml:/etc/ff/config.yaml:ro` | Injects your pipeline configuration (read-only) |
| `-v ff-data:/var/lib/ffwd` | Persists checkpoint data between container restarts |
| `-p 9090:9090` | Exposes the diagnostics/admin API on the host |
| `--cpus 1.0` | Limits the container to one CPU core |
| `--memory 256m` | Hard memory cap; the OOM killer fires if exceeded |

## Checkpoint persistence

:::tip[Always mount a checkpoint volume]
Without a persistent volume for `/var/lib/ffwd`, FastForward loses its file-read
position on every container restart. This causes **duplicate log delivery**
(re-reading from the beginning) or **data loss** (if the output has
already acknowledged earlier batches). A Docker named volume or a host-path
bind mount eliminates both problems.
:::

```bash
# Named volume (recommended — Docker manages the lifecycle)
-v ff-data:/var/lib/ffwd

# Host-path bind mount (useful when you need direct access to checkpoint files)
-v /opt/ffwd/data:/var/lib/ffwd
```

Your configuration must reference the same directory:

```yaml
storage:
  data_dir: /var/lib/ffwd
```

## Resource constraints

For most single-host deployments, one CPU core and 256 MB of memory are a
reasonable starting point. Increase these if your host generates a high volume
of logs or if you run complex SQL transforms.

```bash
# Conservative — suitable for light-to-moderate log volumes
docker run -d --cpus 0.5 --memory 128m ...

# Production — high-throughput hosts (>10 k lines/s)
docker run -d --cpus 2.0 --memory 512m ...
```

Monitor `ffwd_stage_seconds_total` (metric prefix will change in a future release) and container memory usage via `docker stats`
to decide whether you need to adjust. See [Monitoring & Diagnostics](/deployment/monitoring/)
for details on available metrics.

## Environment variable passthrough

Use `-e` flags to inject secrets or endpoint addresses without baking them into
the configuration file. FastForward interpolates `${VAR}` references in YAML values
at startup.

```bash
docker run -d \
  --name ff \
  -e OTEL_ENDPOINT=https://collector.internal:4318 \
  -e OTEL_TOKEN=my-secret-token \
  -v /var/log:/var/log:ro \
  -v ./config.yaml:/etc/ff/config.yaml:ro \
  -v ff-data:/var/lib/ffwd \
  -p 9090:9090 \
  --cpus 1.0 \
  --memory 256m \
  ghcr.io/strawgate/fastforward:latest \
  run --config /etc/ff/config.yaml
```

Then reference them in `config.yaml`:

```yaml
output:
  type: otlp
  endpoint: "${OTEL_ENDPOINT}"
  compression: zstd
  auth:
    bearer_token: "${OTEL_TOKEN}"
```

## Network inputs with port mapping

When FastForward receives logs over TCP or UDP (instead of tailing files), you need
to publish the listener ports.

```bash
docker run -d \
  --name ff \
  -v ./config.yaml:/etc/ff/config.yaml:ro \
  -v ff-data:/var/lib/ffwd \
  -p 9090:9090 \
  -p 5140:5140/tcp \
  -p 5140:5140/udp \
  --cpus 1.0 \
  --memory 256m \
  ghcr.io/strawgate/fastforward:latest \
  run --config /etc/ff/config.yaml
```

Matching input configuration:

```yaml
pipelines:
  syslog-tcp:
    inputs:
      - type: tcp
        listen: 0.0.0.0:5140
        format: raw

  syslog-udp:
    inputs:
      - type: udp
        listen: 0.0.0.0:5140
        format: raw
```

:::caution
Binding to `0.0.0.0` inside the container is required for Docker port mapping
to work. If you bind to `127.0.0.1`, external traffic will not reach FastForward.
:::

## Docker Compose

A Compose file is the easiest way to run FastForward alongside an OpenTelemetry
Collector on the same host. The example below tails host logs, forwards them
over OTLP to the collector sidecar, and exposes the diagnostics API.

```yaml
# docker-compose.yml
services:
  ff:
    image: ghcr.io/strawgate/fastforward:latest
    command: ["run", "--config", "/etc/ff/config.yaml"]
    volumes:
      - /var/log:/var/log:ro
      - ./config.yaml:/etc/ff/config.yaml:ro
      - ff-data:/var/lib/ffwd
    ports:
      - "9090:9090"
    environment:
      - OTEL_ENDPOINT=http://otel-collector:4318
    deploy:
      resources:
        limits:
          cpus: "1.0"
          memory: 256M
    depends_on:
      - otel-collector
    restart: unless-stopped

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config", "/etc/otelcol/config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol/config.yaml:ro
    ports:
      - "4317:4317"   # gRPC receiver
      - "4318:4318"   # HTTP receiver
    restart: unless-stopped

volumes:
  ff-data:
```

Start the stack:

```bash
docker compose up -d
docker compose logs -f ff
```

## Validate container health

```bash
# Process and startup logs
docker ps --filter name=ff
docker logs --tail=100 ff

# Diagnostics
curl -s http://localhost:9090/live | jq .
curl -s http://localhost:9090/admin/v1/status | jq .
```

You should see increasing `inputs[*].lines_total` and `outputs[*].lines_total` when logs are present.

To verify the OTLP output is connected:

```bash
# Check that the output reports no errors
curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[].output'
```

## Rollback

If a config/image update causes failures:

```bash
# Stop and remove current container
docker rm -f ff

# Run last known-good image or config
docker run -d \
  --name ff \
  -v /var/log:/var/log:ro \
  -v ./config.last-known-good.yaml:/etc/ff/config.yaml:ro \
  -v ff-data:/var/lib/ffwd \
  -p 9090:9090 \
  ghcr.io/strawgate/fastforward:<known-good-tag> \
  run --config /etc/ff/config.yaml
```

Then validate with the same diagnostics commands above.

:::tip
Because checkpoint data is stored in the `ff-data` volume, rolling back
the image or configuration does not lose read position. FastForward resumes where
it left off.
:::

## Dockerfile

The release workflow builds multi-arch images for `linux/amd64` and `linux/arm64` using a distroless base image.
See `Dockerfile` and `.github/workflows/release.yml` for details.

## What's next

- [Monitoring & Diagnostics](/deployment/monitoring/) -- health probes, metrics, and the built-in dashboard.
- [Input Types](/configuration/inputs/) -- configure file, TCP, and UDP inputs.
- [Output Types](/configuration/outputs/) -- OTLP and other output options.
- [SQL Transforms](/configuration/sql-transforms/) -- filter and reshape logs before they leave the host.
- [Kubernetes Deployment](/deployment/kubernetes/) -- scale out to a cluster with a DaemonSet.
- [Troubleshooting](/troubleshooting/) -- common issues and diagnostic steps.
