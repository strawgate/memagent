# Docker Deployment

Use this page when you want to run logfwd as a standalone container on a single host.

## Safe defaults

Start with these settings for predictable behavior:

- Mount `/var/log` read-only.
- Mount `config.yaml` read-only.
- Enable diagnostics (`server.diagnostics: 0.0.0.0:9090`).
- Use OTLP compression (`compression: zstd`) for remote collectors.

## Quick start

```bash
docker run -d \
  --name logfwd \
  -v /var/log:/var/log:ro \
  -v ./config.yaml:/etc/logfwd/config.yaml:ro \
  -p 9090:9090 \
  ghcr.io/strawgate/logfwd:latest \
  run --config /etc/logfwd/config.yaml
```

## Validate container health

```bash
# Process and startup logs
docker ps --filter name=logfwd
docker logs --tail=100 logfwd

# Diagnostics
curl -s http://localhost:9090/live | jq .
curl -s http://localhost:9090/admin/v1/status | jq .
```

You should see increasing `inputs[*].lines_total` and `outputs[*].lines_total` when logs are present.

## Rollback

If a config/image update causes failures:

```bash
# Stop and remove current container
docker rm -f logfwd

# Run last known-good image or config
docker run -d \
  --name logfwd \
  -v /var/log:/var/log:ro \
  -v ./config.last-known-good.yaml:/etc/logfwd/config.yaml:ro \
  -p 9090:9090 \
  ghcr.io/strawgate/logfwd:<known-good-tag> \
  run --config /etc/logfwd/config.yaml
```

Then validate with the same diagnostics commands above.

## Dockerfile

The release workflow builds multi-arch images for `linux/amd64` and `linux/arm64` using a distroless base image.
See `Dockerfile` and `.github/workflows/release.yml` for details.
