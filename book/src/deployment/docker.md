# Docker Deployment

## Quick start

```bash
docker run -v /var/log:/var/log:ro \
  -v ./config.yaml:/etc/logfwd/config.yaml:ro \
  ghcr.io/strawgate/memagent:latest \
  --config /etc/logfwd/config.yaml
```

## Dockerfile

The release workflow builds multi-arch images for `linux/amd64` and
`linux/arm64` using a distroless base image.

See `Dockerfile` and `.github/workflows/release.yml` for details.
