# Installation

## Binary download

Download the latest release from [GitHub Releases](https://github.com/strawgate/memagent/releases). Binaries are available for:

| Platform | Artifact |
|----------|----------|
| Linux x86_64 | `logfwd-linux-amd64` |
| Linux ARM64 | `logfwd-linux-arm64` |
| macOS x86_64 (Intel) | `logfwd-darwin-amd64` |
| macOS ARM64 (Apple Silicon) | `logfwd-darwin-arm64` |

```bash
# Example: Linux x86_64
curl -fsSL https://github.com/strawgate/memagent/releases/latest/download/logfwd-linux-amd64 -o logfwd
chmod +x logfwd
sudo mv logfwd /usr/local/bin/
```

## Docker

```bash
docker run --rm \
  -v /var/log:/var/log:ro \
  -v $(pwd)/config.yaml:/etc/logfwd/config.yaml:ro \
  ghcr.io/strawgate/memagent:latest --config /etc/logfwd/config.yaml
```

Images are published to `ghcr.io/strawgate/memagent` for `linux/amd64` and `linux/arm64`. See [Docker deployment](../deployment/docker.md) for compose files and volume configuration.

## From source

Requires the Rust stable toolchain (1.85+).

```bash
git clone https://github.com/strawgate/memagent.git
cd memagent
cargo build --release -p logfwd
```

The binary is at `./target/release/logfwd`. Copy it wherever you need:

```bash
sudo cp target/release/logfwd /usr/local/bin/
```

## Kubernetes

A ready-to-apply DaemonSet manifest is included in the repo:

```bash
kubectl apply -f deploy/daemonset.yml
```

See the [Kubernetes deployment guide](../deployment/kubernetes.md) for resource sizing and collector integration.

## Verify installation

```bash
logfwd --version
logfwd --help
```

## Next step

Head to the [Quick Start](./quickstart.md) to run your first pipeline.
