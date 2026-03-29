# pipe-capture: eBPF container log capture prototype

Captures container stdout/stderr by intercepting `vfs_write` in the kernel via eBPF kprobe. Bypasses the CRI log file path entirely.

## Development (Docker — recommended)

The fastest way to iterate. Edit files locally, build and run inside a privileged container. No Linux machine needed.

```bash
# One-time: build the dev image (~2 min)
docker build -t ebpf-dev -f Dockerfile.dev .

# Interactive shell (keep this open while developing)
docker run -it --rm --privileged \
  -v $(pwd):/src \
  -v /sys/kernel/debug:/sys/kernel/debug:ro \
  -v /sys/fs/bpf:/sys/fs/bpf \
  ebpf-dev bash

# Inside the container — build + run cycle (~17s incremental)
cd /src/pipe-capture-ebpf
cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release

cd /src
cargo build --release
./target/release/pipe-capture target/bpfel-unknown-none/release/pipe-capture /tmp/capture.log
```

Incremental builds take ~17 seconds. Edit on your host, rebuild in the container.

## Development (native Linux)

```bash
# Install deps
rustup toolchain install nightly
rustup component add rust-src --toolchain nightly
cargo install bpf-linker

# Build
cd pipe-capture-ebpf
cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release
cd ..
cargo build --release

# Run (requires root or CAP_BPF)
sudo ./target/release/pipe-capture target/bpfel-unknown-none/release/pipe-capture
```

## Testing

```bash
# In the container or on Linux — start the probe
./target/release/pipe-capture target/bpfel-unknown-none/release/pipe-capture /tmp/capture.log &

# Generate container output (from another shell or the host)
docker run --rm alpine sh -c 'for i in $(seq 1 10000); do echo LINE_$i; done'

# Check capture
wc -l /tmp/capture.log
grep -c LINE_ /tmp/capture.log
```

## Requirements

- **Docker** (for dev container) or **Linux kernel 5.8+** (native)
- cgroup v2 (default on Ubuntu 22.04+)
- `--privileged` flag or `CAP_BPF`

## Architecture

```text
Container write(stdout)
    → vfs_write()
        → kprobe fires → filter by PID → copy to 64MB ring buffer
                                              → userspace reads → output
    → pipe_write() → containerd-shim reads → CRI log file (still happens)
```

## Test results (kernel 6.10/6.17)

| Lines | Events captured | Throughput | Data loss |
|-------|----------------|------------|-----------|
| 10K   | 38K            | 12K/s      | None      |
| 100K  | 287K           | 53K/s      | None      |
| 500K  | 1.37M          | 81K/s      | None      |
| 1M    | 2.63M          | 81K/s      | None      |

Events > lines because we capture both the container's pipe write and containerd-shim's file write. With cgroup filtering (TODO), only the container writes would be captured.

See [#32](https://github.com/strawgate/memagent/issues/32) for roadmap and research notes.
