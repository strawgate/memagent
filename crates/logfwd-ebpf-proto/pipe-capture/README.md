# pipe-capture: eBPF container log capture prototype

Captures container stdout/stderr by intercepting `vfs_write` in the kernel via eBPF kprobe. Bypasses the CRI log file path entirely.

## Requirements

- Linux kernel 5.8+ (ring buffer support)
- cgroup v2
- CAP_BPF or root
- Rust nightly + `rust-src` component
- `bpf-linker` (`cargo install bpf-linker`)

## Build

```bash
# Build eBPF kernel program
cd pipe-capture-ebpf
rustup component add rust-src --toolchain nightly
cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release

# Build userspace loader
cd ..
cargo build --release
```

## Run

```bash
# Start capture (writes to /tmp/ebpf-capture.log)
sudo ./target/release/pipe-capture target/bpfel-unknown-none/release/pipe-capture

# In another terminal, generate container output
docker run --rm alpine sh -c 'for i in $(seq 1 1000); do echo hello-$i; done'
```

## Test results (GCE e2-small, kernel 6.17)

| Lines | Events | Throughput | Data loss |
|-------|--------|------------|-----------|
| 10K   | 38K    | 12K/s      | None      |
| 100K  | 287K   | 53K/s      | None      |
| 500K  | 1.37M  | 81K/s      | None      |
| 1M    | 2.63M  | 81K/s      | None*     |

*Events > lines because we capture both the container pipe write and containerd-shim's file write.

## Architecture

```
Container write(stdout)
    → vfs_write()
        → kprobe fires → filter by PID → copy to ring buffer
                                              → userspace reads → output file
    → pipe_write() → containerd-shim reads → CRI log file (still happens)
```
