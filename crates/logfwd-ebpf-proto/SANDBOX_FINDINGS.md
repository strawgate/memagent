# eBPF Sandbox Exploration Findings

## Environment

- **Kernel**: 6.18.5 (custom, GCE/Coder)
- **Capabilities**: root with `cap_bpf`, `cap_sys_admin`, `cap_sys_module`
- **Toolchain**: clang-18, llvm-18, Rust stable + nightly, libbpf-dev

## Key Kernel Config

| Config | Value | Impact |
|--------|-------|--------|
| `CONFIG_BPF_SYSCALL` | **y** | BPF syscall works |
| `CONFIG_BPF_JIT` | **not set** | BPF runs in interpreter (slower, but works) |
| `CONFIG_KPROBES` | **not set** | Cannot attach to kernel functions |
| `CONFIG_FTRACE` | **not set** | No fentry/fexit, no tracing infrastructure |
| `CONFIG_UPROBES` | **not set** | Cannot attach to userspace functions |
| `CONFIG_CGROUP_BPF` | **y** | Cgroup BPF programs work |
| `CONFIG_BPF_STREAM_PARSER` | **y** | Stream parser available |
| `CONFIG_XDP_SOCKETS` | **y** | XDP works |
| `CONFIG_PERF_EVENTS` | **y** | Perf events exist but BPF_PROG_TYPE_PERF_EVENT fails |
| BTF (`/sys/kernel/btf/vmlinux`) | **absent** | No CO-RE, no fentry |

## BPF Program Types

### Working
- `SOCKET_FILTER` (1) - filter packets on sockets
- `SCHED_CLS` (3) - TC classifier
- `SCHED_ACT` (4) - TC action
- `XDP` (6) - eXpress Data Path
- `CGROUP_SKB` (8) - cgroup socket buffer
- `CGROUP_SOCK_ADDR` (14) - cgroup socket address
- `STRUCT_OPS` (21) - struct operations

### Not Working
- `KPROBE` (2) - needs CONFIG_KPROBES
- `TRACEPOINT` (5) - needs CONFIG_FTRACE
- `PERF_EVENT` (7) - needs tracing infrastructure
- `RAW_TRACEPOINT` (17) - needs CONFIG_FTRACE
- `TRACING` (26) - needs CONFIG_FTRACE + BTF
- `LSM` (27) - needs CONFIG_BPF_LSM
- `SYSCALL` (29) - needs CONFIG_FTRACE

## BPF Map Types

### Working
- `HASH` (1)
- `ARRAY` (2)
- `PERCPU_ARRAY` (6)
- `RINGBUF` (27) - full data path confirmed (see below)

### Not Working
- `INODE_STORAGE` (28)
- `TASK_STORAGE` (29)

## Can We Intercept File Writes?

**No.** Every technique for intercepting `write()` / `vfs_write()` / `pipe_write()`
requires kernel features that are disabled:

| Technique | Requirement | Status |
|-----------|------------|--------|
| kprobe on `vfs_write` | CONFIG_KPROBES | not set |
| kprobe on `pipe_write` | CONFIG_KPROBES | not set |
| tracepoint `sys_enter_write` | CONFIG_FTRACE + CONFIG_KPROBES | not set |
| fentry/fexit | CONFIG_FTRACE + BTF | not set |
| uprobe on libc `write()` | CONFIG_UPROBES (needs KPROBES) | not set |
| BPF LSM hook | CONFIG_BPF_LSM | not set |
| seccomp-bpf notify | Different mechanism, not eBPF | not applicable |

## What We CAN Do

1. **Full BPF lifecycle** - create maps, load programs, attach to sockets
2. **Ring buffer data path** - create ringbuf, reserve/submit from BPF, mmap from userspace
3. **Network filtering** - socket filters, XDP, TC classifiers
4. **Cgroup network policy** - CGROUP_SKB, CGROUP_SOCK_ADDR
5. **Compile BPF C programs** - clang -target bpf works
6. **Build Aya userspace** - Rust aya crate for loading/managing BPF programs

## Ring Buffer mmap Layout (Discovered)

The BPF ring buffer has a 3-section mmap layout, not the 2-section layout
commonly documented:

```
offset 0:              consumer page (consumer_pos u64, read-write)
offset PAGE_SIZE:      producer page (producer_pos u64, read-only)
offset 2 * PAGE_SIZE:  data pages (2x ring size for wrap-around, read-only)
```

Each entry in the data pages has:
- 8-byte header: `u32 len_flags` (bits 31=BUSY, 30=DISCARD, 0-27=length) + `u32 pad`
- Data payload (aligned to 8 bytes)

Consumer reads from `consumer_pos` up to `producer_pos`, checking the BUSY bit
on each header. After consuming, write new `consumer_pos` back to the consumer page.

## Verified Working Data Path

The following was tested end-to-end in this sandbox:

1. Create `BPF_MAP_TYPE_RINGBUF` (256KB)
2. Load socket filter program that calls `bpf_ringbuf_output` (helper 130)
3. Attach to `AF_PACKET` socket on loopback
4. Send UDP packets -> BPF executes -> events written to ring buffer
5. mmap all 3 sections -> read producer_pos -> parse events -> advance consumer_pos
6. **Result: 40 events captured with correct payload (0xDEAD, 0xBEEF)**

Also verified: `bpf_map_lookup_elem` works from socket filter context for
shared counters (40 increments matching 40 packets).

**Note**: `bpf_ringbuf_reserve` + `bpf_ringbuf_submit` (helpers 131/132) load and
verify successfully but produce no events in interpreter mode (no JIT). Use
`bpf_ringbuf_output` (helper 130) instead, which copies stack data to the ring
buffer and works without JIT.

## Recommendations

1. **For write interception development**: Build and unit-test the userspace loader
   and event processing pipeline in this sandbox. The BPF kernel program (kprobe on
   vfs_write) must be tested on a kernel with CONFIG_KPROBES=y.

2. **For sandbox iteration**: Use SOCKET_FILTER programs as a testbed for the ring
   buffer data path, event parsing, and Aya integration. The userspace ring buffer
   consumption code is identical regardless of program type.

3. **Use `bpf_ringbuf_output` not `bpf_ringbuf_reserve`+`bpf_ringbuf_submit`** in
   environments without BPF JIT. The reserve/submit pair verifies but doesn't
   produce events in interpreter mode.

4. **Minimum kernel requirements for pipe-capture**:
   - CONFIG_KPROBES=y
   - CONFIG_BPF_SYSCALL=y
   - CONFIG_BPF_JIT=y (recommended for performance, required for reserve/submit)
   - /sys/kernel/btf/vmlinux (for CO-RE / fentry, optional if using kprobe)
   - Linux 5.8+ (for ring buffer support)
