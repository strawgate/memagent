# eBPF Sandbox Findings

Kernel 6.18.5 (GCE/Coder), root with `cap_bpf` + `cap_sys_admin`, clang-18.

## What Works

| Feature | Status |
|---------|--------|
| BPF syscall, maps (hash, array, percpu_array, ringbuf) | Yes |
| SOCKET_FILTER, SCHED_CLS, SCHED_ACT, XDP, CGROUP_SKB | Yes |
| XDP attachment via BPF_LINK_CREATE | Yes |
| Ring buffer → mmap → userspace consumption | Yes |
| `bpf_ringbuf_output` (helper 130) | Yes |
| Compile BPF C with `clang -target bpf` | Yes |

## What Doesn't Work

| Feature | Why |
|---------|-----|
| kprobe/tracepoint/fentry/uprobe | CONFIG_KPROBES and CONFIG_FTRACE not set |
| BPF JIT | CONFIG_BPF_JIT not set (interpreter works but slower) |
| `bpf_ringbuf_reserve`/`submit` | Load+verify OK but produce no events without JIT |
| BTF / CO-RE | `/sys/kernel/btf/vmlinux` absent |
| INODE_STORAGE, TASK_STORAGE maps | Require disabled features |

## Ring Buffer mmap Layout

3-section layout (not the 2-section layout commonly documented):

```text
offset 0:            consumer page (consumer_pos u64, rw)
offset PAGE_SIZE:    producer page (producer_pos u64, ro)
offset 2*PAGE_SIZE:  data pages (2x ring size for wrap-around, ro)
```

Entry format: 8-byte header (`u32 len_flags` with bits 31=BUSY, 30=DISCARD + `u32 pad`) followed by payload aligned to 8 bytes.

## Minimum Requirements for Production

- CONFIG_BPF_SYSCALL=y (required)
- CONFIG_BPF_JIT=y (recommended; required for reserve/submit)
- CONFIG_KPROBES=y (required for pipe-capture write interception)
- Linux 5.8+ (ring buffer support)
- /sys/kernel/btf/vmlinux (optional, enables CO-RE/fentry)
