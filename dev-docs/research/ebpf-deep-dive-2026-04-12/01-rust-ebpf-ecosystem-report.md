# Rust eBPF Ecosystem Deep Dive — EDR Sensor Research Report

> **Date:** 2026-04-12
> **Context:** Building an eBPF EDR (Endpoint Detection & Response) sensor in Rust with 12+ tracepoint/kprobe programs. Evaluating aya 0.13.1 (userspace) + aya-ebpf 0.1.1 (kernel) against alternatives.

---

## Table of Contents

1. [Feature Matrix](#1-feature-matrix)
2. [Aya Deep Dive](#2-aya-deep-dive)
3. [libbpf-rs Deep Dive](#3-libbpf-rs-deep-dive)
4. [redbpf Status](#4-redbpf-status)
5. [Other Options](#5-other-options)
6. [LSM Support in aya-ebpf — Confirmed](#6-lsm-support-in-aya-ebpf--confirmed)
7. [Reading task_struct Fields (PPID, euid, namespaces)](#7-reading-task_struct-fields-ppid-euid-namespaces)
8. [Migration Risk Assessment](#8-migration-risk-assessment)
9. [Recommendation](#9-recommendation)

---

## 1. Feature Matrix

| Feature | **aya 0.13.1 + aya-ebpf 0.1.1** | **libbpf-rs 0.26.2** | **redbpf 0.22** |
|---|---|---|---|
| **Language (kernel side)** | Rust (via bpf-linker / rustc BPF target) | C (clang) | Rust (via LLVM 12/13) |
| **Language (userspace)** | Rust (pure, no C deps) | Rust (wraps libbpf C library) | Rust (wraps libbpf/LLVM) |
| **LSM programs** | ✅ Full — `#[lsm]` macro + userspace `Lsm` type | ✅ Full — inherits all libbpf program types | ❌ Not supported |
| **CO-RE (Compile-Once, Run-Everywhere)** | ✅ BTF loading/relocation in userspace; ⚠️ kernel-side CO-RE relocations (preserve_access_index) still WIP in rustc | ✅ Full — C compiler + libbpf handles relocations natively | ⚠️ Partial — BTF for maps, vmlinux bindings |
| **Tail calls (BPF_MAP_TYPE_PROG_ARRAY)** | ✅ `ProgramArray::tail_call()` in eBPF; `ProgramArray` map in userspace | ✅ Native support | ✅ `ProgramArray` supported |
| **BPF-to-BPF function calls** | ✅ Supported via bpf-linker function call relocations; global subprograms work | ✅ Native clang support | ⚠️ Limited |
| **Ring buffer** | ✅ Kernel: `RingBuf` with `reserve`/`submit`/`output`; User: `RingBuf` with poll | ✅ `RingBuffer` + `RingBufferBuilder`; `UserRingBuffer` for user→kernel | ✅ `RingBuf` map |
| **Perf event array** | ✅ `PerfEventArray` + `AsyncPerfEventArray` (tokio/async-std) | ✅ `PerfBuffer` + `PerfBufferBuilder` | ✅ `PerfMap` with async stream |
| **BTF support** | ✅ Transparent BTF loading when kernel supports it | ✅ Full BTF introspection via `Btf` type | ✅ BTF for maps + vmlinux bindings |
| **kprobe / kretprobe** | ✅ | ✅ | ✅ |
| **tracepoint** | ✅ + `#[btf_tracepoint]` | ✅ | ✅ |
| **fentry / fexit** | ✅ | ✅ | ❌ |
| **uprobe / uretprobe** | ✅ | ✅ | ✅ |
| **XDP** | ✅ | ✅ | ✅ |
| **TC (classifier)** | ✅ + TCX support | ✅ `TcHook` | ✅ `#[tc_action]` |
| **cgroup programs** | ✅ skb, sock, sock_addr, sockopt, sysctl, device | ✅ | ❌ |
| **socket filter** | ✅ | ✅ | ✅ |
| **raw tracepoint** | ✅ | ✅ | ❌ |
| **perf_event** | ✅ + hardware breakpoints | ✅ `PerfEventOpts` | ❌ |
| **struct_ops** | ❌ Not yet | ✅ `assoc_struct_ops` | ❌ |
| **BPF iterator** | ❌ Not yet | ✅ `Iter` type | ✅ `TaskIter` only |
| **sk_lookup** | ✅ | ✅ | ✅ |
| **netfilter** | ❌ | ✅ `attach_netfilter_with_opts` | ❌ |
| **kprobe_multi / uprobe_multi** | ❌ | ✅ | ❌ |
| **Build toolchain** | `bpf-linker` (LLVM 21) or `rustc --target bpfel-unknown-none` | clang + libbpf-cargo skeleton gen | LLVM 12/13 via cargo-bpf |
| **C dependency** | None (pure Rust; only libc for syscalls) | Yes — links libbpf.a (C library) | Yes — links LLVM |
| **Static binary** | ✅ musl-compatible | ⚠️ Possible with static libbpf | ⚠️ Complex |
| **Async support** | ✅ tokio + async-std | ✅ (via epoll/polling) | ✅ tokio |
| **Maintained** | ✅ Active (releases Nov 2024, commits weekly) | ✅ Very active (v0.26.2, regular releases) | ❌ Dormant (last release ~2022, last commit ~2023) |
| **Minimum kernel** | 4.18+ (varies by feature; LSM needs 5.7+) | 4.18+ (varies; CO-RE needs 5.x+) | 4.15+ |

---

## 2. Aya Deep Dive

### 2.1 Architecture

Aya is a **pure-Rust** eBPF library that handles BPF syscalls directly via `libc`, with no dependency on `libbpf`, `bcc`, or any C toolchain for the userspace component. The kernel-side programs are written in Rust using `aya-ebpf` and compiled via `bpf-linker` or rustc's built-in `bpfel-unknown-none` target.

**Crate structure:**
- `aya` — userspace library (loading, attaching, map access)
- `aya-ebpf` — kernel-side library (`#[no_std]`, BPF helper wrappers, map types)
- `aya-ebpf-macros` — proc macros (`#[kprobe]`, `#[tracepoint]`, `#[lsm]`, etc.)
- `aya-ebpf-bindings` — generated bindings to BPF helper functions
- `aya-obj` — ELF/BTF object file parser
- `aya-log` / `aya-log-ebpf` — structured logging from BPF programs
- `bpf-linker` — LLVM-based BPF linker for Rust

### 2.2 What Works Well

**Program types:** Aya covers the vast majority of BPF program types needed for an EDR:
- kprobe, kretprobe, tracepoint, raw_tracepoint, btf_tracepoint
- fentry, fexit (near-zero overhead tracing)
- **LSM** (full support — see §6)
- uprobe, uretprobe
- XDP, TC/TCX, cgroup programs, socket filter, sk_msg, sock_ops
- perf_event (including hardware breakpoints as of v0.1.2)

**Map types:** Comprehensive coverage:
- HashMap, PerCpuHashMap, LruHashMap, LruPerCpuHashMap
- Array, PerCpuArray, ProgramArray (tail calls)
- RingBuf, PerfEventArray
- BloomFilter, LpmTrie, Queue, Stack, StackTrace
- SockMap, SockHash, CpuMap, DevMap, DevMapHash, XskMap

**BTF:** Transparent BTF support in userspace — Aya reads BTF from the kernel and applies type relocations when loading programs. This enables CO-RE at the userspace level (map creation, program loading with BTF awareness).

**Tail calls:** Fully supported. `ProgramArray` in aya-ebpf provides `tail_call(ctx, index)`. The userspace `ProgramArray` allows setting program file descriptors at indices. This is critical for EDR designs that dispatch from a central probe to specialized handlers.

**BPF-to-BPF function calls:** Supported through `bpf-linker` function call relocation. Rust functions in the same BPF crate can call each other (global subprograms). The linker has `--ignore-inline-never` for older kernels that don't support function calls.

**Ring buffer:** Both kernel-side (`RingBuf::reserve` → `RingBufEntry::submit`) and userspace (`RingBuf::next()` polling) are fully implemented. The reserve/submit pattern avoids copies — critical for high-throughput EDR telemetry. Minimum kernel: 5.8.

**Build story:** Two paths:
1. **bpf-linker** (recommended): `cargo +nightly build --target bpfel-unknown-none -Z build-std=core`. Uses LLVM 21 under the hood. Supports BTF emission with `-C debuginfo=2 -C link-arg=--btf`.
2. **rustc built-in BPF target**: The `bpfel-unknown-none` / `bpfeb-unknown-none` targets are in-tree in rustc. Still requires nightly for `build-std`.

**Release cadence:** aya v0.13.1 released mid-2024; aya-ebpf-bindings v0.1.2 released Nov 2024. GitHub shows consistent weekly commit activity from multiple maintainers. The project has an active Discord community and is used by production systems like `bpfman` (Red Hat).

### 2.3 What's Experimental or Incomplete

**CO-RE kernel-side relocations:** This is the biggest gap. The C eBPF ecosystem uses `__builtin_preserve_access_index()` (a Clang intrinsic) to emit CO-RE relocations that allow BPF programs to access kernel struct fields portably across kernel versions. Aya's GitHub issue [#349](https://github.com/aya-rs/aya/issues/349) tracks adding `core::intrinsics::preserve_access_index` to rustc for the BPF target. As of April 2026, **this is still not fully landed in stable rustc**.

**Workaround for CO-RE:** The community uses `aya-tool` to generate Rust bindings from `vmlinux` BTF:
```bash
aya-tool generate task_struct > src/vmlinux.rs
```
These bindings are kernel-version-specific. Combined with `bpf_probe_read_kernel()`, this gives you working struct access — but it's not portable across kernel versions without recompilation. For EDR deployment across diverse kernels, you'd either:
1. Ship bindings for multiple kernel versions and select at runtime, or
2. Use `bpf_get_current_task_btf()` (returns typed `*mut task_struct` from BTF) for limited CO-RE behavior, or
3. Accept the portability limitation.

**struct_ops:** Not supported. Unlikely to matter for EDR.

**BPF iterators:** Not supported. Minor gap — iterators are mainly used for debugging/introspection tools.

**kprobe_multi / uprobe_multi:** Not supported. These allow attaching to many functions with a single attachment, reducing overhead for wide tracing. Relevant for EDR but not a blocker.

**netfilter programs:** Not supported. Minor gap for EDR (network hooks are usually done via XDP/TC/kprobes).

**`bpf_core_enum_value_exists`:** Not available ([#957](https://github.com/aya-rs/aya/issues/957)). This CO-RE helper for feature detection requires compiler intrinsics that rustc doesn't emit yet.

### 2.4 Ring Buffer Performance at High Throughput

Aya's ring buffer implementation maps the kernel ring buffer pages directly into userspace (mmap), matching the performance characteristics of the kernel's native BPF ring buffer:

- **Zero-copy read path:** Consumer reads directly from shared memory pages.
- **Reserve/submit pattern (kernel side):** `RingBuf::reserve::<T>()` returns a slot, you fill it, then `submit()` — avoids stack-to-ring-buffer copy.
- **Polling:** Userspace can poll via epoll or busy-loop with `RingBuf::next()`.
- **Overflow behavior:** When the ring buffer is full, `reserve()` returns `None`. Events are dropped (not blocked). This is the correct behavior for EDR — you don't want kernel-side backpressure.
- **Per-CPU vs shared:** Unlike PerfEventArray (per-CPU), RingBuf is a single shared buffer. This simplifies userspace polling but means contention under extreme concurrency. For 12 programs, this is fine.

**Recommendation for EDR:** Use RingBuf (not PerfEventArray) for all event delivery. Size it generously (e.g., 64 MB). Use the reserve/submit pattern. Monitor `bpf_ringbuf_query(BPF_RB_AVAIL_DATA)` for backpressure signals.

---

## 3. libbpf-rs Deep Dive

### 3.1 Overview

[libbpf-rs](https://github.com/libbpf/libbpf-rs) (v0.26.2) provides idiomatic Rust bindings over the C `libbpf` library. It uses the "skeleton" pattern: you write BPF programs in C, `libbpf-cargo` generates a Rust skeleton with typed accessors for maps and programs.

### 3.2 Strengths

- **Full CO-RE:** Because BPF programs are written in C and compiled with Clang, you get the full `__builtin_preserve_access_index()` CO-RE machinery. This is the gold standard for portable kernel struct access.
- **Complete feature coverage:** Inherits all program types from libbpf — LSM, struct_ops, BPF iterators, kprobe_multi, uprobe_multi, netfilter.
- **Mature C ecosystem:** Benefits from the massive libbpf C ecosystem — tools, documentation, kernel developer support.
- **Active development:** v0.26.2 (latest), frequent releases, backed by the libbpf project (Meta/kernel community).

### 3.3 Weaknesses

- **C dependency:** Links against `libbpf.a` (C), which in turn needs `libelf`, `zlib`. Complicates static binary builds.
- **Split language:** BPF programs must be written in C. Userspace in Rust. No shared types without codegen.
- **Build complexity:** Requires Clang + libbpf-cargo build step. More moving parts than aya's pure-Rust approach.
- **Skeleton pattern coupling:** The generated skeleton couples your Rust code to specific BPF program/map names. Refactoring BPF programs requires regenerating skeletons.

### 3.4 EDR Relevance

libbpf-rs is **the safest choice if CO-RE portability across many kernel versions is the top priority**. Large EDR vendors (Falco, Tetragon) use libbpf (or Go bindings) precisely because of CO-RE. The tradeoff is losing Rust's advantages in the kernel-side code — type safety, borrow checker, no header dependencies.

---

## 4. redbpf Status

### 4.1 Current State: **Effectively Abandoned**

- **Last tagged release:** v0.22.0 (circa late 2022)
- **Last meaningful commit:** ~2023
- **CNCF Sandbox status:** The project was donated to CNCF under the "foniod" umbrella, but activity has ceased.
- **Build dependency:** Requires LLVM 12 or 13, which is increasingly difficult to source.
- **Missing program types:** No LSM, no fentry/fexit, no cgroup programs, no raw tracepoint. Fatal for an EDR.

### 4.2 Verdict

**Do not use redbpf for new projects.** It was an important pioneering effort but is now superseded by aya in every dimension. The build complexity (LLVM dependency), missing program types, and lack of maintenance make it unsuitable for production EDR work.

---

## 5. Other Options

### 5.1 libbpf-sys

Raw (unsafe) Rust bindings to the C libbpf library, generated by bindgen. This is what libbpf-rs builds on. Using it directly is possible but means writing unsafe code for every libbpf call. **Not recommended** — use libbpf-rs instead.

### 5.2 BumbleBee (solo-io/bumblebee)

A Go-based tool for building, running, and distributing eBPF programs as OCI images. **Not relevant for a Rust EDR sensor** — it's a deployment/packaging tool, not a programming framework. BPF programs are still written in C. The project appears low-activity.

### 5.3 Rust + C Hybrid Approach

A practical middle ground: write BPF programs in C (for full CO-RE), use aya's userspace library to load and manage them. Aya can load arbitrary BPF ELF object files — it's not limited to programs compiled from Rust. This gives you:
- CO-RE kernel-side programs (C + Clang)
- Pure Rust userspace with no libbpf C dependency
- Aya's ergonomic map/program API
- Shared types via `#[repr(C)]` Rust structs matching C struct layouts

This hybrid is increasingly common in the community and is worth considering if CO-RE is essential.

### 5.4 Cilium ebpf (Go)

The Go eBPF library from the Cilium project is the most production-hardened option overall, used by Cilium, Tetragon, Falco, and Datadog. However, it requires Go — not viable if the EDR sensor must be a Rust binary.

---

## 6. LSM Support in aya-ebpf — Confirmed

### 6.1 Kernel-Side: `#[lsm]` Macro — YES, Fully Supported

The `aya-ebpf-macros` crate (v0.1.1+) provides the **`#[lsm]`** attribute macro. This is documented and functional:

```rust
use aya_ebpf::{macros::lsm, programs::LsmContext};

#[lsm(hook = "file_open")]
pub fn file_open(ctx: LsmContext) -> i32 {
    match unsafe { try_file_open(ctx) } {
        Ok(ret) => ret,
        Err(ret) => ret,
    }
}

unsafe fn try_file_open(ctx: LsmContext) -> Result<i32, i32> {
    // Read LSM hook arguments:
    // LSM_HOOK(int, 0, file_open, struct file *file)
    let _file_ptr: *const core::ffi::c_void = ctx.arg(0);
    let _retval: i32 = ctx.arg(1); // previous LSM program's return value
    Ok(0)
}
```

**Additional variant:** `#[lsm_cgroup]` is also available for cgroup-scoped LSM programs.

**Sleepable support:** The macro accepts `sleepable` as a parameter:
```rust
#[lsm(hook = "bprm_check_security", sleepable)]
```

### 6.2 Kernel-Side: `LsmContext`

`LsmContext` provides `arg::<T>(n)` to read hook arguments by index. The argument list matches the kernel's `LSM_HOOK` macro definition (see [lsm_hook_defs.h](https://elixir.bootlin.com/linux/latest/source/include/linux/lsm_hook_defs.h)), with an appended `retval: int` as the last argument.

### 6.3 Userspace: `Lsm` Program Type

```rust
use aya::programs::Lsm;
use aya::Btf;

let btf = Btf::from_sys_fs()?;
let program: &mut Lsm = ebpf.program_mut("file_open").unwrap().try_into()?;
program.load("file_open", &btf)?;
program.attach()?;
```

LSM programs require:
- Kernel compiled with `CONFIG_BPF_LSM=y` and `CONFIG_DEBUG_INFO_BTF=y`
- BPF LSM enabled in boot parameters: `lsm=lockdown,yama,bpf`
- Minimum kernel: **5.7**

### 6.4 EDR-Relevant LSM Hooks

Key hooks for an EDR sensor:

| Hook | Purpose | Arguments |
|---|---|---|
| `bprm_check_security` | Process execution policy | `struct linux_binprm *bprm` |
| `file_open` | File open policy | `struct file *file` |
| `file_permission` | File read/write/exec | `struct file *file, int mask` |
| `mmap_file` | Memory mapping files | `struct file *file, unsigned long prot, ...` |
| `task_alloc` | New task creation | `struct task_struct *task, unsigned long clone_flags` |
| `task_fix_setuid` | Privilege escalation | `struct cred *new, const struct cred *old, int flags` |
| `socket_connect` | Outbound network connections | `struct socket *sock, struct sockaddr *address, int addrlen` |
| `socket_bind` | Socket bind operations | `struct socket *sock, struct sockaddr *address, int addrlen` |
| `sb_mount` | Mount operations | `const char *dev_name, ...` |
| `inode_rename` | File renames | `struct inode *old_dir, struct dentry *old_dentry, ...` |
| `ptrace_access_check` | ptrace attempts | `struct task_struct *child, unsigned int mode` |
| `kernel_module_request` | Module loading | `char *kmod_name` |

**LSM vs kprobe for EDR:** LSM hooks have a critical advantage — they can **deny** operations (return non-zero to block). Kprobes can only observe. For a blocking EDR, LSM is essential.

---

## 7. Reading task_struct Fields (PPID, euid, Namespaces)

### 7.1 Available BPF Helpers in aya-ebpf

Aya exposes the following helpers for process context (from `aya_ebpf::helpers`):

| Helper | Signature | Purpose |
|---|---|---|
| `bpf_get_current_pid_tgid()` | `() -> u64` | Returns `(tgid << 32) \| pid`. tgid = userspace PID. |
| `bpf_get_current_uid_gid()` | `() -> u64` | Returns `(gid << 32) \| uid`. Real UID/GID of current task. |
| `bpf_get_current_comm()` | `() -> Result<[u8; 16], c_long>` | Returns the `comm` field (process name, 16 bytes). |
| `bpf_get_current_task()` | `() -> u64` | Returns raw pointer to current `task_struct` as `u64`. |
| `bpf_get_current_task_btf()` | `() -> *mut task_struct` | Returns typed pointer to `task_struct` (requires BTF, kernel 5.11+). |
| `bpf_probe_read_kernel::<T>(src)` | `(*const T) -> Result<T, c_long>` | Reads a value from kernel memory. |
| `bpf_probe_read_kernel_buf(src, dst)` | `(*const u8, &mut [u8]) -> Result<(), c_long>` | Reads bytes from kernel memory into a buffer. |
| `bpf_probe_read_kernel_str_bytes(src, dst)` | `(*const u8, &mut [u8]) -> Result<&[u8], c_long>` | Reads a null-terminated string from kernel memory. |

### 7.2 Generating vmlinux Bindings

To read `task_struct` fields, you need Rust type definitions that match the kernel's layout. Use `aya-tool`:

```bash
# Generate bindings for specific types
cargo install aya-tool
aya-tool generate task_struct cred nsproxy pid_namespace > ebpf-program/src/vmlinux.rs
```

This produces `#[repr(C)]` Rust structs with correct field offsets for your build kernel's BTF. The generated types include all nested structs.

### 7.3 Reading PPID

```rust
use aya_ebpf::helpers::{bpf_get_current_task, bpf_probe_read_kernel};

// Import generated vmlinux types
mod vmlinux;
use vmlinux::task_struct;

unsafe fn get_ppid() -> Result<u32, i64> {
    let task = bpf_get_current_task() as *const task_struct;

    // Read parent pointer: task->real_parent
    let parent = bpf_probe_read_kernel(&(*task).real_parent)?;

    // Read parent's tgid: task->real_parent->tgid
    let ppid = bpf_probe_read_kernel(&(*parent).tgid)?;

    Ok(ppid as u32)
}
```

### 7.4 Reading Credentials (euid, egid)

```rust
unsafe fn get_euid() -> Result<u32, i64> {
    let task = bpf_get_current_task() as *const task_struct;

    // task->cred->euid.val
    let cred = bpf_probe_read_kernel(&(*task).cred)?;
    let euid = bpf_probe_read_kernel(&(*cred).euid)?;

    // kuid_t is a struct with a single `val` field
    Ok(euid.val)
}
```

### 7.5 Reading Namespace Information

```rust
unsafe fn get_pid_ns_level() -> Result<u32, i64> {
    let task = bpf_get_current_task() as *const task_struct;

    // task->nsproxy->pid_ns_for_children->level
    let nsproxy = bpf_probe_read_kernel(&(*task).nsproxy)?;
    let pidns = bpf_probe_read_kernel(&(*nsproxy).pid_ns_for_children)?;
    let level = bpf_probe_read_kernel(&(*pidns).level)?;

    // level > 0 means we're in a container (non-root PID namespace)
    Ok(level)
}
```

### 7.6 Reading cgroup ID (Container Identification)

```rust
// Available as a raw BPF helper:
use aya_ebpf::helpers::gen::bpf_get_current_cgroup_id;

unsafe fn get_cgroup_id() -> u64 {
    bpf_get_current_cgroup_id()
}
```

### 7.7 Reading Session ID and Process Group

```rust
unsafe fn get_session_id() -> Result<u32, i64> {
    let task = bpf_get_current_task() as *const task_struct;

    // task->group_leader->pid (session leader PID)
    let group_leader = bpf_probe_read_kernel(&(*task).group_leader)?;
    let session_pid = bpf_probe_read_kernel(&(*group_leader).tgid)?;

    Ok(session_pid as u32)
}
```

### 7.8 CO-RE Portability Caveat

**Critical limitation:** The `bpf_probe_read_kernel(&(*task).field)` pattern uses **hardcoded field offsets** from the vmlinux bindings generated at build time. If the kernel changes `task_struct` layout (field reordering, added fields), the offsets break silently — you read garbage or the verifier rejects the program.

**Mitigation strategies:**

1. **`bpf_get_current_task_btf()`** (kernel 5.11+): Returns a BTF-typed pointer. When combined with BPF_CORE_READ (a C macro), the verifier can perform field offset relocations. **However, Rust/aya cannot emit the CO-RE relocations that make this portable.** You still get the correct type but fixed offsets.

2. **Multi-kernel binary builds:** Compile separate BPF object files for major kernel version families (5.10, 5.15, 6.1, 6.6, 6.8) and select at runtime based on detected kernel version.

3. **Hybrid C approach:** Write the task_struct-reading BPF programs in C (with CO-RE), compile with Clang, and load them from Rust using `Ebpf::load_file("task_reader.o")`. The rest of your EDR BPF programs stay in Rust.

4. **Stable-ABI fields only:** Some task_struct fields (`pid`, `tgid`, `comm`) have been stable across many kernel versions. Use `bpf_get_current_pid_tgid()` and `bpf_get_current_comm()` (dedicated helpers) for these — no struct access needed. Reserve manual struct access for less commonly changed fields.

---

## 8. Migration Risk Assessment

### 8.1 Staying with Aya (Current Choice)

| Factor | Assessment |
|---|---|
| **Feature coverage for EDR** | ✅ Excellent — LSM, kprobe, tracepoint, fentry, ring buffer, tail calls all supported |
| **CO-RE gap** | ⚠️ Significant — no kernel-side CO-RE relocations. Requires multi-kernel builds or hybrid C approach for portable task_struct access |
| **Maintenance risk** | Low — active project with multiple maintainers, used by Red Hat's bpfman |
| **Build complexity** | Low — pure Rust, no C toolchain needed for userspace |
| **Developer experience** | Excellent — Rust type safety, proc macros, cargo integration |
| **Community** | Growing — Discord, Gurubase, awesome-aya list |

### 8.2 Migrating to libbpf-rs

| Factor | Assessment |
|---|---|
| **Migration effort** | High — rewrite all 12 BPF programs from Rust to C |
| **CO-RE benefit** | ✅ Full CO-RE — the primary driver for migration |
| **Feature completeness** | ✅ Best — struct_ops, iterators, kprobe_multi, netfilter |
| **Build complexity** | Higher — needs Clang, libbpf, libelf, build scripts |
| **Static binary** | Harder — libbpf.a adds complexity |
| **Type sharing** | Lost — C BPF programs can't share Rust types natively |
| **Ongoing maintenance** | Higher — two languages, skeleton regeneration on BPF changes |

### 8.3 Hybrid Approach (Aya Userspace + C BPF for CO-RE-sensitive programs)

| Factor | Assessment |
|---|---|
| **Migration effort** | Medium — rewrite only CO-RE-sensitive programs in C |
| **CO-RE benefit** | ✅ For the programs that need it |
| **Complexity** | Medium — mixed build system (cargo + clang) |
| **Pragmatism** | High — use the right tool for each job |

---

## 9. Recommendation

### Primary Recommendation: **Stay with Aya, Adopt Hybrid C for CO-RE-Sensitive Programs**

For an EDR sensor with 12 tracepoint/kprobe programs, aya is the right choice as the primary framework:

1. **Keep aya as the userspace framework.** Its pure-Rust, no-C-dependency story is a significant advantage for a security product that needs to be a single static binary.

2. **Write most BPF programs in Rust with aya-ebpf.** The `#[lsm]`, `#[kprobe]`, `#[tracepoint]`, and `#[fentry]` macros provide excellent ergonomics. LSM support is first-class.

3. **Use C (compiled with Clang) for BPF programs that need CO-RE access to task_struct internals.** Specifically, if you need portable PPID/credential/namespace reading across diverse kernel versions (5.10 through 6.x), write those 2-3 specific programs in C and load them via `Ebpf::load_file()`.

4. **Use `RingBuf` for all event delivery.** Reserve/submit pattern, 64 MB+ buffer, poll from userspace with tokio.

5. **Use tail calls (`ProgramArray`) to dispatch from common entry points to specialized handlers.** This keeps individual BPF program complexity low and stays under the verifier's instruction limit.

6. **Use LSM hooks for blocking decisions.** `bprm_check_security`, `file_open`, `socket_connect`, `task_alloc` — these give you deny capability that kprobes cannot.

### When to Reconsider

- **If aya lands full CO-RE** (rustc `preserve_access_index` intrinsics — tracked in [#349](https://github.com/aya-rs/aya/issues/349)): drop the C programs and go pure Rust.
- **If you need struct_ops or kprobe_multi**: evaluate libbpf-rs for those specific programs.
- **If aya maintenance declines**: libbpf-rs is the fallback. Migration cost is high but the project is backed by the kernel community.

### Do NOT Use

- **redbpf** — abandoned, missing critical program types, LLVM build nightmare.
- **BumbleBee** — wrong tool (deployment, not a Rust framework).
- **libbpf-sys directly** — too low-level; use libbpf-rs if you need libbpf.

---

## Appendix A: Aya Crate Version Summary

| Crate | Version | Notes |
|---|---|---|
| `aya` | 0.13.1 | Userspace library |
| `aya-ebpf` | 0.1.1 | Kernel-side library |
| `aya-ebpf-macros` | 0.1.1 | Proc macros (includes `#[lsm]`) |
| `aya-ebpf-bindings` | 0.1.2 | Generated BPF helper bindings (Nov 2024) |
| `aya-obj` | 0.2.1 | ELF/BTF parser |
| `aya-log` | 0.2.1 | Userspace log receiver |
| `aya-log-ebpf` | 0.1.1 | Kernel-side log emitter |
| `bpf-linker` | latest | LLVM 21-based BPF linker |

## Appendix B: Minimum Kernel Versions for EDR Features

| Feature | Minimum Kernel |
|---|---|
| kprobe/kretprobe | 4.1 |
| tracepoint | 4.7 |
| BPF tail calls | 4.2 |
| PerfEventArray | 4.3 |
| Raw tracepoint | 4.17 |
| BTF loading | 5.2 |
| RingBuf | 5.8 |
| fentry/fexit | 5.5 |
| LSM BPF | 5.7 |
| `bpf_get_current_task_btf()` | 5.11 |
| BPF-to-BPF function calls | 4.16 (limited), 5.10+ (reliable) |
| Sleepable BPF programs | 5.10 |
| BPF LSM cgroup | 6.0 |

## Appendix C: Key aya GitHub Issues to Watch

| Issue | Topic | Status |
|---|---|---|
| [#349](https://github.com/aya-rs/aya/issues/349) | CO-RE: rustc `preserve_access_index` intrinsics | Open — blocked on rustc |
| [#551](https://github.com/aya-rs/aya/issues/551) | Reading task_struct in BPF programs (community examples) | Closed (examples work) |
| [#957](https://github.com/aya-rs/aya/issues/957) | `bpf_core_enum_value_exists` for feature detection | Open |
| [#753](https://github.com/aya-rs/aya/issues/753) | CO-RE related issue | Open |
| [#722](https://github.com/aya-rs/aya/issues/722) | CO-RE / BTF integration | Open |
