# eBPF EDR Sensor — Research & Implementation Plan

> **Status:** Active
> **Date:** 2026-04-12
> **Context:** Research synthesis from 5 parallel workstreams (Rust eBPF ecosystem, EDR architecture patterns, high-performance eBPF, MITRE ATT&CK coverage, bleeding-edge kernel features).
> **Sources:** Aya/libbpf-rs ecosystem analysis, Tracee/Tetragon/Falco architecture review, performance modeling, MITRE ATT&CK coverage mapping.

---

## Decisions

### Framework: Stay with Aya + Hybrid C Escape Hatch

**Decision:** Keep aya 0.13.1 (userspace) + aya-ebpf 0.1.1 (kernel). Write 2–3 CO-RE-sensitive BPF programs in C, compile with Clang, load via `Ebpf::load_file()`.

| Factor | Status |
|--------|--------|
| Feature coverage | ✅ LSM, kprobes, tracepoints, raw tracepoints, fentry/fexit, RingBuf, tail calls, BPF-to-BPF calls |
| CO-RE gap | ⚠️ No kernel-side `preserve_access_index` — significant for portable `task_struct` access. Track [aya#349](https://github.com/aya-rs/aya/issues/349); drop C hybrids when rustc lands the intrinsic |
| Eliminated | redbpf (abandoned), libbpf-sys (too low-level), BumbleBee (deployment tool, not framework) |

**Action items:**
1. Pin `bpf-linker` version in CI
2. Add LSM integration tests on kernels ≥5.7 with `CONFIG_BPF_LSM`
3. Generate vmlinux bindings: `aya-tool generate task_struct cred nsproxy pid_namespace`

### Event Transport: RingBuf (not PerfEventArray)

Single shared RingBuf with reserve/submit pattern. Ordering is more valuable for EDR correlation than per-CPU isolation. Revisit sharding only if measured contention at >100K eps.

### Process Tree: In-Kernel (Tetragon Pattern)

`LruHashMap<u32, ProcessEntry>` keyed by TGID, maintained via fork/exec/exit hooks. `(pid, ktime)` tuple as unique process identifier to survive PID recycling.

### Syscall Dispatch: Raw Tracepoint + Tail Calls (Tracee Pattern)

`raw_tracepoint/sys_enter` + `raw_tracepoint/sys_exit` as unified entry points, with `PROG_ARRAY` tail-call dispatch by syscall ID. Layered stages: init → filter → handler.

### DNS: Socket-Level Port 53 Filtering

Hook `udp_sendmsg`/`tcp_sendmsg` with dest port 53 filter. Parse DNS wire format in userspace. Upgrade to packet capture only if detection quality is insufficient.

### LSM Hooks: Observe-Only Initially

All LSM hooks return 0 (no blocking). Defer `fmod_ret` blocking until stability is proven.

---

## Implementation Priorities (Top 10)

| # | Priority | Impact | Size | Deps | Description |
|---|----------|--------|------|------|-------------|
| 1 | Epoll drain + batching | Very High | S | — | Replace `ring.next()` + 1ms sleep with epoll-triggered aggressive drain, lock-free channel, batch serialization |
| 2 | Process state map | Very High | M | — | `LRU_HASH<u32, ProcessEntry>` maintained on fork/exec/exit with parent-key chain |
| 3 | Syscall dispatcher + correlation | Very High | L | ↑#2 | `raw_tracepoint/sys_enter+sys_exit`, per-TID inflight state, `PROG_ARRAY` tail-call dispatch |
| 4 | Execve argv/env capture | Very High | M | #3 | Read `mm->arg_start..arg_end`, multi-stage tail calls for large argv, check LD_PRELOAD/LD_LIBRARY_PATH |
| 5 | Drop telemetry + backpressure | High | S | — | Per-CPU `PerCpuArray<DropStats>` incremented on every `reserve()` failure; userspace health events |
| 6 | LSM hooks | High | M | — | `security_file_permission`, `security_sb_mount`, `security_mmap_file` (shared lib detection) |
| 7 | In-kernel filtering | High | M | #3 | Syscall bitmap → cgroup scope → path prefix filters. Evolve to policy bitmaps later |
| 8 | String/path optimization | Med-High | M | #7 | Tier 1: fixed metadata + truncated basename. Tier 2: full path only on policy match |
| 9 | Namespace transition hooks | High | S-M | #2 | `setns`, `unshare`, `clone3` — container escape detection (T1611) |
| 10 | Socket lifecycle hooks | High | M | #3 | `connect`, `bind`, `listen`, `accept4` — reverse shells, rogue listeners |

**Execution order:** 1 → 5 → 2 → 3 → 4 → 6 → 7 → 9 → 10 → 8

---

## Architecture Plan (Current → Next → Target)

### Current State

```text
12 independent tracepoint/kprobe programs
  → single 16MB RingBuf → sleep-loop consumer → per-event JSON to stdout

Gaps: no ancestry (ppid=0), no syscall success/failure, no filtering, no drop visibility
```

### Next State (Priorities 1–5, ~2–3 months)

```text
raw_tracepoint sys_enter/sys_exit dispatcher
  → tail-call dispatch by syscall ID
  → per-TID inflight correlation map
  → in-kernel process state map (fork/exec/exit)
  → per-CPU drop counters
  → 16–64MB RingBuf → epoll drain → lock-free channel → batch serializer → JSON/OTLP

Wins: return values, ancestry, drop visibility, 3–5x throughput
```

### Target State (Priorities 6–10+, ~6–9 months)

```text
Syscall dispatcher + LSM hooks + targeted kprobes
  → map-backed policy filtering (syscall bitmap → cgroup → path prefix)
  → two-tier capture (metadata-first, selective enrichment)
  → sharded RingBufs (4–16 shards)
  → multi-thread epoll drain → behavioral correlation → OTLP export

Wins: ~73% MITRE coverage, in-kernel noise reduction, 100K+ eps
```

### Throughput Targets

| Metric | Current (est.) | After #1, #5 | After #2–#7 |
|--------|---------------|--------------|-------------|
| Sustainable ingest | ~10K eps | ~50K eps | 100–150K eps |
| Drop rate under burst | Unknown | Observable | <0.1% at load |
| p99 event latency | ~1–2ms | <100µs | <50µs |

---

## Key Patterns to Implement

### Entry→Exit Correlation (Per-Thread State Map)

```rust
#[map]
static TASK_INFO: LruHashMap<u32, TaskInfo> = LruHashMap::with_max_entries(10240, 0);

#[repr(C)]
struct TaskInfo {
    syscall_id: u32,
    args: [u64; 6],
    ret: i64,
    ts: u64,
    traced: bool,
}

// sys_enter: save args, set traced = true
// sys_exit:  if traced, capture ret, emit event, set traced = false
// execve special case: emit on entry (success replaces address space),
//                      emit on exit only if failed (ret != 0)
```

### Process State Map

```rust
#[repr(C)]
struct ProcessEntry {
    pid: u32,
    ktime: u64,           // (pid, ktime) = unique ID across PID recycling
    ppid: u32,
    parent_ktime: u64,
    nspid: u32,
    binary_path: [u8; 256],
    uid: u32,
    euid: u32,
    flags: u32,
}

#[map]
static PROC_MAP: LruHashMap<u32, ProcessEntry> = LruHashMap::with_max_entries(32768, 0);
// Hook: sched_process_fork → create child, set parent link
// Hook: sched_process_exec → update binary, creds
// Hook: sched_process_exit → delete entry
```

### Tail-Call Dispatch

```text
raw_tracepoint/sys_enter
  └→ tail_call(sys_enter_init_tail, syscall_id)
       └→ saves args + task context to TASK_INFO
            └→ tail_call(sys_enter_handler_tail, syscall_id)
                 └→ syscall-specific logic (optional)

raw_tracepoint/sys_exit
  └→ tail_call(sys_exit_init_tail, syscall_id)
       └→ correlates with entry, captures ret
            └→ tail_call(sys_exit_handler_tail, syscall_id)
```

Empty PROG_ARRAY slots fall through silently — only populate monitored syscalls.

### In-Kernel Filtering (Phased)

```rust
// Phase 1: syscall bitmap (check before saving args)
#[map]
static SYSCALL_FILTER: Array<u64> = Array::with_max_entries(8, 0); // 512 bits

// Phase 2: container scope
#[map]
static CGROUP_FILTER: HashMap<u64, u8> = HashMap::with_max_entries(1024, 0);

// Evaluation order (cheapest first):
// 1. Syscall bitmap → skip unmonitored syscalls
// 2. Self-PID filter → skip sensor's own events
// 3. Cgroup filter → scope to monitored containers
// 4. Path prefix filter → tier-2 enrichment for security-relevant paths
```

### Drop Telemetry

```rust
#[map]
static DROP_COUNTERS: PerCpuArray<DropStats> = PerCpuArray::with_max_entries(1, 0);

#[repr(C)]
struct DropStats {
    reserve_fail: u64,
    by_kind: [u64; 16],
}
// Increment on every reserve() failure. Userspace reads + aggregates periodically.
```

### task_struct Reading (CO-RE Caveat)

Use `bpf_get_current_task()` + `bpf_probe_read_kernel()` with `aya-tool`-generated vmlinux bindings. **Field offsets are kernel-version-specific.** Mitigation options:

1. **Hybrid C** — write CO-RE-sensitive readers in C with `BPF_CORE_READ`, load via `Ebpf::load_file()`
2. **Stable helpers** — prefer `bpf_get_current_pid_tgid()`, `bpf_get_current_uid_gid()`, `bpf_get_current_comm()` (no struct access)
3. **Multi-kernel builds** — ship bindings for 5.10/5.15/6.1/6.6, select at runtime

Key fields to extract:

| Field | Path | Helper Available? |
|-------|------|-------------------|
| PID/TGID | `task->{pid,tgid}` | ✅ `bpf_get_current_pid_tgid()` |
| UID/GID | `task->cred->{uid,gid}` | ✅ `bpf_get_current_uid_gid()` |
| Comm | `task->comm` | ✅ `bpf_get_current_comm()` |
| PPID | `task->real_parent->tgid` | ❌ manual read |
| EUID | `task->cred->euid` | ❌ manual read |
| Capabilities | `task->cred->cap_{effective,inheritable,permitted}` | ❌ manual read |
| NS PIDs | `task->thread_pid->numbers[level].nr` | ❌ manual read |
| Namespace inums | `task->nsproxy->{mnt_ns,pid_ns,...}->ns.inum` | ❌ manual read |
| Start time | `task->start_boottime` (5.5+) | ❌ manual read |
| Cgroup ID | — | ✅ `bpf_get_current_cgroup_id()` |

### Shared Library Detection

```rust
#[lsm(hook = "mmap_file")]
fn detect_so_load(ctx: LsmContext) -> i32 {
    let prot: u64 = unsafe { ctx.arg(1) };
    if prot & PROT_EXEC == 0 { return 0; }
    // Extract file path, check ELF magic, emit SHARED_OBJECT_LOADED event
    0 // observe only
}
```

Catches: `dlopen()`, `LD_PRELOAD`, `memfd_create()` + `mmap(PROT_EXEC)`.

---

## MITRE ATT&CK Coverage

### Current → After Top 10 → Extended

| Metric | Current (12 hooks) | After Top 10 | After Extended (+5 hooks) |
|--------|-------------------|-------------|--------------------------|
| Overall coverage | ~30% | ~65–70% | ~73% |
| High-confidence direct | ~15% | ~40% | ~48% |

### Key Techniques Unlocked by Top 10

| Priority | Techniques Enabled |
|----------|--------------------|
| #2 Process state | Ancestry-based behavioral detection across all tactics |
| #3 Correlation | T1055 (injection), success/failure discrimination |
| #4 Argv capture | T1059.\* (command/scripting), T1574.\* (hijack), T1036 (masquerade) |
| #6 LSM hooks | T1003.007/.008 (credential dump), T1098.004 (SSH key), T1546.004 (shell config), T1053.003 (cron) |
| #9 Namespace | T1611 (container escape) — Critical severity |
| #10 Socket | T1090/T1572 (proxy/tunneling), reverse shells |

### Detection Sequences (Multi-Event, Post-Top-10)

1. **Reverse shell:** ProcessExec(shell) → TcpConnect(external) within 1s
2. **Web exploit:** webserver → shell → connect → sensitive file read
3. **Fileless injection:** memfd_create → execveat/ptrace → network beacon
4. **Persistence:** write cron/systemd path → daemon-reload
5. **Container escape:** container process → setns/unshare/mount
6. **Credential theft:** read /etc/shadow → compress → exfiltrate
7. **Anti-forensics:** mass unlink/rename + timestomp burst

### Remaining Gaps

| Gap | Mitigation |
|-----|------------|
| Cloud control-plane (T1578) | Different sensor surface — out of scope |
| Identity/auth events | PAM/auditd integration (medium priority) |
| eBPF tamper (T1562.001) | Hook `bpf()` syscall (medium priority) |
| Packet-level DNS | Port 53 heuristic covers ~80% |
| Hardware/firmware (T1542) | Below OS — out of scope |

---

## Kernel Requirements

### Target: Kernel 6.1 (Full Capability)

| Feature | Min Kernel | On 6.1? |
|---------|-----------|---------|
| Raw tracepoints | 4.17 | ✅ |
| BTF / CO-RE | 5.2 | ✅ |
| fentry/fexit | 5.5 | ✅ |
| LSM BPF | 5.7 | ✅ (needs `CONFIG_BPF_LSM`) |
| RingBuf | 5.8 | ✅ |
| Reliable tail calls + BPF-to-BPF | 5.10+ | ✅ |
| Sleepable BPF | 5.10 | ✅ |
| `bpf_get_current_task_btf()` | 5.11 | ✅ |

### Future Kernels

| Kernel | Feature | Value |
|--------|---------|-------|
| 6.0 | BPF LSM cgroup | Per-container security policies |
| 6.2+ | kprobe_multi | Single attachment for multi-hook — simplifies setup |
| 6.9 | BPF Arena | Zero-copy shared memory — potential RingBuf replacement |

### CI Test Matrix

Test on: **5.15 LTS, 6.1 LTS, 6.6+** — with and without BTF, with and without `CONFIG_BPF_LSM`. LSM hooks degrade gracefully (simply not attached if config absent).

---

## Open Questions (Require Prototyping)

1. **CO-RE hybrid build:** Can C-compiled and Rust-compiled BPF objects coexist in the same Aya `Ebpf` instance?
2. **Tail-call + BPF-to-BPF stack budget:** Combined depth shrinks per-program stack to 256B. Do our event structs fit?
3. **LSM availability in practice:** Survey target distro kernels for `CONFIG_BPF_LSM` defaults (some Ubuntu/RHEL don't enable it).
4. **Verifier limits for enriched events:** Test multi-stage tail-call argv capture with our event struct sizes.
5. **Ring buffer sizing:** Run saturation tests with realistic event mix to choose 16/32/64 MB default.
6. **Per-CPU counter read cost:** Benchmark map read frequency vs. freshness for drop telemetry.

---

## Patterns Adopted/Deferred from Production EDR

| Pattern | Source | Decision | Rationale |
|---------|--------|----------|-----------|
| Raw tracepoint sys_enter/exit dispatch | Tracee | **Adopt** | Foundation for all syscall detection |
| TGID-keyed process map | Tetragon | **Adopt** | In-kernel ancestry essential |
| Layered tail-call stages | Tracee | **Adopt** | Clean separation, verifier-friendly |
| CO-RE task_struct enrichment | All | **Adopt** (hybrid C) | Portable PPID/creds/namespaces |
| Syscall bitmap + cgroup filter | Tracee | **Adopt** (simplified) | Start simple, evolve |
| `security_mmap_file` for SO detection | Tracee | **Adopt** | Catches dlopen + LD_PRELOAD + memfd |
| Cgroup rate throttling | Tetragon | **Defer** | Measure need with drop telemetry first |
| Per-CPU perf buffer topology | Falco | **Skip** | RingBuf ordering > per-CPU isolation for EDR |
| Packet-level DNS capture | Tracee | **Skip** | Socket-level port 53 gives 80% at 20% effort |
| `fmod_ret` LSM blocking | Tetragon | **Defer** | Observe-only first; blocking adds stability risk |

### Map Type Selection

| Purpose | Map Type | Why |
|---------|----------|-----|
| Per-task inflight state | `LRU_HASH` | Auto-evicts on crash; no cleanup needed |
| Process tree | `HASH` | Need explicit delete on exit; no auto-eviction of live procs |
| Syscall filter | `ARRAY` | Fixed-size bitmap; O(1) lookup |
| Cgroup filter | `HASH` | Dynamic set of monitored cgroups |
| Tail-call dispatch | `PROG_ARRAY` | Required by `bpf_tail_call()` |
| Event scratch space | `PERCPU_ARRAY` | No locking; per-CPU copy |
| Path prefix filter | `LPM_TRIE` | Longest-prefix-match |
| Hot-reloadable filters | `HASH_OF_MAPS` | Atomic inner-map swap |
