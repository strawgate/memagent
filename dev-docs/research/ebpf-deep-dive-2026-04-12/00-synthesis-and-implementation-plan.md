# eBPF EDR Sensor: Synthesis & Implementation Plan

> **Date:** 2026-04-12
> **Input:** 4 cloud research reports + 2 local sub-agent reports + sensor code review
> **Status of Report 5 (Bleeding-Edge Kernel):** All attempts rate-limited; kernel version strategy below is synthesized from information in Reports 1–4 and local reports.

---

## A. Framework Decision: Stay with Aya

**Verdict: Stay with Aya. Adopt a surgical hybrid C escape hatch for CO-RE-sensitive programs only.**

**Confidence: High** — all 6 reports independently reach this conclusion.

| Factor | Evidence |
|--------|----------|
| Feature coverage | Aya supports every primitive we need: LSM (`#[lsm]`), kprobes, tracepoints, raw tracepoints, fentry/fexit, RingBuf, tail calls, BPF-to-BPF calls [R1-cloud, R1-local] |
| Migration cost | Rewriting 12 kernel programs from Rust to C (libbpf-rs) is a major architecture rewrite with no capability gain [R1-cloud] |
| Project health | Active maintenance through April 2026, ongoing LSM and ringbuf fixes [R1-cloud] |
| CO-RE gap | Aya lacks `BPF_CORE_READ` macro equivalent in Rust eBPF — significant for portable `task_struct` access across kernel versions [R1-local, R1-cloud] |
| CO-RE mitigation | Write 2–3 CO-RE-sensitive BPF programs in C, compile with Clang, load via `Ebpf::load_file()`. Keep everything else in Rust [R1-local] |
| Alternatives eliminated | redbpf (abandoned since 2022), libbpf-sys (too low-level), BumbleBee (deployment tool, not framework) [R1-cloud, R1-local] |

**Action items:**
1. Pin `bpf-linker` version in CI for reproducible builds
2. Add LSM integration tests on target kernels (≥5.7 with `CONFIG_BPF_LSM`)
3. Track aya issue [#349](https://github.com/aya-rs/aya/issues/349) — when rustc lands `preserve_access_index`, drop the C hybrid programs
4. Generate vmlinux bindings via `aya-tool generate task_struct cred nsproxy pid_namespace` for current Rust-side `task_struct` reading [R1-local]

---

## B. Top 10 Implementation Priorities (Ranked)

### Priority 1: Epoll-Driven Userspace Drain + Batch Processing
**What:** Replace the `ring.next()` + `sleep(1ms)` loop with epoll-triggered aggressive drain and batched serialization/output.

**Why:** Current userspace is the single biggest throughput bottleneck. The 1ms sleep caps drain rate and causes drops under burst. [R3-cloud: ranked #1 optimization, "~1.5x–4x consumer throughput"; R3-attempt-3 confirms]

**How:**
- Use ring buffer's epoll fd for event notification instead of periodic sleep
- On wakeup, drain all available records before sleeping again
- Decouple ingestion from serialization via a lock-free channel (e.g., `crossbeam`)
- Batch format/output (avoid per-event `writeln!`)
- Pin consumer thread to a dedicated CPU where possible

**Complexity:** S — Userspace-only change, no kernel modifications.

**Dependencies:** None. Do this first.

**Code reference:** `sensor-ebpf/src/main.rs` lines with `ring.next()` + `thread::sleep(Duration::from_millis(1))`

---

### Priority 2: In-Kernel Process State Map (TGID-keyed)
**What:** Maintain a `proc_map: LRU_HASH<u32, ProcessEntry>` in kernel, updated on fork/exec/exit, with parent-key chain for ancestry.

**Why:** All 3 production EDR systems (Tracee, Tetragon, Falco) maintain process state in-kernel. Our sensor sets `ppid = 0` and has no ancestry — this is the single biggest detection quality gap. [R2-cloud: ranked #2 pattern; R2-local: "Adopt Tetragon's pattern"; R4-cloud: required for nearly all behavioral detections]

**How (Tetragon pattern):**
```rust
#[repr(C)]
struct ProcessEntry {
    pid: u32,
    ktime: u64,           // (pid, ktime) survives PID recycling
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
```
- Hook `sched_process_fork` (or `kprobe/wake_up_new_task`) → create child entry with `parent_key`
- Hook `sched_process_exec` → update binary, path, creds
- Hook `sched_process_exit` → delete entry

**Complexity:** M — New maps, new hooks, careful lifecycle management.

**Dependencies:** None, but benefits enormously from Priority 3.

---

### Priority 3: Raw sys_enter/sys_exit Dispatcher + Per-Task Correlation
**What:** Implement Tracee-style syscall correlation with raw tracepoints, tail-call dispatch, and per-TID inflight state.

**Why:** Current hooks are enter-only and independent. Without return values, we can't distinguish successful operations from failed attempts — a fundamental detection quality problem. [R2-cloud: ranked #1 pattern, "Very high impact"; R2-local: detailed implementation sketch with Rust code; R4-cloud: required for success/failure discrimination]

**How:**
- `raw_tracepoint/sys_enter` → stash syscall ID + args + timestamp in `task_state_map[tid]`
- `raw_tracepoint/sys_exit` → lookup inflight state, capture return value, emit event
- `PROG_ARRAY` maps for tail-call dispatch by syscall ID (Tracee layered model: init → filter → handler stages)
- Migrate existing per-syscall tracepoints to be handlers within the dispatch framework

**Complexity:** L — Architecture-level change to the kernel program model.

**Dependencies:** Benefits from Priority 2 (process state for enrichment).

---

### Priority 4: Execve/Execveat Argv & Environment Capture
**What:** Capture argv summary, key environment variables (LD_PRELOAD, LD_LIBRARY_PATH), and CWD on process exec.

**Why:** Ranked #1 new hook by MITRE coverage impact — enables detection of T1059.* (command/scripting), T1574.* (execution hijack), T1036 (masquerading), T1204.002 (user execution). Without argv, our ProcessExec event only has the binary path. [R4-cloud: Priority 1; R2-local: "Biggest multiplier: process intent/context for almost all detections"]

**How:**
- Read `mm->arg_start..arg_end` in-kernel at exec time (Tetragon/Falco pattern)
- Use multi-stage tail calls for large argv (verifier budget management — Falco pattern) [R2-cloud §8]
- Capture first N bytes of argv (e.g., 512B) + hash of full argv
- Check key env vars: `LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`
- Emit as enriched `ProcessExecEvent` variant

**Complexity:** M — Requires multi-stage tail calls for verifier compliance.

**Dependencies:** Priority 3 (dispatcher framework).

---

### Priority 5: Ring Buffer Drop Telemetry + Backpressure
**What:** Add per-CPU drop counters for ring reserve failures, per-event-kind rate counters, and adaptive backpressure signaling.

**Why:** Currently, when `EVENTS.reserve()` returns `None`, the event is silently dropped with no observability. This makes it impossible to tune buffer sizing or detect saturation. [R3-cloud: ranked #3, "High confidence operational win"; R3-attempt-3 confirms]

**How:**
```rust
#[map]
static DROP_COUNTERS: PerCpuArray<DropStats> = PerCpuArray::with_max_entries(1, 0);

#[repr(C)]
struct DropStats {
    reserve_fail: u64,
    by_kind: [u64; 16],  // per EventKind
}
```
- Increment on every `reserve()` failure
- Userspace periodically reads and aggregates counters
- Emit "sensor health" events with drop rates, CPU utilization

**Complexity:** S — Small kernel + userspace change.

**Dependencies:** None.

---

### Priority 6: LSM Hooks — `security_file_permission` + `security_sb_mount`
**What:** Add LSM hooks for precise file permission checks and mount operations.

**Why:** `security_file_permission` is the #2 MITRE coverage hook, unlocking T1003.008 (shadow read), T1552.001 (credential files), T1098.004 (SSH key persistence), T1546.004 (shell config), T1053.003 (cron). Mount hooks are #3, required for T1611 (container escape). [R4-cloud: Priorities 2–3; R2-local §10: shared library detection via `security_mmap_file`]

**How:**
- Use Aya's `#[lsm]` macro (confirmed working) [R1-cloud]
- `security_file_permission`: emit events for read/write on sensitive path prefixes (in-kernel prefix filter via map)
- `security_sb_mount`: emit mount source, target, fs type, flags
- `security_mmap_file`: detect shared library loading (PROT_EXEC on file-backed mappings) [R2-local §10]
- All LSM hooks observe-only (return 0), no blocking initially

**Complexity:** M — New program type, requires `CONFIG_BPF_LSM` and kernel ≥5.7.

**Dependencies:** None, but enriched by Priority 2 (process context in events).

---

### Priority 7: In-Kernel Filtering (Syscall Bitmap + Cgroup/Container Scope)
**What:** Add map-backed coarse filtering to suppress uninteresting events before ring buffer submission.

**Why:** Without filtering, file-open events alone can saturate the pipeline. Tracee and Tetragon both gate events in-kernel before submit. [R2-cloud §4: "major event volume reduction"; R3-cloud §5: selective field capture; R2-local §8: detailed bitmap implementation]

**How (start simple, evolve):**
```rust
// Phase 1: syscall-level bitmap
#[map]
static SYSCALL_FILTER: Array<u64> = Array::with_max_entries(8, 0);  // 512 syscalls

// Phase 2: container scope filter
#[map]
static CGROUP_FILTER: HashMap<u64, u8> = HashMap::with_max_entries(1024, 0);
```
- Check syscall bitmap in sys_enter_init (cheapest filter, before saving args)
- Check cgroup filter after having task context
- Userspace populates filter maps at startup and on policy change
- Future: Tracee-style policy bitmaps with `BPF_MAP_TYPE_HASH_OF_MAPS` for hot reload [R2-local §8.1]

**Complexity:** M — Map design + plumbing, but individual filter checks are straightforward.

**Dependencies:** Priority 3 (dispatcher framework provides the filtering insertion point).

---

### Priority 8: String/Path Capture Optimization (Two-Tier Policy)
**What:** Implement metadata-first capture with selective path enrichment to reduce per-event CPU cost.

**Why:** `bpf_probe_read_user_str_bytes` for file paths is the most expensive per-event operation on file-heavy workloads. [R3-cloud §5: "usually the biggest per-event CPU reduction on file-heavy streams"; R3-attempt-3 §1.3 confirms]

**How:**
- **Tier 1 (hot path):** Capture only fixed metadata (pid/tgid/uid/flags/inode) + truncated basename (96B cap)
- **Tier 2 (selective):** Full path capture only when in-kernel policy filter matches (suspicious path prefix, security-relevant operation)
- Avoid duplicate reads for correlated events — cache path metadata per task in per-CPU map when feasible
- Prefer tracepoint native fields over additional helper reads

**Complexity:** M — Requires careful restructuring of existing event paths.

**Dependencies:** Priority 7 (filtering determines which events get tier-2 enrichment).

---

### Priority 9: Namespace Transition Hooks (setns/unshare/clone3)
**What:** Add hooks for namespace manipulation syscalls — the primary container escape vector.

**Why:** Ranked #4 in MITRE hook plan. T1611 (Escape to Host) is rated Critical severity and currently invisible to our sensor. [R4-cloud: Priority 4; R2-cloud §10: "container escape detection pattern"]

**How:**
- `tracepoint/syscalls/sys_enter_setns` — capture target fd + namespace type flags
- `tracepoint/syscalls/sys_enter_unshare` — capture namespace flags
- `tracepoint/syscalls/sys_enter_clone3` — capture namespace-related clone flags
- Correlate with process state map for container-vs-host namespace context
- Suppress known-benign orchestrator noise (containerd, kubelet) via comm/cgroup filter

**Complexity:** S-M — Standard tracepoint hooks, but needs good filtering to avoid noise.

**Dependencies:** Priority 2 (process state for container context discrimination).

---

### Priority 10: Socket Lifecycle Hooks (connect/bind/listen/accept4)
**What:** Extend network visibility beyond TCP state transitions to capture full socket lifecycle.

**Why:** Current sensor only sees established TCP connections. Missing bind/listen means no detection of rogue listeners, reverse shells (stdio-over-socket), or port forwarding. Ranked #5 in MITRE hook plan. [R4-cloud: Priority 5; R4-cloud §6: "reverse shells, tunnels, rogue listeners, lateral pivoting"]

**How:**
- `tracepoint/syscalls/sys_enter_connect` — capture sockaddr (AF_INET, AF_INET6, AF_UNIX)
- `tracepoint/syscalls/sys_enter_bind` — capture port and address
- `tracepoint/syscalls/sys_enter_listen` — capture backlog
- `tracepoint/syscalls/sys_enter_accept4` — capture flags
- Use sys_exit correlation (Priority 3) for success/failure

**Complexity:** M — Multiple new hooks, sockaddr parsing in BPF.

**Dependencies:** Priority 3 (dispatcher for entry/exit correlation).

---

### Summary Matrix

| # | Priority | Impact | Complexity | Dependencies |
|---|----------|--------|------------|--------------|
| 1 | Epoll drain + batching | Very High | S | None |
| 2 | Process state map | Very High | M | — |
| 3 | Syscall dispatcher + correlation | Very High | L | Benefits from #2 |
| 4 | Execve argv/env capture | Very High | M | #3 |
| 5 | Drop telemetry + backpressure | High | S | None |
| 6 | LSM hooks (file_perm, mount, mmap) | High | M | — |
| 7 | In-kernel filtering | High | M | #3 |
| 8 | String/path optimization | Medium-High | M | #7 |
| 9 | Namespace transition hooks | High | S-M | #2 |
| 10 | Socket lifecycle hooks | High | M | #3 |

**Recommended execution order:** 1 → 5 → 2 → 3 → 4 → 6 → 7 → 9 → 10 → 8

(Start with quick wins #1 and #5, then build foundational architecture #2–3, then layer capabilities.)

---

## C. Architecture Evolution Plan

### Current State
```
12 independent tracepoint/kprobe programs
  → single 16MB RingBuf
    → sleep-loop userspace consumer
      → per-event JSON to stdout

Problems:
- No process ancestry (ppid=0)
- No syscall success/failure
- No in-kernel filtering
- No drop visibility
- Userspace bottleneck (1ms sleep loop)
```

### Next State (Priorities 1–5, ~2–3 months)
```
raw_tracepoint sys_enter/sys_exit dispatcher
  → tail-call dispatch by syscall ID
  → per-TID inflight correlation map
  → in-kernel process state map (TGID-keyed, fork/exec/exit maintained)
  → per-CPU drop counters
  → single 16-64MB RingBuf
    → epoll-driven aggressive drain
      → lock-free channel → batch serializer
        → structured output (JSON/OTLP)

Wins: return values, process ancestry, drop visibility, 3-5x throughput
```

### Target State (Priorities 6–10 + beyond, ~6–9 months)
```
Syscall dispatcher + LSM hooks + targeted kprobes
  → map-backed policy filtering (syscall bitmap → cgroup scope → path prefix)
  → process state enrichment (CO-RE: creds, namespaces, caps)
  → two-tier capture (metadata-first, selective enrichment)
  → sharded RingBufs (hash-of-map, 4–16 shards)
    → multi-thread epoll drain
      → userspace behavioral correlation engine
        → OTLP export to logfwd pipeline

Wins: MITRE ~73% coverage, in-kernel noise reduction, 100K+ eps
```

### Patterns to Adopt from Production EDR

| Pattern | Source | Adopt? | Rationale |
|---------|--------|--------|-----------|
| Raw tracepoint sys_enter/exit dispatch | Tracee | **Yes** | Foundation for all syscall-level detection [R2-cloud, R2-local] |
| TGID-keyed process map with parent chain | Tetragon | **Yes** | In-kernel ancestry is essential for policy and detection [R2-cloud, R2-local] |
| CO-RE task_struct enrichment | All three | **Yes** | PPID, namespace IDs, creds, caps — dramatically better attribution [R2-cloud §3] |
| Layered tail-call dispatch (init/filter/handler) | Tracee | **Yes** | Clean separation of concerns, verifier-friendly [R2-local §7] |
| Policy bitmap filtering | Tracee | **Yes (simplified)** | Start with syscall + cgroup; evolve to full bitmap [R2-local §8] |
| Cgroup rate throttling | Tetragon | **Defer** | Nice to have, but not critical until we see production volume issues |
| Per-CPU perf buffer topology | Falco | **Skip** | Ringbuf ordering is more valuable for EDR event correlation; revisit only if measured contention at >100K eps [R3-cloud §2] |
| Packet-level DNS capture | Tracee | **Skip for now** | High complexity (cgroup/skb); socket-level port 53 filtering gives 80% value at 20% effort [R2-cloud §6] |
| fmod_ret LSM blocking | Tetragon | **Defer** | Observe-only first; blocking introduces stability risk |
| Heavy userspace state machine | Falco | **Adopt partially** | Userspace process table + behavioral correlation — but keep kernel state for latency-sensitive policy [R2-cloud §2] |

---

## D. Kernel Version Strategy

### Kernel 6.1 (Current Target) — Full Capability
Everything in our implementation plan works on 6.1:

| Feature | Min Kernel | Available on 6.1? |
|---------|-----------|-------------------|
| RingBuf | 5.8 | ✅ |
| LSM BPF (`#[lsm]`) | 5.7 | ✅ (requires `CONFIG_BPF_LSM`) |
| Raw tracepoints | 4.17 | ✅ |
| BTF / CO-RE | 5.2 | ✅ (if BTF present) |
| `bpf_get_current_task_btf()` | 5.11 | ✅ |
| fentry/fexit | 5.5 | ✅ |
| Tail calls (reliable) | 5.10+ | ✅ |
| BPF-to-BPF function calls | 5.10+ | ✅ |
| Sleepable BPF programs | 5.10 | ✅ |

[Sources: R1-local Appendix B, R3-cloud §8]

### Kernel 6.6 — Incremental Gains
- **BPF LSM cgroup** (kernel 6.0): LSM hooks scoped to cgroups — useful for per-container security policies
- **Improved verifier limits**: larger programs, more complex tail-call chains
- **Better bpftool profiling**: more reliable PMU-based program profiling

**Verdict:** Worth raising to 6.6 as a recommended minimum when available, but not blocking.

### Kernel 6.8–6.9 — Future Opportunities
- **BPF Arena** (6.9): Shared-memory map type for zero-copy event exchange between kernel and userspace. Could eventually replace RingBuf for highest-throughput paths [R3-cloud §6]
- **kprobe_multi** (6.2+): Attach a single BPF program to multiple kprobes — could simplify our multi-hook attachment [R1-local §3.2]

**Verdict:** Track but don't plan on. Arena is the most exciting long-term primitive.

### Recommendation
- **Ship on 6.1** — it has everything we need for the full implementation plan
- **Test CI matrix**: 5.15 LTS, 6.1 LTS, 6.6+ (with and without BTF, with and without `CONFIG_BPF_LSM`) [R1-cloud]
- **Document which features degrade gracefully** on older kernels (e.g., LSM hooks simply not attached if `CONFIG_BPF_LSM` absent)

---

## E. Performance Optimization Plan

### Ring Buffer Tuning

| Parameter | Current | Recommended | Rationale |
|-----------|---------|-------------|-----------|
| Buffer size | 16 MB | 16–64 MB (configurable) | 16 MB is ~100ms at current rates; at 100K eps with paths, only tens of ms headroom [R3-cloud §2, R3-attempt-3 §2.2] |
| Topology | Single global | Single → 4–16 shards (phase 2) | Reduce MPSC spinlock contention under multi-CPU bursts [R3-cloud §2] |
| Consumer model | `next()` + 1ms sleep | Epoll + aggressive drain | "~1.5x–4x consumer throughput" [R3-cloud §4] |
| Notification | Implicit | Epoll when idle, busy-drain when active | Hybrid: low idle CPU, low latency on wakeup [R3-cloud §2] |

**Sizing formula:** `ring_bytes >= peak_eps × avg_event_bytes × burst_window_sec × safety_factor`

Example: 100K eps × 160B × 0.25s × 2.0 = ~8 MB minimum. With path-heavy events (512B avg): ~25 MB minimum. [R3-cloud §2]

### Filtering Strategy

**Layer 1 — In-kernel (cheapest first):**
1. Syscall bitmap: skip unmonitored syscalls before any work
2. Self-PID filter: skip events from sensor process (already done in userspace; move to kernel)
3. Cgroup/container scope: emit only from monitored containers
4. Path prefix filter: tier-2 enrichment only for security-relevant paths

**Layer 2 — Userspace:**
1. Process tree context enrichment
2. Behavioral correlation (multi-event sequences)
3. Risk scoring and ATT&CK tactic stitching
4. Allowlist profiling for known-benign patterns

[R2-cloud §4, R2-local §8, R3-cloud §5, R4-cloud §10]

### Throughput Targets

| Metric | Current (estimated) | After Phase 1 (#1, #5) | After Phase 2 (#2–#7) |
|--------|--------------------|-----------------------|-----------------------|
| Sustainable ingest | ~10K eps | ~50K eps | 100–150K eps |
| Drop rate under burst | Unknown (no telemetry) | Observable, bounded | <0.1% at target load |
| CPU overhead | Unknown | <3% typical node | <5% typical node |
| p99 event latency | ~1–2ms (sleep-dominated) | <100µs | <50µs |

[R3-cloud: "50K–150K events/sec per host class" as realistic production targets]

### Profiling Methodology
1. Enable `kernel.bpf_stats_enabled=1` (temporarily)
2. Use `bpftool prog show` for per-program `run_time_ns`, `run_cnt`
3. Use `bpftool prog profile ... cycles instructions` for PMU metrics
4. Use `BPF_PROG_TEST_RUN` for deterministic microbenchmarks
5. Run saturation test matrix: event rate sweep (10K → 200K eps) × event mix × buffer size × consumer strategy [R3-cloud §7]

---

## F. MITRE ATT&CK Coverage Roadmap

### Current Coverage (12 events)
- **Overall:** ~30% of 60 high-value Linux ATT&CK techniques
- **High-confidence direct:** ~15% (9/60 techniques)
- **Conditional (needs enrichment):** ~15% additional (9/60)

[R4-cloud §1]

### After Top 10 Priorities (estimated)
With Priorities 1–10 implemented (especially #2 process state, #3 correlation, #4 argv, #6 LSM, #9 namespace, #10 socket):

- **Overall:** ~65–70% coverage
- **High-confidence direct:** ~40%
- **Key techniques unlocked:**
  - T1059.* (Command/Scripting) — via argv capture
  - T1611 (Container Escape) — via namespace + mount hooks
  - T1574.* (Execution Hijack) — via LD_PRELOAD in env + security_mmap_file
  - T1003.007/.008 (Credential Dumping) — via security_file_permission
  - T1055 (Process Injection) — via process_vm_writev + ptrace correlation
  - T1070.004/.006 (Anti-Forensics) — via delete + timestomp hooks
  - T1090/T1572 (Proxy/Tunneling) — via socket lifecycle

### After Extended Hook Plan (Priorities 1–12 from MITRE report)
Adding `security_bprm_check`, `utimensat`, `chmod/chown/xattr`, `openat2`, `prctl/capset`, and procfs access hooks:

- **Overall:** ~73% coverage
- **High-confidence direct:** ~48%

[R4-cloud §12]

### Remaining Gaps (require specialized approaches)
| Gap | Why hard | Priority |
|-----|----------|----------|
| Cloud control-plane abuse (T1578) | Not visible from host kernel | Low — different sensor surface |
| Identity/authentication events | Requires PAM/auditd integration | Medium |
| eBPF/tracing tamper (T1562.001) | Requires hooking `bpf()` syscall — meta-detection | Medium |
| Network packet-level DNS (high-fidelity) | Requires cgroup/skb programs | Low — port-53 heuristic covers 80% |
| Hardware/firmware persistence (T1542) | Below OS abstraction | Out of scope |

### Detection Engineering Patterns (Multi-Event)
The MITRE report identifies 7 concrete behavioral detection sequences we can implement once the top 10 priorities are in place [R4-cloud §10]:

1. **Reverse shell:** ProcessExec(shell) → TcpConnect(external) within 1s
2. **Web exploit chain:** webserver spawns shell → connect → sensitive file read
3. **Fileless injection:** memfd_create → execveat/ptrace → network beacon
4. **Persistence:** write cron/systemd path → daemon-reload/start
5. **Container escape:** container process → setns/unshare/mount
6. **Credential theft:** read /etc/shadow → compress → exfiltrate
7. **Anti-forensics:** mass unlink/rename + timestomp burst

---

## G. Disagreements, Weak Evidence, and Open Questions

### Where Reports Disagreed

| Topic | Cloud Report 1 | Local Report 1 | Resolution |
|-------|----------------|-----------------|------------|
| CO-RE gap severity | "Not a blocker" — partial ergonomics, strong underlying support | "Significant" — no kernel-side CO-RE relocations, recommends hybrid C | **Go with local report's assessment.** For an EDR that must work across kernel versions, CO-RE portability is not optional. Plan for hybrid C from the start. |
| Ring buffer sharding urgency | R3-cloud ranks it #4 (Medium-High) | Not directly addressed | **Defer to Phase 2.** No evidence our current 12 hooks generate enough contention to warrant sharding. Measure first with drop telemetry (#5). |
| DNS monitoring approach | R2-cloud: Tracee's packet path is stronger | R2-local: port-53 selectors are cheaper | **Start with socket-level port 53 filtering** (Priority 10). Upgrade to packet capture only if detection quality is insufficient. |

### Where Evidence Was Weak or Speculative

| Claim | Source | Confidence | Why weak |
|-------|--------|------------|----------|
| "1.5x–4x consumer throughput from epoll drain" | R3-cloud | Medium | Directional estimate, not benchmarked on our specific event mix and output format |
| Ringbuf sharding reduces contention at >50K eps | R3-cloud | Medium | Based on general MPSC contention theory; no EDR-specific benchmarks cited |
| "~73% MITRE coverage after 12 hooks" | R4-cloud | Medium | Coverage % depends heavily on enrichment quality and detection rule sophistication, not just hook presence |
| Tail-call overhead ~5ns/call on modern kernels | R3-cloud citing LPC paper | High for network context | May differ for syscall tracepoint context; needs measurement |
| io_uring for ringbuf consumption | R3-cloud | Low | Explicitly flagged as "experimental unless benchmarked" — no production evidence |

### Questions Requiring Prototyping

1. **CO-RE hybrid build validation:** Can we reliably load C-compiled BPF objects alongside Rust-compiled ones in the same Aya `Ebpf` instance? Needs a working prototype. [R1-local §7.8, §8.3]

2. **Tail-call depth budget with BPF-to-BPF calls:** Our target architecture uses both. When combined, per-program stack shrinks to 256B. Need to verify our event processing fits within this budget. [R3-cloud §3]

3. **LSM hook availability in practice:** `CONFIG_BPF_LSM` is not enabled in all distro kernels (e.g., some Ubuntu/RHEL default configs). Need to survey target deployment kernels and implement graceful degradation. [R1-cloud, R4-cloud]

4. **Verifier instruction limits for enriched events:** Multi-stage tail-call argv capture (Falco pattern) needs real verifier testing with our event struct sizes and helper call patterns.

5. **Per-CPU map aggregation overhead in userspace:** How often should we read per-CPU drop/rate counters? Need to benchmark map read cost vs. freshness requirements.

6. **Ring buffer sizing under realistic workload:** Run saturation tests with representative event mix (file-heavy container workload) to determine whether 16 MB, 32 MB, or 64 MB is the right default.

---

## Appendix: Report Source Key

| Key | Report | Type |
|-----|--------|------|
| R1-cloud | `01-rust-ebpf-ecosystem-report.md` (cloud) | Aya vs alternatives feature matrix |
| R1-local | `01-rust-ebpf-ecosystem-report.md` (local sub-agent) | Deep dive with code examples, migration analysis |
| R2-cloud | `02-edr-architecture-patterns-report.md` (cloud) | Tracee/Tetragon/Falco architecture comparison |
| R2-local | `02-edr-architecture-patterns-report.md` (local sub-agent) | Extended patterns with implementation sketches |
| R3-cloud | `03-high-performance-ebpf-report.md` (cloud) | Performance model, ring buffer tuning, optimization plan |
| R3-attempt-3 | Alternate attempt for R3 | CoNEXT 2025 paper quantified costs, confirmed findings |
| R4-cloud | `04-security-coverage-mitre-report.md` (cloud) | MITRE ATT&CK mapping, hook prioritization, detection patterns |
| R5 | (unavailable — all attempts rate-limited) | Bleeding-edge kernel features |
