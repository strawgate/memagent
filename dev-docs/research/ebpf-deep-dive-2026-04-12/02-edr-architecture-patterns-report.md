# eBPF EDR Architecture Patterns: Tracee, Tetragon, Falco

> **Date:** 2026-04-12
> **Purpose:** Understand kernel-side architecture patterns used by leading open-source eBPF EDR tools to close gaps in our sensor.
> **Sources:** Primary analysis of BPF C source code from aquasecurity/tracee, cilium/tetragon, and falcosecurity/libs repositories. All code references verified against `main`/`master` branches as of April 2026.

---

## Table of Contents

1. [Architecture Comparison Table](#1-architecture-comparison-table)
2. [Tracee Architecture Deep Dive](#2-tracee-architecture-deep-dive)
3. [Tetragon Architecture Deep Dive](#3-tetragon-architecture-deep-dive)
4. [Falco Architecture Deep Dive](#4-falco-architecture-deep-dive)
5. [Process Tree Tracking Patterns](#5-process-tree-tracking-patterns)
6. [Entry→Exit Correlation Pattern](#6-entryexit-correlation-pattern)
7. [Tail Call Dispatch Pattern](#7-tail-call-dispatch-pattern)
8. [In-Kernel Filtering Pattern](#8-in-kernel-filtering-pattern)
9. [task_struct Reading: Extracting Process Context](#9-task_struct-reading-extracting-process-context)
10. [Shared Library Loading Detection](#10-shared-library-loading-detection)
11. [DNS Monitoring](#11-dns-monitoring)
12. [Container Awareness](#12-container-awareness)
13. [Top 5 Patterns to Adopt](#13-top-5-patterns-to-adopt)

---

## 1. Architecture Comparison Table

| Dimension | Tracee (Aqua Security) | Tetragon (Cilium/Isovalent) | Falco (Sysdig) |
|-----------|------------------------|-----------------------------|--------------------|
| **Hook strategy** | `raw_tracepoint/sys_enter` + `raw_tracepoint/sys_exit` as unified entry points; kprobes + LSM hooks for non-syscall events | `tracepoint/sched_process_exec` + `kprobe/wake_up_new_task` for process lifecycle; configurable `TracingPolicy` kprobes/tracepoints/LSM | `tp_btf/sys_enter` + `tp_btf/sys_exit` as dispatchers; `sched_process_exec` tracepoint for successful execve |
| **Tail call dispatch** | 8 `BPF_MAP_TYPE_PROG_ARRAY` maps: `sys_enter_init_tail`, `sys_enter_submit_tail`, `sys_enter_tails`, `sys_exit_init_tail`, `sys_exit_submit_tail`, `sys_exit_tails`, `prog_array`, `prog_array_tp` | Per-hook `BPF_MAP_TYPE_PROG_ARRAY` (e.g., `execve_calls[2]`, `kprobe_calls[13]`, `tp_calls[13]`) with named stages: SETUP→FILTER→PROCESS→ARGS→ACTIONS→SEND | `syscall_exit_tail_table` + `syscall_exit_extra_tail_table` for per-syscall dispatch; `custom_sys_exit_calls` for sampling/hotplug logic |
| **Per-task state** | `task_info_map` (LRU_HASH, key=TID, value=`task_info_t`) + `proc_info_map` (LRU_HASH, key=TGID, value=`proc_info_t`) | `execve_map` (HASH, key=TGID, value=`execve_map_value`) persists across entire process lifetime | No persistent per-task map; enrichment at syscall dispatch time from `task_struct` |
| **Process tree** | Userspace tree from fork/exec/exit events; `process_tree_map` in-kernel for filtering only | **In-kernel tree**: `execve_map` entries link via `pkey` (parent key) + `tg_parents_bin` for binary inheritance | Userspace tree from rich fork/exec/exit events with PPID, capabilities, cgroups |
| **In-kernel filtering** | Bitmap-per-policy system: 64 policies evaluated simultaneously via bitwise ops; versioned filter maps (`BPF_MAP_TYPE_HASH_OF_MAPS`) for hot-reload | `TracingPolicy` CRD → compiled to BPF maps: PID filters, namespace filters, capability change filters, argument value matching, LPM-trie string prefix matching | Sampling/dropping mode: `UF_NEVER_DROP`/`UF_ALWAYS_DROP` flags per syscall; `interesting_syscall` bitmap; `drop_failed` flag |
| **Event transport** | `BPF_MAP_TYPE_PERF_EVENT_ARRAY` with per-CPU buffers | `perf_event_output` with `event_output_metric()` wrapper | `BPF_MAP_TYPE_RINGBUF` (per-CPU) in modern driver |
| **Large data** | `save_str_arr_to_buf()` streams argv/envp into perf buffer inline | `data_event_bytes()` sends oversized data (>BUFFER bytes) as separate `MSG_OP_DATA` events via perf | `auxmap` (auxiliary per-CPU map) accumulates variable-length params; tail calls split oversized events |
| **CO-RE support** | Full BTF/CO-RE via `vmlinux.h` + `BPF_CORE_READ`; flavor-based compatibility for old kernels | Full BTF/CO-RE; RHEL7 compatibility via `#ifdef __RHEL7_BPF_PROG` | `tp_btf` programs (BTF-enabled tracepoints) for modern driver; separate classic eBPF driver for older kernels |
| **Language** | BPF C (libbpf), Go userspace | BPF C (custom build), Go userspace | BPF C (libbpf), C++ userspace (libsinsp/libscap) |

---

## 2. Tracee Architecture Deep Dive

### 2.1. Unified Syscall Entry Points

Tracee's core insight: use **two raw tracepoints** as the single gateway for all syscall monitoring.

```
raw_tracepoint/sys_enter        ← TP_PROTO(struct pt_regs *regs, long id)
    │
    ├─→ bpf_tail_call(sys_enter_init_tail, syscall_id)
    │       │
    │       ├─→ sys_enter_init()       ← saves args + task context to task_info_map
    │       │       │
    │       │       ├─→ bpf_tail_call(sys_enter_submit_tail, id)
    │       │       │       └─→ sys_enter_submit()  ← evaluate_scope_filters + emit
    │       │       │               └─→ bpf_tail_call(sys_enter_tails, id)
    │       │       │                       └─→ syscall-specific handler
    │       │       └─→ bpf_tail_call(sys_enter_tails, id)
    │       │               └─→ syscall-specific handler
    │       └─→ (fallthrough: syscall not interesting)
    └─→ (fallthrough: 32-bit compat translation failed)

raw_tracepoint/sys_exit         ← TP_PROTO(struct pt_regs *regs, long ret)
    │
    ├─→ bpf_tail_call(sys_exit_init_tail, syscall_id)
    │       │
    │       └─→ sys_exit_init()        ← marks syscall_traced=false, captures ret
    │               │
    │               ├─→ bpf_tail_call(sys_exit_submit_tail, id)
    │               │       └─→ sys_exit_submit()   ← emit with args + ret
    │               │               └─→ bpf_tail_call(sys_exit_tails, id)
    │               └─→ bpf_tail_call(sys_exit_tails, id)
    └─→ (fallthrough)
```

**Why `raw_tracepoint`?** Unlike regular tracepoints, raw tracepoints give access to the raw `struct pt_regs` and syscall ID without kernel-side formatting. This is critical because:
- It provides register-level access to all 6 syscall arguments
- It fires *before* the kernel processes the syscall (on enter) and *after* (on exit)
- It has lower overhead than kprobes
- The same two programs cover all ~400 syscalls

### 2.2. Per-Task State Tracking

Tracee maintains **two separate state maps**:

```c
// Per-thread state (keyed by TID)
struct task_info_map {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 10240);
    __type(key, u32);              // host TID
    __type(value, task_info_t);    // contains:
    //   task_context_t context     ← cached PPID, namespaces, UID, comm, start_time
    //   syscall_data_t syscall_data ← current syscall args + return value
    //   bool syscall_traced        ← entry/exit correlation flag
    //   u8 container_state         ← STARTED/EXISTED/NONE
};

// Per-process state (keyed by TGID)
struct proc_info_map {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 30720);
    __type(key, u32);              // host TGID
    __type(value, proc_info_t);    // contains:
    //   binary_t binary            ← path + mount namespace for binary filtering
    //   u64 follow_in_scopes       ← which policies are tracking this process
    //   bool new_proc              ← started after tracee (for new_pid filter)
};
```

The `LRU_HASH` type is critical: it auto-evicts the least-recently-used entries, preventing map exhaustion under heavy process churn without requiring explicit cleanup.

### 2.3. `init_task_context()` — Reading Process Context

This function extracts rich context from `task_struct` using BPF CO-RE:

```c
statfunc int init_task_context(task_context_t *tsk_ctx, struct task_struct *task, u32 options)
{
    struct task_struct *leader = get_leader_task(task);                     // task->group_leader
    struct task_struct *parent_process = get_leader_task(get_parent_task(leader));
    // ^ Goes: task → group_leader → real_parent → group_leader
    // This ensures we always get the PARENT PROCESS (not thread)

    tsk_ctx->host_ppid = get_task_pid(parent_process);     // task->pid (host namespace)
    tsk_ctx->tid = get_task_ns_pid(task);                  // via thread_pid->numbers[level].nr
    tsk_ctx->pid = get_task_ns_tgid(task);                 // group_leader's ns PID
    tsk_ctx->ppid = get_task_ns_pid(parent_process);       // parent's ns PID (if same pidns)
    tsk_ctx->pid_id = get_task_pid_ns_id(task);            // pidns->ns.inum
    tsk_ctx->mnt_id = get_task_mnt_ns_id(task);            // nsproxy->mnt_ns->ns.inum
    tsk_ctx->uid = bpf_get_current_uid_gid();              // effective UID
    tsk_ctx->start_time = get_task_start_time(task);       // start_boottime or start_time
    tsk_ctx->leader_start_time = get_task_start_time(leader);
    tsk_ctx->parent_start_time = get_task_start_time(parent_process);
    bpf_get_current_comm(&tsk_ctx->comm, sizeof(tsk_ctx->comm));
    // UTS namespace name (hostname from container's perspective)
    char *uts_name = get_task_uts_name(task);               // nsproxy->uts_ns->name.nodename
    bpf_probe_read_kernel_str(&tsk_ctx->uts_name, TASK_COMM_LEN, uts_name);
}
```

**Key implementation detail**: Tracee reads `start_boottime` (kernel 5.5+) by checking `bpf_core_field_exists(struct task_struct, start_boottime)` at compile time via CO-RE, falling back to `real_start_time` or `start_time` for older kernels. The `(PID, ktime)` pair is used as a *unique process identifier* that survives PID recycling.

### 2.4. The Execve Special Case

Tracee handles execve differently from all other syscalls:

- **On sys_enter**: Emit the execve event with filename, argv, and envp — because after a *successful* exec, the original process memory (containing the argv pointers) is replaced, so the userspace pointers become invalid.
- **On sys_exit**: Only emit if `sys->ret != 0` (the exec *failed*). Failed execs preserve the original address space, so arguments are still valid.

```c
// sys_exit path for execve:
SEC("raw_tracepoint/sys_execve")
int syscall__execve_exit(void *ctx) {
    // ...
    syscall_data_t *sys = &p.task_info->syscall_data;
    if (!sys->ret)      // ret == 0 means success
        return -1;      // skip — was already emitted on entry
    // For failed execs: emit with args + return code
    save_str_to_buf(&p.event->args_buf, (void *)sys->args.args[0], 0);  // filename
    save_str_arr_to_buf(&p.event->args_buf, (const char *const *)sys->args.args[1], 1);
    save_to_submit_buf(&p.event->args_buf, (void *)&sys->ret, sizeof(long), 3);
    return events_perf_submit(&p);
}
```

### 2.5. In-Kernel Filtering via Policy Bitmaps

Tracee's filtering is **the most sophisticated** of the three tools. It supports up to **64 concurrent policies** evaluated simultaneously using a single u64 bitmap:

```c
statfunc bool evaluate_scope_filters(program_data_t *p) {
    u64 matched_policies = match_scope_filters(p);  // returns bitmap
    p->event->context.matched_policies &= matched_policies;
    return p->event->context.matched_policies != 0;  // any policy matched?
}
```

Each filter type (PID, UID, comm, container, binary path, namespace, cgroup, process tree) independently computes a u64 bitmap of matching policies. These are AND'd together. Filters support both equality and inequality:

```c
// Example: comm filter for policy 2 (comm=who), policy 3 (comm=ping), policy 4 (comm!=who)
// For event from "who":
//   equals_in_policies   = 0b0010  (policy 2 matched equality)
//   match_if_key_missing = 0b1000  (policy 4 has != operator)
//   key_used_in_policies = 0b1010  (policy 2 and 4 reference "who")
//   result = 0b0010 | (0b1000 & ~0b1010) = 0b0010  (only policy 2)
```

Filter maps use **`BPF_MAP_TYPE_HASH_OF_MAPS`** with a version key (u16), enabling atomic hot-reload: userspace builds a new inner map, updates the outer map's version entry, then bumps the config version — all without detaching BPF programs.

---

## 3. Tetragon Architecture Deep Dive

### 3.1. In-Kernel Process Tree

Tetragon's defining feature: it maintains a **persistent in-kernel process tree** via `execve_map`.

```c
// Key: TGID (u32)
// Value: execve_map_value — persists from clone to exit
struct execve_map_value {
    struct msg_execve_key key;     // { .pid = tgid, .ktime = start_ktime }
    __u32 nspid;                   // namespace PID
    struct msg_execve_key pkey;    // PARENT's key — links to parent entry
    __u32 flags;                   // EVENT_COMMON_FLAG_CLONE, EVENT_IN_INIT_TREE
    struct msg_ns ns;              // all 8 namespace inums
    struct msg_capabilities caps;  // effective, inheritable, permitted
    struct binary bin;             // binary path, args, postfix for matchBinaries
};
```

**Lifecycle:**

1. **Clone** (`kprobe/wake_up_new_task`): Creates entry, copies parent's binary, sets `pkey = parent->key`, sets `flags = EVENT_COMMON_FLAG_CLONE`.
2. **Exec** (`tracepoint/sched_process_exec`): Updates entry with new binary, path, args, creds, namespaces. Clears `EVENT_COMMON_FLAG_CLONE` flag.
3. **Exit** (`event_exit_send`): Reads entry to emit exit event with matching `(pid, ktime)`, then deletes entry.

**Parent discovery** (`event_find_parent()`):

```c
// Simplified logic:
FUNC_INLINE struct execve_map_value *event_find_parent(void) {
    struct task_struct *task = (struct task_struct *)get_current_task();
    __u32 pid;
    // Walk up: current -> real_parent -> ... (looking for a TGID in execve_map)
    pid = BPF_CORE_READ(task, real_parent, tgid);
    return execve_map_get_noinit(pid);
}
```

### 3.2. TracingPolicy: Configurable Kernel Hooks

Tetragon's `TracingPolicy` CRD lets users attach kprobes, tracepoints, and LSM hooks dynamically:

```yaml
apiVersion: cilium.io/v1alpha1
kind: TracingPolicy
spec:
  kprobes:
    - call: "security_file_open"
      syscall: false
      args:
        - index: 0
          type: "file"
      selectors:
        - matchPIDs:
            - operator: NotIn
              followForks: true
              values: [0, 1]
          matchNamespaces:
            - namespace: Pid
              operator: NotIn
              values: [4026531836]
          matchActions:
            - action: Sigkill
```

In the kernel, this compiles to a `generic_kprobe_event()` → multi-stage tail call pipeline:

```
generic_kprobe_event()
  └→ TAIL_CALL_FILTER:  generic_kprobe_process_filter()  ← PID/NS/cap filters
       └→ TAIL_CALL_SETUP: generic_kprobe_setup_event()
            └→ TAIL_CALL_PROCESS: generic_kprobe_process_event() ← copy args
                 └→ TAIL_CALL_ARGS: generic_kprobe_filter_arg()  ← arg value matching
                      └→ TAIL_CALL_ACTIONS: generic_kprobe_actions() ← sigkill/override/etc.
                           └→ TAIL_CALL_SEND: generic_kprobe_output() ← perf output
```

### 3.3. Rate Limiting

Tetragon implements in-kernel **cgroup-level rate limiting** for execve events:

```c
// In event_execve():
tail_call(ctx, &execve_calls, 0);  // → execve_rate()

// execve_rate():
int execve_rate(void *ctx) {
    struct msg_execve_event *msg = map_lookup_elem(&execve_msg_heap_map, &zero);
    if (cgroup_rate(ctx, &msg->kube, msg->common.ktime))
        tail_call(ctx, &execve_calls, 1);  // → execve_send()
    return 0;  // rate limited — drop silently
}
```

This prevents a fork-bomb from overwhelming the perf buffer.

### 3.4. Large Data Handling

When argv or envp exceeds the inline buffer (BUFFER ≈ 1024 bytes), Tetragon sends it as a separate data event:

```c
if (args_size < BUFFER && args_size < free_size) {
    // Fits inline — copy directly
    probe_read(args, size, (char *)start_stack);
} else {
    // Too large — send as separate MSG_OP_DATA event
    size = data_event_bytes(ctx, (struct data_event_desc *)args,
                           (unsigned long)start_stack, args_size,
                           (struct bpf_map_def *)&data_heap);
    p->flags |= EVENT_DATA_ARGS;
}
```

Userspace reassembles the event by matching the `data_event_desc` to the parent event.

---

## 4. Falco Architecture Deep Dive

### 4.1. Three Driver Modes

Falco (via the `libs` repository) provides **three kernel instrumentation drivers**:

| Driver | Hook Type | Min Kernel | Notes |
|--------|-----------|------------|-------|
| **Kernel Module** | Tracepoints via `register_trace_*` | 2.6+ | Highest compatibility; requires module loading |
| **Classic eBPF** | Raw tracepoints + kprobes | 4.14+ | No module loading; larger instruction limit needed |
| **Modern eBPF** | `tp_btf/sys_enter` + `tp_btf/sys_exit` | 5.8+ | BTF-enabled; uses ring buffers; tail calls for dispatch |

### 4.2. Modern eBPF Architecture

The modern driver uses BTF-enabled tracepoints (`tp_btf`) as dispatchers:

```c
// syscall_exit dispatcher (simplified):
SEC("tp_btf/sys_exit")
int BPF_PROG(sys_exit, struct pt_regs *regs, long ret) {
    uint32_t syscall_id = extract__syscall_id(regs);

    // Quick-reject: is this syscall interesting?
    if (!syscalls_dispatcher__64bit_interesting_syscall(syscall_id))
        return 0;

    // Sampling logic for load shedding
    if (sampling_logic_exit(ctx, syscall_id))
        return 0;

    // Drop failed syscalls if configured
    if (maps__get_drop_failed() && ret < 0)
        return 0;

    // Dispatch to per-syscall handler via tail call
    bpf_tail_call(ctx, &syscall_exit_tail_table, syscall_id);
    return 0;
}
```

### 4.3. Per-Syscall Handlers with Tail Call Chaining

For complex syscalls like execve, Falco chains multiple tail calls to stay within the BPF verifier's instruction limit:

```
execve_x()           ← params 1-14 (ret, exe, args, tid, pid, ptid, cwd, ...)
  └→ t1_execve_x()   ← params 15-27 (cgroups, env, tty, vpgid, loginuid, flags, caps, ...)
       └→ t2_execve_x() ← params 28-31 (trusted_exepath, pgid, egid, filename) + finalize + submit
```

### 4.4. Auxiliary Map Pattern

Instead of Tracee's inline perf buffer serialization, Falco uses **per-CPU auxiliary maps** for event construction:

```c
struct auxiliary_map *auxmap = auxmap__get();                    // per-CPU array lookup
auxmap__preload_event_header(auxmap, PPME_SYSCALL_EXECVE_19_X); // set event type
auxmap__store_s64_param(auxmap, ret);                           // append param
auxmap__store_charbuf_param(auxmap, arg_start_pointer, MAX_PROC_EXE, USER);
// ... more params ...
auxmap__finalize_event_header(auxmap);                          // set final size
auxmap__submit_event(auxmap);                                   // copy to ring buffer
```

### 4.5. Filtering Philosophy

Falco's in-kernel filtering is deliberately **minimal** compared to Tracee/Tetragon:

- **`interesting_syscall` bitmap**: userspace configures which syscalls to capture
- **Sampling/dropping mode**: time-based sampling (e.g., capture 1/128th of events) with `UF_NEVER_DROP` for critical events (execve, clone)
- **`drop_failed` flag**: drop all events with ret < 0
- **All rule evaluation happens in userspace** via the Falco rules engine (YAML/Lua)

This is a deliberate design choice: Falco prioritizes a rich, composable rule language over in-kernel filtering performance.

---

## 5. Process Tree Tracking Patterns

### 5.1. Pattern A: In-Kernel Tree (Tetragon)

```
execve_map[tgid] = {
    key:   { pid: tgid, ktime: start_ktime },
    pkey:  { pid: parent_tgid, ktime: parent_ktime },  ← PARENT LINK
    nspid: namespace_pid,
    bin:   { path, args, ... },
    caps:  { effective, inheritable, permitted },
    ns:    { uts, ipc, mnt, pid, net, ... },
    flags: CLONE | IN_INIT_TREE,
}
```

**Advantages:** Parent lookup is a single map read. Process context is always available for in-kernel policy decisions. No race between fork event emission and userspace tree update.

**Disadvantages:** Map size limits the number of tracked processes. Clone events must be synchronized (Tetragon uses `kprobe/wake_up_new_task` which fires after the task is fully initialized).

### 5.2. Pattern B: Userspace Tree with In-Kernel Fork Events (Tracee)

Tracee sends rich fork events containing:
- Parent: host_tid, ns_tid, host_pid, ns_pid, start_time
- Child: host_tid, ns_tid, host_pid, ns_pid, start_time
- Leader: host_tid, ns_tid, host_pid, ns_pid, start_time (group leader)
- Up-parent: the first real process (not LWP) in the ancestry

Userspace builds and maintains the tree. In-kernel, `process_tree_map` is a **filter-only** map: userspace populates it with PIDs that should be traced, and the kernel propagates entries on fork:

```c
// In sched_process_fork handler:
eq_t *tgid_filtered = bpf_map_lookup_elem(inner_proc_tree_map, &parent_pid);
if (tgid_filtered) {
    bpf_map_update_elem(inner_proc_tree_map, &child_pid, tgid_filtered, BPF_ANY);
}
```

### 5.3. Pattern C: Rich Events, No In-Kernel Tree (Falco)

Falco extracts PPID at event time via `extract__task_ppid_nr(task)`:
```c
pid_t ppid = extract__task_ppid_nr(task);  // task->real_parent->tgid in init namespace
auxmap__store_s64_param(auxmap, (int64_t)ppid);
```

The userspace library (`libsinsp`) maintains the full process table. No in-kernel tree exists.

### 5.4. Recommendation for Our Sensor

**Adopt Tetragon's pattern** (in-kernel `execve_map`) with Tracee's enrichment:

```
// Pseudo-BPF (Rust/aya equivalent):
HashMap<u32, ProcessEntry>  // key = tgid
struct ProcessEntry {
    pid: u32,
    ktime: u64,          // unique ID: (pid, ktime) survives PID recycling
    ppid: u32,
    parent_ktime: u64,   // parent's unique ID
    nspid: u32,
    binary_path: [u8; 256],
    uid: u32,
    flags: u32,
}
```

Hook points:
- `sched_process_fork` or `kprobe/wake_up_new_task` → create entry, copy parent link
- `sched_process_exec` → update binary, path, creds
- `sched_process_exit` → emit exit event, delete entry

---

## 6. Entry→Exit Correlation Pattern

### 6.1. Tracee's Pattern (Recommended)

The key data structure is a per-thread `syscall_data_t` stored in `task_info_map`:

```c
typedef struct syscall_data {
    uint id;                    // syscall number
    args_t args;                // 6 arguments captured on entry
    unsigned long ret;          // return value captured on exit
    u64 ts;                     // timestamp of entry
} syscall_data_t;

typedef struct task_info {
    task_context_t context;     // cached process context
    syscall_data_t syscall_data;
    bool syscall_traced;        // CORRELATION FLAG
    // ...
} task_info_t;
```

**Flow:**

```
sys_enter_init():
  1. task_info = task_info_map.lookup(tid)  // or create if missing
  2. sys = &task_info->syscall_data
  3. sys->id = ctx->args[1]                 // syscall number
  4. sys->args = read_from_pt_regs(ctx->args[0])  // all 6 args
  5. sys->ts = ktime_get_ns()
  6. task_info->syscall_traced = true        // MARK: entry captured

sys_exit_init():
  1. task_info = task_info_map.lookup(tid)
  2. if (!task_info->syscall_traced) return  // no matching entry — skip
  3. task_info->syscall_traced = false       // MARK: correlation consumed
  4. sys = &task_info->syscall_data
  5. ASSERT(sys->id == get_syscall_id(regs)) // sanity check
  6. sys->ret = ctx->args[1]                 // capture return value
  7. // Now we have: syscall_id + 6 args + ret + timestamp
```

**Pseudo-code for Rust/aya implementation:**

```rust
// BPF map
#[map]
static TASK_INFO: HashMap<u32, TaskInfo> = HashMap::with_max_entries(10240, 0);

#[repr(C)]
struct SyscallData {
    id: u32,
    args: [u64; 6],
    ret: i64,
    ts: u64,
}

#[repr(C)]
struct TaskInfo {
    syscall: SyscallData,
    traced: bool,
}

// sys_enter handler:
fn handle_sys_enter(ctx: &RawTracePointContext) -> Result<(), i64> {
    let tid = bpf_get_current_pid_tgid() as u32;
    let info = TASK_INFO.get_ptr_mut(&tid).ok_or(0)?;
    unsafe {
        (*info).syscall.id = ctx.args[1] as u32;
        (*info).syscall.args = read_regs(ctx.args[0]);
        (*info).syscall.ts = bpf_ktime_get_ns();
        (*info).traced = true;
    }
    Ok(())
}

// sys_exit handler:
fn handle_sys_exit(ctx: &RawTracePointContext) -> Result<(), i64> {
    let tid = bpf_get_current_pid_tgid() as u32;
    let info = TASK_INFO.get_ptr_mut(&tid).ok_or(0)?;
    unsafe {
        if !(*info).traced { return Ok(()); }
        (*info).traced = false;
        (*info).syscall.ret = ctx.args[1] as i64;
        // Now emit event with full context
    }
    Ok(())
}
```

### 6.2. Why Not Just Hook sys_exit?

- sys_exit doesn't provide the original arguments (registers are clobbered)
- For execve, the entire address space changes on success — argv pointers are gone
- Without entry data, you can't tell *what file* was opened, *what address* was connected to, etc.

---

## 7. Tail Call Dispatch Pattern

### 7.1. The Problem

A single BPF program cannot handle all ~400 syscalls — the verifier would reject it for exceeding instruction limits. Even with `BPF_COMPLEXITY_LIMIT_INSNS` at 1M (kernel 5.2+), the combined logic is too complex.

### 7.2. Tracee's Pattern: Layered Dispatch

Tracee uses **8 `BPF_MAP_TYPE_PROG_ARRAY` maps** organizing tail calls into phases:

```
Phase 1 (init):    sys_enter_init_tail[syscall_id]    → saves args
Phase 2 (submit):  sys_enter_submit_tail[syscall_id]  → evaluate filters + emit
Phase 3 (custom):  sys_enter_tails[syscall_id]        → syscall-specific logic
```

Each map is indexed by syscall number. Userspace loads the appropriate BPF program into each slot at startup. If a slot is empty, `bpf_tail_call()` falls through silently (returns to caller).

### 7.3. Tetragon's Pattern: Named Pipeline Stages

Tetragon uses **numbered stage indexes** within a single prog_array per hook type:

```c
enum {
    TAIL_CALL_SETUP   = 0,   // initialize msg struct
    TAIL_CALL_PROCESS = 1,   // copy arguments
    TAIL_CALL_FILTER  = 2,   // evaluate selectors
    TAIL_CALL_ARGS    = 3,   // match argument values
    TAIL_CALL_ACTIONS = 4,   // execute actions (sigkill, override, etc.)
    TAIL_CALL_SEND    = 5,   // output to perf buffer
    TAIL_CALL_PATH    = 6,   // resolve file paths
};
```

### 7.4. Falco's Pattern: Per-Syscall + Overflow

```c
// Main dispatch: tail call to per-syscall handler
bpf_tail_call(ctx, &syscall_exit_tail_table, syscall_id);

// Inside handler, when program is too large for verifier:
bpf_tail_call(ctx, &syscall_exit_extra_tail_table, T1_EXECVE_X);
// And again:
bpf_tail_call(ctx, &syscall_exit_extra_tail_table, T2_EXECVE_X);
```

### 7.5. Recommendation

**Adopt Tracee's layered dispatch** — it cleanly separates concerns:

```
Program 1: sys_enter dispatcher (raw_tracepoint/sys_enter)
  └→ tail_call(init_tail, id)

Program 2: sys_enter_init (captures args, common to all syscalls)
  └→ tail_call(handler_tail, id) [optional: for syscall-specific enter logic]

Program 3: sys_exit dispatcher (raw_tracepoint/sys_exit)
  └→ tail_call(exit_init_tail, id)

Program 4: sys_exit_init (captures ret, correlates with entry)
  └→ tail_call(handler_tail, id) [optional: for syscall-specific exit logic]
```

---

## 8. In-Kernel Filtering Pattern

### 8.1. Tracee's Bitmap Filtering (Most Powerful)

The core insight: represent each policy as a **bit position** in a u64. Every filter produces a u64 bitmap of matching policies. AND them all together:

```
matched = ~0ULL                               // start: all policies match
matched &= pid_filter_result                  // narrow by PID
matched &= uid_filter_result                  // narrow by UID
matched &= container_filter_result            // narrow by container status
matched &= comm_filter_result                 // narrow by process name
matched &= namespace_filter_result            // narrow by namespace ID
matched &= binary_filter_result               // narrow by binary path
matched &= cgroup_filter_result               // narrow by cgroup ID
matched &= process_tree_filter_result         // narrow by process ancestry
matched &= enabled_policies                   // mask to only enabled policies

if (matched == 0) DROP                        // no policy matched → don't emit
event.matched_policies = matched              // tell userspace which policies matched
```

Each filter lookup returns an `eq_t` struct:
```c
typedef struct eq {
    u64 equals_in_policies;       // policies where this key triggers an "equal" match
    u64 key_used_in_policies;     // policies that reference this key at all
} eq_t;
```

**Hot reload**: `BPF_MAP_TYPE_HASH_OF_MAPS` with versioned inner maps. Userspace builds the new filter set, swaps the inner map, bumps `config.policies_version`. Zero-downtime updates.

### 8.2. Tetragon's Selector Filtering

Tetragon's filters are compiled from `TracingPolicy` YAML selectors into BPF maps:

- **PID matching**: exact values, ranges
- **Namespace matching**: compare namespace inums from `task_struct` against policy
- **Capability change detection**: compare current caps against stored caps in `execve_map`
- **Argument value matching**: string prefix/suffix via LPM tries, integer ranges, FD-to-path resolution
- **matchBinaries**: binary path matching against a bitset index

The filter stage runs as a **separate tail-called program** (`TAIL_CALL_FILTER`), looping over configured selectors:

```c
ret = generic_process_filter();
if (ret == PFILTER_CONTINUE)
    tail_call(ctx, &kprobe_calls, TAIL_CALL_FILTER);  // more selectors to check
else if (ret == PFILTER_ACCEPT)
    tail_call(ctx, &kprobe_calls, TAIL_CALL_SETUP);   // passed — continue pipeline
// else: PFILTER_REJECT — implicit drop
```

### 8.3. Recommendation for Our Sensor

Start with a **simplified version of Tracee's bitmap pattern**:

1. Define a `FilterConfig` map (ARRAY, 1 entry) with bitmaps of which syscalls to trace
2. Add a `container_filter_map` (HASH, key=cgroup_id) for container-level filtering
3. In sys_enter_init, check the syscall bitmap first — cheapest filter
4. After capturing args, check container/PID/namespace maps — more specific filters

```rust
// Phase 1: syscall-level filter (check before saving args)
#[map]
static SYSCALL_FILTER: Array<u64> = Array::with_max_entries(8, 0);  // 8 * 64 = 512 syscalls

fn is_syscall_interesting(id: u32) -> bool {
    let idx = id / 64;
    let bit = id % 64;
    let bitmap = unsafe { SYSCALL_FILTER.get(idx).unwrap_or(&0) };
    (bitmap >> bit) & 1 == 1
}

// Phase 2: scope filter (check after having task context)
#[map]
static CGROUP_FILTER: HashMap<u64, u8> = HashMap::with_max_entries(1024, 0);

fn is_cgroup_interesting(cgroup_id: u64) -> bool {
    CGROUP_FILTER.get(&cgroup_id).is_some()
}
```

---

## 9. task_struct Reading: Extracting Process Context

### 9.1. Fields All Three Tools Extract

| Field | task_struct Path | Used By |
|-------|-----------------|---------|
| PID (host) | `task->pid` | All |
| TGID (host) | `task->tgid` | All |
| PPID (host) | `task->real_parent->tgid` | All |
| NS PID | `task->thread_pid->numbers[level].nr` | All |
| Start time | `task->start_boottime` (5.5+) or `task->start_time` | Tracee, Tetragon |
| UID | `bpf_get_current_uid_gid()` or `task->cred->uid` | All |
| EUID | `task->cred->euid` | Tetragon, Falco |
| Capabilities | `task->cred->cap_{effective,inheritable,permitted}` | All |
| Comm | `bpf_get_current_comm()` or `task->comm` | All |
| CWD | `task->fs->pwd` (via `d_path`) | Tetragon, Falco |
| Exe path | `task->mm->exe_file->f_path` | Tetragon, Falco |
| Argv | `task->mm->arg_start..arg_end` | Tetragon (in-kernel), Falco (in-kernel) |
| Envp | `task->mm->env_start..env_end` | Tetragon (in-kernel), Falco (in-kernel) |
| Mount NS | `task->nsproxy->mnt_ns->ns.inum` | All |
| PID NS | `task->thread_pid->numbers[level].ns->ns.inum` | All |
| UTS NS | `task->nsproxy->uts_ns->ns.inum` | Tracee, Tetragon |
| Net NS | `task->nsproxy->net_ns->ns.inum` | All |
| Cgroup NS | `task->nsproxy->cgroup_ns->ns.inum` | Tetragon |
| User NS | `task->mm->user_ns->ns.inum` | Tetragon |
| Cgroup ID | `bpf_get_current_cgroup_id()` or cgroup v1 via subsys | All |
| Exit code | `task->exit_code` | Tetragon |
| Audit UID | `task->loginuid.val` | Tetragon, Falco |
| Secure exec | from `bprm->secureexec` | Tetragon |
| Task flags | `task->flags` (PF_KTHREAD, etc.) | Tracee |

### 9.2. CO-RE Compatibility Patterns

All three tools use CO-RE extensively to handle kernel version differences:

```c
// Tetragon: handle PID namespace reading across kernel versions
if (bpf_core_type_exists(struct pid_link)) {
    // Kernel < 5.0: pid is in task->pids[PIDTYPE_PID].pid
    struct task_struct___older_v50 *t = (void *)task;
    pid = BPF_CORE_READ(t, pids[PIDTYPE_PID].pid);
} else {
    // Kernel >= 5.0: pid is in task->thread_pid
    pid = BPF_CORE_READ(task, thread_pid);
}

// Tracee: handle start_time field rename in 5.5
if (bpf_core_field_exists(struct task_struct, start_boottime))
    return BPF_CORE_READ(task, start_boottime);
return BPF_CORE_READ(task, real_start_time);  // or start_time

// Tetragon: handle RHEL7 namespace structs
if (bpf_core_field_exists(nsproxy->uts_ns->ns)) {
    probe_read(&msg->uts_inum, sizeof(msg->uts_inum), _(&nsp.uts_ns->ns.inum));
} else {
    struct uts_namespace___rhel7 *ns = (struct uts_namespace___rhel7 *)_(nsp.uts_ns);
    probe_read(&msg->uts_inum, sizeof(msg->uts_inum), _(&ns->proc_inum));
}
```

### 9.3. Reading Argv In-Kernel vs Userspace

| Approach | Used By | Trade-off |
|----------|---------|-----------|
| Read `mm->arg_start..arg_end` in-kernel | Tetragon, Falco | Reliable for exec events; available after successful exec. Can be large, needs `data_event_bytes()` overflow pattern |
| Read userspace pointers from syscall args | Tracee | Works on sys_enter (before exec replaces memory). Follows the pointer chain: `char **argv` → `char *argv[i]` → string. Subject to TOCTOU |
| Defer to /proc/PID/cmdline | None (too slow) | Would miss short-lived processes |

---

## 10. Shared Library Loading Detection

### 10.1. Tracee: `security_mmap_file` LSM Hook

Tracee detects shared library loading via the `security_mmap_file` LSM hook, which fires whenever `mmap()` is called with `PROT_EXEC`:

```c
// Event: SECURITY_MMAP_FILE
// Fires on: mmap with executable permissions
// → Tracee filters for: file is ELF + has PROT_EXEC + not the main binary
// → Produces: SHARED_OBJECT_LOADED event
```

The detection flow:
1. Hook `security_mmap_file(struct file *file, unsigned long prot, unsigned long flags)`
2. Check `prot & PROT_EXEC` — only care about executable mappings
3. Check the file's ELF magic bytes (via `elf_files_map` cache)
4. Resolve the file path
5. Emit `SHARED_OBJECT_LOADED` with: path, device, inode, ctime

This catches:
- Normal shared library loading via `dlopen()` / dynamic linker
- `LD_PRELOAD` attacks (the preloaded library is still mmap'd with PROT_EXEC)
- In-memory code injection via `memfd_create()` + `mmap(PROT_EXEC)`

### 10.2. Tetragon: TracingPolicy on LSM Hooks

Tetragon supports hooking LSM hooks via TracingPolicy:

```yaml
spec:
  lsm:
    - hook: "file_mprotect"
      args:
        - index: 0
          type: "file"
      selectors:
        - matchArgs:
            - index: 0
              operator: "Postfix"
              values: [".so"]
```

Or for `security_mmap_file`:
```yaml
spec:
  lsm:
    - hook: "mmap_file"
      args:
        - index: 0
          type: "file"
        - index: 1
          type: "int"    # prot
```

Tetragon also supports `fmod_ret` (function return modification) LSM hooks, allowing it to **block** malicious library loads:

```c
__attribute__((section("fmod_ret/security_task_prctl"), used)) long
generic_fmodret_override(void *ctx) {
    __s32 *error = map_lookup_elem(&override_tasks, &id);
    if (!error) return 0;
    map_delete_elem(&override_tasks, &id);
    return (long)*error;  // Override return value → DENY
}
```

### 10.3. Falco

Falco captures `mmap2` and `mprotect` syscalls but does **not** have a dedicated shared library loading event. Detection relies on userspace rules analyzing mmap arguments:

```yaml
- rule: Detect LD_PRELOAD
  condition: >
    evt.type = execve and proc.env contains "LD_PRELOAD"
  output: "LD_PRELOAD detected (env=%proc.env)"
```

### 10.4. Recommendation

Hook `security_mmap_file` (or `security_file_mprotect` as fallback):

```rust
// LSM hook: security_mmap_file
#[lsm(hook = "file_mmap")]
fn detect_so_load(file: *const c_void, prot: u64, _flags: u64) -> i32 {
    if prot & PROT_EXEC == 0 { return 0; }
    // Extract file path, check ELF magic, emit event
    0  // don't block, just observe
}
```

---

## 11. DNS Monitoring

### 11.1. Tracee: Packet-Level DNS Capture

Tracee captures DNS at the **network packet layer** using cgroup/skb BPF programs:

```c
// Network event types include:
SUB_NET_PACKET_DNS = 1 << 7,  // DNS packet detection

// Port-based detection:
#define UDP_PORT_DNS 53
#define TCP_PORT_DNS 53
```

The flow:
1. `cgroup_skb/ingress` and `cgroup_skb/egress` programs intercept all packets
2. Parse IP header → TCP/UDP header → check port 53
3. If port matches, classify as `SUB_NET_PACKET_DNS`
4. Emit the raw packet payload to a separate `net_cap_events` perf buffer
5. **DNS wire format parsing happens entirely in userspace**

Tracee also maintains an `inodemap` (socket inode → task context) and `cgrpctxmap` (packet indexer → event context) to attribute network packets to specific processes — critical because cgroup/skb programs don't have direct access to `bpf_get_current_pid_tgid()`.

### 11.2. Tetragon: Configurable Network Hooks

Tetragon monitors DNS via TracingPolicy kprobes on socket functions:

```yaml
spec:
  kprobes:
    - call: "udp_sendmsg"
      args:
        - index: 0
          type: "sock"
        - index: 1
          type: "char_buf"
      selectors:
        - matchArgs:
            - index: 0
              operator: "DPort"
              values: ["53"]
```

Or hooking `security_socket_sendmsg`:
```yaml
spec:
  lsm:
    - hook: "socket_sendmsg"
      args:
        - index: 0
          type: "socket"
```

DNS wire format parsing is done in userspace via the `ACTION_DNSLOOKUP` action type.

### 11.3. Falco

Falco captures socket syscalls (`sendto`, `recvfrom`, `sendmsg`, `recvmsg`) and filters by port in userspace rules:

```yaml
- rule: DNS Query
  condition: >
    evt.type = sendto and fd.l4proto = udp and fd.rport = 53
```

All DNS parsing is in userspace.

### 11.4. Performance Comparison

| Approach | In-Kernel Cost | Parsing Location | PID Attribution |
|----------|---------------|------------------|-----------------|
| Tracee (cgroup/skb) | Medium — packet parsing overhead | Userspace | Via inode→task map |
| Tetragon (kprobe on udp_sendmsg) | Low — fires only on UDP sends | Userspace | Direct (kprobe context) |
| Falco (syscall sendto) | Low — standard syscall capture | Userspace | Direct |

### 11.5. Recommendation

Hook `udp_sendmsg` and `tcp_sendmsg` with a destination port 53 filter:
- Lower overhead than packet-level capture
- Direct PID attribution (no need for socket→task mapping)
- DNS parsing in userspace (wire format is complex, variable-length, with pointer compression — poor fit for BPF)

---

## 12. Container Awareness

### 12.1. In-Kernel: Cgroup ID Capture

All three tools capture the cgroup ID in-kernel:

```c
// Tracee:
if (p->config->options & OPT_CGROUP_V1) {
    cgroup_id = get_cgroup_v1_subsys0_id(task);  // v1: walk task->cgroups->subsys[0]
} else {
    cgroup_id = bpf_get_current_cgroup_id();       // v2: single hierarchy
}

// Tetragon:
kube->cgrpid = __tg_get_current_cgroup_id(cgrp, cgrpfs_magic);
kube->cgrp_tracker_id = cgrp_get_tracker_id(kube->cgrpid);
// Also reads cgroup name via kernfs node:
name = get_cgroup_name(cgrp);  // cgrp->kn->name
probe_read_str(kube->docker_id, KN_NAME_LENGTH, name);

// Falco:
auxmap__store_cgroups_param(auxmap, task);  // reads full cgroup path
```

### 12.2. Cgroup Name as Container ID

Tetragon reads the **cgroup kernfs name** directly in-kernel. For Docker/containerd, this is typically the container ID:
```
/sys/fs/cgroup/system.slice/docker-<CONTAINER_ID>.scope
                                   ^^^^^^^^^^^^^^^^^^
                                   kn->name = docker-abc123...scope
```

### 12.3. Userspace Enrichment

All three tools resolve cgroup ID → container metadata in userspace:

| Tool | Method |
|------|--------|
| Tracee | `containers_map` (cgroup_id → state); userspace populates via container runtime API + inotify on cgroup filesystem |
| Tetragon | `cgrp_tracker` map; userspace agent watches container runtime (CRI) events |
| Falco | libsinsp queries container runtime APIs (Docker, containerd, CRI-O) at event time |

### 12.4. Recommendation

1. Capture `bpf_get_current_cgroup_id()` on every event (very cheap — single helper call)
2. Maintain a `cgroup_filter_map` (HASH, key=cgroup_id) for filtering
3. In userspace, resolve cgroup ID → container info via:
   - Parse `/proc/PID/cgroup` for initial population
   - Watch containerd/CRI events for runtime updates
   - Cache in a HashMap with cgroup_id as key

---

## 13. Top 5 Patterns to Adopt

Ranked by **impact on closing our gaps** (highest first):

### Pattern 1: Entry→Exit Correlation via Per-Thread State Map

**Impact:** ★★★★★ — Closes our biggest gap. Without return values, we can't distinguish successful from failed operations.

**What to build:**
- A `HashMap<u32, TaskInfo>` (key=TID, value={syscall_id, args[6], ret, ts, traced: bool})
- Hook `raw_tracepoint/sys_enter`: save args + set `traced = true`
- Hook `raw_tracepoint/sys_exit`: if `traced`, capture ret + emit event + set `traced = false`
- Special-case execve: emit on entry (successful) + only emit on exit if failed

**Estimated effort:** ~2 days. This is the foundation that enables everything else.

**Key design decisions:**
- Use `LRU_HASH` to auto-evict stale entries (processes that exit without sys_exit)
- Size: 10240 entries (Tracee's default) covers most workloads
- Include a sanity check: `sys_exit.id == saved.id` to detect mismatches

### Pattern 2: task_struct Context Extraction

**Impact:** ★★★★★ — Closes PPID, UID, capability, and namespace gaps simultaneously.

**What to build:**
- An `init_task_context()` function that reads from `task_struct` via CO-RE:
  - `task->real_parent->tgid` → PPID
  - `task->cred->{euid, cap_effective, cap_inheritable, cap_permitted}` → credentials
  - `task->nsproxy->{mnt_ns, pid_ns, uts_ns, net_ns}->ns.inum` → namespace IDs
  - `task->thread_pid->numbers[level].nr` → namespace PID
  - `task->start_boottime` → process start time (for unique ID)

**Estimated effort:** ~1 day. Most of the work is defining the `vmlinux.h` types and testing CO-RE relocations across kernel versions.

**Key design decisions:**
- Cache the context in the per-task map (don't re-read on every event)
- Use `(tgid, start_boottime)` as a unique process identifier
- Handle the pid_link vs thread_pid difference (kernel < 5.0 vs >= 5.0)

### Pattern 3: Tail Call Dispatch

**Impact:** ★★★★☆ — Enables scalable syscall handling and in-kernel filtering without hitting verifier limits.

**What to build:**
- Two `BPF_MAP_TYPE_PROG_ARRAY` maps: `sys_enter_handlers[512]` and `sys_exit_handlers[512]`
- Unified entry point: `raw_tracepoint/sys_enter` reads syscall ID → tail calls into per-syscall handler
- For syscalls we don't care about: leave the slot empty (tail call silently falls through)
- Userspace configures which slots are populated at startup

**Estimated effort:** ~2 days. aya's `ProgramArray` type supports this directly.

**Key design decisions:**
- Start with a single-level dispatch (no init→submit→handler chain like Tracee)
- Add the init/submit split later when we add filtering
- Populate only the syscalls we care about (openat, execve, connect, etc.)

### Pattern 4: In-Kernel Process Tree

**Impact:** ★★★★☆ — Enables parent→child chain tracking and process-tree-based filtering.

**What to build:**
- `HashMap<u32, ProcessEntry>` (key=TGID) with parent link
- Hook `sched_process_fork`: create child entry, set `parent = (parent_tgid, parent_ktime)`
- Hook `sched_process_exec`: update entry with new binary path, creds
- Hook `sched_process_exit`: emit exit event, delete entry
- Pre-populate at startup by scanning `/proc`

**Estimated effort:** ~3 days. The tricky part is handling the startup race (processes that exist before BPF programs are loaded).

**Key design decisions:**
- Use Tetragon's `(pid, ktime)` tuple as unique key to handle PID recycling
- Copy parent's binary info to child on fork (Tetragon pattern)
- Size map for expected process count (32K entries covers most systems)

### Pattern 5: In-Kernel Filtering via Syscall Bitmap + Cgroup Filter

**Impact:** ★★★☆☆ — Reduces userspace load by 10-100x for targeted monitoring.

**What to build:**
- `Array<u64>` with 8 entries = 512-bit bitmap for syscall filtering
- `HashMap<u64, u8>` for cgroup-level filtering (key=cgroup_id)
- `HashMap<u32, u8>` for PID-level filtering (key=TGID)
- Check syscall bitmap first (cheapest), then cgroup, then PID
- Userspace updates filter maps via map FDs

**Estimated effort:** ~1 day for basic syscall bitmap; ~2 more days for cgroup/PID filters.

**Key design decisions:**
- Start with syscall bitmap only (dramatic impact, trivial to implement)
- Don't try to replicate Tracee's 64-policy bitmap system immediately — that's overkill for v1
- Add cgroup filtering as the second step (enables per-container monitoring)

---

## Appendix A: Map Type Selection Guide

| Pattern | Recommended Map Type | Why |
|---------|---------------------|-----|
| Per-task state | `BPF_MAP_TYPE_LRU_HASH` | Auto-evicts stale entries; no explicit cleanup needed for crashed processes |
| Process tree | `BPF_MAP_TYPE_HASH` | Need explicit delete on exit; don't want auto-eviction of live processes |
| Syscall filter | `BPF_MAP_TYPE_ARRAY` | Fixed-size bitmap; O(1) lookup; no hash overhead |
| Cgroup filter | `BPF_MAP_TYPE_HASH` | Dynamic set of monitored cgroups |
| Tail call dispatch | `BPF_MAP_TYPE_PROG_ARRAY` | Required by `bpf_tail_call()` |
| Event scratch space | `BPF_MAP_TYPE_PERCPU_ARRAY` | No locking; each CPU has its own copy |
| String/path prefix filter | `BPF_MAP_TYPE_LPM_TRIE` | Longest-prefix-match for path-based filtering |
| Hot-reloadable filters | `BPF_MAP_TYPE_HASH_OF_MAPS` | Swap inner maps atomically |

## Appendix B: aya/Rust Implementation Notes

**raw_tracepoint support in aya:** Use `#[raw_tracepoint(tracepoint = "sys_enter")]`. The context provides `args[0]` (pt_regs) and `args[1]` (syscall ID).

**BPF_CORE_READ equivalent:** aya doesn't have CO-RE macros directly, but `aya-ebpf` provides `bpf_probe_read_kernel()` and the `aya-ebpf-bindings` crate generates Rust types from BTF. For field access across kernel versions, use `#[repr(C)]` structs with conditional compilation or runtime BTF checks.

**ProgramArray in aya:** Create with `ProgramArray::try_from(bpf.take_map("MY_PROG_ARRAY"))` in userspace. In BPF, use `maps::ProgramArray` and call `.tail_call(ctx, index)`.

**LRU HashMap:** Supported via `aya_ebpf::maps::LruHashMap`. Declare in BPF with `#[map]` and specify `BPF_MAP_TYPE_LRU_HASH`.

## Appendix C: Source File References

| File | Repository | Key Content |
|------|-----------|-------------|
| `pkg/ebpf/c/tracee.bpf.c` | aquasecurity/tracee | Main BPF program: sys_enter, sys_exit, execve handlers, fork handler |
| `pkg/ebpf/c/common/context.h` | aquasecurity/tracee | `init_task_context()`, `init_program_data()` |
| `pkg/ebpf/c/common/filtering.h` | aquasecurity/tracee | `evaluate_scope_filters()`, bitmap filtering logic |
| `pkg/ebpf/c/common/task.h` | aquasecurity/tracee | `get_task_ppid()`, `get_task_ns_pid()`, namespace readers |
| `pkg/ebpf/c/maps.h` | aquasecurity/tracee | All BPF map definitions |
| `pkg/ebpf/c/types.h` | aquasecurity/tracee | `task_context_t`, `event_context_t` type definitions |
| `bpf/process/bpf_execve_event.c` | cilium/tetragon | `event_execve()`, `execve_rate()`, `execve_send()` |
| `bpf/process/bpf_fork.c` | cilium/tetragon | `event_wake_up_new_task()` — process tree construction |
| `bpf/process/bpf_exit.h` | cilium/tetragon | `event_exit_send()` — process cleanup |
| `bpf/process/bpf_process_event.h` | cilium/tetragon | `get_namespaces()`, `get_current_subj_creds()`, parent tracking |
| `bpf/process/bpf_generic_kprobe.c` | cilium/tetragon | Generic kprobe pipeline with tail call stages |
| `bpf/process/bpf_generic_tracepoint.c` | cilium/tetragon | Generic tracepoint pipeline |
| `driver/modern_bpf/programs/.../execve.bpf.c` | falcosecurity/libs | Execve handler with tail call chaining |
| `driver/modern_bpf/.../syscall_exit.bpf.c` | falcosecurity/libs | Syscall exit dispatcher with sampling logic |
