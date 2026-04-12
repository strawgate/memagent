# EDR Architecture Patterns: How Tracee, Tetragon, and Falco Build Their eBPF Engines

You are handling one workstream inside a larger Codex Cloud fanout for this repository.

## Objective

Deep-dive into how the three leading open-source eBPF security tools (Tracee, Tetragon, Falco) architect their kernel programs and userspace enrichment. Extract concrete patterns we should adopt for our EDR sensor.

## Why this workstream exists

We have a working eBPF sensor with 12 independent hook programs, but it lacks the architectural sophistication of production EDR tools. Specifically:

- **No process tree tracking** — we capture TGID/PID but can't build parent→child chains or correlate events across a process lifetime
- **No entry→exit correlation** — we only hook sys_enter, so we never know if operations succeeded
- **No tail call dispatch** — each syscall is a separate BPF program, which doesn't scale and can't share state
- **No in-kernel filtering** — we emit every event and filter in userspace (wasteful)
- **No task_struct reading** — we can't get PPID, effective UID, capabilities, namespace info from kernel

We need to understand the architecture patterns that Tracee, Tetragon, and Falco use so we can adopt the best ideas.

## Mode

research

## Required execution checklist

- You MUST search the web for Tracee's BPF architecture — specifically how they use raw_tracepoint/sys_enter + sys_exit with tail call dispatch via BPF_MAP_TYPE_PROG_ARRAY.
- You MUST research how Tracee maintains per-task state across syscall entry→exit using `task_info_map` (BPF HashMap keyed by TID).
- You MUST research how Tracee's `init_task_context()` populates process context (PPID, namespaces, cgroup, start_time) from task_struct using BPF_CORE_READ.
- You MUST research how Tetragon builds its in-kernel process tree (`execve_map`) and tracks parent→child relationships.
- You MUST research how Tetragon reads full argv from `mm->arg_start..arg_end` at execve time.
- You MUST research how Tetragon reads CWD, credentials (euid, caps), and namespace information.
- You MUST research Falco's approach — do they use LSM, raw tracepoints, or kprobes? How do they handle process context?
- You MUST investigate how these tools handle the "shared library loading" problem (detecting LD_PRELOAD attacks via mmap/security_mmap_file).
- You MUST investigate how these tools handle DNS monitoring (hooking UDP sends on port 53 vs packet-level hooks).
- You MUST end with a ranked list of architectural patterns we should adopt, ordered by impact.

After completing the required work, explore how these tools handle:
- Rate limiting / event throttling in kernel
- Large event data (argv that exceeds BPF stack size)
- Container escape detection patterns

## Required repo context

Read at least these:
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/src/main.rs` — our current 12 kernel programs
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-common/src/lib.rs` — our event structs
- `crates/logfwd-ebpf-proto/sensor-ebpf/src/main.rs` — our userspace consumer
- `AGENTS.md` — project context

## Deliverable

Write one repo-local output at:

`dev-docs/research/ebpf-deep-dive-2026-04-12/02-edr-architecture-patterns-report.md`

Include:
1. **Architecture comparison table**: Tracee vs Tetragon vs Falco — hook types, dispatch mechanism, state tracking, enrichment approach
2. **Process tree tracking**: How each tool builds and maintains the process tree in-kernel vs userspace
3. **Entry→exit correlation**: The tail call dispatch + per-task state pattern (with pseudo-code or simplified BPF C)
4. **In-kernel filtering**: How policies/rules are pushed into BPF maps for kernel-side filtering
5. **Recommended patterns for our sensor**: Ranked by impact, with implementation sketch for each

## Constraints

- Cite specific source files, functions, or data structures from each project
- Do not just describe what they do — explain HOW they do it (BPF map types, program flow, data structures)
- Distinguish patterns that work with tracepoints from those requiring raw_tracepoints or LSM
- Note kernel version requirements for each pattern
- Do not modify any code — research only

## Success criteria

- Enough architectural detail that we could implement the top 3 patterns without further research
- Clear understanding of which patterns require raw_tracepoints vs standard tracepoints vs LSM

## Decision style

End with: "Here are the top 5 patterns to adopt, in priority order, with estimated complexity."
