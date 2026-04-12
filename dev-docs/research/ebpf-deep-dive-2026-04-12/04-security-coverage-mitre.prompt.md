# eBPF Security Coverage: MITRE ATT&CK Mapping and Detection Engineering

You are handling one workstream inside a larger Codex Cloud fanout for this repository.

## Objective

Map our eBPF sensor's event types to the MITRE ATT&CK framework, identify coverage gaps, and research which additional eBPF hooks would provide the highest-value detections for real-world attacks (container escapes, lateral movement, persistence, credential access, defense evasion).

## Why this workstream exists

We have 12 eBPF event types in our sensor:
1. ProcessExec (sched_process_exec) — binary execution with filename
2. ProcessExit (sched_process_exit) — process termination
3. TcpConnect (kprobe/tcp_v4_connect + inet_sock_set_state) — outbound TCP
4. TcpAccept (inet_sock_set_state SYN_RECV→ESTABLISHED) — inbound TCP
5. FileOpen (sys_enter_openat) — file access with flags
6. FileDelete (sys_enter_unlinkat) — file deletion
7. FileRename (sys_enter_renameat2) — file rename
8. Setuid (sys_enter_setuid) — privilege escalation
9. Setgid (sys_enter_setgid) — privilege escalation
10. ModuleLoad (module/module_load) — kernel module loading
11. Ptrace (sys_enter_ptrace) — process debugging/injection
12. MemfdCreate (sys_enter_memfd_create) — fileless malware staging

We need to understand: What attacks can we detect? What attacks are invisible to us? What's the minimum set of additional hooks needed for credible EDR coverage?

## Mode

research

## Required execution checklist

- You MUST search the web for MITRE ATT&CK techniques mapped to eBPF detection — look for Elastic's, CrowdStrike's, and Aqua's published detection matrices.
- You MUST map each of our 12 event types to specific MITRE ATT&CK techniques (e.g., ProcessExec → T1059 Command and Scripting Interpreter, T1053 Scheduled Task/Job).
- You MUST identify the top 20 ATT&CK techniques that are NOT detectable with our current 12 events.
- You MUST research container escape detection patterns — what eBPF hooks detect nsenter, unshare, mount namespace manipulation, cgroup escape?
- You MUST research credential access detection — what eBPF hooks detect /etc/shadow reads, credential dumping, kerberos ticket theft?
- You MUST research defense evasion detection — what eBPF hooks detect process hollowing, DLL/SO injection via ptrace, LD_PRELOAD, timestomping, log deletion?
- You MUST research lateral movement detection — what eBPF hooks detect SSH tunneling, port forwarding, reverse shells?
- You MUST research persistence detection — what eBPF hooks detect crontab modification, systemd service creation, .bashrc modification, authorized_keys changes?
- You MUST research fileless attack detection — beyond memfd_create, what about shm_open, process_vm_writev, modify_ldt?
- You MUST end with a prioritized list of additional eBPF hooks needed, ordered by MITRE ATT&CK coverage impact.

After completing the required work, explore:
- How Tracee's built-in "signatures" (detection rules) map to ATT&CK
- Tetragon's "TracingPolicy" approach to detection
- Whether behavioral detection (sequences of events) is possible in-kernel vs requiring userspace

## Required repo context

Read at least these:
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-common/src/lib.rs` — our 12 event structs
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/src/main.rs` — our BPF programs
- `AGENTS.md` — project context

## Deliverable

Write one repo-local output at:

`dev-docs/research/ebpf-deep-dive-2026-04-12/04-security-coverage-mitre-report.md`

Include:
1. **Current coverage matrix**: Our 12 events × MITRE ATT&CK techniques they detect
2. **Gap analysis**: Top 20 undetectable techniques with severity assessment
3. **Container security gaps**: What container-specific attacks we miss
4. **Recommended additional hooks**: Prioritized list with:
   - Hook type (tracepoint, kprobe, LSM, raw_tracepoint)
   - Kernel function/tracepoint name
   - MITRE technique(s) it enables
   - Implementation complexity estimate
5. **Detection engineering patterns**: How to combine multiple events into behavioral detections (e.g., "exec + network connect within 1s = potential reverse shell")

## Constraints

- Use the actual MITRE ATT&CK technique IDs (T1XXX.XXX)
- Focus on Linux-specific techniques (not Windows/macOS)
- Prioritize techniques commonly seen in real incidents, not theoretical
- Note which detections require additional context (process tree, argv) vs raw events
- Distinguish "can detect the attempt" from "can detect successful exploitation"
- Do not modify any code — research only

## Success criteria

- A clear coverage map showing what % of relevant ATT&CK techniques we cover
- A prioritized list of 10-15 additional hooks that would close the biggest gaps
- Enough specificity that we can implement the top 5 hooks without further research

## Decision style

End with: "Adding these N hooks would take our ATT&CK coverage from X% to Y%. Here's the implementation order."
