# Bleeding-Edge Linux eBPF: New Kernel Features for Next-Gen EDR Sensors

You are handling one workstream inside a larger Codex Cloud fanout for this repository.

## Objective

Research the newest eBPF kernel features (kernel 6.1 through 6.12+) and evaluate which ones are relevant for building a high-performance EDR sensor. Focus on features that enable capabilities not possible with older BPF — BPF arena, kfuncs, sleepable BPF, BPF tokens, typed pointers, BPF exceptions, global subprograms, and BPF iterators.

## Why this workstream exists

Our sensor targets kernel 6.1 (Debian 12 bookworm) but we need to plan for newer kernels. We want to understand:

- What powerful new BPF features exist that we might be missing?
- Which features are available on 6.1 vs requiring 6.4/6.6/6.8/6.12?
- Are there features that fundamentally change how EDR sensors should be built?
- What's the trajectory of BPF development — what's coming in 2025-2026?

Specifically:
- **BPF arena** (6.9): Shared userspace↔kernel memory — could eliminate ring buffer overhead entirely
- **kfuncs** (5.13+, expanding): Kernel function calls from BPF — access to richer kernel APIs
- **Sleepable BPF** (5.10+): BPF programs that can sleep — enables reading from filesystem, locking
- **BPF tokens** (6.9): Unprivileged BPF program loading — security implications for EDR
- **Typed pointers / BTF** (5.x+): Type-safe kernel data structure access
- **BPF exceptions** (6.7): Error handling without crashing the program
- **Global subprograms** (5.x): Cross-program function sharing
- **BPF iterators** (5.8+): Iterate over kernel data structures from BPF

## Mode

research

## Required execution checklist

- You MUST search the web for recent BPF kernel changelogs — look at lwn.net BPF coverage, bpf-next mailing list summaries, and Cilium/Meta blog posts about new BPF features.
- You MUST create a feature availability matrix: feature × minimum kernel version × relevance to EDR.
- You MUST deep-dive on BPF arena (6.9) — how it works, performance characteristics, whether it could replace ring buffer for event passing, and any production experience reports.
- You MUST deep-dive on kfuncs — which kfuncs are available for security-relevant data (task_struct fields, cred access, dentry resolution, namespace info), and how they compare to BPF_CORE_READ.
- You MUST research sleepable BPF — what program types can be sleepable, what APIs are available (bpf_d_path for full path resolution), and whether LSM BPF programs can be sleepable.
- You MUST research BPF tokens — what they enable for non-root BPF loading, implications for deploying EDR sensors without full root.
- You MUST research typed pointers and BTF-based kernel data access — how this improves safety and portability over raw bpf_probe_read.
- You MUST research BPF-to-BPF function calls and global subprograms — can we share helper code between programs?
- You MUST research BPF exceptions (6.7) — do they allow graceful error handling that the verifier currently rejects?
- You MUST investigate what Rust eBPF frameworks (especially aya) support for each of these features.
- You MUST end with a tiered recommendation: "Use now on 6.1", "Plan for 6.6", "Watch for 6.12+".

After completing the required work, explore:
- BPF signing and attestation for tamper-resistant EDR programs
- eBPF for network security — XDP + TC hooks for network EDR
- Upcoming BPF features in bpf-next that aren't yet released

## Required repo context

Read at least these:
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/src/main.rs` — current BPF programs (shows what helpers we use today)
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/Cargo.toml` — aya-ebpf version
- `AGENTS.md` — project context

## Deliverable

Write one repo-local output at:

`dev-docs/research/ebpf-deep-dive-2026-04-12/05-bleeding-edge-kernel-ebpf-report.md`

Include:
1. **Feature availability matrix**: Feature × kernel version × aya support × EDR relevance
2. **BPF arena deep dive**: How it works, perf numbers, ring buffer comparison, production readiness
3. **kfuncs for EDR**: Which kfuncs provide task/cred/path/namespace access, with examples
4. **Sleepable BPF for EDR**: What it enables (bpf_d_path!), which program types support it
5. **Path to kernel 6.6/6.8**: What features we gain by raising minimum kernel version
6. **Aya support gaps**: Which bleeding-edge features aya doesn't yet support
7. **Tiered adoption plan**: What to use now, plan for, and watch

## Constraints

- Cite kernel commit hashes or version tags for feature introduction
- Distinguish "merged into mainline" from "available in bpf-next but not released"
- Note which features require specific kernel config options
- Be precise about aya-ebpf support — check actual aya source/issues, not assumptions
- Our current target is kernel 6.1 (Debian 12) — clearly separate "available now" from "requires upgrade"
- Do not modify any code — research only

## Success criteria

- A clear matrix showing which features are available on our kernel vs newer ones
- Enough detail on kfuncs and sleepable BPF that we could use them today (if available on 6.1)
- A strategy for when to raise minimum kernel version and what we gain

## Decision style

End with: "On kernel 6.1, use X, Y, Z. Raising to 6.6 unlocks A, B, C. The game-changer at 6.9+ is D."
