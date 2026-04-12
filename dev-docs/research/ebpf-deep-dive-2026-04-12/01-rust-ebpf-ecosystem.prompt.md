# Rust eBPF Ecosystem: State of the Art for Production EDR

You are handling one workstream inside a larger Codex Cloud fanout for this repository.

## Objective

Deep research into the current Rust eBPF ecosystem — which frameworks are production-ready, what capabilities each supports, and which is the right choice for building an EDR (Endpoint Detection & Response) sensor that needs LSM hooks, CO-RE/BTF, kprobes, tracepoints, ring buffers, and tail calls.

## Why this workstream exists

We are building an eBPF-based EDR sensor in Rust as part of the `logfwd` project. We currently use `aya` 0.13.1 (userspace) and `aya-ebpf` 0.1.1 (kernel) with 12 tracepoint/kprobe programs. We need to understand:

- Is aya the right choice, or should we consider alternatives?
- What is aya's current support for LSM BPF programs, BPF_CORE_READ (reading task_struct fields), tail calls, BPF-to-BPF function calls, and typed pointers?
- What are the alternatives (redbpf, libbpf-rs, plain libbpf-sys, bumble-bee) and where do they excel?
- What is the aya project's health — release cadence, maintainer activity, breaking changes?

Our existing sensor code lives in `crates/logfwd-ebpf-proto/sensor-ebpf/` with three sub-crates:
- `sensor-ebpf-kern/` — eBPF kernel programs (aya-ebpf, compiled with cargo +nightly for bpfel-unknown-none)
- `sensor-ebpf-common/` — shared repr(C) types
- `src/main.rs` — userspace loader

## Mode

research

## Required execution checklist

- You MUST search the web for current aya-rs GitHub releases, issues, and PRs related to LSM support, CO-RE, and BTF.
- You MUST search for libbpf-rs (libbpf/libbpf-rs on GitHub) current capabilities and compare.
- You MUST search for redbpf current status (is it maintained? deprecated?).
- You MUST check if aya supports `#[lsm]` program type annotations and what kernel version they require.
- You MUST check if aya supports `BPF_CORE_READ` equivalent via `aya-ebpf` helpers or if manual `bpf_probe_read_kernel` is needed.
- You MUST check if aya supports tail calls (`BPF_MAP_TYPE_PROG_ARRAY`) and BPF-to-BPF function calls.
- You MUST check the aya ring buffer API stability and any known issues with high-throughput scenarios.
- You MUST investigate aya's build story — does it still require bpf-linker, or does the newer aya support compiling eBPF programs without it?
- You MUST end with a concrete recommendation: stay with aya, switch, or hybrid approach.

After completing the required work, use your judgment to explore adjacent options — e.g., Rust eBPF testing frameworks, CI strategies for eBPF programs, eBPF program signing.

## Required repo context

Read at least these:
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/src/main.rs`
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/Cargo.toml`
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-common/src/lib.rs`
- `crates/logfwd-ebpf-proto/sensor-ebpf/src/main.rs`
- `crates/logfwd-ebpf-proto/sensor-ebpf/Cargo.toml`
- `AGENTS.md`

## Deliverable

Write one repo-local output at:

`dev-docs/research/ebpf-deep-dive-2026-04-12/01-rust-ebpf-ecosystem-report.md`

Include:
1. Feature matrix: aya vs libbpf-rs vs redbpf vs libbpf-sys — covering LSM, CO-RE, tail calls, ring buffer, BTF, build toolchain
2. aya capability audit: what works today, what's experimental, what's missing
3. Migration risk assessment if switching
4. Concrete recommendation with justification

## Constraints

- Ground everything in actual GitHub repos, release notes, and issue trackers
- Do not recommend switching frameworks unless the evidence is strong
- Distinguish "not supported" from "supported but undocumented"
- Check actual aya-ebpf source code (macros, helpers) not just docs
- Do not modify any code in this workstream — research only

## Success criteria

- A clear feature matrix that answers: "Can aya do LSM hooks, read task_struct via CO-RE, do tail calls?"
- A recommendation with enough evidence that we can make a framework decision confidently

## Decision style

End with a decisive recommendation, not a survey. State: "Use X because Y, and here's what to watch out for."
