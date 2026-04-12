# High-Performance eBPF: Patterns for Zero-Drop, Low-Overhead EDR at Scale

You are handling one workstream inside a larger Codex Cloud fanout for this repository.

## Objective

Research the best practices and bleeding-edge techniques for building high-performance eBPF programs — specifically for an EDR sensor that must process thousands of events/sec with near-zero overhead and zero event drops.

## Why this workstream exists

Our current sensor benchmarks at ~1,686 events/sec under heavy load with zero drops and a 16MB ring buffer. This is adequate for a single-machine prototype, but we need to understand:

- How do production EDR tools achieve 100K+ events/sec?
- What are the performance characteristics of ring buffer vs perf buffer vs BPF_MAP_TYPE_QUEUE?
- How do tail calls impact performance vs separate programs?
- What are the CPU overhead patterns — per-event cost, map lookup cost, string copy cost?
- How do we minimize allocations and copies in the kernel→userspace path?
- What batch processing patterns exist (BPF_MAP_TYPE_RINGBUF batch reserve)?
- How do we profile and measure eBPF program overhead?

Our sensor currently:
- Uses a single 16MB RingBuf for all 12 event types
- Does individual `reserve → fill → submit` for each event
- Does `bpf_probe_read_user_str_bytes` for every filename (expensive)
- Has no per-CPU buffering or batching
- Processes events one-at-a-time in userspace via polling

## Mode

research

## Required execution checklist

- You MUST search the web for eBPF performance benchmarks and best practices — look for talks from BPF conferences (LSF/MM/BPF, Linux Plumbers), Brendan Gregg's work, and Cilium/Isovalent blog posts.
- You MUST research ring buffer vs perf buffer performance characteristics — when ring buffer wins, when perf buffer wins, and the tradeoffs for multi-producer scenarios.
- You MUST research BPF batch operations — `bpf_ringbuf_reserve`/`submit` batching, `bpf_map_lookup_batch`, and whether batch APIs reduce overhead.
- You MUST research tail call overhead — what's the cost of `bpf_tail_call` vs inline code vs BPF-to-BPF function calls?
- You MUST research per-CPU maps and their use in high-throughput scenarios.
- You MUST research how to profile eBPF programs — `bpf_prog_run_xattr`, `bpftool prog profile`, instruction counts, verifier stats.
- You MUST research userspace consumption patterns — epoll vs busy-poll vs io_uring for ring buffer consumption.
- You MUST research string handling in BPF — cost of `bpf_probe_read_str` vs `bpf_probe_read`, path resolution costs, and tricks to minimize string copies.
- You MUST research BPF JIT compilation effects and how to verify JIT is enabled and working.
- You MUST end with a concrete list of optimizations we should implement, ordered by expected impact.

After completing the required work, explore:
- BPF CO-RE's impact on program load time and runtime performance
- BPF arena (shared memory between BPF and userspace) for zero-copy event passing
- Hardware offload opportunities (if any relevant to EDR)

## Required repo context

Read at least these:
- `crates/logfwd-ebpf-proto/sensor-ebpf/sensor-ebpf-kern/src/main.rs` — our BPF programs
- `crates/logfwd-ebpf-proto/sensor-ebpf/src/main.rs` — our userspace consumer
- `AGENTS.md` — project context (note: logfwd targets 1.7M lines/sec for log parsing)

## Deliverable

Write one repo-local output at:

`dev-docs/research/ebpf-deep-dive-2026-04-12/03-high-performance-ebpf-report.md`

Include:
1. **Performance model**: What are the main cost centers in an eBPF EDR pipeline? (map lookups, string reads, ring buffer ops, context switches)
2. **Ring buffer deep dive**: Optimal sizing, multi-producer behavior, epoll vs polling, callback overhead
3. **Tail call vs function call vs inline**: Performance comparison with numbers if available
4. **Userspace consumption patterns**: Best way to drain ring buffer at high throughput
5. **String handling optimization**: Reducing the cost of path capture
6. **Per-CPU patterns**: When and how to use per-CPU maps/arrays
7. **Profiling methodology**: How to measure and optimize BPF program overhead
8. **Ordered optimization list**: What to implement first for maximum performance gain

## Constraints

- Cite benchmarks, papers, or talks — not just blog post opinions
- Distinguish between kernel 5.x and 6.x capabilities
- Note which optimizations are available on kernel 6.1 (our target)
- Separate "proven production technique" from "experimental/theoretical"
- Do not modify any code — research only

## Success criteria

- A prioritized optimization list with expected impact estimates
- Enough detail on ring buffer tuning and userspace consumption that we can implement immediately
- Understanding of what "good" looks like — target events/sec and overhead % for a production EDR

## Decision style

End with: "Here are the top 10 optimizations in priority order. The first 3 are high-confidence wins; the rest are worth benchmarking."
