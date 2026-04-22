---
title: "Benchmarking"
description: "Criterion, competitive, and exploratory benchmarks"
---

## Criterion microbenchmarks

```bash
just bench
```

Measures scanner throughput, OTLP encoding speed, and compression performance.

## Competitive benchmarks

Compare FastForward against other log forwarders:

```bash
# Binary mode (local dev)
just bench-competitive --lines 1000000 --scenarios passthrough,json_parse,filter

# Docker mode (resource-limited)
just bench-competitive --lines 5000000 --docker --cpus 1 --memory 1g --markdown
```

## Exploratory profiling

```bash
# Stage-by-stage profile
cargo run -p logfwd-bench --release --features bench-tools --bin e2e_profile

# Memory analysis
cargo run -p logfwd-bench --release --features bench-tools --bin sizes

# Real RSS measurement
cargo run -p logfwd-bench --release --features bench-tools --bin rss
```

## Nightly benchmarks

Nightly benchmark automation now lives in
[`strawgate/memagent-e2e`](https://github.com/strawgate/memagent-e2e):

- Kubernetes EPS competitive suite:
  [bench-kind-smoke workflow](https://github.com/strawgate/memagent-e2e/actions/workflows/bench-kind-smoke.yml)
- Open nightly benchmark report issues:
  [open `report:bench-nightly-eps` issues](https://github.com/strawgate/memagent-e2e/issues?q=is%3Aopen+label%3Areport%3Abench-nightly-eps)
