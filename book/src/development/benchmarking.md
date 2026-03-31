# Benchmarking

## Criterion microbenchmarks

```bash
just bench
```

Measures scanner throughput, OTLP encoding speed, and compression performance.

## Competitive benchmarks

Compare logfwd against vector, fluent-bit, filebeat, otelcol, and vlagent:

```bash
# Binary mode (local dev)
just bench-competitive --lines 1000000 --scenarios passthrough,json_parse,filter

# Docker mode (resource-limited)
just bench-competitive --lines 5000000 --docker --cpus 1 --memory 1g --markdown
```

## Exploratory profiling

```bash
# Stage-by-stage profile
cargo run -p logfwd-bench --release --bin e2e-profile

# Memory analysis
cargo run -p logfwd-bench --release --bin sizes

# Real RSS measurement
cargo run -p logfwd-bench --release --bin rss
```

## Nightly benchmarks

Results are published to GitHub Pages automatically via the nightly benchmark
workflow. View at: [strawgate.github.io/memagent/bench/](https://strawgate.github.io/memagent/bench/)
