# Benchmark Scenarios

Each scenario tests a different input/output path end-to-end using two logfwd pipelines: a **sender** (generator → network output) and a **receiver** (network input → null).

## Running

```bash
# Build the fully optimised release binary used by end-to-end benches
just bench-build

# Self-contained (no network, measures raw pipeline throughput)
just bench-self

# TCP end-to-end
just bench-tcp

# UDP end-to-end
just bench-udp

# OTLP end-to-end
just bench-otlp

# Elasticsearch end-to-end
just bench-es
just bench-es-streaming

# All scenarios
just bench-pipelines

# Criterion microbenchmarks (Cargo bench profile, full optimisations)
just bench
```

## Scenarios

| Scenario | Sender | Receiver | What it measures |
|----------|--------|----------|-----------------|
| self | generator → null | — | Raw pipeline: scanner + DataFusion |
| tcp | generator → tcp | tcp → null | TCP serialization + deserialization |
| udp | generator → udp | udp → null | UDP per-datagram overhead |
| otlp | generator → otlp | otlp → null | OTLP protobuf encode + HTTP + decode |
| es | generator → elasticsearch | Elasticsearch | Bulk NDJSON serialization + HTTP |
| es-streaming | generator → elasticsearch (streaming) | Elasticsearch | Chunked bulk streaming vs buffered HTTP |

## CPU limiting

On Linux, use `taskset` to pin to specific cores:
```bash
taskset -c 0 logfwd --config receiver.yaml &
taskset -c 1 logfwd --config sender.yaml &
```
