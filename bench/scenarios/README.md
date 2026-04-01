# Benchmark Scenarios

Each scenario tests a different input/output path end-to-end using two logfwd pipelines: a **sender** (generator → network output) and a **receiver** (network input → null).

## Running

```bash
# Self-contained (no network, measures raw pipeline throughput)
just bench-self

# TCP end-to-end
just bench-tcp

# UDP end-to-end
just bench-udp

# OTLP end-to-end
just bench-otlp

# All scenarios
just bench-pipelines
```

## Scenarios

| Scenario | Sender | Receiver | What it measures |
|----------|--------|----------|-----------------|
| self | generator → null | — | Raw pipeline: scanner + DataFusion |
| tcp | generator → tcp_out | tcp → null | TCP serialization + deserialization |
| udp | generator → udp_out | udp → null | UDP per-datagram overhead |
| otlp | generator → otlp | otlp → null | OTLP protobuf encode + HTTP + decode |

## CPU limiting

On Linux, use `taskset` to pin to specific cores:
```bash
taskset -c 0 logfwd --config receiver.yaml &
taskset -c 1 logfwd --config sender.yaml &
```
