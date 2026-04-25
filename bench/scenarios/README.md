# Benchmark Scenarios

Each scenario tests a different input/output path end-to-end using two ffwd pipelines: a **sender** (generator → network output) and a **receiver** (network input → null).

## Running

```bash
# Self-contained (no network, measures raw pipeline throughput)
just bench-self

# Throughput ceiling (no filter, no network, measures theoretical max)
just bench-ceiling-self

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
```

## Scenarios

| Scenario | Sender | Receiver | What it measures |
|----------|--------|----------|-----------------|
| self | generator → null | — | Raw pipeline: scanner + DataFusion |
| ceiling | generator → null | — | Throughput ceiling: passthrough only (SELECT *) |
| tcp | generator → tcp | tcp → null | TCP serialization + deserialization |
| udp | generator → udp | udp → null | UDP per-datagram overhead |
| otlp | generator → otlp | otlp → null | OTLP protobuf encode + HTTP + decode |
| es | generator → elasticsearch | Elasticsearch | Bulk NDJSON serialization + HTTP |
| es-streaming | generator → elasticsearch (streaming) | Elasticsearch | Chunked bulk streaming vs buffered HTTP |

## CPU limiting

On Linux, use `taskset` to pin to specific cores:
```bash
taskset -c 0 ffwd run --config receiver.yaml &
taskset -c 1 ffwd run --config sender.yaml &
```

## Benchkit OTLP in CI

The `Nightly Benchmarks` workflow also normalizes benchmark outputs with
`strawgate/o11ykit/actions/parse-results` and publishes OTLP JSON artifacts:

- `competitive-otlp.json`
- `rate-otlp.json`
- `criterion-otlp.json`

On `main`, these OTLP snapshots are also stashed to `bench-data` under
`data/runs/*-otlp.json` for downstream tooling and experiments.
