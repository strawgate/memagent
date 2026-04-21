# Research: Kafka Source Contract (Issue #1475)

## Context

logfwd has an example config (`examples/use-cases/kafka-to-otlp.yaml`) and config type stubs for Kafka input, but zero implementation. This research designs the Kafka source contract.

## What to investigate

1. Read the InputSource trait contract:
   - `crates/logfwd-io/src/input.rs` — `InputSource` trait, `InputEvent`, `SourceId`
   - How do existing sources (file, TCP, UDP, OTLP, generator) implement polling?
   - What checkpoint contract do they follow?

2. Evaluate Rust Kafka crates:
   - `rdkafka` (librdkafka binding) — most mature, C dependency
   - `rskafka` — pure Rust, newer
   - `kafka-rust` — older pure Rust
   - Which fits logfwd's static-binary, minimal-dependency philosophy?

3. Design the consumer group model:
   - One consumer group per pipeline? Per logfwd instance?
   - Partition assignment: automatic vs manual?
   - How does rebalancing interact with logfwd's checkpoint system?

4. Design checkpoint integration:
   - Kafka offsets map to logfwd's `ByteOffset` / `SourceId` model how?
   - Commit strategy: auto-commit vs manual commit on checkpoint flush?
   - What happens on logfwd restart? Replay from last committed offset?

5. Design the payload model:
   - Bytes vs structured (JSON, Avro, Protobuf)?
   - How do Kafka headers map to logfwd's source metadata?
   - Topic→pipeline routing?

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws04-kafka-source-report.md` containing:

1. Crate evaluation matrix (rdkafka vs rskafka vs kafka-rust)
2. Consumer group design
3. Checkpoint/offset integration design
4. Payload and header mapping
5. Config schema proposal (KafkaInputConfig fields)
6. Implementation plan with effort estimate
7. Delivery guarantee analysis (at-least-once, exactly-once)

Do NOT implement code changes. Research and design only.
