# Research: OTLP JSON Path Optimization (Issue #823)

## Context

The OTLP receiver has two content-type paths:
- **Protobuf** (`application/x-protobuf`): Uses direct projection via `decode_otlp_protobuf_with_prost` → `convert_request_to_batch`, bypassing the JSON scanner entirely. This is fast.
- **JSON** (`application/json`): Converts OTLP JSON → newline-delimited JSON lines → feeds through the full Scanner pipeline. This round-trips through JSON string → scan → Arrow, which is ~2x slower than the protobuf path.

The protobuf path was recently optimized. The question is whether the JSON path is worth optimizing too.

## What to investigate

1. Read both decode paths:
   - `crates/logfwd-io/src/otlp_receiver/decode.rs` — `decode_otlp_protobuf_with_prost` and `decode_otlp_json`
   - How does the JSON path convert OTLP JSON to NDJSON lines?
   - What information is lost or transformed?

2. Assess real-world usage:
   - What percentage of OTLP traffic uses JSON vs protobuf?
   - The OTLP spec recommends protobuf; is JSON mainly for debugging/testing?
   - Do major OTLP SDKs (OpenTelemetry SDK, Fluent Bit, etc.) default to protobuf?

3. If optimization is worthwhile, design options:
   - Option A: Direct JSON → Arrow (skip NDJSON intermediate, parse OTLP JSON structure directly)
   - Option B: JSON → prost types → existing batch converter (parse JSON into proto structs)
   - Option C: Accept the overhead, document that protobuf is recommended
   - Which gives the best effort/benefit ratio?

## Deliverable

Write a research report to `dev-docs/research/fanout-2026-04-21/ws08-otlp-json-path-report.md` containing:

1. Real-world JSON vs protobuf usage assessment
2. Current JSON path overhead analysis (where time is spent)
3. Optimization options with effort estimates
4. Recommendation: optimize, defer, or document
5. If optimize: implementation plan with file paths

Do NOT implement code changes. Research and design only.
