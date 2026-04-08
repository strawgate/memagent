# Generated Code Dual-Path Evaluation

Date: 2026-04-06

## Goal

Keep both handwritten and generated protocol implementations in-tree while we
measure whether generated code can become fast enough to replace bespoke
encoders/decoders on the hot path.

This is an intentional evaluation state, not a permanent architecture choice.

## Current policy

- Handwritten protocol code remains the production default for hot paths.
- Generated protocol code is allowed as an experimental/reference path.
- Both paths must remain benchmarkable side by side.
- Both paths should remain correctness-comparable via shared tests or decode
  round-trips.

## Where both paths exist today

### OTLP output

- Handwritten path:
  - `crates/logfwd-output/src/otlp_sink.rs`
- Generated benchmark/reference path:
  - `crates/logfwd-output/examples/smart_codegen_bench.rs`
  - uses generated OTLP protobuf types from `opentelemetry-proto`

### OTAP boundary

- Handwritten historical/reference path:
  - `crates/logfwd-bench/benches/otap_proto.rs`
- Generated path:
  - `crates/logfwd-otap-proto/`
  - `crates/logfwd-output/src/otap_sink.rs`
  - `crates/logfwd-io/src/otap_receiver.rs`

## Bench harnesses

- Focused release benchmark:
  - `cargo run --release -p logfwd-output --example smart_codegen_bench`
- Criterion OTAP microbench:
  - `cargo bench -p logfwd-bench --bench otap_proto -- --noplot`

## Current directional benchmark snapshot

Measured with:

```bash
cargo run --release -p logfwd-output --example smart_codegen_bench
```

### OTLP encode

`narrow-1k`

- handwritten: `115 us`
- generated naive: `415 us`
- generated reuse: `417 us`
- generated recycled: `349 us`

Breakdown:

- generated build naive: `275 us`
- generated encode only naive: `147 us`
- generated build recycled: `203 us`
- generated encode only recycled: `145 us`

`wide-10k`

- handwritten: `3.18 ms`
- generated naive: `13.50 ms`
- generated reuse: `13.64 ms`
- generated recycled: `11.88 ms`

Breakdown:

- generated build naive: `8.62 ms`
- generated encode only naive: `4.85 ms`
- generated build recycled: `7.14 ms`
- generated encode only recycled: `7.42 ms`

Interpretation:

- The generated OTLP path is not competitive with the handwritten encoder yet.
- The biggest OTLP loss is object-graph construction, not just protobuf encode.
- Reusing the generated object graph helps, but not enough to replace the
  handwritten path today.

### OTAP boundary

Small payload:

- encode handwritten: `181 ns`
- encode generated: `313 ns`
- decode handwritten: `289 ns`
- decode generated: `546 ns`

Large payload:

- encode handwritten: `12.39 us`
- encode generated: `10.10 us`
- decode handwritten: `5.68 us`
- decode generated: `13.74 us`

Status decode:

- handwritten: `32.6 ns`
- generated: `35.8 ns`

Interpretation:

- Generated OTAP encode is promising for large payloads.
- Generated OTAP decode is still materially slower.
- OTAP is a good candidate for continued generated-code optimization because the
  gap is mixed rather than uniformly bad.

## Exit criteria

Generated code can replace the handwritten path only if all of the following
are true:

- correctness parity is demonstrated by shared tests/oracles
- throughput is within an explicitly accepted budget for the target path
- allocation behavior is not materially worse for the production workload
- operational complexity stays acceptable for the repo

If generated code does not get meaningfully closer after targeted optimization,
we should either:

- keep it as a narrow benchmark/reference path only, or
- delete the generated production candidate and keep the benchmark artifacts

## Optimization plan

### Highest-priority work

- reduce generated object-graph churn for OTLP
- reuse nested vectors/messages more aggressively
- separate `build` cost from `encode` cost in benchmarks
- tune local `prost_build::Config` where we control the generated crate

### Likely useful levers

- own the generated crate instead of relying only on upstream generated types
- use `prost_build::Config` features such as `bytes(...)` where appropriate
- add specialized reusable builders on top of generated message structs
- keep large-message and small-message cases benchmarked separately

### What we should avoid

- assuming codegen settings alone will close a multi-x performance gap
- moving generated code into the proven scanner/kernel hot path prematurely
- leaving dual implementations around without explicit reevaluation

## Decision rule

- OTLP output: handwritten remains default until generated is much closer.
- OTAP boundary: generated remains a viable candidate, especially for encode,
  but decode must improve before we call it a full win.

