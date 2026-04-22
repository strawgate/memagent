# OTLP Projection Decode Benchmarks

> **Status:** Completed
> **Date:** 2026-04-19
> **Context:** Benchmark evidence for OTLP projected decode cutover gates.

## Verdict

Do not make `projected_fallback` the default yet.

The projected decoder remains a strong performance candidate for primitive-heavy
OTLP payloads, but this run does not satisfy the cutover gates from #2301:

- `narrow-1k` and `attrs-heavy` pass the primitive detached-projection gate.
- `trace-heavy` improves over prost, but misses the strict primitive gate.
- `compression-relevant` passes the fallback gate.
- `complex-anyvalue` fails the fallback gate by a wide margin because the
  fallback wrapper pays projection-attempt cost before returning to prost.

Recommendation: keep `prost` as the default, keep projected modes explicit, and
optimize fallback classification before revisiting the default switch.

## Environment

| Field | Value |
|---|---|
| Date | 2026-04-19 |
| Host | `Darwin Mac.ad.weaston.org 25.3.0 arm64` |
| CPU | `Apple M4` |
| Rust | `rustc 1.94.1 (e408947bf 2026-03-25)` |
| Repo HEAD | `484b2d055e28050dd8dbde023bb62e28602c7e29` |
| Benchmark command | `just bench-otlp-io otlp_input_decode_materialize` |
| Criterion settings | benchmark default warm-up, `sample_size(10)` in `otlp_io.rs` |
| Build note | First run spent 22m25s in release/LTO build before measurement. |
| Plot note | `gnuplot` was unavailable; Criterion used the plotters backend. |

## Gate Definitions

| Gate | Formula |
|---|---|
| Primitive gate | `projected_detached_to_batch` median <= `0.80 * prost_reference_to_batch` median |
| Complex/fallback gate | `projected_fallback_to_batch` median <= `1.05 * prost_reference_to_batch` median |

Confidence interval note: pass/fail uses Criterion median point estimates.
Tables also include median confidence intervals so marginal results can be
reviewed before policy decisions.

## Required Fixture Gates

| Fixture | Bucket | Prost median | Candidate | Candidate median | Threshold | Ratio | Result |
|---|---|---:|---|---:|---:|---:|---|
| `narrow-1k` | primitive | 0.711 ms | `projected_detached_to_batch` | 0.418 ms | 0.569 ms | 0.588 | PASS |
| `attrs-heavy` | primitive | 9.039 ms | `projected_detached_to_batch` | 4.013 ms | 7.231 ms | 0.444 | PASS |
| `trace-heavy` | primitive | 9.459 ms | `projected_detached_to_batch` | 8.617 ms | 7.567 ms | 0.911 | FAIL |
| `complex-anyvalue` | complex/fallback | 5.253 ms | `projected_fallback_to_batch` | 10.811 ms | 5.515 ms | 2.058 | FAIL |
| `compression-relevant` | complex/fallback | 14.423 ms | `projected_fallback_to_batch` | 6.573 ms | 15.144 ms | 0.456 | PASS |

## Required Variant Table

| Fixture | Rows | Variant | Median | Median CI | ns/row |
|---|---:|---|---:|---:|---:|
| `narrow-1k` | 1,000 | `prost_reference_to_batch` | 0.711 ms | 0.710-0.719 ms | 711.0 |
| `narrow-1k` | 1,000 | `projected_fallback_to_batch` | 0.362 ms | 0.361-0.367 ms | 361.6 |
| `narrow-1k` | 1,000 | `projected_detached_to_batch` | 0.418 ms | 0.418-0.422 ms | 418.2 |
| `narrow-1k` | 1,000 | `projected_view_to_batch` | 0.360 ms | 0.359-0.361 ms | 360.4 |
| `attrs-heavy` | 2,000 | `prost_reference_to_batch` | 9.039 ms | 9.027-9.052 ms | 4519.3 |
| `attrs-heavy` | 2,000 | `projected_fallback_to_batch` | 3.693 ms | 3.684-3.714 ms | 1846.3 |
| `attrs-heavy` | 2,000 | `projected_detached_to_batch` | 4.013 ms | 3.994-4.026 ms | 2006.7 |
| `attrs-heavy` | 2,000 | `projected_view_to_batch` | 3.721 ms | 3.705-3.750 ms | 1860.7 |
| `trace-heavy` | 8,000 | `prost_reference_to_batch` | 9.459 ms | 9.432-9.470 ms | 1182.4 |
| `trace-heavy` | 8,000 | `projected_fallback_to_batch` | 7.745 ms | 7.375-9.208 ms | 968.2 |
| `trace-heavy` | 8,000 | `projected_detached_to_batch` | 8.617 ms | 8.514-8.655 ms | 1077.2 |
| `trace-heavy` | 8,000 | `projected_view_to_batch` | 7.335 ms | 7.307-8.022 ms | 916.8 |
| `complex-anyvalue` | 2,000 | `prost_reference_to_batch` | 5.253 ms | 5.219-5.315 ms | 2626.4 |
| `complex-anyvalue` | 2,000 | `projected_fallback_to_batch` | 10.811 ms | 10.801-10.913 ms | 5405.5 |
| `compression-relevant` | 5,000 | `prost_reference_to_batch` | 14.423 ms | 14.105-15.467 ms | 2884.6 |
| `compression-relevant` | 5,000 | `projected_fallback_to_batch` | 6.573 ms | 6.410-6.804 ms | 1314.6 |
| `compression-relevant` | 5,000 | `projected_detached_to_batch` | 7.741 ms | 7.555-9.094 ms | 1548.2 |
| `compression-relevant` | 5,000 | `projected_view_to_batch` | 6.313 ms | 6.211-6.485 ms | 1262.6 |

## Interpretation

The projection path is clearly worthwhile for straightforward primitive shapes.
`projected_view_to_batch` is usually fastest, and `projected_fallback_to_batch`
also wins on primitive fixtures where no fallback is needed.

The default-switch blocker is fallback cost. `complex-anyvalue` is valid OTLP
that requires prost fallback; the current wrapper takes roughly 2.06x the prost
median. That is too expensive for a default path unless fallback incidence is
known to be negligible, which we have not proven.

`trace-heavy` also misses the primitive detached gate. It still improves over
prost and `projected_view_to_batch` is stronger, but the agreed gate names
`projected_detached_to_batch`, so the gate should fail as written.

## Next Work

1. Optimize unsupported-but-valid fallback classification so complex `AnyValue`
   payloads avoid a full failed projection attempt when possible.
2. Decide whether the primitive gate should use detached projection, view
   projection, or the production wrapper before encoding it as policy.
3. Add the dedicated projected-decode fuzz and compatibility corpus work before
   any default switch PR.
