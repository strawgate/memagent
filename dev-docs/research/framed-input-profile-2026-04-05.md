# FramedInput profiling report (2026-04-05)

Issue: `perf: profile FramedInput framing and format processing overhead` (#1019)

This report was generated with the new `framed_input_profile` tool:

```bash
just bench-framed-input -- --lines 1000000 --iterations 1 --flamegraph /tmp/framed-input-1m.svg
just bench-framed-input -- --lines 200000 --iterations 3
just bench-framed-input-alloc -- --lines 200000
```

## Baseline

### CPU / throughput / RSS baseline

1M-line run, release build, single-process synthetic pipeline (`FramedInput -> Scanner -> OTLP encode`):

| Scenario | FramedInput | Scanner | Encode | Total | Framed share | Throughput | Peak RSS | Steady RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `passthrough_complete` | 58.58 ms | 1.27 s | 334.40 ms | 1.66 s | 3.5% | 603K lines/s, 159 MiB/s | 1744.9 MiB | 1744.9 MiB |
| `passthrough_remainder` | 600.85 ms | 1.36 s | 335.03 ms | 2.29 s | 26.2% | 436K lines/s, 115 MiB/s | 1927.0 MiB | 1927.0 MiB |
| `passthrough_narrow_raw` | 31.33 ms | 430.76 ms | 112.55 ms | 574.63 ms | 5.5% | 1.74M lines/s, 170 MiB/s | 1441.4 MiB | 1441.4 MiB |
| `cri_full` | 61.99 ms | 580.56 ms | 158.49 ms | 801.05 ms | 7.7% | 624K lines/s, 159 MiB/s | 1591.4 MiB | 1591.4 MiB |

200K-line median run used for finer comparisons:

| Scenario | FramedInput | Scanner | Encode | Total | Framed share | Throughput | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| `passthrough_complete` | 13.38 ms | 241.09 ms | 68.21 ms | 322.68 ms | 4.1% | 620K lines/s, 164 MiB/s | 446.1 MiB |
| `passthrough_remainder` | 126.36 ms | 235.97 ms | 67.24 ms | 429.56 ms | 29.4% | 466K lines/s, 123 MiB/s | 485.3 MiB |
| `passthrough_narrow_raw` | 4.08 ms | 85.63 ms | 21.23 ms | 110.94 ms | 3.7% | 1.80M lines/s, 176 MiB/s | 347.4 MiB |
| `passthrough_narrow_json` | 6.35 ms | 87.97 ms | 21.07 ms | 115.39 ms | 5.5% | 1.73M lines/s, 169 MiB/s | 347.4 MiB |
| `cri_full` | 18.09 ms | 104.59 ms | 29.83 ms | 152.50 ms | 11.9% | 656K lines/s, 167 MiB/s | 375.8 MiB |

### Allocation baseline

200K-line dhat run:

| Scenario | Total allocs | Total bytes | Peak bytes |
|---|---:|---:|---:|
| `passthrough_complete` | 5,274 | 503.02 MiB | 235.66 MiB |
| `passthrough_remainder` | 2,204,256 | 690.05 MiB | 261.56 MiB |
| `passthrough_narrow_raw` | 1,984 | 192.71 MiB | 96.12 MiB |
| `passthrough_narrow_json` | 1,991 | 192.71 MiB | 96.12 MiB |
| `cri_full` | 7,774 | 390.78 MiB | 163.13 MiB |

## Flamegraph findings

Flamegraph command:

```bash
just bench-framed-input -- --lines 1000000 --iterations 1 --flamegraph /tmp/framed-input-1m.svg
```

Top sampled stacks in the 1M-line flamegraph:

| Stack | Samples |
|---|---:|
| `logfwd_arrow::scanner::Scanner::scan_detached` | 80.07% |
| `logfwd_core::json_scanner::scan_streaming` | 49.21% |
| `logfwd_arrow::streaming_builder::StreamingBuilder::finish_batch_detached` | 30.12% |
| `logfwd_arrow::streaming_builder::StreamingBuilder::read_str` / UTF-8 validation | 24.09% |
| `logfwd_output::otlp_sink::OtlpSink::encode_batch` | 19.58% |
| `StreamingBuilder::resolve_field` / `HashMap::get` | 17.11% |
| `FramedInput::poll` | 0.96% |
| `FormatDecoder::process_lines` | 0.07% |

Interpretation:

- On aligned passthrough input, **FramedInput is a small fraction of whole-pipeline CPU**.
- The dominant pipeline cost is still **scanner + Arrow builder work**, followed by OTLP encoding.
- FramedInput becomes important when the workload is **remainder-heavy**: stage timing rises from **4.1%** to **29.4%** of total CPU on the same production-mixed input when chunks are split every 97 bytes.

## Copy sites

Observed copy sites in the current code:

1. `crates/logfwd-io/src/framed.rs:113-115`
   - `let mut chunk = std::mem::take(&mut state.remainder);`
   - `chunk.extend_from_slice(&bytes);`
   - Copy: raw input `Vec<u8>` is copied into a second `Vec<u8>` even when `remainder` is empty.
2. `crates/logfwd-io/src/format.rs:101-107`
   - `out.extend_from_slice(chunk);`
   - Copy: passthrough / passthrough-json copies complete lines into `out_buf`.
3. `crates/logfwd/src/pipeline.rs:1068-1070`
   - `input.buf.extend_from_slice(&bytes);`
   - Copy: FramedInput output is copied again into the async `BytesMut` batch buffer.

## Per-change benchmarking

### Microbenchmark: known copy-path change

200K-line copy-site microbenchmark:

| Candidate | Time | Throughput | Relative to current |
|---|---:|---:|---:|
| Current round-trip (`chunk.extend_from_slice` + `out.extend_from_slice`) | 2.03 ms | 98.55M lines/s | 1.00x |
| Owned-input fast path (`Vec` move when `remainder.is_empty()`) | 867.5 µs | 230.55M lines/s | 2.34x |
| Passthrough-json validation + copy | 3.32 ms | 60.27M lines/s | 0.61x |

### Summary table

| Change | Independent measurement | CPU impact | Memory / alloc impact | Throughput impact | Composes? | Recommendation |
|---|---|---|---|---|---|---|
| **Owned-input fast path in `FramedInput`** | Copy microbench above | `2.03 ms -> 867.5 µs` (**-57.3%**) for the affected copy path | Should reduce transient copy pressure on the no-remainder path; not yet implemented in the main pipeline | `98.55M -> 230.55M lines/s` (**+133.9%**) for the isolated copy path | Yes | **Do first** |
| **Reduce remainder churn / keep chunks line-aligned** | `passthrough_complete` vs `passthrough_remainder` on the same 200K production-mixed workload | FramedInput `13.38 ms -> 126.36 ms` (**9.4x slower**) when chunks are split every 97 bytes | Peak RSS `446.1 -> 485.3 MiB` (**+39.2 MiB**); allocs `5,274 -> 2,204,256` (**+2.20M allocs**) | `620K -> 466K lines/s` (**-24.8%**) | Yes | **Do in parallel with code fixes / config tuning** |
| **Skip `passthrough_json` validation on trusted JSON** | `passthrough_narrow_raw` vs `passthrough_narrow_json` on the same 200K narrow workload | FramedInput `4.08 ms -> 6.35 ms` (**+55.6%**) with validation enabled; disabling validation saves **35.7%** of FramedInput CPU for this case | Peak RSS unchanged (`347.4 MiB` both); allocs nearly identical (`1,984` vs `1,991`) | `1.80M -> 1.73M lines/s` (**~4.0% gain** when validation is skipped) | Mutually exclusive with parse-error counting for malformed JSON | **Use as a config/documentation tweak, not the first code change** |

## Conclusions

1. **FramedInput is not the primary whole-pipeline bottleneck on aligned input.** In the aligned passthrough baseline it is only **3.5%–4.1%** of total CPU; scanner + builder work dominates.
2. **The remainder pattern is the main FramedInput bottleneck.** Split-heavy input pushes FramedInput to **26.2%–29.4%** of total CPU and explodes allocations.
3. **The known `Vec -> Vec` round-trip is real and worth fixing first.** The isolated copy-path microbenchmark shows a **2.34x** improvement opportunity for the no-remainder fast path.
4. **`passthrough_json` validation is measurable but secondary.** It costs a second linear scan, but it is a much smaller win than fixing the no-remainder copy path.
