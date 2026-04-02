> **Historical reference** from the v1 implementation. See [ARCHITECTURE.md](../ARCHITECTURE.md) for the current design.

---

# Research Benchmarks

Findings from prototyping the DataFusion integration. All Python benchmarks
(PyArrow 23.0.1 + DataFusion 52.3.0) validated directional claims. Rust
scan_bench validated the scanner. The native Rust implementations now exist
in `scanner.rs`, `batch_builder.rs`, and `transform.rs`.

## Key Numbers

### DataFusion Query Cost (Python benchmark, 100K lines)

```
Parse (JSON → Arrow):    2,103 ns/line  (22%)
Query (DataFusion SQL):    505 ns/line  ( 5%)  ← the key finding
Serialize (→ JSON):      9,265 ns/line  (73%)
```

**DataFusion is 5% of the pipeline.** The query engine is not the bottleneck.

Simple filter (`WHERE level != 'DEBUG'`): **85 ns/line**.
GROUP BY aggregation: **70 ns/line**.
Complex multi-condition filter: **21 ns/line**.

### DataFusion vs Direct Pipeline Crossover (Python, 8192 events)

```
Transform             DataFusion    Direct     Winner
Simple filter         162 ns/evt    43 ns/evt  Direct (but DF overhead is acceptable)
Filter + rename       139 ns/evt   194 ns/evt  DataFusion 1.4x
Filter + regex        380 ns/evt  1071 ns/evt  DataFusion 2.8x
Complex combined      498 ns/evt  1315 ns/evt  DataFusion 2.6x
```

DataFusion's fixed overhead: ~0.5-2ms per query.
At 8192 rows: ~120 ns/event overhead (negligible).
Crossover for non-trivial transforms: ~2000-5000 events.

### Rust Scanner Benchmarks (scan_bench.rs, 8192 lines × 13 fields × 310 bytes)

```
Scan-only 3 fields (early exit):        74 ns/line   13.5M lines/sec
Scan-and-build all fields into Arrow:  399 ns/line    2.5M lines/sec
Scan-and-build 3 fields (pushdown):    342 ns/line    2.9M lines/sec
Arrow column construction overhead:     93 ns/line
```

**Arrow construction is only 93 ns/line.** Scanning more fields (232 ns) is the real cost.
SQL field pushdown reduces it.

### All-Utf8 vs Typed Columns (Python, 100K rows)

```
Operation                    Typed     Utf8+CAST   Ratio
String equality (level=ERR)  2.19ms    1.86ms      0.85x (Utf8 faster)
GROUP BY COUNT               2.68ms    2.50ms      0.93x (Utf8 faster)
Numeric compare (dur>1000)   2.00ms    3.09ms      1.54x
Numeric AVG                  1.83ms    2.36ms      1.29x
```

String ops (90% of log work): identical or faster with Utf8.
Numeric ops (10%): 1.3-1.5x slower with TRY_CAST.
Memory: 1.01x — essentially identical.

### Multi-Typed Columns (Python, 8192 events)

```
Memory overhead vs all-Utf8: 1.03x (essentially zero)
Filter on typed column (WHERE duration_ms_int > 1000): 5.29ms
Numeric average (AVG(duration_ms_int)): 17.74ms
```

Sparse null bitmaps: 1 bit/row per column. 80% null columns cost almost nothing.

### Map<Utf8, Utf8> vs Flat Columns (Python, 100K lines)

```
Map bracket access filter:    282 ns/line (3x slower than flat)
Flat column filter:            89 ns/line
```

Map works but is 3x slower. Flat columns win. `SELECT * EXCEPT` doesn't work on
map keys. **Decision: flat typed columns, not maps.**

## Dead Ends

- **arrow-json**: Crashes on type conflicts. Unusable for log data. We built our own scanner.
- **DenseUnion types**: DataFusion cannot filter/compare/aggregate Union values. Dead end.
- **Post-hoc type inference**: 5,534 ns/line brute force. **Decision: detect types during
  parsing** — JSON value type routes to typed builder at scan time.

## v1 Pipeline Benchmarks (Rust, measured)

VM-format data (257 bytes/line, 10 variable fields, arm64):
```
jsonlines no compression:  14.5M lines/sec/core
jsonlines + zstd:           3.1M lines/sec/core
OTLP raw body + zstd:       2.6M lines/sec/core
OTLP field extract + zstd:  1.5M lines/sec/core
```

K8s benchmark (VictoriaMetrics log-collectors-benchmark, 0.25 cores):
```
logfwd saturated at 108K/s (= 432K/core)
vlagent saturated at 65K/s (= 260K/core)
logfwd 1.7x faster at same CPU
```

Profile (jsonlines-zstd): read:6% cri:17% zstd:78%.
Compression dominates. Without compression: read:24% cri:76%.

## Projected v2 Throughput

```
scan → DataFusion passthrough: ~500 ns/line → 2.0M lines/sec
scan → DataFusion filter:      ~450 ns/line → 2.2M lines/sec
scan → DataFusion + OTLP out:  ~700 ns/line → 1.4M lines/sec  (10x vlagent)
scan → DataFusion + JSON out:  ~900 ns/line → 1.1M lines/sec  ( 8x vlagent)
```
