# DataFusion SQL Transform: Per-Batch Overhead Profiling

> Profiling report for issue #927.
> Benchmark: `cargo bench -p ffwd-bench --bench transform_overhead`
> Environment: GitHub Actions runner (x86-64)

## 1. Current Baseline

### Phase Decomposition (10K-row production-mixed batch)

Each `SqlTransform::execute()` call performs these phases:

| Phase | Median Time | Notes |
|-------|------------|-------|
| `SessionContext::new()` | **103 µs** | One-time cost (amortized to ~0 after first batch) |
| `MemTable::try_new()` | **~129 ns** | Constant regardless of batch size (wraps existing batch) |
| Table deregister + register | **~1.2 µs** | Catalog hash-map swap; constant cost |
| `ctx.sql()` (plan only) | **57–166 µs** | SQL parse → logical plan → optimization (varies by query) |
| `df.collect()` (execute) | **160–893 µs** | Physical execution (dominates for filter/aggregate queries) |

### Planning Cost by Query Type (`ctx.sql()` only, 10K rows)

| Query | Plan Cost (µs) |
|-------|----------------|
| `SELECT level, message, status` (3-col projection) | 57 |
| `GROUP BY + AVG` | 101 |
| `GROUP BY + COUNT` | 102 |
| `SELECT *` (passthrough) | 119 |
| `LIKE '%users%'` | 131 |
| `WHERE level = 'ERROR'` | 134 |
| `ORDER BY ... LIMIT 100` | 137 |
| `WHERE compound (AND)` | 141 |
| `CASE WHEN ... THEN` | 166 |

**Key finding:** Planning cost is **57–166 µs** per batch, independent of batch size.
The plan is **not cached** between batches — `ctx.sql()` re-parses and re-optimizes
every call. However, at ~120 µs average, planning overhead is modest compared to
execution cost for batches ≥1K rows.

### Full Execution Cost (`ctx.sql()` + `df.collect()`, 10K rows)

| Query | Total (µs) | Plan (µs) | Execute (µs) | Plan % |
|-------|-----------|-----------|-------------|--------|
| Projection (3 cols) | 160 | 57 | 103 | 36% |
| `SELECT *` | 297 | 119 | 178 | 40% |
| `ORDER BY ... LIMIT` | 639 | 137 | 502 | 21% |
| `GROUP BY + COUNT` | 742 | 102 | 640 | 14% |
| `WHERE level = 'ERROR'` | 893 | 134 | 759 | 15% |

**Key finding:** For simple queries (passthrough, projection), planning is **36–40%**
of total time. For filter/aggregate queries, planning drops to **14–21%** because
execution work grows. This means plan caching would yield significant speedups
for passthrough-heavy workloads but diminishing returns for complex queries.

## 2. Per-Change Benchmarking

### Change 1: Context Reuse (Already Implemented)

The current `SqlTransform` caches the `SessionContext` and reuses it across batches.
Measured impact:

| Query | Steady State (µs) | Cold Start (µs) | Savings |
|-------|-------------------|-----------------|---------|
| `SELECT *` | 332 | 498 | **33% faster** |
| `WHERE level = 'ERROR'` | 918 | 1,100 | **17% faster** |
| `regexp_extract(...)` | 2,084 | 2,439 | **15% faster** |

Context reuse saves **~166 µs** (the cost of `SessionContext::new()` + UDF
registration) on every batch after the first. This is already implemented.

### Change 2: Batch Size Scaling (Overhead Amortization)

| Batch Size | SELECT * (µs) | WHERE filter (µs) | GROUP BY (µs) |
|-----------|--------------|-------------------|---------------|
| 100 rows | 323 | 464 | 599 |
| 1K rows | 328 | 502 | 618 |
| 10K rows | 327 | 921 | 772 |
| 100K rows | 327 | 4,971 | 2,127 |

**Key findings:**
- **`SELECT *` (passthrough):** Nearly constant at **~325 µs** regardless of batch
  size. This means overhead is **~325 µs fixed cost** and actual data work is
  negligible (DataFusion can project columns without copying).
- **WHERE filter:** Scales linearly from 464 µs (100 rows) to 4,971 µs (100K
  rows). The fixed overhead (~325 µs) is amortized as batch size grows.
- **GROUP BY:** Sub-linear scaling — hash aggregation is efficient.

**Recommendation:** Batch sizes of **10K–50K rows** are the sweet spot. Below 1K
rows, the fixed ~325 µs overhead dominates. Above 100K, filter/sort queries
become expensive.

### Change 3: Query Complexity Scaling (10K rows, steady state)

| Query | Time (µs) | Relative to Passthrough |
|-------|----------|------------------------|
| `SELECT level, message, status` (projection) | 196 | 0.6× |
| `SELECT * EXCEPT (request_id)` | 322 | 1.0× |
| `SELECT *` (passthrough) | 326 | 1.0× (baseline) |
| `ORDER BY ... LIMIT 100` | 670 | 2.1× |
| `LIKE '%users%'` | 725 | 2.2× |
| `GROUP BY level` | 780 | 2.4× |
| `WHERE compound (AND)` | 808 | 2.5× |
| `CASE WHEN ... THEN` | 914 | 2.8× |
| `GROUP BY + AVG` | 912 | 2.8× |
| `WHERE level = 'ERROR'` | 922 | 2.8× |
| `WHERE level IN (...)` | 1,210 | 3.7× |
| `regexp_extract(...)` | 2,089 | 6.4× |
| `grok(...)` | 52,063 | 160× |

**Key findings:**
- **Projection** is faster than passthrough (fewer columns to handle).
- **Simple filters** (WHERE, LIKE) are 2–3× passthrough.
- **regexp_extract** is 6× passthrough — regex compilation overhead per batch.
- **grok** is **160× passthrough** — the grok pattern engine is extremely
  expensive. This is the single largest bottleneck in the transform layer.

### Overhead vs Work Threshold

At 10K rows with `SELECT *`, the measured time is **326 µs**, of which:
- Table swap: ~1.2 µs (0.4%)
- Planning: ~119 µs (36%)
- Execution: ~178 µs (55%)
- Other (schema hash, normalize): ~28 µs (9%)

For filter queries, the ratio shifts:
- Planning: ~134 µs (15% of 922 µs)
- Execution: ~788 µs (85%)

**The crossover point** where transform overhead exceeds scan time depends on the
query. For `SELECT *`, scan typically takes ~2 ms for 10K rows of production-mixed
data, making the 326 µs transform overhead about **16% of total scan+transform
time**. For `grok()`, the 52 ms transform time **dominates** at ~96% of total.

## 3. Summary Table

| Change | CPU Impact | Memory Impact | Throughput Impact | Composability |
|--------|-----------|---------------|-------------------|---------------|
| **Context reuse** (implemented) | −166 µs/batch (−33% for passthrough) | Negligible (one SessionContext kept alive) | +33% for first batch, +15-17% steady state | Already active |
| **Larger batches** (10K→100K) | Passthrough: flat; Filter: +14× | Higher peak RSS (more rows in memory) | Amortizes 325 µs overhead over more rows | Composes with all |
| **Projection pushdown** (implemented) | −130 µs/batch (326→196 µs) | Less memory (fewer columns) | +67% for SELECT with specific columns | Composes with all |
| **Plan caching** (not implemented) | −119 µs/batch for passthrough | ~10 KB cached plan objects | +36% for passthrough, +15% for filters | Requires schema stability detection |
| **Grok optimization** (not implemented) | −50 ms/batch (52→TBD) | Cached compiled patterns already used | Would unlock 160× improvement potential | Independent of other changes |
| **Regex pre-compilation** (not implemented) | −500 µs/batch estimate | Cached Regex objects | +24% for regexp_extract queries | Independent |

## 4. Recommendations

### Priority 1: Grok Pattern Performance (Highest Impact)
`grok()` at **52 ms** for 10K rows is 160× passthrough. Profile the grok UDF
internals — the `grok` crate may be re-compiling patterns per invocation.
Pre-compiling the pattern once (on `SqlTransform::new()`) and reusing it could
yield massive improvements.

### Priority 2: Plan Caching for Stable Schemas (Medium Impact)
When batch schema is stable (same hash as previous batch), the SQL plan could be
cached and re-executed without `ctx.sql()`. This saves **~120 µs per batch**
(36% for passthrough). Implementation: cache the `DataFrame` or `LogicalPlan`
and re-bind the new `MemTable` without re-planning.

### Priority 3: Batch Size Tuning (Low-Code Impact)
Document that optimal batch sizes are **10K–50K rows**. Below 1K rows, the ~325 µs
fixed overhead per batch becomes significant. The pipeline should coalesce small
batches before transform.

### Priority 4: Regex UDF Compilation Caching (Medium Impact)
`regexp_extract()` at 2 ms for 10K rows suggests per-batch regex compilation
overhead. Caching compiled `Regex` objects keyed by pattern string would help
for repeated patterns.

### What NOT to Optimize
- **MemTable creation** (~129 ns) — already negligible.
- **Table deregister/register** (~1.2 µs) — already negligible.
- **SessionContext creation** (~103 µs) — already amortized via caching.
- **Planning for complex queries** (100–166 µs) — modest compared to execution.
