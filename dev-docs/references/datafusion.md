# DataFusion -- Agent Reference

> Written for DataFusion v45. Workspace currently uses v48 (PR #794). Verify details against current version.

Non-obvious behavior, patterns extracted from source, and gotchas.

---

## 1. SessionContext Lifecycle

### Creation

```rust
use datafusion::prelude::*;

let ctx = SessionContext::new();
// or with config:
let ctx = SessionContext::new_with_config(SessionConfig::new());
```

`SessionContext::new()` is cheap -- it allocates catalog structures and a default
`RuntimeEnv` but does **no** I/O and compiles **no** plans. The internal state is
behind `Arc<RwLock<SessionState>>`, so cloning `SessionContext` shares state.

### Reusing across batches with different schemas

**Yes, this works.** The pattern:

```rust
let ctx = SessionContext::new();
ctx.register_udf(ScalarUDF::from(IntCastUdf::new()));  // register once

// Batch 1: schema {host: Utf8, level: Utf8}
ctx.deregister_table("logs")?;
let t1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
ctx.register_table("logs", Arc::new(t1))?;
let r1 = ctx.sql("SELECT * FROM logs").await?.collect().await?;

// Batch 2: schema {host: Utf8, level: Utf8, region: Utf8}
ctx.deregister_table("logs")?;
let t2 = MemTable::try_new(schema2, vec![vec![batch2]])?;
ctx.register_table("logs", Arc::new(t2))?;
let r2 = ctx.sql("SELECT * FROM logs").await?.collect().await?;
// Works -- the plan is rebuilt from SQL text each time ctx.sql() is called.
```

Key insight: `ctx.sql()` calls `state.create_logical_plan(sql)` which re-parses
and re-plans every time. There is **no cached plan** that goes stale when the
schema changes. The table provider is looked up fresh during planning.

### Best practice: reuse SessionContext

Creating a new SessionContext per batch is safe but wasteful -- UDFs get re-registered
every call. A typical reuse pattern:
1. Create `SessionContext` once at startup
2. Register UDFs once
3. `deregister_table("logs")` + `register_table("logs", ...)` per batch

The cost savings: skip UDF registration and `SessionContext` allocation.
SQL parsing + optimization still runs per batch regardless.

---

## 2. MemTable Registration

### Creating a MemTable from a RecordBatch

```rust
use datafusion::datasource::MemTable;

// partitions: Vec<Vec<RecordBatch>> -- outer vec = partitions, inner = batches
let table = MemTable::try_new(schema, vec![vec![batch]])?;
ctx.register_table("logs", Arc::new(table))?;
```

**Schema requirement:** `MemTable::try_new` validates that every batch's schema is
*contained in* the provided schema (via `SchemaExt::contains`). This means the
MemTable schema can be a superset of individual batch schemas, but not a subset.
If the batch has a field not in the MemTable schema, you get a debug log but
**no error** -- the field is silently ignored.

### Deregister and re-register with different schema

```rust
// Returns the old TableProvider, or None if not found.
let _old = ctx.deregister_table("logs")?;

// Register new table with different schema -- no problem.
let new_table = MemTable::try_new(new_schema, vec![vec![new_batch]])?;
ctx.register_table("logs", Arc::new(new_table))?;
```

`deregister_table` removes from the schema provider's `HashMap`. The old
`Arc<dyn TableProvider>` is dropped when the returned `Option` goes out of scope
(assuming no other references).

**Gotcha:** If you call `register_table` with a name that already exists, it
**replaces** the old table and returns `Some(old_provider)`. So deregister is
technically optional -- but being explicit is clearer.

### Multiple partitions

```rust
// Two partitions for parallelism:
let table = MemTable::try_new(schema, vec![vec![batch_a], vec![batch_b]])?;
```

For a single-batch-per-call pattern, one partition with one batch is fine.

---

## 3. UDF Registration

### The ScalarUDFImpl trait (v45)

Required methods:

```rust
impl ScalarUDFImpl for MyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "my_func" }          // lowercase! SQL lookup is case-insensitive
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> { ... }
}
```

Optional overrides (important ones):

```rust
// When return type depends on argument VALUES (not just types):
fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> { ... }
```

A typical usage is a Grok UDF that uses `return_type_from_args` because the struct
fields depend on the pattern literal. `return_type` is a fallback that returns `Utf8`.

### Registration pattern

```rust
ctx.register_udf(ScalarUDF::from(MyCustomUdf::new()));
```

`ScalarUDF::from(impl ScalarUDFImpl)` wraps the impl in an `Arc`. The
`register_udf` method takes `ScalarUDF` by value (not `Arc`).

**UDF name lookup is lowercase.** SQL `SELECT INT(x)` resolves to a UDF named
`"int"`. Quoting preserves case: `SELECT "Int"(x)` looks for `"Int"`.

### Signature patterns

```rust
// Exact types, one overload:
Signature::exact(vec![DataType::Utf8], Volatility::Immutable)

// Multiple exact overloads:
Signature::new(
    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
    Volatility::Immutable,
)
```

Volatility matters for optimization:
- `Immutable` -- same inputs always produce same output. Optimizer can fold constants.
- `Stable` -- same within a query but may change between queries (e.g., `now()`).
- `Volatile` -- may change per row (e.g., `random()`).

### ColumnarValue: the dual array/scalar type

UDF args come as `ColumnarValue::Array(ArrayRef)` or `ColumnarValue::Scalar(ScalarValue)`.
You **must handle both**. Scalars appear when DataFusion optimizes constants. Pattern:

```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    match &args.args[0] {
        ColumnarValue::Array(array) => {
            // Process array, return Array
        }
        ColumnarValue::Scalar(scalar) => {
            // Convert to 1-element array, process, convert back to scalar
            let arr = scalar.to_array()?;
            let result = process(&arr)?;
            let sv = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(sv))
        }
    }
}
```

---

## 4. SQL Execution

### Basic flow

```rust
let df: DataFrame = ctx.sql("SELECT * FROM logs WHERE level = 'ERROR'").await?;
let batches: Vec<RecordBatch> = df.collect().await?;
```

`ctx.sql()` does: parse SQL -> logical plan -> optimize -> return `DataFrame`.
`df.collect()` does: physical plan -> execute -> collect all batches.

### Handling empty results

`collect()` can return an empty `Vec<RecordBatch>`. This does NOT mean "no schema" --
it means zero rows were produced. To get the schema of an empty result:

```rust
let batches = df.collect().await?;
match batches.len() {
    0 => {
        // Re-plan to get schema:
        let df2 = ctx.sql(&sql).await?;
        let schema: SchemaRef = Arc::clone(df2.schema().inner());
        Ok(RecordBatch::new_empty(schema))
    }
    1 => Ok(batches.into_iter().next().unwrap()),
    _ => {
        let schema = batches[0].schema();
        Ok(arrow::compute::concat_batches(&schema, &batches)?)
    }
}
```

**Better approach** (avoids re-planning): grab schema from the DataFrame before collecting:

```rust
let df = ctx.sql(&sql).await?;
let schema = Arc::clone(df.schema().inner());
let batches = df.collect().await?;
if batches.is_empty() {
    return Ok(RecordBatch::new_empty(schema));
}
```

### Concatenating result batches

DataFusion may return multiple batches (one per partition, or from internal splits).
Use `arrow::compute::concat_batches`:

```rust
use arrow::compute::concat_batches;
let merged = concat_batches(&schema, &batches)?;
```

This is zero-copy for single batches and allocates a new contiguous batch for multiple.

---

## 5. Common Gotchas

### "Cannot start a runtime from within a runtime"

DataFusion's `ctx.sql()` and `df.collect()` are async. If you're already inside a
tokio runtime, calling `Runtime::new().block_on(async { ctx.sql(...) })` panics
with "Cannot start a runtime from within a runtime."

**Workaround when calling from synchronous context:**

```rust
let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
rt.block_on(async { ... })
```

This works when the caller is in a synchronous context.
If you're already inside an async context, just `.await` directly -- no nested runtime.

**If you must bridge sync/async inside an existing runtime**, use
`tokio::task::block_in_place` + a `Handle`:

```rust
let handle = tokio::runtime::Handle::current();
tokio::task::block_in_place(|| handle.block_on(async { ctx.sql(&sql).await }))
```

### Schema errors: querying columns that don't exist

```sql
SELECT hostname FROM logs
-- Error: Schema error: No field named hostname. Valid fields are: level_str, msg_str.
```

DataFusion fails at **planning time** (during `ctx.sql()`), not at execution.
A best practice is to pre-analyze SQL and control which columns the upstream
stage emits.

### Type coercion surprises

- **String literals vs column types:** `WHERE status = 200` fails if `status` is Utf8.
  You need `WHERE status = '200'` or `WHERE CAST(status AS INT) = 200`.
- **NULL propagation:** A custom `int()` UDF using `arrow::compute::cast` does safe
  casting (returns NULL on failure), whereas `CAST('not_a_number' AS INT)` returns
  an **error**.
- **Utf8 vs LargeUtf8:** Arrow has both. If your RecordBatch uses `LargeUtf8` but
  a UDF signature declares `Utf8`, you'll get a type mismatch. Use `DataType::Utf8`
  consistently or use `TypeSignature::Coercible` for flexibility.

### `register_table` silently replaces

Calling `register_table("logs", new_table)` when "logs" already exists **succeeds**
and returns the old table. There is no error. This is usually what you want but can
mask bugs where you forgot to deregister.

### SQL is case-insensitive for identifiers

`SELECT Level_Str FROM logs` resolves to column `level_str`. Column names in Arrow
schemas are **case-sensitive**, but DataFusion's SQL planner lowercases unquoted
identifiers. If your schema has `Level_Str`, query it as `"Level_Str"` (quoted).

---

## 6. Performance

### Cost of SessionContext creation

**Cheap.** Allocates:
- `SessionState` (config, catalog, function registries)
- Default `MemoryCatalogProvider` + `MemorySchemaProvider`
- `RuntimeEnv` (memory pool, disk manager -- lazy-init)

Measured: sub-microsecond. Not worth caching to avoid allocation alone, but worth
caching to preserve registered UDFs and config.

### Cost of SQL parsing

`Parser::parse_sql` (sqlparser): microseconds for simple queries. The parser is
stateless and allocation-light. Not a bottleneck for per-batch execution.

### Cost of plan optimization

`ctx.sql()` runs the full optimizer pipeline (~20 rules by default). For simple
`SELECT ... FROM logs WHERE ...` queries, this is hundreds of microseconds.

**What to consider caching:**
- The `LogicalPlan` could theoretically be cached and re-bound to a new table scan,
  but DataFusion's API doesn't expose a clean way to do this in v45.
- `SessionContext` with UDFs pre-registered -- saves Nx `register_udf` per batch.
- The `tokio::Runtime` -- `Runtime::new()` is ~100us. Reuse if possible.

**What you can't cache:**
- Physical plans -- they reference specific table partitions / schemas.
- `DataFrame` -- it holds a snapshot of the plan, not reusable across schema changes.

### Cost breakdown for a typical per-batch SQL execution

| Step | Cost | Cacheable? |
|---|---|---|
| `Runtime::new()` | ~100us | Yes -- reuse runtime |
| `SessionContext::new()` | <1us | Yes -- reuse ctx |
| `register_udf` xN | ~1us each | Yes -- register once |
| `MemTable::try_new` | <1us | No -- new data each time |
| `register_table` | <1us | No -- new schema possible |
| `ctx.sql()` (parse+optimize) | ~200-500us | No (schema may change) |
| `df.collect()` (execute) | proportional to data | No |

**Bottom line:** At high throughput in ~1000-row batches, the fixed overhead is
~300-600us per batch. The dominant cost is `collect()` (actual data processing).
Reusing `SessionContext` + `Runtime` saves ~100us per batch -- marginal but free.

### Recommended pattern: reusable SQL transform

```rust
pub struct SqlTransform {
    ctx: SessionContext,
    rt: tokio::runtime::Runtime,
    user_sql: String,
}

impl SqlTransform {
    pub fn new(sql: &str, udfs: Vec<ScalarUDF>) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("{e}"))?;

        let ctx = SessionContext::new();
        for udf in udfs {
            ctx.register_udf(udf);
        }

        Ok(Self { ctx, rt, user_sql: sql.to_string() })
    }

    pub fn execute(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        // Swap table -- deregister is optional since register_table replaces,
        // but explicit deregister avoids holding old data in memory during planning.
        let _ = self.ctx.deregister_table("logs");
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| format!("{e}"))?;
        self.ctx.register_table("logs", Arc::new(table))
            .map_err(|e| format!("{e}"))?;

        self.rt.block_on(async {
            let df = self.ctx.sql(&self.user_sql).await
                .map_err(|e| format!("{e}"))?;
            let schema = Arc::clone(df.schema().inner());
            let batches = df.collect().await
                .map_err(|e| format!("{e}"))?;
            match batches.len() {
                0 => Ok(RecordBatch::new_empty(schema)),
                1 => Ok(batches.into_iter().next().unwrap()),
                _ => arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| format!("{e}")),
            }
        })
    }
}
```
