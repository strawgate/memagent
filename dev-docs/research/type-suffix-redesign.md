# Type Suffix Redesign Research

Date: 2026-03-31 (updated 2026-04-01)
Context: #445 — `_str`/`_int`/`_float` suffix convention is root cause of 11 bugs

## Core Requirement: Per-Document Type Fidelity

A field that enters as `200` (integer) in document A and `"OK"` (string)
in document B must exit with those exact types. No type promotion across
documents. As long as the user doesn't modify a column via SQL, the same
values come out the other end.

This rules out single-column-per-field with type promotion (Int→Float→Utf8).
Promotion would turn `200` into `"200"`, losing type information and breaking
downstream systems that expect integers for status codes.

Other log tools (Elasticsearch, Vector) preserve per-document types.
We must too.

## Problem

One JSON key can currently produce up to 3 Arrow columns (`status_int`,
`status_float`, `status_str`). The output layer dispatches on column
**name suffix** to determine serialization format. This breaks when:

- SQL transforms rename columns (suffix lost)
- SQL aggregates produce new columns (no suffix)
- User field names collide with suffixes
- Schema evolves across batches (different suffix sets)

## Revised Understanding

The multi-column approach (`status_int` + `status_str`) is actually
correct for type fidelity — it preserves round-trip JSON → Arrow → JSON
without losing type information. The bugs are NOT caused by having
multiple columns. They're caused by:

1. Output sinks dispatch on column **name suffix** instead of Arrow DataType
2. The SQL rewriter is incomplete and can never handle all SQL patterns
3. Column deduplication in `build_col_infos` uses suffix strings, not DataType
4. The rewriter was never wired in (#602) — it's dead code

## Design: Fix the Dispatch, Not the Schema

### Core rule

Keep the multi-column internal representation. Fix how it's consumed.

### Fix 1: Output sinks dispatch on Arrow DataType

`write_row_json` currently parses column name suffixes to decide how
to serialize. Replace with `field.data_type()` dispatch (matching OTLP):

```rust
match col.data_type() {
    DataType::Int64 => { /* write as JSON number */ }
    DataType::Float64 => { /* write as JSON number */ }
    _ => { /* write as JSON string */ }
}
```

This alone fixes #430, #444, #428.

### Fix 2: Delete the SQL rewriter (dead code)

The rewriter was never wired into the pipeline (#602) — `SqlTransform::execute()`
passes `user_sql` directly to DataFusion. Delete the 772 lines of dead code.

This means users currently write SQL against suffixed names (`status_int`,
`level_str`), which is ugly but functional. The SQL UX problem is separate
from the output dispatch bugs and should be solved independently.

### Open question: SQL UX for bare column names

How to let users write `WHERE status > 400` instead of `WHERE status_int > 400`:

**Option A: DataFusion view aliasing.** Register a view that maps bare
names to typed columns via COALESCE. Problem: for `SELECT status`,
which type wins? COALESCE picks one, losing the other. Recreates
rewriter complexity in SQL form.

**Option B: Custom TableProvider.** Present a schema with bare names,
resolve to typed columns based on query context. Powerful but complex.
DataFusion's `TableProvider` trait would need to inspect the query plan.

**Option C: UDFs.** Register `int(status)` → `status_int`, `str(status)`
→ `status_str`. Explicit, no ambiguity. Users learn a simple convention.
`WHERE int(status) > 400` is clear and predictable.

**Option D: Status quo.** Users write suffixed names. Document it well.
The `COLUMN_NAMING.md` doc already explains this. Not elegant but zero
new code and zero ambiguity.

**Option E: Arrow Union type columns.** Single `status` column of type
`Union<Int64, Utf8>`. Perfect type fidelity in one column. But
DataFusion's Union support is limited (no filtering, grouping, etc.).
Future option as DataFusion matures.

Decision deferred — solve output dispatch bugs first (Fix 1 + Fix 3),
then evaluate SQL UX options separately.

### Fix 3: Column deduplication uses DataType

`build_col_infos()` currently deduplicates by parsing suffix strings.
Replace with DataType-based priority: Int64 > Float64 > Utf8 for
the "primary" column when a field has multiple typed columns.
Fixes #404, #442.

### What stays the same

- Scanner still emits `append_str_by_idx` / `append_int_by_idx` etc.
- Builders still create `_int` / `_float` / `_str` columns internally
- Multi-column representation preserves type fidelity for round-tripping
- OTLP sink already dispatches on DataType (no change needed)

## What gets deleted

- `crates/logfwd-transform/src/rewriter.rs` — 772 lines, dead code (#602)
- `parse_column_name()` in `logfwd-output/src/lib.rs` — suffix parsing
  (replace with DataType dispatch throughout)
- `strip_type_suffix()` in `logfwd-transform/src/lib.rs`
- Name-based type dispatch in `write_row_json()` (already replaced by #568)

## What gets changed

| File | Change |
|------|--------|
| `logfwd-output/lib.rs` | DataType dispatch in write_row_json |
| `logfwd-output/lib.rs` | DataType-based dedup in build_col_infos |
| `logfwd-output/stdout.rs` | DataType dispatch for console format |
| `logfwd-transform/lib.rs` | Schema aliasing instead of rewriting |
| `rewriter.rs` | **DELETE** |

## Bugs this fixes

| Fix | Bugs resolved |
|-----|---------------|
| DataType dispatch in output | #430, #444, #428 |
| Schema aliasing (delete rewriter) | #415, #429, #531 |
| DataType-based dedup | #404, #442 |
| Escape handling fix | #410 |

## Key insight

The multi-column representation is correct and necessary for per-document
type fidelity. The bugs are in the **consumption** layer, not the
**production** layer. The builders produce correct typed columns. The
output sinks consume them incorrectly by parsing column names instead of
checking Arrow DataType.

The SQL rewriter was the wrong abstraction — it tried to hide the
multi-column schema behind bare names, but SQL AST rewriting can never
be complete. The rewriter was dead code anyway (#602). Delete it.

Output sinks should strip suffixes from column names when serializing
(JSON key = bare name) and dispatch on `field.data_type()` to determine
value formatting. This preserves round-trip fidelity: int in → int out,
string in → string out, per document.

## Implementation phases

1. **Delete dead code** — rewriter.rs, strip_type_suffix(). Zero risk.
2. **Output suffix stripping** — output sinks strip `_int`/`_float`/`_str`
   from column names when serializing. Uses `parse_column_name()` for
   name, `data_type()` for format. JSON: `"status": 200` not
   `"status_int": 200`.
3. **SQL UX** — separate effort, options A-E above. Not urgent since
   the rewriter was never wired in and users already use suffixed names.
