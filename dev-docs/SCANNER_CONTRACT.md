# Scanner Contract

This document formalises the contract between callers and the scanner layer
(`Scanner`).  It covers input requirements,
output guarantees, and known limitations.

Architecturally, the scanner sits at the **Parsing / Materialization**
boundary. Upstream source, framing, normalization, and batching layers may use
different internal representations, but this contract describes the shape that
must be presented to the scanner itself.

---

## Input Requirements

### Contiguous buffer boundary

The default scanner entrypoints take a single contiguous `bytes::Bytes`
backing buffer:

- `Scanner::scan(Bytes)`
- `Scanner::scan_detached(Bytes)`

Upstream batching may hold one chunk or many chunks while a batch is being
formed, but the default scan boundary is still one contiguous buffer. If
pre-scan accumulation is fragmented, concatenation must happen before these
scanner entrypoints are called.

### UTF-8

The scanner assumes that **all input bytes are valid UTF-8** unless validation
is explicitly enabled.

- The Arrow builder path validates or lossy-converts most externally supplied
  bytes: field names use `String::from_utf8_lossy`, scanned string values are
  validated before Arrow append, and line capture falls back to lossy
  conversion. Internal builder paths can still call `from_utf8_unchecked` from
  `finish_batch()` for bytes that bypass scanner validation, so callers that
  accept arbitrary raw bytes must not rely on invalid UTF-8 being harmless.
- JSON object keys are unescaped before `wanted_fields` matching and field
  resolution, so a key like `{"a\u002eb": ...}` is surfaced as field `a.b`.
- For production validation set `ScanConfig::validate_utf8 = true`.  This
  performs a safe `from_utf8` check before scanning and returns a descriptive
  scanner error on failure. It has a small throughput cost and is disabled by
  default.
- The fuzz target (`crates/logfwd-core/fuzz/fuzz_targets/scanner.rs`) guards
  against UTF-8 violations for arbitrary byte sequences.
- Upstream components (CRI parser, file tailer) do not currently validate
  UTF-8; callers that control raw byte sources are responsible for ensuring
  validity.

### NDJSON format

The scanner expects **newline-delimited JSON** (`\n`-separated lines):

- Each non-empty line must be a JSON object (`{…}`).
- Non-object lines (JSON arrays, bare scalars, blank lines) are **silently
  skipped**: a row is still emitted with all columns null.
- A final non-empty line without trailing `\n` is processed. Reassembly of
  truly incomplete lines across reads is the responsibility of the format layer
  (`FramedInput` and its format decoders).

### Size limits

- `StreamingBuilder` uses `u32` row counters, so a single batch is limited to
  **4 294 967 295 rows**.
- `StreamingBuilder` stores string positions as `u32` offsets into the input
  buffer, limiting the buffer to **4 GB** (2³² bytes).  In practice, the
  default batch size is ~4 MB, well within this bound.
- Individual field values have no explicit size limit beyond the batch buffer
  size.

---

## Output Guarantees

### Column naming

Each JSON field `<name>` produces columns based on observed types:

**Single type (no conflict):** bare field name with native Arrow type.

| Observed type   | Column name | Arrow type  |
|-----------------|-------------|-------------|
| Integer (only)  | `<name>`    | `Int64`     |
| Float (only)    | `<name>`    | `Float64`   |
| String (only)   | `<name>`    | `Utf8` / `Utf8View` |
| Boolean (only)  | `<name>`    | `Boolean` |

**Multiple types (conflict):** a single `StructArray` column with typed children.

| Observed types  | Column name | Arrow type  |
|-----------------|-------------|-------------|
| Integer + String | `<name>`   | `Struct { int: Int64, str: Utf8View }` |
| Boolean + String | `<name>`   | `Struct { bool: Boolean, str: Utf8View }` |
| All three       | `<name>`   | `Struct { int: Int64, float: Float64, str: Utf8View }` |
| All four        | `<name>`   | `Struct { int: Int64, float: Float64, bool: Boolean, str: Utf8View }` |

Conflict structs are detected by `is_conflict_struct()` (child fields named
from `{"int", "float", "str", "bool"}`). They only appear when a field has
multiple types across rows within the same batch.

| Special column  | Description       | Arrow type  |
|-----------------|-------------------|-------------|
| `body` (default) | Original raw line | `Utf8` / `Utf8View` |

The line-capture column is only present when `ScanConfig::line_field_name` is set.

### Nullability

**All columns are nullable**.  A null indicates that a particular row did not
contain a value for that field (or contained it as a different type, see
[Type conflicts](#type-conflicts)).

### Row count

The number of rows in the output `RecordBatch` equals the number of
**non-empty lines** in the input buffer (regardless of whether each line is a
valid JSON object).

### Type conflicts

If the same field name appears as different JSON types across rows — for
example `"status": 200` in one row and `"status": "OK"` in another — the
builder produces a single **StructArray conflict column** with typed children:
`status: Struct { int: Int64, str: Utf8View }`. Within each child array the
rows that did not supply that type are null.

If a field has only one type across all rows in the batch, it gets a bare
column name with the native Arrow type (e.g., `status` as `Int64`).

### Duplicate keys

When a JSON object contains the same key more than once, **first-writer-wins**:
only the first occurrence is stored; subsequent occurrences are silently
ignored.

This guarantee holds for the first 63 fields in a row.  For fields 64 and
beyond the duplicate-key detection bitmask is exhausted and a duplicate key
*may* overwrite the first occurrence.  In practice, log lines rarely exceed 63
fields.

### Integers vs. floats

- A numeric value with a decimal point (`.`) or an exponent (`e`/`E`) is
  stored as `Float64`.
- A value without either is first tried as `Int64` via `parse_int_fast`.  On
  overflow (value does not fit in `i64`) it falls back to `Float64`.
- `true` and `false` are stored in a native `Boolean` column for boolean-only
  fields, or in the `bool: Boolean` child of conflict structs for mixed-type
  fields.
- `null` JSON values produce a null entry in the appropriate column.

### String escape decoding

JSON escape sequences in string values (`\"`, `\\`, `\/`, `\b`, `\f`, `\n`,
`\r`, `\t`, `\uXXXX`) are decoded to their UTF-8 representation during
extraction.  This prevents double-escaping when values are re-serialized
downstream (see issue #410).

The `body` line-capture column is **not** decoded — it stores the original raw
line verbatim.

### Escaped-key normalization

JSON Unicode escapes in object keys (e.g., `\u002e`) are decoded before field
name matching.  A wanted field `a.b` matches both `"a.b"` and `"a\u002eb"` in
source JSON.  This normalization applies to all standard JSON escape sequences
(`\"`, `\\`, `\/`, `\b`, `\f`, `\n`, `\r`, `\t`, `\uXXXX`).

### Batch reuse

`Scanner` can be reused across batches. Each call to `scan()` or
`scan_detached()` resets builder state internally and produces an
independent `RecordBatch`; schema and data from previous batches are not
carried over.

---

## Known Limitations

- **No UTF-8 validation by default** — see [UTF-8](#utf-8) above.
- **Silent skip on non-object lines** — JSON arrays and scalars at the
  top-level are skipped with no error or warning.
- **Duplicate-key detection is limited to the first 63 fields per row** — for
  rows with 64+ distinct field names, a duplicate key in the 64th field or
  beyond will not be detected and may silently produce incorrect data.
- **`StreamingBuilder` offsets are `u32`** — buffers larger than 4 GB are
  unsupported.
- **`body` column** — setting `line_field_name` is supported by both `scan()` and
  `scan_detached()` modes.
