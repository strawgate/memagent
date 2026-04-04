# Arrow Rust -- Agent Reference

> Written for Arrow v54.3.1. Workspace currently uses v55 (PR #794). Verify details against current version.

Non-obvious API details sourced from the actual crate source at
`~/.cargo/registry/src/index.crates.io-*/arrow-{array,ipc,schema,buffer}-54.3.1/`.

---

## 1. RecordBatch Construction

### What an agent needs to know

- `RecordBatch::try_new(schema, columns)` validates: column count matches schema
  field count, all columns have the same length, data types match, and non-nullable
  fields have zero nulls. Any mismatch returns `Err`, not a panic.
- `schema()` clones the `Arc<Schema>`. Use `schema_ref()` to borrow without cloning.
- `RecordBatch` stores `row_count` separately -- this matters for zero-column batches.
- `RecordBatchOptions::match_field_names` defaults to `true`. With `false`, it uses
  `DataType::equals_datatype` which ignores field names inside nested types.
- `try_from_iter` infers nullability from `null_count() > 0`. If you need explicit
  nullable=true on a column with no current nulls, use `try_from_iter_with_nullable`.

### Direct array construction (preferred for hot path)

```rust
use std::sync::Arc;
use arrow_array::{RecordBatch, StringArray, Int64Array, Float64Array, ArrayRef};
use arrow_schema::{Schema, Field, DataType};

let schema = Arc::new(Schema::new(vec![
    Field::new("msg",   DataType::Utf8,    true),
    Field::new("ts",    DataType::Int64,   false),
    Field::new("value", DataType::Float64, true),
]));

let msg: ArrayRef   = Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")]));
let ts: ArrayRef    = Arc::new(Int64Array::from(vec![1, 2, 3]));
let value: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None]));

let batch = RecordBatch::try_new(schema, vec![msg, ts, value]).unwrap();
```

### Builder pattern (when row count is unknown upfront)

```rust
use arrow_array::builder::{StringBuilder, Int64Builder, Float64Builder};

let mut msg_b   = StringBuilder::new();
let mut ts_b    = Int64Builder::new();
let mut value_b = Float64Builder::new();

for _ in 0..1000 {
    msg_b.append_value("line");
    ts_b.append_value(42);
    value_b.append_null();       // explicit null
}

// finish() resets the builder -- it can be reused for the next batch
let msg_arr   = Arc::new(msg_b.finish())   as ArrayRef;
let ts_arr    = Arc::new(ts_b.finish())    as ArrayRef;
let value_arr = Arc::new(value_b.finish()) as ArrayRef;

let batch = RecordBatch::try_new(schema.clone(), vec![msg_arr, ts_arr, value_arr]).unwrap();
```

### How nulls work

- For primitive arrays: `Int64Array::from(vec![Some(1), None, Some(3)])` -- the `None`
  generates a null bitmap automatically.
- For string arrays: `StringArray::from(vec![Some("a"), None])` works the same way.
- A `Field` with `nullable=false` will cause `try_new` to error if the array has any
  nulls. This is checked at construction time via `null_count()`.
- Null bitmap is stored as a `NullBuffer` inside each array, not on the RecordBatch.

### Quick construction for tests

```rust
use arrow_array::record_batch;

let batch = record_batch!(
    ("a", Int32, [1, 2, 3]),
    ("b", Float64, [Some(4.0), None, Some(5.0)]),
    ("c", Utf8, ["alpha", "beta", "gamma"])
).unwrap();
```

The `record_batch!` macro always marks fields `nullable=true`.

---

## 2. StringViewArray

### What an agent needs to know

`StringViewArray` is the Arrow "view" layout (Utf8View data type). It differs from
`StringArray` (Utf8 data type) in a fundamental way:

| | StringArray (Utf8) | StringViewArray (Utf8View) |
|---|---|---|
| Layout | offsets buffer + single values buffer | views buffer (16 bytes each) + N data buffers |
| Short strings (<= 12 bytes) | always in values buffer | inlined in the view, zero indirection |
| take/filter | must copy string bytes | only manipulate 16-byte views |
| Prefix comparison | must deref to values buffer | first 4 bytes inlined in view |
| Memory overhead per element | 4 bytes (i32 offset) | 16 bytes (u128 view) |
| Best for | small datasets, simple writes | large datasets, filtering, zero-copy references |

**In a log processing pipeline**, StringViewArray is ideal for streaming builders because
log lines can reference the original buffer without copying. Use `append_block` +
`try_append_view` for zero-copy construction from a parsed buffer.

### Construction: simple

```rust
use arrow_array::StringViewArray;

// From vec (copies data)
let arr = StringViewArray::from(vec!["hello", "world"]);
let arr = StringViewArray::from(vec![Some("hello"), None, Some("world")]);

// From iterator
let arr: StringViewArray = vec![Some("a"), None, Some("c")].into_iter().collect();
```

### Construction: zero-copy from existing buffer

```rust
use arrow_array::builder::StringViewBuilder;
use arrow_buffer::Buffer;

let mut builder = StringViewBuilder::new();

// Your raw data buffer (e.g., a parsed log page)
let raw_bytes: &[u8] = b"helloworld_this_is_a_long_string";
let block = builder.append_block(Buffer::from(raw_bytes));

// Point views into the existing buffer -- no copy of string data
builder.try_append_view(block, 0, 5).unwrap();   // "hello"
builder.try_append_view(block, 5, 27).unwrap();  // "world_this_is_a_long_string"

let array = builder.finish();
assert_eq!(array.value(0), "hello");
```

**Key detail**: `append_block` flushes any in-progress internal block first, then stores
your buffer as-is. The returned `u32` is the block index. `try_append_view` validates
bounds and UTF-8; `append_view_unchecked` skips validation (unsafe).

### Construction: with deduplication

```rust
let mut builder = StringViewBuilder::new()
    .with_deduplicate_strings();  // uses ahash internally

builder.append_value("repeated");
builder.append_value("repeated"); // reuses the same view
let arr = builder.finish();
// Only one copy of "repeated" in the data buffers
```

### Converting StringArray to StringViewArray (zero-copy when possible)

```rust
use arrow_array::{StringArray, StringViewArray};

let string_arr = StringArray::from(vec!["a", "b", "c"]);
// Zero-copy if total value bytes < u32::MAX (4GB)
let view_arr = StringViewArray::from(&string_arr);
```

This reuses the existing values buffer by calling `append_block` internally.

### gc() -- compacting after filter/slice

After `filter` or `slice`, the data buffers may hold much more data than the views
reference. Call `gc()` to compact:

```rust
let compacted = view_array.gc();
// Rebuilds data buffers to contain only referenced data
// WARNING: this copies all string data -- expensive. Only use when you know
// the view array references a small fraction of the original buffers.
```

### Schema declaration for StringViewArray

```rust
use arrow_schema::{Field, DataType};

// StringArray uses DataType::Utf8
let f1 = Field::new("msg", DataType::Utf8, true);

// StringViewArray uses DataType::Utf8View
let f2 = Field::new("msg", DataType::Utf8View, true);

// These are DIFFERENT types. RecordBatch::try_new will reject a StringViewArray
// column if the schema field says Utf8 (and vice versa).
```

---

## 3. IPC FileWriter vs StreamWriter

### What an agent needs to know

Both write Arrow IPC format. The key differences:

| | FileWriter | StreamWriter |
|---|---|---|
| Writer trait bound | `Write` | `Write` |
| Reader trait bound | `Read + Seek` | `Read` (no Seek) |
| Footer | Yes -- contains block offsets for random access | No -- just EOS marker |
| Random access | Yes, via `FileReader::set_index(n)` | No -- must read sequentially |
| Use case | Disk-backed queues (random access to batches) | Streaming over network/pipe |

**For a disk-backed queue**, use `FileWriter` with atomic seal (write to `.tmp` → `fsync`
→ `rename`). The footer stores block offsets, so you can seek to batch N directly without
scanning. Readers never see incomplete files — either the file exists (complete with footer)
or it doesn't. Delete orphaned `.tmp` files on startup.

### Cargo features for compression

```toml
# In Cargo.toml -- using the umbrella crate:
arrow = { version = "54", features = ["ipc_compression"] }

# Or using arrow-ipc directly:
arrow-ipc = { version = "54", features = ["zstd"] }
# Also available: "lz4" (uses lz4_flex)
```

Without the feature flag, calling `try_with_compression` will succeed at construction
but **panic at runtime** when you write the first batch. The `zstd` feature depends on
the `zstd` crate v0.13.

### FileWriter with zstd compression

```rust
use std::fs::File;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_ipc::CompressionType;
use arrow_schema::Schema;

let schema = Schema::new(vec![/* fields */]);
let file = File::create("data.arrow").unwrap();

let options = IpcWriteOptions::default()
    .try_with_compression(Some(CompressionType::ZSTD))
    .unwrap();

let mut writer = FileWriter::try_new_with_options(
    file,
    &schema,
    options,
).unwrap();

// try_new_buffered wraps in BufWriter automatically -- prefer this:
// let mut writer = FileWriter::try_new_buffered(file, &schema).unwrap();
// (but try_new_buffered doesn't take options, so for compression use try_new_with_options)

writer.write(&batch).unwrap();
writer.write(&batch2).unwrap();

// MUST call finish() -- writes the footer with block offsets
writer.finish().unwrap();
// Writing after finish() returns Err, does not panic
```

### StreamWriter with zstd compression

```rust
use arrow_ipc::writer::{StreamWriter, IpcWriteOptions};
use arrow_ipc::CompressionType;

let options = IpcWriteOptions::default()
    .try_with_compression(Some(CompressionType::ZSTD))
    .unwrap();

let buffer: Vec<u8> = Vec::new();
let mut writer = StreamWriter::try_new_with_options(buffer, &schema, options).unwrap();

writer.write(&batch).unwrap();
writer.finish().unwrap();

// into_inner() calls finish() if not already called, then returns the writer
let bytes: Vec<u8> = writer.into_inner().unwrap();
```

### Reading back -- FileReader (random access)

```rust
use std::fs::File;
use arrow_ipc::reader::FileReader;

let file = File::open("data.arrow").unwrap();
let reader = FileReader::try_new_buffered(file, None).unwrap(); // None = no projection

let schema = reader.schema(); // Arc<Schema>
let num_batches = reader.num_batches();

// Sequential iteration
for batch_result in reader {
    let batch = batch_result.unwrap();
}

// Random access
let mut reader = FileReader::try_new_buffered(
    File::open("data.arrow").unwrap(), None
).unwrap();
reader.set_index(5).unwrap(); // seek to batch 5
let batch = reader.next().unwrap().unwrap();
```

### Reading back -- StreamReader

```rust
use arrow_ipc::reader::StreamReader;

let cursor = std::io::Cursor::new(bytes);
let reader = StreamReader::try_new(cursor, None).unwrap();

for batch_result in reader {
    let batch = batch_result.unwrap();
}
```

### Compression gotcha

The IPC compression format is: `[8 bytes uncompressed_len][compressed data]`.
If compressed data would be larger than uncompressed, Arrow writes
`[-1 as i64][uncompressed data]` instead. This is handled transparently, but means
file sizes with compression enabled can occasionally be *larger* than without (by 8
bytes per buffer) for already-compressed or random data.

---

## 4. Schema Evolution

### What an agent needs to know

Arrow RecordBatch is strict about schema matching. There is no implicit coercion.

### concat_batches requires identical schemas

```rust
use arrow_select::concat::concat_batches;

// concat_batches takes a target schema and ignores the schemas of input batches.
// BUT it will error if the underlying array types don't match.
let merged = concat_batches(&target_schema, &[&batch1, &batch2]).unwrap();
// Error if batch1.column(i).data_type() != target_schema.field(i).data_type()
```

**Critical**: `concat_batches` indexes columns by position, not by name. If batch1 has
columns `[a, b]` and batch2 has columns `[b, a]` (swapped), you'll get garbage data
without any error (types might happen to match).

### Schema::try_merge for combining schemas

```rust
use arrow_schema::Schema;

let s1 = Schema::new(vec![
    Field::new("a", DataType::Int64, true),
]);
let s2 = Schema::new(vec![
    Field::new("a", DataType::Int64, false),  // nullable differs
    Field::new("b", DataType::Utf8, true),    // new field
]);

let merged = Schema::try_merge(vec![s1, s2]).unwrap();
// Result: a: Int64 (nullable=true, widened), b: Utf8 (nullable=true)
// Merge widens nullability: if either is nullable, result is nullable.
// Errors if same field name has different data types.
```

### Schema::contains for superset checking

```rust
// schema.contains(other) returns true if every field in `other` exists in `schema`
// with a compatible type. Metadata must also be a superset.
let is_superset = new_schema.contains(&old_schema);
```

### RecordBatch::with_schema for re-labeling

```rust
// Replaces the schema without touching columns. The new schema must be a
// superset of the current schema (checked via Schema::contains).
let relabeled = batch.with_schema(new_schema).unwrap();
```

### Handling schema mismatches in practice

When batches arrive with evolving schemas (e.g., new fields appear):

1. Merge schemas with `Schema::try_merge`
2. For each batch missing a column, create a null array of the right type and length
3. Reorder columns to match the merged schema
4. Then `concat_batches` works

```rust
use arrow_array::new_empty_array;

fn pad_batch_to_schema(batch: &RecordBatch, target: &SchemaRef) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        match batch.column_by_name(field.name()) {
            Some(col) => columns.push(col.clone()),
            None => {
                // Create a null-filled array of the right type and length
                let null_arr = arrow_array::new_null_array(field.data_type(), batch.num_rows());
                columns.push(null_arr);
            }
        }
    }
    RecordBatch::try_new(target.clone(), columns).unwrap()
}
```

---

## 5. Common Gotchas

### Array length mismatch -- Err, not panic

```rust
let a = Int32Array::from(vec![1, 2, 3]);
let b = Int64Array::from(vec![1, 2]);  // length 2, not 3
let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]);
// Err: "all columns in a record batch must have the same length"
```

This is a runtime error, not compile-time. Easy to hit when building columns independently.

### Non-nullable field with nulls -- Err, not panic

```rust
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int32, false), // nullable = false
]));
let arr = Int32Array::from(vec![Some(1), None, Some(3)]); // has a null!
let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]);
// Err: "Column 'id' is declared as non-nullable but contains null values"
```

### Downcast failures

```rust
use arrow_array::cast::AsArray;

let arr: ArrayRef = Arc::new(StringArray::from(vec!["a"]));

// PANICS: wrong type
// let ints: &Int32Array = arr.as_primitive::<Int32Type>();

// Safe: returns None
let maybe: Option<&Int32Array> = arr.as_primitive_opt::<Int32Type>();
assert!(maybe.is_none());

// Correct downcast
let strings: &StringArray = arr.as_string::<i32>();
```

The `as_*()` methods on the `AsArray` trait panic on type mismatch. Always use
`as_*_opt()` if you're not certain of the type.

### Utf8 vs Utf8View type mismatch

```rust
let schema = Arc::new(Schema::new(vec![
    Field::new("msg", DataType::Utf8, true),      // expects StringArray
]));
let arr: ArrayRef = Arc::new(StringViewArray::from(vec!["hello"])); // Utf8View
let batch = RecordBatch::try_new(schema, vec![arr]);
// Err: "column types must match schema types, expected Utf8 but found Utf8View"
```

If your schema says `Utf8` and your array is `StringViewArray`, it will fail. These
are distinct types. Be consistent.

### FileWriter: forgetting finish()

If you drop a `FileWriter` without calling `finish()`, the footer is never written.
The file will be unreadable by `FileReader` (it looks for the magic bytes + footer at
the end). There is no `Drop` impl that auto-finishes. `StreamWriter::into_inner()`
auto-calls `finish()`, but `FileWriter` has no `into_inner()`.

### Writing after finish()

Both `FileWriter::write()` and `StreamWriter::write()` return `Err` if called after
`finish()`. They do not panic, but the error message is easy to miss if you `unwrap()`
elsewhere.

### concat_batches column order

As noted in section 4: `concat_batches` matches columns by position index, not by name.
If two batches have the same schema fields in different orders, concatenation produces
silently wrong data (or an error if types differ at the same index).

---

## 6. Performance Patterns

### Reuse builders across batches

`finish()` resets the builder's internal state but retains allocated capacity.
Call `finish()` per batch, then keep appending to the same builder:

```rust
let mut builder = StringBuilder::with_capacity(expected_strings, expected_total_bytes);

loop {
    for line in next_chunk() {
        builder.append_value(line);
    }
    let arr = builder.finish(); // resets length to 0, keeps capacity
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    send(batch);
    // builder is ready for reuse -- no reallocation
}
```

`StringBuilder::with_capacity(items, data_bytes)` pre-allocates both the offsets buffer
and the values buffer. This avoids reallocation during the first batch.

For `StringViewBuilder`, use `with_capacity(items)` -- data buffer grows via the block
size strategy (8KB -> 16KB -> ... -> 2MB exponential growth, or fixed via
`with_fixed_block_size`).

### Avoid cloning Arc<Schema> unnecessarily

```rust
// BAD: clones the Arc every call
for batch in batches {
    let s = batch.schema(); // clones Arc
}

// GOOD: borrow the Arc reference
for batch in batches {
    let s: &SchemaRef = batch.schema_ref(); // no clone
}
```

`schema()` returns `self.schema.clone()` (Arc clone = atomic increment, cheap but not
free). In tight loops, use `schema_ref()`.

### Batch size sweet spots

- Arrow IPC overhead is per-batch (message header, alignment padding). Very small batches
  (< 100 rows) have high relative overhead.
- Very large batches (> 1M rows) can cause memory pressure and make streaming less
  granular.
- Sweet spot for throughput: **8K-64K rows per batch**, depending on column width.
- For IPC with zstd compression, larger batches compress better (more data per compression
  frame). Aim for batches that produce ~256KB-1MB of compressed data.

### StringViewBuilder block size for log lines

For log processing workloads (variable length log lines, many > 12 bytes):

```rust
let mut builder = StringViewBuilder::new()
    .with_fixed_block_size(256 * 1024); // 256KB blocks
```

The default exponential strategy (8KB -> 2MB) works well for most cases. Fixed block size
is useful when you want predictable memory behavior, e.g., in a streaming pipeline with
memory limits.

### Zero-copy IPC reading with mmap

`FileReader` can read from a memory-mapped file. Since Arrow IPC is designed for zero-copy,
the buffers in the returned `RecordBatch` can point directly into the mmap region:

```rust
// See arrow/examples/zero_copy_ipc.rs in the arrow-rs repo
// Uses FileReader with a Cursor<&[u8]> backed by mmap
```

This is relevant for disk-backed queues: if you mmap the IPC file, reads are zero-copy
(the OS manages paging). The `FileReader` requires `Read + Seek`, which `Cursor<&[u8]>`
provides over an mmap slice.

### Avoid re-encoding dictionaries

`DictionaryTracker` in the IPC writer tracks which dictionaries have been sent. For
`FileWriter`, `error_on_replacement = true` by default -- if a dictionary changes between
batches, it errors. For `StreamWriter`, `error_on_replacement = false` -- it silently
replaces. If your data has stable dictionaries (e.g., log levels), the writer only
encodes the dictionary once.
