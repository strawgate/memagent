# Kernel boundary design patterns in Rust

> **Status:** Historical
> **Date:** 2026-03
> **Context:** Research on trait-based visitor and type-based IR patterns for kernel boundary design.

**The trait-based visitor pattern and the type-based intermediate representation are not competing choices — the best Rust systems offer both.** Serde proves this definitively: its primary path uses trait method calls that compile to zero-overhead monomorphized code with no intermediate allocation, while `serde_json::Value` provides a concrete enum for dynamic cases. For a log parser like logfwd processing 1M+ lines/sec, the practical answer is to define the kernel boundary as a visitor trait for the hot path, backed by concrete kernel types for testing, debugging, and flexible consumers. Real-world evidence from rustls, wasmtime, simd-json, and arrow-rs converges on the same architecture: a thin boundary-defining crate or module that owns the shared vocabulary (newtypes, error enums, traits), with everything else hidden behind `pub(crate)` and private dependencies.

## The visitor pattern eliminates allocations entirely — serde proves it

Serde's architecture is the single most instructive case study for logfwd's boundary design. Serde defines a fixed set of **29 kernel types** (bool, i8–i128, u8–u128, f32, f64, char, string, bytes, option, unit, seq, map, struct, and their variants) that constitute its data model. The critical insight, noted by Josh McGuigan's source analysis, is that **this data model never materializes as an enum or struct**. It exists purely as trait method signatures — `visit_i64`, `visit_str`, `visit_map` — and data flows directly from format bytes through the Deserializer into the Visitor with no intermediate allocation.

For compound types like maps, serde avoids buffering the entire structure by providing accessor traits (`MapAccess`, `SeqAccess`) that stream elements lazily:

```rust
fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
    let mut map = MyMap::with_capacity(access.size_hint().unwrap_or(0));
    while let Some((key, value)) = access.next_entry()? {
        map.insert(key, value);  // Elements stream directly into target
    }
    Ok(map)
}
```

This maps directly to logfwd's design challenge. Instead of the kernel returning `Vec<ParsedField>` and the outer crate iterating it, the kernel could call trait methods on a builder:

```rust
// Kernel defines this trait (the "port")
pub trait LineVisitor {
    fn visit_str_field(&mut self, key: &[u8], value: &[u8]);
    fn visit_int_field(&mut self, key: &[u8], value: i64);
    fn visit_float_field(&mut self, key: &[u8], value: f64);
    fn visit_null_field(&mut self, key: &[u8]);
    fn visit_nested_field(&mut self, key: &[u8], raw: &[u8]);
    fn finish_line(&mut self);
}

// Outer crate implements it with Arrow builders
struct ArrowLineVisitor { /* StringBuilder, Int64Builder, etc. */ }
impl LineVisitor for ArrowLineVisitor { /* append to Arrow builders */ }
```

**Serde achieves zero-cost through three mechanisms**: monomorphization (all traits use generics, not `dyn`), inlining across trait boundaries with optimizations, and zero-sized Visitors (unit structs that exist only at the type level). The Rust compiler sees through the entire Serialize→Serializer→Formatter chain and can reduce it to code equivalent to handwritten serialization.

Serde's zero-copy mechanism also maps perfectly to logfwd. The `Deserializer<'de>` lifetime parameter lets borrowed slices flow through the boundary. The Visitor trait provides three string methods — `visit_str(&str)` (transient), `visit_borrowed_str(&'de str)` (zero-copy from input), and `visit_string(String)` (owned). **logfwd's kernel already uses `&'a [u8]` slices — these can flow directly through a visitor trait boundary with no copy**, as long as the visitor's lifetime is tied to the input buffer.

## Real projects overwhelmingly choose traits for hot paths, types for everything else

The provability concern — that trait boundaries introduce unknown callees — is real but manageable. Evidence from security-critical and performance-critical Rust projects reveals a consistent pattern: **traits at the boundary, but with deliberate constraints on who can implement them**.

Ring, the cryptography library, uses **concrete types at its public boundary** (e.g., `SealingKey<N>`, `Digest`, `EphemeralPrivateKey`) and reserves traits only for behavioral variation (how nonces advance, how signatures are verified). Crucially, ring's traits use a **sealed pattern** that prevents external implementations:

```rust
// ring's approach: sealed traits prevent arbitrary implementations
mod private { pub trait Sealed {} }
pub trait Padding: private::Sealed { /* ... */ }
```

This gives ring the expressiveness of traits while keeping the set of valid implementations fixed and auditable. For logfwd's kernel, **sealing the `LineVisitor` trait is unnecessary** — the kernel calls *out* through it, so the implementations live in the outer crate. But for any trait the kernel exposes for extension, sealing preserves provability.

Wasmtime provides a different data point. Its boundary between Cranelift (compiler backend) and the runtime is mediated by the `Compiler` trait defined in `wasmtime-environ`:

```rust
pub trait Compiler: Send + Sync {
    fn compile_function(
        &self, translation: &ModuleTranslation<'_>,
        index: DefinedFuncIndex, data: FunctionBodyData<'_>,
        types: &ModuleTypesBuilder, symbol: &str,
    ) -> Result<CompiledFunctionBody, CompileError>;
}
```

The compiled output is returned as `Box<dyn Any + Send>` — **complete type erasure**. The runtime never sees Cranelift-specific types. Only the compiler implementation knows how to unpack them. This existential typing pattern is extreme but effective for hard isolation.

For logfwd specifically, the trait-based boundary (kernel calls `visitor.visit_str_field(...)`) is better for provability than it might seem. The kernel's logic is fully deterministic: given the same input, it makes the same sequence of visitor calls. **The trait boundary does not introduce unknown control flow into the kernel — it introduces unknown side effects in the callee**, which is the outer crate's problem. A formal verification tool analyzing the kernel can model the visitor as an opaque sink and still prove parsing correctness.

The type-based approach (`Vec<ParsedField>`) remains valuable as a **secondary API** for consumers who don't want to implement a visitor. Serde does exactly this: the trait path is primary, `serde_json::Value` is the fallback. For logfwd, provide both:

```rust
// Primary: visitor (zero-alloc hot path)
pub fn parse_line<V: LineVisitor>(input: &[u8], visitor: &mut V) -> Result<(), ParseError>;

// Secondary: returns intermediate types (convenient, testable)
pub fn parse_line_to_fields<'a>(input: &'a [u8]) -> Result<ParsedLine<'a>, ParseError>;
```

## Performance at the boundary costs less than you think — with the right patterns

The allocation cost of `Vec::with_capacity(16)` for a 32-byte `ParsedField` type is approximately **50–100 ns** (25–50 ns allocation + 25–50 ns deallocation with jemalloc). At 1M lines/sec (1 µs per line budget), this consumes **5–10% of the budget per Vec** — significant but not catastrophic.

However, the dominant pattern in high-throughput Rust parsers is **buffer reuse**, which eliminates this cost entirely:

```rust
let mut fields = Vec::with_capacity(16);
for line in input_lines {
    fields.clear();  // Resets length, keeps capacity — zero allocation
    parse_line_into(line, &mut fields);
    convert_to_arrow(&fields);
}
```

simd-json uses this pattern via its `Buffers` struct, which persists allocations across parse calls. The csv crate's `csv-core` sub-crate goes further: it's a **zero-allocation DFA** that occupies only a few hundred stack bytes, with the outer `csv` crate handling all buffer management. **sonic-rs outperforms simd-json specifically because it skips the intermediate tape representation**, parsing directly into Rust structs via serde's visitor — confirming that the visitor path is genuinely faster.

Regarding `SmallVec<[ParsedField; 16]>`: benchmarks from the "Improving SmallVec" analysis show that **Vec is often faster than SmallVec** due to the per-operation inline-vs-heap check overhead. `extend` is 2.9× faster on Vec; `from_slice` is 5.5× faster. SmallVec's advantage is cache locality when many SmallVecs are stored contiguously (e.g., `Vec<SmallVec<[T; 4]>>`). For logfwd's case — a single `ParsedLine` per iteration — **`ArrayVec<ParsedField, 16>` is the better stack-only option** when field count is bounded, or Vec with buffer reuse when it's not.

Arena allocation via bumpalo offers a third path: **~2–5 ns per allocation** (just a pointer bump) versus ~50–100 ns for malloc/free. The Oxc JavaScript compiler saw a **20% performance improvement** switching AST allocation to bumpalo. For logfwd, arena allocation could make the type-based `ParsedLine` path nearly as fast as the visitor path, at the cost of lifetime complexity (`'arena` parameters on all arena-allocated types).

## Error handling should follow ring's opacity, not rustls's richness

Ring and rustls represent two poles of error handling at security boundaries, and **for a provable kernel, ring's approach is clearly better**.

Ring's error type is deliberately opaque:

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Unspecified;  // Zero-sized, zero-allocation
```

Ring's documentation explains: "Experience with using and implementing other crypto libraries has shown that sophisticated error reporting facilities often cause significant bugs themselves." The more specific `KeyRejected(&'static str)` uses only static string constants and `pub(crate)` constructors — external code cannot create arbitrary error values.

For logfwd's kernel, this translates to a small, exhaustive error enum:

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ParseError {
    UnexpectedEnd,
    InvalidUtf8,
    MalformedKeyValue,
    NestingTooDeep,
    LineTooLong,
}
```

No strings, no allocations, no `Box<dyn Error>`. The outer crate converts these to richer errors using `From`:

```rust
// Outer crate
#[derive(thiserror::Error, Debug)]
pub enum LogfwdError {
    #[error("parse error at byte {offset}: {kind:?}")]
    Parse { kind: kernel::ParseError, offset: usize, context: String },
    #[error("arrow conversion failed: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}
```

Rustls evolved *away* from string-based errors: its `PeerMisbehaved(String)` variant from v0.19 became `PeerMisbehaved(PeerMisbehaved)` with ~75 structured enum variants. **This evolution confirms that starting with simple enums and adding structure later is the right direction**, not the reverse. The kernel should use `Result<T, ParseError>` — not error flags, not option types. `Result` is Rust's standard error protocol and integrates with `?` for clean control flow.

## Keeping the boundary narrow requires a shared vocabulary crate

The "skinny boundary" problem — config, data, errors, and metrics all needing to cross — has a clear solution pattern used by wasmtime, tantivy, and rustls: **define a thin boundary crate that owns all shared types**.

Wasmtime's `wasmtime-environ` crate defines the vocabulary (newtype indices like `FuncIndex`, the `Compiler` trait, `CompileError`, `ModuleTranslation`) shared between Cranelift and the runtime. Neither side depends on the other. Tantivy does the same with `tantivy-tokenizer-api` — external tokenizer implementations depend only on this thin crate, not on all of tantivy.

For logfwd, the kernel crate *is* the boundary-defining crate. Its public surface should include only:

- **Types**: `ParsedField<'a>`, `TypedValue<'a>`, `ParsedLine<'a>` (if using type-based path)
- **Traits**: `LineVisitor` (if using visitor path)
- **Errors**: `ParseError` enum
- **Entry point**: `parse_line()` or `Parser::new()` with minimal config
- **Config**: A single `ParserConfig` struct with builder pattern

Config should funnel through a single entry point (wasmtime's `Config → Engine` pattern, tantivy's `Schema → Index` pattern). Metrics should use the `tracing` crate's global subscriber — **not** cross the boundary as types. The kernel emits `tracing::span!` and `tracing::event!` calls; the outer crate installs the subscriber. This keeps metrics entirely out of the boundary API.

Wasmtime demonstrates another powerful narrowing technique: **opaque handles**. Store-connected types like `Func` and `Memory` are thin indices, not full objects. Their data lives inside the `Store`, accessed only through `&Store` or `&mut Store` references. For logfwd, if the parser needs to maintain state across lines (e.g., schema inference), expose a `ParserHandle` that's an opaque index into internal state, not the state itself.

## Testing boundaries with proptest and roundtrip properties

Arrow-rs, despite its extensive type conversion surface, **does not use property-based testing** — it relies on exhaustive example-based tests. This is a missed opportunity. Proptest is well-suited to boundary testing, with one important limitation: **`Arbitrary` only works for owned data**, not borrowed data like `&[u8]`.

The workaround for logfwd's borrowed kernel types is to generate owned data and borrow in the test:

```rust
use proptest::prelude::*;

fn typed_value_strategy() -> impl Strategy<Value = OwnedTypedValue> {
    prop_oneof![
        any::<Vec<u8>>().prop_map(OwnedTypedValue::Str),
        any::<i64>().prop_map(OwnedTypedValue::Int),
        any::<f64>().prop_map(OwnedTypedValue::Float),
        Just(OwnedTypedValue::Null),
    ]
}

proptest! {
    #[test]
    fn arrow_conversion_preserves_values(
        fields in prop::collection::vec(
            (any::<Vec<u8>>(), typed_value_strategy()), 1..16
        )
    ) {
        // Convert owned → borrowed kernel types
        let parsed: Vec<ParsedField> = fields.iter()
            .map(|(k, v)| ParsedField { key: k.as_slice(), value: v.as_ref() })
            .collect();
        // Convert to Arrow
        let batch = to_record_batch(&parsed);
        // Convert back and verify
        let roundtripped = from_record_batch(&batch);
        prop_assert_eq!(parsed.len(), roundtripped.len());
        for (orig, rt) in parsed.iter().zip(roundtripped.iter()) {
            prop_assert_eq!(orig.value, rt.value);
        }
    }
}
```

The **roundtrip property** (`kernel → Arrow → kernel = identity`) is the most powerful single test for boundary correctness. Supplement it with **preservation properties**: null handling (kernel `Null` → Arrow null bitmap → kernel `Null`), type fidelity (kernel `Int(42)` → Arrow Int64 column → kernel `Int(42)`), and edge cases (empty strings, `f64::NAN`, maximum-length fields).

Arrow-rs's builder pattern gives the outer crate a natural structure for conversion code. Each kernel `TypedValue` variant maps to a specific Arrow builder:

```rust
match field.value {
    TypedValue::Str(bytes) => str_builder.append_value(std::str::from_utf8(bytes)?),
    TypedValue::Int(n) => int_builder.append_value(n),
    TypedValue::Float(f) => float_builder.append_value(f),
    TypedValue::Null => { str_builder.append_null(); int_builder.append_null(); /* ... */ }
    TypedValue::Nested(raw) => nested_builder.append_value(raw),
}
```

## Evolving the kernel API over a decade without breaking proofs

Rustls's transition from ring to aws-lc-rs provides the blueprint for long-term API evolution. The three-step pattern: **introduce abstraction (0.22), switch default (0.23), extract to separate crates (0.24)** — took roughly a year and kept the public API stable throughout.

For logfwd's kernel, the key stability mechanisms are:

**`#[non_exhaustive]` on all public enums**, including `ParseError` and `TypedValue`. This allows adding variants (e.g., `TypedValue::Timestamp(i64)`) without a semver-major bump. The cost is that consumers must always include a wildcard match arm. For a kernel intended to last a decade, this tradeoff is clearly worthwhile. Rustls marks every public enum `#[non_exhaustive]`.

**Sealed traits for kernel-internal extension points**. If the kernel defines traits that only it should implement, seal them. Ring's approach — `fn method(&self, _: private::Token)` for partial sealing — is elegant:

```rust
mod private { pub struct Token; }
pub trait KernelInternal {
    fn parse_field(&self, input: &[u8], _: private::Token) -> ParsedField;
}
```

**Extension traits for new functionality**. When the kernel needs new convenience methods, add them via `ParsedLineExt` rather than modifying `ParsedLine`. The blanket `impl<T: ParsedLine> ParsedLineExt for T` makes them available automatically.

**The newtype wrapper pattern** hides internal representation. Instead of exposing `ParsedLine<'a> { fields: SmallVec<[ParsedField<'a>; 16]> }`, expose `ParsedLine<'a>(/* private */)` with accessor methods. This lets you change from SmallVec to ArrayVec to Vec internally without breaking the API.

When Arrow makes breaking changes (new major version), **only the outer crate changes — the kernel is untouched** because it has no Arrow dependency. This is the fundamental architectural win of the kernel/shell split. The outer crate can even support multiple Arrow versions simultaneously via feature flags, or use the semver trick (release a compatibility shim from the old version that re-exports types from the new version).

## Conclusion

The evidence from production Rust systems points to a specific architecture for logfwd: **a visitor trait as the primary boundary, concrete kernel types as the secondary boundary, and a thin shared vocabulary that both sides understand**. The visitor path (serde's model) eliminates per-line allocation and enables zero-copy flow of `&[u8]` slices from input through parsing through Arrow building. The type path (serde_json::Value model) provides testability, debuggability, and flexibility for consumers who don't want to implement a trait.

The less obvious finding is about provability. The trait-based visitor does not harm kernel verification — it helps it. The kernel's contract becomes: "given input X, I make visitor calls Y in order Z." This is a purely functional specification, testable with a recording visitor, provable without knowing what the visitor does internally. The type-based approach has the same provability profile (given X, produce Y), but **the visitor approach produces a more naturally verifiable specification because each call is atomic and ordered**, while a returned Vec must be verified as a whole.

Three concrete patterns deserve emphasis. First, ring's error opacity (`struct Unspecified;`) demonstrates that **less error information is more correct** in a provable kernel — every detail you add is a detail you must prove. Second, wasmtime's `Box<dyn Any + Send>` for compiled output shows that **type erasure at boundaries is a feature, not a bug** — it enforces that the consumer cannot depend on representation details. Third, the buffer-reuse pattern (`Vec::clear()` between iterations) means that even the "wasteful" type-based approach costs essentially nothing in steady state — the allocation happens once, not per line.