# Code Style

Style preferences enforced during code review. These are NOT linting
rules — they are subjective conventions that keep the codebase
consistent. CodeRabbit and human reviewers enforce these.

## Mindset

The ethos this codebase writes toward — applied at the level of every
function, not every PR:

- **The type system is the primary design tool.** Encode state, ownership,
  and invariants so misuse fails to compile. Prefer a compile error over
  a runtime check, a runtime check over a comment, a comment over
  implicit knowledge.
- **Parse, don't validate.** A validator returns `bool` and leaves knowledge
  in the programmer's head. A parser returns a typed value and puts that
  knowledge in the type system. Parse untrusted input at the boundary
  (config load, receiver, CLI) into domain types; internal functions
  only see already-proven-valid inputs. `Result<Email, ParseError>`, not
  `fn is_valid_email(&str) -> bool`.
- **Make invalid states unrepresentable.** Enums for closed sets of
  states, newtypes for validated primitives, typestate for state
  machines, smart constructors with private fields. `bool` flags
  standing in for state transitions are a regression.
- **The borrow checker signals design.** If ownership feels messy, the
  architecture is wrong — refactor, don't reach for `.clone()`, `Rc`,
  or `Arc`.
- **Measure, don't guess.** No perf claim without a `criterion`
  baseline. No optimization without a profile. See
  `DEVELOPING.md` → *Performance change workflow*.
- **Compile time is a quality attribute.** Gratuitous generics, deep
  macro expansion, and heavy dependencies that balloon build times
  degrade iteration speed — treat that as a real defect, not an
  aesthetic one. See `ARCHITECTURE.md` → *Compile time as a quality
  attribute*.

Meta-test: *would a maintainer of `serde`, `tokio`, or the Rust standard
library merge this?*

## Workspace Lint Policy

Workspace-level lints are in the root `Cargo.toml` under `[workspace.lints]`.
All crates inherit via `[lints] workspace = true`. Do not add per-crate lint
overrides — adjust the workspace config instead.

- **clippy::pedantic** is warn-level with selective allows for noisy lints.
- **No `.unwrap()` in production paths.** Use `?`, `.expect("reason")`, or
  `unwrap_or`. CI runs `clippy -- -D warnings`, so any clippy warning is
  a build failure.
- **unsafe_code** is `forbid` in logfwd-core. Other crates allow it sparingly.
  Every `unsafe` block must have a `// SAFETY:` comment.
  `clippy::undocumented_unsafe_blocks` is `deny` — the SAFETY comment is
  enforced, not encouraged.
- **`dbg!` is forbidden.** `clippy::dbg_macro = deny` workspace-wide.
- **`large_enum_variant`** is warn workspace-wide. New enum variants
  whose stack size dwarfs the others should be boxed or refactored.
- **`print_stdout` / `print_stderr`** are warn workspace-wide.
  Observability must go through `tracing`, not stdout/stderr. Crates
  that legitimately print (the `logfwd` binary, benchmark harnesses,
  the standalone eBPF sensor, the runtime CLI-bootstrap module) opt
  out with a file-level `#![allow(clippy::print_stdout, clippy::print_stderr)]`
  at the crate root plus a one-line comment explaining why.
- **`missing_docs`** is warn at the crate root for stable-surface
  crates (`logfwd-core`, `logfwd-types`). `logfwd-config` has ~280
  pre-existing schema-field gaps and is not yet gated on this; new
  public items must still carry doc comments by review.
- **`clippy::await_holding_lock` and `clippy::await_holding_refcell_ref`**
  are warn workspace-wide. A `MutexGuard` (or `RefCell` borrow) held
  across `.await` leaks when the future is dropped. The dylint lint
  `cancel_safe_no_lock_across_await` provides a scoped, author-asserted
  version for functions marked `#[cancel_safe]`.
- **`#[must_use]` discipline.** `clippy::let_underscore_future = deny`
  (silently dropping a future is always a bug).
  `clippy::return_self_not_must_use = warn` (builder-chain methods
  that return `Self` without `#[must_use]` are almost always wrong —
  the caller built a value and threw it away). Mark the following
  with `#[must_use]` explicitly:
  - Every builder method that returns `Self`.
  - Constructors that return a guard or permit (`_Handle`, `_Permit`,
    `_Guard`).
  - `Result`-returning functions where ignoring `Ok(())` means the
    operation did not actually happen — `flush`, `shutdown`,
    `commit_checkpoint`, and similar. `Result` already carries
    `#[must_use]` at the type level, but an explicit annotation on
    the function makes the intent clear.
  We deliberately do **not** enable `clippy::let_underscore_must_use`
  or `clippy::must_use_candidate` — both flag the canonical
  `let _ = write!(buf, ...)` and `let _ = sender.send(...)` patterns,
  and would force stylistic churn with negligible correctness gain.
- **overflow-checks** are enabled in release builds.

All lint levels live at the workspace root or as file-level
`#![allow]` opt-outs. Per-crate `[lints.clippy]` tables in individual
`Cargo.toml` files are not permitted — they fragment the lint story.

### Boundary guards (CI scripts)

Beyond clippy lints, the following Python guards run as part of
`just lint` and `just lint-all`:

| Guard | What it enforces |
|---|---|
| `scripts/check_no_box_dyn_error.py` | No `Box<dyn Error>` in any public signature outside the binary crate (`logfwd`) and bench/test/example paths. Library crates must expose `thiserror` enums. |
| `scripts/check_no_panic_in_production.py` | No `panic!`/`todo!`/`unimplemented!` in production paths of `logfwd-runtime` and `logfwd-output`. Test modules and `#[test]` functions are exempt; genuinely unreachable invariants can use `// ALLOW-PANIC: <reason>`. |
| `scripts/check_no_raw_payload_injection.py` | No source-metadata injection into raw payload bytes (see `CRATE_RULES.md` → `logfwd-io`). |

### Semantic lints (dylint)

For invariants that require type resolution — catching `.clone()` on a
heap type, detecting allocations inside a `#[hot_path]` function,
verifying `.await` doesn't happen while holding a `Mutex` guard —
clippy alone is insufficient. These live in `crates/logfwd-lints/` as
a dylint library and are invoked via `just dylint`.

| Lint | Attribute | What it flags |
|---|---|---|
| `hot_path_no_alloc` | `#[hot_path]` | Heap allocation inside the tagged function — `Box::new`, `Vec::new`/`with_capacity`, `String::from`/`new`, `.to_string()`, `.to_vec()`, `.to_owned()`, `.clone()` on heap types, `.collect()` into owned containers, `Arc::new`, `Rc::new`, `HashMap`/`HashSet` allocation. |
| `cancel_safe_no_lock_across_await` | `#[cancel_safe]` | Lock guard held across a `.await` point inside the tagged async function — `std::sync`/`parking_lot`/`tokio::sync` mutex & rwlock guards, `RefCell::borrow{,_mut}`. A dropped future (e.g., a losing `tokio::select!` branch) leaks the guard until the surrounding scope unwinds. |
| `no_panic_in_body` | `#[no_panic]` | Direct panic sources inside the tagged function — `panic!`/`todo!`/`unimplemented!`/`unreachable!`/`assert*!` macros, `.unwrap()`/`.expect()`, slice/array indexing. Shallow check — does **not** trace calls. |

The lints are complementary to the workspace-wide clippy restrictions
`clippy::await_holding_lock` and `clippy::await_holding_refcell_ref`
(both `warn`), which fire on every async function. `#[cancel_safe]`
adds intent-declaration and reserves space for stricter future
enforcement (e.g., cancel-unsafe-future reachability).

The dylint library lives in its own isolated workspace
(`crates/logfwd-lints/` is excluded from the main workspace because it
pins a specific rustc nightly and links against `rustc-private` crates).
The attribute crate (`logfwd-lint-attrs/`) IS in the main workspace —
it's a regular proc-macro crate. See `crates/logfwd-lints/README.md`
for installation and the roadmap for additional lints
(`#[checkpoint_ordered]`, `#[owned_by_actor]`, MIR-based `cancel_safe`).

## Ownership and Types

- **Prefer `&str` / `&[u8]` over owned `String` / `Vec<u8>`** in function
  parameters when the callee doesn't need ownership.
- **Use `Cow<str>` correctly.** `.into_owned()` only allocates when borrowed;
  `.to_string()` always allocates.
- **Don't clone when you can borrow.** Especially avoid cloning in loops —
  clone once outside the loop if needed.
- **`#[non_exhaustive]`** on all public enums so new variants aren't breaking.
- **Private fields by default.** Use `pub(crate)` for internal access, expose
  via methods when external access is needed.
- **Enums over strings for config values.** Parse into typed enums at
  deserialization, not at use sites.
- **Parse at the boundary, operate on typed values inside.** Every untrusted
  input — config YAML, CLI args, inbound HTTP/OTLP/TCP bytes, file contents —
  is parsed into a domain type at the crate boundary. Internal functions
  accept the already-parsed type. The parser is the only fallible path;
  internal consumers never re-check invariants. If you find yourself writing
  `fn validate_x(&T) -> Result<(), Error>` that the caller is trusted to
  run first, rewrite it as `fn parse_x(raw) -> Result<X, Error>` and make
  `X`'s constructor the only way to get one.
- **Newtype domain primitives that get confused.** `SourceId(u64)`,
  `BatchSize(u32)`, `SourcePath(Arc<str>)`. Bare `u64`/`usize` is fine
  for indices and counts with obvious meaning at the call site; it is
  not fine for identifiers, sizes-with-units, or anything that could be
  accidentally passed where the other belongs.
- **Typestate for state machines.** New state machines encode transitions
  in types (`Pipeline<Building>` → `Pipeline<Running>` → `Pipeline<Draining>`),
  not `bool` flags or string-valued `state` fields. Invalid transitions
  should fail to compile, not at runtime. See `logfwd-types/src/pipeline/`
  for the existing pattern.
- **Smart constructors with private fields.** If a type has an invariant
  (non-empty, sorted, UTF-8, within range), keep its fields private and
  expose the only path to construction via `T::new(...) -> Result<T, _>`
  or `T::try_from(...)`. Consumers then rely on the type, not on a
  convention.
- **`TryFrom`/`TryInto` for fallible integer conversions.** Raw `as`
  truncates silently. Use `u32::try_from(x)?` at boundaries where the
  conversion can fail. `as` is acceptable for infallible casts (widening,
  byte-level reinterpretation under `#[allow]` with a comment). We
  deliberately do not enable `clippy::as_conversions` workspace-wide
  because the SIMD and Arrow paths legitimately use `as` for byte-level
  reinterpretation and contiguous-buffer indexing; the rule is enforced
  by review on new code in non-SIMD paths.

## Naming

- **Functions:** `verb_noun` — `parse_timestamp`, `encode_batch`, `scan_line`
- **Booleans:** `is_`, `has_`, `should_` prefix — `is_full`, `has_pending`
- **Builders:** `new()` constructor, method chaining where natural
- **Config fields:** snake_case matching YAML keys
- **No abbreviations** except: `buf`, `pos`, `len`, `idx`, `cfg`, `ctx`

## Error Handling

- **Libraries return `thiserror` enums. The binary uses `anyhow`.** Every
  crate except `logfwd` (the binary) returns structured `thiserror` error
  types so callers can match on variants. The `logfwd` crate is allowed to
  use `anyhow::Context` at the application shell to add diagnostic
  breadcrumbs as errors propagate to exit. **Never** expose
  `Box<dyn Error>` in a public library signature — it strips the caller's
  ability to recover.
- **Public APIs:** return `Result`, never `panic!` or `assert!` on user input
- **Internal invariants:** `debug_assert!` for programmer errors, not `assert!`
- **Error messages:** include context — `"failed to open {path}: {err}"` not `"IO error"`
- **No `unwrap()` in production paths.** Use `?` or `.expect("reason")`.
  Every surviving `expect` must name the invariant that makes it safe
  (`"config schema guarantees at least one input"`), not just restate the
  call.
- **Sentinel values:** use `Option` instead of magic values (0, -1, empty string)

## Unsafe Code

- **`unsafe` is forbidden in `logfwd-core`** (`#![forbid(unsafe_code)]`)
  and allowed sparingly in `logfwd-arrow` for SIMD. Other crates should
  avoid `unsafe` unless there is no safe alternative.
- **Every `unsafe` block carries a `// SAFETY:` comment** naming the
  invariants that make the operation sound. `clippy::undocumented_unsafe_blocks`
  is `deny` in the workspace — this is enforced, not a suggestion.
- **Minimize scope.** The `unsafe` block is as small as possible — one
  operation per block, not a whole function body.
- **Wrap in a safe abstraction.** An `unsafe` implementation detail
  should be paired with a safe public API. Consumers should never see
  `unsafe fn` on a public surface.
- **SIMD equivalence is a proof obligation, not a review opinion.** Any
  `unsafe` SIMD path must have a proptest showing it produces the same
  output as the scalar fallback. See `CRATE_RULES.md` for `logfwd-arrow`.

## Hot Path Rules

The hot path is: reader → framer → scanner → builders → OTLP encoder → compress.

- **No per-record allocations.** Reuse buffers with `.clear()`.
- **No `format!()` or `.to_string()` in loops.**
- **No `Vec::push` inside per-line loops** — pre-allocate with `with_capacity`.
- **Prefer `&[u8]` over `&str`** in parsing — avoids UTF-8 validation overhead.
- **Benchmark before and after** if touching hot path code. No perf
  claim lands without a `criterion` baseline; see `DEVELOPING.md` →
  *Performance change workflow*.
- **Observability uses `tracing`, not `println!`.** `println!`/`eprintln!`/
  `dbg!` in production paths goes through code review specifically
  because it bypasses structured logging and cannot be filtered at runtime.
  `dbg!` is forbidden outright (`clippy::dbg_macro = deny`).
- **Mark hot-path functions with `#[hot_path]` (from `logfwd-lint-attrs`).**
  The dylint lint `hot_path_no_alloc` flags heap allocations in the
  function body — `Box::new`, `Vec::new`/`with_capacity`, `String::new`/`from`,
  `.to_string()`, `.to_vec()`, `.to_owned()`, `.clone()` on heap types,
  `.collect()` into owned containers, `Arc::new`, `Rc::new`, `HashMap`/`HashSet`
  allocation. Run via `just dylint`. See `crates/logfwd-lints/README.md`.
  The attribute is a compile-time no-op; at runtime the function is
  identical to the un-annotated version.

## Abstractions

- **No traits for single implementations.** A function is simpler.
- **No wrapper types** unless they enforce an invariant.
- **No feature flags** for functionality — just change the code.
- **No `pub use` re-exports** for backwards compatibility — we have no users.

## Public API Shape

Rules of thumb when designing a public (or `pub(crate)`) function or type:

- **Accept the most general useful type, return the specific one.**
  Prefer `&str` over `String`, `&[T]` over `Vec<T>`, `impl IntoIterator<Item = T>`
  over `Vec<T>`, `impl AsRef<Path>` over `&Path`. Return concrete types
  (`Result<Scanner, ScanError>`), not trait objects.
- **Derive the standard traits where they make sense.** `Debug`, `Clone`,
  `PartialEq`, `Eq`, `Hash`, `Default` when the type is a pure value.
  Manual impls only when the derived behavior is wrong.
- **Builders for configurations with more than three knobs.** A struct
  with 5+ optional fields should expose a builder, not a constructor
  taking `Option<T>`s.
- **Doc comment with a working example** on every `pub` item in crates
  with a stable API surface (`logfwd-core`, `logfwd-types`, `logfwd-config`).
  Examples compile as doc-tests — they are regression coverage.
- **Prefer generics for static dispatch.** Reach for `&dyn Trait` only
  when heterogeneity genuinely requires it, or when monomorphization
  cost across a crate boundary outweighs the call-site overhead (see
  compile-time notes in `ARCHITECTURE.md`).

## Output Config Schema

Output configuration has exactly one user-facing shape: the tagged enum
`OutputConfigV2` (`#[serde(tag = "type", rename_all = "snake_case")]`) in
`crates/logfwd-config/src/types.rs`. The flat `OutputConfig` struct is an
in-memory normalized view; it is not a deserialization target any more.

- **Don't add shared fields to `OutputConfig`.** Every output knob lives
  on the typed variant it applies to (`ElasticsearchOutputConfig`,
  `LokiOutputConfig`, `ArrowIpcOutputConfig`, …). `deny_unknown_fields`
  on each variant rejects the field on every other type for free.
- **New output types are new V2 variants**, not new fields on the flat
  struct. Add the variant struct, extend the V2 enum, extend the two
  `From` matches, and the runtime picks it up through `build_sink_factory`.
- **No V1 fallback path.** There used to be a legacy flat-shape fallback
  deserializer; it is gone. If a new YAML shape has to be supported,
  extend V2 directly.
- **Avoid cross-output validators in `validate.rs`.** They ran before the
  V2-only cutover and caught `tenant_id` on a stdout output, etc. V2's
  strict schemas now reject those at parse time with a clearer
  `unknown field ...` error. New validators should assert genuine
  inter-field relationships on a single variant, not cross-variant
  exclusion.

## Comments

- **Doc comments (`///`) on all public items.** Describe behavior, not just the name.
- **No comments restating the code.** `// increment counter` above `counter += 1` is noise.
- **Use comments for WHY, not WHAT.** Explain non-obvious decisions.
- **TODO format:** `// TODO(#123): description` with issue number.

## Tests

- **One test per behavior**, not per function.
- **Test names describe the scenario:** `empty_input_returns_none`, not `test_parse`.
- **No `#[should_panic]`** — test the Result/Option return instead.
- **Kani proofs** for pure logic in logfwd-core (see `dev-docs/DESIGN.md`).
- **proptest** for property-based testing of complex inputs.

## Git

- **Commit messages:** `type: concise description` — `fix:`, `feat:`, `refactor:`, `docs:`, `bench:`, `test:`
- **One concern per commit.** Don't mix fixes with features.
- **No merge commits on feature branches.** Rebase onto main.
- **PR titles:** same format as commit messages, under 70 chars.

## Module Organization

- **One concept per file.** `json_scanner.rs` does scanning, `scanner.rs` defines the builder protocol, and `structural.rs` does structural detection.
- **Tests at bottom of file** in `#[cfg(test)] mod tests {}`.
- **Kani proofs** in `#[cfg(kani)] mod verification {}`.
- **Platform-specific code:** use `wide` crate for portable SIMD, not `#[cfg(target_arch)]`.
