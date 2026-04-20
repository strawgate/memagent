# logfwd-lints

Dylint library implementing semantic lints for the logfwd workspace.

**This crate is excluded from the main workspace.** It pins a specific
Rust nightly and links against `rustc_private` crates. Run it via
`cargo dylint`, not `cargo build`.

## Usage

```bash
# One-time: install dylint + the nightly the library pins.
cargo install cargo-dylint dylint-link --locked
rustup toolchain install nightly-2025-09-18 \
    --component llvm-tools-preview --component rustc-dev

# From repo root:
just dylint
# or equivalently:
cargo dylint --path crates/logfwd-lints -- --workspace
```

## Lints

| Lint | Level | What it flags |
|---|---|---|
| `hot_path_no_alloc` | warn | Heap allocations inside a function marked `#[logfwd_lint_attrs::hot_path]` — `Box::new`, `Vec::new`/`with_capacity`, `String::from`, `.to_string()`, `.to_vec()`, `.to_owned()`, `.clone()` on heap types, `.collect::<Vec/String/HashMap>()`, `format!`, `vec![]`, `Arc::new`, `Rc::new`, `HashMap::new`/`with_capacity`, `HashSet::new`/`with_capacity`. |
| `cancel_safe_no_lock_across_await` | warn | Lock guards held across a `.await` point inside a function marked `#[logfwd_lint_attrs::cancel_safe]` — `std::sync::MutexGuard`/`RwLockReadGuard`/`RwLockWriteGuard`, `parking_lot` variants, `lock_api` generic guards, `tokio::sync::MutexGuard`/`OwnedMutexGuard`/`RwLock(Owned)?{Read,Write}Guard`, `core::cell::Ref`/`RefMut`. A dropped future — common in `tokio::select!` branches — leaks the guard until scope unwinds. |
| `no_panic_in_body` | warn | Direct `panic!`/`todo!`/`unimplemented!`/`unreachable!`/`assert*!` macros, `.unwrap()`/`.expect()`, or slice/array indexing inside a function marked `#[logfwd_lint_attrs::no_panic]`. Does **not** perform transitive reachability analysis — functions called from the body are not inspected. |

## Related workspace-wide clippy lints

The dylint lints above run alongside these clippy lints that provide
a broader safety net (all enforced workspace-wide via root `Cargo.toml`):

- `clippy::await_holding_lock` (warn) — same lock-across-await check as
  `cancel_safe_no_lock_across_await`, but scoped to every async fn in
  the workspace. The dylint version adds targeted, documented assertions
  via the `#[cancel_safe]` attribute.
- `clippy::await_holding_refcell_ref` (warn) — same for `RefCell` refs.
- `clippy::dbg_macro` (deny), `clippy::undocumented_unsafe_blocks` (deny),
  `clippy::let_underscore_future` (deny).

## How to tag a function

```rust
use logfwd_lint_attrs::{hot_path, cancel_safe, no_panic};

#[hot_path]
fn scan_line(buf: &[u8]) -> usize {
    // `let owned = buf.to_vec();` would fire `hot_path_no_alloc`.
    buf.len()
}

#[cancel_safe]
async fn select_loop(rx: &mut Receiver<Msg>) -> Option<Msg> {
    // Holding a std::sync::MutexGuard across the recv().await below
    // would fire `cancel_safe_no_lock_across_await`.
    rx.recv().await
}

#[no_panic]
fn parse_header(b: &[u8]) -> Option<u32> {
    // `b[0]` would fire `no_panic_in_body` (indexing can panic).
    b.first().map(|&x| u32::from(x))
}
```

See `dev-docs/CODE_STYLE.md` for where each attribute is currently used
and the review policy around expanding coverage.

## Adding a new lint

1. Create a new submodule under `src/lints/`.
2. Use `rustc_session::declare_lint!` and `declare_lint_pass!` at the
   top of the module. Do **not** use `dylint_linting::declare_late_lint!`
   (that macro generates its own `register_lints` and can only be used
   by single-lint libraries).
3. Implement `LateLintPass::check_fn` or similar. If the lint relies
   on a `#[logfwd_lint_attrs::<name>]` attribute, look for the marker
   `__logfwd_<name>__` doc attribute via `util::has_doc_marker`.
4. Register in `src/lib.rs::register_lints`.
5. Add a corresponding attribute in `crates/logfwd-lint-attrs/src/lib.rs`.
6. Add a UI test in `ui/<lint_name>.rs`.
7. Document in the Lints table above, in
   `dev-docs/CODE_STYLE.md` → *Semantic lints (dylint)*, and in
   `crates/logfwd-lint-attrs/src/lib.rs` crate-level docs.

## Roadmap

Future lints for which this scaffolding makes the cost ~50-100 lines
each:

- `#[checkpoint_ordered]` — encode the checkpoint-store protocol
  (flush happens-before persist; recovery reads before flush).
- `#[owned_by_actor]` — mark a type that must not cross task boundaries
  without an explicit handoff.
- `#[pure]` — no observable side effects, no IO, no globals.
- MIR-based `cancel_safe` check — the current implementation uses HIR
  binding scopes, which can miss some guard lifetimes that clippy's
  MIR-based `await_holding_lock` catches. A MIR-scoped version would
  supersede the current lint.
