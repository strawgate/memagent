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

| Lint | Level | Target | What it flags |
|---|---|---|---|
| `hot_path_no_alloc` | warn | `#[hot_path]` fn | Heap allocations — `Box::new`, `Vec::new`/`with_capacity`, `String::from`, `.to_string()`, `.to_vec()`, `.to_owned()`, `.clone()` on heap types, `.collect::<Vec/String/HashMap>()`, `format!`, `vec![]`, `Arc::new`, `Rc::new`, `HashMap::new`/`with_capacity`, `HashSet::new`/`with_capacity`. |
| `cancel_safe_no_lock_across_await` | warn | `#[cancel_safe]` async fn | Lock guards held across a `.await` via MIR `coroutine_witnesses` — `std::sync::MutexGuard`/`RwLockReadGuard`/`RwLockWriteGuard`, `parking_lot` variants, `lock_api` generic guards, `tokio::sync::MutexGuard`/`OwnedMutexGuard`/`RwLock(Owned)?{Read,Write}Guard`, `core::cell::Ref`/`RefMut`. Same mechanism clippy's `await_holding_lock` uses; the dylint version is scoped to tagged fns so the contract is visible in the source. |
| `no_panic_in_body` | warn | `#[no_panic]` fn | Direct `panic!`/`todo!`/`unimplemented!`/`unreachable!`/`assert*!` macros, `.unwrap()`/`.expect()`, or slice/array indexing. Shallow — no transitive reachability. |
| `pure_no_side_effects` | warn | `#[pure]` fn | IO (`std::fs`, `std::net`, stdio), env/process/thread APIs, clocks (`Instant::now`, `SystemTime::now`), `rand::*` / `rand_core::*` / `rand_chacha::*` / `rand_xoshiro::*`, `std::ptr::read`/`write`, and access to any `static mut`. Shallow check. |
| `owned_by_actor_no_spawn_capture` | warn | `#[owned_by_actor]` struct/enum | `tokio::spawn`/`spawn_blocking`/`spawn_local`/`std::thread::spawn` calls whose closure captures a value of the tagged type (directly or via `Adt` generic args / `Ref` / `RawPtr` / tuple components). Current implementation also fires on legitimate ownership-transfer moves; see *Known limitations* below. |

## Related workspace-wide clippy lints

The dylint lints above run alongside these clippy lints that provide
a broader safety net (all enforced workspace-wide via root `Cargo.toml`):

- `clippy::await_holding_lock` (warn) — same MIR-based lock-across-await
  check as `cancel_safe_no_lock_across_await`, scoped to every async fn
  in the workspace. The dylint version adds targeted, documented
  assertions via the `#[cancel_safe]` attribute.
- `clippy::await_holding_refcell_ref` (warn) — same for `RefCell` refs.
- `clippy::large_futures` (warn) — flags async fns whose generated
  future is unexpectedly large; expensive if awaited in a `select!`
  branch since each branch must be pinned.
- `clippy::dbg_macro` (deny), `clippy::undocumented_unsafe_blocks` (deny),
  `clippy::let_underscore_future` (deny).

## Known limitations

- **`owned_by_actor_no_spawn_capture`** fires on any capture into a
  spawn closure, including legitimate ownership-transfer moves. This
  makes the lint a good fit for types whose single-actor lifecycle is
  born-and-died-inside-one-task, but over-aggressive for types whose
  idiomatic handoff is "move into a spawned worker." Distinguishing
  `move`-by-value from shared capture requires inspecting the closure
  upvar capture kind (`ByValue` vs `ByRef`) and is roadmapped.
- **`pure_no_side_effects`** and **`no_panic_in_body`** are shallow —
  they flag direct calls in the tagged function body, not calls
  reached transitively through other functions. Use these on small
  audited helpers; pair with documented call-graph review for larger
  guarantees.
- **`cancel_safe_no_lock_across_await`** uses the same MIR mechanism
  as clippy's `await_holding_lock`, so they agree on the lock set. The
  scoping difference is the point: the dylint version lets you opt
  functions out of the clippy warn via `#[allow(clippy::await_holding_lock)]`
  while still keeping the strict check on tagged `#[cancel_safe]` fns.

## How to tag a function or type

```rust
use logfwd_lint_attrs::{hot_path, cancel_safe, no_panic, pure, owned_by_actor};

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

#[pure]
fn checksum(bytes: &[u8]) -> u64 {
    // `std::time::Instant::now()` would fire `pure_no_side_effects`.
    bytes.iter().fold(0u64, |acc, &b| acc.wrapping_add(u64::from(b)))
}

#[owned_by_actor]
struct DiagnosticsHandle { /* … */ }

fn hand_off(handle: DiagnosticsHandle) {
    tokio::spawn(async move {
        // capture of DiagnosticsHandle into spawn — fires
        // `owned_by_actor_no_spawn_capture`.
        let _ = handle;
    });
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
  (flush happens-before persist; recovery reads before flush). Needs
  domain-specific state-machine encoding.
- Refine `owned_by_actor_no_spawn_capture` to distinguish capture
  kind (`ByValue` `move` vs `ByRef` shared) so legitimate ownership
  transfers don't fire.
- `#[deterministic]` — no time, no rand, no env. Subset of `#[pure]`
  that ignores stdio but still bans non-deterministic sources.
- `#[must_not_await_on(cancel_unsafe_futures)]` — configurable list
  of known-cancel-unsafe futures (e.g., `AsyncReadExt::read`); fire
  if a `#[cancel_safe]` fn awaits on any of them.
