# logfwd-lints

Dylint library implementing semantic lints for the logfwd workspace.

**This crate is excluded from the main workspace.** It pins a specific
Rust nightly and links against `rustc_private` crates. Run it via
`cargo dylint`, not `cargo build`.

## Usage

```bash
# One-time: install dylint.
cargo install cargo-dylint dylint-link --locked

# From repo root:
just dylint
# or equivalently:
cargo dylint --path crates/logfwd-lints -- --workspace
```

## Lints

| Lint | Level | What it flags |
|---|---|---|
| `hot_path_no_alloc` | warn | Heap allocations inside a function marked `#[logfwd_lint_attrs::hot_path]` — `Box::new`, `Vec::new`/`with_capacity`, `String::from`, `.to_string()`, `.to_vec()`, `.to_owned()`, `.clone()` on heap types, `.collect::<Vec/String/HashMap>()`, `format!`, `vec![]`, `Arc::new`, `Rc::new`, `HashMap::new`/`with_capacity`, `HashSet::new`/`with_capacity`. |

## How to tag a function

```rust
use logfwd_lint_attrs::hot_path;

#[hot_path]
fn scan_line(buf: &[u8]) -> usize {
    // `let owned = buf.to_vec();` would fire the lint.
    buf.len()
}
```

See `dev-docs/CODE_STYLE.md` → *Hot Path Rules* for policy.

## Adding a new lint

1. Add a new `declare_late_lint!` stanza in `src/lib.rs`.
2. Implement `LateLintPass::check_*` with the predicate.
3. Add a UI test in `ui/`.
4. Document it in the table above and in `dev-docs/CODE_STYLE.md`.

Candidate future lints (see `dev-docs/CODE_STYLE.md`):

- `#[cancel_safe]` — no `std::sync::Mutex` guard or buffered
  `mpsc::Sender` held across `.await` in tokio `select!` branches.
- `#[no_panic]` — transitive reachability check.
- `#[checkpoint_ordered]` — protocol assertion for checkpoint-store
  operations.
