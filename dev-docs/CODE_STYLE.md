# Code Style

Style preferences enforced during code review. These are NOT linting
rules — they are subjective conventions that keep the codebase
consistent. CodeRabbit and human reviewers enforce these.

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
- **overflow-checks** are enabled in release builds.

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

## Naming

- **Functions:** `verb_noun` — `parse_timestamp`, `encode_batch`, `scan_line`
- **Booleans:** `is_`, `has_`, `should_` prefix — `is_full`, `has_pending`
- **Builders:** `new()` constructor, method chaining where natural
- **Config fields:** snake_case matching YAML keys
- **No abbreviations** except: `buf`, `pos`, `len`, `idx`, `cfg`, `ctx`

## Error Handling

- **Public APIs:** return `Result`, never `panic!` or `assert!` on user input
- **Internal invariants:** `debug_assert!` for programmer errors, not `assert!`
- **Error messages:** include context — `"failed to open {path}: {err}"` not `"IO error"`
- **No `unwrap()` in production paths.** Use `?` or `.expect("reason")`
- **Sentinel values:** use `Option` instead of magic values (0, -1, empty string)

## Hot Path Rules

The hot path is: reader → framer → scanner → builders → OTLP encoder → compress.

- **No per-record allocations.** Reuse buffers with `.clear()`.
- **No `format!()` or `.to_string()` in loops.**
- **No `Vec::push` inside per-line loops** — pre-allocate with `with_capacity`.
- **Prefer `&[u8]` over `&str`** in parsing — avoids UTF-8 validation overhead.
- **Benchmark before and after** if touching hot path code.

## Abstractions

- **No traits for single implementations.** A function is simpler.
- **No wrapper types** unless they enforce an invariant.
- **No feature flags** for functionality — just change the code.
- **No `pub use` re-exports** for backwards compatibility — we have no users.

## Comments

- **Doc comments (`///`) on all public items.** Describe behavior, not just the name.
- **No comments restating the code.** `// increment counter` above `counter += 1` is noise.
- **Use comments for WHY, not WHAT.** Explain non-obvious decisions.
- **TODO format:** `// TODO(#123): description` with issue number.

## Tests

- **One test per behavior**, not per function.
- **Test names describe the scenario:** `empty_input_returns_none`, not `test_parse`.
- **No `#[should_panic]`** — test the Result/Option return instead.
- **Kani proofs** for pure logic in logfwd-core (see `dev-docs/DECISIONS.md`).
- **proptest** for property-based testing of complex inputs.

## Git

- **Commit messages:** `type: concise description` — `fix:`, `feat:`, `refactor:`, `docs:`, `bench:`, `test:`
- **One concern per commit.** Don't mix fixes with features.
- **No merge commits on feature branches.** Rebase onto master.
- **PR titles:** same format as commit messages, under 70 chars.

## Module Organization

- **One concept per file.** `scanner.rs` does scanning, `structural.rs` does structural detection.
- **Tests at bottom of file** in `#[cfg(test)] mod tests {}`.
- **Kani proofs** in `#[cfg(kani)] mod verification {}`.
- **Platform-specific code:** use `wide` crate for portable SIMD, not `#[cfg(target_arch)]`.
