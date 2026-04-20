//! Attribute macros that tag functions for semantic lints.
//!
//! Each attribute is a compile-time no-op: it prepends a hidden
//! `#[doc = "__logfwd_<name>__"]` marker to its input and returns the
//! result. The dylint lint library (`crates/logfwd-lints/`) finds the
//! marker post-expansion to enforce the invariant each attribute asserts.
//!
//! Proc-macro attributes are consumed during expansion (the `#[hot_path]`
//! token itself vanishes), so the marker-via-doc-attribute is the portable
//! way to carry the annotation into HIR without requiring nightly tool
//! attributes.
//!
//! # Available attributes
//!
//! | Attribute | Lint | Target | Asserts |
//! |---|---|---|---|
//! | `#[hot_path]` | `hot_path_no_alloc` | fn | No heap allocation in the function body. |
//! | `#[cancel_safe]` | `cancel_safe_no_lock_across_await` | async fn | No `MutexGuard`/`RwLock*Guard`/`RefCell` ref (std, parking_lot, tokio::sync, lock_api) live across `.await` per MIR coroutine layout. |
//! | `#[no_panic]` | `no_panic_in_body` | fn | No direct panic macros, no `.unwrap()`/`.expect()`, and no slice/array indexing in the function body. |
//! | `#[pure]` | `pure_no_side_effects` | fn | No IO (`std::fs`/`std::net`/stdio), no env/process/thread, no clocks, no `static mut` access, no `rand::*`. Shallow check. |
//! | `#[owned_by_actor]` | `owned_by_actor_no_spawn_capture` | struct / enum | Values of the tagged type never cross into `tokio::spawn`/`spawn_blocking`/`spawn_local` or `std::thread::spawn` closures. |
//!
//! # Runtime behavior
//!
//! These attributes generate no code. They exist purely so the lint can
//! find them. Functions behave identically with or without the attribute
//! at runtime.

#![warn(missing_docs)]

use proc_macro::TokenStream;

/// Mark a function as lying on the pipeline hot path.
///
/// The `hot_path_no_alloc` dylint lint flags any heap allocation inside
/// a function carrying this attribute. See crate-level docs.
#[proc_macro_attribute]
pub fn hot_path(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_hot_path__", item)
}

/// Mark an async function as cancel-safe.
///
/// The `cancel_safe_no_lock_across_await` dylint lint flags any lock
/// guard held across a `.await` in a function carrying this attribute.
/// Awaits in a `tokio::select!` branch can drop the future at the
/// `.await` point; if the dropped future is holding a `Mutex` guard,
/// the mutex stays locked (or worse, deadlocks with the task that was
/// supposed to release it). Marking a function `#[cancel_safe]` is an
/// author-asserted contract; the lint enforces the lock-across-await
/// piece mechanically.
///
/// This attribute does *not* guarantee the async operations called
/// inside are themselves cancel-safe — that remains a review-level
/// concern for now (see the tokio `select!` cancel-safety table).
#[proc_macro_attribute]
pub fn cancel_safe(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_cancel_safe__", item)
}

/// Mark a function as panic-free.
///
/// The `no_panic_in_body` dylint lint flags direct panic macros
/// (`panic!`, `todo!`, `unimplemented!`, `unreachable!`), `.unwrap()`,
/// `.expect()`, and slice/array indexing expressions in the function
/// body. It does *not* perform transitive reachability analysis —
/// functions called from the body are not inspected.
///
/// Use this on small, audited pure-logic functions (parsers, state
/// transitions, encoders) where the review guarantee "this function
/// itself will never panic" is valuable even without transitive proof.
#[proc_macro_attribute]
pub fn no_panic(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_no_panic__", item)
}

/// Mark a function as pure (no observable side effects).
///
/// The `pure_no_side_effects` dylint lint flags calls in the function
/// body to IO (`std::fs`, `std::net`, `std::io::stdin`/`stdout`/`stderr`),
/// process/env/thread APIs (`std::env`, `std::process`, `std::thread`),
/// clocks (`Instant::now`, `SystemTime::now`), `std::os::*`, and access
/// to any `static mut`. Random-number sources from `rand::*` are also
/// flagged.
///
/// Like `#[no_panic]`, the check is shallow — functions called from the
/// body are not inspected. Use on small computational helpers
/// (parsers, encoders, state transitions) that must be deterministic
/// and side-effect-free for testability and verification.
#[proc_macro_attribute]
pub fn pure(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_pure__", item)
}

/// Mark a type as owned by a single actor (task/thread).
///
/// The `owned_by_actor_no_spawn_capture` dylint lint flags
/// `tokio::spawn`, `tokio::task::spawn_blocking`, `tokio::task::spawn_local`,
/// and `std::thread::spawn` call sites whose closure captures a value
/// whose type carries this attribute. The attribute goes on the type
/// definition (`struct`/`enum`), not on a function.
///
/// Use this to keep single-ownership actor boundaries explicit:
/// checkpoint-store state, input-source session state, connection
/// pools that aren't designed to be shared, etc. Instead of relying on
/// `!Send` bounds (which can be hard to read on trait objects), the
/// attribute + lint makes the contract visible at the type definition
/// and catches every illegal capture mechanically.
#[proc_macro_attribute]
pub fn owned_by_actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_owned_by_actor__", item)
}

fn prepend_marker(marker: &str, item: TokenStream) -> TokenStream {
    let attr: TokenStream = format!("#[doc = \"{marker}\"]")
        .parse()
        .expect("valid marker attribute");
    let mut out = attr;
    out.extend(item);
    out
}
