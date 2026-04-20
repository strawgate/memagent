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
//! | Attribute | Lint | Asserts |
//! |---|---|---|
//! | `#[hot_path]` | `hot_path_no_alloc` | No heap allocation in the function body. |
//! | `#[cancel_safe]` | `cancel_safe_no_lock_across_await` | No `std::sync::MutexGuard`, `parking_lot` guard, `RwLock{Read,Write}Guard`, `RefCell::borrow{,_mut}` guard, or `tokio::sync::MutexGuard` live across `.await`. |
//! | `#[no_panic]` | `no_panic_in_body` | No direct `panic!`/`todo!`/`unimplemented!`/`unreachable!` macros, no `.unwrap()`/`.expect()`, and no slice/array indexing in the function body. |
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

fn prepend_marker(marker: &str, item: TokenStream) -> TokenStream {
    let attr: TokenStream = format!("#[doc = \"{marker}\"]")
        .parse()
        .expect("valid marker attribute");
    let mut out = attr;
    out.extend(item);
    out
}
