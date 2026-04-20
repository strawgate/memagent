//! Dylint library implementing semantic lints for the logfwd workspace.
//!
//! Lints are invoked via `cargo dylint --all`. Each lint corresponds to
//! an attribute in the `logfwd-lint-attrs` proc-macro crate:
//!
//! | Attribute | Lint | What it flags |
//! |---|---|---|
//! | `#[hot_path]` | `hot_path_no_alloc` | Heap allocations in the tagged function body. |
//! | `#[cancel_safe]` | `cancel_safe_no_lock_across_await` | Lock guards held across `.await`. |
//! | `#[no_panic]` | `no_panic_in_body` | Direct panic macros, `.unwrap()`, `.expect()`, or indexing in the tagged body. |
//!
//! The attribute proc macros are consumed during expansion, so they
//! prepend a hidden `#[doc = "__logfwd_<name>__"]` marker that survives
//! to HIR. Each lint looks for its marker.

#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_middle;
extern crate rustc_session;
extern crate rustc_span;

pub mod lints;

dylint_linting::dylint_library!();

/// Register every lint this library ships.
///
/// Called by `cargo dylint` at driver initialization.
#[unsafe(no_mangle)]
pub fn register_lints(sess: &rustc_session::Session, lint_store: &mut rustc_lint::LintStore) {
    dylint_linting::init_config(sess);
    lint_store.register_lints(&[
        lints::hot_path_no_alloc::HOT_PATH_NO_ALLOC,
        lints::cancel_safe::CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT,
        lints::no_panic::NO_PANIC_IN_BODY,
    ]);
    lint_store.register_late_pass(|_| Box::new(lints::hot_path_no_alloc::HotPathNoAlloc));
    lint_store.register_late_pass(|_| Box::new(lints::cancel_safe::CancelSafeNoLockAcrossAwait));
    lint_store.register_late_pass(|_| Box::new(lints::no_panic::NoPanicInBody));
}
