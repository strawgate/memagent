//! Dylint library implementing semantic lints for the ffwd workspace.
//!
//! Lints are invoked via `cargo dylint --all`. Each lint corresponds to
//! an attribute in the `ffwd-lint-attrs` proc-macro crate:
//!
//! | Attribute | Lint | Target | What it flags |
//! |---|---|---|---|
//! | `#[hot_path]` | `hot_path_no_alloc` | fn | Heap allocations in the tagged function body. |
//! | `#[cancel_safe]` | `cancel_safe_no_lock_across_await` | async fn | Lock guards held across `.await` (MIR coroutine witnesses). |
//! | `#[no_panic]` | `no_panic_in_body` | fn | Direct panic macros, `.unwrap()`, `.expect()`, or indexing in the tagged body. |
//! | `#[pure]` | `pure_no_side_effects` | fn | IO, env/process/thread, clocks, `rand::*`, `static mut` access in the tagged body. |
//! | `#[owned_by_actor]` | `owned_by_actor_no_spawn_capture` | struct/enum | Value of the tagged type captured by a `tokio::spawn` / `std::thread::spawn` closure. |
//!
//! The attribute proc macros are consumed during expansion, so they
//! prepend a hidden `#[doc = "__ffwd_<name>__"]` marker that survives
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
        lints::pure::PURE_NO_SIDE_EFFECTS,
        lints::owned_by_actor::OWNED_BY_ACTOR_NO_SPAWN_CAPTURE,
    ]);
    lint_store.register_late_pass(|_| Box::new(lints::hot_path_no_alloc::HotPathNoAlloc));
    lint_store.register_late_pass(|_| Box::new(lints::cancel_safe::CancelSafeNoLockAcrossAwait));
    lint_store.register_late_pass(|_| Box::new(lints::no_panic::NoPanicInBody));
    lint_store.register_late_pass(|_| Box::new(lints::pure::PureNoSideEffects));
    lint_store.register_late_pass(|_| Box::new(lints::owned_by_actor::OwnedByActorNoSpawnCapture));
}
