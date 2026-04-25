//! `cancel_safe_no_lock_across_await`: flag lock guards held across
//! `.await` points inside functions marked
//! `#[ffwd_lint_attrs::cancel_safe]`.
//!
//! A future dropped mid-`.await` (common in `tokio::select!` branches)
//! cannot run its destructors until the drop point is reached. If that
//! future holds a `MutexGuard`, the lock stays held until the outer
//! scope unwinds.
//!
//! This lint uses the same MIR `coroutine_witnesses` mechanism as
//! `clippy::await_holding_lock`, but scoped to functions that carry the
//! `#[cancel_safe]` contract. The author declaration + scoped
//! enforcement keeps the per-function assertion visible in the source
//! without forcing every async fn in the codebase to satisfy the
//! no-lock-across-await rule.

use clippy_utils::diagnostics::span_lint_and_then;
use rustc_hir::def_id::DefId;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_middle::mir::CoroutineLayout;
use rustc_middle::ty::TyKind;

use crate::lints::util::has_doc_marker;

rustc_session::declare_lint! {
    /// ### What it does
    ///
    /// Flags locks held across `.await` points inside functions marked
    /// with `#[ffwd_lint_attrs::cancel_safe]`.
    ///
    /// ### Why is this bad?
    ///
    /// A future dropped mid-`.await` (common in `tokio::select!` branches)
    /// leaks the `MutexGuard` until the surrounding scope unwinds. If
    /// another task is racing for the same lock, the system deadlocks
    /// or stalls.
    ///
    /// Uses MIR `coroutine_witnesses` for accurate liveness analysis —
    /// matches what `clippy::await_holding_lock` does but is scoped
    /// to tagged functions.
    pub CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT,
    Warn,
    "lock guard held across `.await` in a function marked #[cancel_safe]"
}

rustc_session::declare_lint_pass!(CancelSafeNoLockAcrossAwait => [CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT]);

const CANCEL_SAFE_MARKER: &str = "__ffwd_cancel_safe__";

/// Unqualified type names considered lock guards. Matched against the
/// final segment of the ADT def-path so path reshuffles in std (e.g.
/// the move of `MutexGuard` under `std::sync::poison::mutex`) don't
/// break the lint.
const LOCK_GUARD_TYPE_NAMES: &[&str] = &[
    "MutexGuard",
    "OwnedMutexGuard",
    "FairMutexGuard",
    "ReentrantMutexGuard",
    "RwLockReadGuard",
    "RwLockWriteGuard",
    "OwnedRwLockReadGuard",
    "OwnedRwLockWriteGuard",
    "Ref",
    "RefMut",
];

impl<'tcx> LateLintPass<'tcx> for CancelSafeNoLockAcrossAwait {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        // Only look at async closures — the MIR coroutine layout is
        // attached to the desugared coroutine that backs every
        // `async fn` / `async { }` block.
        let ExprKind::Closure(closure) = &expr.kind else {
            return;
        };
        let rustc_hir::ClosureKind::Coroutine(rustc_hir::CoroutineKind::Desugared(
            rustc_hir::CoroutineDesugaring::Async,
            _,
        )) = closure.kind
        else {
            return;
        };

        // Walk up to find the enclosing `#[cancel_safe]` marker. If no
        // ancestor carries it, leave this async block alone.
        if !ancestor_has_cancel_safe_marker(cx, closure.def_id) {
            return;
        }

        let Some(coroutine_layout) = cx.tcx.mir_coroutine_witnesses(closure.def_id) else {
            return;
        };
        check_interior_types(cx, coroutine_layout);
    }
}

fn ancestor_has_cancel_safe_marker(
    cx: &LateContext<'_>,
    def_id: rustc_span::def_id::LocalDefId,
) -> bool {
    let mut hir_id = cx.tcx.local_def_id_to_hir_id(def_id);
    for _ in 0..6 {
        if has_doc_marker(cx, hir_id, CANCEL_SAFE_MARKER) {
            return true;
        }
        let parent_owner = cx.tcx.hir_get_parent_item(hir_id);
        let parent_hir_id = cx.tcx.local_def_id_to_hir_id(parent_owner.def_id);
        if parent_hir_id == hir_id {
            return false;
        }
        hir_id = parent_hir_id;
    }
    false
}

fn check_interior_types<'tcx>(cx: &LateContext<'tcx>, coroutine: &CoroutineLayout<'tcx>) {
    for ty_cause in coroutine.field_tys.iter() {
        let TyKind::Adt(adt, _) = ty_cause.ty.kind() else { continue };
        let Some(name) = guard_type_name(cx, adt.did()) else { continue };
        span_lint_and_then(
            cx,
            CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT,
            ty_cause.source_info.span,
            format!(
                "`{name}` is held across an `.await` in a #[cancel_safe] function. \
                 If this future is dropped (e.g., a `tokio::select!` branch losing) \
                 the guard leaks until the surrounding scope unwinds. Drop the guard \
                 before `.await` or switch to a tokio::sync primitive with explicit drops."
            ),
            |_diag| {},
        );
    }
}

fn guard_type_name(cx: &LateContext<'_>, def_id: DefId) -> Option<String> {
    // Prefer rustc's diagnostic-item names when available — they survive
    // std module reshuffles.
    if let Some(name) = cx.tcx.get_diagnostic_name(def_id) {
        let s = name.as_str();
        if s == "MutexGuard" || s == "RwLockReadGuard" || s == "RwLockWriteGuard" {
            return Some(s.to_string());
        }
        if s == "RefCellRef" {
            return Some("core::cell::Ref".to_string());
        }
        if s == "RefCellRefMut" {
            return Some("core::cell::RefMut".to_string());
        }
    }

    // Fallback to path-based detection (covers parking_lot, tokio::sync,
    // etc., which don't carry diagnostic-item attrs).
    let path = cx.get_def_path(def_id);
    let last = path.last()?.as_str();
    if !LOCK_GUARD_TYPE_NAMES.contains(&last) {
        return None;
    }
    // Disambiguate `Ref`/`RefMut`: only flag if the path includes
    // `cell` somewhere, so arbitrary types named `Ref` aren't swept up.
    if (last == "Ref" || last == "RefMut")
        && !path.iter().any(|s| s.as_str() == "cell")
    {
        return None;
    }
    if path.len() >= 2 {
        Some(format!(
            "{}::{}",
            path[path.len() - 2].as_str(),
            last
        ))
    } else {
        Some(last.to_string())
    }
}

