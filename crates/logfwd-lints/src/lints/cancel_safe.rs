//! `cancel_safe_no_lock_across_await`: flag lock guards held across
//! `.await` points inside functions marked
//! `#[logfwd_lint_attrs::cancel_safe]`.
//!
//! A future dropped mid-`.await` (common in `tokio::select!` branches)
//! cannot run its destructors until the drop point is reached. If that
//! future holds a `MutexGuard`, the lock stays held until the outer
//! scope unwinds. In a `select!` where another branch is also trying
//! to take the same lock, this is a deadlock.
//!
//! This lint complements the workspace-wide `clippy::await_holding_lock`
//! and `clippy::await_holding_refcell_ref` warnings by providing
//! targeted, documented assertions: `#[cancel_safe]` declares "this
//! function is safe to `select!`-drop at any `.await` point" and the
//! lint enforces the lock-guard component of that contract.

use clippy_utils::diagnostics::span_lint_and_then;
use rustc_hir::intravisit::{FnKind, Visitor, walk_body, walk_expr};
use rustc_hir::{Body, Expr, ExprKind, FnDecl, HirId, StmtKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_middle::ty::Ty;
use rustc_span::{Span, Symbol};

use crate::lints::util::has_doc_marker;

rustc_session::declare_lint! {
    /// ### What it does
    ///
    /// Flags locks held across `.await` points inside functions marked
    /// with `#[logfwd_lint_attrs::cancel_safe]`.
    ///
    /// ### Why is this bad?
    ///
    /// A future dropped mid-`.await` (common in `tokio::select!` branches)
    /// leaks the `MutexGuard` until the surrounding scope unwinds. If
    /// another task is racing for the same lock, the system deadlocks
    /// or stalls.
    ///
    /// This lint complements `clippy::await_holding_lock` by scoping
    /// the check to functions that have declared a cancel-safe contract,
    /// making the assertion explicit in the source.
    pub CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT,
    Warn,
    "lock guard held across `.await` in a function marked #[cancel_safe]"
}

rustc_session::declare_lint_pass!(CancelSafeNoLockAcrossAwait => [CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT]);

const CANCEL_SAFE_MARKER: &str = "__logfwd_cancel_safe__";

/// Types whose existence at an `.await` point means a lock is held.
/// We match the fully-qualified type path of the *binding* that's in
/// scope.
const LOCK_GUARD_TYPE_PATHS: &[&[&str]] = &[
    &["std", "sync", "mutex", "MutexGuard"],
    &["std", "sync", "rwlock", "RwLockReadGuard"],
    &["std", "sync", "rwlock", "RwLockWriteGuard"],
    &["parking_lot", "mutex", "MutexGuard"],
    &["parking_lot", "rwlock", "RwLockReadGuard"],
    &["parking_lot", "rwlock", "RwLockWriteGuard"],
    &["parking_lot", "fair_mutex", "FairMutexGuard"],
    &["lock_api", "mutex", "MutexGuard"],
    &["lock_api", "rwlock", "RwLockReadGuard"],
    &["lock_api", "rwlock", "RwLockWriteGuard"],
    &["tokio", "sync", "mutex", "MutexGuard"],
    &["tokio", "sync", "mutex", "OwnedMutexGuard"],
    &["tokio", "sync", "rwlock", "read_guard", "RwLockReadGuard"],
    &["tokio", "sync", "rwlock", "write_guard", "RwLockWriteGuard"],
    &["tokio", "sync", "rwlock", "owned_read_guard", "OwnedRwLockReadGuard"],
    &["tokio", "sync", "rwlock", "owned_write_guard", "OwnedRwLockWriteGuard"],
    &["core", "cell", "Ref"],
    &["core", "cell", "RefMut"],
];

impl<'tcx> LateLintPass<'tcx> for CancelSafeNoLockAcrossAwait {
    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        _kind: FnKind<'tcx>,
        _decl: &'tcx FnDecl<'tcx>,
        body: &'tcx Body<'tcx>,
        _span: Span,
        def_id: rustc_span::def_id::LocalDefId,
    ) {
        // `async fn` desugars to `fn foo() -> impl Future { async move { body } }`.
        // `check_fn` fires twice: once on the outer fn (which carries
        // the `#[cancel_safe]` doc marker) and once on the inner
        // async-block coroutine (which owns the `.await` points).
        // Walk up the HIR parent chain so we fire on the coroutine
        // body where the visitor can actually see the awaits.
        if !function_or_ancestor_has_marker(cx, def_id) {
            return;
        }

        let mut visitor = AwaitLockVisitor {
            cx,
            lock_bindings: Vec::new(),
            hits: Vec::new(),
        };
        walk_body(&mut visitor, body);

        for (span, name) in visitor.hits {
            span_lint_and_then(
                cx,
                CANCEL_SAFE_NO_LOCK_ACROSS_AWAIT,
                span,
                format!(
                    "`.await` while `{name}` is in scope — if this future is dropped \
                     (e.g., a `tokio::select!` branch losing) the guard leaks until \
                     scope unwinds. Take the lock, finish the critical section, drop \
                     the guard, then `.await`."
                ),
                |_diag| {},
            );
        }
    }
}

fn function_or_ancestor_has_marker(
    cx: &LateContext<'_>,
    def_id: rustc_span::def_id::LocalDefId,
) -> bool {
    let mut hir_id = cx.tcx.local_def_id_to_hir_id(def_id);
    // Bounded walk — async fn desugars nest at most a couple of levels.
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

struct AwaitLockVisitor<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    /// Bindings currently in scope whose type is a lock guard. Stored
    /// as a stack — entries are pushed on `let` and popped on block
    /// exit. The `HirId` is the binding's node id (for future narrowing);
    /// the `String` is a human-readable type name for diagnostics.
    lock_bindings: Vec<(HirId, String)>,
    hits: Vec<(Span, String)>,
}

impl<'a, 'tcx> AwaitLockVisitor<'a, 'tcx> {
    /// Return the name of the innermost lock guard still in scope, if any.
    fn innermost_active_lock(&self) -> Option<&str> {
        self.lock_bindings.last().map(|(_, name)| name.as_str())
    }
}

impl<'a, 'tcx> Visitor<'tcx> for AwaitLockVisitor<'a, 'tcx> {
    fn visit_block(&mut self, block: &'tcx rustc_hir::Block<'tcx>) {
        let stack_depth = self.lock_bindings.len();

        for stmt in block.stmts {
            // Track `let` bindings whose type is a lock guard.
            if let StmtKind::Let(let_stmt) = &stmt.kind
                && let Some(init) = let_stmt.init
            {
                // Walk the statement FIRST so that any `.await` in the
                // initializer (e.g. `mutex.lock().await`) is visited
                // before the guard binding is tracked. Otherwise the
                // guard appears in scope during its own initializer,
                // causing false positives.
                rustc_hir::intravisit::walk_stmt(self, stmt);
                let ty = self.cx.typeck_results().expr_ty(init);
                if let Some(name) = lock_guard_type_name(self.cx, ty) {
                    let binding_hir_id = let_stmt.hir_id;
                    self.lock_bindings.push((binding_hir_id, name));
                }
            } else {
                rustc_hir::intravisit::walk_stmt(self, stmt);
            }
        }
        if let Some(expr) = block.expr {
            self.visit_expr(expr);
        }

        // Bindings introduced in this block go out of scope at the
        // closing `}`.
        self.lock_bindings.truncate(stack_depth);
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if matches!(expr.kind, ExprKind::Yield(_, rustc_hir::YieldSource::Await { .. }))
            && let Some(name) = self.innermost_active_lock()
        {
            self.hits.push((expr.span, name.to_string()));
        }
        walk_expr(self, expr);
    }
}

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

fn lock_guard_type_name<'tcx>(cx: &LateContext<'tcx>, ty: Ty<'tcx>) -> Option<String> {
    use rustc_middle::ty::TyKind;
    let TyKind::Adt(adt, _) = ty.kind() else { return None };
    let path = cx.get_def_path(adt.did());
    let segs: Vec<&str> = path.iter().map(Symbol::as_str).collect();

    // Primary: full-path match against the known list.
    for candidate in LOCK_GUARD_TYPE_PATHS {
        if segs.len() == candidate.len()
            && segs.iter().zip(candidate.iter()).all(|(a, e)| a == e)
        {
            return Some(pretty_path(&segs));
        }
    }

    // Fallback: match just the final segment (the type name). `Ref` /
    // `RefMut` are disambiguated by also requiring "cell" somewhere in
    // the path so we don't flag arbitrary types named `Ref`.
    let last = *segs.last()?;
    if LOCK_GUARD_TYPE_NAMES.contains(&last) {
        if (last == "Ref" || last == "RefMut") && !segs.iter().any(|s| *s == "cell") {
            return None;
        }
        return Some(pretty_path(&segs));
    }
    None
}

fn pretty_path(segs: &[&str]) -> String {
    if segs.len() >= 2 {
        format!("{}::{}", segs[segs.len() - 2], segs[segs.len() - 1])
    } else {
        segs.last().copied().unwrap_or("<guard>").to_string()
    }
}
