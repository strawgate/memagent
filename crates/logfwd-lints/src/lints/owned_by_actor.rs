//! `owned_by_actor_no_spawn_capture`: flag `tokio::spawn` /
//! `tokio::task::spawn_blocking` / `tokio::task::spawn_local` /
//! `std::thread::spawn` call sites whose closure captures a value whose
//! type is marked `#[logfwd_lint_attrs::owned_by_actor]`.
//!
//! Usage model: attribute goes on a **type definition** (struct or enum).
//! The proc-macro prepends `#[doc = "__logfwd_owned_by_actor__"]`
//! which survives to HIR. When we see a spawn call whose closure
//! captures a value of a tagged type, we flag.

use clippy_utils::diagnostics::span_lint_and_then;
use rustc_hir::def::Res;
use rustc_hir::def_id::DefId;
use rustc_hir::{Expr, ExprKind, QPath};
use rustc_lint::{LateContext, LateLintPass};
use rustc_middle::ty::{Ty, TyKind};
use rustc_span::Symbol;

use crate::lints::util::{has_doc_marker, path_matches};

rustc_session::declare_lint! {
    /// ### What it does
    ///
    /// Flags call sites of `tokio::spawn` / `spawn_blocking` /
    /// `spawn_local` / `std::thread::spawn` that capture a value whose
    /// type is marked `#[logfwd_lint_attrs::owned_by_actor]`.
    ///
    /// ### Why is this bad?
    ///
    /// Some types model per-actor (task/thread) state — a handle into a
    /// thread-local resource, per-connection decoder state keyed on the
    /// spawning task — that is not designed to be shared *or* cleanly
    /// moved across tasks. Rust's `!Send` enforces the cross-task part
    /// for reference-typed fields, but it doesn't catch `Send` types
    /// whose designs discourage movement (e.g., anything whose identity
    /// is keyed to an actor's local scheduler or metric tags).
    ///
    /// ### Known limitation
    ///
    /// The current implementation flags *any* capture into a spawn
    /// closure, including a legitimate `move`-ownership transfer. Use
    /// it only on types that should live and die inside a single task;
    /// types whose idiomatic handoff is "move into a spawned worker and
    /// never touch again" (e.g., the per-input IO worker state in
    /// `logfwd-runtime`) will produce noisy true-positives and should
    /// be left untagged.
    ///
    /// A future version should distinguish ownership transfer
    /// (`move`) from shared capture; doing so requires looking at the
    /// upvar capture kind (`ByValue` vs `ByRef`) and possibly the
    /// MIR-level `StorageDead` of the original binding. Tracked as a
    /// roadmap item in `crates/logfwd-lints/README.md`.
    pub OWNED_BY_ACTOR_NO_SPAWN_CAPTURE,
    Warn,
    "value of #[owned_by_actor] type captured by a spawn closure"
}

rustc_session::declare_lint_pass!(OwnedByActorNoSpawnCapture => [OWNED_BY_ACTOR_NO_SPAWN_CAPTURE]);

const OWNED_BY_ACTOR_MARKER: &str = "__logfwd_owned_by_actor__";

/// Function paths whose call wraps its closure in a new task/thread.
/// A closure argument here executes in a different actor context.
const SPAWN_FN_PATHS: &[&[&str]] = &[
    &["tokio", "task", "spawn", "spawn"],
    &["tokio", "task", "spawn_blocking", "spawn_blocking"],
    &["tokio", "task", "spawn_local", "spawn_local"],
    // public re-exports
    &["tokio", "spawn"],
    // std::thread
    &["std", "thread", "spawn"],
    // tokio runtime methods (Runtime::spawn, Handle::spawn, etc. —
    // matched by the terminal segment since paths shift between
    // versions).
];

impl<'tcx> LateLintPass<'tcx> for OwnedByActorNoSpawnCapture {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        let Some((spawn_name, closure_expr)) = spawn_call_with_closure(cx, expr) else {
            return;
        };
        let ExprKind::Closure(closure) = &closure_expr.kind else { return };

        // Inspect captured upvars: each upvar has a type in the
        // typeck_results. If any upvar type (or a transitive Adt
        // component) is marked `#[owned_by_actor]`, fire.
        let def_id = closure.def_id;
        let tcx = cx.tcx;

        // Walk all upvars of the closure.
        let typeck = cx.typeck_results();
        let Some(upvars) = tcx.upvars_mentioned(def_id) else { return };
        for (captured_hir_id, _upvar) in upvars {
            // Resolve the captured place's type.
            let ty = typeck.node_type(*captured_hir_id);
            if let Some(reason) = any_component_owned_by_actor(cx, ty) {
                span_lint_and_then(
                    cx,
                    OWNED_BY_ACTOR_NO_SPAWN_CAPTURE,
                    closure_expr.span,
                    format!(
                        "`{spawn_name}` closure captures a value whose type {reason} — \
                         this type is marked #[owned_by_actor] and must not cross the \
                         spawn boundary. Move the work into the owning actor, or pass \
                         an owned extract across an explicit channel."
                    ),
                    |_diag| {},
                );
                return; // one hit per spawn is enough
            }
        }
    }
}

/// If `expr` is a direct call to a spawn function, return the
/// human-readable name of the spawn and the first argument (the
/// closure expression).
fn spawn_call_with_closure<'tcx>(
    cx: &LateContext<'tcx>,
    expr: &'tcx Expr<'tcx>,
) -> Option<(String, &'tcx Expr<'tcx>)> {
    match &expr.kind {
        ExprKind::Call(callee, args) => {
            if args.is_empty() {
                return None;
            }
            let ExprKind::Path(qpath) = &callee.kind else { return None };
            let res = match qpath {
                QPath::Resolved(_, path) => path.res,
                QPath::TypeRelative(..) | QPath::LangItem(..) => {
                    cx.typeck_results().qpath_res(qpath, callee.hir_id)
                }
            };
            let Res::Def(_, def_id) = res else { return None };
            let path = cx.get_def_path(def_id);
            for candidate in SPAWN_FN_PATHS {
                if path_matches(&path, candidate) {
                    return Some((candidate.join("::"), args.last().unwrap()));
                }
            }
            None
        }
        ExprKind::MethodCall(method, _recv, args, _span) => {
            let name = method.ident.name.as_str();
            if name == "spawn" || name == "spawn_blocking" || name == "spawn_local" {
                return args
                    .last()
                    .map(|arg| (format!("<runtime>::{name}"), arg));
            }
            None
        }
        _ => None,
    }
}

/// Returns `Some(reason)` if `ty` is — or transitively contains via
/// Adt generic args — a type marked `#[owned_by_actor]`.
fn any_component_owned_by_actor<'tcx>(
    cx: &LateContext<'tcx>,
    ty: Ty<'tcx>,
) -> Option<String> {
    let mut visited: Vec<DefId> = Vec::new();
    visit_ty(cx, ty, &mut visited, 0)
}

fn visit_ty<'tcx>(
    cx: &LateContext<'tcx>,
    ty: Ty<'tcx>,
    visited: &mut Vec<DefId>,
    depth: usize,
) -> Option<String> {
    if depth > 8 {
        return None;
    }
    match ty.kind() {
        TyKind::Adt(adt, substs) => {
            if visited.contains(&adt.did()) {
                return None;
            }
            visited.push(adt.did());
            if type_has_owned_by_actor_marker(cx, adt.did()) {
                let name = cx
                    .get_def_path(adt.did())
                    .iter()
                    .map(Symbol::as_str)
                    .collect::<Vec<_>>()
                    .join("::");
                return Some(format!("`{name}` is #[owned_by_actor]"));
            }
            for arg in substs.iter() {
                if let Some(inner) = arg.as_type()
                    && let Some(reason) = visit_ty(cx, inner, visited, depth + 1)
                {
                    return Some(reason);
                }
            }
            None
        }
        TyKind::Ref(_, inner, _) | TyKind::RawPtr(inner, _) => {
            visit_ty(cx, *inner, visited, depth + 1)
        }
        TyKind::Tuple(elems) => elems
            .iter()
            .find_map(|t| visit_ty(cx, t, visited, depth + 1)),
        _ => None,
    }
}

fn type_has_owned_by_actor_marker(cx: &LateContext<'_>, def_id: DefId) -> bool {
    let Some(local_def_id) = def_id.as_local() else {
        // Cross-crate types: we can't see their HIR attrs, but we can
        // query the tcx for attributes on the def.
        for attr in cx.tcx.get_all_attrs(def_id) {
            if attr.has_name(Symbol::intern("doc"))
                && let Some(value) = attr.value_str()
                && value.as_str() == OWNED_BY_ACTOR_MARKER
            {
                return true;
            }
        }
        return false;
    };
    let hir_id = cx.tcx.local_def_id_to_hir_id(local_def_id);
    has_doc_marker(cx, hir_id, OWNED_BY_ACTOR_MARKER)
}
