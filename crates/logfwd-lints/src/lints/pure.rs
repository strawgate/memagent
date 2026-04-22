//! `pure_no_side_effects`: flag calls to IO, env, process, thread,
//! clock, and `rand` APIs inside functions marked
//! `#[logfwd_lint_attrs::pure]`. Also flag reads or writes of any
//! `static mut`.
//!
//! Shallow check — does not follow called functions. Use on small
//! audited helpers that should be deterministic.

use clippy_utils::diagnostics::span_lint_and_then;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{FnKind, Visitor, walk_body, walk_expr};
use rustc_hir::{Body, Expr, ExprKind, FnDecl, Mutability, QPath};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;

use crate::lints::util::{has_doc_marker, path_matches};

rustc_session::declare_lint! {
    /// ### What it does
    ///
    /// Flags observable side effects inside a function marked
    /// `#[logfwd_lint_attrs::pure]` — IO, env/process/thread APIs,
    /// system clocks, `rand::*` sampling, and any read or write of a
    /// `static mut` variable.
    ///
    /// ### Why is this bad?
    ///
    /// Pure helpers (parsers, encoders, state transitions) are the
    /// easiest to test, verify, and reason about. Drift — a parser
    /// that suddenly calls `SystemTime::now()` for a fallback, or
    /// touches a `static mut` counter — silently breaks that. The
    /// attribute declares the contract; the lint enforces it.
    pub PURE_NO_SIDE_EFFECTS,
    Warn,
    "observable side effect inside a function marked #[pure]"
}

rustc_session::declare_lint_pass!(PureNoSideEffects => [PURE_NO_SIDE_EFFECTS]);

const PURE_MARKER: &str = "__logfwd_pure__";

/// Function-path prefixes whose call constitutes a side effect. A
/// prefix match is enough — e.g., any path starting with
/// `["std", "fs"]` is an `std::fs::*` call.
const IMPURE_PREFIX_PATHS: &[&[&str]] = &[
    &["std", "fs"],
    &["std", "net"],
    &["std", "env"],
    &["std", "process"],
    &["std", "thread"],
    &["std", "os"],
    // stdio
    &["std", "io", "stdin"],
    &["std", "io", "stdout"],
    &["std", "io", "stderr"],
    // clocks
    &["std", "time", "SystemTime"],
    &["std", "time", "Instant"],
    &["core", "time", "Instant"],
    // random
    &["rand"],
    &["rand_core"],
    &["rand_chacha"],
    &["rand_xoshiro"],
    // explicit raw-pointer memory ops commonly flagged in pure-review
    &["std", "ptr", "read"],
    &["std", "ptr", "write"],
    &["core", "ptr", "read"],
    &["core", "ptr", "write"],
];

impl<'tcx> LateLintPass<'tcx> for PureNoSideEffects {
    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        _kind: FnKind<'tcx>,
        _decl: &'tcx FnDecl<'tcx>,
        body: &'tcx Body<'tcx>,
        _span: Span,
        def_id: rustc_span::def_id::LocalDefId,
    ) {
        let hir_id = cx.tcx.local_def_id_to_hir_id(def_id);
        if !has_doc_marker(cx, hir_id, PURE_MARKER) {
            return;
        }

        let mut visitor = PureVisitor { cx, hits: Vec::new() };
        walk_body(&mut visitor, body);

        for (span, reason) in visitor.hits {
            span_lint_and_then(
                cx,
                PURE_NO_SIDE_EFFECTS,
                span,
                format!("side effect in #[pure] function: {reason}"),
                |_diag| {},
            );
        }
    }
}

struct PureVisitor<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    hits: Vec<(Span, String)>,
}

impl<'a, 'tcx> Visitor<'tcx> for PureVisitor<'a, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if let Some(reason) = describe_side_effect(self.cx, expr) {
            self.hits.push((expr.span.source_callsite(), reason));
        }
        walk_expr(self, expr);
    }
}

fn describe_side_effect<'tcx>(
    cx: &LateContext<'tcx>,
    expr: &Expr<'tcx>,
) -> Option<String> {
    match &expr.kind {
        ExprKind::Call(callee, _) => impure_call(cx, callee),
        ExprKind::MethodCall(..) => impure_method_call(cx, expr),
        ExprKind::Path(qpath) => static_mut_access(cx, qpath, expr.hir_id),
        _ => None,
    }
}

fn impure_call<'tcx>(cx: &LateContext<'tcx>, callee: &Expr<'tcx>) -> Option<String> {
    let ExprKind::Path(qpath) = &callee.kind else {
        return None;
    };
    let res = match qpath {
        QPath::Resolved(_, path) => path.res,
        QPath::TypeRelative(..) | QPath::LangItem(..) => {
            cx.typeck_results().qpath_res(qpath, callee.hir_id)
        }
    };
    let Res::Def(_, def_id) = res else { return None };
    let path = cx.get_def_path(def_id);
    for prefix in IMPURE_PREFIX_PATHS {
        if path_starts_with(&path, prefix) {
            let joined = path
                .iter()
                .map(|s| s.as_str().to_string())
                .collect::<Vec<_>>()
                .join("::");
            return Some(format!("call `{joined}`"));
        }
    }
    None
}

/// Detect method calls (e.g. `reader.read(...)`, `rng.gen()`) that
/// resolve to a side-effectful function based on their def-path.
fn impure_method_call<'tcx>(cx: &LateContext<'tcx>, expr: &Expr<'tcx>) -> Option<String> {
    let def_id = cx.typeck_results().type_dependent_def_id(expr.hir_id)?;
    let path = cx.get_def_path(def_id);
    for prefix in IMPURE_PREFIX_PATHS {
        if path_starts_with(&path, prefix) {
            let joined = path
                .iter()
                .map(|s| s.as_str().to_string())
                .collect::<Vec<_>>()
                .join("::");
            return Some(format!("method call `{joined}`"));
        }
    }
    None
}

fn static_mut_access(
    cx: &LateContext<'_>,
    qpath: &QPath<'_>,
    hir_id: rustc_hir::HirId,
) -> Option<String> {
    let res = match qpath {
        QPath::Resolved(_, path) => path.res,
        QPath::TypeRelative(..) | QPath::LangItem(..) => {
            cx.typeck_results().qpath_res(qpath, hir_id)
        }
    };
    // `static mut X: T = ...` resolves via `Res::Def(DefKind::Static { mutability: Mut, ... }, _)`.
    if let Res::Def(rustc_hir::def::DefKind::Static { mutability, .. }, def_id) = res
        && matches!(mutability, Mutability::Mut)
    {
        let path = cx.get_def_path(def_id);
        let joined = path
            .iter()
            .map(|s| s.as_str().to_string())
            .collect::<Vec<_>>()
            .join("::");
        return Some(format!("access to `static mut {joined}`"));
    }
    None
}

fn path_starts_with(actual: &[rustc_span::Symbol], prefix: &[&str]) -> bool {
    if actual.len() < prefix.len() {
        return false;
    }
    path_matches(&actual[..prefix.len()], prefix)
}
