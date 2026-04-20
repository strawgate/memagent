//! `no_panic_in_body`: flag panic macros, `.unwrap()`, `.expect()`,
//! and slice/array indexing inside functions marked
//! `#[logfwd_lint_attrs::no_panic]`.
//!
//! This is a shallow check — no transitive reachability across function
//! calls. Its value is scoped: the clippy lints
//! `clippy::panic`/`clippy::unwrap_used`/`clippy::indexing_slicing`
//! exist workspace-wide but are too noisy for the whole codebase.
//! `#[no_panic]` lets authors opt individual small functions into the
//! strict check without forcing it everywhere.

use clippy_utils::diagnostics::span_lint_and_then;
use clippy_utils::macros::root_macro_call_first_node;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{FnKind, Visitor, walk_body, walk_expr};
use rustc_hir::{Body, Expr, ExprKind, FnDecl, QPath};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;

use crate::lints::util::{has_doc_marker, path_matches};

rustc_session::declare_lint! {
    /// ### What it does
    ///
    /// Flags direct panic macros, `.unwrap()`, `.expect()`, and slice
    /// indexing in the body of a function marked with
    /// `#[logfwd_lint_attrs::no_panic]`. Does not perform transitive
    /// reachability analysis — functions called from the body are not
    /// inspected.
    ///
    /// ### Why is this bad?
    ///
    /// Some small, audited functions (parsers, state transitions,
    /// encoders) carry a review-level "this will not panic" guarantee.
    /// Marking them `#[no_panic]` makes that guarantee mechanically
    /// enforced for obvious panic sources, so it does not silently
    /// rot as the function grows.
    pub NO_PANIC_IN_BODY,
    Warn,
    "direct panic source inside a function marked #[no_panic]"
}

rustc_session::declare_lint_pass!(NoPanicInBody => [NO_PANIC_IN_BODY]);

const NO_PANIC_MARKER: &str = "__logfwd_no_panic__";

/// Function paths whose call is treated as a direct panic source.
const PANIC_FN_PATHS: &[&[&str]] = &[
    &["core", "panicking", "panic"],
    &["core", "panicking", "panic_fmt"],
    &["std", "panicking", "panic"],
    &["core", "option", "unwrap_failed"],
    &["core", "result", "unwrap_failed"],
];

/// Method names whose call on *any* receiver is treated as a panic
/// source in this context. `.unwrap()` and `.expect()` dominate; the
/// rest are common "or-die" shorthands.
const PANIC_METHOD_NAMES: &[&str] = &[
    "unwrap",
    "expect",
    "unwrap_or_default_infallible", // placeholder for future additions
];

/// Macros we flag. Matched by their `core`/`std` lang-item names at
/// call-resolution time.
const PANIC_MACRO_FN_PATHS: &[&[&str]] = &[
    // Fully-qualified paths of the panic-ish intrinsics that macros
    // expand into. The actual `panic!` / `todo!` / `unimplemented!` /
    // `unreachable!` macros expand to one of these functions under the
    // hood.
    &["core", "panicking", "panic_explicit"],
    &["core", "panicking", "panic_display"],
    &["core", "panicking", "panic_nounwind"],
    &["core", "panicking", "const_panic_fmt"],
];

impl<'tcx> LateLintPass<'tcx> for NoPanicInBody {
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
        if !has_doc_marker(cx, hir_id, NO_PANIC_MARKER) {
            return;
        }

        let mut visitor = PanicVisitor { cx, hits: Vec::new() };
        walk_body(&mut visitor, body);

        for (span, reason) in visitor.hits {
            span_lint_and_then(
                cx,
                NO_PANIC_IN_BODY,
                span,
                format!("panic source in #[no_panic] function: {reason}"),
                |_diag| {},
            );
        }
    }
}

struct PanicVisitor<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    hits: Vec<(Span, String)>,
}

impl<'a, 'tcx> Visitor<'tcx> for PanicVisitor<'a, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if let Some(reason) = describe_panic(self.cx, expr) {
            // Use the user-visible call site, not whatever internal
            // macro-body position the expression span points at.
            self.hits.push((expr.span.source_callsite(), reason));
            // Don't recurse into macro expansions — the macro itself
            // is the panic source we want to report. Walking into the
            // expanded HIR would match intrinsics like
            // `core::panicking::panic_display`, double-counting the
            // same source location.
            return;
        }
        walk_expr(self, expr);
    }
}

/// Panic-inducing macros whose presence as the root expansion of an
/// expression we flag.
const PANIC_MACRO_NAMES: &[&str] = &[
    "panic",
    "todo",
    "unimplemented",
    "unreachable",
    "assert",
    "assert_eq",
    "assert_ne",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_ne",
];

fn describe_panic<'tcx>(cx: &LateContext<'tcx>, expr: &Expr<'tcx>) -> Option<String> {
    // First: is this expression the root of a panic-inducing macro
    // expansion? Catches `panic!`/`todo!`/`unimplemented!`/`unreachable!`
    // regardless of which intrinsic they internally call.
    if let Some(mac) = root_macro_call_first_node(cx, expr) {
        let name = cx.tcx.item_name(mac.def_id);
        if PANIC_MACRO_NAMES.contains(&name.as_str()) {
            return Some(format!("`{name}!` macro"));
        }
    }
    match &expr.kind {
        ExprKind::Call(callee, _) => resolved_path_matches_panic(cx, callee),
        ExprKind::MethodCall(path, _recv, _args, _span) => {
            let name = path.ident.name.as_str();
            if PANIC_METHOD_NAMES.contains(&name) {
                return Some(format!("method `.{name}()`"));
            }
            None
        }
        ExprKind::Index(..) => Some(
            "slice/array indexing (`x[i]`) can panic on out-of-bounds — use `.get(i)` or a \
             bounded iterator"
                .to_string(),
        ),
        _ => None,
    }
}

fn resolved_path_matches_panic<'tcx>(
    cx: &LateContext<'tcx>,
    callee: &Expr<'tcx>,
) -> Option<String> {
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
    for candidate in PANIC_FN_PATHS.iter().chain(PANIC_MACRO_FN_PATHS.iter()) {
        if path_matches(&path, candidate) {
            return Some(format!("panic intrinsic `{}`", candidate.join("::")));
        }
    }
    // Fallback: any path ending in `panicking::<something>` where the
    // final segment starts with `panic` or equals `unwrap_failed`. This
    // catches private/moved paths across std/core releases.
    if path.len() >= 2 {
        let penultimate = path[path.len() - 2].as_str();
        let last = path[path.len() - 1].as_str();
        if penultimate == "panicking"
            && (last.starts_with("panic") || last == "unwrap_failed" || last == "assert_failed")
        {
            return Some(format!(
                "panic intrinsic `...::{}::{}`",
                penultimate, last
            ));
        }
    }
    None
}
