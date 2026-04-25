//! `hot_path_no_alloc`: flag heap allocations in functions marked
//! `#[ffwd_lint_attrs::hot_path]`.

use clippy_utils::diagnostics::span_lint_and_then;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{FnKind, Visitor, walk_body, walk_expr};
use rustc_hir::{Body, Expr, ExprKind, FnDecl, QPath};
use rustc_lint::{LateContext, LateLintPass};
use rustc_middle::ty::Ty;
use rustc_span::{Span, Symbol};

use crate::lints::util::{has_doc_marker, path_matches};

rustc_session::declare_lint! {
    /// ### What it does
    ///
    /// Flags heap allocations and owned-value conversions inside functions
    /// marked with `#[ffwd_lint_attrs::hot_path]`.
    ///
    /// ### Why is this bad?
    ///
    /// Hot-path code in ffwd (reader → framer → scanner → builders → OTLP
    /// encoder → compress) must be allocation-free per-record. Allocation
    /// there costs throughput proportional to input volume. The attribute
    /// exists to declare the no-alloc contract explicitly so drift is
    /// caught at review time rather than in production profiles.
    pub HOT_PATH_NO_ALLOC,
    Warn,
    "heap allocation inside a function marked #[hot_path]"
}

rustc_session::declare_lint_pass!(HotPathNoAlloc => [HOT_PATH_NO_ALLOC]);

const HOT_PATH_MARKER: &str = "__ffwd_hot_path__";

/// Function paths whose call constitutes a heap allocation.
const ALLOC_FN_PATHS: &[&[&str]] = &[
    &["alloc", "boxed", "Box", "new"],
    &["alloc", "vec", "Vec", "new"],
    &["alloc", "vec", "Vec", "with_capacity"],
    &["alloc", "string", "String", "new"],
    &["alloc", "string", "String", "with_capacity"],
    &["alloc", "string", "String", "from"],
    &["alloc", "sync", "Arc", "new"],
    &["alloc", "rc", "Rc", "new"],
    &["std", "collections", "hash", "map", "HashMap", "new"],
    &["std", "collections", "hash", "map", "HashMap", "with_capacity"],
    &["std", "collections", "hash", "set", "HashSet", "new"],
    &["std", "collections", "hash", "set", "HashSet", "with_capacity"],
];

/// Method-name allocation tells: any receiver plus one of these method
/// names signals a heap write.
const ALLOC_METHOD_NAMES: &[&str] = &[
    "to_string",
    "to_vec",
    "to_owned",
    "into_owned",
    "into_boxed_slice",
    "into_boxed_str",
    "into_vec",
    "clone_into",
];

impl<'tcx> LateLintPass<'tcx> for HotPathNoAlloc {
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
        if !has_doc_marker(cx, hir_id, HOT_PATH_MARKER) {
            return;
        }

        let mut visitor = AllocVisitor { cx, hits: Vec::new() };
        walk_body(&mut visitor, body);

        for (span, reason) in visitor.hits {
            span_lint_and_then(
                cx,
                HOT_PATH_NO_ALLOC,
                span,
                format!("heap allocation in #[hot_path] function: {reason}"),
                |_diag| {},
            );
        }
    }
}

struct AllocVisitor<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    hits: Vec<(Span, String)>,
}

impl<'a, 'tcx> Visitor<'tcx> for AllocVisitor<'a, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if let Some(reason) = describe_alloc(self.cx, expr) {
            self.hits.push((expr.span, reason));
        }
        walk_expr(self, expr);
    }
}

fn describe_alloc<'tcx>(cx: &LateContext<'tcx>, expr: &Expr<'tcx>) -> Option<String> {
    match &expr.kind {
        ExprKind::Call(callee, _) => resolved_path_matches_alloc(cx, callee),
        ExprKind::MethodCall(path, _receiver, _args, _span) => {
            let name = path.ident.name.as_str();
            if ALLOC_METHOD_NAMES.contains(&name) {
                return Some(format!("method `.{name}()`"));
            }
            if name == "clone" {
                let receiver_ty = cx.typeck_results().expr_ty(expr);
                if type_is_heap(cx, receiver_ty) {
                    return Some(format!("`.clone()` on heap type `{receiver_ty}`"));
                }
            }
            if name == "collect" {
                let ty = cx.typeck_results().expr_ty(expr);
                if type_is_heap(cx, ty) {
                    return Some(format!("`.collect()` into heap type `{ty}`"));
                }
            }
            None
        }
        _ => None,
    }
}

fn resolved_path_matches_alloc<'tcx>(
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
    for candidate in ALLOC_FN_PATHS {
        if path_matches(&path, candidate) {
            return Some(format!("call `{}`", candidate.join("::")));
        }
    }
    None
}

fn type_is_heap<'tcx>(cx: &LateContext<'tcx>, ty: Ty<'tcx>) -> bool {
    use rustc_middle::ty::TyKind;
    match ty.kind() {
        TyKind::Adt(adt, _) => {
            let path = cx.get_def_path(adt.did());
            let lang = path.iter().map(Symbol::as_str).collect::<Vec<_>>();
            matches!(
                lang.as_slice(),
                ["alloc", "string", "String"]
                    | ["alloc", "vec", "Vec"]
                    | ["alloc", "boxed", "Box"]
                    | ["alloc", "sync", "Arc"]
                    | ["alloc", "rc", "Rc"]
                    | ["std", "collections", "hash", "map", "HashMap"]
                    | ["std", "collections", "hash", "set", "HashSet"]
            )
        }
        _ => false,
    }
}
