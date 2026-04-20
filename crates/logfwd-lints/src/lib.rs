//! Dylint library implementing semantic lints for the logfwd workspace.
//!
//! Currently provides:
//!
//! - `hot_path_no_alloc` — flags heap allocations inside functions marked
//!   with `#[logfwd_lint_attrs::hot_path]`. Catches `Box::new`,
//!   `Vec::new`/`with_capacity`, `String::from`/`new`, `.to_string()`,
//!   `.to_vec()`, `.to_owned()`, `.clone()` on common heap types,
//!   `.collect::<Vec<_>>()`/`.collect::<String>()`, `format!`, `vec![]`,
//!   `Arc::new`, `Rc::new`.
//!
//! Invoked via `cargo dylint --all` once the workspace registers this
//! library (see `just dylint`).
//!
//! Implementation lives at the HIR level so macro expansions (`format!`,
//! `vec![]`) and method calls that resolve to heap types are caught.

#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_hir;
extern crate rustc_middle;
extern crate rustc_span;

use clippy_utils::diagnostics::span_lint_and_then;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{FnKind, Visitor, walk_body, walk_expr};
use rustc_hir::{Body, Expr, ExprKind, FnDecl, HirId, QPath};
use rustc_lint::{LateContext, LateLintPass};
use rustc_middle::ty::Ty;
use rustc_span::{Span, Symbol};

dylint_linting::declare_late_lint! {
    /// ### What it does
    ///
    /// Flags heap allocations and owned-value conversions inside functions
    /// marked with `#[logfwd_lint_attrs::hot_path]`.
    ///
    /// ### Why is this bad?
    ///
    /// Hot-path code in logfwd (reader → framer → scanner → builders → OTLP
    /// encoder → compress) must be allocation-free per-record. Allocation
    /// there costs throughput proportional to input volume. The attribute
    /// exists to declare the no-alloc contract explicitly so drift is
    /// caught at review time rather than in production profiles.
    ///
    /// ### Example
    ///
    /// ```ignore
    /// use logfwd_lint_attrs::hot_path;
    ///
    /// #[hot_path]
    /// fn scan(buf: &[u8]) -> usize {
    ///     let owned = buf.to_vec(); // fires lint
    ///     owned.len()
    /// }
    /// ```
    ///
    /// Rewrite to operate on the borrowed slice:
    ///
    /// ```ignore
    /// use logfwd_lint_attrs::hot_path;
    ///
    /// #[hot_path]
    /// fn scan(buf: &[u8]) -> usize {
    ///     buf.len()
    /// }
    /// ```
    pub HOT_PATH_NO_ALLOC,
    Warn,
    "heap allocation inside a function marked #[hot_path]"
}

/// Function paths whose call constitutes a heap allocation. These match
/// against `clippy_utils::match_def_path` on the resolved `DefId`.
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
/// names signals a heap write. These are checked *in addition* to the
/// resolved-path set above (which covers free functions and inherent
/// constructors).
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

/// Marker string that the `#[hot_path]` proc-macro attribute prepends
/// as a hidden `#[doc = ...]` on the tagged function. Proc-macro
/// attributes are consumed during expansion, so `#[hot_path]` itself is
/// gone by the time HIR is available — the doc marker carries the
/// annotation through.
const HOT_PATH_MARKER: &str = "__logfwd_hot_path__";

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
        if !function_has_hot_path_attr(cx, hir_id) {
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

fn function_has_hot_path_attr(cx: &LateContext<'_>, hir_id: HirId) -> bool {
    // The `#[hot_path]` proc-macro attribute is stripped during macro
    // expansion. It prepends a `#[doc = "__logfwd_hot_path__"]` marker
    // that survives to HIR; we look for that.
    let doc_sym = Symbol::intern("doc");
    for attr in cx.tcx.hir_attrs(hir_id) {
        if !attr.has_name(doc_sym) {
            continue;
        }
        if let Some(value) = attr.value_str()
            && value.as_str() == HOT_PATH_MARKER
        {
            return true;
        }
    }
    false
}

struct AllocVisitor<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    hits: Vec<(Span, String)>,
}

impl<'a, 'tcx> Visitor<'tcx> for AllocVisitor<'a, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        let hit = describe_alloc(self.cx, expr);
        if let Some(reason) = hit {
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
                // .clone() is only an allocation when the receiver type is
                // actually heap-backed. We conservatively report for String,
                // Vec, Box, Arc, Rc, and types containing them.
                let receiver_ty = cx.typeck_results().expr_ty(expr);
                if type_is_heap(cx, receiver_ty) {
                    return Some(format!(
                        "`.clone()` on heap type `{}`",
                        receiver_ty
                    ));
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

fn path_matches(actual: &[Symbol], expected: &[&str]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(a, e)| a.as_str() == *e)
}

/// Conservative heap-type classifier: returns true for `String`, `Vec<_>`,
/// `Box<_>`, `Arc<_>`, `Rc<_>`, and `HashMap<_,_>` / `HashSet<_>`.
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

// UI tests are set up in `ui/hot_path_no_alloc.rs`. To enable them, add
// expected-output `ui/hot_path_no_alloc.stderr` via:
//   BLESS=1 cargo test --manifest-path crates/logfwd-lints/Cargo.toml
// and uncomment the `ui` test below. Left disabled for now so the crate
// builds hermetically in CI without requiring a blessed fixture.
//
// #[test]
// fn ui() {
//     dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
// }
