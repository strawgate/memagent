//! Shared helpers used across lints.

use rustc_hir::HirId;
use rustc_lint::LateContext;
use rustc_span::Symbol;

/// Returns true if the item identified by `hir_id` carries a
/// `#[doc = "<marker>"]` attribute. Proc-macro attributes in
/// `logfwd-lint-attrs` prepend these markers so the lint can find
/// tagged items after macro expansion strips the attribute itself.
pub(crate) fn has_doc_marker(cx: &LateContext<'_>, hir_id: HirId, marker: &str) -> bool {
    let doc_sym = Symbol::intern("doc");
    for attr in cx.tcx.hir_attrs(hir_id) {
        if !attr.has_name(doc_sym) {
            continue;
        }
        if let Some(value) = attr.value_str()
            && value.as_str() == marker
        {
            return true;
        }
    }
    false
}

/// Returns true when `actual` (a def-path as `&[Symbol]`) matches
/// `expected` segment-by-segment. Used to identify known types by
/// their fully-qualified crate path.
#[inline]
pub(crate) fn path_matches(actual: &[Symbol], expected: &[&str]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(a, e)| a.as_str() == *e)
}
