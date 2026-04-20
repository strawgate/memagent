//! Attribute macros that tag functions for semantic lints.
//!
//! Each attribute is a compile-time no-op: it returns its input token
//! stream unchanged. The dylint lint library (`crates/logfwd-lints/`)
//! inspects the attributes post-expansion to enforce the invariant they
//! assert.
//!
//! # Available attributes
//!
//! - `#[hot_path]` — the tagged function is on the pipeline's hot path.
//!   The `hot_path_no_alloc` lint flags any heap allocation inside the
//!   function body (direct calls to `Box::new`, `Vec::new`,
//!   `Vec::with_capacity`, `String::from`, `.to_string()`, `.to_vec()`,
//!   `.to_owned()`, `format!`, `vec![]`, `.collect()` into an owned
//!   container, `Arc::new`, `Rc::new`).
//!
//! # Runtime behavior
//!
//! These attributes generate no code. They exist purely so the lint can
//! find them. Functions behave identically with or without the attribute
//! at runtime.

#![warn(missing_docs)]

use proc_macro::TokenStream;

/// Mark a function as lying on the pipeline hot path.
///
/// The `hot_path_no_alloc` dylint lint flags any heap allocation inside
/// a function carrying this attribute. See crate-level docs.
///
/// The macro prepends a hidden doc attribute
/// `#[doc = "__logfwd_hot_path__"]` so the lint can find the function
/// after macro expansion. At runtime the function behaves identically
/// to the un-annotated version; the doc attribute has no codegen effect.
#[proc_macro_attribute]
pub fn hot_path(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let marker: TokenStream = "#[doc = \"__logfwd_hot_path__\"]"
        .parse()
        .expect("valid marker attribute");
    let mut out = marker;
    out.extend(item);
    out
}
