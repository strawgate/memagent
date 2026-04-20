//! Attribute macros that tag functions for semantic lints.
//!
//! Each attribute is a compile-time no-op: it prepends a hidden
//! `#[doc = "__logfwd_<name>__"]` marker to its input and returns the
//! result. The dylint lint library (`crates/logfwd-lints/`) finds the
//! marker post-expansion to enforce the invariant each attribute asserts.
//!
//! Proc-macro attributes are consumed during expansion (the `#[hot_path]`
//! token itself vanishes), so the marker-via-doc-attribute is the portable
//! way to carry the annotation into HIR without requiring nightly tool
//! attributes.
//!
//! # Available attributes
//!
//! | Attribute | Lint | Target | Asserts |
//! |---|---|---|---|
//! | `#[hot_path]` | `hot_path_no_alloc` | fn | No heap allocation in the function body. |
//! | `#[cancel_safe]` | `cancel_safe_no_lock_across_await` | async fn | No `MutexGuard`/`RwLock*Guard`/`RefCell` ref (std, parking_lot, tokio::sync, lock_api) live across `.await` per MIR coroutine layout. |
//! | `#[no_panic]` | `no_panic_in_body` | fn | No direct panic macros, no `.unwrap()`/`.expect()`, and no slice/array indexing in the function body. |
//! | `#[pure]` | `pure_no_side_effects` | fn | No IO (`std::fs`/`std::net`/stdio), no env/process/thread, no clocks, no `static mut` access, no `rand::*`. Shallow check. |
//! | `#[owned_by_actor]` | `owned_by_actor_no_spawn_capture` | struct / enum | Values of the tagged type never cross into `tokio::spawn`/`spawn_blocking`/`spawn_local` or `std::thread::spawn` closures. |
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
#[proc_macro_attribute]
pub fn hot_path(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_hot_path__", item)
}

/// Mark an async function as cancel-safe.
///
/// The `cancel_safe_no_lock_across_await` dylint lint flags any lock
/// guard held across a `.await` in a function carrying this attribute.
/// Awaits in a `tokio::select!` branch can drop the future at the
/// `.await` point; if the dropped future is holding a `Mutex` guard,
/// the mutex stays locked (or worse, deadlocks with the task that was
/// supposed to release it). Marking a function `#[cancel_safe]` is an
/// author-asserted contract; the lint enforces the lock-across-await
/// piece mechanically.
///
/// This attribute does *not* guarantee the async operations called
/// inside are themselves cancel-safe — that remains a review-level
/// concern for now (see the tokio `select!` cancel-safety table).
#[proc_macro_attribute]
pub fn cancel_safe(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_cancel_safe__", item)
}

/// Mark a function as panic-free.
///
/// The `no_panic_in_body` dylint lint flags direct panic macros
/// (`panic!`, `todo!`, `unimplemented!`, `unreachable!`), `.unwrap()`,
/// `.expect()`, and slice/array indexing expressions in the function
/// body. It does *not* perform transitive reachability analysis —
/// functions called from the body are not inspected.
///
/// Use this on small, audited pure-logic functions (parsers, state
/// transitions, encoders) where the review guarantee "this function
/// itself will never panic" is valuable even without transitive proof.
#[proc_macro_attribute]
pub fn no_panic(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_no_panic__", item)
}

/// Mark a function as pure (no observable side effects).
///
/// The `pure_no_side_effects` dylint lint flags calls in the function
/// body to IO (`std::fs`, `std::net`, `std::io::stdin`/`stdout`/`stderr`),
/// process/env/thread APIs (`std::env`, `std::process`, `std::thread`),
/// clocks (`Instant::now`, `SystemTime::now`), `std::os::*`, and access
/// to any `static mut`. Random-number sources from `rand::*` are also
/// flagged.
///
/// Like `#[no_panic]`, the check is shallow — functions called from the
/// body are not inspected. Use on small computational helpers
/// (parsers, encoders, state transitions) that must be deterministic
/// and side-effect-free for testability and verification.
#[proc_macro_attribute]
pub fn pure(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_pure__", item)
}

/// Mark a type as owned by a single actor (task/thread).
///
/// The `owned_by_actor_no_spawn_capture` dylint lint flags
/// `tokio::spawn`, `tokio::task::spawn_blocking`, `tokio::task::spawn_local`,
/// and `std::thread::spawn` call sites whose closure captures a value
/// whose type carries this attribute. The attribute goes on the type
/// definition (`struct`/`enum`), not on a function.
///
/// Use this to keep single-ownership actor boundaries explicit:
/// checkpoint-store state, input-source session state, connection
/// pools that aren't designed to be shared, etc. Instead of relying on
/// `!Send` bounds (which can be hard to read on trait objects), the
/// attribute + lint makes the contract visible at the type definition
/// and catches captures at spawn call sites mechanically.
///
/// Note: this lint only detects captures in inline closures and async
/// blocks passed directly to spawn; it does not track stored futures
/// that are later passed to spawn.
#[proc_macro_attribute]
pub fn owned_by_actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    prepend_marker("__logfwd_owned_by_actor__", item)
}

fn prepend_marker(marker: &str, item: TokenStream) -> TokenStream {
    let attr: TokenStream = format!("#[doc = \"{marker}\"]")
        .parse()
        .expect("valid marker attribute");
    let mut out = attr;
    out.extend(item);
    out
}

// ---------------------------------------------------------------------------
// Compile-time verification linkage attributes
// ---------------------------------------------------------------------------

use quote::{format_ident, quote};
use syn::{
    ItemFn, LitStr, MetaNameValue, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

struct VerifiedArgs {
    kani: Option<LitStr>,
    proptest: Option<LitStr>,
}

impl Parse for VerifiedArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let pairs = Punctuated::<MetaNameValue, Token![,]>::parse_terminated(input)?;
        let mut kani = None;
        let mut proptest = None;

        for pair in pairs {
            if pair.path.is_ident("kani") {
                match pair.value {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(value),
                        ..
                    }) => {
                        kani = Some(value);
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            pair,
                            "expected string literal: kani = \"verify_name\"",
                        ));
                    }
                }
            } else if pair.path.is_ident("proptest") {
                match pair.value {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(value),
                        ..
                    }) => {
                        proptest = Some(value);
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            pair,
                            "expected string literal: proptest = \"test_name\"",
                        ));
                    }
                }
            } else {
                return Err(syn::Error::new_spanned(
                    pair.path,
                    "unsupported key, expected `kani` or `proptest`",
                ));
            }
        }

        if kani.is_none() && proptest.is_none() {
            return Err(syn::Error::new(
                input.span(),
                "expected at least one linkage: kani = \"...\" or proptest = \"...\"",
            ));
        }

        Ok(Self { kani, proptest })
    }
}

/// Marks a function as proof-verified.
///
/// `#[verified(kani = "verify_function_name")]` emits a compile-time check
/// under `cfg(kani)` that references `self::verification::<proof_name>`.
/// If the proof function does not exist, compilation fails at the annotation site.
///
/// Also accepts `proptest = "test_name"` to emit a `#[cfg(test)]` doc-marker
/// for xtask/structural tooling.
#[proc_macro_attribute]
pub fn verified(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as VerifiedArgs);
    let function = parse_macro_input!(item as ItemFn);
    let function_name = function.sig.ident.clone();
    let marker_ident = format_ident!("__verified_marker_{}", function_name);

    let kani_assertion = if let Some(kani_name) = args.kani {
        let kani_proof = format_ident!("{}", kani_name.value());
        quote! {
            #[cfg(kani)]
            #[doc(hidden)]
            #[allow(non_upper_case_globals)]
            const #marker_ident: () = {
                let _proof: fn() = self::verification::#kani_proof;
            };
        }
    } else {
        quote! {}
    };

    let proptest_marker = if let Some(test_name) = args.proptest {
        let value = test_name.value();
        quote! {
            #[cfg(test)]
            #[doc(hidden)]
            const _: &str = concat!("proptest:", #value);
        }
    } else {
        quote! {}
    };

    TokenStream::from(quote! {
        #function
        #kani_assertion
        #proptest_marker
    })
}

/// Marker attribute for functions that process untrusted input.
///
/// Emits a hidden const marker for xtask/lint tooling. No runtime effect.
#[proc_macro_attribute]
pub fn trust_boundary(_args: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    let function_name = function.sig.ident.clone();
    let marker_ident = format_ident!("__trust_boundary_marker_{}", function_name);

    TokenStream::from(quote! {
        #function
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        const #marker_ident: &str = concat!("trust_boundary:", stringify!(#function_name));
    })
}

/// Marker attribute for explicit exemptions from proof linkage checks.
///
/// Documents that a function intentionally lacks a Kani proof (e.g.,
/// internal helpers, decode paths where proptest provides coverage).
/// No runtime effect.
#[proc_macro_attribute]
pub fn allow_unproven(_args: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    let function_name = function.sig.ident.clone();
    let marker_ident = format_ident!("__allow_unproven_marker_{}", function_name);

    TokenStream::from(quote! {
        #function
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        const #marker_ident: &str = concat!("allow_unproven:", stringify!(#function_name));
    })
}
