// UI test: every ~should_fire~ function should trigger
// `hot_path_no_alloc`. Functions without the attribute, or without an
// allocation, must stay silent.

// Re-export the attribute under its expected path. The UI harness
// compiles this file standalone against a stub `logfwd_lint_attrs` crate
// that `dylint_testing` auto-stubs via `#[allow]` when not present. To
// keep the test hermetic we declare the attribute locally as an external
// attribute namespace.

#![feature(register_tool)]
#![register_tool(logfwd_lint_attrs)]

// A no-op attribute reimplemented locally so the UI test does not
// depend on the real `logfwd_lint_attrs` crate compiling against the
// dylint nightly toolchain.
macro_rules! hot_path {
    ($($t:tt)*) => { $($t)* };
}

fn main() {}

// Should NOT fire: no attribute.
fn untagged() -> Vec<u8> {
    Vec::with_capacity(16)
}

// Should NOT fire: untagged, even though there's an allocation.
fn untagged_string() -> String {
    "hello".to_string()
}

// Should fire: tagged and allocates.
#[logfwd_lint_attrs::hot_path]
fn tagged_alloc() -> usize {
    let v: Vec<u8> = Vec::with_capacity(16);
    v.len()
}

// Should fire: tagged, .to_string() on a &str.
#[logfwd_lint_attrs::hot_path]
fn tagged_to_string(s: &str) -> usize {
    s.to_string().len()
}
