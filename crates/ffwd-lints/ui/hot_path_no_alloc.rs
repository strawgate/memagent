// UI test: every `should_fire` function should trigger
// `hot_path_no_alloc`. Functions without the attribute, or without an
// allocation, must stay silent.

// A local macro that replicates what the real `ffwd_lint_attrs::hot_path`
// proc-macro does: prepend a `#[doc = "__ffwd_hot_path__"]` marker so
// the lint can detect annotated functions.
macro_rules! hot_path {
    ($(#[$m:meta])* $vis:vis fn $name:ident $($rest:tt)*) => {
        #[doc = "__ffwd_hot_path__"]
        $(#[$m])* $vis fn $name $($rest)*
    };
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

// Should fire: tagged and allocates via Vec::with_capacity.
hot_path! {
fn tagged_alloc() -> usize {
    let v: Vec<u8> = Vec::with_capacity(16);
    v.len()
}
}

// Should fire: tagged, .to_string() on a &str.
hot_path! {
fn tagged_to_string(s: &str) -> usize {
    s.to_string().len()
}
}

// Should fire: tagged, format!() allocates.
hot_path! {
fn tagged_format(x: u32) -> String {
    format!("value: {x}")
}
}

// Should NOT fire: tagged but Arc::clone is just a refcount bump.
hot_path! {
fn tagged_arc_clone(a: &std::sync::Arc<String>) -> std::sync::Arc<String> {
    std::sync::Arc::clone(a)
}
}

// Should NOT fire: tagged but no allocations.
hot_path! {
fn tagged_no_alloc(buf: &[u8]) -> usize {
    buf.len()
}
}

// Should fire: tagged, .clone() on String allocates.
hot_path! {
fn tagged_clone_string(s: &String) -> String {
    s.clone()
}
}

// Should fire: tagged, .collect() into Vec.
hot_path! {
fn tagged_collect(s: &[u8]) -> Vec<u8> {
    s.iter().copied().collect()
}
}
