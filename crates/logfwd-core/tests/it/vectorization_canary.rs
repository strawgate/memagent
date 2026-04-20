// vectorization_canary.rs — Verify that key functions are auto-vectorized by LLVM.
//
// Extracts find_char_mask from the actual structural.rs source, wraps it
// in a standalone .rs file, compiles with LLVM vectorization remarks enabled,
// and asserts that LLVM reports "vectorized loop".
//
// This catches code changes that accidentally break auto-vectorization
// (e.g., adding early exits or non-trivial loop-carried dependencies).
//
// NOTE: This test checks that LLVM *can* vectorize the function in isolation.
// Under thin LTO, the function may be inlined into a larger context where
// LLVM's cost model skips vectorization — that's a separate (known) issue.

use std::process::Command;

/// Extract the body of `find_char_mask` from structural.rs source.
///
/// Looks for `fn find_char_mask(` and captures everything through the
/// matching closing brace. Returns a standalone .rs file that rustc can compile.
fn build_canary_source() -> String {
    let src = include_str!("../../src/structural.rs");

    let fn_start = src
        .find("fn find_char_mask(")
        .expect("find_char_mask not found in structural.rs");

    // Walk forward from fn_start to find the matching closing brace.
    let body = &src[fn_start..];
    let mut depth = 0u32;
    let mut end = 0;
    for (i, ch) in body.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = i + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(end > 0, "could not find closing brace of find_char_mask");

    let func = &body[..end];

    // Wrap in a compilable file. Make it pub + #[inline(never)] + #[no_mangle]
    // so rustc doesn't DCE it and LLVM optimizes it as a standalone function.
    let func = func.replacen("#[inline]", "", 1);

    format!(
        "#[inline(never)]\n\
         #[unsafe(no_mangle)]\n\
         pub {func}\n\
         fn main() {{}}\n"
    )
}

#[test]
fn find_char_mask_is_vectorized() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let src_path = dir.path().join("canary.rs");
    let canary_src = build_canary_source();
    std::fs::write(&src_path, &canary_src).expect("write source");

    let output = Command::new("rustc")
        .args([
            "-O",
            "--edition",
            "2024",
            "-C",
            "llvm-args=-pass-remarks=loop-vectorize",
            src_path.to_str().unwrap(),
            "-o",
        ])
        .arg(dir.path().join("canary"))
        .output()
        .expect("run rustc");

    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "rustc failed to compile canary:\n{stderr}\n\nsource:\n{canary_src}"
    );

    // LLVM emits "vectorized loop (vectorization width: N, interleaved count: M)"
    // when it successfully auto-vectorizes a loop.
    assert!(
        stderr.contains("vectorized loop"),
        "LLVM did not auto-vectorize find_char_mask.\n\
         This means a code change broke the vectorization pattern.\n\
         The loop should be branchless with no early exits.\n\
         \n\
         compiled source:\n{canary_src}\n\
         rustc stderr:\n{stderr}"
    );
}
