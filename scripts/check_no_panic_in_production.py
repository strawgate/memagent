#!/usr/bin/env python3
"""Fail if `panic!`, `todo!`, or `unimplemented!` appears outside test code
in production crates (`logfwd-runtime`, `logfwd-output`).

A production panic crashes the running pipeline. These macros are
acceptable in tests, under a `// ALLOW-PANIC: <reason>` line marker, or
inside any `#[cfg(test)]` module or `#[test]`-annotated function (including
parameterized forms like `#[tokio::test(flavor = "multi_thread")]`).

This is a conservative regex-based AST walk. It is a best-effort guard,
not a full parser; for anything subtler than these patterns, see the
dylint roadmap in `dev-docs/CODE_STYLE.md`.

Heuristics:
  * Tracks brace depth across the file.
  * A `#[cfg(test)]` preceding a `mod ... {`, `fn ... {`, or `impl ... {`
    opens a test scope inherited by all nested items.
  * `#[test]`, `#[tokio::test]`, `#[tokio::test(...)]`, `#[proptest]`,
    `#[proptest(...)]`, and any `*::test` (parameterized or not) flag the
    following scope as test.
  * `#[cfg(test)] mod name;` (external file declaration) marks the
    referenced file (either `name.rs` or `name/mod.rs` sibling) as
    entirely test scope.
  * `#![cfg(test)]` at the top of a file marks the whole file as test.
  * `unreachable!` is allowed (expresses an invariant, not a panic).
  * Same-line comment `// ALLOW-PANIC: <reason>` opts a single line out.

See `dev-docs/CODE_STYLE.md` -> Error Handling.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parent.parent
CRATES_ROOT = REPO_ROOT / "crates"

# Production crates whose src/ is subject to the guard. Tests/benches
# in these crates are still allowed via the in-file test-context tracking.
GUARDED_CRATES = ("logfwd-runtime", "logfwd-output")

# Macros that constitute a hard production panic.
PANIC_MACROS = re.compile(r"\b(panic|todo|unimplemented)!\s*\(")

# Test attributes that designate a following `mod`/`fn`/`impl` as test
# scope. The optional `(...)` allows parameterized forms like
# `#[tokio::test(flavor = "multi_thread")]` and `#[proptest(cases = 100)]`.
TEST_ATTR = re.compile(
    r"^[ \t]*#\[\s*"
    r"(?:cfg\s*\(\s*test\s*\)"
    r"|test"
    r"|proptest"
    r"|tracing_test::traced_test"
    r"|[A-Za-z_][A-Za-z_0-9]*(?:::[A-Za-z_][A-Za-z_0-9]*)*::test"
    r")"
    r"(?:\s*\([^)]*\))?"
    r"\s*[\],]"
)

# File-level inner attribute marking the whole file as test-only.
INNER_CFG_TEST = re.compile(r"^[ \t]*#!\[\s*cfg\s*\(\s*test\s*\)\s*\]")

# External test module declaration: `#[cfg(test)] mod name;`. Captured
# across two lines in the pre-scan.
EXT_TEST_MOD = re.compile(
    r"^[ \t]*#\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*\n"
    r"[ \t]*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z_0-9]*)\s*;",
    re.MULTILINE,
)

ALLOW_MARKER = "// ALLOW-PANIC:"


def relpath(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def collect_files() -> list[Path]:
    return [
        path
        for crate in GUARDED_CRATES
        if (crate_src := CRATES_ROOT / crate / "src").exists()
        for path in sorted(crate_src.rglob("*.rs"))
    ]


def strip_strings_and_line_comment(line: str) -> str:
    """Remove string literals first (so `//` inside a string is not
    mistaken for a comment), then drop trailing `//` comments. The
    order is load-bearing: reversing it hides any panic that follows
    a string containing `//`.
    """
    line = re.sub(r'"(?:[^"\\]|\\.)*"', '""', line)
    line = re.sub(r"'(?:[^'\\]|\\.)*'", "''", line)
    comment_idx = line.find("//")
    if comment_idx != -1:
        line = line[:comment_idx]
    return line


def collect_external_test_files(files: list[Path]) -> set[Path]:
    """Pre-scan: find `#[cfg(test)] mod name;` declarations in each file
    and mark the corresponding source files as test-only.
    """
    external: set[Path] = set()
    for path in files:
        text = path.read_text(encoding="utf-8")
        for match in EXT_TEST_MOD.finditer(text):
            name = match.group(1)
            parent = path.parent
            # `mod <name>;` in `foo.rs` refers to:
            #   - `foo/<name>.rs`             (Rust 2018 path convention)
            #   - `foo/<name>/mod.rs`         (legacy)
            # `mod <name>;` in `foo/mod.rs` or `foo/lib.rs` refers to:
            #   - `foo/<name>.rs`
            #   - `foo/<name>/mod.rs`
            # We follow the 2018 convention first, then legacy.
            stem = path.stem
            if stem in ("mod", "lib", "main") or path.name == f"{stem}.rs":
                sibling_root = parent if stem in ("mod", "lib", "main") else parent / stem
                candidates = [
                    parent / f"{name}.rs",
                    parent / name / "mod.rs",
                    sibling_root / f"{name}.rs",
                    sibling_root / name / "mod.rs",
                ]
            else:
                candidates = [parent / f"{name}.rs", parent / name / "mod.rs"]
            for candidate in candidates:
                if candidate.exists():
                    external.add(candidate.resolve())
                    break
    return external


def file_is_inner_test(path: Path) -> bool:
    """True if the file starts with `#![cfg(test)]` (file-wide test attr)."""
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped or stripped.startswith("//"):
                    continue
                return bool(INNER_CFG_TEST.match(line))
    except OSError:
        return False
    return False


def find_violations_in_file(path: Path, is_all_test: bool) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    # Stack of brace depths at which a test scope was opened. A panic is
    # allowed iff this stack is non-empty or the whole file is test-only.
    test_scope_stack: list[int] = []
    depth = 0
    pending_test_attr = False

    violations: list[str] = []

    for lineno, raw_line in enumerate(lines, start=1):
        stripped_line = strip_strings_and_line_comment(raw_line)
        bare = raw_line.strip()

        if not bare or bare.startswith("//"):
            continue

        if bare.startswith(("#[", "#![")):
            if TEST_ATTR.match(raw_line):
                pending_test_attr = True
            continue

        opens = stripped_line.count("{")
        closes = stripped_line.count("}")

        # A test attribute can sit above a signature that rustfmt wraps
        # across multiple lines (e.g., `#[tokio::test]\nasync fn foo(\n
        # args,\n) -> Result<…>\nwhere\n    T: Clone,\n{`). The `{` that
        # opens the scope may land on a line with no `fn`/`mod`/`impl`
        # keyword. Match on the brace alone:
        #   * `{` present → the pending attr annotates this scope; push.
        #   * `;` present without `{` → non-scope item (trait method
        #     signature or extern `mod foo;`); discard the pending attr.
        #   * neither → continuation line; keep the flag alive.
        if pending_test_attr:
            if "{" in stripped_line:
                test_scope_stack.append(depth)
                pending_test_attr = False
            elif ";" in stripped_line:
                pending_test_attr = False

        depth += opens

        if (
            PANIC_MACROS.search(stripped_line)
            and ALLOW_MARKER not in raw_line
            and not test_scope_stack
            and not is_all_test
        ):
            snippet = bare[:120]
            violations.append(f"{relpath(path)}:{lineno}: {snippet}")

        depth -= closes
        while test_scope_stack and depth <= test_scope_stack[-1]:
            test_scope_stack.pop()

    return violations


def main() -> int:
    files = collect_files()
    external_test_files = collect_external_test_files(files)

    offenders: list[str] = []
    for path in files:
        is_all_test = (
            path.resolve() in external_test_files or file_is_inner_test(path)
        )
        offenders.extend(find_violations_in_file(path, is_all_test))

    if not offenders:
        print("No-panic-in-production guard OK.")
        return 0

    print(
        "Production-panic guard failed in logfwd-runtime / logfwd-output. "
        "Use `?`, structured error variants, or `// ALLOW-PANIC: <reason>` "
        "for genuinely unreachable invariants. See dev-docs/CODE_STYLE.md "
        "-> Error Handling.",
        file=sys.stderr,
    )
    print("Violations:", file=sys.stderr)
    for entry in offenders:
        print(f"  - {entry}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
