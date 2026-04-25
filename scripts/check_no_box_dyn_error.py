#!/usr/bin/env python3
"""Fail if a public signature returns or accepts `Box<dyn Error>`.

Library crates must expose `thiserror`-style enums with matchable variants
so callers can recover. `Box<dyn Error>` strips that ability and is
permitted only in the binary crate (`logfwd`) and in benches/profilers.

Scope:
- Every `.rs` file under `crates/<name>/src/` for crates not on the
  allowlist (binaries, benches, test-utility crates, sensor prototypes).
- Matches `Box<dyn Error>`, `Box<dyn std::error::Error>`, and trait-bound
  variants like `Box<dyn Error + Send + Sync>`.
- Catches both `pub fn`/`pub(crate) fn` declarations and trait-method
  signatures inside `pub trait` / `pub(crate) trait` blocks (where
  `pub` is implicit on methods).
- Signatures are accumulated across wrapped lines until `;` or `{` so
  multi-line return types are covered.

See `dev-docs/CODE_STYLE.md` -> Error Handling.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parent.parent
CRATES_ROOT = REPO_ROOT / "crates"

# Crates allowed to use Box<dyn Error> in public signatures. The binary
# crate (`logfwd`) is the application shell; benches/profilers, test
# utilities, and experimental sensor binaries are not library APIs.
ALLOWED_CRATES = {
    "logfwd",
    "logfwd-bench",
    "logfwd-test-utils",
    "logfwd-ebpf-proto",
}

# Top-level crate-relative path components that opt a file out of the
# library-API guard even in an otherwise-restricted crate. Only the
# component immediately under the crate root counts (so `src/tests/foo.rs`
# is still guarded — it is production code that happens to live under a
# directory named `tests`).
TOP_LEVEL_EXEMPT = {"tests", "benches", "examples"}

# `src/bin/` is the conventional location for binary targets inside a
# library crate; those are applications, not library surface.
SRC_BIN_PREFIX = ("src", "bin")

# Box<dyn Error[...]>, across trait bounds. The `\b` after `Error`
# tolerates `Error>`, `Error +`, and whitespace.
BOX_DYN_ERROR = re.compile(
    r"Box\s*<\s*dyn\s+(?:std::error::|core::error::|alloc::error::)?Error\b"
)

# A signature opener we care about: `fn` or `type` at the start of a
# trimmed line. Trait-method visibility is implicit, so inside a
# `pub trait` block every `fn` is public. Outside, we require a `pub`
# prefix on the declaration line that reaches the `fn` token.
SIGNATURE_HEAD = re.compile(r"^\s*(?:pub\b[^;{]*?)?\bfn\b|^\s*(?:pub\b[^;{]*?)?\btype\b")

PUB_TRAIT_OPEN = re.compile(r"^\s*pub(?:\([^)]*\))?\s+(?:unsafe\s+)?trait\b[^{;]*\{")


def relpath(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def crate_name(path: Path) -> str:
    return path.relative_to(CRATES_ROOT).parts[0]


def is_exempt(path: Path) -> bool:
    if crate_name(path) in ALLOWED_CRATES:
        return True
    rel_parts = path.relative_to(CRATES_ROOT / crate_name(path)).parts
    if rel_parts[:2] == SRC_BIN_PREFIX:
        return True
    return bool(rel_parts) and rel_parts[0] in TOP_LEVEL_EXEMPT


def rust_files() -> list[Path]:
    return sorted(CRATES_ROOT.rglob("*.rs"))


def strip_strings_and_line_comment(line: str) -> str:
    """Remove string literals first (so `//` inside a string is not
    mistaken for a comment), then drop trailing `//` comments.
    """
    line = re.sub(r'"(?:[^"\\]|\\.)*"', '""', line)
    line = re.sub(r"'(?:[^'\\]|\\.)*'", "''", line)
    comment_idx = line.find("//")
    if comment_idx != -1:
        line = line[:comment_idx]
    return line


def find_violations_in_file(path: Path) -> list[str]:
    """Scan a single file for public signatures mentioning Box<dyn Error>.

    Tracks whether we are inside a `pub trait { ... }` block so that
    trait-method signatures (which don't carry a `pub` keyword) are also
    flagged. Accumulates wrapped signatures up to the first `;` or `{`.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    violations: list[str] = []

    # Stack of brace depths at which a pub-trait scope was entered.
    # Non-empty => we're inside a public trait and should treat trait
    # method/type signatures as public.
    pub_trait_stack: list[int] = []
    # Generic scope stack tracking any trait/impl so a pub-trait's enter
    # depth is popped when the whole block closes.
    depth = 0
    pub_trait_entry_depths: list[int] = []

    accum: list[str] = []
    accum_start_line = 0
    accum_is_public = False

    def flush(reason: str) -> None:  # reason is purely for debugging
        nonlocal accum, accum_start_line, accum_is_public
        if accum and accum_is_public:
            joined = " ".join(accum)
            if BOX_DYN_ERROR.search(joined):
                snippet = joined.strip()[:160]
                violations.append(f"{relpath(path)}:{accum_start_line}: {snippet}")
        accum = []
        accum_start_line = 0
        accum_is_public = False

    for lineno, raw_line in enumerate(lines, start=1):
        stripped = strip_strings_and_line_comment(raw_line)
        bare = stripped.strip()
        if not bare:
            continue

        # Accumulation: if we are mid-signature, keep appending until
        # we see `;` or `{` that terminates it.
        if accum:
            accum.append(stripped)
            # Detect end of signature.
            if ";" in stripped or "{" in stripped:
                flush("terminator")
                # fall through to handle brace deltas below
        else:
            # Start of a new signature?
            if SIGNATURE_HEAD.match(stripped) or (
                pub_trait_stack and re.match(r"^\s*fn\b|^\s*type\b", stripped)
            ):
                accum_start_line = lineno
                accum.append(stripped)
                accum_is_public = (
                    bool(re.match(r"^\s*pub\b", stripped)) or bool(pub_trait_stack)
                )
                if ";" in stripped or "{" in stripped:
                    flush("single-line")

        # Track `pub trait ... {` entries. Match on the original stripped
        # line so we don't mistake the signature accumulator for scope.
        if PUB_TRAIT_OPEN.match(stripped):
            pub_trait_entry_depths.append(depth)

        # Update brace depth using the stripped line (strings/comments
        # removed).
        depth += stripped.count("{")
        depth -= stripped.count("}")
        # Pop any pub-trait scope whose enter-depth is now above current
        # depth (the block closed).
        while pub_trait_entry_depths and depth <= pub_trait_entry_depths[-1]:
            pub_trait_entry_depths.pop()
        # Mirror into the scope stack exposed to the matcher.
        pub_trait_stack = pub_trait_entry_depths[:]

    # Trailing accumulator (shouldn't normally happen — signatures end
    # with `;` or `{`).
    flush("eof")
    return violations


def main() -> int:
    offenders: list[str] = []
    for path in rust_files():
        if is_exempt(path):
            continue
        offenders.extend(find_violations_in_file(path))

    if not offenders:
        print("No-Box<dyn Error>-in-public-signatures guard OK.")
        return 0

    print(
        "Public-signature Box<dyn Error> guard failed. "
        "Library crates must expose thiserror enums with matchable variants. "
        "See dev-docs/CODE_STYLE.md -> Error Handling.",
        file=sys.stderr,
    )
    print("Violations:", file=sys.stderr)
    for entry in offenders:
        print(f"  - {entry}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
