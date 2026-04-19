#!/usr/bin/env python3
"""Fail if raw source-metadata injection patterns expand beyond legacy allowlist."""

from __future__ import annotations

from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parent.parent
CRATES_ROOT = REPO_ROOT / "crates"

LEGACY_INJECTION_FILE = "crates/logfwd-io/src/framed.rs"
ALLOWED_INJECTION_HELPER_FILES = {LEGACY_INJECTION_FILE}
ALLOWED_SOURCE_PATH_SUPPORT_FILES = {
    LEGACY_INJECTION_FILE,
    "crates/logfwd-io/src/format.rs",
}

INJECTION_HELPER_PATTERN = re.compile(
    r"\binject_[A-Za-z0-9_]*source[A-Za-z0-9_]*metadata\s*\("
)
SOURCE_PATH_SUPPORT_PATTERN = re.compile(r"\bsupports_source_path_injection\s*\(")
RAW_SOURCE_LITERAL_WRITE_PATTERN = re.compile(
    r'extend_from_slice\(\s*b"(?:[^"\\]|\\.)*_source_',
    re.MULTILINE,
)


def rust_files() -> list[Path]:
    return sorted(CRATES_ROOT.rglob("*.rs"))


def relpath(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def find_expansions() -> list[str]:
    violations: list[str] = []

    for path in rust_files():
        text = path.read_text(encoding="utf-8")
        rel = relpath(path)

        if INJECTION_HELPER_PATTERN.search(text) and rel not in ALLOWED_INJECTION_HELPER_FILES:
            violations.append(
                f"{rel}: source-metadata injection helper usage found outside legacy allowlist"
            )

        if SOURCE_PATH_SUPPORT_PATTERN.search(text) and rel not in ALLOWED_SOURCE_PATH_SUPPORT_FILES:
            violations.append(
                f"{rel}: source-path injection toggle used outside legacy allowlist"
            )

        if RAW_SOURCE_LITERAL_WRITE_PATTERN.search(text) and rel != LEGACY_INJECTION_FILE:
            violations.append(
                f"{rel}: raw byte write of '_source_' field found outside legacy allowlist"
            )

    return violations


def main() -> int:
    offenders = find_expansions()
    if not offenders:
        print(
            "No-raw-payload-injection guard OK "
            "(legacy seam restricted to crates/logfwd-io/src/framed.rs)."
        )
        return 0

    print(
        "No-raw-payload-injection guard failed. "
        "New raw source-metadata injections are only allowed at the legacy seam; "
        "expand this checker before claiming broader resource.attributes.* enforcement.",
        file=sys.stderr,
    )
    print("Violations:", file=sys.stderr)
    for entry in offenders:
        print(f"  - {entry}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
