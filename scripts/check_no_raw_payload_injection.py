#!/usr/bin/env python3
"""Fail if source metadata is injected into raw payload bytes."""

from __future__ import annotations

from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parent.parent
CRATES_ROOT = REPO_ROOT / "crates"

INJECTION_HELPER_PATTERN = re.compile(
    r"\binject_[A-Za-z0-9_]*source[A-Za-z0-9_]*metadata\s*\("
)
SOURCE_PATH_SUPPORT_PATTERN = re.compile(r"\bsupports_source_path_injection\s*\(")
RAW_SOURCE_LITERAL_WRITE_PATTERN = re.compile(
    r'extend_from_slice\(\s*b"(?:[^"\\]|\\.)*(?:_source_|_input)',
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

        if INJECTION_HELPER_PATTERN.search(text):
            violations.append(
                f"{rel}: source-metadata injection helper usage found"
            )

        if SOURCE_PATH_SUPPORT_PATTERN.search(text):
            violations.append(
                f"{rel}: source-path injection toggle found"
            )

        if RAW_SOURCE_LITERAL_WRITE_PATTERN.search(text):
            violations.append(
                f"{rel}: raw byte write of source metadata field found"
            )

    return violations


def main() -> int:
    offenders = find_expansions()
    if not offenders:
        print("No-raw-payload-injection guard OK.")
        return 0

    print(
        "No-raw-payload-injection guard failed. "
        "Source metadata must be attached after scan, not written into raw payload bytes.",
        file=sys.stderr,
    )
    print("Violations:", file=sys.stderr)
    for entry in offenders:
        print(f"  - {entry}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
