#!/usr/bin/env python3
"""Validate lifecycle headers for research markdown files."""

from __future__ import annotations

from pathlib import Path
import re

ALLOWED_STATUS = {"Active", "Completed", "Historical"}
ROOT = Path("dev-docs/research")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    failures: list[str] = []

    if not lines or not lines[0].startswith("# "):
        failures.append(f"{path}: first line must be markdown title")

    head = "\n".join(lines[:14])
    status_match = re.search(r"^> \*\*Status:\*\*\s*(.+)$", head, flags=re.MULTILINE)
    date_match = re.search(r"^> \*\*Date:\*\*\s*(.+)$", head, flags=re.MULTILINE)
    context_match = re.search(r"^> \*\*Context:\*\*\s*(.+)$", head, flags=re.MULTILINE)

    if status_match is None:
        failures.append(f"{path}: missing '> **Status:** ...' header")
    else:
        status = status_match.group(1).strip()
        if status not in ALLOWED_STATUS:
            allowed = ", ".join(sorted(ALLOWED_STATUS))
            failures.append(f"{path}: invalid status '{status}' (allowed: {allowed})")

    if date_match is None:
        failures.append(f"{path}: missing '> **Date:** ...' header")
    else:
        date_value = date_match.group(1).strip()
        if not DATE_RE.match(date_value):
            failures.append(
                f"{path}: invalid date '{date_value}' (expected YYYY-MM-DD)"
            )

    if context_match is None:
        failures.append(f"{path}: missing '> **Context:** ...' header")
    else:
        context = context_match.group(1).strip()
        if not context:
            failures.append(f"{path}: context must not be empty")

    return failures


def main() -> int:
    if not ROOT.exists():
        print(f"missing research directory: {ROOT}")
        return 1

    failures: list[str] = []
    files = sorted(ROOT.rglob("*.md"))
    if not files:
        print("no research markdown files found")
        return 1

    checked_files = 0
    for path in files:
        if path == ROOT / "README.md":
            continue
        checked_files += 1
        failures.extend(validate_file(path))

    if checked_files == 0:
        failures.append(f"{ROOT}: no research note files found (only README.md)")

    if failures:
        print("Research metadata validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print(f"Research metadata validation passed ({checked_files} files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
