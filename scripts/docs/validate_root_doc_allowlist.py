#!/usr/bin/env python3
"""Enforce allowed top-level markdown files."""

from pathlib import Path

ALLOWED = {
    "AGENTS.md",
    "CLAUDE.md",
    "CHANGELOG.md",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "DEVELOPING.md",
    "README.md",
    "SECURITY.md",
    "SUPPORT.md",
}


def main() -> int:
    root = Path(".")
    found = {p.name for p in root.glob("*.md") if p.is_file()}
    disallowed = sorted(found - ALLOWED)
    failures: list[str] = []

    if disallowed:
        failures.append("Top-level markdown allowlist validation failed:")
        for name in disallowed:
            failures.append(f"- unexpected root markdown file: {name}")

    claude = root / "CLAUDE.md"
    if claude.exists() and not claude.is_symlink():
        failures.append("- CLAUDE.md must be a symlink to AGENTS.md")

    if failures:
        print("\n".join(failures))
        return 1

    print("Top-level markdown allowlist validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
