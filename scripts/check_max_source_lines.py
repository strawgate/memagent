#!/usr/bin/env python3
"""Fail if a tracked source file exceeds the repository line-count guardrail."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys


MAX_SOURCE_LINES = 3900
SOURCE_SUFFIXES = {
    ".rs",
    ".py",
    ".sh",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".mjs",
    ".cjs",
    ".html",
}
EXCLUDED_PARTS = {
    ".git",
    "target",
    "node_modules",
    "dashboard-dist",
}
EXCLUDED_FILES = {
    "crates/ffwd-diagnostics/src/dashboard.html",
}


def should_check(path: Path, repo_root: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix not in SOURCE_SUFFIXES:
        return False
    if any(part in EXCLUDED_PARTS for part in path.parts):
        return False
    rel = path.relative_to(repo_root).as_posix()
    return rel not in EXCLUDED_FILES


def tracked_files(repo_root: Path) -> list[Path]:
    git = shutil.which("git")
    if git is None:
        print("error: git not found on PATH", file=sys.stderr)
        sys.exit(1)
    proc = subprocess.run(
        [git, "ls-files"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [repo_root / line for line in proc.stdout.splitlines() if line]


def line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return sum(1 for _ in handle)


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    offenders: list[tuple[int, str]] = []

    for path in tracked_files(repo_root):
        if not should_check(path, repo_root):
            continue
        lines = line_count(path)
        if lines > MAX_SOURCE_LINES:
            offenders.append((lines, path.relative_to(repo_root).as_posix()))

    if not offenders:
        print(f"Source file length check OK (max allowed: {MAX_SOURCE_LINES} lines)")
        return 0

    offenders.sort(reverse=True)
    print(
        f"Source file length check failed: {len(offenders)} file(s) exceed "
        f"{MAX_SOURCE_LINES} lines.",
        file=sys.stderr,
    )
    for lines, relpath in offenders:
        print(f"  {lines:>5}  {relpath}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
