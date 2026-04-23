#!/usr/bin/env python3
"""Audit oversized Rust source files and emit split recommendations.

Usage:
  python scripts/audit_large_rust_files.py [--root .] [--threshold 1000] [--target 600] [--top 10]
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class FileStat:
    path: str
    lines: int


EXCLUDE_DIRS = {".git", "target", "node_modules"}


def rust_files(root: str):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for name in filenames:
            if name.endswith(".rs"):
                yield os.path.join(dirpath, name)


def line_count(path: str) -> int:
    with open(path, "r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def collect(root: str, threshold: int) -> list[FileStat]:
    out: list[FileStat] = []
    for path in rust_files(root):
        count = line_count(path)
        if count > threshold:
            rel = os.path.relpath(path, root)
            out.append(FileStat(path=rel, lines=count))
    out.sort(key=lambda item: item.lines, reverse=True)
    return out


def suggest_parts(lines: int, target: int) -> int:
    parts, rem = divmod(lines, target)
    return parts + (1 if rem else 0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--threshold", type=int, default=1000)
    parser.add_argument("--target", type=int, default=600)
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    stats = collect(args.root, args.threshold)
    selected = stats[: args.top]

    print(
        f"Oversized Rust files (> {args.threshold} lines): {len(stats)} total. "
        f"Top {len(selected)} shown."
    )
    print(
        "rank\tlines\tsuggested_parts\testimated_max_lines_per_part\tpath"
    )
    for idx, item in enumerate(selected, start=1):
        parts = suggest_parts(item.lines, args.target)
        estimate = (item.lines + parts - 1) // parts
        print(
            f"{idx}\t{item.lines}\t{parts}\t{estimate}\t{item.path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
