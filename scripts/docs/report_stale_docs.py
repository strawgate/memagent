#!/usr/bin/env python3
"""Report operational docs whose last git commit is older than 90 days."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess

STALE_DAYS = 90
TARGETS = [
    Path("book/src/troubleshooting.md"),
    Path("book/src/deployment/kubernetes.md"),
    Path("book/src/deployment/docker.md"),
]


@dataclass
class FileAge:
    path: Path
    days_old: int
    last_commit_iso: str


def git_last_commit_iso(path: Path) -> str:
    result = subprocess.run(
        ["git", "log", "-1", "--format=%cI", "--", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main() -> int:
    now = datetime.now(timezone.utc)
    stale: list[FileAge] = []
    fresh: list[FileAge] = []

    for path in TARGETS:
        if not path.exists():
            print(f"missing target file: {path}")
            return 1

        iso = git_last_commit_iso(path)
        ts = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        age_days = (now - ts).days

        entry = FileAge(path=path, days_old=age_days, last_commit_iso=iso)
        if age_days > STALE_DAYS:
            stale.append(entry)
        else:
            fresh.append(entry)

    print(f"Operational doc freshness report (threshold: {STALE_DAYS} days)")
    for entry in fresh:
        print(f"- OK   {entry.path} ({entry.days_old}d, last_commit={entry.last_commit_iso})")
    for entry in stale:
        print(f"- STALE {entry.path} ({entry.days_old}d, last_commit={entry.last_commit_iso})")

    # Non-blocking report for now.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
