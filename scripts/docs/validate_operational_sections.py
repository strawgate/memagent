#!/usr/bin/env python3
"""Validate required sections in high-risk operational documentation pages."""

from pathlib import Path

REQUIRED_MARKERS = {
    "book/src/content/docs/troubleshooting.md": [
        ":::tip[Before you start]",
        "## Symptom-first triage",
        "## Recovery fallback (safe mode)",
    ],
    "book/src/content/docs/deployment/kubernetes.md": [
        ":::tip[Production safe defaults]",
        "### Validate rollout",
        "### Rollback",
    ],
    "book/src/content/docs/deployment/docker.md": [
        "## Safe defaults",
        "## Validate container health",
        "## Rollback",
    ],
}


def main() -> int:
    failures: list[str] = []

    for rel_path, markers in REQUIRED_MARKERS.items():
        path = Path(rel_path)
        if not path.exists():
            failures.append(f"missing file: {rel_path}")
            continue

        text = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker not in text:
                failures.append(f"{rel_path}: missing section '{marker}'")

    if failures:
        print("Documentation section validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Documentation section validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
