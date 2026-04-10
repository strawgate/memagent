#!/usr/bin/env python3
"""
Run a TLC reachability/coverage config where invariant violations are expected.

Coverage configs encode each reachability witness as an invariant over the
negated target state (~P). TLC must report each invariant as violated at least
once. This script turns that expected non-zero TLC exit into a pass/fail check.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


INVARIANT_DECL_RE = re.compile(r"^[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*$")
VIOLATION_RE = re.compile(r"Invariant[ \t]+([A-Za-z_][A-Za-z0-9_]*)[ \t]+is violated")


def parse_expected_invariants(cfg_path: Path) -> list[str]:
    lines = cfg_path.read_text(encoding="utf-8").splitlines()
    expected: list[str] = []
    in_block = False
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("\\*"):
            continue
        if not in_block:
            if line == "INVARIANTS":
                in_block = True
            continue
        # End block when a new section starts (uppercase header token).
        if line.isupper() and not line.startswith(("TRUE", "FALSE")):
            break
        match = INVARIANT_DECL_RE.match(line)
        if match:
            expected.append(match.group(1))
    return expected


def run_tlc(jar: Path, tla_file: Path, cfg: Path, workers: str) -> subprocess.CompletedProcess[str]:
    cmd = [
        "java",
        "-cp",
        str(jar),
        "tlc2.TLC",
        str(tla_file),
        "-config",
        str(cfg),
        "-workers",
        workers,
        "-continue",
    ]
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jar", required=True, type=Path, help="Path to tla2tools.jar")
    parser.add_argument("--tla-file", required=True, type=Path, help="TLA module to run")
    parser.add_argument("--config", required=True, type=Path, help="Coverage .cfg file")
    parser.add_argument("--workers", default="auto", help="TLC worker setting (default: auto)")
    args = parser.parse_args()

    expected = parse_expected_invariants(args.config)
    if not expected:
        print(f"ERROR: no INVARIANTS parsed from {args.config}", file=sys.stderr)
        return 2

    proc = run_tlc(args.jar, args.tla_file, args.config, args.workers)
    output = proc.stdout
    violated = set(VIOLATION_RE.findall(output))
    missing = [name for name in expected if name not in violated]

    print(
        f"[tlc-coverage] config={args.config} expected={len(expected)} "
        f"violated={len(violated)} exit={proc.returncode}"
    )

    if proc.returncode == 0:
        print("ERROR: TLC exited 0 for coverage run; expected invariant violations.", file=sys.stderr)
        print(output, file=sys.stderr)
        return 1

    if missing:
        print(
            "ERROR: missing expected reachability witnesses: "
            + ", ".join(missing),
            file=sys.stderr,
        )
        print(output, file=sys.stderr)
        return 1

    print("[tlc-coverage] all expected reachability invariants were violated at least once")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

