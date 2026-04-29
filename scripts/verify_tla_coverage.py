#!/usr/bin/env python3
"""Run TLC coverage configs as witness checks.

Coverage configs encode reachability as invariants over negated predicates.
A successful check therefore looks like an invariant violation for each listed
coverage invariant. This harness runs each invariant in isolation and treats the
expected witness violation as success.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
from typing import Sequence

SECTION_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
KNOWN_CFG_SECTIONS = {
    "SPECIFICATION",
    "INIT",
    "NEXT",
    "CONSTANTS",
    "CONSTRAINTS",
    "ACTION_CONSTRAINTS",
    "INVARIANTS",
    "PROPERTIES",
    "SYMMETRY",
    "VIEW",
    "CHECK_DEADLOCK",
    "POSTCONDITION",
    "ALIAS",
}


def is_section_header(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if len(line) != len(line.lstrip()):
        return False
    return stripped in KNOWN_CFG_SECTIONS and SECTION_RE.match(stripped) is not None


def split_cfg_sections(lines: Sequence[str]) -> tuple[list[str], list[str], list[str]]:
    start = None
    end = None
    for idx, raw in enumerate(lines):
        if raw.strip() == "INVARIANTS":
            start = idx
            continue
        if start is not None and idx > start and is_section_header(raw):
            end = idx
            break
    if start is None:
        raise ValueError("config does not contain an INVARIANTS section")
    if end is None:
        end = len(lines)
    return list(lines[:start]), list(lines[start + 1 : end]), list(lines[end:])


def parse_invariants(body_lines: Sequence[str]) -> list[str]:
    invariants: list[str] = []
    for raw in body_lines:
        candidate = raw.split("\\*", 1)[0].strip()
        if not candidate:
            continue
        invariants.append(candidate.rstrip(","))
    if not invariants:
        raise ValueError("coverage config INVARIANTS section is empty")
    return invariants


def find_java() -> str:
    """Find a Java runtime, matching the repo's justfile fallback behavior."""
    java_home = os.environ.get("JAVA_HOME")
    candidates = [
        os.environ.get("JAVA_BIN"),
        os.path.join(java_home, "bin", "java") if java_home else None,
        "/opt/homebrew/opt/openjdk/bin/java",
        "/usr/local/opt/openjdk/bin/java",
        "java",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        java_bin = shutil.which(candidate) if os.path.basename(candidate) == candidate else candidate
        if not java_bin:
            continue
        if not os.path.exists(java_bin):
            continue
        proc = subprocess.run(
            [java_bin, "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if proc.returncode == 0:
            return java_bin
    raise RuntimeError(
        "Java runtime not found. Install OpenJDK (e.g. 'brew install openjdk') or set JAVA_BIN."
    )


def run_one(jar: pathlib.Path, tla_file: pathlib.Path, cfg_lines: Sequence[str], invariant: str) -> None:
    before, _body, after = split_cfg_sections(cfg_lines)
    rendered = before + ["INVARIANTS", f"    {invariant}"] + after
    with tempfile.NamedTemporaryFile("w", suffix=".cfg", delete=False) as fh:
        fh.write("\n".join(rendered))
        fh.write("\n")
        tmp_cfg = pathlib.Path(fh.name)

    try:
        java_bin = find_java()
        cmd = [
            java_bin,
            "-cp",
            str(jar),
            "tlc2.TLC",
            str(tla_file),
            "-config",
            str(tmp_cfg),
            "-workers",
            "auto",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        output = proc.stdout + proc.stderr
        lowered = output.lower()

        if proc.returncode == 0:
            raise RuntimeError(
                f"coverage invariant {invariant} did not produce a witness violation; TLC exited 0"
            )
        if invariant.lower() not in lowered or "violat" not in lowered:
            snippet = "\n".join(output.splitlines()[-20:])
            raise RuntimeError(
                f"coverage invariant {invariant} failed for an unexpected reason\n{snippet}"
            )

        print(f"ok: {invariant}")
    finally:
        tmp_cfg.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jar", required=True, type=pathlib.Path)
    parser.add_argument("--tla-file", required=True, type=pathlib.Path)
    parser.add_argument("--config", required=True, type=pathlib.Path)
    args = parser.parse_args()

    cfg_lines = args.config.read_text(encoding="utf-8").splitlines()
    _before, body, _after = split_cfg_sections(cfg_lines)
    invariants = parse_invariants(body)

    print(f"Checking {args.config} ({len(invariants)} coverage invariants)")
    for invariant in invariants:
        run_one(args.jar, args.tla_file, cfg_lines, invariant)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
