#!/usr/bin/env python3
"""Validate CI/just verification trigger contracts stay in sync."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
JUSTFILE = ROOT / "justfile"

REQUIRED_KANI_CRATES = {
    "logfwd-core",
    "logfwd-arrow",
    "logfwd-io",
    "logfwd-output",
}

REQUIRED_KANI_FILTER_PATTERNS = {
    "crates/logfwd-core/**",
    "crates/logfwd-arrow/**",
    "crates/logfwd-io/**",
    "crates/logfwd-output/**",
    "Cargo.toml",
    "Cargo.lock",
    "dev-docs/verification/kani-boundary-contract.toml",
    "scripts/verify_kani_boundary_contract.py",
    ".github/workflows/ci.yml",
}

REQUIRED_TLA_FILTER_PATTERNS = {
    "tla/**",
    "crates/logfwd-types/src/pipeline/**",
    "crates/logfwd-runtime/src/pipeline/**",
    "crates/logfwd-io/src/tail/**",
}

REQUIRED_GUARDRAIL_SCRIPTS = {
    "scripts/verify_kani_boundary_contract.py",
    "scripts/verify_tlc_matrix_contract.py",
    "scripts/verify_proptest_regressions.py",
    "scripts/verify_verification_trigger_contract.py",
}

REQUIRED_JUST_GUARDRAIL_RECIPES = {
    "kani-boundary",
    "tlc-matrix-contract",
    "proptest-regressions",
    "verification-trigger-contract",
}


def _strip_yaml_value(value: str) -> str:
    value = value.strip()
    if value and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def extract_paths_filter_entries(ci_text: str, filter_name: str) -> set[str]:
    lines = ci_text.splitlines()
    filters_start = None
    filters_indent = None

    for idx, line in enumerate(lines):
        if line.strip() == "filters: |":
            filters_start = idx + 1
            filters_indent = len(line) - len(line.lstrip(" "))
            break

    if filters_start is None or filters_indent is None:
        raise ValueError("ci.yml missing dorny/paths-filter literal filters block")

    block: list[str] = []
    for line in lines[filters_start:]:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if stripped and indent <= filters_indent:
            break
        block.append(line)

    entries: set[str] = set()
    in_filter = False
    filter_indent = None

    for line in block:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if not stripped:
            continue

        if stripped == f"{filter_name}:":
            in_filter = True
            filter_indent = indent
            continue

        if in_filter and filter_indent is not None and indent <= filter_indent:
            break

        if in_filter and stripped.startswith("- "):
            entries.add(_strip_yaml_value(stripped[2:]))

    if not entries:
        raise ValueError(f"ci.yml filter '{filter_name}' missing or empty")

    return entries


def extract_job_block(ci_text: str, job_name: str) -> list[str]:
    lines = ci_text.splitlines()
    start = None
    job_indent = None

    for idx, line in enumerate(lines):
        if line.strip() == f"{job_name}:":
            start = idx
            job_indent = len(line) - len(line.lstrip(" "))
            break

    if start is None or job_indent is None:
        raise ValueError(f"ci.yml missing job '{job_name}'")

    block = [lines[start]]
    for line in lines[start + 1 :]:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if stripped and indent <= job_indent:
            break
        block.append(line)
    return block


def extract_kani_args_crates(ci_text: str) -> set[str]:
    kani_block = extract_job_block(ci_text, "kani")
    args_lines: list[str] = []
    collecting = False
    args_indent = None

    for line in kani_block:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if not collecting and stripped.startswith("args:"):
            collecting = True
            args_indent = indent
            after_colon = stripped.split(":", 1)[1].strip()
            if after_colon and after_colon not in {"|", "|-", ">", ">-"}:
                args_lines.append(after_colon)
            continue

        if collecting:
            if stripped and args_indent is not None and indent <= args_indent:
                break
            args_lines.append(stripped)

    if not args_lines:
        raise ValueError("ci.yml kani job missing args block")

    joined = " ".join(args_lines)
    crates = set(re.findall(r"-p\s+([A-Za-z0-9_-]+)", joined))
    if not crates:
        raise ValueError("ci.yml kani args missing -p crate entries")
    return crates


def extract_guardrail_scripts(ci_text: str) -> set[str]:
    guardrail_block = extract_job_block(ci_text, "verification-guardrail")
    scripts = set()
    for line in guardrail_block:
        stripped = line.strip()
        if stripped.startswith("run: python3 scripts/"):
            scripts.add(stripped.removeprefix("run: python3 ").strip())
    return scripts


def extract_just_recipe_deps(just_text: str, recipe: str) -> set[str]:
    match = re.search(rf"^{re.escape(recipe)}:\s*(.*)$", just_text, re.MULTILINE)
    if not match:
        raise ValueError(f"justfile missing recipe '{recipe}'")
    return {token for token in match.group(1).split() if token}


def extract_just_kani_required_crates(just_text: str) -> set[str]:
    lines = just_text.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if line.strip() == "kani-required:":
            start = idx + 1
            break
    if start is None:
        raise ValueError("justfile missing 'kani-required' recipe")

    block: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith(" "):
            break
        block.append(line)

    joined = " ".join(part.strip() for part in block)
    crates = set(re.findall(r"cargo\s+kani\s+-p\s+([A-Za-z0-9_-]+)", joined))
    if not crates:
        raise ValueError("justfile 'kani-required' has no cargo kani -p entries")
    return crates


def pattern_base_exists(pattern: str) -> bool:
    wildcard_idx = len(pattern)
    for token in ("**", "*", "?"):
        idx = pattern.find(token)
        if idx != -1:
            wildcard_idx = min(wildcard_idx, idx)
    base = pattern[:wildcard_idx].rstrip("/")
    if not base:
        return True
    return (ROOT / base).exists()


def validate() -> list[str]:
    errors: list[str] = []
    ci_text = CI_WORKFLOW.read_text(encoding="utf-8")
    just_text = JUSTFILE.read_text(encoding="utf-8")

    kani_filter = extract_paths_filter_entries(ci_text, "kani_required")
    tla_filter = extract_paths_filter_entries(ci_text, "tla")
    kani_ci_crates = extract_kani_args_crates(ci_text)
    kani_just_crates = extract_just_kani_required_crates(just_text)
    guardrail_scripts = extract_guardrail_scripts(ci_text)
    guardrail_deps = extract_just_recipe_deps(just_text, "verification-guardrail")

    for pattern in sorted(REQUIRED_KANI_FILTER_PATTERNS):
        if pattern not in kani_filter:
            errors.append(f"kani_required filter missing pattern: {pattern}")
    for pattern in sorted(REQUIRED_TLA_FILTER_PATTERNS):
        if pattern not in tla_filter:
            errors.append(f"tla filter missing pattern: {pattern}")

    for pattern in sorted(kani_filter | tla_filter):
        if not pattern_base_exists(pattern):
            errors.append(f"filter pattern base path does not exist: {pattern}")

    for crate in sorted(REQUIRED_KANI_CRATES):
        if crate not in kani_ci_crates:
            errors.append(f"ci kani args missing crate: {crate}")
        if crate not in kani_just_crates:
            errors.append(f"justfile kani-required missing crate: {crate}")

    for crate in sorted(kani_ci_crates - kani_just_crates):
        errors.append(f"ci kani crate not present in just kani-required: {crate}")
    for crate in sorted(kani_just_crates - kani_ci_crates):
        errors.append(f"just kani-required crate not present in ci kani args: {crate}")

    for script in sorted(REQUIRED_GUARDRAIL_SCRIPTS):
        if not (ROOT / script).is_file():
            errors.append(f"guardrail script missing on disk: {script}")
        if script not in guardrail_scripts:
            errors.append(f"verification-guardrail job missing script: {script}")

    for dep in sorted(REQUIRED_JUST_GUARDRAIL_RECIPES):
        if dep not in guardrail_deps:
            errors.append(f"just verification-guardrail missing dependency: {dep}")

    return errors


def main() -> int:
    try:
        errors = validate()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if errors:
        print("Verification trigger contract validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("Verification trigger contract OK")
    return 0


class VerificationTriggerContractTests(unittest.TestCase):
    def test_extract_paths_filter_entries(self) -> None:
        ci_text = """
jobs:
  changes:
    steps:
      - uses: dorny/paths-filter@v3
        with:
          filters: |
            kani_required:
              - 'crates/logfwd-core/**'
              - 'Cargo.toml'
            tla:
              - 'tla/**'
"""
        entries = extract_paths_filter_entries(ci_text, "kani_required")
        self.assertEqual(entries, {"crates/logfwd-core/**", "Cargo.toml"})

    def test_extract_kani_args_crates(self) -> None:
        ci_text = """
jobs:
  kani:
    steps:
      - uses: model-checking/kani-github-action@v1
        with:
          args: >-
            -p logfwd-core
            -p logfwd-io
            -Z function-contracts
"""
        self.assertEqual(extract_kani_args_crates(ci_text), {"logfwd-core", "logfwd-io"})


if __name__ == "__main__":
    raise SystemExit(main())
