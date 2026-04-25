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
    "ffwd-core",
    "ffwd-arrow",
    "ffwd-io",
    "ffwd-output",
}

REQUIRED_KANI_FILTER_PATTERNS = {
    "crates/ffwd-kani/**",
    "crates/ffwd-core/**",
    "crates/ffwd-arrow/**",
    "crates/ffwd-io/**",
    "crates/ffwd-output/**",
    "crates/ffwd-diagnostics/**",
    "Cargo.toml",
    "Cargo.lock",
    "dev-docs/verification/kani-boundary-contract.toml",
    "scripts/verify_kani_boundary_contract.py",
    ".github/workflows/ci.yml",
}

REQUIRED_TLA_FILTER_PATTERNS = {
    "tla/**",
    "crates/ffwd-types/src/pipeline/**",
    "crates/ffwd-runtime/src/pipeline/**",
    "crates/ffwd-io/src/tail/**",
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


def _is_required_just_command(line: str, command: str) -> bool:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return False
    return bool(re.match(rf"^(?:@|-)?\s*{re.escape(command)}(?:\s*(?:#.*)?)$", stripped))


def _collect_step_lines(job_block: list[str], idx: int) -> list[str]:
    step_start = idx
    step_item_indent = None
    for back_idx in range(idx, -1, -1):
        candidate = job_block[back_idx]
        candidate_stripped = candidate.strip()
        candidate_indent = len(candidate) - len(candidate.lstrip(" "))
        if candidate_stripped.startswith("- "):
            step_start = back_idx
            step_item_indent = candidate_indent
            break

    if step_item_indent is None:
        step_item_indent = len(job_block[idx]) - len(job_block[idx].lstrip(" "))

    step_lines = [job_block[step_start]]
    for next_line in job_block[step_start + 1 :]:
        next_stripped = next_line.strip()
        next_indent = len(next_line) - len(next_line.lstrip(" "))
        if next_stripped.startswith("- ") and next_indent == step_item_indent:
            break
        step_lines.append(next_line)
    return step_lines


def _is_yaml_block_scalar_header(line: str, key: str) -> bool:
    return bool(re.match(rf"^{re.escape(key)}:\s*[|>][-+]?\s*$", line.strip()))


def extract_job_block(ci_text: str, job_name: str) -> list[str]:
    lines = ci_text.splitlines()
    jobs_start = None
    jobs_indent = None

    for idx, line in enumerate(lines):
        if line.strip() == "jobs:":
            jobs_start = idx
            jobs_indent = len(line) - len(line.lstrip(" "))
            break

    if jobs_start is None or jobs_indent is None:
        raise ValueError("ci.yml missing top-level 'jobs' section")

    start = None
    job_indent = None
    for idx, line in enumerate(lines[jobs_start + 1 :], start=jobs_start + 1):
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if stripped and indent <= jobs_indent:
            break
        if indent == jobs_indent + 2 and stripped == f"{job_name}:":
            start = idx
            job_indent = indent
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


def extract_paths_filter_entries(ci_text: str, filter_name: str) -> set[str]:
    changes_block = extract_job_block(ci_text, "changes")
    filters_block: list[str] | None = None

    for idx, line in enumerate(changes_block):
        stripped = line.strip()
        if not ("uses:" in stripped and "dorny/paths-filter@" in stripped):
            continue

        step_lines = _collect_step_lines(changes_block, idx)

        if not any(step_line.strip() == "id: filter" for step_line in step_lines):
            continue

        filters_start = None
        filters_indent = None
        for step_idx, step_line in enumerate(step_lines):
            if _is_yaml_block_scalar_header(step_line, "filters"):
                filters_start = step_idx + 1
                filters_indent = len(step_line) - len(step_line.lstrip(" "))
                break

        if filters_start is None or filters_indent is None:
            continue

        block: list[str] = []
        for block_line in step_lines[filters_start:]:
            block_stripped = block_line.strip()
            block_indent = len(block_line) - len(block_line.lstrip(" "))
            if block_stripped and block_indent <= filters_indent:
                break
            block.append(block_line)
        filters_block = block
        break

    if filters_block is None:
        raise ValueError("ci.yml missing changes.filter dorny/paths-filter filters block")

    entries: set[str] = set()
    in_filter = False
    filter_indent = None

    for line in filters_block:
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


KANI_JOB_NAMES = [
    "kani",
    "kani-core",
    "kani-arrow",
    "kani-periphery",
    "kani-runtime-types",
    "kani-io-output",
]


def _extract_kani_crates_from_block(kani_block: list[str]) -> set[str]:
    """Extract -p crate names from a single Kani job block."""
    crates: set[str] = set()

    # Collect crates from kani-github-action args blocks.
    for idx, line in enumerate(kani_block):
        stripped = line.strip()
        if not ("uses:" in stripped and "model-checking/kani-github-action@" in stripped):
            continue

        step_lines = _collect_step_lines(kani_block, idx)

        args_lines: list[str] = []
        collecting = False
        args_indent = None
        for step_line in step_lines:
            step_stripped = step_line.strip()
            indent = len(step_line) - len(step_line.lstrip(" "))
            if not collecting and step_stripped.startswith("args:"):
                collecting = True
                args_indent = indent
                after_colon = step_stripped.split(":", 1)[1].strip()
                if after_colon and after_colon not in {"|", "|-", ">", ">-"}:
                    args_lines.append(after_colon)
                continue
            if collecting:
                if step_stripped and args_indent is not None and indent <= args_indent:
                    break
                args_lines.append(step_stripped)

        joined = " ".join(args_lines)
        crates |= set(re.findall(r"-p\s+([A-Za-z0-9_-]+)", joined))

    # Also collect crates from raw `run: cargo kani -p ...` steps
    # (used when a package needs flags like --lib that can't be mixed
    # with other packages in a single kani-github-action invocation).
    for line in kani_block:
        stripped = line.strip()
        if stripped.startswith("run:"):
            cmd = stripped.removeprefix("run:").strip()
            if "cargo kani" in cmd or "cargo-kani" in cmd:
                crates |= set(re.findall(r"-p\s+([A-Za-z0-9_-]+)", cmd))

    return crates


def extract_kani_args_crates(ci_text: str) -> set[str]:
    crates: set[str] = set()

    # Support both single 'kani' job and split shard jobs.
    for job_name in KANI_JOB_NAMES:
        try:
            kani_block = extract_job_block(ci_text, job_name)
            crates |= _extract_kani_crates_from_block(kani_block)
        except ValueError:
            continue

    if not crates:
        raise ValueError("ci.yml kani job(s) missing -p crate entries")
    return crates


def extract_guardrail_scripts(ci_text: str) -> set[str]:
    lint_block = extract_job_block(ci_text, "lint")
    scripts = set()
    for line in lint_block:
        stripped = line.strip()
        if stripped.startswith("run: python3 scripts/verify_"):
            scripts.add(stripped.removeprefix("run: python3 ").strip())
    return scripts


def extract_just_recipe_deps(just_text: str, recipe: str) -> set[str]:
    match = re.search(rf"^{re.escape(recipe)}:\s*(.*)$", just_text, re.MULTILINE)
    if not match:
        raise ValueError(f"justfile missing recipe '{recipe}'")
    return {token for token in match.group(1).split() if token}


def extract_just_recipe_body(just_text: str, recipe: str) -> list[str]:
    lines = just_text.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if re.match(rf"^{re.escape(recipe)}:\s*(.*)$", line):
            start = idx + 1
            break
    if start is None:
        raise ValueError(f"justfile missing recipe '{recipe}'")

    body: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith((" ", "\t")):
            break
        body.append(line.strip())
    return body


def extract_just_kani_required_crates(just_text: str) -> set[str]:
    lines = just_text.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if re.match(r"^kani-required:\s*(.*)$", line):
            start = idx + 1
            break
    if start is None:
        raise ValueError("justfile missing 'kani-required' recipe")

    block: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith((" ", "\t")):
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
    trigger_body = extract_just_recipe_body(just_text, "verification-trigger-contract")

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

    required_trigger_cmd = "python3 scripts/verify_verification_trigger_contract.py"
    if not any(_is_required_just_command(line, required_trigger_cmd) for line in trigger_body):
        errors.append(
            "just verification-trigger-contract missing required command: "
            f"{required_trigger_cmd}"
        )

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
        id: filter
        with:
          filters: |
            kani_required:
              - 'crates/ffwd-core/**'
              - 'Cargo.toml'
            tla:
              - 'tla/**'
"""
        entries = extract_paths_filter_entries(ci_text, "kani_required")
        self.assertEqual(entries, {"crates/ffwd-core/**", "Cargo.toml"})

    def test_extract_paths_filter_entries_uses_changes_job_block(self) -> None:
        ci_text = """
jobs:
  unrelated:
    steps:
      - uses: some/other-action@v1
        with:
          filters: |
            kani_required:
              - 'docs/**'
  changes:
    steps:
      - uses: dorny/paths-filter@v3
        id: filter
        with:
          filters: |
            kani_required:
              - 'crates/ffwd-core/**'
"""
        entries = extract_paths_filter_entries(ci_text, "kani_required")
        self.assertEqual(entries, {"crates/ffwd-core/**"})

    def test_extract_paths_filter_entries_uses_filter_id_step(self) -> None:
        ci_text = """
jobs:
  changes:
    steps:
      - uses: dorny/paths-filter@v3
        with:
          filters: |
            kani_required:
              - 'docs/**'
      - uses: dorny/paths-filter@v3
        id: filter
        with:
          filters: |
            kani_required:
              - 'crates/ffwd-core/**'
"""
        entries = extract_paths_filter_entries(ci_text, "kani_required")
        self.assertEqual(entries, {"crates/ffwd-core/**"})

    def test_extract_paths_filter_entries_accepts_yaml_block_scalar_variants(self) -> None:
        for block_style in ("|", "|-", ">", ">-"):
            with self.subTest(block_style=block_style):
                ci_text = f"""
jobs:
  changes:
    steps:
      - uses: dorny/paths-filter@v3
        id: filter
        with:
          filters: {block_style}
            kani_required:
              - 'crates/ffwd-core/**'
"""
                entries = extract_paths_filter_entries(ci_text, "kani_required")
                self.assertEqual(entries, {"crates/ffwd-core/**"})

    def test_extract_kani_args_crates(self) -> None:
        ci_text = """
jobs:
  kani:
    steps:
      - uses: model-checking/kani-github-action@v1
        with:
          args: >-
            -p ffwd-core
            -p ffwd-io
            -Z function-contracts
"""
        self.assertEqual(extract_kani_args_crates(ci_text), {"ffwd-core", "ffwd-io"})

    def test_extract_kani_args_crates_split_shards(self) -> None:
        ci_text = """
jobs:
  kani-core:
    steps:
      - uses: model-checking/kani-github-action@v1
        with:
          args: >-
            -p ffwd-core
  kani-arrow:
    steps:
      - uses: model-checking/kani-github-action@v1
        with:
          args: >-
            -p ffwd-arrow
            --lib
  kani-periphery:
    steps:
      - uses: model-checking/kani-github-action@v1
        with:
          args: >-
            -p ffwd-io
            -p ffwd-output
"""
        self.assertEqual(
            extract_kani_args_crates(ci_text),
            {"ffwd-core", "ffwd-arrow", "ffwd-io", "ffwd-output"},
        )

    def test_extract_kani_args_crates_uses_kani_action_step(self) -> None:
        ci_text = """
jobs:
  kani:
    steps:
      - uses: some/other-action@v1
        with:
          args: >-
            -p fake
      - uses: model-checking/kani-github-action@v1
        with:
          args: >-
            -p ffwd-core
"""
        self.assertEqual(extract_kani_args_crates(ci_text), {"ffwd-core"})

    def test_extract_just_kani_required_crates_accepts_recipe_dependencies(self) -> None:
        just_text = """
kani-required: prep-cache
    cargo kani -p ffwd-core
    cargo kani -p ffwd-io
"""
        self.assertEqual(
            extract_just_kani_required_crates(just_text),
            {"ffwd-core", "ffwd-io"},
        )

    def test_extract_job_block_scopes_to_jobs_section(self) -> None:
        ci_text = """
metadata:
  changes:
    note: not a job
jobs:
  changes:
    runs-on: ubuntu-latest
"""
        block = extract_job_block(ci_text, "changes")
        self.assertIn("    runs-on: ubuntu-latest", block)

    def test_extract_just_recipe_body(self) -> None:
        just_text = """
verification-trigger-contract:
    python3 scripts/verify_verification_trigger_contract.py
verification-guardrail: verification-trigger-contract
"""
        body = extract_just_recipe_body(just_text, "verification-trigger-contract")
        self.assertIn(
            "python3 scripts/verify_verification_trigger_contract.py",
            body,
        )

    def test_extract_just_recipe_body_accepts_tab_indentation(self) -> None:
        just_text = (
            "verification-trigger-contract:\n"
            "\tpython3 scripts/verify_verification_trigger_contract.py\n"
            "verification-guardrail: verification-trigger-contract\n"
        )
        body = extract_just_recipe_body(just_text, "verification-trigger-contract")
        self.assertIn(
            "python3 scripts/verify_verification_trigger_contract.py",
            body,
        )

    def test_is_required_just_command(self) -> None:
        required = "python3 scripts/verify_verification_trigger_contract.py"
        self.assertTrue(_is_required_just_command(required, required))
        self.assertTrue(_is_required_just_command(f"@{required}", required))
        self.assertFalse(_is_required_just_command(f"echo '{required}'", required))
        self.assertFalse(_is_required_just_command(f"# {required}", required))


if __name__ == "__main__":
    raise SystemExit(main())
