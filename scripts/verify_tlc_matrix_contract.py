#!/usr/bin/env python3
"""Validate that CI's TLC jobs cover expected TLA config files."""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
TLA_DIR = ROOT / "tla"
IGNORED_CFG_SUFFIXES = (".coverage.cfg", ".thorough.cfg", ".nightly.thorough.cfg")

# Job names holding run-tlc composite-action invocations.  Any top-level job
# whose name starts with one of these prefixes is parsed for TLC entries;
# missing every expected cfg is treated as a hard error.
_TLC_JOB_PREFIXES = ("tlc-",)

# Matches a run-tlc action usage line.  Accepts both the list-item-inline
# form (`- uses: ./.github/actions/run-tlc`) and the mapping-style form
# where `uses:` sits on its own line under a sibling `- name:` key.  We
# then scan the step block for the `tla-file:` / `config:` inputs.
_RUN_TLC_ACTION_RE = re.compile(r"^\s*-?\s*uses:\s*\./\.github/actions/run-tlc\s*$")
_TLA_FILE_RE = re.compile(r"^\s*tla-file:\s*(\S+)\s*$")
_CONFIG_RE = re.compile(r"^\s*config:\s*(\S+)\s*$")

# Legacy inline-command matchers (still accepted so this script can validate
# workflows that have not yet migrated to the composite action).
_TLC_JAVA_RE = re.compile(r"tlc2\.TLC\s+(\S+)\s+-config\s+(\S+)")
_TLC_COVERAGE_RE = re.compile(r"verify_tla_coverage\.py\s+.*--tla-file\s+(\S+)\s+--config\s+(\S+)")


@dataclass(frozen=True)
class TlcEntry:
    tla_file: str
    config: str


def _iter_job_blocks(workflow_text: str) -> Iterator[tuple[str, list[str]]]:
    """Yield (job_name, block_lines) for each top-level job in ci.yml."""
    lines = workflow_text.splitlines()
    in_jobs = False
    jobs_indent = 0
    job_name: str | None = None
    job_indent = 0
    job_start: int | None = None

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))

        if not in_jobs:
            if stripped == "jobs:":
                in_jobs = True
                jobs_indent = indent
            continue

        # A line at or below the jobs: indent ends the jobs section entirely.
        if indent <= jobs_indent and stripped != "jobs:":
            if job_name is not None and job_start is not None:
                yield job_name, lines[job_start:idx]
            return

        # A job header is a mapping key exactly one level inside `jobs:`.
        # The workflow uses 2-space YAML indentation throughout (enforced by
        # our actionlint/format conventions); widening this to also accept
        # 4-space would pick up step-level keys as spurious jobs.
        if stripped.endswith(":") and indent == jobs_indent + 2:
            if job_name is not None and job_start is not None:
                yield job_name, lines[job_start:idx]
            job_name = stripped[:-1]
            job_indent = indent
            job_start = idx

    if in_jobs and job_name is not None and job_start is not None:
        yield job_name, lines[job_start:]


def _extract_action_entries(block: list[str]) -> list[TlcEntry]:
    """Parse composite-action run-tlc invocations.

    Accepts both step forms that GitHub Actions permits:
      - `- uses: ./.github/actions/run-tlc` (list-item-inline)
      - `- name: ...` on one line and `  uses: ...` on the next.

    For each match we walk backward to locate the step's list-item marker
    (its start index and indent) and forward until the next sibling list
    item at that same indent (its end index).  We then scan the *entire*
    step block for `tla-file:` and `config:` inputs — YAML mappings don't
    constrain key order, so `with:` may legitimately precede `uses:`.
    """
    entries: list[TlcEntry] = []
    for start_idx, line in enumerate(block):
        if not _RUN_TLC_ACTION_RE.match(line):
            continue

        # Walk backward until we find the list-item marker ("- ") that
        # introduces this step; that line is the step's start.
        step_start: int | None = None
        step_indent: int | None = None
        for back in range(start_idx, -1, -1):
            candidate = block[back]
            stripped_candidate = candidate.lstrip(" ")
            if stripped_candidate.startswith("- "):
                step_start = back
                step_indent = len(candidate) - len(stripped_candidate)
                break
        if step_start is None or step_indent is None:
            # `uses:` wasn't part of a list step — malformed, skip.
            continue

        # Walk forward from the uses: line until the next sibling list item.
        step_end = len(block)
        for look in range(start_idx + 1, len(block)):
            nxt = block[look]
            nxt_stripped = nxt.lstrip(" ")
            nxt_indent = len(nxt) - len(nxt_stripped)
            if nxt_stripped.startswith("- ") and nxt_indent <= step_indent:
                step_end = look
                break

        # Now scan the full step block (which may place `with:` before
        # `uses:`) for the two inputs we care about.
        tla_file: str | None = None
        config: str | None = None
        for look in range(step_start, step_end):
            nxt = block[look]
            m_tla = _TLA_FILE_RE.match(nxt)
            m_cfg = _CONFIG_RE.match(nxt)
            if m_tla:
                tla_file = m_tla.group(1)
            elif m_cfg:
                config = m_cfg.group(1)
        if tla_file and config:
            entries.append(TlcEntry(tla_file=tla_file, config=config))
    return entries


def parse_tlc_entries(workflow_text: str) -> list[TlcEntry]:
    """Collect TLC entries from every job whose name starts with tlc-."""
    entries: list[TlcEntry] = []
    found_tlc_job = False
    for job_name, block in _iter_job_blocks(workflow_text):
        if not job_name.startswith(_TLC_JOB_PREFIXES):
            continue
        found_tlc_job = True
        entries.extend(_extract_action_entries(block))
        for line in block:
            stripped = line.strip()
            m = _TLC_JAVA_RE.search(stripped) or _TLC_COVERAGE_RE.search(stripped)
            if m:
                entries.append(TlcEntry(tla_file=m.group(1), config=m.group(2)))

    if not found_tlc_job:
        raise ValueError("ci.yml missing any tlc-* job")
    if not entries:
        raise ValueError("ci.yml tlc-* jobs contain no TLC run steps")

    return entries


def expected_ci_cfgs() -> set[str]:
    expected: set[str] = set()
    for cfg in TLA_DIR.glob("*.cfg"):
        if cfg.name.endswith(IGNORED_CFG_SUFFIXES):
            continue
        expected.add(f"tla/{cfg.name}")
    return expected


def expected_mc_tla_for_cfg(config_path: str) -> str:
    config_name = Path(config_path).name
    match = re.match(r"^([A-Za-z0-9_]+)", config_name)
    if not match:
        raise ValueError(f"unable to derive spec name from config {config_path}")
    return f"tla/MC{match.group(1)}.tla"


def validate() -> list[str]:
    errors: list[str] = []
    entries = parse_tlc_entries(CI_WORKFLOW.read_text(encoding="utf-8"))

    ci_cfgs: set[str] = set()
    seen_cfg: set[str] = set()

    for entry in entries:
        if not entry.tla_file:
            errors.append("tlc step has empty tla_file")
            continue
        if not entry.config:
            errors.append("tlc step has empty config")
            continue

        if entry.config in seen_cfg:
            errors.append(f"{entry.config}: duplicated tlc config entry")
        seen_cfg.add(entry.config)
        ci_cfgs.add(entry.config)

        cfg_path = ROOT / entry.config
        if not cfg_path.is_file():
            errors.append(f"{entry.config}: listed config does not exist")

        tla_path = ROOT / entry.tla_file
        if not tla_path.is_file():
            errors.append(f"{entry.tla_file}: listed tla file does not exist")

        try:
            expected_tla = expected_mc_tla_for_cfg(entry.config)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if entry.tla_file != expected_tla:
            errors.append(
                f"{entry.config}: expected tla file {expected_tla}, found {entry.tla_file}"
            )

    # Filter to non-coverage/non-thorough configs for the completeness check
    ci_standard_cfgs = {c for c in ci_cfgs if not c.endswith(IGNORED_CFG_SUFFIXES)}
    expected_cfg_set = expected_ci_cfgs()
    missing = sorted(expected_cfg_set - ci_standard_cfgs)
    extra = sorted(ci_standard_cfgs - expected_cfg_set)

    for cfg in missing:
        errors.append(
            f"{cfg}: missing from tlc job (add step or mark as ignored suffix)"
        )
    for cfg in extra:
        errors.append(
            f"{cfg}: present in tlc job but not in expected non-coverage/non-thorough cfg set"
        )

    return errors


def main() -> int:
    try:
        errors = validate()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if errors:
        print("TLC matrix contract validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("TLC matrix contract OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
