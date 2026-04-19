#!/usr/bin/env python3
"""Validate that CI's TLC job covers expected TLA config files."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
TLA_DIR = ROOT / "tla"
IGNORED_CFG_SUFFIXES = (".coverage.cfg", ".thorough.cfg")

# Matches: java -cp ... tlc2.TLC <tla_file> -config <config> ...
_TLC_JAVA_RE = re.compile(r"tlc2\.TLC\s+(\S+)\s+-config\s+(\S+)")
# Matches: python3 scripts/verify_tla_coverage.py ... --tla-file <tla_file> --config <config>
_TLC_COVERAGE_RE = re.compile(r"verify_tla_coverage\.py\s+.*--tla-file\s+(\S+)\s+--config\s+(\S+)")


@dataclass(frozen=True)
class TlcEntry:
    tla_file: str
    config: str


def parse_tlc_entries(workflow_text: str) -> list[TlcEntry]:
    """Parse TLC entries from sequential steps in the tlc job."""
    lines = workflow_text.splitlines()
    tlc_start = None
    tlc_indent = 0

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "tlc:":
            tlc_start = idx
            tlc_indent = len(line) - len(line.lstrip(" "))
            break
    if tlc_start is None:
        raise ValueError("ci.yml missing top-level tlc job")

    tlc_lines: list[str] = []
    for idx in range(tlc_start + 1, len(lines)):
        line = lines[idx]
        stripped = line.strip()
        if stripped:
            indent = len(line) - len(line.lstrip(" "))
            if indent <= tlc_indent:
                break
        tlc_lines.append(line)

    entries: list[TlcEntry] = []
    for line in tlc_lines:
        stripped = line.strip()
        m = _TLC_JAVA_RE.search(stripped) or _TLC_COVERAGE_RE.search(stripped)
        if m:
            entries.append(TlcEntry(tla_file=m.group(1), config=m.group(2)))

    if not entries:
        raise ValueError("ci.yml tlc job has no TLC run steps")

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
