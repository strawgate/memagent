#!/usr/bin/env python3
"""Validate that CI's TLC matrix covers expected TLA config files."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
TLA_DIR = ROOT / "tla"
IGNORED_CFG_SUFFIXES = (".coverage.cfg", ".thorough.cfg")


@dataclass(frozen=True)
class TlcMatrixEntry:
    spec: str
    tla_file: str
    config: str
    property: str


def _strip_yaml_value(value: str) -> str:
    value = value.strip()
    if value and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def parse_tlc_matrix_entries(workflow_text: str) -> list[TlcMatrixEntry]:
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

    entries: list[TlcMatrixEntry] = []
    current: dict[str, str] | None = None
    in_include = False
    include_indent: int | None = None
    item_indent: int | None = None
    allowed_keys = {"spec", "tla_file", "config", "property"}

    def flush_current() -> None:
        nonlocal current
        if current is None:
            return
        entries.append(
            TlcMatrixEntry(
                spec=current.get("spec", ""),
                tla_file=current.get("tla_file", ""),
                config=current.get("config", ""),
                property=current.get("property", ""),
            )
        )
        current = None

    for line in tlc_lines:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))

        if not in_include:
            if stripped == "include:":
                in_include = True
                include_indent = indent
            continue

        if stripped and include_indent is not None and indent <= include_indent:
            flush_current()
            break

        if not stripped:
            continue

        if stripped.startswith("- "):
            flush_current()
            item_indent = indent
            current = {}
            stripped = stripped[2:].strip()
            if ":" not in stripped:
                continue

            key, raw_value = stripped.split(":", 1)
            key = key.strip()
            if key in allowed_keys:
                current[key] = _strip_yaml_value(raw_value)
            continue

        if current is None or item_indent is None or indent <= item_indent or ":" not in stripped:
            continue

        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        if key in allowed_keys:
            current[key] = _strip_yaml_value(raw_value)

    flush_current()

    if not entries:
        raise ValueError("ci.yml tlc matrix has no entries")

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
    entries = parse_tlc_matrix_entries(CI_WORKFLOW.read_text(encoding="utf-8"))

    matrix_cfgs: set[str] = set()
    seen_cfg: set[str] = set()

    for entry in entries:
        if not entry.spec:
            errors.append("tlc matrix entry has empty spec")
        if not entry.tla_file:
            errors.append(f"{entry.spec}: tlc matrix entry has empty tla_file")
            continue
        if not entry.config:
            errors.append(f"{entry.spec}: tlc matrix entry has empty config")
            continue
        if not entry.property:
            errors.append(f"{entry.spec}: tlc matrix entry has empty property")

        if entry.config in seen_cfg:
            errors.append(f"{entry.config}: duplicated tlc matrix config entry")
        seen_cfg.add(entry.config)
        matrix_cfgs.add(entry.config)

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

    expected_cfg_set = expected_ci_cfgs()
    missing = sorted(expected_cfg_set - matrix_cfgs)
    extra = sorted(matrix_cfgs - expected_cfg_set)

    for cfg in missing:
        errors.append(
            f"{cfg}: missing from tlc matrix (add entry or mark as ignored suffix)"
        )
    for cfg in extra:
        errors.append(
            f"{cfg}: present in tlc matrix but not in expected non-coverage/non-thorough cfg set"
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
