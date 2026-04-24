#!/usr/bin/env python3
"""Validate proptest regression-file policy."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CRATES = ROOT / "crates"
NO_PERSISTENCE_ALLOWLIST = {
    "crates/logfwd-core/src/reassembler.rs",
    "crates/logfwd-core/src/scanner.rs",
    "crates/logfwd-core/src/structural.rs",
    "crates/logfwd-core/src/cri.rs",
    "crates/logfwd-core/src/json_scanner.rs",
    "crates/logfwd-core/src/otlp.rs",
    "crates/logfwd-types/src/pipeline/lifecycle.rs",
    "crates/logfwd-io/tests/it/checkpoint_state_machine.rs",
    "crates/logfwd-io/tests/it/framed_state_machine.rs",
    "crates/logfwd-io/tests/it/framed_buffered_equivalence.rs",
}


def iter_rust_files() -> list[Path]:
    return sorted(CRATES.rglob("*.rs"))


def iter_regression_files() -> list[Path]:
    return sorted(CRATES.rglob("*.proptest-regressions"))


def has_failure_persistence_none(text: str) -> bool:
    return (
        re.search(
            r"failure_persistence\s*[:=]\s*(?:None\b|(?:[A-Za-z_]\w*::)*FailurePersistence::None\b)",
            text,
        )
        is not None
    )


def has_proptest_macro(text: str) -> bool:
    return "proptest!" in text or "prop_state_machine!" in text


def has_seed_entries(text: str) -> bool:
    return any(line.startswith("cc ") for line in text.splitlines())


def validate() -> list[str]:
    errors: list[str] = []

    no_persistence_files: set[str] = set()
    for rust_file in iter_rust_files():
        text = rust_file.read_text(encoding="utf-8")
        if has_failure_persistence_none(text):
            no_persistence_files.add(rust_file.relative_to(ROOT).as_posix())

    unexpected_no_persistence = sorted(no_persistence_files - NO_PERSISTENCE_ALLOWLIST)
    missing_allowlist_entries = sorted(NO_PERSISTENCE_ALLOWLIST - no_persistence_files)

    for path in unexpected_no_persistence:
        errors.append(
            f"{path}: uses failure_persistence: None but is not in NO_PERSISTENCE_ALLOWLIST"
        )
    for path in missing_allowlist_entries:
        errors.append(
            f"{path}: listed in NO_PERSISTENCE_ALLOWLIST but no longer uses failure_persistence: None"
        )

    for regression_file in iter_regression_files():
        rel = regression_file.relative_to(ROOT).as_posix()
        text = regression_file.read_text(encoding="utf-8")
        if not has_seed_entries(text):
            errors.append(f"{rel}: regression file has no seed entries (lines starting with 'cc ')")

        companion = regression_file.with_suffix(".rs")
        if not companion.is_file():
            errors.append(
                f"{rel}: expected companion Rust test file {companion.relative_to(ROOT).as_posix()}"
            )
            continue

        companion_rel = companion.relative_to(ROOT).as_posix()
        companion_text = companion.read_text(encoding="utf-8")
        if not has_proptest_macro(companion_text):
            errors.append(
                f"{rel}: companion test file {companion_rel} has no proptest macro"
            )
        if has_failure_persistence_none(companion_text):
            errors.append(
                f"{rel}: companion test file {companion_rel} disables failure persistence"
            )

    return errors


def main() -> int:
    try:
        errors = validate()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if errors:
        print("Proptest regression policy validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("Proptest regression policy OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
