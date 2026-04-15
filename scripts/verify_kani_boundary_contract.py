#!/usr/bin/env python3
"""Validate the non-core Kani boundary contract."""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10
    try:
        import tomli as tomllib
    except ModuleNotFoundError as exc:  # pragma: no cover - script-level failure path
        raise SystemExit(
            "error: Python 3.11+ or the 'tomli' package is required to read "
            "dev-docs/verification/kani-boundary-contract.toml"
        ) from exc


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "dev-docs" / "verification" / "kani-boundary-contract.toml"
VALID_STATUSES = {"required", "recommended", "exempt"}


def load_contract() -> list[dict[str, object]]:
    with CONTRACT.open("rb") as fh:
        data = tomllib.load(fh)
    if data.get("version") != 1:
        raise ValueError("kani-boundary contract version must be 1")
    seams = data.get("seams")
    if not isinstance(seams, list) or not seams:
        raise ValueError("kani-boundary contract must define at least one [[seams]] entry")
    return seams


def rust_files_with_kani() -> set[str]:
    result: set[str] = set()
    for path in (ROOT / "crates").rglob("*.rs"):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("crates/logfwd-core/"):
            continue
        text = path.read_text(encoding="utf-8")
        if "#[cfg(kani)]" in text or "#[kani::proof]" in text:
            result.add(rel)
    return result


def validate() -> list[str]:
    errors: list[str] = []
    seams = load_contract()
    seen: set[str] = set()
    proof_files = rust_files_with_kani()

    for idx, seam in enumerate(seams, start=1):
        if not isinstance(seam, dict):
            errors.append(f"entry {idx}: seam entry must be a table")
            continue

        path = seam.get("path")
        status = seam.get("status")
        reason = seam.get("reason")

        if not isinstance(path, str) or not path:
            errors.append(f"entry {idx}: missing non-empty path")
            continue
        if path in seen:
            errors.append(f"{path}: duplicate manifest entry")
            continue
        seen.add(path)

        if not isinstance(status, str) or status not in VALID_STATUSES:
            errors.append(f"{path}: invalid status {status!r}")
            continue
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f"{path}: missing non-empty reason")

        file_path = ROOT / path
        if not file_path.is_file():
            errors.append(f"{path}: listed file does not exist")
            continue

        text = file_path.read_text(encoding="utf-8")
        has_kani_cfg = "#[cfg(kani)]" in text
        has_kani_proof = "#[kani::proof]" in text

        if status == "required":
            if not has_kani_cfg:
                errors.append(f"{path}: required seam is missing #[cfg(kani)]")
            if not has_kani_proof:
                errors.append(f"{path}: required seam is missing #[kani::proof]")
        elif status == "exempt" and (has_kani_cfg or has_kani_proof):
            errors.append(f"{path}: exempt seam unexpectedly contains Kani proofs")

    missing = sorted(proof_files - seen)
    for path in missing:
        errors.append(
            f"{path}: contains non-core Kani proofs but is missing from kani-boundary-contract.toml"
        )

    return errors


def main() -> int:
    try:
        errors = validate()
    except Exception as exc:  # pragma: no cover - script-level failure path
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if errors:
        print("Kani boundary contract validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("Kani boundary contract OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
