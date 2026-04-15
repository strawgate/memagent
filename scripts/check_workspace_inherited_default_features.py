#!/usr/bin/env python3
"""Fail if a crate overrides `default-features` on an inherited dependency."""

from __future__ import annotations

from pathlib import Path
import sys

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError as exc:
        print(
            "error: Python 3.11+ or the 'tomli' package is required to read TOML",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc


def find_cargo_tomls(repo_root: Path) -> list[Path]:
    manifests: list[Path] = []
    for path in sorted(repo_root.rglob("Cargo.toml")):
        if not path.is_file():
            continue
        rel_parts = path.relative_to(repo_root).parts
        if "target" in rel_parts:
            continue
        manifests.append(path)
    return manifests


def iter_dependency_tables(doc: dict) -> list[tuple[str, dict]]:
    tables: list[tuple[str, dict]] = []
    for key in (
        "dependencies",
        "dev-dependencies",
        "build-dependencies",
    ):
        value = doc.get(key)
        if isinstance(value, dict):
            tables.append((key, value))

    target = doc.get("target")
    if isinstance(target, dict):
        for target_key, target_value in target.items():
            if not isinstance(target_value, dict):
                continue
            for dep_key in ("dependencies", "dev-dependencies", "build-dependencies"):
                dep_table = target_value.get(dep_key)
                if isinstance(dep_table, dict):
                    tables.append((f"target.{target_key}.{dep_key}", dep_table))
    return tables


def check_manifest(path: Path, repo_root: Path) -> list[str]:
    doc = tomllib.loads(path.read_text(encoding="utf-8"))
    violations: list[str] = []
    for table_name, dep_table in iter_dependency_tables(doc):
        for dep_name, dep_spec in dep_table.items():
            if not isinstance(dep_spec, dict):
                continue
            if dep_spec.get("workspace") is True and "default-features" in dep_spec:
                rel = path.relative_to(repo_root).as_posix()
                violations.append(
                    f"{rel}: [{table_name}] dependency '{dep_name}' sets "
                    "'workspace = true' and 'default-features' together"
                )
    return violations


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    violations: list[str] = []
    for manifest in find_cargo_tomls(repo_root):
        violations.extend(check_manifest(manifest, repo_root))

    if violations:
        print(
            "Workspace dependency inheritance guard failed:\n"
            "do not set `default-features` on dependencies declared with "
            "`workspace = true`.\n"
            "Move default-feature policy to `[workspace.dependencies]` instead."
        )
        for entry in violations:
            print(f"  - {entry}")
        return 1

    print("Workspace dependency inheritance guard OK (no workspace+default-features overrides).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
