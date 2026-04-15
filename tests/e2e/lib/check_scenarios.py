#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path


FAMILY_DEFAULTS = {
    "compose": {"up", "down", "collect", "verify"},
    "kind": set(),
    "otlp": {"up", "down", "collect"},
    "custom": set(),
}

FAMILY_REQUIRED_FILES = {
    "compose": {"compose.yaml", "memagent.yaml", "oracle.json", "run_workload.sh"},
    "kind": {"oracle.json", "run_workload.sh"},
    "otlp": {"run_workload.sh", "verify.sh"},
    "custom": {"run_workload.sh"},
}

PHASES = ("up", "run_workload", "verify", "collect", "down")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate memagent-e2e scenario layout and workflow wiring.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--scenario-id")
    return parser.parse_args()


def detect_family(scenario_id: str) -> str:
    if scenario_id.startswith("compose-"):
        return "compose"
    if scenario_id.startswith("kind-"):
        return "kind"
    if scenario_id.startswith("otlp-"):
        return "otlp"
    return "custom"


def validate_scenario(repo_root: Path, scenario_id: str) -> list[str]:
    scenario_dir = repo_root / "tests" / "e2e" / "scenarios" / scenario_id
    family = detect_family(scenario_id)
    errors: list[str] = []

    if not scenario_dir.is_dir():
        return [f"missing scenario directory: {scenario_dir}"]

    for relative_path in sorted(FAMILY_REQUIRED_FILES[family]):
        if not (scenario_dir / relative_path).exists():
            errors.append(f"{scenario_id}: missing required file {relative_path}")

    for phase in PHASES:
        script_name = f"{phase}.sh"
        if (scenario_dir / script_name).exists():
            continue
        if phase in FAMILY_DEFAULTS[family]:
            continue
        errors.append(f"{scenario_id}: missing {script_name} and no {family} default exists")

    workflow_path = repo_root / ".github" / "workflows" / f"e2e-{scenario_id}.yml"
    if not workflow_path.exists():
        errors.append(f"{scenario_id}: missing workflow {workflow_path.name}")

    return errors


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    scenarios_root = repo_root / "tests" / "e2e" / "scenarios"

    if args.scenario_id:
        scenario_ids = [args.scenario_id]
    else:
        scenario_ids = sorted(path.name for path in scenarios_root.iterdir() if path.is_dir())

    errors: list[str] = []
    for scenario_id in scenario_ids:
        errors.extend(validate_scenario(repo_root, scenario_id))

    if errors:
        for error in errors:
            print(error)
        raise SystemExit(1)

    print(f"validated {len(scenario_ids)} scenario(s)")


if __name__ == "__main__":
    main()
