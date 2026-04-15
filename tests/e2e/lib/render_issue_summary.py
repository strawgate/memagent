#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys

try:
    from reporting.markdown import markdown_table
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(REPO_ROOT))
    from reporting.markdown import markdown_table


@dataclass
class ScenarioResult:
    artifact_name: str
    scenario: str
    policy: str
    passed: bool
    expected_count: int
    actual_count: int
    missing_count: int
    duplicate_count: int
    extra_count: int
    order_violations: int
    null_field_violations: int
    source_checked: bool
    source_passed: bool | None
    source_missing_count: int
    source_duplicate_count: int
    source_extra_count: int
    source_null_field_violations: int
    reason: str | None
    missing_preview: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a suite summary from downloaded e2e artifacts.")
    parser.add_argument("--artifacts-root", required=True)
    parser.add_argument("--suite-name", required=True)
    parser.add_argument("--suite-key", required=True)
    parser.add_argument("--memagent-ref", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--output-markdown", required=True)
    parser.add_argument("--output-json", required=True)
    return parser.parse_args()


def load_result(path: Path, artifact_name: str) -> ScenarioResult:
    payload = json.loads(path.read_text(encoding="utf-8"))
    scenario = payload.get("scenario") or artifact_name
    return ScenarioResult(
        artifact_name=artifact_name,
        scenario=scenario,
        policy=payload.get("policy", "unknown"),
        passed=bool(payload.get("passed")),
        expected_count=int(payload.get("expected_count", 0)),
        actual_count=int(payload.get("actual_count", 0)),
        missing_count=int(payload.get("missing_count", 0)),
        duplicate_count=int(payload.get("duplicate_count", 0)),
        extra_count=int(payload.get("extra_count", 0)),
        order_violations=int(payload.get("order_violations", 0)),
        null_field_violations=int(payload.get("null_field_violations", 0)),
        source_checked=bool(payload.get("source_checked")),
        source_passed=payload.get("source_passed"),
        source_missing_count=int(payload.get("source_missing_count", 0)),
        source_duplicate_count=int(payload.get("source_duplicate_count", 0)),
        source_extra_count=int(payload.get("source_extra_count", 0)),
        source_null_field_violations=int(payload.get("source_null_field_violations", 0)),
        reason=payload.get("reason"),
        missing_preview=payload.get("missing_preview") or [],
    )


def scan_artifacts(root: Path) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    if not root.exists():
        return results
    for artifact_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        result_path = artifact_dir / "result.json"
        if result_path.exists():
            results.append(load_result(result_path, artifact_dir.name))
    return results


def render_markdown(
    suite_name: str,
    suite_key: str,
    memagent_ref: str,
    run_url: str,
    results: list[ScenarioResult],
) -> str:
    total = len(results)
    passed = sum(1 for result in results if result.passed)
    failed = total - passed
    status = "PASS" if failed == 0 and total > 0 else "FAIL"
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        f"# {suite_name} Report",
        "",
        f"- Status: `{status}`",
        f"- Suite key: `{suite_key}`",
        f"- Memagent ref: `{memagent_ref}`",
        f"- Updated: `{updated_at}`",
        f"- Workflow run: [view run]({run_url})",
        f"- Scenarios: `{total}` total, `{passed}` passed, `{failed}` failed",
    ]

    table_rows: list[list[str]] = []
    if not results:
        table_rows.append(["_none_", "FAIL", "n/a", "unknown", "0", "0", "0", "0", "0", "0", "0"])
    else:
        for result in results:
            source_status = "PASS" if result.source_passed else "FAIL" if result.source_checked else "n/a"
            table_rows.append(
                [
                    result.scenario,
                    "PASS" if result.passed else "FAIL",
                    source_status,
                    f"`{result.policy}`",
                    str(result.expected_count),
                    str(result.actual_count),
                    str(result.missing_count),
                    str(result.duplicate_count),
                    str(result.extra_count),
                    str(result.order_violations),
                    str(result.null_field_violations),
                ]
            )

    lines.append("")
    lines.extend(
        markdown_table(
            headers=[
                "Scenario",
                "Status",
                "Source",
                "Policy",
                "Expected",
                "Actual",
                "Missing",
                "Duplicates",
                "Extras",
                "Order Violations",
                "Null Violations",
            ],
            rows=table_rows,
            align=["left", "left", "left", "left", "right", "right", "right", "right", "right", "right", "right"],
        )
    )

    failing = [result for result in results if not result.passed]
    if failing:
        lines.extend(["", "## Failures", ""])
        for result in failing:
            lines.append(f"### {result.scenario}")
            lines.append("")
            if result.reason:
                lines.append(f"- Reason: `{result.reason}`")
            lines.append(f"- Policy: `{result.policy}`")
            lines.append(f"- Counts: expected `{result.expected_count}`, actual `{result.actual_count}`, missing `{result.missing_count}`")
            lines.append(
                f"- Duplicates / extras / order / nulls: `{result.duplicate_count}` / `{result.extra_count}` / `{result.order_violations}` / `{result.null_field_violations}`"
            )
            if result.source_checked:
                lines.append(
                    f"- Source evidence: `{'PASS' if result.source_passed else 'FAIL'}` with missing / duplicates / extras / nulls `{result.source_missing_count}` / `{result.source_duplicate_count}` / `{result.source_extra_count}` / `{result.source_null_field_violations}`"
                )
            if result.missing_preview:
                lines.extend(["", "Missing preview:", "", "```json", json.dumps(result.missing_preview[:5], indent=2, sort_keys=True), "```"])
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    artifacts_root = Path(args.artifacts_root)
    results = scan_artifacts(artifacts_root)

    summary_payload = {
        "suite_name": args.suite_name,
        "suite_key": args.suite_key,
        "memagent_ref": args.memagent_ref,
        "run_url": args.run_url,
        "scenario_count": len(results),
        "passed_count": sum(1 for result in results if result.passed),
        "failed_count": sum(1 for result in results if not result.passed),
        "results": [
            {
                "artifact_name": result.artifact_name,
                "scenario": result.scenario,
                "policy": result.policy,
                "passed": result.passed,
                "expected_count": result.expected_count,
                "actual_count": result.actual_count,
                "missing_count": result.missing_count,
                "duplicate_count": result.duplicate_count,
                "extra_count": result.extra_count,
                "order_violations": result.order_violations,
                "null_field_violations": result.null_field_violations,
                "source_checked": result.source_checked,
                "source_passed": result.source_passed,
                "source_missing_count": result.source_missing_count,
                "source_duplicate_count": result.source_duplicate_count,
                "source_extra_count": result.source_extra_count,
                "source_null_field_violations": result.source_null_field_violations,
                "reason": result.reason,
                "missing_preview": result.missing_preview,
            }
            for result in results
        ],
    }

    markdown = render_markdown(
        suite_name=args.suite_name,
        suite_key=args.suite_key,
        memagent_ref=args.memagent_ref,
        run_url=args.run_url,
        results=results,
    )

    Path(args.output_markdown).write_text(markdown, encoding="utf-8")
    Path(args.output_json).write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
