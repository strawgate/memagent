#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare expected and actual e2e rows.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--expected", required=True)
    parser.add_argument("--actual-ndjson", required=True)
    parser.add_argument("--source-json")
    parser.add_argument("--results-dir", required=True)
    return parser.parse_args()


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_ndjson(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not Path(path).exists():
        return rows
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def project_row(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: row.get(key) for key in keys}


def encode_row(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True)


def multiset_difference(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> list[dict[str, Any]]:
    difference: list[dict[str, Any]] = []
    right_counter = Counter(encode_row(row) for row in right)
    for row in left:
        encoded = encode_row(row)
        if right_counter[encoded] > 0:
            right_counter[encoded] -= 1
        else:
            difference.append(row)
    return difference


def count_duplicates(rows: list[dict[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    counter = Counter(encode_row(row) for row in rows)
    duplicates: list[dict[str, Any]] = []
    duplicate_count = 0
    for row in rows:
        encoded = encode_row(row)
        if counter[encoded] > 1:
            duplicate_count += 1
            counter[encoded] = 0
            duplicates.append(row)
    return duplicate_count, duplicates


def count_null_violations(rows: list[dict[str, Any]], required_fields: list[str]) -> tuple[int, list[dict[str, Any]]]:
    violations: list[dict[str, Any]] = []
    for row in rows:
        if any(row.get(field) is None for field in required_fields):
            violations.append(row)
    return len(violations), violations


def count_order_violations(rows: list[dict[str, Any]], order_key: str, source_key: str | None) -> int:
    order_violations = 0
    last_seen: dict[str, Any] = {}
    for row in rows:
        current = row.get(order_key)
        if current is None:
            continue
        source = str(row.get(source_key)) if source_key else "__global__"
        if source in last_seen and current < last_seen[source]:
            order_violations += 1
        last_seen[source] = current
    return order_violations


def evaluate_rows(
    *,
    expected_rows: list[dict[str, Any]],
    actual_rows: list[dict[str, Any]],
    compare_keys: list[str],
    identity_keys: list[str],
    required_fields: list[str],
    policy: str,
    order_key: str,
    source_key: str | None,
    max_extra_rows: int,
) -> dict[str, Any]:
    projected_expected = [project_row(row, compare_keys) for row in expected_rows]
    projected_actual = [project_row(row, compare_keys) for row in actual_rows]
    identity_actual = [project_row(row, identity_keys) for row in actual_rows]

    missing_rows = multiset_difference(projected_expected, projected_actual)
    extra_rows = multiset_difference(projected_actual, projected_expected)
    duplicate_count, duplicate_preview = count_duplicates(identity_actual)
    null_field_violations, null_field_preview = count_null_violations(actual_rows, required_fields)
    order_violations = count_order_violations(actual_rows, order_key, source_key)
    extra_count = len(extra_rows)

    passed = False
    if policy == "exact_once_ordered":
        passed = (
            projected_actual == projected_expected
            and duplicate_count == 0
            and null_field_violations == 0
            and order_violations == 0
        )
    elif policy == "exact_once_unordered":
        passed = Counter(encode_row(row) for row in projected_actual) == Counter(
            encode_row(row) for row in projected_expected
        )
        passed = passed and duplicate_count == 0 and null_field_violations == 0
    elif policy == "at_least_once_bounded_duplicates":
        expected_index = 0
        for row in projected_actual:
            if expected_index < len(projected_expected) and row == projected_expected[expected_index]:
                expected_index += 1
        passed = (
            expected_index == len(projected_expected)
            and extra_count <= max_extra_rows
            and null_field_violations == 0
            and order_violations == 0
        )
    else:
        raise SystemExit(f"unsupported oracle policy: {policy}")

    return {
        "passed": passed,
        "projected_expected": projected_expected,
        "projected_actual": projected_actual,
        "missing_rows": missing_rows,
        "extra_rows": extra_rows,
        "duplicate_count": duplicate_count,
        "duplicate_preview": duplicate_preview,
        "extra_count": extra_count,
        "order_violations": order_violations,
        "null_field_violations": null_field_violations,
        "null_field_preview": null_field_preview,
    }


def main() -> None:
    args = parse_args()
    config = load_json(args.config)
    expected_rows = load_json(args.expected)
    actual_rows = load_ndjson(args.actual_ndjson)
    source_rows = load_json(args.source_json) if args.source_json and Path(args.source_json).exists() else None

    selector = config.get("selector") or {}
    selector_field = selector.get("field")
    selector_value = selector.get("value")
    if selector_field:
        actual_rows = [row for row in actual_rows if row.get(selector_field) == selector_value]

    compare_keys = config.get("compare_keys")
    if not compare_keys:
        all_keys = set()
        for row in expected_rows:
            all_keys.update(row.keys())
        compare_keys = sorted(all_keys)

    identity_keys = config.get("identity_keys") or compare_keys
    required_fields = config.get("required_fields") or identity_keys
    source_key = config.get("source_key")

    policy = config["policy"]
    order_key = config.get("order_key", "seq")
    sink_eval = evaluate_rows(
        expected_rows=expected_rows,
        actual_rows=actual_rows,
        compare_keys=compare_keys,
        identity_keys=identity_keys,
        required_fields=required_fields,
        policy=policy,
        order_key=order_key,
        source_key=source_key,
        max_extra_rows=int(config.get("max_extra_rows", len(expected_rows))),
    )

    source_eval = None
    if source_rows is not None:
        source_compare_keys = config.get("source_compare_keys") or identity_keys
        source_identity_keys = config.get("source_identity_keys") or identity_keys
        source_required_fields = config.get("source_required_fields") or source_compare_keys
        source_policy = config.get("source_policy", "exact_once_unordered")
        source_order_key = config.get("source_order_key", order_key)
        source_eval = evaluate_rows(
            expected_rows=expected_rows,
            actual_rows=source_rows,
            compare_keys=source_compare_keys,
            identity_keys=source_identity_keys,
            required_fields=source_required_fields,
            policy=source_policy,
            order_key=source_order_key,
            source_key=source_key,
            max_extra_rows=int(config.get("source_max_extra_rows", len(expected_rows))),
        )

    passed = sink_eval["passed"] and (source_eval["passed"] if source_eval else True)

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    actual_rows_path = results_dir / "actual_rows.json"
    result_path = results_dir / "result.json"
    summary_path = results_dir / "summary.md"

    actual_rows_path.write_text(json.dumps(sink_eval["projected_actual"], indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = {
        "scenario": config.get("scenario"),
        "policy": policy,
        "passed": passed,
        "reason": (
            "source-evidence-mismatch"
            if source_eval and not source_eval["passed"]
            else "forwarder-mismatch"
            if not sink_eval["passed"]
            else None
        ),
        "expected_count": len(sink_eval["projected_expected"]),
        "actual_count": len(sink_eval["projected_actual"]),
        "missing_count": len(sink_eval["missing_rows"]),
        "duplicate_count": sink_eval["duplicate_count"],
        "extra_count": sink_eval["extra_count"],
        "order_violations": sink_eval["order_violations"],
        "null_field_violations": sink_eval["null_field_violations"],
        "identity_keys": identity_keys,
        "compare_keys": compare_keys,
        "missing_preview": sink_eval["missing_rows"][:10],
        "duplicate_preview": sink_eval["duplicate_preview"][:10],
        "extra_preview": sink_eval["extra_rows"][:10],
        "null_field_preview": sink_eval["null_field_preview"][:10],
        "source_checked": source_eval is not None,
        "source_passed": source_eval["passed"] if source_eval else None,
        "source_policy": source_policy if source_eval else None,
        "source_compare_keys": source_compare_keys if source_eval else None,
        "source_expected_count": len(source_eval["projected_expected"]) if source_eval else 0,
        "source_actual_count": len(source_eval["projected_actual"]) if source_eval else 0,
        "source_missing_count": len(source_eval["missing_rows"]) if source_eval else 0,
        "source_duplicate_count": source_eval["duplicate_count"] if source_eval else 0,
        "source_extra_count": source_eval["extra_count"] if source_eval else 0,
        "source_order_violations": source_eval["order_violations"] if source_eval else 0,
        "source_null_field_violations": source_eval["null_field_violations"] if source_eval else 0,
        "source_missing_preview": source_eval["missing_rows"][:10] if source_eval else [],
        "source_duplicate_preview": source_eval["duplicate_preview"][:10] if source_eval else [],
        "source_extra_preview": source_eval["extra_rows"][:10] if source_eval else [],
        "source_null_field_preview": source_eval["null_field_preview"][:10] if source_eval else [],
    }
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    status = "PASS" if passed else "FAIL"
    summary_lines = [
        f"## {config.get('scenario', 'e2e-scenario')}",
        "",
        f"- Status: `{status}`",
        f"- Policy: `{policy}`",
        f"- Expected rows: `{len(sink_eval['projected_expected'])}`",
        f"- Actual rows: `{len(sink_eval['projected_actual'])}`",
        f"- Missing rows: `{len(sink_eval['missing_rows'])}`",
        f"- Duplicate rows: `{sink_eval['duplicate_count']}`",
        f"- Extra rows: `{sink_eval['extra_count']}`",
        f"- Order violations: `{sink_eval['order_violations']}`",
        f"- Null field violations: `{sink_eval['null_field_violations']}`",
        f"- Identity keys: `{', '.join(identity_keys)}`",
        f"- Compare keys: `{', '.join(compare_keys)}`",
    ]
    if source_eval:
        summary_lines.extend(
            [
                f"- Source evidence: `{'PASS' if source_eval['passed'] else 'FAIL'}`",
                f"- Source policy: `{source_policy}`",
                f"- Source actual rows: `{len(source_eval['projected_actual'])}`",
                f"- Source missing rows: `{len(source_eval['missing_rows'])}`",
                f"- Source duplicate rows: `{source_eval['duplicate_count']}`",
                f"- Source extra rows: `{source_eval['extra_count']}`",
                f"- Source null field violations: `{source_eval['null_field_violations']}`",
            ]
        )
    if sink_eval["missing_rows"]:
        summary_lines.extend(["", "### Missing Preview", "", "```json", json.dumps(sink_eval["missing_rows"][:5], indent=2, sort_keys=True), "```"])
    if sink_eval["duplicate_preview"]:
        summary_lines.extend(["", "### Duplicate Preview", "", "```json", json.dumps(sink_eval["duplicate_preview"][:5], indent=2, sort_keys=True), "```"])
    if sink_eval["extra_rows"]:
        summary_lines.extend(["", "### Extra Preview", "", "```json", json.dumps(sink_eval["extra_rows"][:5], indent=2, sort_keys=True), "```"])
    if sink_eval["null_field_preview"]:
        summary_lines.extend(["", "### Null Field Preview", "", "```json", json.dumps(sink_eval["null_field_preview"][:5], indent=2, sort_keys=True), "```"])
    if source_eval and source_eval["missing_rows"]:
        summary_lines.extend(["", "### Source Missing Preview", "", "```json", json.dumps(source_eval["missing_rows"][:5], indent=2, sort_keys=True), "```"])
    if source_eval and source_eval["extra_rows"]:
        summary_lines.extend(["", "### Source Extra Preview", "", "```json", json.dumps(source_eval["extra_rows"][:5], indent=2, sort_keys=True), "```"])
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
