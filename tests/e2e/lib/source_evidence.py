#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build normalized source evidence rows from service logs.")
    parser.add_argument("--mode", required=True, choices=["nginx-access", "redis-monitor", "memcached-verbose", "json-lines"])
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--source-id", required=True)
    return parser.parse_args()


def derive_seq(event_id: str) -> int | None:
    match = re.search(r":(\d+)$", event_id)
    if not match:
        return None
    return int(match.group(1))


def parse_nginx_access(path: Path, scenario: str, source_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    prefix = f"/e2e/{scenario}/"
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or not line.startswith(prefix):
                continue
            log_path, _, status_text = line.partition("|")
            event_id = log_path.removeprefix(prefix)
            rows.append(
                {
                    "scenario": scenario,
                    "source_id": source_id,
                    "event_id": event_id,
                    "seq": derive_seq(event_id),
                    "method": "GET",
                    "path": log_path,
                    "status": int(status_text),
                }
            )
    return rows


def parse_redis_monitor(path: Path, scenario: str, source_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pattern = re.compile(r'"(?P<command>[A-Z]+)" "(?P<key>[^"]+)" "(?P<value>[^"]+)"')
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if scenario not in line or '"SET"' not in line:
                continue
            match = pattern.search(line)
            if not match:
                continue
            key = match.group("key")
            rows.append(
                {
                    "scenario": scenario,
                    "source_id": source_id,
                    "event_id": key,
                    "seq": derive_seq(key),
                    "command": match.group("command"),
                    "key": key,
                    "value": match.group("value"),
                }
            )
    return rows


def parse_memcached_verbose(path: Path, scenario: str, source_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pattern = re.compile(r"\b(?P<command>set|get)\s+(?P<key>[^ ]+)")
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if scenario not in line:
                continue
            match = pattern.search(line)
            if not match:
                continue
            key = match.group("key")
            rows.append(
                {
                    "scenario": scenario,
                    "source_id": source_id,
                    "event_id": key,
                    "seq": derive_seq(key),
                    "command": match.group("command"),
                    "key": key,
                }
            )
    return rows


def parse_json_lines(path: Path, scenario: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            row = json.loads(line)
            if row.get("scenario") == scenario:
                rows.append(row)
    return rows


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if args.mode == "nginx-access":
        rows = parse_nginx_access(input_path, args.scenario, args.source_id)
    elif args.mode == "redis-monitor":
        rows = parse_redis_monitor(input_path, args.scenario, args.source_id)
    elif args.mode == "memcached-verbose":
        rows = parse_memcached_verbose(input_path, args.scenario, args.source_id)
    else:
        rows = parse_json_lines(input_path, args.scenario)

    output_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
