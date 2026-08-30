from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize redacted dbtv run artifacts")
    parser.add_argument("run_json", nargs="+", type=Path)
    args = parser.parse_args()
    rows = [_row(path) for path in args.run_json]
    fields = [
        "invocation_id",
        "command",
        "state",
        "remote_connection_attempted",
        "remote_query_count",
        "snapshots_reused",
        "snapshots_refreshed",
        "planning_seconds",
        "extraction_seconds",
        "binding_seconds",
        "dbt_seconds",
        "artifact",
    ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)


def _row(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    timings = raw.get("timings") or {}
    return {
        "invocation_id": raw.get("invocation_id"),
        "command": raw.get("command"),
        "state": raw.get("state"),
        "remote_connection_attempted": raw.get("remote_connection_attempted"),
        "remote_query_count": raw.get("remote_query_count"),
        "snapshots_reused": raw.get("snapshots_reused"),
        "snapshots_refreshed": raw.get("snapshots_refreshed"),
        "planning_seconds": timings.get("planning"),
        "extraction_seconds": timings.get("extraction"),
        "binding_seconds": timings.get("binding"),
        "dbt_seconds": timings.get("dbt"),
        "artifact": str(path),
    }


if __name__ == "__main__":
    main()
