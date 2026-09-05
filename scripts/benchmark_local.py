"""Reproducible local benchmark: uv run python scripts/benchmark_local.py --output report.json."""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path

import duckdb

from dbtv.config.loader import load_config
from dbtv.core.models import SourceMode
from dbtv.datasets import engine_versions
from dbtv.orchestration import CommandOptions, Orchestrator
from dbtv.project.discovery import discover_project
from dbtv.workspace import Workspace


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.rows <= 5_000_000 or not 1 <= args.repeats <= 20:
        parser.error("rows must be 1..5,000,000 and repeats 1..20")
    measurements = []
    with tempfile.TemporaryDirectory(prefix="dbtv-benchmark-") as temporary:
        root = Path(temporary)
        (root / "models").mkdir()
        (root / "profiles").mkdir()
        (root / "dbt_project.yml").write_text(
            "name: benchmark\nversion: '1.0'\nconfig-version: 2\nprofile: benchmark\n"
        )
        (root / "profiles" / "profiles.yml").write_text(
            f"benchmark:\n  outputs:\n    dev:\n      type: duckdb\n"
            f"      path: {root / 'parse-only.duckdb'}\n      schema: main\n"
        )
        (root / "models" / "sources.yml").write_text(
            "version: 2\nsources:\n  - name: app\n    database: raw\n"
            "    schema: app\n    tables:\n      - name: events\n"
        )
        model = root / "models" / "account_totals.sql"
        model.write_text(
            "{{ config(materialized='table') }}\n"
            "select account_id, count(*) as events, sum(amount) as total "
            "from {{ source('app', 'events') }} group by account_id\n"
        )
        (root / "dbtv.yml").write_text(
            json.dumps(
                {
                    "project": {"production_target": "dev"},
                    "source": {
                        "connector": "parquet",
                        "credential_resolver": "none",
                        "plugin": {"tables": {"raw.app.events": "events.parquet"}},
                    },
                    "data_profiles": {
                        "developer": {"default": {"strategy": "limit", "limit": args.rows}}
                    },
                }
            )
        )
        connection = duckdb.connect()
        connection.execute(
            f"COPY (SELECT i as event_id, i % 1000 as account_id, "
            f"(i % 100)::DOUBLE / 10 AS amount FROM range({args.rows}) t(i)) "
            f"TO '{root / 'events.parquet'}' (FORMAT PARQUET)"
        )
        connection.close()
        input_bytes = (root / "events.parquet").stat().st_size
        project = discover_project(root, profiles_dir=root / "profiles")
        config = load_config(root)
        workspace = Workspace(root)
        for scenario in ("cold", "warm", "edited", "offline"):
            if scenario == "edited":
                model.write_text(model.read_text() + "-- measured edit\n")
            if scenario == "offline":
                (root / "events.parquet").unlink()
            for _ in range(args.repeats if scenario in {"warm", "offline"} else 1):
                run = workspace.create_run()
                start = time.perf_counter()
                result = Orchestrator().execute(
                    project=project,
                    config=config,
                    workspace=workspace,
                    run=run,
                    options=CommandOptions(
                        command="run",
                        source_mode=(
                            SourceMode.OFFLINE if scenario == "offline" else SourceMode.AUTO
                        ),
                    ),
                )
                if result.summary.exit_code:
                    raise RuntimeError(result.dbt_stdout + result.dbt_stderr)
                measurements.append(
                    {
                        "scenario": scenario,
                        "wall_seconds": time.perf_counter() - start,
                        "timings": result.summary.timings,
                        "refreshed": result.summary.snapshots_refreshed,
                        "reused": result.summary.snapshots_reused,
                        "remote_queries": result.summary.remote_query_count,
                        "parsing": json.loads((run.root / "parsing.json").read_text()),
                    }
                )
                print(f"{scenario}: {measurements[-1]['wall_seconds']:.3f}s", flush=True)
        report = {
            "rows": args.rows,
            "input_parquet_bytes": input_bytes,
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "engine_versions": engine_versions(),
            "scope": "Local synthetic aggregation; no warehouse or production speedup claim.",
            "measurements": measurements,
            "median_seconds": {
                scenario: statistics.median(
                    item["wall_seconds"] for item in measurements if item["scenario"] == scenario
                )
                for scenario in ("cold", "warm", "edited", "offline")
            },
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
