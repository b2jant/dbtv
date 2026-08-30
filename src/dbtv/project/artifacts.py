from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dbtv.core.errors import ManifestError
from dbtv.core.models import DbtNodeResult


def load_run_results(path: Path) -> tuple[DbtNodeResult, ...]:
    if not path.is_file():
        return ()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"Unable to read dbt run results at {path}: {exc}") from exc
    results = raw.get("results") if isinstance(raw, dict) else None
    if not isinstance(results, list):
        raise ManifestError(f"dbt run results at {path} do not contain a results list.")
    normalized: list[DbtNodeResult] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        node = item.get("unique_id")
        if not isinstance(node, str):
            continue
        failures = item.get("failures")
        normalized.append(
            DbtNodeResult(
                unique_id=node,
                status=str(item.get("status", "unknown")),
                message=_optional_text(item.get("message")),
                execution_time=float(item.get("execution_time") or 0),
                failures=int(failures) if isinstance(failures, int) else None,
            )
        )
    return tuple(normalized)


def _optional_text(value: Any) -> str | None:
    return str(value) if value not in {None, ""} else None
