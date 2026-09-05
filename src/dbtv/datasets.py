from __future__ import annotations

from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from dbtv.core.hashing import sha256_value
from dbtv.core.models import DatasetSnapshot, ExecutionPlan
from dbtv.core.redaction import redact
from dbtv.state import StateIndex
from dbtv.workspace import RunWorkspace, Workspace


def engine_versions() -> dict[str, str]:
    result = {}
    for name in ("dbtv", "dbt-core", "dbt-duckdb", "duckdb", "pyarrow", "dbt-snowflake"):
        try:
            result[name] = version(name)
        except PackageNotFoundError:
            result[name] = "not-installed"
    return result


def commit_dataset(
    state: StateIndex,
    workspace: Workspace,
    run: RunWorkspace,
    plan: ExecutionPlan,
    snapshots: tuple[DatasetSnapshot, ...],
    invocation: dict[str, Any],
) -> str:
    identity = {
        "format_version": 1,
        "snapshots": {item.source.unique_id: item.snapshot_key for item in snapshots},
        "source_scopes": {item.source.unique_id: item.connection_scope for item in snapshots},
    }
    dataset_id = sha256_value(identity)
    payload = {
        **identity,
        "dataset_id": dataset_id,
        "created_by_run": run.invocation_id,
        "project_fingerprint": plan.project_fingerprint,
        "engine_versions": engine_versions(),
        "invocation": invocation,
        "capture_consistency": "Independent source captures; no cross-source transaction implied.",
        "sources": [
            {
                "source_id": item.source.unique_id,
                "created_at": item.created_at,
                "completed_at": item.completed_at,
                "verified_at": item.verified_at or item.completed_at,
                "sampling": asdict(item.sampling),
                "rows": item.row_count,
                "bytes": item.byte_count,
                "source_version": asdict(item.source_version) if item.source_version else None,
            }
            for item in snapshots
        ],
    }
    payload = redact(payload)
    state.commit_dataset(dataset_id, payload)
    # SQLite commits activation of every member and the dataset together. The sidecar
    # is written afterwards, so it is also a record of a completed activation.
    directory = workspace.root / "datasets"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{dataset_id.split(':', 1)[1]}.json"
    if not path.exists():
        workspace.write_json(path, state.get_dataset(dataset_id))
    workspace.write_json(run.root / "dataset.json", payload)
    return dataset_id
