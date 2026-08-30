from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from dbtv.config.loader import load_config
from dbtv.connectors.registry import ConnectorRegistry
from dbtv.core.errors import OfflineViolation
from dbtv.core.models import (
    CanonicalField,
    CanonicalSchema,
    ExtractionBatch,
    ExtractionEstimate,
    Relation,
    SnapshotRequest,
    SourceCapabilities,
    SourceMode,
    SourceVersion,
)
from dbtv.credentials.dbt_profile import ResolvedCredentialHandle
from dbtv.orchestration import CommandOptions, Orchestrator
from dbtv.project.discovery import discover_project
from dbtv.workspace import Workspace


class FakeResolver:
    def __init__(self) -> None:
        self.resolve_count = 0

    def resolve(self, **_: Any) -> ResolvedCredentialHandle:
        self.resolve_count += 1
        return ResolvedCredentialHandle("fake", {"account": "not-used"})


class FakeConnector:
    def __init__(self, factory: FakeFactory) -> None:
        self.factory = factory

    def capabilities(self) -> SourceCapabilities:
        return self.factory.capabilities()

    def open(self) -> None:
        self.factory.open_count += 1

    def close(self) -> None:
        self.factory.close_count += 1

    def inspect_schema(self, relation: Relation) -> CanonicalSchema:
        return CanonicalSchema(
            (CanonicalField("id", "int64", True),),
            "sha256:fake",
        )

    def source_version(self, relation: Relation) -> SourceVersion | None:
        return None

    def estimate(self, request: SnapshotRequest) -> ExtractionEstimate | None:
        return ExtractionEstimate(row_count=3)

    def extract(self, request: SnapshotRequest, cancellation: Any) -> list[ExtractionBatch]:
        self.factory.extract_count += 1
        return [
            ExtractionBatch(
                pa.record_batch(
                    {
                        "id": [1, 2, 3],
                        "amount": [10.0, 20.0, 30.0],
                    }
                ),
                query_id="fake-query-1",
            )
        ]

    def cancel(self, query_id: str) -> None:
        self.factory.cancel_count += 1


class FakeFactory:
    def __init__(self) -> None:
        self.create_count = 0
        self.open_count = 0
        self.close_count = 0
        self.extract_count = 0
        self.cancel_count = 0

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(True, True, True, True, True, True, False, False, False, True)

    def create(self, **_: Any) -> FakeConnector:
        self.create_count += 1
        return FakeConnector(self)


def _project(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "analytics"
    profiles = tmp_path / "profiles"
    (project / "models").mkdir(parents=True)
    profiles.mkdir()
    (project / "dbt_project.yml").write_text(
        "name: analytics\n"
        "version: '1.0'\n"
        "config-version: 2\n"
        "profile: analytics_profile\n"
        "model-paths: [models]\n",
        encoding="utf-8",
    )
    (project / "models" / "sources.yml").write_text(
        "version: 2\n"
        "sources:\n"
        "  - name: app\n"
        "    database: REMOTE\n"
        "    schema: RAW\n"
        "    tables:\n"
        "      - name: orders\n",
        encoding="utf-8",
    )
    (project / "models" / "stg_orders.sql").write_text(
        "{{ config(materialized='table') }}\n"
        "select id, amount from {{ source('app', 'orders') }}\n",
        encoding="utf-8",
    )
    (project / "models" / "stg_orders.yml").write_text(
        "version: 2\n"
        "models:\n"
        "  - name: stg_orders\n"
        "    columns:\n"
        "      - name: id\n"
        "        data_tests:\n"
        "          - not_null\n",
        encoding="utf-8",
    )
    (profiles / "profiles.yml").write_text(
        "analytics_profile:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {tmp_path / 'production-parse-only.duckdb'}\n"
        "      schema: analytics\n",
        encoding="utf-8",
    )
    (project / "dbtv.yml").write_text(
        json.dumps(
            {
                "version": 1,
                "project": {"production_target": "dev"},
                "source": {"connector": "fake"},
                "local": {"database": ".dbtv/local.duckdb", "schema": "dbtv_dev"},
                "data_profiles": {"developer": {"default": {"strategy": "limit", "limit": 10}}},
            }
        ),
        encoding="utf-8",
    )
    return project, profiles


def test_cold_run_then_zero_connector_offline_run(tmp_path: Path) -> None:
    project_dir, profiles_dir = _project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles_dir)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    factory = FakeFactory()
    resolver = FakeResolver()
    orchestrator = Orchestrator(
        connector_registry=ConnectorRegistry(factories={"fake": factory}),
        credential_resolver=resolver,  # type: ignore[arg-type]
    )
    cold = orchestrator.execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run("00000000-0000-4000-8000-000000000001"),
        options=CommandOptions(command="run", select=("stg_orders",)),
    )
    assert cold.summary.exit_code == 0
    assert cold.summary.snapshots_refreshed == 1
    assert cold.summary.remote_query_count == 1
    assert factory.create_count == 1
    assert factory.extract_count == 1
    assert resolver.resolve_count == 1
    connection = duckdb.connect(str(config.local.database), read_only=True)
    try:
        assert connection.execute("select count(*) from dbtv_dev.stg_orders").fetchone() == (3,)
    finally:
        connection.close()

    offline = orchestrator.execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run("00000000-0000-4000-8000-000000000002"),
        options=CommandOptions(
            command="run",
            select=("stg_orders",),
            source_mode=SourceMode.OFFLINE,
        ),
    )
    assert offline.summary.exit_code == 0
    assert not offline.summary.remote_connection_attempted
    assert offline.summary.remote_query_count == 0
    assert offline.summary.snapshots_reused == 1
    assert factory.create_count == 1
    assert resolver.resolve_count == 1

    tested = orchestrator.execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run("00000000-0000-4000-8000-000000000003"),
        options=CommandOptions(
            command="test",
            select=("stg_orders",),
            source_mode=SourceMode.OFFLINE,
        ),
    )
    assert tested.summary.exit_code == 0
    assert tested.summary.remote_query_count == 0
    assert factory.create_count == 1

    built = orchestrator.execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run("00000000-0000-4000-8000-000000000005"),
        options=CommandOptions(
            command="build",
            select=("stg_orders",),
            source_mode=SourceMode.OFFLINE,
        ),
    )
    assert built.summary.exit_code == 0
    assert built.summary.dbt_results
    assert all(result.status in {"success", "pass"} for result in built.summary.dbt_results)


def test_missing_offline_snapshot_never_resolves_credentials_or_connector(
    tmp_path: Path,
) -> None:
    project_dir, profiles_dir = _project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles_dir)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    factory = FakeFactory()
    resolver = FakeResolver()
    orchestrator = Orchestrator(
        connector_registry=ConnectorRegistry(offline=True, factories={"fake": factory}),
        credential_resolver=resolver,  # type: ignore[arg-type]
    )
    with pytest.raises(OfflineViolation, match="unavailable"):
        orchestrator.execute(
            project=project,
            config=config,
            workspace=workspace,
            run=workspace.create_run("00000000-0000-4000-8000-000000000004"),
            options=CommandOptions(
                command="run",
                select=("stg_orders",),
                source_mode=SourceMode.OFFLINE,
            ),
        )
    assert resolver.resolve_count == 0
    assert factory.create_count == 0


def test_sync_commits_data_without_creating_local_database(tmp_path: Path) -> None:
    project_dir, profiles_dir = _project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles_dir)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    factory = FakeFactory()
    result = Orchestrator(
        connector_registry=ConnectorRegistry(factories={"fake": factory}),
        credential_resolver=FakeResolver(),  # type: ignore[arg-type]
    ).execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run("00000000-0000-4000-8000-000000000006"),
        options=CommandOptions(command="sync", select=("stg_orders",)),
    )
    assert result.summary.exit_code == 0
    assert result.summary.snapshots_refreshed == 1
    assert not config.local.database.exists()
