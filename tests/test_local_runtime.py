from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from test_orchestration_e2e import _project
from test_snapshot_store import _request, _store

from dbtv.cli.app import main
from dbtv.config.loader import load_config
from dbtv.config.schema import DbtvConfig
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CacheError, CompatibilityError, PolicyError
from dbtv.core.models import ExtractionBatch, SnapshotAction, SourceMode
from dbtv.orchestration import CommandOptions, Orchestrator
from dbtv.project.discovery import discover_project
from dbtv.project.fingerprint import project_fingerprint
from dbtv.resources import RunBudget
from dbtv.sources import connection_scope
from dbtv.state import StateIndex
from dbtv.validation import compare_runs, execution_context, replay_run, validate_incremental
from dbtv.workspace import Workspace


def parquet_project(tmp_path: Path) -> tuple[Path, Path]:
    project, profiles = _project(tmp_path)
    raw = json.loads((project / "dbtv.yml").read_text())
    raw["source"] = {
        "connector": "parquet",
        "credential_resolver": "none",
        "plugin": {"tables": {"REMOTE.RAW.orders": "orders.parquet"}},
    }
    (project / "dbtv.yml").write_text(json.dumps(raw))
    pq.write_table(
        pa.table({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]}), project / "orders.parquet"
    )
    return project, profiles


def test_connection_scope_separates_accounts_roles_and_ignores_secrets(tmp_path: Path) -> None:
    project_dir, profiles = _project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    path = profiles / "profiles.yml"

    def profile(account: str, role: str, password: str) -> str:
        path.write_text(
            json.dumps(
                {
                    "analytics_profile": {
                        "outputs": {
                            "dev": {
                                "type": "snowflake",
                                "account": account,
                                "role": role,
                                "password": password,
                                "user": "person",
                            }
                        }
                    }
                }
            )
        )
        return connection_scope("default", config.source, project, config)

    first = profile("one", "reader", "{{ env_var('MISSING_SECRET') }}")
    assert first == profile("one", "reader", "different-secret")
    assert first != profile("two", "reader", "different-secret")
    assert first != profile("one", "admin", "different-secret")


def test_dataset_activation_rolls_back_and_recovery_ignores_unpublished(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    request = replace(_request(), connection_scope="scope-one")

    def write(value: int):
        return store.write(
            request,
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": [value]}))],
            cancellation=CancellationToken(),
            invocation_id=f"write-{value}",
            activate=False,
        )

    first = write(1)
    state.commit_dataset(
        "first",
        {
            "dataset_id": "first",
            "snapshots": {
                first.source.unique_id: first.snapshot_key,
            },
        },
    )
    second = write(2)
    with pytest.raises(CacheError):
        state.commit_dataset(
            "bad",
            {
                "snapshots": {
                    second.source.unique_id: second.snapshot_key,
                    "missing": "sha256:missing",
                }
            },
        )
    assert (
        state.active_snapshot(request.source.unique_id, first.request_fingerprint).snapshot_key
        == first.snapshot_key
    )
    store.rebuild_index()
    assert (
        store.decide(request, provider="fake", mode="offline").snapshot.snapshot_key
        == first.snapshot_key
    )
    store.retain_previous = 0
    state.activate(request.source.unique_id, second.snapshot_key)
    store.garbage_collect()
    assert first.root.is_dir(), "Dataset references protect historical replay inputs"


def test_scoped_snapshots_cannot_be_reused_across_connections(tmp_path: Path) -> None:
    store, _ = _store(tmp_path)
    request = replace(_request(), connection_scope="account-a")
    store.write(
        request,
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="write",
    )
    assert (
        store.decide(
            replace(request, connection_scope="account-b"), provider="fake", mode="offline"
        ).action
        is SnapshotAction.MISSING
    )


def test_actual_resource_budget_counts_all_sources(tmp_path: Path) -> None:
    config = DbtvConfig().resolve_paths(tmp_path)
    budget = RunBudget(config, tmp_path, CancellationToken(), max_rows=3, max_bytes="10B")
    budget.consume("source-a", 2, 6)
    with pytest.raises(PolicyError, match="Arrow bytes"):
        budget.consume("source-b", 2, 6)
    rows = RunBudget(config, tmp_path, CancellationToken(), max_rows=3)
    rows.consume("source-a", 2, 1)
    with pytest.raises(PolicyError, match="rows"):
        rows.consume("source-a", 2, 1)


def test_fingerprint_tracks_installed_package_macros(tmp_path: Path) -> None:
    path = tmp_path / "dbt_packages" / "example" / "macros"
    path.mkdir(parents=True)
    macro = path / "helper.sql"
    macro.write_text("select 1")
    first = project_fingerprint(tmp_path)
    macro.write_text("select 2")
    assert project_fingerprint(tmp_path) != first


def test_parquet_run_offline_replay_and_exact_comparison(tmp_path: Path) -> None:
    project_dir, profiles = parquet_project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    cold = Orchestrator().execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run(),
        options=CommandOptions(command="build", capture_results=True),
    )
    assert cold.summary.exit_code == 0
    assert not cold.summary.remote_connection_attempted
    assert cold.summary.remote_query_count == 0
    assert cold.summary.dataset_id
    doctor = CliRunner().invoke(
        main, ["--project-dir", str(project_dir), "--profiles-dir", str(profiles), "doctor"]
    )
    assert doctor.exit_code == 0, doctor.output
    original_bytes = config.local.database.read_bytes()
    (project_dir / "orders.parquet").unlink()
    replay = replay_run(
        project=project, config=config, workspace=workspace, original_id=cold.summary.invocation_id
    )
    assert replay.summary.exit_code == 0
    assert replay.summary.snapshots_reused == 1
    assert config.local.database.read_bytes() == original_bytes
    assert compare_runs(
        workspace, cold.summary.invocation_id, replay.summary.invocation_id, config
    )["equal"]
    model = project_dir / "models" / "stg_orders.sql"
    model.write_text(model.read_text().replace("id, amount", "id, amount * 2 as amount"))
    with pytest.raises(CompatibilityError, match="changed"):
        replay_run(
            project=project,
            config=config,
            workspace=workspace,
            original_id=cold.summary.invocation_id,
        )
    edited = replay_run(
        project=project,
        config=config,
        workspace=workspace,
        original_id=cold.summary.invocation_id,
        allow_code_change=True,
    )
    comparison = compare_runs(
        workspace, cold.summary.invocation_id, edited.summary.invocation_id, config
    )
    assert not comparison["equal"]
    assert comparison["results"][0]["left_only_rows"] == 3
    inspected = CliRunner().invoke(
        main,
        [
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles),
            "--output",
            "json",
            "inspect",
            "--query",
            "select * from REMOTE.RAW.orders",
        ],
    )
    assert inspected.exit_code == 0, inspected.output
    assert len(json.loads(inspected.output)["rows"]) == 3


def test_cohort_preserves_children_and_offline_uses_no_original_files(tmp_path: Path) -> None:
    project_dir, profiles = parquet_project(tmp_path)
    raw = json.loads((project_dir / "dbtv.yml").read_text())
    raw["connections"] = {
        "customers": {
            "connector": "parquet",
            "credential_resolver": "none",
            "plugin": {"tables": {"REMOTE.RAW.customers": "customers.parquet"}},
        }
    }
    raw["routes"] = [{"select": "source:app.customers", "connection": "customers"}]
    raw["data_profiles"]["developer"]["default"]["limit"] = 1
    raw["data_profiles"]["developer"]["cohorts"] = [
        {
            "select": "source:app.orders",
            "parent": "source:app.customers",
            "parent_key": "id",
            "key": "customer_id",
            "max_keys": 1,
        }
    ]
    (project_dir / "dbtv.yml").write_text(json.dumps(raw))
    with (project_dir / "models" / "sources.yml").open("a") as handle:
        handle.write("      - name: customers\n")
    (project_dir / "models" / "customers.sql").write_text(
        "select * from {{ source('app', 'customers') }}"
    )
    pq.write_table(pa.table({"id": [7]}), project_dir / "customers.parquet")
    pq.write_table(
        pa.table({"id": [1, 2, 3], "customer_id": [7, 7, 9], "amount": [1.0, 2.0, 3.0]}),
        project_dir / "orders.parquet",
    )
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    result = Orchestrator().execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run(),
        options=CommandOptions(command="sync"),
    )
    assert {item.source.table_name: item.row_count for item in result.snapshots} == {
        "orders": 2,
        "customers": 1,
    }
    # A parent capture may finish before an over-budget child fails. Recovery
    # must preserve the previously published complete dataset.
    pq.write_table(pa.table({"id": [9]}), project_dir / "customers.parquet")
    pq.write_table(
        pa.table({"id": [3, 4], "customer_id": [9, 9], "amount": [3.0, 4.0]}),
        project_dir / "orders.parquet",
    )
    limited = config.model_copy(
        update={
            "policy": config.policy.model_copy(
                update={
                    "max_rows_per_source": 1,
                }
            )
        }
    )
    with pytest.raises(PolicyError, match="rows"):
        Orchestrator().execute(
            project=project,
            config=limited,
            workspace=workspace,
            run=workspace.create_run(),
            options=CommandOptions(command="sync", source_mode=SourceMode.REFRESH),
        )
    state = StateIndex(workspace.state_path, workspace.locks)
    assert len(state.list_datasets()) == 1
    from datetime import timedelta

    from dbtv.snapshot.store import ParquetSnapshotStore

    store = ParquetSnapshotStore(
        config.cache.root, state=state, locks_dir=workspace.locks, ttl=timedelta(hours=1)
    )
    store.rebuild_index()
    (project_dir / "customers.parquet").unlink()
    (project_dir / "orders.parquet").unlink()
    offline = Orchestrator().execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run(),
        options=CommandOptions(command="sync", source_mode=SourceMode.OFFLINE),
    )
    assert offline.summary.snapshots_reused == 2
    assert offline.summary.dataset_id == result.summary.dataset_id


def test_incremental_scenario_detects_missing_updates(tmp_path: Path) -> None:
    project_dir, profiles = parquet_project(tmp_path)
    model = project_dir / "models" / "stg_orders.sql"
    model.write_text(
        "{{ config(materialized='incremental', unique_key='id') }}\n"
        "select id, amount from {{ source('app', 'orders') }}\n"
        "{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}"
    )
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    first = Orchestrator().execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run(),
        options=CommandOptions(command="sync"),
    )
    pq.write_table(
        pa.table({"id": [1, 2, 3, 4], "amount": [99.0, 20.0, 30.0, 40.0]}),
        project_dir / "orders.parquet",
    )
    second = Orchestrator().execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run(),
        options=CommandOptions(command="sync", source_mode=SourceMode.REFRESH),
    )
    report = validate_incremental(
        project=project,
        config=config,
        workspace=workspace,
        base_dataset=first.summary.dataset_id,
        next_dataset=second.summary.dataset_id,
        select=(),
    )
    assert not report["equal"], "Append-only filtering misses a changed existing id"
    assert report["results"][0]["left_only_rows"] == 1
    assert not config.local.database.exists(), "Scenario output state must remain isolated"


def test_replay_context_tracks_environment_without_storing_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "model.sql").write_text("select {{ env_var('DBTV_CONTEXT_TEST') }}")
    config = DbtvConfig().resolve_paths(tmp_path)
    monkeypatch.setenv("DBTV_CONTEXT_TEST", "first-secret-value")
    first = execution_context(config, tmp_path)
    monkeypatch.setenv("DBTV_CONTEXT_TEST", "second-secret-value")
    assert execution_context(config, tmp_path) != first
    assert "secret" not in first


def test_tighter_ttl_takes_effect_on_existing_snapshot(tmp_path: Path) -> None:
    from datetime import timedelta

    store, _ = _store(tmp_path)
    request = _request()
    store.write(
        request,
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="first",
    )
    store.ttl = timedelta(0)
    assert store.decide(request, provider="fake", mode="auto").action is SnapshotAction.REFRESH
    assert store.decide(request, provider="fake", mode="offline").action is SnapshotAction.REUSE


def test_comparison_counts_duplicates_and_cli_reports_difference(tmp_path: Path) -> None:
    from dbtv.core.hashing import sha256_file

    workspace = Workspace(tmp_path)
    (tmp_path / "dbt_project.yml").write_text("name: comparison\nprofile: comparison\n")
    runs = [workspace.create_run(), workspace.create_run()]
    for run, values in zip(runs, ([1, 1, 2], [1, 2]), strict=True):
        workspace.write_json(run.root / "dataset.json", {"dataset_id": "same"})
        directory = run.root / "results"
        directory.mkdir()
        path = directory / "result.parquet"
        pq.write_table(pa.table({"id": values}), path)
        workspace.write_json(
            directory / "index.json",
            {
                "outputs": {
                    "model.example": {"file": path.name, "checksum": sha256_file(path)},
                }
            },
        )
    result = CliRunner().invoke(
        main,
        ["--project-dir", str(tmp_path), "compare", runs[0].invocation_id, runs[1].invocation_id],
    )
    assert result.exit_code == 1, result.output
    report = json.loads(result.output)
    assert report["results"][0]["left_only_rows"] == 1
    assert report["results"][0]["right_only_rows"] == 0


def test_cohort_literals_preserve_quotes_keywords_and_backslashes() -> None:
    import duckdb

    from dbtv.core.sql import validate_predicate
    from dbtv.sources import _cohort_literal

    value = "O'Reilly; select\\west"
    expression = _cohort_literal(value)
    validate_predicate(f"customer IN ({expression})")
    connection = duckdb.connect()
    try:
        assert connection.execute(f"SELECT {expression}").fetchone() == (value,)
    finally:
        connection.close()


def test_missing_local_ancestor_is_explained_without_expanding_selection(tmp_path: Path) -> None:
    project_dir, profiles = parquet_project(tmp_path)
    (project_dir / "models" / "downstream.sql").write_text(
        "select * from {{ ref('stg_orders') }}",
    )
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    run = workspace.create_run()
    with pytest.raises(CompatibilityError, match="Missing local prerequisites"):
        Orchestrator().execute(
            project=project,
            config=config,
            workspace=workspace,
            run=run,
            options=CommandOptions(command="run", select=("downstream",)),
        )
    boundaries = json.loads((run.root / "dependencies.json").read_text())
    assert boundaries[0]["unique_id"] == "model.analytics.stg_orders"
    assert boundaries[0]["available"] is False
    built = Orchestrator().execute(
        project=project,
        config=config,
        workspace=workspace,
        run=workspace.create_run(),
        options=CommandOptions(
            command="build", select=("+downstream",), source_mode=SourceMode.OFFLINE
        ),
    )
    assert built.summary.exit_code == 0


def test_identical_refresh_preserves_identity_and_advances_freshness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime, timedelta

    from dbtv.snapshot import store as module

    now = datetime.now(UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(module, "datetime", Clock)
    store, state = _store(tmp_path)

    def write(pinned=False):
        return store.write(
            _request(),
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
            cancellation=CancellationToken(),
            invocation_id="refresh",
            pinned=pinned,
        )

    first = write()
    first_metadata = (first.root / "metadata.json").read_text()
    now += timedelta(hours=2)
    assert store.decide(_request(), provider="fake", mode="auto").action is SnapshotAction.REFRESH
    second = write(pinned=True)
    assert first.snapshot_key == second.snapshot_key
    assert second.completed_at == first.completed_at
    assert second.verified_at == now.isoformat()
    assert store.decide(_request(), provider="fake", mode="auto").action is SnapshotAction.REUSE
    assert (
        json.loads((first.root / "metadata.json").read_text())["completed_at"]
        == json.loads(first_metadata)["completed_at"]
    )
    store.rebuild_index()
    assert store.decide(_request(), provider="fake", mode="auto").action is SnapshotAction.REUSE
    assert state.get_snapshot(first.snapshot_key).pinned
