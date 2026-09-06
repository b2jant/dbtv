from __future__ import annotations

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_local_runtime import parquet_project
from test_project_contracts import manifest_raw, node

from dbtv import validation
from dbtv.config.loader import load_config
from dbtv.config.schema import DbtvConfig
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CacheError, CompatibilityError, ConfigError, PolicyError
from dbtv.core.hashing import sha256_file
from dbtv.datasets import engine_versions
from dbtv.orchestration import CommandOptions, Orchestrator
from dbtv.project.discovery import discover_project
from dbtv.project.fingerprint import project_fingerprint
from dbtv.state import StateIndex
from dbtv.workspace import Workspace


@pytest.fixture
def comparisons(tmp_path):
    config = DbtvConfig().resolve_paths(tmp_path)
    workspace = Workspace(tmp_path)
    runs = [workspace.create_run(), workspace.create_run()]

    def write(run, values=(1, 2), column="id", dataset="same", output="model.p.a"):
        workspace.write_json(run.root / "dataset.json", {"dataset_id": dataset})
        path = run.root / "results" / "data.parquet"
        path.parent.mkdir(exist_ok=True)
        pq.write_table(pa.table({column: list(values)}), path)
        workspace.write_json(
            path.parent / "index.json",
            {"outputs": {output: {"file": path.name, "checksum": sha256_file(path)}}},
        )

    for run in runs:
        write(run)

    def compare(**kwargs):
        return validation.compare_runs(
            workspace, runs[0].invocation_id, runs[1].invocation_id, config, **kwargs
        )

    return workspace, runs, write, compare


@pytest.mark.parametrize("difference", ["schema", "output", "input"])
def test_comparison_reports_schema_missing_output_and_different_inputs(comparisons, difference):
    _, runs, write, compare = comparisons
    write(
        runs[1],
        **{
            "schema": {"column": "renamed"},
            "output": {"output": "model.p.b"},
            "input": {"dataset": "different"},
        }[difference],
    )
    if difference == "input":
        with pytest.raises(CompatibilityError, match="same frozen inputs"):
            compare()
        report = compare(allow_different_inputs=True)
        assert report["equal"] and not report["same_inputs"]
    else:
        report = compare()
        assert not report["equal"]
        assert report["results"][0]["reason"] == (
            "schema differs" if difference == "schema" else "missing output"
        )


@pytest.mark.parametrize("damage", ["index", "file", "checksum", "unsafe", "empty"])
def test_comparison_rejects_invalid_capture_artifacts(comparisons, damage):
    workspace, runs, _, compare = comparisons
    directory = runs[1].root / "results"
    index_path = directory / "index.json"
    index = json.loads(index_path.read_text())
    if damage == "index":
        index_path.unlink()
    elif damage == "file":
        (directory / "data.parquet").unlink()
    elif damage == "checksum":
        (directory / "data.parquet").write_bytes(b"corrupt")
    elif damage == "unsafe":
        index["outputs"]["model.p.a"]["file"] = "../../outside.parquet"
        workspace.write_json(index_path, index)
    else:
        for run in runs:
            workspace.write_json(run.root / "results" / "index.json", {"outputs": {}})
    with pytest.raises((CacheError, ConfigError)):
        compare()


def test_comparison_surfaces_budget_failure_and_stops_monitor(comparisons, monkeypatch):
    _, _, _, compare = comparisons
    budget = Mock(failure=PolicyError("disk budget"))
    budget.start.side_effect = OSError("disk unavailable")
    monkeypatch.setattr(validation, "RunBudget", lambda *a, **kw: budget)
    with pytest.raises(PolicyError, match="disk budget"):
        compare()
    budget.stop.assert_called_once()


def test_output_capture_skips_unmaterialized_nodes_and_reads_attached_catalog(tmp_path):
    config = DbtvConfig().resolve_paths(tmp_path)
    workspace = Workspace(tmp_path)
    run = workspace.create_run()
    database = duckdb.connect(str(config.local.database))
    database.close()
    attached = tmp_path / "output.duckdb"
    with duckdb.connect(str(attached)) as connection:
        connection.execute("create table example as select 1 as id")
    raw = manifest_raw(
        {
            "model.p.a": node(database="output", schema="main"),
            "model.p.ephemeral": node(config={"materialized": "ephemeral"}),
            "test.p.a": node("test"),
        }
    )
    (run.local_target_path / "manifest.json").write_text(json.dumps(raw))
    validation.capture_outputs(
        config=config,
        workspace=workspace,
        run=run,
        selected=("model.p.a", "model.p.ephemeral", "test.p.a", "missing"),
        attachments={"output": attached},
        token=CancellationToken(),
    )
    outputs = json.loads((run.root / "results" / "index.json").read_text())["outputs"]
    assert list(outputs) == ["model.p.a"]
    assert outputs["model.p.a"]["rows"] == 1


@pytest.fixture
def replay_fixture(tmp_path):
    project_dir, profiles = parquet_project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    workspace = Workspace(project_dir)
    run = workspace.create_run()
    record = {
        "dataset_id": "sha256:fixture",
        "project_fingerprint": project_fingerprint(project_dir),
        "engine_versions": engine_versions(),
        "invocation": asdict(CommandOptions(command="run")),
    }
    workspace.write_json(run.root / "dataset.json", record)
    workspace.write_json(
        run.root / "execution-context.json",
        {"config_fingerprint": validation.execution_context(config, project_dir)},
    )
    workspace.write_json(
        run.local_target_path / "manifest.json",
        manifest_raw({"model.p.a": node(config={"materialized": "incremental"})}),
    )
    workspace.write_json(run.plan_path, {"local_selected_ids": ["missing", "model.p.a"]})
    return project, config, workspace, run, record


@pytest.mark.parametrize("kind", ["redacted", "sync", "incremental"])
def test_replay_rejects_unreplayable_invocations(replay_fixture, kind):
    project, config, workspace, run, record = replay_fixture
    if kind == "redacted":
        record["invocation"]["variables"] = "<redacted>"
    if kind == "sync":
        record["invocation"]["command"] = "sync"
    workspace.write_json(run.root / "dataset.json", record)
    with pytest.raises((ConfigError, CompatibilityError)):
        validation.replay_run(
            project=project, config=config, workspace=workspace, original_id=run.invocation_id
        )


def test_replay_full_refresh_preserves_frozen_dataset_and_isolates_outputs(
    replay_fixture, monkeypatch
):
    project, config, workspace, run, record = replay_fixture
    execute = Mock(return_value=SimpleNamespace(summary=SimpleNamespace(exit_code=0)))
    monkeypatch.setattr(Orchestrator, "execute", execute)
    validation.replay_run(
        project=project,
        config=config,
        workspace=workspace,
        original_id=run.invocation_id,
        full_refresh=True,
    )
    kwargs = execute.call_args.kwargs
    assert kwargs["options"].dataset_id == record["dataset_id"]
    assert kwargs["options"].full_refresh
    assert kwargs["config"].local.database != config.local.database
    with pytest.raises(ConfigError):
        validation.read_run(workspace, "../invalid")


def test_dynamic_environment_variables_change_replay_context(tmp_path, monkeypatch):
    (tmp_path / "macro.sql").write_text("{{ env_var(variable_name) }}")
    config = DbtvConfig().resolve_paths(tmp_path)
    first = validation.execution_context(config, tmp_path)
    monkeypatch.setenv("DBTV_DYNAMIC_CONTEXT_CANARY", "changed")
    assert validation.execution_context(config, tmp_path) != first


@pytest.mark.parametrize(
    "scenario", ["redacted", "seed_failure", "not_incremental", "step_failure", "reference_failure"]
)
def test_incremental_validation_rejects_invalid_or_failed_scenarios(
    replay_fixture, monkeypatch, scenario
):
    project, config, workspace, _, record = replay_fixture
    if scenario == "redacted":
        record["invocation"]["variables"] = "<redacted>"
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize()
    state.commit_dataset("base", {"snapshots": {}, "invocation": record["invocation"]})
    calls = []

    def execute(**kwargs):
        calls.append(kwargs)
        selected = "table" if scenario == "not_incremental" else "incremental"
        workspace.write_json(
            kwargs["run"].local_target_path / "manifest.json",
            manifest_raw({"model.p.a": node(config={"materialized": selected})}),
        )
        failed = {"seed_failure": 1, "step_failure": 2, "reference_failure": 3}.get(scenario)
        return SimpleNamespace(
            summary=SimpleNamespace(exit_code=int(len(calls) == failed)),
            plan=SimpleNamespace(local_selected_ids=("missing", "model.p.a")),
        )

    monkeypatch.setattr(Orchestrator, "execute", lambda self, **kw: execute(**kw))
    with pytest.raises((CompatibilityError, ConfigError)):
        validation.validate_incremental(
            project=project,
            config=config,
            workspace=workspace,
            base_dataset="base",
            next_dataset="next",
            select=("+model.p.a",),
        )
    if len(calls) == 3:
        assert calls[0]["options"].full_refresh
        assert not calls[1]["options"].full_refresh
        assert calls[2]["options"].full_refresh
