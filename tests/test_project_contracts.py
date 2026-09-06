from __future__ import annotations

import importlib.metadata
import json
import os
import runpy
import signal
import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dbtv.config.schema import DbtvConfig
from dbtv.core.errors import DbtInvocationError, ManifestError, ProjectError
from dbtv.core.models import ResourceType, utc_now
from dbtv.datasets import engine_versions
from dbtv.project import dbt_invoker
from dbtv.project.artifacts import load_run_results
from dbtv.project.boundaries import dependency_boundaries, lineage
from dbtv.project.discovery import discover_project
from dbtv.project.fingerprint import project_files
from dbtv.project.manifest import load_manifest, normalize_manifest
from dbtv.project.planner import ProjectPlanner
from dbtv.project.selection import parse_dbt_ls_json, upstream_sources
from dbtv.workspace import Workspace


def manifest_raw(nodes=None, sources=None):
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
        "nodes": nodes or {},
        "sources": sources or {},
    }


def node(kind="model", parents=(), **fields):
    return {
        "resource_type": kind,
        "name": "example",
        "schema": "app",
        "database": "raw",
        "source_name": "app",
        "depends_on": {"nodes": list(parents)},
        **fields,
    }


@pytest.mark.parametrize("text", ["[", "[]", "{}"])
def test_manifest_rejects_invalid_artifacts(tmp_path, text) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(text)
    with pytest.raises(ManifestError):
        load_manifest(path)
    path.unlink()
    with pytest.raises(ManifestError):
        load_manifest(path)


def test_manifest_unknown_resources_missing_relations_and_cyclic_dependencies() -> None:
    raw = manifest_raw(
        {
            "model.a": node(parents=("model.b", "missing", "source.raw")),
            "model.b": node(parents=("model.a",)),
            "unknown.x": node("future_resource"),
            "model.no_schema": node(schema=None),
            "model.no_name": node(name=None),
            "malformed": "ignore",
        },
        {"source.raw": node("source")},
    )
    manifest = normalize_manifest(raw)
    assert manifest.nodes["unknown.x"].resource_type is ResourceType.UNKNOWN
    assert manifest.nodes["model.no_schema"].relation is None
    assert manifest.nodes["model.no_name"].relation is None
    assert manifest.nodes["model.a"].source_ref is None
    assert upstream_sources(manifest, ("model.a", "model.b")) == ("source.raw",)
    graph = lineage(manifest, ("model.a",))
    assert len(graph["nodes"]) == 3
    assert dependency_boundaries(manifest, ("model.a", "model.b", "missing"), "build") == []
    assert dependency_boundaries(manifest, ("model.a",), "unknown") == []
    boundary = dependency_boundaries(manifest, ("model.a",), "run")
    assert boundary[0]["unique_id"] == "model.b"
    assert datetime.fromisoformat(utc_now()).tzinfo is not None


@pytest.mark.parametrize("text", ["[", "[]", "{}"])
def test_run_results_reject_malformed_payloads(tmp_path, text) -> None:
    path = tmp_path / "run_results.json"
    assert load_run_results(path) == ()
    path.write_text(text)
    with pytest.raises(ManifestError):
        load_run_results(path)


def test_result_normalization_skips_invalid_entries_and_retains_failures(tmp_path) -> None:
    path = tmp_path / "results.json"
    path.write_text(
        json.dumps(
            {
                "results": [
                    None,
                    {},
                    {
                        "unique_id": "test.a",
                        "status": "fail",
                        "message": "one bad row",
                        "failures": 1,
                    },
                ]
            }
        )
    )
    results = load_run_results(path)
    assert len(results) == 1
    assert results[0].failures == 1
    assert results[0].message == "one bad row"


def test_selection_filters_logs_deduplicates_and_reports_malformed_json() -> None:
    output = 'log\n{}\n{"unique_id": "model.a"}\n{invalid\n{"unique_id": "model.a"}'
    assert parse_dbt_ls_json(output) == ("model.a",)
    assert parse_dbt_ls_json("{}") == ()
    with pytest.raises(DbtInvocationError):
        parse_dbt_ls_json("{invalid")


@pytest.mark.parametrize("text", ["[", "- item", "profile: p", "name: p"])
def test_discovery_reports_invalid_project(tmp_path, text) -> None:
    path = tmp_path / "dbt_project.yml"
    path.write_text(text)
    with pytest.raises(ProjectError):
        discover_project(path)


def test_discovery_uses_environment_profiles_and_avoids_symlink_cycles(
    tmp_path, monkeypatch
) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p")
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path / "profiles"))
    assert discover_project(tmp_path).profiles_dir == tmp_path / "profiles"
    (tmp_path / "loop").symlink_to(tmp_path, target_is_directory=True)
    assert project_files(tmp_path) == (tmp_path / "dbt_project.yml",)


def test_package_metadata_fallbacks_and_module_entrypoint(monkeypatch, capsys) -> None:
    import dbtv
    import dbtv.datasets

    with monkeypatch.context() as patch:
        patch.setattr(
            importlib.metadata,
            "version",
            Mock(side_effect=importlib.metadata.PackageNotFoundError()),
        )
        namespace = runpy.run_path(dbtv.__file__)
        assert namespace["__version__"] == "0.1.0a1"
        patch.setattr(
            dbtv.datasets, "version", Mock(side_effect=importlib.metadata.PackageNotFoundError())
        )
        assert set(engine_versions().values()) == {"not-installed"}
    monkeypatch.setattr(sys, "argv", ["dbtv", "version"])
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("dbtv", run_name="__main__")
    assert exit_info.value.code == 0
    assert "dbtv" in capsys.readouterr().out
    runpy.run_module("dbtv.__main__", run_name="imported_entrypoint")


def test_dbt_executable_missing_and_command_failure(tmp_path, monkeypatch) -> None:
    with pytest.raises(DbtInvocationError, match="not found"):
        dbt_invoker.DbtInvoker(str(tmp_path / "missing")).resolved_executable()
    invoker = dbt_invoker.DbtInvoker(sys.executable)
    with pytest.raises(DbtInvocationError) as error:
        invoker.run(["-c", "import sys; print('bad input', file=sys.stderr); sys.exit(2)"])
    assert error.value.context["stderr"].strip() == "bad input"
    assert invoker.version().return_code == 0
    monkeypatch.setattr(dbt_invoker.shutil, "which", lambda _: None)
    monkeypatch.setattr(
        dbt_invoker, "os", SimpleNamespace(name="nt", path=os.path, access=os.access, X_OK=os.X_OK)
    )
    monkeypatch.setattr(dbt_invoker.sys, "executable", str(tmp_path / "python.exe"))
    executable = tmp_path / "dbt.exe"
    executable.write_text("fixture")
    executable.chmod(0o700)
    assert dbt_invoker.DbtInvoker().resolved_executable() == str(executable)


@pytest.mark.parametrize("exits_before_force", [False, True])
def test_process_termination_escalates_only_for_running_children(
    monkeypatch, exits_before_force
) -> None:
    process = Mock(pid=123)
    process.poll.side_effect = [None, 0 if exits_before_force else None]
    signals = Mock()
    timers = []
    monkeypatch.setattr(dbt_invoker.os, "killpg", signals)
    monkeypatch.setattr(
        dbt_invoker.threading, "Timer", lambda delay, callback: timers.append(callback) or Mock()
    )
    dbt_invoker._terminate_process(process)
    timers[0]()
    expected = [signal.SIGTERM] + ([] if exits_before_force else [signal.SIGKILL])
    assert [call.args[1] for call in signals.call_args_list] == expected
    signals.reset_mock()
    process.poll.side_effect = None
    process.poll.return_value = 0
    dbt_invoker._terminate_process(process)
    signals.assert_not_called()


@pytest.fixture
def planning(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("name: p\nprofile: p")
    (tmp_path / "profiles.yml").write_text("p: {}")
    project = discover_project(tmp_path, profiles_dir=tmp_path)
    config = DbtvConfig.model_validate({"project": {"production_target": "dev"}}).resolve_paths(
        tmp_path
    )
    run = Workspace(tmp_path).create_run()
    invoker = Mock()
    invoker.resolved_executable.return_value = sys.executable
    return project, config, run, ProjectPlanner(invoker)


@pytest.mark.parametrize("difference", ["missing", "incomplete"])
def test_planner_reports_divergent_source_graphs(planning, monkeypatch, difference) -> None:
    project, config, run, planner = planning
    source = node("source", source_name=None if difference == "incomplete" else "app")
    production = normalize_manifest(manifest_raw(sources={"source.raw": source}))
    local = production if difference == "incomplete" else normalize_manifest(manifest_raw())
    monkeypatch.setattr(
        planner,
        "_resolve_target",
        Mock(side_effect=[(production, ("source.raw",), False), (local, ("source.raw",), False)]),
    )
    plan = planner.build(
        project=project, config=config, run=run, select=(), exclude=(), variables=None
    )
    assert plan.source_mappings == ()
    expected = "DBTV-MANIFEST-002" if difference == "incomplete" else "DBTV-CFG-002"
    assert expected in {finding.rule_id for finding in plan.findings}
    config.project.production_target = None
    with pytest.raises(ManifestError, match="No production target"):
        planner.build(
            project=project, config=config, run=run, select=(), exclude=(), variables=None
        )


@pytest.mark.parametrize("cache_checksum", [None, "corrupt"])
def test_planner_ignores_unverified_parse_cache_and_preserves_options(
    planning, cache_checksum
) -> None:
    project, config, run, planner = planning
    cache = project.root / "cache"
    cache.mkdir()
    (cache / "partial_parse.msgpack").write_bytes(b"untrusted cached state")
    if cache_checksum:
        (cache / "checksum").write_text(cache_checksum)
    target = run.local_target_path
    commands = []

    def invoke(args, **kwargs):
        commands.append(args)
        assert not (target / "partial_parse.msgpack").exists()
        if args[0] == "parse":
            (target / "manifest.json").write_text(json.dumps(manifest_raw()))
        return SimpleNamespace(stdout="{}")

    planner.invoker.run.side_effect = invoke
    _, selected, hit = planner._resolve_target(
        project=project,
        profiles_dir=project.profiles_dir,
        target="dev",
        target_path=target,
        cache_dir=cache,
        select=("a",),
        exclude=("b",),
        variables="{x: 1}",
        log_path=run.root / "logs",
        partial_parse=True,
        cache_parsing=True,
        cancellation=None,
    )
    assert not selected and not hit
    assert "--exclude" in commands[0] and "--vars" in commands[1]
    planner._parse(
        project=project,
        profiles_dir=project.profiles_dir,
        target="dev",
        target_path=target,
        variables="{x: 2}",
        log_path=run.root / "logs",
        partial_parse=False,
    )
    planner._list(
        project=project,
        profiles_dir=project.profiles_dir,
        target="dev",
        target_path=target,
        select=(),
        exclude=(),
        variables="{x: 2}",
        log_path=run.root / "logs",
        partial_parse=False,
        cancellation=None,
    )
    assert all("--no-partial-parse" in command for command in commands[-2:])
