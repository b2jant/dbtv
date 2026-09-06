from __future__ import annotations

import json
import signal
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import click
import pyarrow as pa
import pytest
from click.testing import CliRunner
from test_connector_contracts import request
from test_local_runtime import parquet_project
from test_snapshot_store import _request, _store

from dbtv.cli import app, render
from dbtv.config.schema import SourceSettings
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import ConfigError, DbtInvocationError, DbtvError
from dbtv.core.events import RunEvent
from dbtv.core.models import (
    CompatibilityFinding,
    DbtNodeResult,
    ExecutionPlan,
    ExtractionBatch,
    ExtractionEstimate,
    RunSummary,
    SnapshotAction,
    SnapshotDecision,
    SourceMode,
)
from dbtv.sources import PlannedSource
from dbtv.state import StateIndex
from dbtv.workspace import Workspace


def summary(root, code=0):
    return RunSummary(
        "run",
        "build",
        "SUCCEEDED" if not code else "DBT_FAILED",
        code,
        False,
        0,
        1,
        0,
        root / "local.duckdb",
        "local",
        root,
        "start",
        "finish",
        {"dbt": 1.0},
    )


def plan(root, findings=()):
    return ExecutionPlan(
        "run",
        root,
        "p",
        "dev",
        "local",
        ("model.p.a",),
        ("model.p.a",),
        (),
        findings,
        "v12",
        "v12",
        "sha256:project",
        "sha256:plan",
    )


@pytest.fixture
def cli(tmp_path):
    project, profiles = parquet_project(tmp_path)
    runner = CliRunner()

    def invoke(*args, input=None):
        return runner.invoke(
            app.main,
            ["--project-dir", str(project), "--profiles-dir", str(profiles), *args],
            input=input,
        )

    return project, profiles, invoke


@pytest.mark.parametrize("output", ["json", "console"])
@pytest.mark.parametrize("code", [0, 3])
def test_execution_cli_preserves_options_results_events_and_exit_codes(
    cli, monkeypatch, output, code
):
    project, _, invoke = cli
    captured = {}
    log = project / "event-log.jsonl"

    def execute(**kwargs):
        captured.update(kwargs)
        kwargs["event_consumer"](RunEvent("run", "ready", "binding"))
        if code:
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            assert kwargs["cancellation"].cancelled
        return SimpleNamespace(
            summary=summary(project, code), dbt_stdout="dbt complete", dbt_stderr="warning"
        )

    monkeypatch.setattr(app.Orchestrator, "execute", lambda self, **kw: execute(**kw))
    previous = signal.getsignal(signal.SIGINT)
    result = invoke(
        "--output",
        output,
        "--profile",
        "override",
        "--target",
        "production",
        "--log-path",
        str(log),
        "build",
        "--offline",
        "--fidelity",
        "strict",
        "--capture-results",
        "--select",
        "+model",
        "--exclude",
        "skip",
        "--vars",
        "{x: 1}",
    )
    assert result.exit_code == code, result.output
    assert captured["options"].source_mode is SourceMode.OFFLINE
    assert captured["options"].capture_results
    assert captured["config"].project.profile == "override"
    assert captured["config"].project.production_target == "production"
    assert signal.getsignal(signal.SIGINT) == previous
    assert json.loads(log.read_text())["name"] == "ready"


def test_quiet_execution_and_internal_error_redaction(cli, monkeypatch):
    project, _, invoke = cli
    monkeypatch.setattr(
        app.Orchestrator,
        "execute",
        lambda *a, **kw: SimpleNamespace(summary=summary(project), dbt_stdout="", dbt_stderr=""),
    )
    assert invoke("run").exit_code == 0
    assert invoke("--quiet", "run").exit_code == 0
    monkeypatch.setattr(app, "discover_project", Mock(side_effect=RuntimeError("secret-canary")))
    failed = invoke("--output", "json", "run")
    assert failed.exit_code == 10
    assert "secret-canary" not in failed.output
    assert "DBTV-INTERNAL-001" in failed.output
    with click.Context(click.Command("fixture")), pytest.raises(click.exceptions.Exit):
        app.guarded(lambda: (_ for _ in ()).throw(ConfigError("bad")))()


def test_version_missing_dependencies_and_profile_inference(cli, monkeypatch):
    project, profiles, invoke = cli
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr(app, "package_version", Mock(side_effect=app.PackageNotFoundError()))
    monkeypatch.setattr(
        app.DbtInvoker, "resolved_executable", Mock(side_effect=DbtInvocationError("missing"))
    )
    result = invoke("--no-color", "--output", "json", "version")
    assert result.exit_code == 0
    assert json.loads(result.output)["dbt_executable"] is None
    assert json.loads(result.output)["duckdb"] is None
    assert invoke("init").exit_code == 3
    (project / ".gitignore").write_text(".dbtv/\n")
    assert invoke("init", "--force", "--update-gitignore").exit_code == 0
    assert (project / ".gitignore").read_text() == ".dbtv/\n"
    for text in ("[", "[]", "{}", "analytics_profile: {}"):
        (profiles / "profiles.yml").write_text(text)
        assert app._infer_profile_target(profiles, "analytics_profile") is None


@pytest.mark.parametrize("healthy", [True, False])
def test_doctor_checks_named_connections_without_opening_them(cli, monkeypatch, healthy):
    project, _, invoke = cli
    raw = json.loads((project / "dbtv.yml").read_text())
    raw["source"] = {"connector": "snowflake"}
    raw["connections"] = {
        "remote": {"connector": "snowflake"},
        "file": {"connector": "parquet", "credential_resolver": "none"},
    }
    (project / "dbtv.yml").write_text(json.dumps(raw))
    monkeypatch.setattr(
        app.DbtInvoker,
        "version",
        lambda _: SimpleNamespace(return_code=0, stdout="snowflake duckdb", stderr=""),
    )
    resolver = Mock()
    if not healthy:
        resolver.resolve.side_effect = ConfigError("missing credential")
    monkeypatch.setattr(app.CredentialResolverRegistry, "create", lambda *a, **kw: resolver)
    result = invoke("--output", "json", "doctor")
    assert result.exit_code == (0 if healthy else 2), result.output
    checks = json.loads(result.output)["checks"]
    assert any(c["name"] == "connection remote" for c in checks)
    assert resolver.resolve.call_count == 2


def test_doctor_reports_missing_executable_and_project(cli, monkeypatch):
    project, _, invoke = cli
    monkeypatch.setattr(app.DbtInvoker, "version", Mock(side_effect=DbtInvocationError("missing")))
    assert invoke("doctor").exit_code == 2
    (project / "dbt_project.yml").unlink()
    assert invoke("doctor").exit_code == 2


@pytest.mark.parametrize("output", ["console", "json"])
def test_status_and_diagnostics_lifecycle(cli, output):
    project, _, invoke = cli
    assert "No .dbtv" in invoke("status").output
    workspace = Workspace(project)
    workspace.ensure()
    workspace.runs.rmdir()
    assert invoke("--output", output, "status").exit_code == 0
    workspace.create_run()
    assert invoke("--output", output, "status", "--rebuild-index").exit_code == 0
    archive = project / f"diagnostics-{output}.zip"
    assert invoke("--output", output, "diagnostics", "--destination", str(archive)).exit_code == 0
    assert archive.is_file()
    assert invoke("inspect").exit_code == 8


def test_datasets_list_show_drop_and_missing_id(cli):
    project, _, invoke = cli
    workspace = Workspace(project)
    workspace.ensure()
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("test")
    dataset = "sha256:" + "a" * 64
    payload = {"dataset_id": dataset, "snapshots": {}, "created_by_run": "fixture"}
    state.commit_dataset(dataset, payload)
    assert json.loads(invoke("datasets", "list").output)["datasets"][0]["source_count"] == 0
    assert json.loads(invoke("datasets", "show", dataset).output) == payload
    assert invoke("datasets", "drop", dataset).exit_code == 2
    assert invoke("datasets", "drop", dataset, "--yes").exit_code == 0
    assert invoke("datasets", "show", dataset).exit_code == 7


@pytest.mark.parametrize("code", [0, 3])
def test_validation_cli_delegates_options_and_exits(cli, monkeypatch, code):
    project, _, invoke = cli
    replay = Mock(return_value=SimpleNamespace(summary=summary(project, code)))
    validate = Mock(return_value={"equal": not code})
    monkeypatch.setattr("dbtv.validation.replay_run", replay)
    monkeypatch.setattr("dbtv.validation.validate_incremental", validate)
    assert invoke("replay", "original", "--allow-code-change", "--full-refresh").exit_code == code
    assert replay.call_args.kwargs["allow_code_change"]
    result = invoke(
        "validate-incremental",
        "--base-dataset",
        "base",
        "--next-dataset",
        "next",
        "--select",
        "+model",
    )
    assert result.exit_code == int(bool(code))
    assert validate.call_args.kwargs["select"] == ("+model",)
    monkeypatch.setattr("dbtv.validation.compare_runs", Mock(return_value={"equal": True}))
    run = Workspace(project).create_run()
    assert invoke("compare", "left", run.invocation_id).exit_code == 0


def test_event_filtering_and_signal_restoration(cli, capsys):
    project, _, _ = cli
    context = app.CliContext(
        project, None, None, "json", None, None, False, False, "warning", None, None
    )
    app._consume_event(context, {"severity": "debug"})
    assert not capsys.readouterr().out
    app._consume_event(context, {"severity": "warning"})
    assert "warning" in capsys.readouterr().out
    app._consume_event(replace(context, quiet=True), {"severity": "error"})
    assert not capsys.readouterr().out
    previous = signal.getsignal(signal.SIGINT)

    def operation(token):
        signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
        assert token.cancelled
        raise ConfigError("cancelled operation")

    with pytest.raises(ConfigError):
        app._cancellable(operation)
    assert signal.getsignal(signal.SIGINT) == previous


def test_rendering_includes_validation_details(cli, capsys):
    project, _, _ = cli
    error = DbtvError("failed", hint="retry", context={"artifact_path": "/artifacts"})
    render.render_error(error)
    assert "Artifacts:" in capsys.readouterr().err
    findings = (
        CompatibilityFinding("RULE", "error", "unsupported"),
        CompatibilityFinding("WARN", "warning", "review"),
    )
    decisions = [
        {
            "source_unique_id": "source.p.a",
            "action": "refresh",
            "reason": "missing",
            "sampling": {"strategy": "full"},
            "estimated_rows": 2,
            "estimated_bytes": 10,
        }
    ]
    render.render_plan(plan(project, findings), source_decisions=decisions)
    assert "unsupported" in capsys.readouterr().out
    render.render_plan(plan(project))
    capsys.readouterr()
    render.render_plan(plan(project), output="json")
    assert "source_decisions" not in json.loads(capsys.readouterr().out)
    render.render_rows(("id",), ((1,),))
    assert "id" in capsys.readouterr().out
    render.render_summary(
        replace(
            summary(project),
            dataset_id="sha256:dataset",
            warnings=("review",),
            dbt_results=(DbtNodeResult("test.p.a", "fail", None, 1, 1),),
        )
    )
    assert "WARNING" in capsys.readouterr().out


@pytest.mark.parametrize("scenario", ["normal", "no_estimate", "over_budget", "cohort", "blocking"])
def test_plan_estimates_are_bounded_and_close_connections(cli, monkeypatch, scenario):
    project, _, invoke = cli
    findings = (CompatibilityFinding("RULE", "error", "blocked"),) if scenario == "blocking" else ()
    monkeypatch.setattr(app.ProjectPlanner, "build", lambda *a, **kw: plan(project, findings))
    source = PlannedSource(
        "default",
        SourceSettings(),
        SnapshotDecision(request(), SnapshotAction.REFRESH, "missing"),
        cohort_parent="source.parent" if scenario == "cohort" else None,
    )
    if scenario == "no_estimate":
        source = replace(
            source, settings=SourceSettings(connector="parquet", credential_resolver="none")
        )
    monkeypatch.setattr(app, "prepare_sources", lambda **kw: SimpleNamespace(sources=(source,)))
    connector = Mock()
    connector.estimate.return_value = (
        None
        if scenario == "no_estimate"
        else ExtractionEstimate(2, 30 * 1024**3 if scenario == "over_budget" else 20)
    )
    monkeypatch.setattr(app.ConnectorRegistry, "create", lambda *a, **kw: connector)
    monkeypatch.setattr(app.CredentialResolverRegistry, "create", lambda *a, **kw: Mock())
    result = invoke("--output", "json", "plan", "--remote-estimates", "--fidelity", "strict")
    assert result.exit_code == {"over_budget": 6, "blocking": 9}.get(scenario, 0), result.output
    assert connector.close.call_count == (0 if scenario == "cohort" else 1)


def test_cleanup_guards_previews_and_artifact_removal(cli):
    project, _, invoke = cli
    assert invoke("clean").exit_code == 2
    assert "Nothing matches" in invoke("clean", "--runs").output
    workspace = Workspace(project)
    run = workspace.create_run()
    (run.root / "keep").write_text("artifact")
    assert invoke("--non-interactive", "clean", "--runs").exit_code == 2
    assert invoke("clean", "--runs", input="n\n").exit_code == 1
    assert run.root.exists()
    assert invoke("clean", "--runs", input="y\n").exit_code == 0
    assert not run.root.exists()
    for unsafe in (project, workspace.root, project.parent, workspace.cache):
        with pytest.raises(ConfigError, match="unsafe cleanup"):
            app._validate_clean_target(unsafe, workspace, project / "local.duckdb", workspace.cache)


def test_cleanup_respects_snapshot_retention_and_all_cleans_generated_files(cli):
    project, _, invoke = cli
    store, state = _store(project)

    def write(value):
        return store.write(
            _request(),
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": [value]}))],
            cancellation=CancellationToken(),
            invocation_id=f"run-{value}",
        )

    old = write(1)
    active = write(2)
    assert invoke("status").exit_code == 0
    assert invoke("clean", "--snapshots", "--older-than", "1d", "--yes").exit_code == 0
    assert old.root.exists()
    assert invoke("--output", "json", "clean", "--snapshots", "--yes").exit_code == 0
    assert not old.root.exists() and active.root.exists()
    pending = store.tmp_root / "pending"
    pending.mkdir()
    (pending / "data").write_text("partial")
    assert invoke("clean", "--snapshots", "--older-than", "1d", "--yes").exit_code == 0
    assert pending.exists()
    workspace = Workspace(project)
    for directory in (
        workspace.catalogs,
        workspace.generated,
        workspace.tmp,
        workspace.manifests,
        workspace.root / "parsing",
        workspace.root / "datasets",
        workspace.root / "diagnostics",
    ):
        directory.mkdir(exist_ok=True)
        (directory / "fixture").write_text("artifact")
    (workspace.root / "local.duckdb").write_text("database")
    (workspace.root / "local.duckdb.wal").write_text("wal")
    (workspace.runs / "ignored-file").write_text("ignore")
    (workspace.runs / "symlink").symlink_to(project)
    assert invoke("clean", "--all", "--preview").exit_code == 0
    result = invoke("clean", "--all", "--yes")
    assert result.exit_code == 0, result.output
    assert not active.root.exists() and not pending.exists()
    assert not state.path.exists()
