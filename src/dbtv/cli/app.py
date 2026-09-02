from __future__ import annotations

import json
import os
import platform
import shutil
import signal
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import wraps
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import click
import yaml

from dbtv import __version__
from dbtv.backend.duckdb import DuckDbExecutionBackend
from dbtv.cli.render import (
    render_checks,
    render_error,
    render_plan,
    render_rows,
    render_summary,
)
from dbtv.config.loader import DEFAULT_CONFIG_NAME, load_config, render_default_config
from dbtv.connectors.registry import ConnectorRegistry
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import (
    BindingError,
    ConfigError,
    DbtvError,
    OfflineViolation,
    PolicyError,
    ProjectError,
)
from dbtv.core.models import FidelityMode, SnapshotRequest, SourceMode
from dbtv.core.units import parse_duration, parse_size
from dbtv.credentials.registry import CredentialResolverRegistry
from dbtv.diagnostics import create_diagnostics_bundle
from dbtv.orchestration import CommandOptions, Orchestrator
from dbtv.project.dbt_invoker import DbtInvoker
from dbtv.project.discovery import discover_project
from dbtv.project.planner import ProjectPlanner
from dbtv.snapshot.policy import LocalPolicyEngine, apply_max_rows, resolve_sampling
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.state import StateIndex
from dbtv.workspace import Workspace

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class CliContext:
    project_dir: Path
    profiles_dir: Path | None
    config_path: Path | None
    output: str
    profile: str | None
    target: str | None
    non_interactive: bool
    quiet: bool
    log_level: str
    log_path: Path | None
    invocation_id: str | None


def guarded(function: Callable[P, R]) -> Callable[P, R]:
    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        context = click.get_current_context().find_root().obj
        output = context.output if isinstance(context, CliContext) else "console"
        try:
            return function(*args, **kwargs)
        except DbtvError as error:
            render_error(error, output=output)
            raise click.exceptions.Exit(error.exit_code) from error
        except (click.ClickException, click.Abort, click.exceptions.Exit):
            raise
        except Exception as exc:
            internal_error = DbtvError(
                "dbtv encountered an unexpected internal error.",
                context={"exception_type": type(exc).__name__},
            )
            render_error(internal_error, output=output)
            raise click.exceptions.Exit(10) from exc

    return wrapper


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--project-dir", type=click.Path(path_type=Path), default=Path.cwd)
@click.option("--profiles-dir", type=click.Path(path_type=Path), default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=None)
@click.option("--profile", default=None, help="Override the dbt profile name.")
@click.option("--target", default=None, help="Override the production target.")
@click.option("--output", type=click.Choice(["console", "json"]), default="console")
@click.option("--non-interactive", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.option(
    "--log-level",
    type=click.Choice(["error", "warning", "info", "debug"]),
    default="info",
)
@click.option("--log-path", type=click.Path(path_type=Path), default=None)
@click.option("--no-color", is_flag=True)
@click.option("--invocation-id", default=None)
@click.pass_context
def main(
    context: click.Context,
    project_dir: Path,
    profiles_dir: Path | None,
    config_path: Path | None,
    profile: str | None,
    target: str | None,
    output: str,
    non_interactive: bool,
    quiet: bool,
    log_level: str,
    log_path: Path | None,
    no_color: bool,
    invocation_id: str | None,
) -> None:
    """Accelerate selected dbt work locally with source snapshots and DuckDB."""
    if no_color:
        os.environ.setdefault("NO_COLOR", "1")
    context.obj = CliContext(
        project_dir.resolve(),
        profiles_dir,
        config_path,
        output,
        profile,
        target,
        non_interactive,
        quiet,
        log_level,
        log_path.resolve() if log_path else None,
        invocation_id,
    )


@main.command("version")
@click.pass_obj
def version_command(context: CliContext) -> None:
    """Show dbtv and runtime versions."""
    payload = {
        "dbtv": __version__,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "dbt_executable": _resolved_dbt_executable(),
        "dbt_core": _installed_version("dbt-core"),
        "dbt_duckdb": _installed_version("dbt-duckdb"),
        "duckdb": _installed_version("duckdb"),
        "pyarrow": _installed_version("pyarrow"),
        "snowflake_connector": _installed_version("snowflake-connector-python"),
        "source_connectors": ConnectorRegistry().names(),
    }
    if context.output == "json":
        click.echo(json.dumps(payload, sort_keys=True))
    else:
        for key, value in payload.items():
            click.echo(f"{key}: {value or 'not installed'}")


@main.command("init")
@click.option("--force", is_flag=True, help="Replace an existing dbtv.yml.")
@click.option(
    "--update-gitignore",
    is_flag=True,
    help="Add the local .dbtv workspace to .gitignore.",
)
@click.pass_obj
@guarded
def init_command(context: CliContext, force: bool, update_gitignore: bool) -> None:
    """Initialize dbtv configuration in an existing dbt project."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    destination = context.config_path or project.root / DEFAULT_CONFIG_NAME
    if destination.exists() and not force:
        raise ProjectError(
            f"Configuration already exists at {destination}.",
            hint="Use --force only if replacing it is intentional.",
        )
    production_target = context.target or _infer_profile_target(
        project.profiles_dir, project.profile_name
    )
    destination.write_text(
        render_default_config(production_target=production_target),
        encoding="utf-8",
    )
    Workspace(project.root).ensure()
    if update_gitignore:
        gitignore = project.root / ".gitignore"
        existing = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
        entries = {line.strip() for line in existing.splitlines()}
        if ".dbtv/" not in entries:
            separator = "" if not existing or existing.endswith("\n") else "\n"
            gitignore.write_text(existing + separator + ".dbtv/\n", encoding="utf-8")
    click.echo(f"Created {destination}")
    click.echo(f"Workspace {project.root / '.dbtv'}")
    if not update_gitignore:
        click.echo("Suggestion: add .dbtv/ to .gitignore or rerun init --update-gitignore.")


@main.command("doctor")
@click.pass_obj
@guarded
def doctor_command(context: CliContext) -> None:
    """Validate the project and required local tooling."""
    checks: list[dict[str, Any]] = []
    py_ok = (3, 11) <= sys.version_info[:2] < (3, 14)
    checks.append(
        {
            "name": "python",
            "status": "ok" if py_ok else "error",
            "detail": platform.python_version(),
        }
    )
    try:
        project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
        checks.append({"name": "dbt project", "status": "ok", "detail": str(project.root)})
        config = _load_context_config(context, project.root)
        checks.append({"name": "configuration", "status": "ok", "detail": "version 1"})
        profiles_file = project.profiles_dir / "profiles.yml"
        checks.append(
            {
                "name": "profiles.yml",
                "status": "ok" if profiles_file.is_file() else "error",
                "detail": str(profiles_file),
            }
        )
        invoker = DbtInvoker(config.project.dbt_executable)
        try:
            result = invoker.version()
            checks.append(
                {
                    "name": "dbt executable",
                    "status": "ok" if result.return_code == 0 else "error",
                    "detail": invoker.resolved_executable(),
                }
            )
            output = f"{result.stdout}\n{result.stderr}".lower()
            connector_registry = ConnectorRegistry()
            source_adapter = (
                connector_registry.dbt_adapter_name(config.source.connector)
                if config.source.connector in connector_registry.names()
                else None
            )
            if source_adapter:
                checks.append(
                    {
                        "name": f"dbt-{source_adapter}",
                        "status": "ok" if source_adapter in output else "error",
                        "detail": (
                            "adapter detected"
                            if source_adapter in output
                            else "adapter not detected"
                        ),
                    }
                )
            checks.append(
                {
                    "name": "dbt-duckdb",
                    "status": "ok" if "duckdb" in output else "error",
                    "detail": "adapter detected" if "duckdb" in output else "adapter not detected",
                }
            )
        except DbtvError as error:
            checks.append({"name": "dbt executable", "status": "error", "detail": error.message})
        writable = os.access(project.root, os.W_OK)
        checks.append(
            {
                "name": "project writable",
                "status": "ok" if writable else "error",
                "detail": str(project.root),
            }
        )
        connector_registry = ConnectorRegistry()
        connector_available = config.source.connector in connector_registry.names()
        distributions = (
            "duckdb",
            "pyarrow",
            *(
                connector_registry.dependencies(config.source.connector)
                if connector_available
                else ()
            ),
        )
        for distribution in dict.fromkeys(distributions):
            installed = _installed_version(distribution)
            checks.append(
                {
                    "name": distribution,
                    "status": "ok" if installed else "error",
                    "detail": installed or "not installed",
                }
            )
        checks.append(
            {
                "name": "source connector",
                "status": "ok" if connector_available else "error",
                "detail": config.source.connector,
            }
        )
        if config.project.production_target:
            try:
                CredentialResolverRegistry().create(config.source.credential_resolver).resolve(
                    profiles_dir=project.profiles_dir,
                    profile_name=config.project.profile or project.profile_name,
                    target_name=config.project.production_target,
                    env=os.environ,
                    interactive=not context.non_interactive,
                )
                checks.append(
                    {
                        "name": "credential resolution",
                        "status": "ok",
                        "detail": "resolved in memory; no connection attempted",
                    }
                )
            except DbtvError as error:
                checks.append(
                    {
                        "name": "credential resolution",
                        "status": "error",
                        "detail": error.message,
                    }
                )
        free_bytes = shutil.disk_usage(project.root).free
        checks.append(
            {
                "name": "free disk",
                "status": "ok" if free_bytes >= 1024**3 else "error",
                "detail": f"{free_bytes / 1024**3:.1f} GB",
            }
        )
    except DbtvError as error:
        checks.append({"name": "dbt project", "status": "error", "detail": error.message})

    render_checks(checks, output=context.output)
    if any(check["status"] == "error" for check in checks):
        raise click.exceptions.Exit(2)


@main.command("plan")
@click.option("--select", "select_", multiple=True)
@click.option("--exclude", multiple=True)
@click.option("--vars", "variables", default=None)
@click.option(
    "--source-mode",
    type=click.Choice([item.value for item in SourceMode]),
    default="auto",
)
@click.option("--data-profile", default=None)
@click.option("--cache-ttl", default=None)
@click.option("--max-rows", type=click.IntRange(min=1), default=None)
@click.option("--max-bytes", default=None)
@click.option("--allow-full-source", is_flag=True)
@click.option("--snapshot-id", default=None)
@click.option(
    "--remote-estimates",
    is_flag=True,
    help="Connect to the source and estimate planned working sets.",
)
@click.option(
    "--fidelity",
    type=click.Choice([item.value for item in FidelityMode]),
    default=None,
)
@click.pass_obj
@guarded
def plan_command(
    context: CliContext,
    select_: tuple[str, ...],
    exclude: tuple[str, ...],
    variables: str | None,
    source_mode: str,
    data_profile: str | None,
    cache_ttl: str | None,
    max_rows: int | None,
    max_bytes: str | None,
    allow_full_source: bool,
    snapshot_id: str | None,
    remote_estimates: bool,
    fidelity: str | None,
) -> None:
    """Resolve the production/local graph and required source mappings."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    config = _load_context_config(context, project.root)
    if remote_estimates and source_mode == SourceMode.OFFLINE.value:
        raise OfflineViolation("Remote estimates are forbidden in offline mode.")
    workspace = Workspace(project.root)
    run = workspace.create_run(context.invocation_id)
    planner = ProjectPlanner(DbtInvoker(config.project.dbt_executable))
    plan = planner.build(
        project=project,
        config=config,
        run=run,
        select=select_,
        exclude=exclude,
        variables=variables,
    )
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize(run.invocation_id)
    ttl = parse_duration(cache_ttl or config.cache.default_ttl)
    for mapping in plan.source_mappings:
        for tag in mapping.tags:
            if tag in config.policy.max_cache_age_for_tags:
                ttl = min(ttl, parse_duration(config.policy.max_cache_age_for_tags[tag]))
    store = ParquetSnapshotStore(
        config.cache.root,
        state=state,
        locks_dir=workspace.locks,
        ttl=ttl,
        compression=config.cache.compression,
        row_group_target_bytes=config.cache.row_group_target_bytes,
        integrity=config.cache.integrity,
        maximum_size=min(
            parse_size(config.cache.maximum_size),
            parse_size(max_bytes) if max_bytes else 2**63 - 1,
        ),
        retain_previous=config.cache.retain_previous_snapshots,
        file_mode=int(config.policy.cache_file_mode, 8),
        directory_mode=int(config.policy.cache_directory_mode, 8),
    )
    profile_name = data_profile or config.default_data_profile
    if profile_name not in config.data_profiles:
        raise ConfigError(f"Unknown data profile {profile_name!r}.")
    selected_profile = config.data_profiles[profile_name]
    selected_fidelity = FidelityMode(fidelity or config.compatibility.mode)
    policy = LocalPolicyEngine(config.policy, allow_full_source=allow_full_source)
    source_decisions: list[dict[str, Any]] = []
    requests: list[SnapshotRequest] = []
    for mapping in plan.source_mappings:
        sampling = apply_max_rows(resolve_sampling(selected_profile, mapping.source), max_rows)
        policy.evaluate_sampling(
            mapping.source,
            sampling,
            tags=mapping.tags,
            fidelity=selected_fidelity,
        )
        request = SnapshotRequest(
            mapping.source,
            mapping.production_relation,
            sampling,
            selected_fidelity,
            query_tag=(
                f"{config.source.session.query_tag_prefix}/plan/"
                f"{run.invocation_id}/{mapping.source.unique_id}"
            )[:256],
        )
        requests.append(request)
        decision = store.decide(
            request,
            provider=config.source.connector,
            mode=source_mode,
            snapshot_id=snapshot_id,
        )
        source_decisions.append(
            {
                "source_unique_id": mapping.source.unique_id,
                "action": decision.action.value,
                "reason": decision.reason,
                "sampling": asdict(sampling),
                "snapshot_key": (decision.snapshot.snapshot_key if decision.snapshot else None),
            }
        )
    remote_estimate_queries = 0
    if remote_estimates and requests:
        credentials = (
            CredentialResolverRegistry()
            .create(config.source.credential_resolver)
            .resolve(
                profiles_dir=project.profiles_dir,
                profile_name=config.project.profile or project.profile_name,
                target_name=plan.production_target,
                env=os.environ,
                interactive=not context.non_interactive,
            )
        )
        connector = ConnectorRegistry().create(
            config.source.connector,
            credentials=credentials,
            session=config.source.session,
            extraction=config.source.extraction,
            plugin_config=config.source.plugin,
        )
        estimated_bytes = 0
        try:
            connector.open()
            for request, decision_payload in zip(requests, source_decisions, strict=True):
                estimate = connector.estimate(request)
                remote_estimate_queries += 1
                decision_payload["estimated_rows"] = estimate.row_count if estimate else None
                decision_payload["estimated_bytes"] = estimate.byte_count if estimate else None
                if estimate and estimate.byte_count is not None:
                    estimated_bytes += estimate.byte_count
        finally:
            connector.close()
        if estimated_bytes > parse_size(config.policy.max_estimated_bytes_per_run):
            raise PolicyError(
                "Estimated source data exceeds the configured per-run policy limit.",
                hint="Choose a smaller data profile or update the approved policy limit.",
            )
    artifact = plan.to_dict()
    artifact["source_decisions"] = source_decisions
    artifact["remote_estimates_requested"] = remote_estimates
    artifact["remote_estimate_query_count"] = remote_estimate_queries
    artifact["remote_access_on_execution"] = any(
        item["action"] == "refresh" for item in source_decisions
    )
    workspace.write_json(run.plan_path, artifact)
    render_plan(plan, output=context.output, source_decisions=source_decisions)
    if context.output == "console":
        click.echo(f"Plan artifact: {run.root / 'plan.json'}")
    if any(finding.severity == "error" for finding in plan.findings):
        raise click.exceptions.Exit(9)


@main.command("status")
@click.option("--rebuild-index", is_flag=True, help="Rebuild snapshot state from sidecars.")
@click.pass_obj
@guarded
def status_command(context: CliContext, rebuild_index: bool) -> None:
    """Show snapshots, local database, recent runs, and lock files."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    config = _load_context_config(context, project.root)
    workspace = Workspace(project.root)
    if not workspace.root.exists():
        click.echo("No .dbtv workspace exists yet.")
        return
    runs = (
        sorted(
            (path for path in workspace.runs.iterdir() if path.is_dir()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if workspace.runs.exists()
        else []
    )
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("status")
    rebuild_report = None
    if rebuild_index:
        rebuild_report = ParquetSnapshotStore(
            config.cache.root,
            state=state,
            locks_dir=workspace.locks,
            ttl=parse_duration(config.cache.default_ttl),
            compression=config.cache.compression,
            row_group_target_bytes=config.cache.row_group_target_bytes,
            integrity=config.cache.integrity,
            maximum_size=parse_size(config.cache.maximum_size),
            retain_previous=config.cache.retain_previous_snapshots,
            file_mode=int(config.policy.cache_file_mode, 8),
            directory_mode=int(config.policy.cache_directory_mode, 8),
        ).rebuild_index()
    snapshots = state.list_snapshots()
    payload = {
        "workspace": str(workspace.root),
        "run_count": len(runs),
        "recent_runs": [path.name for path in runs[:5]],
        "local_database": str(config.local.database),
        "local_database_exists": config.local.database.is_file(),
        "snapshot_count": len(snapshots),
        "snapshot_bytes": sum(item.byte_count for item in snapshots),
        "pending_snapshot_writes": (
            [path.name for path in (config.cache.root / "tmp").iterdir()]
            if (config.cache.root / "tmp").exists()
            else []
        ),
        "active_snapshots": [
            {
                "source": item.source_unique_id,
                "snapshot_key": item.snapshot_key,
                "created_at": item.created_at,
                "expires_at": item.expires_at,
                "rows": item.row_count,
                "bytes": item.byte_count,
            }
            for item in snapshots
            if item.active
        ],
        "lock_files": [path.name for path in workspace.locks.glob("*.lock")],
        "rebuild": rebuild_report,
    }
    if context.output == "json":
        click.echo(json.dumps(payload, sort_keys=True))
    else:
        for key, value in payload.items():
            click.echo(f"{key}: {value}")


def _execution_options(function: Callable[..., Any]) -> Callable[..., Any]:
    options = [
        click.option("--select", "select_", multiple=True),
        click.option("--exclude", multiple=True),
        click.option("--vars", "variables", default=None),
        click.option(
            "--source-mode",
            type=click.Choice([item.value for item in SourceMode]),
            default="auto",
        ),
        click.option("--offline", is_flag=True, help="Require zero source connectivity."),
        click.option("--data-profile", default=None),
        click.option("--cache-ttl", default=None),
        click.option("--max-rows", type=click.IntRange(min=1), default=None),
        click.option("--max-bytes", default=None),
        click.option("--allow-full-source", is_flag=True),
        click.option("--snapshot-id", default=None),
        click.option("--keep-snapshot", is_flag=True),
        click.option(
            "--fidelity",
            type=click.Choice([item.value for item in FidelityMode]),
            default=None,
        ),
        click.option("--full-refresh", is_flag=True),
    ]
    decorated = function
    for option in reversed(options):
        decorated = option(decorated)
    return decorated


def _execution_command(name: str) -> Callable[..., None]:
    @click.pass_obj
    @guarded
    def command(
        context: CliContext,
        select_: tuple[str, ...],
        exclude: tuple[str, ...],
        variables: str | None,
        source_mode: str,
        offline: bool,
        data_profile: str | None,
        cache_ttl: str | None,
        max_rows: int | None,
        max_bytes: str | None,
        allow_full_source: bool,
        snapshot_id: str | None,
        keep_snapshot: bool,
        fidelity: str | None,
        full_refresh: bool,
    ) -> None:
        project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
        config = _load_context_config(context, project.root)
        workspace = Workspace(project.root)
        run = workspace.create_run(context.invocation_id)
        token = CancellationToken()
        previous = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, lambda *_: token.cancel())
        try:
            result = Orchestrator(
                connector_registry=ConnectorRegistry(
                    offline=offline or source_mode == SourceMode.OFFLINE.value
                )
            ).execute(
                project=project,
                config=config,
                workspace=workspace,
                run=run,
                options=CommandOptions(
                    command=name,
                    select=select_,
                    exclude=exclude,
                    variables=variables,
                    source_mode=SourceMode.OFFLINE if offline else SourceMode(source_mode),
                    data_profile=data_profile,
                    cache_ttl=cache_ttl,
                    max_rows=max_rows,
                    max_bytes=max_bytes,
                    allow_full_source=allow_full_source,
                    snapshot_id=snapshot_id,
                    keep_snapshot=keep_snapshot,
                    fidelity=FidelityMode(fidelity) if fidelity else None,
                    full_refresh=full_refresh,
                    interactive=not context.non_interactive,
                ),
                cancellation=token,
                event_consumer=lambda event: _consume_event(context, event.to_dict()),
            )
        finally:
            signal.signal(signal.SIGINT, previous)
        if context.output == "console" and not context.quiet:
            if result.dbt_stdout.strip():
                click.echo(result.dbt_stdout.rstrip())
            if result.dbt_stderr.strip():
                click.echo(result.dbt_stderr.rstrip(), err=True)
        render_summary(result.summary, output=context.output)
        if result.summary.exit_code:
            raise click.exceptions.Exit(result.summary.exit_code)

    command.__name__ = f"{name}_command"
    command.__doc__ = f"Prepare snapshots and execute local dbt {name}."
    return _execution_options(command)


for _name in ("sync", "run", "test", "build"):
    main.command(_name)(_execution_command(_name))


@main.command("inspect")
@click.option("--query", default="SHOW ALL TABLES", help="One read-only DuckDB query.")
@click.pass_obj
@guarded
def inspect_command(context: CliContext, query: str) -> None:
    """Query the persistent local DuckDB database in read-only mode."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    config = _load_context_config(context, project.root)
    if not config.local.database.is_file():
        raise BindingError(
            f"Local DuckDB database does not exist at {config.local.database}.",
            hint="Complete `dbtv run` or `dbtv build` first.",
        )
    columns, rows = DuckDbExecutionBackend(config.local.database, Workspace(project.root)).inspect(
        query
    )
    render_rows(columns, rows, output=context.output)


@main.command("clean")
@click.option("--preview", is_flag=True)
@click.option("--snapshots", is_flag=True)
@click.option("--runs", "clean_runs", is_flag=True)
@click.option("--local-database", is_flag=True)
@click.option("--older-than", default=None)
@click.option("--all", "clean_all", is_flag=True)
@click.option("--yes", is_flag=True)
@click.pass_obj
@guarded
def clean_command(
    context: CliContext,
    preview: bool,
    snapshots: bool,
    clean_runs: bool,
    local_database: bool,
    older_than: str | None,
    clean_all: bool,
    yes: bool,
) -> None:
    """Preview or remove exact local dbtv artifacts."""
    if not any((snapshots, clean_runs, local_database, clean_all)):
        raise ConfigError("Choose --snapshots, --runs, --local-database, or --all.")
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    config = _load_context_config(context, project.root)
    workspace = Workspace(project.root)
    workspace.ensure()
    cutoff = datetime.now(UTC) - parse_duration(older_than) if older_than else None
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("clean")
    targets: list[tuple[str, Path, str | None]] = []
    if snapshots or clean_all:
        for record in state.list_snapshots():
            created = datetime.fromisoformat(record.created_at)
            if cutoff and created >= cutoff:
                continue
            if not clean_all and (record.active or record.pinned):
                continue
            targets.append(("snapshot", record.metadata_path.parent, record.snapshot_key))
        pending_root = config.cache.root / "tmp"
        if pending_root.exists():
            for path in pending_root.iterdir():
                modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
                if cutoff and modified >= cutoff:
                    continue
                targets.append(("pending snapshot", path, None))
    if clean_runs or clean_all:
        paths = workspace.runs.iterdir() if workspace.runs.exists() else ()
        for path in paths:
            if not path.is_dir() or path.is_symlink():
                continue
            modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
            if cutoff and modified >= cutoff:
                continue
            targets.append(("run", path, None))
    if local_database or clean_all:
        for path in (config.local.database, Path(f"{config.local.database}.wal")):
            if path.exists():
                targets.append(("local database", path, None))
    if clean_all:
        if config.cache.root.exists():
            for path in config.cache.root.iterdir():
                targets.append(("snapshot cache", path, None))
        for directory, kind in (
            (workspace.catalogs, "catalog"),
            (workspace.generated, "generated"),
            (workspace.tmp, "temporary"),
            (workspace.manifests, "manifest cache"),
            (workspace.root / "diagnostics", "diagnostics"),
        ):
            if directory.exists():
                for path in directory.iterdir():
                    targets.append((kind, path, None))
        for path in (
            workspace.state_path,
            Path(f"{workspace.state_path}-wal"),
            Path(f"{workspace.state_path}-shm"),
        ):
            if path.exists():
                targets.append(("state index", path, None))
    payload = [{"kind": kind, "path": str(path)} for kind, path, _ in targets]
    if context.output == "json":
        click.echo(json.dumps({"preview": preview, "targets": payload}, sort_keys=True))
    else:
        if not targets:
            click.echo("Nothing matches the requested cleanup.")
        for item in payload:
            click.echo(f"{item['kind']}: {item['path']}")
    if preview or not targets:
        return
    if not yes:
        if context.non_interactive:
            raise ConfigError("Cleanup requires --yes in non-interactive mode.")
        click.confirm("Remove exactly these local artifacts?", abort=True)
    for kind, path, snapshot_key in targets:
        _validate_clean_target(path, workspace, config.local.database, config.cache.root)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
        if kind == "snapshot" and snapshot_key:
            state.delete_snapshot(snapshot_key)
    if context.output == "console":
        click.echo(f"Removed {len(targets)} artifact(s).")


def _validate_clean_target(
    path: Path,
    workspace: Workspace,
    local_database: Path,
    cache_root: Path,
) -> None:
    resolved = path.resolve()
    allowed = (
        resolved.is_relative_to(workspace.root.resolve())
        or resolved == local_database.resolve()
        or resolved.is_relative_to(cache_root.resolve())
    )
    if not allowed or resolved in {
        workspace.root.resolve(),
        workspace.project_dir.resolve(),
        cache_root.resolve(),
    }:
        raise ConfigError(f"Refusing unsafe cleanup target {resolved}.")


@main.command("diagnostics")
@click.option("--destination", type=click.Path(path_type=Path), default=None)
@click.pass_obj
@guarded
def diagnostics_command(context: CliContext, destination: Path | None) -> None:
    """Create a redacted diagnostics archive without cached data or credentials."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    config = _load_context_config(context, project.root)
    output = create_diagnostics_bundle(
        config=config,
        workspace=Workspace(project.root),
        destination=destination,
    )
    if context.output == "json":
        click.echo(json.dumps({"diagnostics": str(output)}, sort_keys=True))
    else:
        click.echo(f"Created {output}")


def _load_context_config(context: CliContext, project_dir: Path) -> Any:
    config = load_config(project_dir, context.config_path)
    project_settings = config.project.model_copy(
        update={
            **({"profile": context.profile} if context.profile else {}),
            **({"production_target": context.target} if context.target else {}),
        }
    )
    return config.model_copy(update={"project": project_settings})


def _installed_version(distribution: str) -> str | None:
    try:
        return package_version(distribution)
    except PackageNotFoundError:
        return None


def _resolved_dbt_executable() -> str | None:
    try:
        return DbtInvoker().resolved_executable()
    except DbtvError:
        return None


def _infer_profile_target(profiles_dir: Path, profile_name: str) -> str | None:
    path = profiles_dir / "profiles.yml"
    if not path.is_file():
        return None
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get(profile_name), dict):
        return None
    target = raw[profile_name].get("target")
    return str(target) if target else None


def _consume_event(context: CliContext, payload: dict[str, Any]) -> None:
    line = json.dumps(payload, sort_keys=True, default=str)
    severity = str(payload.get("severity", "info"))
    ranks = {"debug": 10, "info": 20, "warning": 30, "error": 40}
    if context.log_path:
        context.log_path.parent.mkdir(parents=True, exist_ok=True)
        with context.log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    if (
        context.output == "json"
        and not context.quiet
        and ranks.get(severity, 20) >= ranks[context.log_level]
    ):
        click.echo(line)
