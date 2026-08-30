from __future__ import annotations

import json
import os
import platform
import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import click

from dbtv import __version__
from dbtv.cli.render import render_checks, render_error, render_plan
from dbtv.config.loader import DEFAULT_CONFIG_NAME, load_config, render_default_config
from dbtv.core.errors import DbtvError, NotImplementedMilestone, ProjectError
from dbtv.project.dbt_invoker import DbtInvoker
from dbtv.project.discovery import discover_project
from dbtv.project.planner import ProjectPlanner
from dbtv.workspace import Workspace

P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class CliContext:
    project_dir: Path
    profiles_dir: Path | None
    config_path: Path | None
    output: str


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

    return wrapper


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--project-dir", type=click.Path(path_type=Path), default=Path.cwd)
@click.option("--profiles-dir", type=click.Path(path_type=Path), default=None)
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=None)
@click.option("--output", type=click.Choice(["console", "json"]), default="console")
@click.pass_context
def main(
    context: click.Context,
    project_dir: Path,
    profiles_dir: Path | None,
    config_path: Path | None,
    output: str,
) -> None:
    """Accelerate selected dbt work locally with source snapshots and DuckDB."""
    context.obj = CliContext(project_dir.resolve(), profiles_dir, config_path, output)


@main.command("version")
@click.pass_obj
def version_command(context: CliContext) -> None:
    """Show dbtv and runtime versions."""
    payload = {
        "dbtv": __version__,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "dbt_executable": shutil.which("dbt"),
    }
    if context.output == "json":
        click.echo(json.dumps(payload, sort_keys=True))
    else:
        for key, value in payload.items():
            click.echo(f"{key}: {value or 'not installed'}")


@main.command("init")
@click.option("--force", is_flag=True, help="Replace an existing dbtv.yml.")
@click.pass_obj
@guarded
def init_command(context: CliContext, force: bool) -> None:
    """Initialize dbtv configuration in an existing dbt project."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    destination = context.config_path or project.root / DEFAULT_CONFIG_NAME
    if destination.exists() and not force:
        raise ProjectError(
            f"Configuration already exists at {destination}.",
            hint="Use --force only if replacing it is intentional.",
        )
    destination.write_text(render_default_config(), encoding="utf-8")
    Workspace(project.root).ensure()
    click.echo(f"Created {destination}")
    click.echo(f"Workspace {project.root / '.dbtv'}")


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
        config = load_config(project.root, context.config_path)
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
            checks.append(
                {
                    "name": "dbt-snowflake",
                    "status": "ok" if "snowflake" in output else "error",
                    "detail": (
                        "adapter detected" if "snowflake" in output else "adapter not detected"
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
            checks.append(
                {"name": "dbt executable", "status": "error", "detail": error.message}
            )
        writable = os.access(project.root, os.W_OK)
        checks.append(
            {
                "name": "project writable",
                "status": "ok" if writable else "error",
                "detail": str(project.root),
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
@click.pass_obj
@guarded
def plan_command(
    context: CliContext,
    select_: tuple[str, ...],
    exclude: tuple[str, ...],
    variables: str | None,
) -> None:
    """Resolve the production/local graph and required source mappings."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    config = load_config(project.root, context.config_path)
    workspace = Workspace(project.root)
    run = workspace.create_run()
    planner = ProjectPlanner(DbtInvoker(config.project.dbt_executable))
    plan = planner.build(
        project=project,
        config=config,
        run=run,
        select=select_,
        exclude=exclude,
        variables=variables,
    )
    workspace.write_json(run.root / "plan.json", plan.to_dict())
    render_plan(plan, output=context.output)
    if context.output == "console":
        click.echo(f"Plan artifact: {run.root / 'plan.json'}")
    if any(finding.severity == "error" for finding in plan.findings):
        raise click.exceptions.Exit(9)


@main.command("status")
@click.pass_obj
@guarded
def status_command(context: CliContext) -> None:
    """Show local workspace and recent planning runs."""
    project = discover_project(context.project_dir, profiles_dir=context.profiles_dir)
    workspace = Workspace(project.root)
    if not workspace.root.exists():
        click.echo("No .dbtv workspace exists yet.")
        return
    runs = sorted(
        (path for path in workspace.runs.iterdir() if path.is_dir()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    ) if workspace.runs.exists() else []
    payload = {
        "workspace": str(workspace.root),
        "run_count": len(runs),
        "recent_runs": [path.name for path in runs[:5]],
        "local_database": str(project.root / ".dbtv" / "local.duckdb"),
    }
    if context.output == "json":
        click.echo(json.dumps(payload, sort_keys=True))
    else:
        for key, value in payload.items():
            click.echo(f"{key}: {value}")


def _milestone_command(name: str) -> Callable[..., None]:
    @click.pass_obj
    @guarded
    def command(_: CliContext) -> None:
        raise NotImplementedMilestone(name)

    return command


for _name in ("sync", "run", "test", "build", "inspect", "clean"):
    main.command(_name)(_milestone_command(_name))
