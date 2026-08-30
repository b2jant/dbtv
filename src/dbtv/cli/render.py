from __future__ import annotations

import json
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from dbtv.core.errors import DbtvError
from dbtv.core.models import ExecutionPlan


def render_error(error: DbtvError, *, output: str = "console") -> None:
    if output == "json":
        click.echo(
            json.dumps(
                {
                    "ok": False,
                    "error_id": error.error_id,
                    "message": error.message,
                    "hint": error.hint,
                    "exit_code": error.exit_code,
                },
                sort_keys=True,
            ),
            err=True,
        )
        return
    console = Console(stderr=True)
    console.print(f"[bold red]{error.message}[/bold red]")
    console.print(f"[dim]{error.error_id}[/dim]")
    if error.hint:
        console.print(f"[yellow]Next:[/yellow] {error.hint}")


def render_plan(plan: ExecutionPlan, *, output: str = "console") -> None:
    if output == "json":
        click.echo(json.dumps(plan.to_dict(), sort_keys=True))
        return
    console = Console()
    console.print(f"[bold]Plan {plan.plan_hash[:19]}[/bold]")
    console.print(
        f"Project: {plan.project_name}  Production: {plan.production_target}  "
        f"Local: {plan.local_target}"
    )
    console.print(
        f"Selected: {len(plan.selected_ids)} production / "
        f"{len(plan.local_selected_ids)} local"
    )

    table = Table(title="Required source mappings")
    table.add_column("dbt source")
    table.add_column("Snowflake relation")
    table.add_column("Local relation")
    for mapping in plan.source_mappings:
        table.add_row(
            mapping.source.unique_id,
            mapping.production_relation.display_name,
            mapping.local_relation.display_name,
        )
    console.print(table)
    for finding in plan.findings:
        style = "red" if finding.severity == "error" else "yellow"
        label = f"{finding.severity.upper()} {finding.rule_id}:"
        console.print(f"[{style}]{label}[/{style}] {finding.message}")


def render_checks(checks: list[dict[str, Any]], *, output: str = "console") -> None:
    if output == "json":
        click.echo(json.dumps({"checks": checks}, sort_keys=True))
        return
    table = Table(title="dbtv doctor")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail")
    for check in checks:
        status = str(check["status"])
        color = "green" if status == "ok" else "red"
        table.add_row(str(check["name"]), f"[{color}]{status}[/{color}]", str(check["detail"]))
    Console().print(table)
