from __future__ import annotations

import json
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from dbtv.core.errors import DbtvError
from dbtv.core.models import ExecutionPlan, RunSummary


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
                    "artifact_path": error.context.get("artifact_path"),
                    "phase": error.context.get("phase"),
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
    if artifact := error.context.get("artifact_path"):
        console.print(f"[dim]Artifacts: {artifact}[/dim]")


def render_plan(
    plan: ExecutionPlan,
    *,
    output: str = "console",
    source_decisions: list[dict[str, Any]] | None = None,
) -> None:
    if output == "json":
        payload = plan.to_dict()
        if source_decisions is not None:
            payload["source_decisions"] = source_decisions
        click.echo(json.dumps(payload, sort_keys=True))
        return
    console = Console()
    console.print(f"[bold]Plan {plan.plan_hash[:19]}[/bold]")
    console.print(
        f"Project: {plan.project_name}  Production: {plan.production_target}  "
        f"Local: {plan.local_target}"
    )
    console.print(
        f"Selected: {len(plan.selected_ids)} production / {len(plan.local_selected_ids)} local"
    )

    table = Table(title="Required source mappings")
    table.add_column("dbt source")
    table.add_column("Source relation")
    table.add_column("Local relation")
    for mapping in plan.source_mappings:
        table.add_row(
            mapping.source.unique_id,
            mapping.production_relation.display_name,
            mapping.local_relation.display_name,
        )
    console.print(table)
    if source_decisions:
        decisions = Table(title="Snapshot decisions")
        decisions.add_column("dbt source")
        decisions.add_column("Action")
        decisions.add_column("Working set")
        decisions.add_column("Estimate")
        decisions.add_column("Reason")
        for item in source_decisions:
            sampling = item["sampling"]
            working_set = sampling["strategy"]
            if sampling.get("limit"):
                working_set += f"={sampling['limit']}"
            estimated_rows = item.get("estimated_rows")
            estimated_bytes = item.get("estimated_bytes")
            estimate = "-"
            if estimated_rows is not None:
                estimate = f"{estimated_rows} rows"
            if estimated_bytes is not None:
                estimate += f" / {estimated_bytes} bytes"
            decisions.add_row(
                str(item["source_unique_id"]),
                str(item["action"]),
                working_set,
                estimate,
                str(item["reason"]),
            )
        console.print(decisions)
        remote = any(item["action"] == "refresh" for item in source_decisions)
        console.print(f"Remote access on execution: {'yes' if remote else 'no'}")
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


def render_summary(summary: RunSummary, *, output: str = "console") -> None:
    if output == "json":
        click.echo(json.dumps(summary.to_dict(), sort_keys=True))
        return
    console = Console()
    color = "green" if summary.exit_code == 0 else "red"
    console.print(f"[{color}][bold]{summary.state}[/bold][/{color}] {summary.command}")
    table = Table(show_header=False)
    table.add_column("Field", style="dim")
    table.add_column("Value")
    table.add_row("Remote connection attempted", str(summary.remote_connection_attempted).lower())
    table.add_row("Remote queries", str(summary.remote_query_count))
    table.add_row(
        "Snapshots", f"{summary.snapshots_reused} reused, {summary.snapshots_refreshed} refreshed"
    )
    table.add_row("DuckDB", str(summary.local_database))
    table.add_row("dbt target", summary.dbt_target)
    if summary.dbt_results:
        statuses: dict[str, int] = {}
        for result in summary.dbt_results:
            statuses[result.status] = statuses.get(result.status, 0) + 1
        table.add_row(
            "dbt results",
            ", ".join(f"{status}={count}" for status, count in sorted(statuses.items())),
        )
    table.add_row("Artifacts", str(summary.run_artifact_dir))
    table.add_row(
        "Timing", ", ".join(f"{key}={value:.2f}s" for key, value in summary.timings.items())
    )
    console.print(table)
    for warning in summary.warnings:
        console.print(f"[yellow]WARNING:[/yellow] {warning}")


def render_rows(
    columns: tuple[str, ...],
    rows: tuple[tuple[Any, ...], ...],
    *,
    output: str = "console",
) -> None:
    if output == "json":
        click.echo(
            json.dumps(
                {
                    "columns": columns,
                    "rows": [dict(zip(columns, row, strict=True)) for row in rows],
                },
                sort_keys=True,
                default=str,
            )
        )
        return
    table = Table()
    for column in columns:
        table.add_column(column)
    for row in rows:
        table.add_row(*(str(value) for value in row))
    Console().print(table)
