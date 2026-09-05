"""Frozen-input replay and exact local result comparison, separate from dbt execution."""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

from dbtv.backend.duckdb import quote_duckdb_identifier as identifier
from dbtv.backend.duckdb import quote_duckdb_string as literal
from dbtv.config.schema import DbtvConfig
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CacheError, CompatibilityError, ConfigError
from dbtv.core.hashing import sha256_file, sha256_value
from dbtv.core.locks import FileLock
from dbtv.core.models import FidelityMode, ResourceType, SourceMode
from dbtv.datasets import engine_versions
from dbtv.project.discovery import DbtProject
from dbtv.project.fingerprint import project_files, project_fingerprint
from dbtv.project.manifest import load_manifest
from dbtv.resources import RunBudget
from dbtv.state import StateIndex
from dbtv.workspace import RunWorkspace, Workspace


def execution_context(config: DbtvConfig, project_dir: Path) -> str:
    value = config.model_dump(mode="json")
    # Fresh replay output locations do not change SQL/configuration semantics.
    for key in ("database", "temp_directory"):
        value["local"].pop(key)
    environment: set[str] = set()
    dynamic = False
    for path in project_files(project_dir):
        if path.suffix == ".csv":
            continue
        code = path.read_text(errors="replace")
        names = re.findall(r"\benv_var\s*\(\s*['\"]([^'\"]+)['\"]", code)
        environment.update(names)
        dynamic |= len(re.findall(r"\benv_var\s*\(", code)) > len(names)
    selected_environment: dict[str, str | None] = (
        {key: value for key, value in os.environ.items() if key not in {"_", "SHLVL", "OLDPWD"}}
        if dynamic
        else {name: os.environ.get(name) for name in sorted(environment)}
    )
    value["environment_fingerprint"] = sha256_value(selected_environment)
    return sha256_value(value)


def read_run(workspace: Workspace, run_id: str) -> tuple[Path, dict[str, Any]]:
    try:
        path = workspace.runs / str(uuid.UUID(run_id))
        raw = json.loads((path / "dataset.json").read_text())
    except (ValueError, OSError) as exc:
        raise ConfigError("Run must identify an existing run with a committed dataset.") from exc
    return path, dict(raw)


def isolated_config(config: DbtvConfig, run: RunWorkspace) -> DbtvConfig:
    return config.model_copy(
        update={
            "local": config.local.model_copy(
                update={
                    "database": run.root / "database" / config.local.database.name,
                    "temp_directory": run.root / "spill",
                }
            )
        }
    )


def isolated_workspace(workspace: Workspace, run: RunWorkspace) -> Workspace:
    isolated = Workspace(workspace.project_dir)
    isolated.catalogs = run.root / "catalogs"
    return isolated


def capture_outputs(
    *,
    config: DbtvConfig,
    workspace: Workspace,
    run: RunWorkspace,
    selected: tuple[str, ...],
    attachments: dict[str, Path],
    token: CancellationToken,
) -> None:
    import duckdb

    manifest = load_manifest(run.local_target_path / "manifest.json")
    directory = run.root / "results"
    directory.mkdir(exist_ok=True)
    connection = duckdb.connect(
        str(config.local.database),
        read_only=True,
        config={
            "memory_limit": config.local.memory_limit,
            "threads": config.local.threads,
            "temp_directory": str(config.local.temp_directory),
            "max_temp_directory_size": config.local.max_temp_directory_size,
        },
    )
    unregister = token.register(connection.interrupt)
    outputs: dict[str, Any] = {}
    try:
        for catalog, path in attachments.items():
            connection.execute(f"ATTACH {literal(str(path))} AS {identifier(catalog)} (READ_ONLY)")
        for node_id in selected:
            node = manifest.get(node_id)
            if (
                node is None
                or node.resource_type
                not in {
                    ResourceType.MODEL,
                    ResourceType.SEED,
                    ResourceType.SNAPSHOT,
                }
                or node.config.get("materialized") == "ephemeral"
                or node.relation is None
            ):
                continue
            relation = ".".join(
                identifier(part)
                for part in (
                    node.relation.catalog,
                    node.relation.schema,
                    node.relation.identifier,
                )
                if part
            )
            token.raise_if_cancelled()
            filename = sha256_value(node_id).split(":", 1)[1] + ".parquet"
            path = directory / filename
            row = connection.execute(
                f"COPY (SELECT * FROM {relation}) TO {literal(str(path))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)",
            ).fetchone()
            assert row is not None
            outputs[node_id] = {
                "file": filename,
                "checksum": sha256_file(path),
                "rows": int(row[0]),
                "bytes": path.stat().st_size,
            }
        workspace.write_json(
            directory / "index.json",
            {
                "format_version": 1,
                "outputs": outputs,
                "comparison": "Exact multiset equality; unordered rows, duplicates retained.",
            },
        )
    finally:
        unregister()
        connection.close()


def compare_runs(
    workspace: Workspace,
    left_id: str,
    right_id: str,
    config: DbtvConfig,
    *,
    allow_different_inputs: bool = False,
    token: CancellationToken | None = None,
) -> dict[str, Any]:
    cancellation = token or CancellationToken()
    budget = RunBudget(config, workspace.project_dir, cancellation)
    with FileLock(
        workspace.locks / "workspace-use.lock",
        invocation_id="comparison",
        command="compare",
        timeout_seconds=0,
    ):
        try:
            budget.start()
            result = _compare_runs(
                workspace,
                left_id,
                right_id,
                config,
                allow_different_inputs=allow_different_inputs,
                token=cancellation,
            )
            cancellation.raise_if_cancelled()
            budget.check_disk()
            return result
        except BaseException as exc:
            if budget.failure:
                raise budget.failure from exc
            raise
        finally:
            budget.stop()


def _compare_runs(
    workspace: Workspace,
    left_id: str,
    right_id: str,
    config: DbtvConfig,
    *,
    allow_different_inputs: bool = False,
    token: CancellationToken | None = None,
) -> dict[str, Any]:
    import duckdb

    left_path, left = read_run(workspace, left_id)
    right_path, right = read_run(workspace, right_id)
    same_inputs = left["dataset_id"] == right["dataset_id"]
    if not same_inputs and not allow_different_inputs:
        raise CompatibilityError(
            "Result comparison requires the same frozen inputs.",
            hint="Use --allow-different-inputs for an intentional data comparison.",
        )
    indices = []
    for root in (left_path, right_path):
        try:
            indices.append(json.loads((root / "results" / "index.json").read_text())["outputs"])
        except (OSError, ValueError, KeyError) as exc:
            raise ConfigError("Both runs need --capture-results for comparison.") from exc
    cancellation = token or CancellationToken()
    connection = duckdb.connect(
        config={
            "memory_limit": config.local.memory_limit,
            "threads": config.local.threads,
            "temp_directory": str(config.local.temp_directory),
            "max_temp_directory_size": config.local.max_temp_directory_size,
        }
    )
    unregister = cancellation.register(connection.interrupt)
    results = []
    try:
        for node_id in sorted(set(indices[0]) | set(indices[1])):
            if any(node_id not in index for index in indices):
                results.append({"unique_id": node_id, "equal": False, "reason": "missing output"})
                continue
            paths = []
            for root, index in zip((left_path, right_path), indices, strict=True):
                entry = index[node_id]
                path = root / "results" / str(entry["file"])
                if (
                    path.parent.resolve() != (root / "results").resolve()
                    or not path.is_file()
                    or sha256_file(path) != entry["checksum"]
                ):
                    raise CacheError("Captured result file is missing or corrupt.")
                paths.append(path)
            for label, path in zip(("lhs", "rhs"), paths, strict=True):
                connection.execute(
                    f"CREATE OR REPLACE VIEW {label} AS "
                    f"SELECT * FROM read_parquet({literal(str(path))})"
                )
            schemas = [
                connection.execute(f"DESCRIBE {label}").fetchall() for label in ("lhs", "rhs")
            ]
            if schemas[0] != schemas[1]:
                results.append(
                    {
                        "unique_id": node_id,
                        "equal": False,
                        "reason": "schema differs",
                        "left_schema": schemas[0],
                        "right_schema": schemas[1],
                    }
                )
                continue
            counts = []
            for a, b in (("lhs", "rhs"), ("rhs", "lhs")):
                row = connection.execute(
                    f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})"
                ).fetchone()
                assert row is not None
                counts.append(int(row[0]))
            results.append(
                {
                    "unique_id": node_id,
                    "equal": counts == [0, 0],
                    "left_only_rows": counts[0],
                    "right_only_rows": counts[1],
                }
            )
        if not results:
            raise ConfigError("No materialized outputs were captured to compare.")
        return {
            "left_run": left_id,
            "right_run": right_id,
            "same_inputs": same_inputs,
            "equal": all(item["equal"] for item in results),
            "results": results,
            "semantics": "Exact DuckDB multiset equality, including duplicate counts.",
        }
    finally:
        unregister()
        connection.close()


def replay_run(
    *,
    project: DbtProject,
    config: DbtvConfig,
    workspace: Workspace,
    original_id: str,
    allow_code_change: bool = False,
    full_refresh: bool = False,
    cancellation: CancellationToken | None = None,
) -> Any:
    from dbtv.orchestration import CommandOptions, Orchestrator

    original_path, record = read_run(workspace, original_id)
    context = json.loads((original_path / "execution-context.json").read_text())
    if not allow_code_change and (
        record["project_fingerprint"] != project_fingerprint(project.root)
        or record["engine_versions"] != engine_versions()
        or context["config_fingerprint"] != execution_context(config, project.root)
    ):
        raise CompatibilityError(
            "Project, engine, or execution configuration changed since this run.",
            hint="Use --allow-code-change to validate an intentional change.",
        )
    invocation = record["invocation"]
    if "<redacted>" in json.dumps(invocation):
        raise ConfigError("Recorded invocation contains redacted values and cannot be replayed.")
    options = CommandOptions(
        **{
            **invocation,
            "select": tuple(invocation["select"]),
            "exclude": tuple(invocation["exclude"]),
            "source_mode": SourceMode.OFFLINE,
            "dataset_id": record["dataset_id"],
            "snapshot_id": None,
            "fidelity": FidelityMode(invocation["fidelity"]) if invocation["fidelity"] else None,
            "full_refresh": full_refresh or invocation["full_refresh"],
            "capture_results": True,
        }
    )
    if options.command not in {"run", "build"}:
        raise ConfigError("Replay requires a run or build invocation.")
    manifest = load_manifest(original_path / "local-target" / "manifest.json")
    plan = json.loads((original_path / "plan.json").read_text())
    if not options.full_refresh and any(
        manifest.nodes[item].config.get("materialized") == "incremental"
        for item in plan["local_selected_ids"]
        if item in manifest.nodes
    ):
        raise CompatibilityError(
            "Replay has no original incremental output state.",
            hint="Use --full-refresh for a fresh rebuild, or validate-incremental.",
        )
    run = workspace.create_run()
    result = Orchestrator().execute(
        project=project,
        config=isolated_config(config, run),
        workspace=isolated_workspace(workspace, run),
        run=run,
        options=options,
        cancellation=cancellation,
    )
    workspace.write_json(
        run.root / "replay.json",
        {
            "original_run": original_id,
            "isolated_outputs": True,
            "allow_code_change": allow_code_change,
            "full_refresh": options.full_refresh,
        },
    )
    return result


def validate_incremental(
    *,
    project: DbtProject,
    config: DbtvConfig,
    workspace: Workspace,
    base_dataset: str,
    next_dataset: str,
    select: tuple[str, ...],
    cancellation: CancellationToken | None = None,
) -> dict[str, Any]:
    from dbtv.orchestration import CommandOptions, Orchestrator

    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("incremental-validation")
    original = state.get_dataset(base_dataset)
    invocation = original["invocation"]
    if "<redacted>" in json.dumps(invocation):
        raise ConfigError("Dataset invocation contains redacted values.")
    options = CommandOptions(
        **{
            **invocation,
            "command": "build",
            "select": select or tuple(invocation["select"]),
            "exclude": tuple(invocation["exclude"]),
            "source_mode": SourceMode.OFFLINE,
            "fidelity": FidelityMode(invocation["fidelity"]) if invocation["fidelity"] else None,
            "capture_results": True,
            "dataset_id": base_dataset,
            "snapshot_id": None,
            "full_refresh": True,
        }
    )
    seed_run = workspace.create_run()
    branch_config = isolated_config(config, seed_run)
    branch_workspace = isolated_workspace(workspace, seed_run)
    seed = Orchestrator().execute(
        project=project,
        config=branch_config,
        workspace=branch_workspace,
        run=seed_run,
        options=options,
        cancellation=cancellation,
    )
    if seed.summary.exit_code:
        raise CompatibilityError("Initial incremental scenario build failed.")
    manifest = load_manifest(seed_run.local_target_path / "manifest.json")
    if not any(
        manifest.nodes[item].config.get("materialized") == "incremental"
        for item in seed.plan.local_selected_ids
        if item in manifest.nodes
    ):
        raise ConfigError("Selection contains no incremental model.")
    step_run = workspace.create_run()
    step = Orchestrator().execute(
        project=project,
        config=branch_config,
        workspace=branch_workspace,
        run=step_run,
        options=replace(options, dataset_id=next_dataset, full_refresh=False),
        cancellation=cancellation,
    )
    reference_run = workspace.create_run()
    reference = Orchestrator().execute(
        project=project,
        config=isolated_config(config, reference_run),
        workspace=isolated_workspace(workspace, reference_run),
        run=reference_run,
        options=replace(options, dataset_id=next_dataset),
        cancellation=cancellation,
    )
    if step.summary.exit_code or reference.summary.exit_code:
        raise CompatibilityError("Incremental or reference build failed; inspect run artifacts.")
    report = compare_runs(
        workspace, step_run.invocation_id, reference_run.invocation_id, config, token=cancellation
    )
    report.update(
        {
            "base_dataset": base_dataset,
            "next_dataset": next_dataset,
            "initial_run": seed_run.invocation_id,
            "scope": "Configured two-state scenario; not a proof for all production inputs.",
        }
    )
    workspace.write_json(step_run.root / "incremental-validation.json", report)
    return report
