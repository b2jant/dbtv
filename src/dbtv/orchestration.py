from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from queue import Empty, LifoQueue
from typing import Any

from dbtv.backend.duckdb import DuckDbExecutionBackend
from dbtv.compatibility import CompatibilityAnalyzer
from dbtv.config.schema import DbtvConfig
from dbtv.connectors.registry import ConnectorRegistry
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CompatibilityError, DbtvError, OfflineViolation, PolicyError
from dbtv.core.events import EventReporter, RunEvent
from dbtv.core.locks import FileLock
from dbtv.core.models import (
    CredentialResolver,
    DatasetSnapshot,
    DbtNodeResult,
    ExecutionPlan,
    ExtractionBatch,
    FidelityMode,
    RunSummary,
    SamplingSpec,
    SamplingStrategy,
    SnapshotAction,
    SourceBinding,
    SourceCapabilities,
    SourceMode,
)
from dbtv.credentials.registry import CredentialResolverRegistry
from dbtv.datasets import commit_dataset
from dbtv.project.artifacts import load_run_results
from dbtv.project.boundaries import dependency_boundaries, lineage
from dbtv.project.dbt_invoker import DbtInvoker
from dbtv.project.discovery import DbtProject
from dbtv.project.manifest import load_manifest
from dbtv.project.planner import ProjectPlanner
from dbtv.project.profile import LOCAL_TARGET_NAME, write_local_profile
from dbtv.resources import RunBudget
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.sources import PlannedSource, materialize_cohort, prepare_sources, source_levels
from dbtv.state import StateIndex
from dbtv.validation import capture_outputs, execution_context
from dbtv.workspace import RunWorkspace, Workspace


@dataclass(frozen=True)
class CommandOptions:
    command: str
    select: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    variables: str | None = None
    source_mode: SourceMode = SourceMode.AUTO
    data_profile: str | None = None
    cache_ttl: str | None = None
    max_rows: int | None = None
    max_bytes: str | None = None
    allow_full_source: bool = False
    snapshot_id: str | None = None
    keep_snapshot: bool = False
    fidelity: FidelityMode | None = None
    full_refresh: bool = False
    interactive: bool = True
    dataset_id: str | None = None
    capture_results: bool = False


@dataclass(frozen=True)
class OrchestrationResult:
    plan: ExecutionPlan
    summary: RunSummary
    snapshots: tuple[DatasetSnapshot, ...]
    dbt_stdout: str = ""
    dbt_stderr: str = ""


class Orchestrator:
    def __init__(
        self,
        *,
        connector_registry: ConnectorRegistry | None = None,
        credential_resolver: CredentialResolver | None = None,
        credential_registry: CredentialResolverRegistry | None = None,
    ) -> None:
        self.connector_registry = connector_registry
        self.credential_resolver = credential_resolver
        self.credential_registry = credential_registry

    def execute(
        self,
        *,
        project: DbtProject,
        config: DbtvConfig,
        workspace: Workspace,
        run: RunWorkspace,
        options: CommandOptions,
        cancellation: CancellationToken | None = None,
        event_consumer: Callable[[RunEvent], None] | None = None,
    ) -> OrchestrationResult:
        workspace.ensure()
        with FileLock(
            workspace.locks / "workspace-use.lock",
            invocation_id=run.invocation_id,
            command=options.command,
            timeout_seconds=0,
        ):
            return self._execute(
                project=project,
                config=config,
                workspace=workspace,
                run=run,
                options=options,
                cancellation=cancellation,
                event_consumer=event_consumer,
            )

    def _execute(
        self,
        *,
        project: DbtProject,
        config: DbtvConfig,
        workspace: Workspace,
        run: RunWorkspace,
        options: CommandOptions,
        cancellation: CancellationToken | None = None,
        event_consumer: Callable[[RunEvent], None] | None = None,
    ) -> OrchestrationResult:
        token = cancellation or CancellationToken()
        if options.capture_results and options.command not in {"run", "build"}:
            raise PolicyError("Result capture requires a run or build command.")
        config.local.temp_directory.mkdir(parents=True, exist_ok=True)
        started = datetime.now(UTC)
        timings: dict[str, float] = {}
        reporter = EventReporter(run.events_path, consumer=event_consumer)
        reporter.emit(
            RunEvent(
                run.invocation_id, "run.started", "startup", attributes={"command": options.command}
            )
        )
        state = StateIndex(workspace.state_path, workspace.locks)
        state.initialize(run.invocation_id)
        state.start_run(
            run.invocation_id, options.command, "PLANNING", started.isoformat(), run.root
        )
        transitions: list[dict[str, str]] = [{"state": "PLANNING", "at": started.isoformat()}]

        def transition(name: str) -> None:
            state.update_run_state(run.invocation_id, name)
            transitions.append({"state": name, "at": datetime.now(UTC).isoformat()})

        remote_attempted = False
        remote_queries = 0
        activity_lock = threading.Lock()

        def remote_event(event: str) -> None:
            nonlocal remote_attempted, remote_queries
            with activity_lock:
                if event == "connection":
                    remote_attempted = True
                elif event == "query_completed":
                    remote_queries += 1

        reused = 0
        refreshed = 0
        snapshots: tuple[DatasetSnapshot, ...] = ()
        plan: ExecutionPlan | None = None
        budget = RunBudget(
            config, project.root, token, max_rows=options.max_rows, max_bytes=options.max_bytes
        )
        try:
            budget.start()
            workspace.write_json(
                run.root / "execution-context.json",
                {
                    "config_fingerprint": execution_context(config, project.root),
                    "initial_database_exists": config.local.database.is_file(),
                },
            )
            phase = time.perf_counter()
            invoker = DbtInvoker(config.project.dbt_executable)
            plan = ProjectPlanner(invoker).build(
                project=project,
                config=config,
                run=run,
                select=options.select,
                exclude=options.exclude,
                variables=options.variables,
                cancellation=token,
            )
            timings["planning"] = time.perf_counter() - phase
            workspace.write_json(run.plan_path, plan.to_dict())
            reporter.emit(
                RunEvent(
                    run.invocation_id,
                    "plan.completed",
                    "planning",
                    attributes={
                        "plan_hash": plan.plan_hash,
                        "selected_count": len(plan.local_selected_ids),
                        "source_count": len(plan.source_mappings),
                    },
                )
            )
            local_manifest = load_manifest(run.local_target_path / "manifest.json")
            workspace.write_json(
                run.root / "lineage.json",
                lineage(
                    local_manifest,
                    plan.local_selected_ids,
                ),
            )
            transition("PLANNED")
            blocking = [finding for finding in plan.findings if finding.severity == "error"]
            if blocking:
                raise CompatibilityError(
                    f"Plan contains {len(blocking)} blocking compatibility finding(s).",
                    hint="Review plan.json or use configured rule overrides only after validation.",
                )

            preparation = prepare_sources(
                project=project,
                config=config,
                workspace=workspace,
                state=state,
                plan=plan,
                options=options,
            )
            store = preparation.store
            decisions = tuple(source.decision for source in preparation.sources)
            requests = [decision.request for decision in decisions]
            workspace.write_json(
                run.plan_path,
                {
                    **plan.to_dict(),
                    "source_decisions": [source.to_dict() for source in preparation.sources],
                },
            )
            reporter.emit(RunEvent(run.invocation_id, "policy.allowed", "policy"))
            transition("POLICY_APPROVED")
            missing = [
                decision for decision in decisions if decision.action is SnapshotAction.MISSING
            ]
            if missing:
                sources = ", ".join(decision.request.source.unique_id for decision in missing)
                raise OfflineViolation(
                    "Required committed snapshots are unavailable in "
                    f"{options.source_mode.value} mode: {sources}."
                )
            refresh_decisions = [
                decision for decision in decisions if decision.action is SnapshotAction.REFRESH
            ]
            reused = sum(decision.action is SnapshotAction.REUSE for decision in decisions)
            if options.source_mode is SourceMode.OFFLINE and refresh_decisions:
                raise OfflineViolation("Offline execution cannot refresh snapshots.")

            resolved: dict[str, DatasetSnapshot] = {
                decision.request.source.unique_id: decision.snapshot
                for decision in decisions
                if decision.action is SnapshotAction.REUSE and decision.snapshot is not None
            }
            if options.keep_snapshot:
                resolved = {
                    source_id: store.pin(snapshot) for source_id, snapshot in resolved.items()
                }
            for decision in decisions:
                if decision.action is SnapshotAction.REUSE:
                    reporter.emit(
                        RunEvent(
                            run.invocation_id,
                            "snapshot.cache_hit",
                            "snapshot",
                            attributes={"source_id": decision.request.source.unique_id},
                        )
                    )

            if refresh_decisions:
                phase = time.perf_counter()
                registry = self.connector_registry or ConnectorRegistry()
                refresh_sources = [
                    source
                    for source in preparation.sources
                    if source.decision.action is SnapshotAction.REFRESH
                ]
                refreshed_snapshots = self._refresh(
                    refresh_sources,
                    registry=registry,
                    project=project,
                    config=config,
                    options=options,
                    store=store,
                    run=run,
                    reporter=reporter,
                    cancellation=token,
                    pinned=options.keep_snapshot,
                    budget=budget,
                    known=resolved,
                    on_remote_event=remote_event,
                )
                resolved.update(
                    {snapshot.source.unique_id: snapshot for snapshot in refreshed_snapshots}
                )
                refreshed = len(refreshed_snapshots)
                timings["extraction"] = time.perf_counter() - phase

            snapshots = tuple(resolved[request.source.unique_id] for request in requests)
            workspace.write_json(
                run.root / "cohorts.json",
                [
                    {
                        "source_id": source.decision.request.source.unique_id,
                        "parent_source_id": source.cohort_parent,
                        "parent_snapshot": resolved[source.cohort_parent].snapshot_key,
                        "parent_rows": resolved[source.cohort_parent].row_count,
                        "matching_child_rows": resolved[
                            source.decision.request.source.unique_id
                        ].row_count,
                        "max_keys": source.cohort.max_keys,
                        "coverage": "Matching non-null parent keys within the child predicate; "
                        "budgets abort instead of truncating children. Captures are independent.",
                    }
                    for source in preparation.sources
                    if source.cohort and source.cohort_parent
                ],
            )
            transition("SOURCES_READY")
            findings = tuple(
                finding
                for snapshot in snapshots
                for finding in CompatibilityAnalyzer(config.compatibility).analyze_schema(
                    snapshot.schema, snapshot.source.unique_id
                )
            )
            workspace.write_json(
                run.root / "compatibility.json", [asdict(item) for item in findings]
            )
            if any(finding.severity == "error" for finding in findings):
                raise CompatibilityError("Snapshot type fidelity analysis found blocking issues.")

            token.raise_if_cancelled()
            budget.check_disk()
            dataset_id = commit_dataset(
                state,
                workspace,
                run,
                plan,
                snapshots,
                asdict(options),
            )
            dbt_stdout = ""
            dbt_stderr = ""
            dbt_results: tuple[DbtNodeResult, ...] = ()
            exit_code = 0
            if options.command != "sync":
                phase = time.perf_counter()
                bindings = [
                    SourceBinding(mapping, resolved[mapping.source.unique_id])
                    for mapping in plan.source_mappings
                ]
                backend = DuckDbExecutionBackend(
                    config.local.database,
                    workspace,
                    install_compatibility_shims=config.compatibility.install_shims,
                )
                binding_report = backend.bind(bindings, run.invocation_id)
                binding_report = backend.verify(binding_report, bindings)
                workspace.write_json(
                    run.bindings_path,
                    {
                        "results": [asdict(result) for result in binding_report.results],
                        "attachments": {
                            name: str(path) for name, path in binding_report.attachments.items()
                        },
                    },
                )
                for binding in bindings:
                    state.record_binding(run.invocation_id, binding)
                write_local_profile(
                    run.generated_profiles_dir,
                    project,
                    config,
                    attachments=binding_report.attachments,
                )
                boundaries = backend.check_prerequisites(
                    dependency_boundaries(local_manifest, plan.local_selected_ids, options.command),
                    dict(binding_report.attachments),
                )
                workspace.write_json(run.root / "dependencies.json", boundaries)
                missing_models = [item["unique_id"] for item in boundaries if not item["available"]]
                if missing_models:
                    raise CompatibilityError(
                        "Missing local prerequisites: " + ", ".join(missing_models),
                        hint="Use --select +model for ancestors and build for seeds "
                        "and snapshots, or materialize these prerequisites locally first.",
                    )
                reporter.emit(RunEvent(run.invocation_id, "binding.verified", "binding"))
                transition("BOUND")
                timings["binding"] = time.perf_counter() - phase

                phase = time.perf_counter()
                with FileLock(
                    workspace.locks / "duckdb-writer.lock",
                    invocation_id=run.invocation_id,
                    command=options.command,
                    timeout_seconds=0,
                ):
                    reporter.emit(
                        RunEvent(
                            run.invocation_id,
                            "dbt.execution.started",
                            "dbt",
                            attributes={"command": options.command},
                        )
                    )
                    result = invoker.run(
                        self._dbt_args(options.command, project, run, options),
                        cwd=project.root,
                        env=_local_dbt_environment(run),
                        check=False,
                        cancellation=token,
                    )
                dbt_stdout, dbt_stderr = result.stdout, result.stderr
                exit_code = 0 if result.return_code == 0 else 1
                dbt_results = load_run_results(run.local_target_path / "run_results.json")
                workspace.write_json(
                    run.root / "dbt-results.json",
                    [asdict(item) for item in dbt_results],
                )
                timings["dbt"] = time.perf_counter() - phase
                if options.capture_results and exit_code == 0:
                    phase = time.perf_counter()
                    capture_outputs(
                        config=config,
                        workspace=workspace,
                        run=run,
                        selected=tuple(
                            item.unique_id for item in dbt_results if item.status == "success"
                        ),
                        attachments=dict(binding_report.attachments),
                        token=token,
                    )
                    timings["result_capture"] = time.perf_counter() - phase
                transition("DBT_COMPLETED")

            token.raise_if_cancelled()
            budget.check_disk()
            completed = datetime.now(UTC)
            state_name = "SUCCEEDED" if exit_code == 0 else "DBT_FAILED"
            summary = RunSummary(
                invocation_id=run.invocation_id,
                command=options.command,
                dataset_id=dataset_id,
                state=state_name,
                exit_code=exit_code,
                remote_connection_attempted=remote_attempted,
                remote_query_count=remote_queries,
                snapshots_reused=reused,
                snapshots_refreshed=refreshed,
                local_database=config.local.database,
                dbt_target=LOCAL_TARGET_NAME,
                run_artifact_dir=run.root,
                started_at=started.isoformat(),
                completed_at=completed.isoformat(),
                timings=timings,
                warnings=tuple(
                    finding.message for finding in plan.findings if finding.severity == "warning"
                ),
                dbt_results=dbt_results,
                state_transitions=tuple(
                    [*transitions, {"state": state_name, "at": completed.isoformat()}]
                ),
            )
            workspace.write_json(run.run_path, summary.to_dict())
            workspace.write_json(run.timings_path, timings)
            state.finish_run(
                run.invocation_id,
                state=state_name,
                completed_at=completed.isoformat(),
                remote_query_count=remote_queries,
                exit_code=exit_code,
            )
            reporter.emit(
                RunEvent(
                    run.invocation_id,
                    "run.completed",
                    "complete",
                    attributes=summary.to_dict(),
                )
            )
            return OrchestrationResult(plan, summary, snapshots, dbt_stdout, dbt_stderr)
        except BaseException as exc:
            exc = budget.failure or exc
            completed = datetime.now(UTC)
            exit_code = exc.exit_code if isinstance(exc, DbtvError) else 10
            if isinstance(exc, DbtvError):
                exc.context.setdefault("artifact_path", str(run.root))
                exc.context.setdefault("phase", transitions[-1]["state"])
            state.finish_run(
                run.invocation_id,
                state="FAILED",
                completed_at=completed.isoformat(),
                remote_query_count=remote_queries,
                exit_code=exit_code,
            )
            failure = {
                "invocation_id": run.invocation_id,
                "command": options.command,
                "state": "FAILED",
                "exit_code": exit_code,
                "remote_connection_attempted": remote_attempted,
                "remote_query_count": remote_queries,
                "started_at": started.isoformat(),
                "completed_at": completed.isoformat(),
                "error_id": exc.error_id if isinstance(exc, DbtvError) else "DBTV-INTERNAL-001",
                "message": str(exc),
                "run_artifact_dir": str(run.root),
                "state_transitions": [
                    *transitions,
                    {"state": "FAILED", "at": completed.isoformat()},
                ],
            }
            workspace.write_json(run.run_path, failure)
            reporter.emit(
                RunEvent(
                    run.invocation_id,
                    "run.failed",
                    "complete",
                    severity="error",
                    attributes=failure,
                )
            )
            raise exc
        finally:
            budget.stop()
            workspace.write_json(run.root / "resources.json", budget.to_dict())

    def _refresh(
        self,
        sources: list[PlannedSource],
        *,
        registry: ConnectorRegistry,
        project: DbtProject,
        config: DbtvConfig,
        options: CommandOptions,
        store: ParquetSnapshotStore,
        run: RunWorkspace,
        reporter: EventReporter,
        cancellation: CancellationToken,
        pinned: bool,
        budget: RunBudget,
        known: dict[str, DatasetSnapshot],
        on_remote_event: Callable[[str], None],
    ) -> tuple[DatasetSnapshot, ...]:
        credentials: dict[str, object] = {}
        sessions: dict[str, LifoQueue[Any]] = {}
        for source in sources:
            settings = source.settings
            capabilities = registry.capabilities(settings.connector)
            _validate_capabilities(source.decision.request.sampling, capabilities)
            if source.decision.request.projection and not capabilities.projection_pushdown:
                raise PolicyError("The selected connector does not support projections.")
            if source.connection_name in sessions:
                continue
            sessions[source.connection_name] = LifoQueue()
            if settings.credential_resolver == "none":
                credentials[source.connection_name] = None
            else:
                resolver = self.credential_resolver or (
                    self.credential_registry or CredentialResolverRegistry()
                ).create(settings.credential_resolver)
                credentials[source.connection_name] = resolver.resolve(
                    profiles_dir=project.profiles_dir,
                    profile_name=settings.profile or config.project.profile or project.profile_name,
                    target_name=settings.target or config.project.production_target or "",
                    env=os.environ,
                    interactive=options.interactive,
                )

        def refresh(source: PlannedSource) -> DatasetSnapshot:
            cancellation.raise_if_cancelled()
            decision, settings = source.decision, source.settings
            source_id = decision.request.source.unique_id
            reporter.emit(
                RunEvent(
                    run.invocation_id,
                    "snapshot.refresh_started",
                    "extraction",
                    attributes={"source_id": source_id},
                )
            )
            available = sessions[source.connection_name]
            try:
                connector = available.get_nowait()
            except Empty:
                connector = registry.create(
                    settings.connector,
                    credentials=credentials[source.connection_name],
                    session=settings.session,
                    extraction=settings.extraction,
                    plugin_config=settings.plugin,
                )
                try:
                    if registry.capabilities(settings.connector).remote_access:
                        on_remote_event("connection")
                    connector.open()
                except BaseException:
                    connector.close()
                    raise
            try:
                source_version = connector.source_version(decision.request.relation)

                def bounded_batches() -> Iterable[ExtractionBatch]:
                    for batch in connector.extract(decision.request, cancellation):
                        cancellation.raise_if_cancelled()
                        budget.consume(source_id, batch.data.num_rows, batch.data.nbytes)
                        yield batch
                        budget.check_disk()
                    if registry.capabilities(settings.connector).remote_access:
                        on_remote_event("query_completed")
                    after = connector.source_version(decision.request.relation)
                    if source_version and after and source_version.value != after.value:
                        raise PolicyError(f"Source changed during extraction: {source_id}.")

                snapshot = store.write(
                    decision.request,
                    provider=settings.connector,
                    batches=bounded_batches(),
                    cancellation=cancellation,
                    invocation_id=run.invocation_id,
                    source_version=source_version,
                    provider_version=(
                        str(connector.version()) if hasattr(connector, "version") else None
                    ),
                    pinned=pinned,
                    activate=False,
                )
                reporter.emit(
                    RunEvent(
                        run.invocation_id,
                        "snapshot.committed",
                        "snapshot",
                        attributes={
                            "source_id": source_id,
                            "snapshot_key": snapshot.snapshot_key,
                            "rows": snapshot.row_count,
                            "bytes": snapshot.byte_count,
                        },
                    )
                )
                return snapshot
            finally:
                available.put(connector)

        completed: list[DatasetSnapshot] = []
        try:
            for level in source_levels(sources):
                prepared = [
                    materialize_cohort(source, known[source.cohort_parent], config)
                    if source.cohort_parent
                    else source
                    for source in level
                ]
                workers = min(config.source.extraction.parallel_sources, len(prepared))
                futures: dict[Future[DatasetSnapshot], PlannedSource] = {}
                with ThreadPoolExecutor(
                    max_workers=workers, thread_name_prefix="dbtv-extract"
                ) as pool:
                    for source in prepared:
                        futures[pool.submit(refresh, source)] = source
                    try:
                        for future in as_completed(futures):
                            snapshot = future.result()
                            completed.append(snapshot)
                            known[snapshot.source.unique_id] = snapshot
                    except BaseException:
                        cancellation.cancel()
                        for future in futures:
                            future.cancel()
                        raise
        finally:
            for available in sessions.values():
                while not available.empty():
                    available.get_nowait().close()
        return tuple(sorted(completed, key=lambda snapshot: snapshot.source.unique_id))

    @staticmethod
    def _dbt_args(
        command: str,
        project: DbtProject,
        run: RunWorkspace,
        options: CommandOptions,
    ) -> list[str]:
        args = [
            command,
            "--project-dir",
            str(project.root),
            "--profiles-dir",
            str(run.generated_profiles_dir),
            "--target",
            LOCAL_TARGET_NAME,
            "--target-path",
            str(run.local_target_path),
        ]
        if options.select:
            args.extend(["--select", *options.select])
        if options.exclude:
            args.extend(["--exclude", *options.exclude])
        if options.variables:
            args.extend(["--vars", options.variables])
        if options.full_refresh and command in {"run", "build"}:
            args.append("--full-refresh")
        return args


def _validate_capabilities(
    sampling: SamplingSpec,
    capabilities: SourceCapabilities,
) -> None:
    if sampling.strategy is SamplingStrategy.HASH and not capabilities.deterministic_sampling:
        raise PolicyError("The selected connector does not support deterministic hash sampling.")
    if sampling.strategy is SamplingStrategy.BERNOULLI and not capabilities.bernoulli_sampling:
        raise PolicyError("The selected connector does not support Bernoulli sampling.")
    if sampling.strategy in {SamplingStrategy.WHERE, SamplingStrategy.WHERE_LIMIT} and not (
        capabilities.predicate_pushdown
    ):
        raise PolicyError("The selected connector cannot push down the sampling predicate.")
    if sampling.strategy in {SamplingStrategy.LIMIT, SamplingStrategy.WHERE_LIMIT} and not (
        capabilities.limit_pushdown
    ):
        raise PolicyError("The selected connector cannot push down the row limit.")


def _local_dbt_environment(run: RunWorkspace) -> dict[str, str]:
    log_path = run.root / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    return {
        "DBT_LOG_PATH": str(log_path),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
