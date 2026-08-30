from __future__ import annotations

import os
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

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
    FidelityMode,
    RunSummary,
    SamplingSpec,
    SamplingStrategy,
    SnapshotAction,
    SnapshotDecision,
    SnapshotRequest,
    SourceBinding,
    SourceCapabilities,
    SourceMode,
)
from dbtv.core.units import parse_duration, parse_size
from dbtv.credentials.registry import CredentialResolverRegistry
from dbtv.project.artifacts import load_run_results
from dbtv.project.dbt_invoker import DbtInvoker
from dbtv.project.discovery import DbtProject
from dbtv.project.planner import ProjectPlanner
from dbtv.project.profile import LOCAL_TARGET_NAME, write_local_profile
from dbtv.snapshot.policy import LocalPolicyEngine, apply_max_rows, resolve_sampling
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.state import StateIndex
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
        token = cancellation or CancellationToken()
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
        reused = 0
        refreshed = 0
        snapshots: tuple[DatasetSnapshot, ...] = ()
        plan: ExecutionPlan | None = None
        try:
            phase = time.perf_counter()
            invoker = DbtInvoker(config.project.dbt_executable)
            plan = ProjectPlanner(invoker).build(
                project=project,
                config=config,
                run=run,
                select=options.select,
                exclude=options.exclude,
                variables=options.variables,
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
            transition("PLANNED")
            blocking = [finding for finding in plan.findings if finding.severity == "error"]
            if blocking:
                raise CompatibilityError(
                    f"Plan contains {len(blocking)} blocking compatibility finding(s).",
                    hint="Review plan.json or use configured rule overrides only after validation.",
                )

            profile_name = options.data_profile or config.default_data_profile
            if profile_name not in config.data_profiles:
                raise CompatibilityError(f"Unknown data profile {profile_name!r}.")
            profile = config.data_profiles[profile_name]
            requests: list[SnapshotRequest] = []
            policy = LocalPolicyEngine(
                config.policy,
                allow_full_source=options.allow_full_source,
            )
            fidelity = options.fidelity or FidelityMode(config.compatibility.mode)
            for mapping in plan.source_mappings:
                sampling = resolve_sampling(profile, mapping.source)
                sampling = apply_max_rows(sampling, options.max_rows)
                policy.evaluate_sampling(
                    mapping.source,
                    sampling,
                    tags=mapping.tags,
                    fidelity=fidelity,
                )
                requests.append(
                    SnapshotRequest(
                        source=mapping.source,
                        relation=mapping.production_relation,
                        sampling=sampling,
                        fidelity=fidelity,
                        query_tag=(
                            f"{config.source.session.query_tag_prefix}/"
                            f"{run.invocation_id}/{mapping.source.unique_id}"
                        )[:256],
                    )
                )
            reporter.emit(RunEvent(run.invocation_id, "policy.allowed", "policy"))
            transition("POLICY_APPROVED")

            ttl = parse_duration(options.cache_ttl or config.cache.default_ttl)
            for mapping in plan.source_mappings:
                for tag in mapping.tags:
                    if tag in config.policy.max_cache_age_for_tags:
                        ttl = min(
                            ttl,
                            parse_duration(config.policy.max_cache_age_for_tags[tag]),
                        )
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
                    parse_size(options.max_bytes) if options.max_bytes else 2**63 - 1,
                ),
                retain_previous=config.cache.retain_previous_snapshots,
                file_mode=int(config.policy.cache_file_mode, 8),
                directory_mode=int(config.policy.cache_directory_mode, 8),
            )
            decisions = tuple(
                store.decide(
                    request,
                    provider=config.source.connector,
                    mode=options.source_mode.value,
                    snapshot_id=options.snapshot_id,
                )
                for request in requests
            )
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
                remote_attempted = True
                registry = self.connector_registry or ConnectorRegistry()
                capabilities = registry.capabilities(config.source.connector)
                for decision in refresh_decisions:
                    _validate_capabilities(decision.request.sampling, capabilities)
                resolver = self.credential_resolver or (
                    self.credential_registry or CredentialResolverRegistry()
                ).create(config.source.credential_resolver)
                credentials = resolver.resolve(
                    profiles_dir=project.profiles_dir,
                    profile_name=config.project.profile or project.profile_name,
                    target_name=plan.production_target,
                    env=os.environ,
                    interactive=options.interactive,
                )
                refreshed_snapshots = self._refresh(
                    refresh_decisions,
                    registry=registry,
                    credentials=credentials,
                    config=config,
                    store=store,
                    run=run,
                    reporter=reporter,
                    cancellation=token,
                    pinned=options.keep_snapshot,
                )
                resolved.update(
                    {snapshot.source.unique_id: snapshot for snapshot in refreshed_snapshots}
                )
                refreshed = len(refreshed_snapshots)
                remote_queries = refreshed
                timings["extraction"] = time.perf_counter() - phase

            snapshots = tuple(resolved[request.source.unique_id] for request in requests)
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
                reporter.emit(RunEvent(run.invocation_id, "binding.verified", "binding"))
                transition("BOUND")
                timings["binding"] = time.perf_counter() - phase

                phase = time.perf_counter()
                compile_args = self._dbt_args("compile", project, run, options)
                with FileLock(
                    workspace.locks / "duckdb-writer.lock",
                    invocation_id=run.invocation_id,
                    command=options.command,
                    timeout_seconds=0,
                ):
                    invoker.run(
                        compile_args,
                        cwd=project.root,
                        env=_local_dbt_environment(run),
                        cancellation=token,
                    )
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
                transition("DBT_COMPLETED")

            completed = datetime.now(UTC)
            state_name = "SUCCEEDED" if exit_code == 0 else "DBT_FAILED"
            summary = RunSummary(
                invocation_id=run.invocation_id,
                command=options.command,
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
            raise

    def _refresh(
        self,
        decisions: list[SnapshotDecision],
        *,
        registry: ConnectorRegistry,
        credentials: object,
        config: DbtvConfig,
        store: ParquetSnapshotStore,
        run: RunWorkspace,
        reporter: EventReporter,
        cancellation: CancellationToken,
        pinned: bool,
    ) -> tuple[DatasetSnapshot, ...]:
        def refresh(decision: SnapshotDecision) -> DatasetSnapshot:
            cancellation.raise_if_cancelled()
            reporter.emit(
                RunEvent(
                    run.invocation_id,
                    "snapshot.refresh_started",
                    "extraction",
                    attributes={"source_id": decision.request.source.unique_id},
                )
            )
            connector = registry.create(
                config.source.connector,
                credentials=credentials,
                session=config.source.session,
                extraction=config.source.extraction,
                plugin_config=config.source.plugin,
            )
            try:
                connector.open()
                source_version = connector.source_version(decision.request.relation)
                snapshot = store.write(
                    decision.request,
                    provider=config.source.connector,
                    batches=connector.extract(decision.request, cancellation),
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
                            "source_id": decision.request.source.unique_id,
                            "snapshot_key": snapshot.snapshot_key,
                            "rows": snapshot.row_count,
                            "bytes": snapshot.byte_count,
                        },
                    )
                )
                return snapshot
            finally:
                connector.close()

        workers = min(config.source.extraction.parallel_sources, len(decisions))
        futures: dict[Future[DatasetSnapshot], SnapshotDecision] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dbtv-extract") as pool:
            for decision in decisions:
                futures[pool.submit(refresh, decision)] = decision
            completed: list[DatasetSnapshot] = []
            try:
                for future in as_completed(futures):
                    completed.append(future.result())
            except BaseException:
                cancellation.cancel()
                for future in futures:
                    future.cancel()
                raise
        ordered = tuple(sorted(completed, key=lambda snapshot: snapshot.source.unique_id))
        for snapshot in ordered:
            store.activate(snapshot)
        return ordered

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
