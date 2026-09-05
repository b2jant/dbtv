"""Shared local source planning. This module never creates a connector or resolves secrets."""

from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from dbtv.config.schema import CohortRule, DbtvConfig, SourceSettings
from dbtv.core.errors import ConfigError, PolicyError
from dbtv.core.hashing import sha256_value
from dbtv.core.models import (
    DatasetSnapshot,
    ExecutionPlan,
    FidelityMode,
    SamplingSpec,
    SamplingStrategy,
    SnapshotAction,
    SnapshotDecision,
    SnapshotRequest,
    SourceMapping,
)
from dbtv.core.units import parse_duration, parse_size
from dbtv.credentials.dbt_profile import _resolve_value
from dbtv.project.discovery import DbtProject
from dbtv.snapshot.policy import LocalPolicyEngine, apply_max_rows, resolve_sampling
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.state import StateIndex
from dbtv.workspace import Workspace

if TYPE_CHECKING:
    from dbtv.orchestration import CommandOptions

# Only public connection/access fields enter the scope. Authentication secrets are never read.
_SCOPE_FIELDS = {
    "type",
    "account",
    "host",
    "port",
    "user",
    "role",
    "database",
    "schema",
    "warehouse",
    "authenticator",
    "path",
    "project",
    "location",
    "catalog",
}


@dataclass(frozen=True)
class PlannedSource:
    connection_name: str
    settings: SourceSettings
    decision: SnapshotDecision
    cohort: CohortRule | None = None
    cohort_parent: str | None = None
    cohort_ready: bool = False

    def to_dict(self) -> dict[str, Any]:
        from dataclasses import asdict

        decision = self.decision
        return {
            "source_unique_id": decision.request.source.unique_id,
            "connection": self.connection_name,
            "connector": self.settings.connector,
            "connection_scope": decision.request.connection_scope,
            "action": decision.action.value,
            "reason": decision.reason,
            "sampling": asdict(decision.request.sampling),
            "projection": decision.request.projection,
            "snapshot_key": decision.snapshot.snapshot_key if decision.snapshot else None,
            "cohort_parent": self.cohort_parent,
            "local_input": self.settings.connector == "parquet",
        }


@dataclass(frozen=True)
class SourcePreparation:
    store: ParquetSnapshotStore
    sources: tuple[PlannedSource, ...]


def source_matches(pattern: str, mapping: SourceMapping) -> bool:
    source = mapping.source
    return any(
        fnmatch.fnmatchcase(value, pattern)
        for value in (
            source.unique_id,
            f"source:{source.source_name}.{source.table_name}",
            f"source:{source.package_name}.{source.source_name}.{source.table_name}",
        )
    )


def connection_scope(
    name: str,
    settings: SourceSettings,
    project: DbtProject,
    config: DbtvConfig,
) -> str:
    profile_name = settings.profile or config.project.profile or project.profile_name
    target = settings.target or config.project.production_target
    public: dict[str, Any] = {}
    if settings.credential_resolver == "dbt_profile":
        try:
            raw = yaml.safe_load((project.profiles_dir / "profiles.yml").read_text())
            output = raw[profile_name]["outputs"][target]
            public = {
                key: _resolve_value(value, os.environ, key)
                for key, value in output.items()
                if key in _SCOPE_FIELDS
            }
        except (OSError, KeyError, TypeError, yaml.YAMLError) as exc:
            raise ConfigError("Cannot identify the configured source profile and target.") from exc
    return sha256_value(
        {
            "version": 2,
            "connection": name,
            "connector": settings.connector,
            "profile": profile_name,
            "target": target,
            "public": public,
            "identity": settings.identity,
            "plugin": settings.plugin,
        }
    )


def prepare_sources(
    *,
    project: DbtProject,
    config: DbtvConfig,
    workspace: Workspace,
    state: StateIndex,
    plan: ExecutionPlan,
    options: CommandOptions,
) -> SourcePreparation:
    if options.dataset_id and (options.snapshot_id or options.source_mode.value == "refresh"):
        raise ConfigError("A frozen dataset cannot be combined with snapshot-id or refresh mode.")
    profile_name = options.data_profile or config.default_data_profile
    if profile_name not in config.data_profiles:
        raise ConfigError(f"Unknown data profile {profile_name!r}.")
    ttl = parse_duration(options.cache_ttl or config.cache.default_ttl)
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
        maximum_size=parse_size(config.cache.maximum_size),
        retain_previous=config.cache.retain_previous_snapshots,
        file_mode=int(config.policy.cache_file_mode, 8),
        directory_mode=int(config.policy.cache_directory_mode, 8),
    )
    policy = LocalPolicyEngine(config.policy, allow_full_source=options.allow_full_source)
    fidelity = options.fidelity or FidelityMode(config.compatibility.mode)
    scopes: dict[str, str] = {}
    sources: list[PlannedSource] = []
    dataset = state.get_dataset(options.dataset_id) if options.dataset_id else None
    for mapping in plan.source_mappings:
        name, settings, projection = "default", config.source, None
        matching = [route for route in config.routes if source_matches(route.select, mapping)]
        if len(matching) > 1:
            raise ConfigError(f"Ambiguous source routes for {mapping.source.unique_id}.")
        if matching:
            route = matching[0]
            name, settings = route.connection, config.connections[route.connection]
            projection = tuple(route.projection) if route.projection else None
        # Normalize local file paths without importing provider code, including offline runs.
        if settings.connector == "parquet":
            plugin = dict(settings.plugin)
            tables = plugin.get("tables", {})
            if not isinstance(tables, dict):
                raise ConfigError("Parquet plugin.tables must map relation names to paths.")
            plugin["tables"] = {
                str(k): str((project.root / Path(str(v)).expanduser()).resolve())
                for k, v in tables.items()
            }
            settings = settings.model_copy(update={"plugin": plugin})
        if name not in scopes:
            scopes[name] = connection_scope(name, settings, project, config)
        sampling = apply_max_rows(
            resolve_sampling(config.data_profiles[profile_name], mapping.source),
            options.max_rows,
        )
        matching_cohorts = [
            rule
            for rule in config.data_profiles[profile_name].cohorts
            if source_matches(rule.select, mapping)
        ]
        if len(matching_cohorts) > 1:
            raise ConfigError(f"Ambiguous cohort rules for {mapping.source.unique_id}.")
        cohort = matching_cohorts[0] if matching_cohorts else None
        parent_id = None
        if cohort:
            parents = [item for item in plan.source_mappings if source_matches(cohort.parent, item)]
            if len(parents) != 1:
                raise ConfigError(
                    "Cohort parent must identify exactly one selected upstream source."
                )
            parent_id = parents[0].source.unique_id
            if (
                set(mapping.tags) & set(config.policy.require_explicit_where_for_tags)
                and not sampling.where
            ):
                raise PolicyError(
                    "Cohort source still requires its configured explicit where predicate."
                )
            sampling = SamplingSpec(SamplingStrategy.WHERE, where=sampling.where or "TRUE")
        policy.evaluate_sampling(mapping.source, sampling, tags=mapping.tags, fidelity=fidelity)
        request = SnapshotRequest(
            mapping.source,
            mapping.production_relation,
            sampling,
            fidelity,
            projection=projection,
            connection_scope=scopes[name],
            query_tag=(
                f"{settings.session.query_tag_prefix}/{plan.invocation_id}/"
                f"{mapping.source.unique_id}"
            )[:256],
        )
        snapshot_id = options.snapshot_id
        if dataset is not None:
            snapshot_id = dataset["snapshots"].get(mapping.source.unique_id)
            if snapshot_id is None:
                raise ConfigError(f"Dataset does not contain {mapping.source.unique_id}.")
        decision = store.decide(
            request,
            provider=settings.connector,
            mode="offline" if dataset else options.source_mode.value,
            snapshot_id=snapshot_id,
        )
        sources.append(PlannedSource(name, settings, decision, cohort, parent_id))
    by_id = {mapping.source.unique_id: mapping for mapping in plan.source_mappings}
    resolved: dict[str, PlannedSource] = {}
    for level in source_levels(sources):
        for source in level:
            if source.cohort_parent:
                parent = resolved[source.cohort_parent].decision
                if parent.action is SnapshotAction.REUSE and parent.snapshot:
                    source = materialize_cohort(source, parent.snapshot, config)
                    source_id = source.decision.request.source.unique_id
                    decision = store.decide(
                        source.decision.request,
                        provider=source.settings.connector,
                        mode="offline" if dataset else options.source_mode.value,
                        snapshot_id=(
                            dataset["snapshots"].get(source_id) if dataset else options.snapshot_id
                        ),
                    )
                    source = replace(source, decision=decision)
                else:
                    source = replace(
                        source,
                        decision=replace(
                            source.decision,
                            action=parent.action,
                            snapshot=None,
                            reason="Waiting for cohort parent capture",
                        ),
                    )
            source_id = source.decision.request.source.unique_id
            decision = source.decision
            if decision.action is SnapshotAction.REUSE and decision.snapshot:
                maximum = min(config.policy.max_rows_per_source, options.max_rows or 2**63 - 1)
                if decision.snapshot.row_count > maximum:
                    raise PolicyError(f"Cached rows exceed the current row budget for {source_id}.")
                for tag in by_id[source_id].tags:
                    maximum_age = config.policy.max_cache_age_for_tags.get(tag)
                    if maximum_age and (
                        datetime.now(UTC)
                        - datetime.fromisoformat(
                            decision.snapshot.verified_at or decision.snapshot.completed_at
                        )
                        > parse_duration(maximum_age)
                    ):
                        action = (
                            SnapshotAction.MISSING
                            if dataset
                            or options.source_mode.value
                            in {
                                "offline",
                                "cached",
                            }
                            else SnapshotAction.REFRESH
                        )
                        source = replace(
                            source,
                            decision=replace(
                                decision,
                                action=action,
                                snapshot=None,
                                reason="Source tag cache-age policy exceeded",
                            ),
                        )
            resolved[source_id] = source
    return SourcePreparation(store, tuple(resolved.values()))


def source_levels(sources: list[PlannedSource]) -> list[list[PlannedSource]]:
    remaining = list(sources)
    levels = []
    while remaining:
        pending_ids = {item.decision.request.source.unique_id for item in remaining}
        ready = [item for item in remaining if item.cohort_parent not in pending_ids]
        if not ready:
            raise ConfigError("Cohort source dependencies contain a cycle.")
        levels.append(ready)
        remaining = [item for item in remaining if item not in ready]
    return levels


def materialize_cohort(
    source: PlannedSource, parent: DatasetSnapshot, config: DbtvConfig
) -> PlannedSource:
    if source.cohort_ready or source.cohort is None:
        return source
    import duckdb

    from dbtv.backend.duckdb import quote_duckdb_identifier as identifier

    rule = source.cohort
    connection = duckdb.connect(
        config={
            "threads": 1,
            "memory_limit": "256MB",
            "temp_directory": str(config.local.temp_directory),
            "max_temp_directory_size": config.local.max_temp_directory_size,
        }
    )
    try:
        rows = connection.execute(
            f"SELECT DISTINCT {identifier(rule.parent_key)} FROM read_parquet(?) "
            f"WHERE {identifier(rule.parent_key)} IS NOT NULL "
            f"ORDER BY 1 LIMIT {rule.max_keys + 1}",
            [[str(path) for path in parent.parquet_paths]],
        ).fetchall()
    except duckdb.Error as exc:
        raise PolicyError(
            "Cannot read the configured cohort key from the parent snapshot."
        ) from exc
    finally:
        connection.close()
    if len(rows) > rule.max_keys:
        raise PolicyError(f"Cohort exceeds its {rule.max_keys} distinct-key limit.")
    values = ", ".join(_cohort_literal(row[0]) for row in rows)
    predicate = f"{identifier(rule.key)} IN ({values})" if rows else "FALSE"
    base = source.decision.request
    request = replace(
        base,
        sampling=SamplingSpec(
            SamplingStrategy.WHERE,
            where=f"({base.sampling.where or 'TRUE'}) AND ({predicate})",
        ),
        connection_scope=sha256_value(
            {
                "connection": base.connection_scope,
                "parent_snapshot": parent.snapshot_key,
                "recipe": rule.model_dump(),
            }
        ),
    )
    return replace(source, decision=replace(source.decision, request=request), cohort_ready=True)


def _cohort_literal(value: Any) -> str:
    import math
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (float, Decimal)) and math.isfinite(value):
        return str(value)
    if isinstance(value, (str, date, datetime)):
        text = str(value)
        if any(ord(character) < 32 for character in text):
            raise PolicyError("Cohort string keys cannot contain control characters.")
        # CHR is shared by the supported SQL engines and avoids dialect-specific
        # backslash escapes while keeping each fragment a standard quoted literal.
        parts = ["'" + part.replace("'", "''") + "'" for part in text.split(chr(92))]
        return "(" + " || CHR(92) || ".join(parts) + ")"
    raise PolicyError("Cohort keys must be finite numbers, strings, booleans, or dates.")
