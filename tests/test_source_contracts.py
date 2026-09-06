from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pytest
from test_cli_contracts import plan
from test_local_runtime import parquet_project
from test_snapshot_store import _request, _store

from dbtv.config.loader import load_config
from dbtv.config.schema import CohortRule, SourceRoute, SourceSettings
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import ConfigError, PolicyError
from dbtv.core.models import ExtractionBatch, SnapshotAction, SourceMapping, SourceMode
from dbtv.orchestration import CommandOptions
from dbtv.project.discovery import discover_project
from dbtv.sources import _cohort_literal, connection_scope, materialize_cohort, prepare_sources
from dbtv.workspace import Workspace


@pytest.fixture
def source_plan(tmp_path):
    project_dir, profiles = parquet_project(tmp_path)
    project = discover_project(project_dir, profiles_dir=profiles)
    config = load_config(project_dir)
    config.source = SourceSettings(connector="fake", credential_resolver="none")
    store, state = _store(project_dir)
    request = _request()
    parent = SourceMapping(request.source, request.relation, request.relation)
    child = replace(
        parent,
        source=replace(parent.source, unique_id="source.analytics.app.items", table_name="items"),
    )
    execution = replace(plan(project_dir), source_mappings=(parent, child))

    def prepare(options=None):
        return prepare_sources(
            project=project,
            config=config,
            workspace=Workspace(project_dir),
            state=state,
            plan=execution,
            options=options or CommandOptions(command="sync"),
        )

    return config, store, state, prepare, project, execution


@pytest.mark.parametrize(
    "options",
    [
        CommandOptions(command="sync", data_profile="missing"),
        CommandOptions(command="sync", dataset_id="dataset", snapshot_id="snapshot"),
        CommandOptions(command="sync", dataset_id="dataset", source_mode=SourceMode.REFRESH),
    ],
)
def test_source_planning_rejects_conflicting_or_missing_input_selection(source_plan, options):
    with pytest.raises(ConfigError):
        source_plan[3](options)


def test_source_routes_validate_ambiguity_projection_and_file_mapping(source_plan):
    config, _, _, prepare, project, _ = source_plan
    config.connections["one"] = config.source
    config.routes = [SourceRoute(select="source:app.orders", connection="one", projection=["id"])]
    assert prepare().sources[0].decision.request.projection == ("id",)
    config.routes *= 2
    with pytest.raises(ConfigError, match="Ambiguous source routes"):
        prepare()
    config.routes = []
    config.source = SourceSettings(
        connector="parquet", credential_resolver="none", plugin={"tables": []}
    )
    with pytest.raises(ConfigError, match="must map"):
        prepare()
    with pytest.raises(ConfigError, match="Cannot identify"):
        connection_scope("bad", SourceSettings(profile="missing"), project, config)


def rule(**kwargs):
    return CohortRule(
        select="source:app.items",
        parent="source:app.orders",
        parent_key="id",
        key="order_id",
        **kwargs,
    )


def test_cohort_recipes_reject_ambiguity_missing_parent_and_cycles(source_plan):
    config, _, _, prepare, _, _ = source_plan
    profile = config.data_profiles["developer"]
    profile.cohorts = [rule(), rule()]
    with pytest.raises(ConfigError, match="Ambiguous cohort"):
        prepare()
    profile.cohorts = [rule().model_copy(update={"parent": "missing"})]
    with pytest.raises(ConfigError, match="exactly one"):
        prepare()
    profile.cohorts = [
        rule(),
        rule().model_copy(update={"select": "source:app.orders", "parent": "source:app.items"}),
    ]
    with pytest.raises(ConfigError, match="cycle"):
        prepare()


def test_frozen_dataset_must_contain_every_selected_source(source_plan):
    _, _, state, prepare, _, _ = source_plan
    state.commit_dataset("empty", {"snapshots": {}})
    with pytest.raises(ConfigError, match="does not contain"):
        prepare(CommandOptions(command="sync", dataset_id="empty"))


@pytest.mark.parametrize("kind", ["missing_key", "too_many", "empty"])
def test_cohort_capture_enforces_distinct_key_contract(source_plan, kind):
    config, store, _, prepare, _, _ = source_plan
    config.data_profiles["developer"].cohorts = [rule(max_keys=1)]
    sources = prepare().sources
    batch = pa.record_batch(
        {
            "wrong" if kind == "missing_key" else "id": pa.array(
                [] if kind == "empty" else [1, 2], type=pa.int64()
            )
        }
    )
    parent = store.write(
        sources[0].decision.request,
        provider="fake",
        batches=[ExtractionBatch(batch)],
        cancellation=CancellationToken(),
        invocation_id="parent",
    )
    if kind == "empty":
        prepared = materialize_cohort(sources[1], parent, config)
        assert "FALSE" in prepared.decision.request.sampling.where
        assert materialize_cohort(prepared, parent, config) == prepared
        assert materialize_cohort(sources[0], parent, config) == sources[0]
    else:
        with pytest.raises(
            PolicyError, match="cohort key" if kind == "missing_key" else "distinct-key"
        ):
            materialize_cohort(sources[1], parent, config)


def test_cached_rows_are_checked_against_current_budget(source_plan, monkeypatch):
    config, _, _, prepare, _, _ = source_plan
    source = prepare().sources[0]
    snapshot = source_plan[1].write(
        source.decision.request,
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1, 2]}))],
        cancellation=CancellationToken(),
        invocation_id="rows",
    )
    from dbtv.snapshot.store import ParquetSnapshotStore

    monkeypatch.setattr(
        ParquetSnapshotStore,
        "decide",
        lambda self, request, **kw: replace(
            source.decision,
            request=request,
            action=SnapshotAction.REUSE,
            snapshot=replace(snapshot, row_count=5_000_001),
        ),
    )
    with pytest.raises(PolicyError, match="Cached rows"):
        prepare()


@pytest.mark.parametrize("mode", [SourceMode.OFFLINE, SourceMode.AUTO])
def test_tag_age_policy_rechecks_reused_snapshots(source_plan, monkeypatch, mode):
    config, store, state, _, project, execution = source_plan
    execution = replace(
        execution,
        source_mappings=tuple(
            replace(m, tags=("private", "other")) for m in execution.source_mappings
        ),
    )
    config.policy.max_cache_age_for_tags = {"private": "1h"}
    source = source_plan[3]().sources[0]
    snapshot = store.write(
        source.decision.request,
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="old",
    )
    from dbtv.snapshot.store import ParquetSnapshotStore

    stale = replace(snapshot, completed_at=(datetime.now(UTC) - timedelta(days=1)).isoformat())
    monkeypatch.setattr(
        ParquetSnapshotStore,
        "decide",
        lambda self, request, **kw: replace(
            source.decision, request=request, action=SnapshotAction.REUSE, snapshot=stale
        ),
    )
    prepared = prepare_sources(
        project=project,
        config=config,
        workspace=Workspace(project.root),
        state=state,
        plan=execution,
        options=CommandOptions(command="sync", source_mode=mode),
    )
    expected = SnapshotAction.MISSING if mode is SourceMode.OFFLINE else SnapshotAction.REFRESH
    assert all(s.decision.action is expected for s in prepared.sources)
    assert prepared.store.ttl == timedelta(hours=1)
    config.data_profiles["developer"].cohorts = [rule()]
    config.policy.require_explicit_where_for_tags = ["private"]
    with pytest.raises(PolicyError):
        prepare_sources(
            project=project,
            config=config,
            workspace=Workspace(project.root),
            state=state,
            plan=replace(
                execution,
                source_mappings=(
                    replace(execution.source_mappings[0], tags=()),
                    execution.source_mappings[1],
                ),
            ),
            options=CommandOptions(command="sync"),
        )


@pytest.mark.parametrize(
    "value,expected", [(True, "TRUE"), (False, "FALSE"), (1.5, "1.5"), (Decimal("2.50"), "2.50")]
)
def test_scalar_cohort_keys_preserve_values(value, expected):
    assert _cohort_literal(value) == expected


@pytest.mark.parametrize("value", [float("inf"), float("nan"), {}, "line\nbreak"])
def test_invalid_cohort_keys_are_rejected(value):
    with pytest.raises(PolicyError):
        _cohort_literal(value)
