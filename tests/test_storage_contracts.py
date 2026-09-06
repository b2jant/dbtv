from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest
from test_snapshot_store import _request, _store

from dbtv.backend.duckdb import DuckDbExecutionBackend, quote_duckdb_identifier
from dbtv.compatibility import CompatibilityAnalyzer
from dbtv.config.schema import CompatibilitySettings, DataProfile, PolicySettings
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import BindingError, CacheError, ExtractionError, PolicyError
from dbtv.core.hashing import sha256_value
from dbtv.core.models import (
    ExtractionBatch,
    FidelityMode,
    SamplingSpec,
    SamplingStrategy,
    SnapshotAction,
    SourceBinding,
    SourceMapping,
    SourceVersion,
)
from dbtv.project.manifest import normalize_manifest
from dbtv.snapshot.policy import LocalPolicyEngine, apply_max_rows, resolve_sampling
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.state import StateIndex
from dbtv.workspace import Workspace


@pytest.fixture
def cache(tmp_path):
    store, state = _store(tmp_path)

    def write(value=1, **kwargs):
        return store.write(
            _request(),
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": [value]}))],
            cancellation=CancellationToken(),
            invocation_id="test",
            **kwargs,
        )

    return store, state, write


def test_sampling_rule_precedence_and_runtime_caps():
    profile = DataProfile.model_validate(
        {
            "default": {"strategy": "full"},
            "sources": [
                {"select": "source:other.*", "strategy": "limit", "limit": 1},
                {"select": "source:app.orders", "strategy": "limit", "limit": 8},
                {"select": "source:analytics.app.orders", "strategy": "limit", "limit": 5},
            ],
        }
    )
    assert resolve_sampling(profile, _request().source).limit == 5
    assert apply_max_rows(SamplingSpec(SamplingStrategy.FULL), 4).limit == 4
    assert (
        apply_max_rows(SamplingSpec(SamplingStrategy.WHERE, where="true"), 4).strategy
        is SamplingStrategy.WHERE_LIMIT
    )
    assert apply_max_rows(SamplingSpec(SamplingStrategy.LIMIT, limit=2), 4).limit == 2
    sample = SamplingSpec(SamplingStrategy.HASH, key="id", rate=0.1)
    assert apply_max_rows(sample, 4) == sample
    engine = LocalPolicyEngine(PolicySettings(require_fidelity="strict", max_rows_per_source=5))
    with pytest.raises(PolicyError, match="fidelity"):
        engine.evaluate_sampling(_request().source, sample, fidelity=FidelityMode.WARN)
    with pytest.raises(PolicyError, match="cap"):
        engine.evaluate_sampling(_request().source, SamplingSpec(SamplingStrategy.LIMIT, limit=6))


def test_unsupported_materializations_honor_warn_and_deny_overrides():
    from test_project_contracts import manifest_raw, node

    manifest = normalize_manifest(
        manifest_raw({"model.p.a": node(config={"materialized": "dynamic_table"})})
    )
    for setting, severity in [
        (CompatibilitySettings(warn_rules=["DBTV-MAT-001"]), "warning"),
        (CompatibilitySettings(mode="warn", deny_rules=["DBTV-MAT-001"]), "error"),
    ]:
        finding = CompatibilityAnalyzer(setting).analyze_manifest(manifest, ("model.p.a",))[0]
        assert finding.severity == severity


def test_cache_lookup_activation_and_explicit_snapshot_validation(cache):
    store, state, write = cache
    assert store.lookup("missing") is None
    assert (
        store.decide(_request(), provider="fake", mode="offline", snapshot_id="missing").action
        is SnapshotAction.MISSING
    )
    snapshot = write(activate=False)
    assert store.lookup(snapshot.snapshot_key).row_count == 1
    store.activate(snapshot)
    different = replace(_request(), projection=("id",))
    assert (
        store.decide(
            different, provider="fake", mode="offline", snapshot_id=snapshot.snapshot_key
        ).reason
        == "snapshot request differs"
    )
    assert not store._expired(replace(snapshot, expires_at=None))
    state.mark_corrupt(snapshot.snapshot_key)
    assert store.lookup(snapshot.snapshot_key) is None


def test_empty_and_multiple_batches_preserve_schema_and_projection(cache):
    store, _, _ = cache
    with pytest.raises(ExtractionError, match="no Arrow schema"):
        store.write(
            _request(),
            provider="fake",
            batches=[],
            cancellation=CancellationToken(),
            invocation_id="empty",
        )
    store.compression = "none"
    batch = pa.record_batch({"id": pa.array([], type=pa.int64())})
    snapshot = store.write(
        replace(_request(), projection=("id",)),
        provider="fake",
        batches=[ExtractionBatch(batch), ExtractionBatch(batch)],
        cancellation=CancellationToken(),
        invocation_id="empty-typed",
        source_version=SourceVersion("v1", "today"),
    )
    restored = store.lookup(snapshot.snapshot_key)
    assert restored.row_count == 0 and restored.projection == ("id",)
    assert restored.source_version.value == "v1"


@pytest.mark.parametrize("kind", ["incomplete", "unsafe", "missing", "checksum"])
def test_cache_integrity_rejects_incomplete_unsafe_and_corrupt_data(cache, kind):
    store, _, write = cache
    store.integrity = "checksums"
    snapshot = write()
    if kind == "incomplete":
        (snapshot.root / "_SUCCESS").unlink()
    elif kind == "unsafe":
        snapshot = replace(snapshot, files=(replace(snapshot.files[0], path="../outside"),))
    elif kind == "missing":
        snapshot.parquet_paths[0].unlink()
    else:
        snapshot = replace(snapshot, files=(replace(snapshot.files[0], checksum="bad"),))
    with pytest.raises(CacheError):
        store.validate(snapshot)


@pytest.mark.parametrize("kind", ["identity", "timezone", "json"])
def test_cache_rejects_invalid_observation_receipts(cache, kind):
    store, _, write = cache
    snapshot = write()
    receipt = {
        "snapshot_key": snapshot.snapshot_key,
        "verified_at": "2026-01-01T00:00:00+00:00",
        "expires_at": "2026-01-02T00:00:00+00:00",
    }
    if kind == "identity":
        receipt["snapshot_key"] = "wrong"
    if kind == "timezone":
        receipt["verified_at"] = "2026-01-01T00:00:00"
    (snapshot.root / "observation.json").write_text("{" if kind == "json" else json.dumps(receipt))
    with pytest.raises(CacheError, match="metadata"):
        store.load(snapshot.root / "metadata.json")


def test_identical_refresh_can_pin_without_activation_and_rejects_corrupt_existing(cache):
    store, state, write = cache
    first = write(activate=False)
    pinned = write(pinned=True, activate=False)
    assert pinned.pinned and pinned.snapshot_key == first.snapshot_key
    assert not state.get_snapshot(first.snapshot_key).active
    assert write().snapshot_key == first.snapshot_key
    (first.root / "metadata.json").write_text("{")
    with pytest.raises(CacheError, match="pin"):
        store.pin(first)
    with pytest.raises(CacheError):
        write()
    assert not list(store.tmp_root.iterdir())


def test_commit_index_failure_leaves_recoverable_files(cache, monkeypatch):
    store, _, write = cache
    with monkeypatch.context() as patch:
        patch.setattr(
            store.state, "record_snapshot", Mock(side_effect=OSError("index unavailable"))
        )
        with pytest.raises(CacheError, match="commit"):
            write()
    assert not list(store.tmp_root.iterdir())
    assert store.rebuild_index()["snapshots_recovered"] == 1


def test_garbage_collection_respects_age_history_and_unsafe_index_entries(cache, tmp_path):
    store, state, write = cache
    first = write(1)
    second = write(2)
    assert store.garbage_collect()["snapshots_removed"] == 0
    store.retain_previous = 0
    assert (
        store.garbage_collect(older_than=datetime.now(UTC) - timedelta(days=1))["snapshots_removed"]
        == 0
    )
    report = store.garbage_collect(older_than=datetime.now(UTC))
    assert report["snapshots_removed"] == 1 and report["bytes_removed"] == first.byte_count
    assert not first.root.exists() and second.root.exists()
    state.record_snapshot(first, tmp_path / "outside" / "metadata.json")
    assert store.garbage_collect()["snapshots_removed"] == 0


def test_rebuild_skips_corrupt_snapshots_and_invalid_datasets(cache):
    store, state, write = cache
    first = write()
    (first.root / "_SUCCESS").unlink()
    directory = state.path.parent / "datasets"
    directory.mkdir()
    (directory / "bad.json").write_text("{")
    identity = {
        "format_version": 1,
        "snapshots": {"missing": "sha256:missing"},
        "source_scopes": {},
    }
    (directory / "missing.json").write_text(
        json.dumps({**identity, "dataset_id": sha256_value(identity)})
    )
    (directory / "wrong.json").write_text(json.dumps({**identity, "dataset_id": "wrong"}))
    assert store.rebuild_index() == {"snapshots_recovered": 0, "corrupt_snapshots": 1}
    assert not state.list_datasets()


def test_unsafe_snapshot_root_and_legacy_index_migration(tmp_path):
    state = StateIndex(tmp_path / "state.sqlite", tmp_path / "locks")
    with pytest.raises(CacheError, match="unsafe snapshot root"):
        ParquetSnapshotStore(
            Path("/"), state=state, locks_dir=state.locks_dir, ttl=timedelta(hours=1)
        )
    with sqlite3.connect(state.path) as connection:
        connection.execute(
            "CREATE TABLE snapshots (snapshot_key TEXT, source_unique_id TEXT, active INTEGER)"
        )
        connection.execute("CREATE TABLE runs (invocation_id TEXT)")
    state.initialize()
    with sqlite3.connect(state.path) as connection:
        assert {r[1] for r in connection.execute("PRAGMA table_info(snapshots)")} >= {
            "pinned",
            "request_fingerprint",
        }
        assert "exit_code" in {r[1] for r in connection.execute("PRAGMA table_info(runs)")}


@pytest.fixture
def binding(cache, tmp_path):
    _, _, write = cache
    snapshot = write()
    relation = replace(snapshot.remote_relation, catalog=None)
    value = SourceBinding(
        SourceMapping(snapshot.source, snapshot.remote_relation, relation), snapshot
    )
    backend = DuckDbExecutionBackend(
        tmp_path / "local.duckdb", Workspace(tmp_path), install_compatibility_shims=False
    )
    return backend, value


def test_local_catalog_binding_and_inspection_boundaries(binding):
    backend, value = binding
    report = backend.bind([value], "bind")
    assert not report.attachments
    assert backend.verify(report, [value]).results[0].verified
    assert backend.check_prerequisites([{"relation": None}], {}) == [
        {"relation": None, "available": False}
    ]
    assert backend.inspect("select * from APP.ORDERS", allowed_paths=value.snapshot.parquet_paths)[
        1
    ] == ((1,),)
    assert backend.inspect("pragma database_size")[0]
    (backend.workspace.catalogs / "catalog-map.json").write_text(
        json.dumps({"outside": "/not/allowed.duckdb"})
    )
    assert backend.inspect("select 1")[1] == ((1,),)
    with pytest.raises(BindingError, match="inspection failed"):
        backend.inspect("select missing from nonexistent")
    (backend.workspace.catalogs / "catalog-map.json").write_text(
        json.dumps({"": str(backend.workspace.catalogs / "broken.duckdb")})
    )
    with pytest.raises(BindingError, match="invalid identifier"):
        backend.inspect("select 1")


@pytest.mark.parametrize("kind", ["empty", "missing_file", "identifier"])
def test_binding_failure_is_actionable_and_releases_database(binding, kind):
    backend, value = binding
    if kind == "empty":
        value = replace(value, snapshot=replace(value.snapshot, files=()))
    elif kind == "missing_file":
        value.snapshot.parquet_paths[0].unlink()
    else:
        value = replace(
            value,
            mapping=replace(
                value.mapping, local_relation=replace(value.mapping.local_relation, schema="")
            ),
        )
    with pytest.raises(BindingError):
        backend.bind([value], "bad-binding")
    import duckdb

    duckdb.connect(str(backend.database)).close()


def test_verification_reports_missing_relation_and_preserves_domain_errors(binding):
    backend, value = binding
    report = backend.bind([value], "bind")
    missing = replace(
        value,
        mapping=replace(
            value.mapping,
            local_relation=replace(value.mapping.local_relation, identifier="missing"),
        ),
    )
    with pytest.raises(BindingError, match="verification failed"):
        backend.verify(report, [missing])
    bad = replace(
        value,
        mapping=replace(
            value.mapping, local_relation=replace(value.mapping.local_relation, identifier="\x00")
        ),
    )
    with pytest.raises(BindingError, match="invalid identifier"):
        backend.verify(report, [bad])
    with pytest.raises(BindingError):
        quote_duckdb_identifier("")


def test_missing_current_catalog_closes_connection(binding, monkeypatch):
    backend, _ = binding
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = None
    monkeypatch.setattr("duckdb.connect", lambda *a, **kw: connection)
    with pytest.raises(BindingError, match="current catalog"):
        backend.bind([], "missing-catalog")
    connection.close.assert_called_once()
