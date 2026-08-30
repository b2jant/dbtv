from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CacheError, CancellationError, ExtractionError
from dbtv.core.models import (
    ExtractionBatch,
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotAction,
    SnapshotRequest,
    SourceRef,
)
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.state import StateIndex
from dbtv.workspace import Workspace


def _store(tmp_path: Path) -> tuple[ParquetSnapshotStore, StateIndex]:
    workspace = Workspace(tmp_path)
    workspace.ensure()
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("test")
    return (
        ParquetSnapshotStore(
            workspace.cache,
            state=state,
            locks_dir=workspace.locks,
            ttl=timedelta(hours=1),
            maximum_size=10 * 1024**2,
        ),
        state,
    )


def _request() -> SnapshotRequest:
    return SnapshotRequest(
        SourceRef("source.analytics.app.orders", "analytics", "app", "orders"),
        Relation("RAW", "APP", "ORDERS"),
        SamplingSpec(SamplingStrategy.LIMIT, limit=100),
        FidelityMode.STRICT,
    )


def test_atomic_snapshot_round_trip_and_offline_decision(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    batch = pa.record_batch({"id": [1, 2], "amount": [10.5, 20.0]})
    snapshot = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(batch, "query-1")],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )
    assert snapshot.row_count == 2
    assert snapshot.parquet_paths[0].is_file()
    assert (snapshot.root / "_SUCCESS").is_file()
    metadata = json.loads((snapshot.root / "metadata.json").read_text(encoding="utf-8"))
    assert "query-1" in json.dumps(metadata)
    assert "password" not in json.dumps(metadata).lower()
    decision = store.decide(_request(), provider="fake", mode="offline")
    assert decision.action is SnapshotAction.REUSE
    assert (
        state.active_snapshot(snapshot.source.unique_id, snapshot.request_fingerprint) is not None
    )


def test_failed_refresh_never_replaces_active_snapshot(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    batch = pa.record_batch({"id": [1]})
    active = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(batch)],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )

    def failing_batches() -> object:
        yield ExtractionBatch(batch)
        raise RuntimeError("transport dropped")

    with pytest.raises(RuntimeError, match="transport dropped"):
        store.write(
            _request(),
            provider="fake",
            batches=failing_batches(),  # type: ignore[arg-type]
            cancellation=CancellationToken(),
            invocation_id="run-2",
            activate=False,
        )
    record = state.active_snapshot(active.source.unique_id, active.request_fingerprint)
    assert record is not None
    assert record.snapshot_key == active.snapshot_key
    assert not any(store.tmp_root.iterdir())


def test_refresh_with_changed_rows_commits_a_new_content_snapshot(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    first = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )
    refreshed = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [2]}))],
        cancellation=CancellationToken(),
        invocation_id="run-2",
    )

    assert refreshed.snapshot_key != first.snapshot_key
    assert refreshed.content_fingerprint != first.content_fingerprint
    assert first.root.is_dir()
    active = state.active_snapshot(
        refreshed.source.unique_id,
        refreshed.request_fingerprint,
    )
    assert active is not None and active.snapshot_key == refreshed.snapshot_key


def test_corrupt_snapshot_is_quarantined_and_not_reused(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    snapshot = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )
    snapshot.parquet_paths[0].write_bytes(b"not parquet")
    decision = store.decide(_request(), provider="fake", mode="offline")
    assert decision.action is SnapshotAction.MISSING
    record = state.get_snapshot(snapshot.snapshot_key)
    assert record is not None
    assert record.state == "CORRUPT"
    assert not record.active


def test_cancelled_or_over_quota_snapshot_leaves_no_pending_data(tmp_path: Path) -> None:
    store, _ = _store(tmp_path)
    token = CancellationToken()
    token.cancel()
    with pytest.raises(CancellationError):
        store.write(
            _request(),
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
            cancellation=token,
            invocation_id="run-cancelled",
        )
    assert not any(store.tmp_root.iterdir())

    store.maximum_size = 1
    with pytest.raises(CacheError, match="quota"):
        store.write(
            _request(),
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": list(range(100))}))],
            cancellation=CancellationToken(),
            invocation_id="run-quota",
        )
    assert not any(store.tmp_root.iterdir())


def test_schema_drift_aborts_pending_snapshot(tmp_path: Path) -> None:
    store, _ = _store(tmp_path)

    with pytest.raises(ExtractionError, match="Schema changed"):
        store.write(
            _request(),
            provider="fake",
            batches=[
                ExtractionBatch(pa.record_batch({"id": pa.array([1], type=pa.int64())})),
                ExtractionBatch(pa.record_batch({"id": pa.array(["1"], type=pa.string())})),
            ],
            cancellation=CancellationToken(),
            invocation_id="run-schema-drift",
        )

    assert not any(store.tmp_root.iterdir())


def test_disk_write_failure_preserves_previous_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, state = _store(tmp_path)
    active = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )

    def fail_write(*_: object, **__: object) -> None:
        raise OSError("simulated disk full")

    monkeypatch.setattr(pq, "write_table", fail_write)
    with pytest.raises(CacheError, match="write or commit"):
        store.write(
            _request(),
            provider="fake",
            batches=[ExtractionBatch(pa.record_batch({"id": [2]}))],
            cancellation=CancellationToken(),
            invocation_id="run-2",
        )

    record = state.active_snapshot(active.source.unique_id, active.request_fingerprint)
    assert record is not None and record.snapshot_key == active.snapshot_key
    assert not any(store.tmp_root.iterdir())


def test_state_index_can_be_rebuilt_from_authoritative_sidecars(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    snapshot = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )
    state.clear_snapshots()
    assert state.list_snapshots() == ()
    report = store.rebuild_index()
    assert report == {"snapshots_recovered": 1, "corrupt_snapshots": 0}
    assert state.get_snapshot(snapshot.snapshot_key) is not None


def test_pinning_updates_sidecar_and_survives_index_rebuild(tmp_path: Path) -> None:
    store, state = _store(tmp_path)
    snapshot = store.write(
        _request(),
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1]}))],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )
    pinned = store.pin(snapshot)
    assert pinned.pinned
    metadata = json.loads((pinned.root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["pinned"] is True
    state.clear_snapshots()
    store.rebuild_index()
    record = state.get_snapshot(snapshot.snapshot_key)
    assert record is not None and record.pinned
