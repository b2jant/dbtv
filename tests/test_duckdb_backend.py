from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from dbtv.backend.duckdb import DuckDbExecutionBackend
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import BindingError
from dbtv.core.models import (
    ExtractionBatch,
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotRequest,
    SourceBinding,
    SourceMapping,
    SourceRef,
)
from dbtv.snapshot.store import ParquetSnapshotStore
from dbtv.state import StateIndex
from dbtv.workspace import Workspace


def test_exact_catalog_schema_view_binding(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path)
    workspace.ensure()
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("test")
    store = ParquetSnapshotStore(
        workspace.cache,
        state=state,
        locks_dir=workspace.locks,
        ttl=timedelta(hours=1),
    )
    source = SourceRef("source.analytics.app.orders", "analytics", "app", "orders")
    request = SnapshotRequest(
        source,
        Relation("RAW", "APP", "ORDERS"),
        SamplingSpec(SamplingStrategy.LIMIT, limit=10),
        FidelityMode.STRICT,
    )
    snapshot = store.write(
        request,
        provider="fake",
        batches=[ExtractionBatch(pa.record_batch({"id": [1, 2]}))],
        cancellation=CancellationToken(),
        invocation_id="run-1",
    )
    mapping = SourceMapping(
        source,
        request.relation,
        Relation("LOCAL-RAW", "Mixed Schema", "Order Items"),
    )
    binding = SourceBinding(mapping, snapshot)
    backend = DuckDbExecutionBackend(workspace.root / "local.duckdb", workspace)
    report = backend.bind([binding], "run-1")
    verified = backend.verify(report, [binding])
    assert verified.results[0].verified
    assert "LOCAL-RAW" in verified.attachments


@pytest.mark.parametrize(
    "query",
    [
        "delete from anything",
        "pragma enable_external_access=true",
        "pragma install_extension('httpfs')",
        "select * from read_parquet('/tmp/private.parquet')",
        "select install_extension('httpfs')",
        "select 1; delete from anything",
    ],
)
def test_inspect_rejects_non_read_only_queries(tmp_path: Path, query: str) -> None:
    database = tmp_path / "inspect.duckdb"
    connection = duckdb.connect(str(database))
    connection.close()
    workspace = Workspace(tmp_path / "workspace")
    workspace.ensure()
    backend = DuckDbExecutionBackend(database, workspace)

    with pytest.raises(BindingError, match="read-only"):
        backend.inspect(query)


def test_inspect_allows_local_select_with_external_access_disabled(tmp_path: Path) -> None:
    database = tmp_path / "inspect.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("create table local_values as select 42 as answer")
    connection.close()
    workspace = Workspace(tmp_path / "workspace")
    workspace.ensure()

    columns, rows = DuckDbExecutionBackend(database, workspace).inspect(
        "select answer from local_values"
    )

    assert columns == ("answer",)
    assert rows == ((42,),)
