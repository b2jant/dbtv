from __future__ import annotations

from typing import Any

import pyarrow as pa
import snowflake.connector

from dbtv.config.schema import ExtractionSettings, SourceSessionSettings
from dbtv.connectors.snowflake import SnowflakeConnector
from dbtv.core.cancellation import CancellationToken
from dbtv.core.models import (
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotRequest,
    SourceRef,
)
from dbtv.credentials.dbt_profile import ResolvedCredentialHandle


class FakeCursor:
    sfqid = "snowflake-query-id"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.aborted: list[str] = []

    def execute(self, query: str, **kwargs: Any) -> None:
        self.calls.append((query, kwargs))

    def fetch_arrow_batches(self) -> list[Any]:
        return [pa.table({"id": list(range(2_000))})]

    def fetchone(self) -> tuple[int]:
        return (17,)

    def abort_query(self, query_id: str) -> None:
        self.aborted.append(query_id)

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_snowflake_connector_streams_bounded_arrow_and_tags_statement(
    monkeypatch: Any,
) -> None:
    connection = FakeConnection()
    connect_parameters: dict[str, Any] = {}

    def connect(**kwargs: Any) -> FakeConnection:
        connect_parameters.update(kwargs)
        return connection

    monkeypatch.setattr(snowflake.connector, "connect", connect)
    connector = SnowflakeConnector(
        ResolvedCredentialHandle(
            "snowflake",
            {"account": "acme", "user": "developer", "password": "canary"},
        ),
        session=SourceSessionSettings(statement_timeout_seconds=30),
        extraction=ExtractionSettings(arrow_batch_rows=1_000, max_retries=0),
    )
    request = SnapshotRequest(
        SourceRef("source.analytics.app.orders", "analytics", "app", "orders"),
        Relation("RAW", "APP", "ORDERS"),
        SamplingSpec(SamplingStrategy.LIMIT, limit=2),
        FidelityMode.STRICT,
        query_tag="dbtv/invocation/source",
    )
    batches = list(connector.extract(request, CancellationToken()))
    connector.close()
    assert [batch.data.num_rows for batch in batches] == [1_000, 1_000]
    query, kwargs = connection.cursor_instance.calls[0]
    assert query.endswith("LIMIT 2")
    assert kwargs["timeout"] == 30
    assert kwargs["_statement_params"] == {"QUERY_TAG": request.query_tag}
    assert connect_parameters["application"] == "dbtv"
    assert connect_parameters["session_parameters"]["STATEMENT_TIMEOUT_IN_SECONDS"] == 30
    assert connection.closed


def test_snowflake_estimate_counts_the_bounded_working_set(monkeypatch: Any) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(snowflake.connector, "connect", lambda **_: connection)
    connector = SnowflakeConnector(
        ResolvedCredentialHandle("snowflake", {"account": "acme", "user": "developer"}),
        session=SourceSessionSettings(statement_timeout_seconds=30),
        extraction=ExtractionSettings(max_retries=0),
    )
    request = SnapshotRequest(
        SourceRef("source.analytics.app.orders", "analytics", "app", "orders"),
        Relation("RAW", "APP", "ORDERS"),
        SamplingSpec(SamplingStrategy.LIMIT, limit=100),
        FidelityMode.STRICT,
        query_tag="dbtv/plan/source",
    )

    estimate = connector.estimate(request)
    connector.close()

    assert estimate is not None and estimate.row_count == 17
    query, kwargs = connection.cursor_instance.calls[0]
    assert query == (
        'SELECT COUNT(*) FROM (SELECT * FROM "RAW"."APP"."ORDERS" LIMIT 100) AS DBTV_ESTIMATE'
    )
    assert kwargs["_statement_params"] == {"QUERY_TAG": request.query_tag}
