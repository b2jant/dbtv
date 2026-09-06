from __future__ import annotations

from dataclasses import replace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dbtv.config.schema import ExtractionSettings, SourceSessionSettings
from dbtv.connectors.parquet import ParquetConnector
from dbtv.connectors.registry import ConnectorRegistry
from dbtv.connectors.snowflake.connector import SnowflakeConnectorFactory
from dbtv.connectors.snowflake.query import render_select
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CancellationError, ConfigError, ExtractionError, SourceConnectionError
from dbtv.core.models import (
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotRequest,
    SourceRef,
)
from dbtv.core.sql import quote_identifier
from dbtv.credentials.dbt_profile import ResolvedCredentialHandle


def request(strategy=SamplingStrategy.FULL, **kwargs):
    return SnapshotRequest(
        SourceRef("source.p.app.data", "p", "app", "data"),
        Relation("RAW", "APP", "DATA"),
        SamplingSpec(strategy, **kwargs),
        FidelityMode.STRICT,
    )


@pytest.fixture
def snowflake(monkeypatch):
    cursor = Mock(sfqid="query-id")
    cursor.fetch_arrow_batches.return_value = [pa.table({"id": [1, 2]})]
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr("snowflake.connector.connect", Mock(return_value=connection))
    connector = SnowflakeConnectorFactory().create(
        credentials=ResolvedCredentialHandle("snowflake", {"account": "fixture", "user": "reader"}),
        session=SourceSessionSettings(),
        extraction=ExtractionSettings(max_retries=2, retry_base_seconds=0.001),
    )
    yield connector, cursor, connection
    connector.close()


def test_snowflake_schema_lifecycle_and_empty_estimate(snowflake) -> None:
    connector, cursor, connection = snowflake
    cursor.fetch_arrow_all.return_value = pa.table({"id": [1]})
    cursor.fetchone.return_value = None
    with connector:
        assert connector.capabilities() == SnowflakeConnectorFactory().capabilities()
        assert connector.version()
        assert connector.source_version(request().relation) is None
        schema = connector.inspect_schema(request().relation)
        assert schema.fields[0].name == "id"
        assert schema.fields[0].arrow_type == "int64"
        assert connector.estimate(request()).row_count == 0
    connection.close.assert_called_once()
    connector.close()


def test_snowflake_connection_and_estimate_errors(snowflake, monkeypatch) -> None:
    connector, cursor, _ = snowflake
    cursor.execute.side_effect = ValueError("invalid query")
    with pytest.raises(ExtractionError, match="estimate"):
        connector.estimate(request())
    cursor.close.assert_called_once()
    connector.close()
    monkeypatch.setattr("snowflake.connector.connect", Mock(side_effect=OSError("network")))
    with pytest.raises(SourceConnectionError, match="establish"):
        connector.open()


def test_snowflake_retries_transient_failure_and_closes_cursor(snowflake) -> None:
    connector, cursor, _ = snowflake
    transient = type("OperationalError", (Exception,), {})
    cursor.execute.side_effect = [transient(), None]
    assert sum(b.data.num_rows for b in connector.extract(request(), CancellationToken())) == 2
    assert cursor.execute.call_count == 2
    cursor.close.assert_called_once()
    assert connector._active_cursor is None


@pytest.mark.parametrize("retryable,attempts", [(False, 1), (True, 3)])
def test_snowflake_retry_limits_and_semantic_failure(snowflake, retryable, attempts) -> None:
    connector, cursor, _ = snowflake
    error = type("OperationalError" if retryable else "ProgrammingError", (Exception,), {})
    cursor.execute.side_effect = error("failed")
    with pytest.raises(ExtractionError, match="query failed"):
        list(connector.extract(request(), CancellationToken()))
    assert cursor.execute.call_count == attempts
    cursor.close.assert_called_once()


@pytest.mark.parametrize("query_id", [None, "query-id"])
def test_snowflake_cancellation_remains_cancellation(snowflake, query_id) -> None:
    connector, cursor, _ = snowflake
    token = CancellationToken()
    cursor.sfqid = query_id
    cursor.execute.side_effect = lambda *args, **kwargs: token.cancel()
    with pytest.raises(CancellationError):
        list(connector.extract(request(), token))
    assert cursor.abort_query.call_count == bool(query_id)
    cursor.close.assert_called_once()
    connector.cancel("query-id")


def test_snowflake_transfer_failure_does_not_retry_query(snowflake) -> None:
    connector, cursor, _ = snowflake
    cursor.fetch_arrow_batches.side_effect = OSError("transfer interrupted")
    with pytest.raises(ExtractionError, match="Arrow transfer"):
        list(connector.extract(request(), CancellationToken()))
    cursor.execute.assert_called_once()
    cursor.close.assert_called_once()


@pytest.mark.parametrize(
    "sampling",
    [
        SamplingSpec(SamplingStrategy.HASH),
        SamplingSpec(SamplingStrategy.BERNOULLI),
        SamplingSpec(SamplingStrategy.LIMIT),
    ],
)
def test_snowflake_rejects_incomplete_sampling(sampling) -> None:
    with pytest.raises(ConfigError):
        render_select(replace(request(), sampling=sampling))


@pytest.mark.parametrize("seed", [None, 42])
def test_snowflake_bernoulli_projection(seed) -> None:
    query = render_select(
        replace(request(SamplingStrategy.BERNOULLI, rate=0.25, seed=seed), projection=("id",))
    )
    assert query.startswith('SELECT "id" FROM "RAW"."APP"."DATA" SAMPLE BERNOULLI (25)')
    assert ("SEED (42)" in query) == (seed is not None)
    with pytest.raises(ConfigError):
        quote_identifier("")
    with pytest.raises(ConfigError):
        ConnectorRegistry().capabilities("not-installed")


@pytest.fixture
def parquet(tmp_path):
    pq.write_table(pa.table({"id": [1, 2], "value": ["a", "b"]}), tmp_path / "data.parquet")
    connector = ParquetConnector(
        extraction=ExtractionSettings(),
        plugin_config={"tables": {"RAW.APP.DATA": str(tmp_path)}},
    )
    connector.open()
    yield connector
    connector.close()


def test_parquet_projection_hash_schema_and_estimates(parquet) -> None:
    assert not parquet.capabilities().remote_access
    assert parquet.version()
    assert len(parquet.inspect_schema(request().relation).fields) == 2
    selected = replace(request(SamplingStrategy.HASH, key="id", rate=1, seed=4), projection=("id",))
    assert parquet.estimate(selected).row_count == 2
    batches = list(parquet.extract(selected, CancellationToken()))
    assert batches[0].data.to_pydict() == {"id": [1, 2]}
    parquet.cancel("unused")
    parquet.close()
    parquet.close()
    parquet.cancel("unused")


@pytest.mark.parametrize("kind", ["missing", "empty_directory", "wrong_suffix"])
def test_parquet_requires_existing_parquet_input(tmp_path, parquet, kind) -> None:
    if kind == "missing":
        parquet.tables.clear()
    else:
        path = tmp_path / "input"
        if kind == "empty_directory":
            path.mkdir()
        else:
            path.write_text("not parquet")
        parquet.tables["RAW.APP.DATA"] = str(path)
    with pytest.raises(ConfigError):
        parquet.inspect_schema(request().relation)


@pytest.mark.parametrize("strategy", [SamplingStrategy.HASH, SamplingStrategy.BERNOULLI])
def test_parquet_rejects_unsupported_sampling(parquet, strategy) -> None:
    with pytest.raises(ConfigError):
        list(parquet.extract(request(strategy), CancellationToken()))


def test_parquet_empty_input_preserves_schema_and_bad_sql_is_extraction_error(parquet) -> None:
    batches = list(
        parquet.extract(request(SamplingStrategy.WHERE, where="id < 0"), CancellationToken())
    )
    assert len(batches) == 1
    assert batches[0].data.num_rows == 0
    assert batches[0].data.schema.names == ["id", "value"]
    with pytest.raises(ExtractionError):
        list(
            parquet.extract(
                request(SamplingStrategy.WHERE, where="missing = 1"), CancellationToken()
            )
        )
