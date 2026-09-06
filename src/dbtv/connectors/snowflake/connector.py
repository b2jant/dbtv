from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from dataclasses import asdict
from importlib.metadata import version
from typing import Any

from dbtv.config.schema import ExtractionSettings, SourceSessionSettings
from dbtv.connectors.snowflake.query import render_relation, render_select
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CancellationError, ExtractionError, SourceConnectionError
from dbtv.core.hashing import sha256_value
from dbtv.core.models import (
    CanonicalField,
    CanonicalSchema,
    ExtractionBatch,
    ExtractionEstimate,
    Relation,
    SnapshotRequest,
    SourceCapabilities,
    SourceVersion,
)
from dbtv.credentials.dbt_profile import ResolvedCredentialHandle


class SnowflakeConnectorFactory:
    def dependencies(self) -> tuple[str, ...]:
        return ("snowflake-connector-python",)

    def dbt_adapter_name(self) -> str:
        return "snowflake"

    def capabilities(self) -> SourceCapabilities:
        return SnowflakeConnector.capability_set()

    def create(
        self,
        *,
        credentials: ResolvedCredentialHandle,
        session: SourceSessionSettings,
        extraction: ExtractionSettings,
        **_: Any,
    ) -> SnowflakeConnector:
        return SnowflakeConnector(credentials, session=session, extraction=extraction)


class SnowflakeConnector:
    def __init__(
        self,
        credentials: ResolvedCredentialHandle,
        *,
        session: SourceSessionSettings,
        extraction: ExtractionSettings,
    ) -> None:
        self.credentials = credentials
        self.session = session
        self.extraction = extraction
        self._connection: Any = None
        self._active_cursor: Any = None

    @staticmethod
    def capability_set() -> SourceCapabilities:
        return SourceCapabilities(
            arrow_streaming=True,
            projection_pushdown=True,
            predicate_pushdown=True,
            limit_pushdown=True,
            bernoulli_sampling=True,
            deterministic_sampling=True,
            snapshot_identity=False,
            consistent_snapshot_time=False,
            direct_duckdb_read=False,
            cancellable_queries=True,
        )

    def capabilities(self) -> SourceCapabilities:
        return self.capability_set()

    def version(self) -> str:
        return version("snowflake-connector-python")

    def open(self) -> None:
        if self._connection is not None:
            return
        try:
            import snowflake.connector

            parameters = self.credentials.parameters()
            parameters.setdefault("login_timeout", self.session.login_timeout_seconds)
            parameters.setdefault("network_timeout", self.session.network_timeout_seconds)
            parameters.setdefault("application", "dbtv")
            parameters.setdefault("validate_default_parameters", True)
            session_parameters = dict(parameters.pop("session_parameters", {}))
            session_parameters.update(
                {
                    "QUERY_TAG": self.session.query_tag_prefix,
                    "STATEMENT_TIMEOUT_IN_SECONDS": self.session.statement_timeout_seconds,
                    "TIMEZONE": self.session.timezone,
                }
            )
            parameters["session_parameters"] = session_parameters
            self._connection = snowflake.connector.connect(**parameters)
        except Exception as exc:
            raise SourceConnectionError(
                "Unable to establish the Snowflake source connection.",
                hint="Validate the dbt target, authenticator, role, warehouse, and network access.",
            ) from exc

    def close(self) -> None:
        connection, self._connection = self._connection, None
        self._active_cursor = None
        if connection is not None:
            with suppress(Exception):
                connection.close()

    def __enter__(self) -> SnowflakeConnector:
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def inspect_schema(self, relation: Relation) -> CanonicalSchema:
        self.open()
        cursor = self._connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {render_relation(relation)} LIMIT 0")
            table = cursor.fetch_arrow_all()
            return _canonical_schema(table.schema)
        finally:
            cursor.close()

    def source_version(self, relation: Relation) -> SourceVersion | None:
        return None

    def estimate(self, request: SnapshotRequest) -> ExtractionEstimate | None:
        self.open()
        cursor = self._connection.cursor()
        try:
            query = f"SELECT COUNT(*) FROM ({render_select(request)}) AS DBTV_ESTIMATE"
            statement_params = {"QUERY_TAG": request.query_tag} if request.query_tag else None
            cursor.execute(
                query,
                timeout=self.session.statement_timeout_seconds,
                _statement_params=statement_params,
            )
            row = cursor.fetchone()
            return ExtractionEstimate(row_count=int(row[0]) if row else 0)
        except Exception as exc:
            raise ExtractionError(
                f"Unable to estimate the working set for {request.source.unique_id}."
            ) from exc
        finally:
            cursor.close()

    def extract(
        self,
        request: SnapshotRequest,
        cancellation: CancellationToken,
    ) -> Iterable[ExtractionBatch]:
        self.open()
        query = render_select(request)
        cursor = self._connection.cursor()
        self._active_cursor = cursor
        query_id: str | None = None
        unregister = cancellation.register(
            lambda: self.cancel(str(cursor.sfqid)) if cursor.sfqid else None
        )
        try:
            attempt = 0
            while True:
                cancellation.raise_if_cancelled()
                try:
                    statement_params = (
                        {"QUERY_TAG": request.query_tag} if request.query_tag else None
                    )
                    cursor.execute(
                        query,
                        timeout=self.session.statement_timeout_seconds,
                        _statement_params=statement_params,
                    )
                    query_id = str(cursor.sfqid) if cursor.sfqid else None
                    break
                except Exception as exc:
                    if attempt >= self.extraction.max_retries or not _retryable(exc):
                        raise ExtractionError(
                            f"Snowflake query failed for {request.source.unique_id}.",
                            hint=(
                                "Review the read-only role, warehouse, relation, and sampling rule."
                            ),
                        ) from exc
                    cancellation.raise_if_cancelled()
                    cancellation.wait(self.extraction.retry_base_seconds * (2**attempt))
                    attempt += 1
            for table in cursor.fetch_arrow_batches():
                cancellation.raise_if_cancelled()
                for batch in table.to_batches(max_chunksize=self.extraction.arrow_batch_rows):
                    yield ExtractionBatch(batch, query_id=query_id)
        except (CancellationError, ExtractionError):
            raise
        except Exception as exc:
            raise ExtractionError(f"Arrow transfer failed for {request.source.unique_id}.") from exc
        finally:
            unregister()
            cursor.close()
            self._active_cursor = None

    def cancel(self, query_id: str) -> None:
        cursor = self._active_cursor
        if cursor is not None and query_id:
            with suppress(Exception):
                cursor.abort_query(query_id)


def _retryable(error: Exception) -> bool:
    return error.__class__.__name__ in {
        "OperationalError",
        "DatabaseError",
        "InterfaceError",
        "ServiceUnavailableError",
    }


def _canonical_schema(schema: Any) -> CanonicalSchema:
    fields = tuple(
        CanonicalField(
            name=str(field.name),
            arrow_type=str(field.type),
            nullable=bool(field.nullable),
        )
        for field in schema
    )
    return CanonicalSchema(
        fields=fields,
        fingerprint=sha256_value([asdict(field) for field in fields]),
    )
