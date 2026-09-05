"""Local Parquet input; the provider boundary remains streamed Arrow batches."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dbtv.backend.duckdb import quote_duckdb_identifier as quote_identifier
from dbtv.config.schema import ExtractionSettings
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import ConfigError, ExtractionError
from dbtv.core.hashing import sha256_value
from dbtv.core.models import (
    CanonicalSchema,
    ExtractionBatch,
    ExtractionEstimate,
    Relation,
    SamplingStrategy,
    SnapshotRequest,
    SourceCapabilities,
    SourceVersion,
)
from dbtv.core.sql import validate_predicate
from dbtv.snapshot.store import _canonical_schema


class ParquetConnectorFactory:
    def create(self, **kwargs: Any) -> ParquetConnector:
        return ParquetConnector(**kwargs)

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            True, True, True, True, False, True, True, False, False, True, remote_access=False
        )

    def dependencies(self) -> tuple[str, ...]:
        return ("duckdb", "pyarrow")

    def dbt_adapter_name(self) -> str | None:
        return None


class ParquetConnector:
    def __init__(
        self, *, extraction: ExtractionSettings, plugin_config: dict[str, Any], **kwargs: Any
    ) -> None:
        self.extraction = extraction
        self.tables = plugin_config.get("tables", {})
        self.connection: Any = None

    def capabilities(self) -> SourceCapabilities:
        return ParquetConnectorFactory().capabilities()

    def version(self) -> str:
        import duckdb

        return str(duckdb.__version__)

    def open(self) -> None:
        import duckdb

        self.connection = duckdb.connect(config={"threads": 1})

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def _files(self, relation: Relation) -> list[str]:
        configured = self.tables.get(relation.display_name)
        if configured is None:
            raise ConfigError(f"Parquet plugin.tables has no path for {relation.display_name}.")
        path = Path(str(configured)).expanduser().resolve()
        files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
        if not files or any(not item.is_file() or item.suffix != ".parquet" for item in files):
            raise ConfigError(
                f"Expected a local Parquet file or directory for {relation.display_name}."
            )
        return [str(item) for item in files]

    def inspect_schema(self, relation: Relation) -> CanonicalSchema:
        import pyarrow.parquet as pq  # type: ignore[import-untyped]

        return _canonical_schema(pq.read_schema(self._files(relation)[0]))

    def source_version(self, relation: Relation) -> SourceVersion:
        entries = []
        for value in self._files(relation):
            stat = Path(value).stat()
            entries.append((value, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns))
        return SourceVersion(sha256_value(entries), datetime.now(UTC).isoformat())

    def _query(self, request: SnapshotRequest) -> tuple[str, list[str]]:
        files = self._files(request.relation)
        projection = (
            ", ".join(map(quote_identifier, request.projection)) if request.projection else "*"
        )
        query = f"SELECT {projection} FROM read_parquet(?, union_by_name=false)"
        sampling = request.sampling
        if sampling.where:
            query += f" WHERE {validate_predicate(sampling.where)}"
        elif sampling.strategy is SamplingStrategy.HASH:
            if sampling.key is None or sampling.rate is None:
                raise ConfigError("Hash sampling requires a key and rate.")
            query += (
                f" WHERE hash({quote_identifier(sampling.key)}, {sampling.seed or 0})"
                f" % 1000000 < {int(sampling.rate * 1_000_000)}"
            )
        if sampling.strategy is SamplingStrategy.BERNOULLI:
            raise ConfigError("Parquet connector does not support Bernoulli sampling.")
        if sampling.limit is not None:
            query += f" LIMIT {int(sampling.limit)}"
        return query, files

    def estimate(self, request: SnapshotRequest) -> ExtractionEstimate:
        query, files = self._query(request)
        row = self.connection.execute(f"SELECT count(*) FROM ({query})", [files]).fetchone()
        return ExtractionEstimate(row_count=int(row[0]), byte_count=None)

    def extract(
        self, request: SnapshotRequest, cancellation: CancellationToken
    ) -> Iterable[ExtractionBatch]:
        import pyarrow as pa

        query, files = self._query(request)
        unregister = cancellation.register(self.connection.interrupt)
        try:
            cursor = self.connection.execute(query, [files])
            arrow_reader = getattr(cursor, "to_arrow_reader", cursor.fetch_record_batch)
            reader = arrow_reader(self.extraction.arrow_batch_rows)
            emitted = False
            for batch in reader:
                cancellation.raise_if_cancelled()
                emitted = True
                yield ExtractionBatch(batch)
            if not emitted:
                yield ExtractionBatch(
                    pa.RecordBatch.from_arrays(
                        [pa.array([], type=field.type) for field in reader.schema],
                        schema=reader.schema,
                    )
                )
        except Exception as exc:
            cancellation.raise_if_cancelled()
            raise ExtractionError(
                f"Parquet extraction failed for {request.source.unique_id}."
            ) from exc
        finally:
            unregister()

    def cancel(self, query_id: str) -> None:
        if self.connection is not None:
            self.connection.interrupt()
