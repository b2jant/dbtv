from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from dbtv.backend.shims import install_shims
from dbtv.core.errors import BindingError
from dbtv.core.hashing import sha256_value
from dbtv.core.locks import FileLock
from dbtv.core.models import BindingReport, BindingResult, SourceBinding
from dbtv.workspace import Workspace

_SAFE_QUERY = re.compile(r"^\s*(?:select|with|explain|describe|show)\b", re.I)
_SAFE_PRAGMA = re.compile(
    r"^\s*pragma\s+(?:database_size|storage_info|table_info|database_list|version)\b",
    re.I,
)
_UNSAFE_INSPECT = re.compile(
    r"\b(?:install|load|attach|detach|copy|export|import|"
    r"read_(?:blob|csv|csv_auto|json|json_auto|ndjson|parquet|text)|"
    r"install_extension|load_extension|httpfs|sqlite_scan|postgres_scan|mysql_scan)\b",
    re.I,
)


def quote_duckdb_identifier(value: str) -> str:
    if not value or any(ord(character) < 32 for character in value):
        raise BindingError("Local relation contains an invalid identifier.")
    return '"' + value.replace('"', '""') + '"'


def quote_duckdb_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDbExecutionBackend:
    def __init__(
        self,
        database: Path,
        workspace: Workspace,
        *,
        install_compatibility_shims: bool = True,
    ) -> None:
        self.database = database.resolve()
        self.workspace = workspace
        self.install_compatibility_shims = install_compatibility_shims

    def bind(self, bindings: list[SourceBinding], invocation_id: str) -> BindingReport:
        try:
            import duckdb
        except ImportError as exc:  # pragma: no cover - packaging contract
            raise BindingError("DuckDB is not installed in this environment.") from exc
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.workspace.catalogs.mkdir(parents=True, exist_ok=True)
        results: list[BindingResult] = []
        attachments: dict[str, Path] = {}
        with FileLock(
            self.workspace.locks / "duckdb-writer.lock",
            invocation_id=invocation_id,
            command="bind",
            timeout_seconds=0,
        ):
            main = duckdb.connect(str(self.database))
            try:
                if self.install_compatibility_shims:
                    install_shims(main)
                current = main.execute("SELECT current_database()").fetchone()
                if current is None:
                    raise BindingError("DuckDB did not return its current catalog.")
                main_catalog = str(current[0])
            finally:
                main.close()
            for binding in bindings:
                relation = binding.mapping.local_relation
                catalog = relation.catalog
                if catalog and catalog.casefold() != main_catalog.casefold():
                    catalog_hash = sha256_value(catalog).split(":", 1)[1][:20]
                    catalog_path = self.workspace.catalogs / f"{catalog_hash}.duckdb"
                    attachments[catalog] = catalog_path
                else:
                    catalog_path = self.database
                connection = duckdb.connect(str(catalog_path))
                try:
                    schema = quote_duckdb_identifier(relation.schema)
                    identifier = quote_duckdb_identifier(relation.identifier)
                    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    paths = ", ".join(
                        quote_duckdb_string(str(path.resolve()))
                        for path in binding.snapshot.parquet_paths
                    )
                    if not paths:
                        raise BindingError(
                            f"Snapshot {binding.snapshot.snapshot_key} has no Parquet files."
                        )
                    connection.execute(
                        f"CREATE OR REPLACE VIEW {schema}.{identifier} AS "
                        f"SELECT * FROM read_parquet([{paths}], union_by_name=true)"
                    )
                except Exception as exc:
                    if isinstance(exc, BindingError):
                        raise
                    raise BindingError(
                        f"Unable to bind {binding.mapping.source.unique_id} to DuckDB.",
                        context={"relation": relation.display_name},
                    ) from exc
                finally:
                    connection.close()
                results.append(
                    BindingResult(
                        source_unique_id=binding.mapping.source.unique_id,
                        relation=relation.display_name,
                        catalog_path=str(catalog_path),
                        verified=False,
                        row_count=binding.snapshot.row_count,
                    )
                )
            self.workspace.write_json(
                self.workspace.catalogs / "catalog-map.json",
                {catalog: str(path) for catalog, path in sorted(attachments.items())},
            )
        return BindingReport(tuple(results), attachments)

    def verify(
        self,
        report: BindingReport,
        bindings: list[SourceBinding],
    ) -> BindingReport:
        try:
            import duckdb

            connection = duckdb.connect(str(self.database))
            try:
                for catalog, path in report.attachments.items():
                    connection.execute(
                        f"ATTACH {quote_duckdb_string(str(path))} AS "
                        f"{quote_duckdb_identifier(catalog)} (READ_ONLY)"
                    )
                verified: list[BindingResult] = []
                by_source = {result.source_unique_id: result for result in report.results}
                for binding in bindings:
                    relation = binding.mapping.local_relation
                    rendered = ".".join(
                        quote_duckdb_identifier(part)
                        for part in (relation.catalog, relation.schema, relation.identifier)
                        if part
                    )
                    connection.execute(f"SELECT * FROM {rendered} LIMIT 0")
                    current = by_source[binding.mapping.source.unique_id]
                    verified.append(
                        BindingResult(
                            source_unique_id=current.source_unique_id,
                            relation=current.relation,
                            catalog_path=current.catalog_path,
                            verified=True,
                            row_count=current.row_count,
                        )
                    )
                return BindingReport(tuple(verified), report.attachments)
            finally:
                connection.close()
        except Exception as exc:
            if isinstance(exc, BindingError):
                raise
            raise BindingError(
                "DuckDB source binding verification failed.",
                hint="Inspect the binding artifact and local manifest relation names.",
            ) from exc

    def check_prerequisites(
        self, boundaries: list[dict[str, Any]], attachments: dict[str, Path]
    ) -> list[dict[str, Any]]:
        import duckdb

        connection = duckdb.connect(str(self.database), read_only=True)
        try:
            for catalog, path in attachments.items():
                connection.execute(
                    f"ATTACH {quote_duckdb_string(str(path))} AS "
                    f"{quote_duckdb_identifier(catalog)} (READ_ONLY)"
                )
            results = []
            for boundary in boundaries:
                relation = boundary["relation"]
                present = False
                if relation:
                    rendered = ".".join(
                        quote_duckdb_identifier(relation[key])
                        for key in ("catalog", "schema", "identifier")
                        if relation.get(key)
                    )
                    try:
                        connection.execute(f"SELECT * FROM {rendered} LIMIT 0")
                        present = True
                    except duckdb.Error:
                        pass
                results.append({**boundary, "available": present})
            return results
        finally:
            connection.close()

    def inspect(
        self, query: str, *, max_rows: int = 1000, allowed_paths: tuple[Path, ...] = ()
    ) -> tuple[tuple[str, ...], tuple[tuple[Any, ...], ...]]:
        one_statement = ";" not in query.rstrip().rstrip(";")
        if (
            not one_statement
            or _UNSAFE_INSPECT.search(query)
            or not (_SAFE_QUERY.match(query) or _SAFE_PRAGMA.match(query))
        ):
            raise BindingError(
                "Inspect accepts one read-only SELECT, WITH, EXPLAIN, DESCRIBE, "
                "SHOW, or PRAGMA query."
            )
        try:
            import duckdb

            connection = duckdb.connect(
                str(self.database),
                read_only=True,
                config={"memory_limit": "1GB", "threads": 1},
            )
            try:
                catalog_map = self.workspace.catalogs / "catalog-map.json"
                if catalog_map.is_file():
                    for catalog, path in json.loads(catalog_map.read_text()).items():
                        resolved = Path(path).resolve()
                        if resolved.is_relative_to(self.workspace.catalogs.resolve()):
                            connection.execute(
                                f"ATTACH {quote_duckdb_string(str(resolved))} AS "
                                f"{quote_duckdb_identifier(catalog)} (READ_ONLY)"
                            )
                paths = ", ".join(
                    quote_duckdb_string(str(path.resolve())) for path in allowed_paths
                )
                connection.execute(f"SET allowed_paths = [{paths}]")
                connection.execute("SET enable_external_access = false")
                cursor = connection.execute(query)
                columns = tuple(item[0] for item in (cursor.description or ()))
                rows = tuple(tuple(row) for row in cursor.fetchmany(max_rows))
                return columns, rows
            finally:
                connection.close()
        except Exception as exc:
            if isinstance(exc, BindingError):
                raise
            raise BindingError("Read-only DuckDB inspection failed.") from exc
