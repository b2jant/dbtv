from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DuckDbShim:
    name: str
    version: int
    sql: str


SHIMS = (
    DuckDbShim(
        "iff",
        1,
        "CREATE OR REPLACE MACRO iff(condition, value_if_true, value_if_false) "
        "AS CASE WHEN condition THEN value_if_true ELSE value_if_false END",
    ),
    DuckDbShim(
        "equal_null",
        1,
        "CREATE OR REPLACE MACRO equal_null(left_value, right_value) "
        "AS left_value IS NOT DISTINCT FROM right_value",
    ),
    DuckDbShim(
        "zeroifnull",
        1,
        "CREATE OR REPLACE MACRO zeroifnull(input_value) AS coalesce(input_value, 0)",
    ),
)


def install_shims(connection: Any) -> tuple[DuckDbShim, ...]:
    connection.execute("CREATE SCHEMA IF NOT EXISTS _dbtv_metadata")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS _dbtv_metadata.shims (
            name VARCHAR PRIMARY KEY,
            version INTEGER NOT NULL,
            installed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
        """
    )
    for shim in SHIMS:
        connection.execute(shim.sql)
        connection.execute(
            """
            INSERT INTO _dbtv_metadata.shims (name, version)
            VALUES (?, ?)
            ON CONFLICT (name) DO UPDATE SET
                version = excluded.version,
                installed_at = now()
            """,
            [shim.name, shim.version],
        )
    return SHIMS
