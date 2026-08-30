from __future__ import annotations

import duckdb

from dbtv.backend.shims import SHIMS, install_shims


def test_duckdb_shims_have_version_ledger_and_semantic_contracts() -> None:
    connection = duckdb.connect(":memory:")
    try:
        installed = install_shims(connection)
        assert installed == SHIMS
        assert connection.execute("select iff(true, 1, 2), iff(false, 1, 2)").fetchone() == (
            1,
            2,
        )
        assert connection.execute("select equal_null(NULL, NULL)").fetchone() == (True,)
        assert connection.execute("select zeroifnull(NULL)").fetchone() == (0,)
        ledger = connection.execute(
            "select name, version from _dbtv_metadata.shims order by name"
        ).fetchall()
        assert ledger == sorted((shim.name, shim.version) for shim in SHIMS)
    finally:
        connection.close()
