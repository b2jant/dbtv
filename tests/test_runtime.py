from __future__ import annotations

import stat
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from dbtv.connectors.registry import ConnectorRegistry
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CancellationError, LockError, OfflineViolation
from dbtv.core.locks import FileLock
from dbtv.project.dbt_invoker import DbtInvoker


def test_lock_conflict_reports_owner(tmp_path: Path) -> None:
    path = tmp_path / "writer.lock"
    with (
        FileLock(path, invocation_id="one", command="run"),
        pytest.raises(LockError, match="already locked"),
        FileLock(path, invocation_id="two", command="build"),
    ):
        pass


def test_stale_lock_file_is_recovered_when_no_process_holds_it(tmp_path: Path) -> None:
    path = tmp_path / "writer.lock"
    path.write_text('{"pid": 999999, "invocation_id": "stale"}', encoding="utf-8")

    with FileLock(path, invocation_id="fresh", command="run"):
        assert '"invocation_id": "fresh"' in path.read_text(encoding="utf-8")


def test_dbt_child_process_is_terminated_on_cancellation(tmp_path: Path) -> None:
    executable = tmp_path / "slow-dbt.py"
    executable.write_text(
        textwrap.dedent(
            f"""\
            #!{sys.executable}
            import time
            time.sleep(30)
            """
        ),
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    token = CancellationToken()
    timer = threading.Timer(0.1, token.cancel)
    timer.start()
    try:
        with pytest.raises(CancellationError):
            DbtInvoker(str(executable)).run(["run"], cancellation=token)
    finally:
        timer.cancel()


def test_offline_registry_cannot_instantiate_even_an_installed_factory() -> None:
    class Factory:
        def capabilities(self) -> object:
            return object()

        def create(self, **_: object) -> object:
            return object()

    registry = ConnectorRegistry(
        offline=True,
        factories={"fake": Factory()},  # type: ignore[dict-item]
    )
    with pytest.raises(OfflineViolation):
        registry.create("fake")
