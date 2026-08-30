from __future__ import annotations

import json
import os
import socket
import time
from contextlib import AbstractContextManager, suppress
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, TextIO

from dbtv.core.errors import LockError

if os.name == "nt":  # pragma: no cover - exercised on Windows CI
    import msvcrt

    _lock_module: Any = msvcrt
else:
    import fcntl

    _lock_module = fcntl


class FileLock(AbstractContextManager["FileLock"]):
    """Small cross-platform advisory lock with auditable owner metadata."""

    def __init__(
        self,
        path: Path,
        *,
        invocation_id: str,
        command: str,
        timeout_seconds: float = 0,
    ) -> None:
        self.path = path
        self.invocation_id = invocation_id
        self.command = command
        self.timeout_seconds = timeout_seconds
        self._handle: TextIO | None = None

    def __enter__(self) -> FileLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        with suppress(OSError):
            self.path.chmod(0o600)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self._acquire(handle)
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    handle.seek(0)
                    owner = handle.read().strip() or "unknown owner"
                    handle.close()
                    raise LockError(
                        f"Workspace resource is already locked: {self.path}",
                        hint=f"Wait for the active process to finish. Lock owner: {owner}",
                    ) from exc
                time.sleep(0.05)
        handle.seek(0)
        handle.truncate()
        json.dump(
            {
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "invocation_id": self.invocation_id,
                "command": self.command,
                "acquired_at": datetime.now(UTC).isoformat(),
            },
            handle,
            sort_keys=True,
        )
        handle.flush()
        os.fsync(handle.fileno())
        self._handle = handle
        return self

    def _acquire(self, handle: TextIO) -> None:
        if os.name == "nt":  # pragma: no cover - exercised on Windows CI
            _lock_module.locking(handle.fileno(), _lock_module.LK_NBLCK, 1)
        else:
            _lock_module.flock(handle.fileno(), _lock_module.LOCK_EX | _lock_module.LOCK_NB)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self._handle
        if handle is None:
            return
        if os.name == "nt":  # pragma: no cover - exercised on Windows CI
            _lock_module.locking(handle.fileno(), _lock_module.LK_UNLCK, 1)
        else:
            _lock_module.flock(handle.fileno(), _lock_module.LOCK_UN)
        handle.close()
        self._handle = None
