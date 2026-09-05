from __future__ import annotations

import shutil
import threading
from collections.abc import Iterable
from pathlib import Path

from dbtv.config.schema import DbtvConfig
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import PolicyError
from dbtv.core.units import parse_size


class RunBudget:
    """Actual extraction limits plus cooperative disk monitoring during local execution."""

    def __init__(
        self,
        config: DbtvConfig,
        project_dir: Path,
        token: CancellationToken,
        *,
        max_rows: int | None = None,
        max_bytes: str | None = None,
    ) -> None:
        self.config = config
        self.token = token
        self.row_limit = min(config.policy.max_rows_per_source, max_rows or 2**63 - 1)
        self.byte_limit = min(
            parse_size(config.policy.max_extracted_bytes_per_run),
            parse_size(max_bytes) if max_bytes else 2**63 - 1,
        )
        self.roots = _distinct_roots(
            [
                project_dir / ".dbtv",
                config.cache.root,
                config.local.database,
                config.local.temp_directory,
            ]
        )
        self.rows: dict[str, int] = {}
        self.extracted_bytes = 0
        self.peak_workspace_bytes = 0
        self.failure: PolicyError | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def consume(self, source_id: str, rows: int, byte_count: int) -> None:
        with self._lock:
            self.rows[source_id] = self.rows.get(source_id, 0) + rows
            self.extracted_bytes += byte_count
            if self.rows[source_id] > self.row_limit:
                raise PolicyError(f"Actual extracted rows exceed the limit for {source_id}.")
            if self.extracted_bytes > self.byte_limit:
                raise PolicyError("Actual extracted Arrow bytes exceed the per-run limit.")

    def check_disk(self) -> None:
        usage = sum(_size(path) for path in self.roots)
        self.peak_workspace_bytes = max(self.peak_workspace_bytes, usage)
        if usage > parse_size(self.config.policy.max_workspace_bytes):
            raise PolicyError("Workspace, snapshots, outputs, and spill exceed the disk budget.")
        if _size(self.config.cache.root) > parse_size(self.config.cache.maximum_size):
            raise PolicyError("Snapshot cache quota exceeded, including concurrent pending writes.")
        minimum = parse_size(self.config.policy.minimum_free_disk)
        for path in self.roots:
            existing = path
            while not existing.exists():
                existing = existing.parent
            if shutil.disk_usage(existing).free < minimum:
                raise PolicyError("Free disk space is below policy.minimum_free_disk.")

    def start(self) -> None:
        self.check_disk()

        def monitor() -> None:
            while not self._stop.wait(0.25):
                try:
                    self.check_disk()
                except (PolicyError, OSError) as exc:
                    self.failure = (
                        exc
                        if isinstance(exc, PolicyError)
                        else PolicyError("Cannot measure the configured disk budget.")
                    )
                    self.token.cancel()
                    return

        self._thread = threading.Thread(target=monitor, name="dbtv-disk-budget", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def to_dict(self) -> dict[str, object]:
        return {
            "extracted_rows": dict(self.rows),
            "extracted_arrow_bytes": self.extracted_bytes,
            "peak_workspace_bytes": self.peak_workspace_bytes,
            "workspace_monitor_interval_seconds": 0.25,
            "workspace_limit_bytes": parse_size(self.config.policy.max_workspace_bytes),
            "extraction_limit_bytes": self.byte_limit,
        }


def _distinct_roots(paths: Iterable[Path]) -> tuple[Path, ...]:
    ordered = sorted({path.resolve() for path in paths}, key=lambda path: len(path.parts))
    result: list[Path] = []
    for path in ordered:
        if not any(path.is_relative_to(parent) for parent in result):
            result.append(path)
    return tuple(result)


def _size(path: Path) -> int:
    if path.is_symlink():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except FileNotFoundError:
            return 0
    total = 0
    if path.is_dir():
        try:
            for child in path.iterdir():
                total += _size(child)
        except FileNotFoundError:
            pass
    return total
