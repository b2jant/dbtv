from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

from dbtv.core.locks import FileLock
from dbtv.core.models import DatasetSnapshot, SourceBinding


@dataclass(frozen=True)
class SnapshotIndexRecord:
    snapshot_key: str
    request_fingerprint: str
    source_unique_id: str
    state: str
    created_at: str
    expires_at: str | None
    row_count: int
    byte_count: int
    metadata_path: Path
    active: bool
    pinned: bool


class StateIndex:
    """Rebuildable SQLite index; snapshot sidecar metadata remains authoritative."""

    def __init__(self, path: Path, locks_dir: Path) -> None:
        self.path = path
        self.locks_dir = locks_dir

    def initialize(self, invocation_id: str = "bootstrap") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with (
            FileLock(
                self.locks_dir / "state-migration.lock",
                invocation_id=invocation_id,
                command="state-migration",
                timeout_seconds=5,
            ),
            self._connect() as connection,
        ):
            connection.executescript(
                """
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS snapshots (
                        snapshot_key TEXT PRIMARY KEY,
                        request_fingerprint TEXT NOT NULL,
                        source_unique_id TEXT NOT NULL,
                        state TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        expires_at TEXT,
                        row_count INTEGER NOT NULL,
                        byte_count INTEGER NOT NULL,
                        metadata_path TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 0,
                        pinned INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE IF NOT EXISTS runs (
                        invocation_id TEXT PRIMARY KEY,
                        command TEXT NOT NULL,
                        state TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        completed_at TEXT,
                        remote_query_count INTEGER NOT NULL DEFAULT 0,
                        artifact_path TEXT NOT NULL,
                        exit_code INTEGER
                    );
                    CREATE TABLE IF NOT EXISTS bindings (
                        invocation_id TEXT NOT NULL,
                        source_unique_id TEXT NOT NULL,
                        snapshot_key TEXT NOT NULL,
                        local_relation_json TEXT NOT NULL,
                        PRIMARY KEY (invocation_id, source_unique_id)
                    );
                    """
            )
            self._migrate(connection)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS snapshots_source_request "
                "ON snapshots(source_unique_id, request_fingerprint, active)"
            )
        with suppress(OSError):
            self.path.chmod(0o600)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _migrate(connection: sqlite3.Connection) -> None:
        snapshot_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(snapshots)")
        }
        if "request_fingerprint" not in snapshot_columns:
            connection.execute(
                "ALTER TABLE snapshots ADD COLUMN request_fingerprint TEXT NOT NULL DEFAULT ''"
            )
        if "pinned" not in snapshot_columns:
            connection.execute("ALTER TABLE snapshots ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0")
        run_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(runs)")}
        if "exit_code" not in run_columns:
            connection.execute("ALTER TABLE runs ADD COLUMN exit_code INTEGER")
        connection.execute("PRAGMA user_version=1")

    def record_snapshot(self, snapshot: DatasetSnapshot, metadata_path: Path) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO snapshots (
                    snapshot_key, request_fingerprint, source_unique_id, state,
                    created_at, expires_at, row_count, byte_count, metadata_path,
                    active, pinned
                ) VALUES (?, ?, ?, 'COMMITTED', ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(snapshot_key) DO UPDATE SET
                    state='COMMITTED', expires_at=excluded.expires_at,
                    row_count=excluded.row_count, byte_count=excluded.byte_count,
                    metadata_path=excluded.metadata_path, pinned=excluded.pinned
                """,
                (
                    snapshot.snapshot_key,
                    snapshot.request_fingerprint,
                    snapshot.source.unique_id,
                    snapshot.created_at,
                    snapshot.expires_at,
                    snapshot.row_count,
                    snapshot.byte_count,
                    str(metadata_path),
                    int(snapshot.pinned),
                ),
            )

    def activate(self, source_unique_id: str, snapshot_key: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE snapshots SET active=0 WHERE source_unique_id=?",
                (source_unique_id,),
            )
            connection.execute(
                "UPDATE snapshots SET active=1 WHERE snapshot_key=?",
                (snapshot_key,),
            )

    def mark_corrupt(self, snapshot_key: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE snapshots SET state='CORRUPT', active=0 WHERE snapshot_key=?",
                (snapshot_key,),
            )

    def set_pinned(self, snapshot_key: str, pinned: bool) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE snapshots SET pinned=? WHERE snapshot_key=?",
                (int(pinned), snapshot_key),
            )

    def get_snapshot(self, snapshot_key: str) -> SnapshotIndexRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE snapshot_key=?",
                (snapshot_key,),
            ).fetchone()
        return self._record(row) if row else None

    def active_snapshot(
        self, source_unique_id: str, request_fingerprint: str
    ) -> SnapshotIndexRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM snapshots
                WHERE source_unique_id=? AND request_fingerprint=?
                  AND active=1 AND state='COMMITTED'
                ORDER BY created_at DESC LIMIT 1
                """,
                (source_unique_id, request_fingerprint),
            ).fetchone()
        return self._record(row) if row else None

    def list_snapshots(self) -> tuple[SnapshotIndexRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM snapshots ORDER BY created_at DESC").fetchall()
        return tuple(self._record(row) for row in rows)

    def clear_snapshots(self) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM snapshots")

    def delete_snapshot(self, snapshot_key: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM snapshots WHERE snapshot_key=?", (snapshot_key,))

    def start_run(
        self,
        invocation_id: str,
        command: str,
        state: str,
        started_at: str,
        artifact_path: Path,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO runs
                (invocation_id, command, state, started_at, artifact_path)
                VALUES (?, ?, ?, ?, ?)
                """,
                (invocation_id, command, state, started_at, str(artifact_path)),
            )

    def finish_run(
        self,
        invocation_id: str,
        *,
        state: str,
        completed_at: str,
        remote_query_count: int,
        exit_code: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE runs SET state=?, completed_at=?, remote_query_count=?, exit_code=?
                WHERE invocation_id=?
                """,
                (state, completed_at, remote_query_count, exit_code, invocation_id),
            )

    def update_run_state(self, invocation_id: str, state: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE runs SET state=? WHERE invocation_id=?",
                (state, invocation_id),
            )

    def record_binding(self, invocation_id: str, binding: SourceBinding) -> None:
        relation = binding.mapping.local_relation
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO bindings
                (invocation_id, source_unique_id, snapshot_key, local_relation_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    invocation_id,
                    binding.mapping.source.unique_id,
                    binding.snapshot.snapshot_key,
                    json.dumps(
                        {
                            "catalog": relation.catalog,
                            "schema": relation.schema,
                            "identifier": relation.identifier,
                        },
                        sort_keys=True,
                    ),
                ),
            )

    @staticmethod
    def _record(row: sqlite3.Row) -> SnapshotIndexRecord:
        return SnapshotIndexRecord(
            snapshot_key=str(row["snapshot_key"]),
            request_fingerprint=str(row["request_fingerprint"]),
            source_unique_id=str(row["source_unique_id"]),
            state=str(row["state"]),
            created_at=str(row["created_at"]),
            expires_at=str(row["expires_at"]) if row["expires_at"] else None,
            row_count=int(row["row_count"]),
            byte_count=int(row["byte_count"]),
            metadata_path=Path(str(row["metadata_path"])),
            active=bool(row["active"]),
            pinned=bool(row["pinned"]),
        )
