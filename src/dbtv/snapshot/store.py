from __future__ import annotations

import json
import os
import shutil
import sys
import uuid
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dbtv import __version__
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CacheError, ExtractionError
from dbtv.core.hashing import sha256_file, sha256_value
from dbtv.core.locks import FileLock
from dbtv.core.models import (
    CanonicalField,
    CanonicalSchema,
    DatasetSnapshot,
    ExtractionBatch,
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotAction,
    SnapshotDecision,
    SnapshotFile,
    SnapshotRequest,
    SourceRef,
    SourceVersion,
)
from dbtv.snapshot.keys import request_fingerprint, snapshot_key
from dbtv.state import SnapshotIndexRecord, StateIndex


class ParquetSnapshotStore:
    def __init__(
        self,
        root: Path,
        *,
        state: StateIndex,
        locks_dir: Path,
        ttl: timedelta,
        compression: str = "zstd",
        row_group_target_bytes: int = 134_217_728,
        integrity: str = "metadata_and_sizes",
        maximum_size: int = 100 * 1024**3,
        retain_previous: int = 2,
        file_mode: int = 0o600,
        directory_mode: int = 0o700,
    ) -> None:
        self.root = root.resolve()
        self.snapshots_root = self.root / "snapshots"
        self.tmp_root = self.root / "tmp"
        self.state = state
        self.locks_dir = locks_dir
        self.ttl = ttl
        self.compression = compression
        self.row_group_target_bytes = row_group_target_bytes
        self.integrity = integrity
        self.maximum_size = maximum_size
        self.retain_previous = retain_previous
        self.file_mode = file_mode
        self.directory_mode = directory_mode
        self.ensure()

    def ensure(self) -> None:
        if self.root in {Path("/").resolve(), Path.home().resolve()}:
            raise CacheError(f"Refusing unsafe snapshot root {self.root}.")
        for path in (self.root, self.snapshots_root, self.tmp_root):
            path.mkdir(parents=True, exist_ok=True)
            with suppress(OSError):
                path.chmod(self.directory_mode)

    def decide(
        self,
        request: SnapshotRequest,
        *,
        provider: str,
        mode: str,
        snapshot_id: str | None = None,
    ) -> SnapshotDecision:
        fingerprint = request_fingerprint(request, provider)
        record = (
            self.state.get_snapshot(snapshot_id)
            if snapshot_id
            else self.state.active_snapshot(request.source.unique_id, fingerprint)
        )
        snapshot: DatasetSnapshot | None = None
        if record and record.state == "COMMITTED":
            try:
                snapshot = self._load_record(record)
                self.validate(snapshot)
            except CacheError:
                self.state.mark_corrupt(record.snapshot_key)
                snapshot = None
        if snapshot_id and snapshot is None:
            return SnapshotDecision(request, SnapshotAction.MISSING, "pinned snapshot not found")
        if snapshot and snapshot.request_fingerprint != fingerprint:
            return SnapshotDecision(request, SnapshotAction.MISSING, "snapshot request differs")
        if mode == "refresh":
            return SnapshotDecision(request, SnapshotAction.REFRESH, "refresh requested", snapshot)
        if snapshot and (mode in {"offline", "cached"} or not self._expired(snapshot)):
            return SnapshotDecision(
                request, SnapshotAction.REUSE, "valid committed snapshot", snapshot
            )
        if mode in {"offline", "cached"}:
            return SnapshotDecision(request, SnapshotAction.MISSING, f"{mode} cache miss")
        reason = "snapshot expired" if snapshot else "snapshot missing"
        return SnapshotDecision(request, SnapshotAction.REFRESH, reason, snapshot)

    def write(
        self,
        request: SnapshotRequest,
        *,
        provider: str,
        batches: Iterable[ExtractionBatch],
        cancellation: CancellationToken,
        invocation_id: str,
        source_version: SourceVersion | None = None,
        provider_version: str | None = None,
        pinned: bool = False,
        activate: bool = True,
    ) -> DatasetSnapshot:
        try:
            import pyarrow as pa  # type: ignore[import-untyped]
            import pyarrow.parquet as pq  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - packaging contract
            raise CacheError("PyArrow is required for Parquet snapshots.") from exc

        fingerprint = request_fingerprint(request, provider)
        source_hash = sha256_value(request.source.unique_id).split(":", 1)[1][:20]
        lock_path = self.locks_dir / f"snapshot-{source_hash}.lock"
        with FileLock(
            lock_path,
            invocation_id=invocation_id,
            command="snapshot-write",
            timeout_seconds=0,
        ):
            pending = self.tmp_root / f"snapshot-{uuid.uuid4().hex}"
            data_dir = pending / "data"
            data_dir.mkdir(parents=True)
            created = datetime.now(UTC)
            row_count = 0
            byte_count = 0
            query_id: str | None = None
            files: list[SnapshotFile] = []
            content_parts: list[dict[str, str | int]] = []
            arrow_schema: Any = None
            try:
                for index, extracted in enumerate(batches):
                    cancellation.raise_if_cancelled()
                    batch = extracted.data
                    if arrow_schema is None:
                        arrow_schema = batch.schema
                    elif not batch.schema.equals(arrow_schema, check_metadata=False):
                        raise ExtractionError(
                            f"Schema changed during extraction of {request.source.unique_id}."
                        )
                    table = pa.Table.from_batches([batch])
                    relative = Path("data") / f"part-{index:05d}.parquet"
                    target = pending / relative
                    pq.write_table(
                        table,
                        target,
                        compression=None if self.compression == "none" else self.compression,
                        row_group_size=_row_group_rows(
                            table.num_rows,
                            table.nbytes,
                            self.row_group_target_bytes,
                        ),
                    )
                    size = target.stat().st_size
                    byte_count += size
                    row_count += table.num_rows
                    if self.current_size() + byte_count > self.maximum_size:
                        raise CacheError(
                            "Snapshot cache quota would be exceeded.",
                            hint="Run `dbtv clean --snapshots --preview` or lower the working set.",
                            context={"bytes_written": byte_count, "quota": self.maximum_size},
                        )
                    content_checksum = sha256_file(target)
                    checksum = content_checksum if self.integrity == "checksums" else None
                    files.append(SnapshotFile(str(relative), size, checksum))
                    content_parts.append(
                        {
                            "path": str(relative),
                            "size": size,
                            "checksum": content_checksum,
                        }
                    )
                    query_id = extracted.query_id or query_id
                if arrow_schema is None:
                    raise ExtractionError(
                        f"Connector returned no Arrow schema for {request.source.unique_id}."
                    )
                canonical = _canonical_schema(arrow_schema)
                content_fingerprint = sha256_value(content_parts)
                key = snapshot_key(
                    request,
                    provider,
                    canonical,
                    source_version,
                    content_fingerprint,
                )
                completed = datetime.now(UTC)
                final = self.snapshots_root / source_hash / key.split(":", 1)[1]
                snapshot = DatasetSnapshot(
                    snapshot_key=key,
                    request_fingerprint=fingerprint,
                    content_fingerprint=content_fingerprint,
                    source=request.source,
                    provider=provider,
                    remote_relation=request.relation,
                    sampling=request.sampling,
                    fidelity=request.fidelity,
                    projection=request.projection,
                    schema=canonical,
                    source_version=source_version,
                    root=final,
                    files=tuple(files),
                    created_at=created.isoformat(),
                    completed_at=completed.isoformat(),
                    expires_at=(completed + self.ttl).isoformat(),
                    row_count=row_count,
                    byte_count=byte_count,
                    query_id=query_id,
                    query_tag=request.query_tag,
                    provider_version=provider_version,
                    pinned=pinned,
                    connection_scope=request.connection_scope,
                )
                (pending / "metadata.json").write_text(
                    json.dumps(_metadata(snapshot), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                (pending / "_SUCCESS").write_text("", encoding="utf-8")
                _secure_tree(
                    pending,
                    directory_mode=self.directory_mode,
                    file_mode=self.file_mode,
                )
                final.parent.mkdir(parents=True, exist_ok=True)
                with suppress(OSError):
                    final.parent.chmod(self.directory_mode)
                if final.exists():
                    shutil.rmtree(pending)
                    existing = self.load(final / "metadata.json")
                    self.validate(existing)
                    if pinned and not existing.pinned:
                        existing = self.pin(existing)
                    # Capture identity is immutable. A separate receipt records that
                    # an identical input was actually re-read, including its new TTL.
                    receipt = {
                        "snapshot_key": existing.snapshot_key,
                        "verified_at": completed.isoformat(),
                        "expires_at": (completed + self.ttl).isoformat(),
                        "query_id": query_id,
                        "query_tag": request.query_tag,
                    }
                    temporary = final / ".observation.json.tmp"
                    temporary.write_text(json.dumps(receipt, sort_keys=True) + "\n")
                    temporary.chmod(self.file_mode)
                    os.replace(temporary, final / "observation.json")
                    existing = replace(
                        existing,
                        verified_at=receipt["verified_at"],
                        expires_at=receipt["expires_at"],
                    )
                    self.state.record_snapshot(existing, final / "metadata.json")
                    if activate:
                        self.state.activate(existing.source.unique_id, existing.snapshot_key)
                    return existing
                os.replace(pending, final)
                snapshot = DatasetSnapshot(**{**snapshot.__dict__, "root": final})
                self.validate(snapshot)
                self.state.record_snapshot(snapshot, final / "metadata.json")
                if activate:
                    self.state.activate(snapshot.source.unique_id, snapshot.snapshot_key)
                return snapshot
            except OSError as exc:
                if pending.exists():
                    shutil.rmtree(pending, ignore_errors=True)
                raise CacheError(
                    "Unable to write or commit the local snapshot.",
                    hint="Check free disk space, file permissions, and cache path health.",
                    context={"bytes_written": byte_count},
                ) from exc
            except BaseException:
                if pending.exists():
                    shutil.rmtree(pending, ignore_errors=True)
                raise

    def lookup(self, key: str) -> DatasetSnapshot | None:
        record = self.state.get_snapshot(key)
        if not record or record.state != "COMMITTED":
            return None
        return self._load_record(record)

    def load(self, metadata_path: Path) -> DatasetSnapshot:
        try:
            raw = json.loads(metadata_path.read_text(encoding="utf-8"))
            snapshot = _snapshot_from_metadata(raw, metadata_path.parent)
            receipt_path = metadata_path.parent / "observation.json"
            if receipt_path.is_file():
                receipt = json.loads(receipt_path.read_text())
                if receipt["snapshot_key"] != snapshot.snapshot_key:
                    raise ValueError("Observation does not match its snapshot")
                verified = datetime.fromisoformat(receipt["verified_at"])
                expiry = datetime.fromisoformat(receipt["expires_at"])
                if verified.tzinfo is None or expiry.tzinfo is None:
                    raise ValueError("Observation must have timezone-aware timestamps")
                snapshot = replace(
                    snapshot, verified_at=verified.isoformat(), expires_at=expiry.isoformat()
                )
            return snapshot
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CacheError(f"Invalid snapshot metadata at {metadata_path}.") from exc

    def validate(self, snapshot: DatasetSnapshot) -> None:
        if not (snapshot.root / "_SUCCESS").is_file():
            raise CacheError(f"Snapshot {snapshot.snapshot_key} is incomplete.")
        for item in snapshot.files:
            path = (snapshot.root / item.path).resolve()
            if not path.is_relative_to(snapshot.root.resolve()) or not path.is_file():
                raise CacheError(f"Snapshot file is missing or unsafe: {item.path}")
            if (
                self.integrity in {"metadata_and_sizes", "checksums"}
                and path.stat().st_size != item.size
            ):
                raise CacheError(f"Snapshot file size mismatch: {item.path}")
            if self.integrity == "checksums" and item.checksum != sha256_file(path):
                raise CacheError(f"Snapshot checksum mismatch: {item.path}")

    def activate(self, snapshot: DatasetSnapshot) -> None:
        self.validate(snapshot)
        self.state.activate(snapshot.source.unique_id, snapshot.snapshot_key)

    def pin(self, snapshot: DatasetSnapshot) -> DatasetSnapshot:
        metadata_path = snapshot.root / "metadata.json"
        try:
            raw = json.loads(metadata_path.read_text(encoding="utf-8"))
            raw["pinned"] = True
            temporary = metadata_path.with_name(f".metadata.{uuid.uuid4().hex}.tmp")
            temporary.write_text(
                json.dumps(raw, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            with suppress(OSError):
                temporary.chmod(self.file_mode)
            os.replace(temporary, metadata_path)
        except (OSError, ValueError, TypeError) as exc:
            raise CacheError(f"Unable to pin snapshot {snapshot.snapshot_key}.") from exc
        self.state.set_pinned(snapshot.snapshot_key, True)
        return replace(snapshot, pinned=True)

    def current_size(self) -> int:
        return sum(record.byte_count for record in self.state.list_snapshots())

    def garbage_collect(
        self,
        *,
        older_than: datetime | None = None,
        invocation_id: str = "garbage-collection",
    ) -> dict[str, int]:
        with FileLock(
            self.locks_dir / "garbage-collection.lock",
            invocation_id=invocation_id,
            command="garbage-collection",
            timeout_seconds=5,
        ):
            return self._garbage_collect_unlocked(older_than=older_than)

    def _garbage_collect_unlocked(
        self,
        *,
        older_than: datetime | None = None,
    ) -> dict[str, int]:
        removed = 0
        bytes_removed = 0
        records = self.state.list_snapshots()
        referenced = self.state.dataset_references()
        retained_by_source: dict[str, int] = {}
        for record in records:
            retained = retained_by_source.get(record.source_unique_id, 0)
            keep_history = retained < self.retain_previous
            if record.active or record.pinned or keep_history or record.snapshot_key in referenced:
                retained_by_source[record.source_unique_id] = retained + 1
                continue
            if older_than and datetime.fromisoformat(record.created_at) >= older_than:
                continue
            root = record.metadata_path.parent.resolve()
            if root.is_relative_to(self.snapshots_root) and root != self.snapshots_root:
                shutil.rmtree(root)
                self.state.delete_snapshot(record.snapshot_key)
                removed += 1
                bytes_removed += record.byte_count
        return {"snapshots_removed": removed, "bytes_removed": bytes_removed}

    def rebuild_index(self) -> dict[str, int]:
        self.state.clear_snapshots()
        recovered: list[DatasetSnapshot] = []
        corrupt = 0
        for metadata_path in sorted(self.snapshots_root.glob("*/*/metadata.json")):
            try:
                snapshot = self.load(metadata_path)
                self.validate(snapshot)
                self.state.record_snapshot(snapshot, metadata_path)
                recovered.append(snapshot)
            except CacheError:
                corrupt += 1
        # Scoped snapshots are published only through complete datasets. An interrupted
        # refresh may have written valid files without publishing its source set.
        for snapshot in sorted(recovered, key=lambda item: item.completed_at):
            if not snapshot.connection_scope:
                self.state.activate(snapshot.source.unique_id, snapshot.snapshot_key)
        payloads = {item["dataset_id"]: item for item in self.state.list_datasets()}
        directory = self.state.path.parent / "datasets"
        for path in directory.glob("*.json"):
            try:
                payload = json.loads(path.read_text())
                identity = {
                    key: payload[key]
                    for key in (
                        "format_version",
                        "snapshots",
                        "source_scopes",
                    )
                }
                if sha256_value(identity) == payload["dataset_id"]:
                    payloads.setdefault(payload["dataset_id"], payload)
            except (OSError, ValueError, KeyError, TypeError):
                continue
        for payload in sorted(
            payloads.values(),
            key=lambda item: (
                bool(item.get("active")),
                max((source["completed_at"] for source in item.get("sources", [])), default=""),
            ),
        ):
            try:
                self.state.commit_dataset(payload["dataset_id"], payload)
            except CacheError:
                continue
        return {"snapshots_recovered": len(recovered), "corrupt_snapshots": corrupt}

    def _load_record(self, record: SnapshotIndexRecord) -> DatasetSnapshot:
        return self.load(record.metadata_path)

    def _expired(self, snapshot: DatasetSnapshot) -> bool:
        current_expiry = (
            datetime.fromisoformat(snapshot.verified_at or snapshot.completed_at) + self.ttl
        )
        if snapshot.expires_at:
            current_expiry = min(current_expiry, datetime.fromisoformat(snapshot.expires_at))
        return current_expiry <= datetime.now(UTC)


def _canonical_schema(schema: Any) -> CanonicalSchema:
    fields = tuple(
        CanonicalField(str(field.name), str(field.type), bool(field.nullable)) for field in schema
    )
    return CanonicalSchema(fields, sha256_value([asdict(field) for field in fields]))


def _metadata(snapshot: DatasetSnapshot) -> dict[str, Any]:
    return {
        "format_version": 1,
        "connection_scope": snapshot.connection_scope,
        "snapshot_key": snapshot.snapshot_key,
        "request_fingerprint": snapshot.request_fingerprint,
        "content_fingerprint": snapshot.content_fingerprint,
        "source": asdict(snapshot.source),
        "provider": {
            "name": snapshot.provider,
            "connector_version": snapshot.provider_version,
        },
        "remote_relation": asdict(snapshot.remote_relation),
        "request": {
            "projection": snapshot.projection,
            "sampling": asdict(snapshot.sampling),
            "fidelity": snapshot.fidelity.value,
        },
        "source_version": asdict(snapshot.source_version) if snapshot.source_version else None,
        "schema": asdict(snapshot.schema),
        "created_at": snapshot.created_at,
        "completed_at": snapshot.completed_at,
        "expires_at": snapshot.expires_at,
        "row_count": snapshot.row_count,
        "byte_count": snapshot.byte_count,
        "files": [asdict(item) for item in snapshot.files],
        "query": {
            "query_id": snapshot.query_id,
            "query_tag": snapshot.query_tag,
            "submitted_at": snapshot.created_at,
            "completed_at": snapshot.completed_at,
        },
        "pinned": snapshot.pinned,
        "tool": {"dbtv_version": __version__, "python_version": sys.version},
    }


def _snapshot_from_metadata(raw: dict[str, Any], root: Path) -> DatasetSnapshot:
    sampling = raw["request"]["sampling"]
    schema = raw["schema"]
    return DatasetSnapshot(
        snapshot_key=str(raw["snapshot_key"]),
        request_fingerprint=str(raw["request_fingerprint"]),
        content_fingerprint=str(raw.get("content_fingerprint", raw["snapshot_key"])),
        source=SourceRef(**raw["source"]),
        provider=str(raw["provider"]["name"]),
        remote_relation=Relation(**raw["remote_relation"]),
        sampling=SamplingSpec(
            strategy=SamplingStrategy(sampling["strategy"]),
            limit=sampling.get("limit"),
            where=sampling.get("where"),
            key=sampling.get("key"),
            rate=sampling.get("rate"),
            seed=sampling.get("seed"),
        ),
        fidelity=FidelityMode(raw["request"]["fidelity"]),
        projection=(
            tuple(str(item) for item in raw["request"]["projection"])
            if raw["request"].get("projection")
            else None
        ),
        schema=CanonicalSchema(
            fields=tuple(CanonicalField(**field) for field in schema["fields"]),
            fingerprint=str(schema["fingerprint"]),
        ),
        source_version=(
            SourceVersion(**raw["source_version"]) if raw.get("source_version") else None
        ),
        root=root,
        files=tuple(SnapshotFile(**item) for item in raw["files"]),
        created_at=str(raw["created_at"]),
        completed_at=str(raw["completed_at"]),
        expires_at=str(raw["expires_at"]) if raw.get("expires_at") else None,
        row_count=int(raw["row_count"]),
        byte_count=int(raw["byte_count"]),
        query_id=raw.get("query", {}).get("query_id"),
        query_tag=raw.get("query", {}).get("query_tag"),
        provider_version=raw.get("provider", {}).get("connector_version"),
        pinned=bool(raw.get("pinned", False)),
        connection_scope=str(raw.get("connection_scope", "")),
    )


def _secure_tree(root: Path, *, directory_mode: int, file_mode: int) -> None:
    for directory, _, files in os.walk(root):
        path = Path(directory)
        with suppress(OSError):
            path.chmod(directory_mode)
        for name in files:
            with suppress(OSError):
                (path / name).chmod(file_mode)


def _row_group_rows(rows: int, uncompressed_bytes: int, target_bytes: int) -> int | None:
    if rows <= 0 or uncompressed_bytes <= 0:
        return None
    bytes_per_row = max(1, uncompressed_bytes / rows)
    return max(1, int(target_bytes / bytes_per_row))
