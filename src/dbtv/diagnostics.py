from __future__ import annotations

import json
import platform
import sys
import zipfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dbtv import __version__
from dbtv.config.schema import DbtvConfig
from dbtv.core.redaction import redact, redact_text
from dbtv.state import StateIndex
from dbtv.workspace import Workspace

_SAFE_RUN_FILES = {
    "run.json",
    "plan.json",
    "events.jsonl",
    "timings.json",
    "compatibility.json",
    "bindings.json",
    "dbt-results.json",
}


def create_diagnostics_bundle(
    *,
    config: DbtvConfig,
    workspace: Workspace,
    destination: Path | None = None,
) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output = destination or workspace.root / "diagnostics" / f"dbtv-{timestamp}.zip"
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    state = StateIndex(workspace.state_path, workspace.locks)
    state.initialize("diagnostics")
    snapshots = state.list_snapshots()
    system = {
        "dbtv": __version__,
        "python": sys.version,
        "platform": platform.platform(),
        "generated_at": datetime.now(UTC).isoformat(),
        "workspace": str(workspace.root),
    }
    cache = {
        "snapshot_count": len(snapshots),
        "snapshot_bytes": sum(record.byte_count for record in snapshots),
        "snapshots": [
            {
                "source_unique_id": record.source_unique_id,
                "snapshot_key": record.snapshot_key,
                "state": record.state,
                "created_at": record.created_at,
                "expires_at": record.expires_at,
                "row_count": record.row_count,
                "byte_count": record.byte_count,
                "active": record.active,
                "pinned": record.pinned,
            }
            for record in snapshots
        ],
    }
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        _write_json(archive, "system.json", system)
        _write_json(
            archive,
            "resolved-config-redacted.json",
            config.model_dump(mode="json", by_alias=True),
        )
        _write_json(archive, "cache-summary.json", cache)
        runs = (
            sorted(
                (path for path in workspace.runs.iterdir() if path.is_dir()),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )[:5]
            if workspace.runs.exists()
            else []
        )
        for run in runs:
            for name in _SAFE_RUN_FILES:
                path = run / name
                if path.is_file() and path.stat().st_size <= 10 * 1024**2:
                    text = path.read_text(encoding="utf-8", errors="replace")
                    archive.writestr(f"runs/{run.name}/{name}", _redact_text(text))
    with suppress(OSError):
        output.chmod(0o600)
    return output


def _write_json(archive: zipfile.ZipFile, name: str, value: Any) -> None:
    archive.writestr(
        name,
        json.dumps(redact(value), indent=2, sort_keys=True, default=str) + "\n",
    )


def _redact_text(text: str) -> str:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        lines: list[str] = []
        for line in text.splitlines():
            try:
                lines.append(json.dumps(redact(json.loads(line)), sort_keys=True))
            except json.JSONDecodeError:
                lines.append(redact_text(line))
        return "\n".join(lines) + ("\n" if text.endswith("\n") else "")
    return json.dumps(redact(value), indent=2, sort_keys=True) + "\n"
