from __future__ import annotations

import hashlib
from pathlib import Path


def project_fingerprint(project_dir: Path) -> str:
    digest = hashlib.sha256()
    excluded = {".dbtv", "target", "logs", ".git", ".venv", "dbt_packages"}
    suffixes = {".sql", ".yml", ".yaml", ".py", ".csv"}
    paths = sorted(
        path
        for path in project_dir.rglob("*")
        if path.is_file()
        and path.suffix.lower() in suffixes
        and not any(part in excluded for part in path.relative_to(project_dir).parts)
    )
    for path in paths:
        relative = path.relative_to(project_dir).as_posix().encode()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"
