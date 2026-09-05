from __future__ import annotations

import hashlib
import os
from pathlib import Path


def project_files(project_dir: Path) -> tuple[Path, ...]:
    excluded = {".dbtv", "target", "logs", ".git", ".venv"}
    suffixes = {".sql", ".yml", ".yaml", ".py", ".csv"}
    paths: list[Path] = []
    visited: set[Path] = set()
    for root, directories, files in os.walk(project_dir, followlinks=True):
        directory = Path(root)
        resolved = directory.resolve()
        if resolved in visited:
            directories.clear()
            continue
        visited.add(resolved)
        directories[:] = [name for name in directories if name not in excluded]
        paths.extend(directory / name for name in files if Path(name).suffix.lower() in suffixes)
    return tuple(sorted(paths))


def project_fingerprint(project_dir: Path) -> str:
    digest = hashlib.sha256()
    for path in project_files(project_dir):
        relative = path.relative_to(project_dir).as_posix().encode()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                content.update(chunk)
        digest.update(content.digest())
    return f"sha256:{digest.hexdigest()}"
