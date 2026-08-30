from __future__ import annotations

import json
import os
import uuid
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dbtv.core.errors import ConfigError
from dbtv.core.redaction import redact


@dataclass(frozen=True)
class RunWorkspace:
    invocation_id: str
    root: Path
    production_target_path: Path
    local_target_path: Path
    generated_profiles_dir: Path


class Workspace:
    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir.resolve()
        self.root = self.project_dir / ".dbtv"
        self.runs = self.root / "runs"
        self.manifests = self.root / "manifests"
        self.cache = self.root / "cache"
        self.catalogs = self.root / "catalogs"
        self.locks = self.root / "locks"
        self.tmp = self.root / "tmp"

    def ensure(self) -> None:
        if self.root == self.project_dir or self.root.parent != self.project_dir:
            raise ConfigError("Refusing unsafe dbtv workspace path.")
        for path in (
            self.root,
            self.runs,
            self.manifests,
            self.cache,
            self.catalogs,
            self.locks,
            self.tmp,
        ):
            path.mkdir(parents=True, exist_ok=True)
            with suppress(OSError):
                path.chmod(0o700)

    def create_run(self, invocation_id: str | None = None) -> RunWorkspace:
        self.ensure()
        run_id = invocation_id or str(uuid.uuid4())
        root = self.runs / run_id
        production = root / "production-target"
        local = root / "local-target"
        profiles = root / "generated-profiles"
        for path in (root, production, local, profiles):
            path.mkdir(parents=True, exist_ok=False)
        return RunWorkspace(run_id, root, production, local, profiles)

    def write_json(self, path: Path, value: Any) -> None:
        _atomic_write(path, json.dumps(redact(value), indent=2, sort_keys=True) + "\n")


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(text, encoding="utf-8")
    with suppress(OSError):
        temporary.chmod(0o600)
    os.replace(temporary, path)
