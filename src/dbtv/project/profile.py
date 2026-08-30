from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import Any

import yaml

from dbtv.config.schema import DbtvConfig
from dbtv.project.discovery import DbtProject

LOCAL_TARGET_NAME = "dbtv_local"


def render_local_profile(project: DbtProject, config: DbtvConfig) -> str:
    output: dict[str, Any] = {
        "type": "duckdb",
        "path": str(config.local.database),
        "schema": config.local.schema_name,
        "threads": config.local.threads,
        "extensions": ["parquet", "json"],
        "settings": {
            "memory_limit": config.local.memory_limit,
            "temp_directory": str(config.local.temp_directory),
            "preserve_identifier_case": True,
        },
    }
    payload = {
        project.profile_name: {
            "target": LOCAL_TARGET_NAME,
            "outputs": {LOCAL_TARGET_NAME: output},
        }
    }
    return yaml.safe_dump(payload, sort_keys=False)


def write_local_profile(directory: Path, project: DbtProject, config: DbtvConfig) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "profiles.yml"
    path.write_text(render_local_profile(project, config), encoding="utf-8")
    with suppress(OSError):
        path.chmod(0o600)
    return path
