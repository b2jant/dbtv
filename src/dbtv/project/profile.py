from __future__ import annotations

import os
import uuid
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any

import yaml

from dbtv.config.schema import DbtvConfig
from dbtv.project.discovery import DbtProject

LOCAL_TARGET_NAME = "dbtv_local"


def render_local_profile(
    project: DbtProject,
    config: DbtvConfig,
    *,
    attachments: Mapping[str, Path] | None = None,
) -> str:
    output: dict[str, Any] = {
        "type": "duckdb",
        "path": str(config.local.database),
        "schema": config.local.schema_name,
        "threads": config.local.threads,
        "settings": {
            "memory_limit": config.local.memory_limit,
            "temp_directory": str(config.local.temp_directory),
            "preserve_identifier_case": config.local.preserve_identifier_case,
            "max_temp_directory_size": config.local.max_temp_directory_size,
        },
    }
    if attachments:
        output["attach"] = [
            {
                "path": str(path),
                "alias": _profile_alias(alias),
                "read_only": True,
            }
            for alias, path in sorted(attachments.items())
        ]
    payload = {
        project.profile_name: {
            "target": LOCAL_TARGET_NAME,
            "outputs": {LOCAL_TARGET_NAME: output},
        }
    }
    return yaml.safe_dump(payload, sort_keys=False)


def write_local_profile(
    directory: Path,
    project: DbtProject,
    config: DbtvConfig,
    *,
    attachments: Mapping[str, Path] | None = None,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "profiles.yml"
    temporary = path.with_name(f".profiles.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        render_local_profile(project, config, attachments=attachments),
        encoding="utf-8",
    )
    with suppress(OSError):
        temporary.chmod(0o600)
    os.replace(temporary, path)
    return path


def _profile_alias(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
