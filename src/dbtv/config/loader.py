from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from dbtv.config.schema import DbtvConfig
from dbtv.core.errors import ConfigError

DEFAULT_CONFIG_NAME = "dbtv.yml"


def load_config(project_dir: Path, explicit_path: Path | None = None) -> DbtvConfig:
    path = explicit_path or project_dir / DEFAULT_CONFIG_NAME
    raw: dict[str, Any] = {}
    if path.exists():
        try:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise ConfigError(f"Unable to read configuration at {path}: {exc}") from exc
        if loaded is not None and not isinstance(loaded, dict):
            raise ConfigError(f"Configuration at {path} must be a YAML mapping.")
        raw = loaded or {}

    _apply_environment(raw)
    try:
        config = DbtvConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Invalid dbtv configuration at {path}:\n{exc}") from exc

    if config.default_data_profile not in config.data_profiles:
        raise ConfigError(
            f"default_data_profile {config.default_data_profile!r} is not defined."
        )
    return config.resolve_paths(project_dir)


def render_default_config(*, production_target: str | None = None) -> str:
    config = DbtvConfig()
    if production_target:
        config = config.model_copy(
            update={
                "project": config.project.model_copy(
                    update={"production_target": production_target}
                )
            }
        )
    payload = config.model_dump(mode="json", by_alias=True, exclude_none=True)
    return yaml.safe_dump(payload, sort_keys=False)


def _apply_environment(raw: dict[str, Any]) -> None:
    mappings = {
        "DBTV_DBT_EXECUTABLE": ("project", "dbt_executable"),
        "DBTV_PRODUCTION_TARGET": ("project", "production_target"),
        "DBTV_LOCAL_DATABASE": ("local", "database"),
        "DBTV_CACHE_ROOT": ("cache", "root"),
    }
    for env_name, path in mappings.items():
        value = os.getenv(env_name)
        if value is None:
            continue
        current = raw
        for component in path[:-1]:
            current = current.setdefault(component, {})
        current[path[-1]] = value
