from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from platformdirs import user_config_path
from pydantic import ValidationError

from dbtv.config.schema import DbtvConfig
from dbtv.core.errors import ConfigError

DEFAULT_CONFIG_NAME = "dbtv.yml"


def load_config(project_dir: Path, explicit_path: Path | None = None) -> DbtvConfig:
    path = explicit_path or project_dir / DEFAULT_CONFIG_NAME
    raw = _read_mapping(user_config_path("dbtv") / DEFAULT_CONFIG_NAME, required=False)
    if path.exists():
        raw = _deep_merge(raw, _read_mapping(path, required=True))

    _apply_environment(raw)
    _reject_secrets(raw)
    try:
        config = DbtvConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Invalid dbtv configuration at {path}:\n{exc}") from exc

    resolved = config.resolve_paths(project_dir)
    _validate_workspace_paths(resolved, project_dir.resolve())
    return resolved


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
        "DBTV_DEFAULT_DATA_PROFILE": ("default_data_profile",),
        "DBTV_SOURCE_CONNECTOR": ("source", "connector"),
        "DBTV_LOCAL_MEMORY_LIMIT": ("local", "memory_limit"),
    }
    for env_name, path in mappings.items():
        value = os.getenv(env_name)
        if value is None:
            continue
        current = raw
        for component in path[:-1]:
            current = current.setdefault(component, {})
        current[path[-1]] = value


def _read_mapping(path: Path, *, required: bool) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise ConfigError(f"Configuration does not exist at {path}.")
        return {}
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"Unable to read configuration at {path}: {exc}") from exc
    if loaded is not None and not isinstance(loaded, dict):
        raise ConfigError(f"Configuration at {path} must be a YAML mapping.")
    return loaded or {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _reject_secrets(raw: dict[str, Any]) -> None:
    prohibited = {
        "password",
        "token",
        "private_key",
        "private_key_path",
        "private_key_passphrase",
        "passphrase",
        "secret",
    }

    def walk(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = (*path, str(key))
                if str(key).lower() in prohibited:
                    raise ConfigError(
                        f"Secret field {'.'.join(child_path)!r} is forbidden in dbtv.yml.",
                        hint="Keep credentials in the existing dbt profile or environment.",
                    )
                walk(child, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, (*path, str(index)))

    walk(raw, ())


def _validate_workspace_paths(config: DbtvConfig, project_dir: Path) -> None:
    forbidden = {
        Path("/").resolve(),
        Path.home().resolve(),
        project_dir,
        (project_dir / ".dbtv").resolve(),
    }
    for label, path in {
        "cache.root": config.cache.root,
        "local.temp_directory": config.local.temp_directory,
    }.items():
        resolved = path.resolve()
        if resolved in forbidden:
            raise ConfigError(f"Unsafe {label} path: {resolved}")
    if config.local.database.resolve() in forbidden or config.local.database.is_dir():
        raise ConfigError(f"Unsafe local.database path: {config.local.database.resolve()}")
