from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from dbtv.core.errors import ProjectError


@dataclass(frozen=True)
class DbtProject:
    root: Path
    project_file: Path
    name: str
    profile_name: str
    profiles_dir: Path


def discover_project(
    start: Path,
    *,
    profiles_dir: Path | None = None,
) -> DbtProject:
    root = _find_project_root(start.resolve())
    project_file = root / "dbt_project.yml"
    try:
        raw = yaml.safe_load(project_file.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ProjectError(f"Unable to read {project_file}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ProjectError(f"{project_file} must contain a YAML mapping.")
    name = raw.get("name")
    profile_name = raw.get("profile")
    if not isinstance(name, str) or not name:
        raise ProjectError(f"{project_file} does not define a project name.")
    if not isinstance(profile_name, str) or not profile_name:
        raise ProjectError(f"{project_file} does not define a profile name.")
    resolved_profiles = _profiles_dir(profiles_dir)
    return DbtProject(root, project_file, name, profile_name, resolved_profiles)


def _find_project_root(start: Path) -> Path:
    candidate = start if start.is_dir() else start.parent
    for current in (candidate, *candidate.parents):
        if (current / "dbt_project.yml").is_file():
            return current
    raise ProjectError(
        f"No dbt_project.yml found from {start} upward.",
        hint="Run from a dbt project or pass --project-dir.",
    )


def _profiles_dir(explicit: Path | None) -> Path:
    if explicit:
        return explicit.expanduser().resolve()
    if env_path := os.getenv("DBT_PROFILES_DIR"):
        return Path(env_path).expanduser().resolve()
    return (Path.home() / ".dbt").resolve()

