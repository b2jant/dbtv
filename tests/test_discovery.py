from pathlib import Path

import pytest

from dbtv.core.errors import ProjectError
from dbtv.project.discovery import discover_project


def test_discovers_project_from_nested_directory(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text(
        "name: analytics\nprofile: analytics_profile\n", encoding="utf-8"
    )
    nested = tmp_path / "models" / "staging"
    nested.mkdir(parents=True)
    project = discover_project(nested, profiles_dir=tmp_path / "profiles")
    assert project.root == tmp_path
    assert project.name == "analytics"
    assert project.profile_name == "analytics_profile"


def test_missing_project_fails(tmp_path: Path) -> None:
    with pytest.raises(ProjectError):
        discover_project(tmp_path)

