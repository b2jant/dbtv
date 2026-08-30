from pathlib import Path

import pytest

from dbtv.config.loader import load_config, render_default_config
from dbtv.core.errors import ConfigError


def test_default_configuration_resolves_paths(tmp_path: Path) -> None:
    config = load_config(tmp_path)
    assert config.version == 1
    assert config.local.database == tmp_path / ".dbtv/local.duckdb"
    assert config.cache.root == tmp_path / ".dbtv/cache"


def test_rendered_default_round_trips(tmp_path: Path) -> None:
    (tmp_path / "dbtv.yml").write_text(render_default_config(), encoding="utf-8")
    config = load_config(tmp_path)
    assert config.default_data_profile == "developer"
    assert config.data_profiles["developer"].default.limit == 100_000


def test_unknown_configuration_key_fails(tmp_path: Path) -> None:
    (tmp_path / "dbtv.yml").write_text("version: 1\nunknown: true\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path)

