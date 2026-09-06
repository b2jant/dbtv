from __future__ import annotations

import json
import pickle
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from dbtv.config import loader
from dbtv.config.schema import DbtvConfig, PolicySettings, SamplingRule
from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import CancellationError, ConfigError, CredentialError, PolicyError
from dbtv.core.events import EventReporter, RunEvent
from dbtv.core.locks import FileLock
from dbtv.core.units import parse_duration, parse_size
from dbtv.credentials import dbt_profile as credentials
from dbtv.credentials import registry
from dbtv.resources import RunBudget, _size


def test_cancel_notifies_peers_despite_callback_failure() -> None:
    token = CancellationToken()
    failed = Mock(side_effect=RuntimeError("cannot cancel remote query"))
    peer = Mock()
    removed = Mock()
    unregister = token.register(removed)
    unregister()
    unregister()
    token.register(failed)
    token.register(peer)
    token.cancel()
    token.cancel()
    failed.assert_called_once()
    peer.assert_called_once()
    removed.assert_not_called()
    late = Mock()
    token.register(late)()
    late.assert_called_once()
    with pytest.raises(CancellationError):
        token.wait(0)
    CancellationToken().wait(0)


def test_event_consumer_receives_event_and_disk_record_is_redacted(tmp_path: Path) -> None:
    consumer = Mock()
    event = RunEvent("run", "example", "test", attributes={"password": "canary"})
    path = tmp_path / "events.jsonl"
    EventReporter(path, consumer=consumer).emit(event)
    consumer.assert_called_once_with(event)
    assert json.loads(path.read_text())["attributes"]["password"] == "<redacted>"


def test_lock_can_retry_then_release_idempotently(tmp_path: Path, monkeypatch) -> None:
    lock = FileLock(tmp_path / "lock", invocation_id="retry", command="run", timeout_seconds=5)
    acquire = lock._acquire
    calls = 0

    def temporarily_busy(handle):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise BlockingIOError()
        acquire(handle)

    monkeypatch.setattr(lock, "_acquire", temporarily_busy)
    with lock:
        assert json.loads(lock.path.read_text())["invocation_id"] == "retry"
    lock.__exit__(None, None, None)
    assert calls == 2


@pytest.mark.parametrize("parser,value", [(parse_duration, "forever"), (parse_size, "many")])
def test_invalid_units_are_configuration_errors(parser, value) -> None:
    with pytest.raises(ConfigError):
        parser(value)


@pytest.mark.parametrize(
    "raw",
    [
        {"strategy": "limit"},
        {"strategy": "where"},
        {"strategy": "hash", "key": "id"},
        {"strategy": "bernoulli"},
        {"strategy": "where", "where": "true; drop table x"},
    ],
)
def test_invalid_sampling_contracts(raw) -> None:
    with pytest.raises(ValidationError):
        SamplingRule.model_validate(raw)


@pytest.mark.parametrize(
    "raw",
    [
        {"default_data_profile": " "},
        {"default_data_profile": "missing"},
        {"routes": [{"select": "*", "connection": "missing"}]},
        {
            "connections": {"one": {}},
            "routes": [{"select": "*", "connection": "one", "projection": []}],
        },
        {"policy": {"cache_file_mode": "777"}},
    ],
)
def test_invalid_configuration_references(raw) -> None:
    with pytest.raises(ValidationError):
        DbtvConfig.model_validate(raw)


def test_configuration_precedence_and_environment(tmp_path: Path, monkeypatch) -> None:
    user = tmp_path / "user"
    project = tmp_path / "project"
    user.mkdir()
    project.mkdir()
    (user / "dbtv.yml").write_text("local:\n  threads: 2\n  memory_limit: 1GB\n")
    (project / "custom.yml").write_text("local:\n  threads: 3\n")
    monkeypatch.setattr(loader, "user_config_path", lambda _: user)
    monkeypatch.setenv("DBTV_LOCAL_MEMORY_LIMIT", "2GB")
    monkeypatch.setenv("DBTV_DEFAULT_DATA_PROFILE", "developer")
    config = loader.load_config(project, project / "custom.yml")
    assert config.local.threads == 3
    assert config.local.memory_limit == "2GB"
    assert "production_target: dev" in loader.render_default_config(production_target="dev")
    assert PolicySettings(max_cache_age_for_tags={"private": "1h"}).max_cache_age_for_tags


@pytest.mark.parametrize("contents", ["[", "- item"])
def test_configuration_requires_readable_mapping(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "config.yml"
    path.write_text(contents)
    with pytest.raises(ConfigError):
        loader._read_mapping(path, required=True)
    path.unlink()
    with pytest.raises(ConfigError, match="does not exist"):
        loader._read_mapping(path, required=True)


@pytest.mark.parametrize("section,key", [("cache", "root"), ("local", "database")])
def test_configuration_rejects_workspace_root_as_artifact_path(tmp_path, section, key) -> None:
    config = DbtvConfig.model_validate({section: {key: str(tmp_path)}}).resolve_paths(tmp_path)
    with pytest.raises(ConfigError, match="Unsafe"):
        loader._validate_workspace_paths(config, tmp_path)
    with pytest.raises(ConfigError, match="forbidden"):
        loader._reject_secrets({"plugin": [{"password": "canary"}]})


@pytest.mark.parametrize("raw", [None, [], {}, {"profile": {}}, {"profile": {"outputs": {}}}])
def test_credentials_reject_missing_profile_structure(tmp_path: Path, raw) -> None:
    (tmp_path / "profiles.yml").write_text(json.dumps(raw))
    with pytest.raises(CredentialError):
        credentials.DbtProfileCredentialResolver().resolve(
            profiles_dir=tmp_path,
            profile_name="profile",
            target_name="dev",
            env={},
            interactive=True,
        )


@pytest.mark.parametrize(
    "target",
    [{"type": "duckdb"}, {"type": "snowflake", "account": "example"}],
)
def test_credentials_reject_wrong_adapter_and_missing_required_fields(tmp_path, target) -> None:
    (tmp_path / "profiles.yml").write_text(json.dumps({"profile": {"outputs": {"dev": target}}}))
    with pytest.raises(CredentialError):
        credentials.DbtProfileCredentialResolver().resolve(
            profiles_dir=tmp_path,
            profile_name="profile",
            target_name="dev",
            env={},
            interactive=True,
        )


def test_credentials_missing_file_defaults_and_nested_values(tmp_path: Path) -> None:
    with pytest.raises(CredentialError, match="Unable to read"):
        credentials.DbtProfileCredentialResolver().resolve(
            profiles_dir=tmp_path, profile_name="p", target_name="dev", env={}, interactive=True
        )
    value = {"nested": [True, "{{ env_var('OPTION', 'fallback') }}"]}
    assert credentials._resolve_value(value, {}, "test") == {"nested": [True, "fallback"]}
    with pytest.raises(CredentialError, match="not set"):
        credentials._resolve_value("{{ env_var('REQUIRED') }}", {}, "test")
    params = credentials._connector_parameters(
        {"connect_timeout": 5, "private_key_passphrase": "p"}
    )
    assert params == {"login_timeout": 5, "private_key_passphrase": b"p"}
    assert credentials._connector_parameters({"private_key_path": "key.pem"}) == {
        "private_key_file": "key.pem"
    }
    handle = credentials.ResolvedCredentialHandle("snowflake", {"password": "canary"})
    assert "canary" not in str(handle)
    with pytest.raises(TypeError, match="serialized"):
        pickle.dumps(handle)


def test_installed_credential_resolver_loading(monkeypatch) -> None:
    point = SimpleNamespace(
        name="profile", load=lambda: credentials.DbtProfileCredentialResolverFactory
    )
    monkeypatch.setattr(registry, "entry_points", lambda **_: [point])
    resolvers = registry.CredentialResolverRegistry()
    assert resolvers.names() == ("profile",)
    assert resolvers.create("profile").supports("snowflake")
    with pytest.raises(ConfigError, match="No credential resolver"):
        resolvers.create("missing")


@pytest.mark.parametrize("kind", ["workspace", "cache", "free"])
def test_disk_budget_rejects_exhaustion(tmp_path: Path, monkeypatch, kind) -> None:
    config = DbtvConfig().resolve_paths(tmp_path)
    config.cache.root.mkdir(parents=True)
    (config.cache.root / "data").write_bytes(b"12")
    if kind == "workspace":
        config.policy.max_workspace_bytes = "1B"
    elif kind == "cache":
        config.cache.maximum_size = "1B"
    else:
        monkeypatch.setattr("dbtv.resources.shutil.disk_usage", lambda _: SimpleNamespace(free=0))
    with pytest.raises(PolicyError):
        RunBudget(config, tmp_path, CancellationToken()).check_disk()


@pytest.mark.parametrize("error", [PolicyError("budget exceeded"), OSError("disk unavailable")])
def test_disk_monitor_cancels_running_work_and_preserves_failure(
    tmp_path, monkeypatch, error
) -> None:
    token = CancellationToken()
    budget = RunBudget(DbtvConfig().resolve_paths(tmp_path), tmp_path, token)
    monkeypatch.setattr(budget, "check_disk", Mock(side_effect=[None, error]))
    budget.start()
    try:
        assert token._event.wait(3), "Disk monitor did not cancel work"
        assert isinstance(budget.failure, PolicyError)
    finally:
        budget.stop()


def test_disk_accounting_ignores_links_and_disappearing_files(tmp_path, monkeypatch) -> None:
    link = tmp_path / "link"
    link.symlink_to(tmp_path)
    assert _size(link) == 0
    budget = RunBudget(DbtvConfig().resolve_paths(tmp_path), tmp_path, CancellationToken())
    budget.check_disk()
    budget.stop()
    missing_file = Mock(spec=Path)
    missing_file.is_symlink.return_value = False
    missing_file.is_file.return_value = True
    missing_file.stat.side_effect = FileNotFoundError()
    assert _size(missing_file) == 0
    missing_file.is_file.return_value = False
    missing_file.is_dir.return_value = True
    missing_file.iterdir.side_effect = FileNotFoundError()
    assert _size(missing_file) == 0
