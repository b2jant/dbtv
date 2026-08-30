from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from dbtv.core.errors import CredentialError
from dbtv.credentials.dbt_profile import DbtProfileCredentialResolver
from dbtv.credentials.registry import CredentialResolverRegistry


def _profile(path: Path, password: str = "{{ env_var('SF_PASSWORD') }}") -> None:
    path.mkdir()
    (path / "profiles.yml").write_text(
        "analytics:\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        "      account: acme\n"
        "      user: developer\n"
        f'      password: "{password}"\n',
        encoding="utf-8",
    )


def test_profile_resolver_supports_restricted_env_var_and_redacts(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles"
    _profile(profiles)
    handle = DbtProfileCredentialResolver().resolve(
        profiles_dir=profiles,
        profile_name="analytics",
        target_name="dev",
        env={"SF_PASSWORD": "canary-secret"},
        interactive=False,
    )
    assert handle.parameters()["password"] == "canary-secret"
    assert "canary-secret" not in repr(handle)
    assert "canary-secret" not in str(handle)
    with pytest.raises(TypeError):
        pickle.dumps(handle)


def test_profile_resolver_rejects_unrestricted_jinja(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles"
    _profile(profiles, "{{ modules.os.getenv('SF_PASSWORD') }}")
    with pytest.raises(CredentialError, match="Unsupported Jinja"):
        DbtProfileCredentialResolver().resolve(
            profiles_dir=profiles,
            profile_name="analytics",
            target_name="dev",
            env={},
            interactive=False,
        )


def test_noninteractive_rejects_browser_auth(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "analytics:\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        "      account: acme\n"
        "      user: developer\n"
        "      authenticator: externalbrowser\n",
        encoding="utf-8",
    )
    with pytest.raises(CredentialError, match="interactive"):
        DbtProfileCredentialResolver().resolve(
            profiles_dir=profiles,
            profile_name="analytics",
            target_name="dev",
            env={},
            interactive=False,
        )


def test_dbt_private_key_fields_map_to_public_connector_parameters(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "analytics:\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        "      account: acme\n"
        "      user: developer\n"
        "      private_key_path: /secure/key.p8\n"
        "      private_key_passphrase: phrase\n"
        "      connect_retries: 5\n",
        encoding="utf-8",
    )
    handle = DbtProfileCredentialResolver().resolve(
        profiles_dir=profiles,
        profile_name="analytics",
        target_name="dev",
        env={},
        interactive=False,
    )
    parameters = handle.parameters()
    assert parameters["private_key_file"] == "/secure/key.p8"
    assert parameters["private_key_file_pwd"] == "phrase"
    assert "private_key_path" not in parameters
    assert "connect_retries" not in parameters


def test_credential_resolver_registry_accepts_provider_plugins() -> None:
    resolver = DbtProfileCredentialResolver()

    class Factory:
        def create(self, **_: object) -> DbtProfileCredentialResolver:
            return resolver

    registry = CredentialResolverRegistry(factories={"custom": Factory()})

    assert "custom" in registry.names()
    assert registry.create("custom") is resolver
