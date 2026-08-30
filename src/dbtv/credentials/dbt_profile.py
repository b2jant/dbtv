from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NoReturn

import yaml

from dbtv.core.errors import CredentialError

_ENV_VAR = re.compile(
    r"^\s*\{\{\s*env_var\(\s*(?P<q>['\"])(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    r"(?P=q)(?:\s*,\s*(?P<dq>['\"])(?P<default>.*?)(?P=dq))?\s*\)\s*\}\}\s*$"
)

_SNOWFLAKE_FIELDS = {
    "account",
    "user",
    "password",
    "authenticator",
    "token",
    "private_key",
    "private_key_path",
    "private_key_passphrase",
    "role",
    "database",
    "warehouse",
    "schema",
    "client_session_keep_alive",
    "connect_retries",
    "connect_timeout",
    "retry_on_database_errors",
    "reuse_connections",
    "query_tag",
}


class ResolvedCredentialHandle:
    """Non-serializable in-memory wrapper whose text forms cannot expose secrets."""

    __slots__ = ("_parameters", "profile_type")

    def __init__(self, profile_type: str, parameters: Mapping[str, Any]) -> None:
        self.profile_type = profile_type
        self._parameters = dict(parameters)

    def __repr__(self) -> str:
        return f"ResolvedCredentialHandle(profile_type={self.profile_type!r}, <redacted>)"

    def __str__(self) -> str:
        return "<redacted credential handle>"

    def parameters(self) -> dict[str, Any]:
        return dict(self._parameters)

    def __reduce__(self) -> NoReturn:
        raise TypeError("Resolved credential handles cannot be serialized")


class DbtProfileCredentialResolver:
    def supports(self, profile_type: str) -> bool:
        return profile_type.lower() == "snowflake"

    def resolve(
        self,
        *,
        profiles_dir: Path,
        profile_name: str,
        target_name: str,
        env: Mapping[str, str],
        interactive: bool,
    ) -> ResolvedCredentialHandle:
        path = profiles_dir / "profiles.yml"
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise CredentialError(f"Unable to read dbt profile at {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise CredentialError(f"dbt profile at {path} must contain a YAML mapping.")
        profile = raw.get(profile_name)
        if not isinstance(profile, dict):
            raise CredentialError(f"dbt profile {profile_name!r} was not found in {path}.")
        outputs = profile.get("outputs")
        if not isinstance(outputs, dict) or not isinstance(outputs.get(target_name), dict):
            raise CredentialError(
                f"dbt target {target_name!r} was not found in profile {profile_name!r}."
            )
        target = outputs[target_name]
        resolved = {
            str(key): _resolve_value(value, env, f"{profile_name}.outputs.{target_name}.{key}")
            for key, value in target.items()
        }
        profile_type = str(resolved.get("type", "")).lower()
        if not self.supports(profile_type):
            raise CredentialError(
                f"Production target type {profile_type!r} is not supported by this resolver."
            )
        authenticator = str(resolved.get("authenticator", "snowflake")).lower()
        if not interactive and authenticator in {"externalbrowser", "oauth_authorization_code"}:
            raise CredentialError(
                f"Authenticator {authenticator!r} requires an interactive login.",
                hint="Use an approved non-interactive authenticator or remove --non-interactive.",
            )
        parameters = _connector_parameters(resolved)
        for required in ("account", "user"):
            if not parameters.get(required):
                raise CredentialError(
                    f"Snowflake profile field {required!r} is required for target {target_name!r}."
                )
        return ResolvedCredentialHandle(profile_type, parameters)


class DbtProfileCredentialResolverFactory:
    def create(self, **_: Any) -> DbtProfileCredentialResolver:
        return DbtProfileCredentialResolver()


def _resolve_value(value: Any, env: Mapping[str, str], field_path: str) -> Any:
    if isinstance(value, dict):
        return {
            key: _resolve_value(child, env, f"{field_path}.{key}") for key, child in value.items()
        }
    if isinstance(value, list):
        return [
            _resolve_value(child, env, f"{field_path}.{index}") for index, child in enumerate(value)
        ]
    if not isinstance(value, str):
        return value
    match = _ENV_VAR.fullmatch(value)
    if match:
        name = match.group("name")
        if name in env:
            return env[name]
        default = match.group("default")
        if default is not None:
            return default
        raise CredentialError(f"Environment variable {name!r} required by {field_path} is not set.")
    if "{{" in value or "{%" in value or "{#" in value:
        raise CredentialError(
            f"Unsupported Jinja expression in credential field {field_path}.",
            hint="Use a literal scalar or a complete {{ env_var('NAME') }} expression.",
        )
    return value


def _connector_parameters(resolved: Mapping[str, Any]) -> dict[str, Any]:
    parameters = {
        key: value
        for key, value in resolved.items()
        if key in _SNOWFLAKE_FIELDS
        and key
        not in {
            "connect_retries",
            "retry_on_database_errors",
            "reuse_connections",
            "query_tag",
        }
    }
    if "connect_timeout" in parameters:
        parameters["login_timeout"] = parameters.pop("connect_timeout")
    if "private_key_path" in parameters:
        parameters["private_key_file"] = parameters.pop("private_key_path")
        if "private_key_passphrase" in parameters:
            parameters["private_key_file_pwd"] = parameters.pop("private_key_passphrase")
    elif isinstance(parameters.get("private_key_passphrase"), str):
        parameters["private_key_passphrase"] = parameters["private_key_passphrase"].encode()
    return parameters
