from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any, Protocol, cast

from dbtv.core.errors import ConfigError
from dbtv.core.models import CredentialResolver


class CredentialResolverFactory(Protocol):
    def create(self, **kwargs: Any) -> CredentialResolver: ...


class CredentialResolverRegistry:
    def __init__(
        self,
        *,
        factories: dict[str, CredentialResolverFactory] | None = None,
    ) -> None:
        self.factories = factories or {}

    def names(self) -> tuple[str, ...]:
        points = entry_points(group="dbtv.credential_resolvers")
        return tuple(sorted({*(point.name for point in points), *self.factories}))

    def create(self, name: str, **kwargs: Any) -> CredentialResolver:
        if name in self.factories:
            return self.factories[name].create(**kwargs)
        matches = [
            point for point in entry_points(group="dbtv.credential_resolvers") if point.name == name
        ]
        if not matches:
            raise ConfigError(f"No credential resolver named {name!r} is installed.")
        factory_type = cast(type[CredentialResolverFactory], matches[0].load())
        return factory_type().create(**kwargs)
