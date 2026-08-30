from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any, Protocol, cast

from dbtv.core.errors import ConfigError, OfflineViolation
from dbtv.core.models import SourceCapabilities, SourceConnector


class ConnectorFactory(Protocol):
    def create(self, **kwargs: Any) -> SourceConnector: ...

    def capabilities(self) -> SourceCapabilities: ...

    def dependencies(self) -> tuple[str, ...]: ...

    def dbt_adapter_name(self) -> str | None: ...


class ConnectorRegistry:
    def __init__(
        self,
        *,
        offline: bool = False,
        factories: dict[str, ConnectorFactory] | None = None,
    ) -> None:
        self.offline = offline
        self.factories = factories or {}

    def names(self) -> tuple[str, ...]:
        points = entry_points(group="dbtv.source_connectors")
        return tuple(sorted({*(point.name for point in points), *self.factories}))

    def create(self, name: str, **kwargs: Any) -> SourceConnector:
        if self.offline:
            raise OfflineViolation("Source connector creation is forbidden in offline mode.")
        return self._factory(name).create(**kwargs)

    def capabilities(self, name: str) -> SourceCapabilities:
        return self._factory(name).capabilities()

    def dependencies(self, name: str) -> tuple[str, ...]:
        factory = self._factory(name)
        method = getattr(factory, "dependencies", None)
        return tuple(str(item) for item in method()) if method else ()

    def dbt_adapter_name(self, name: str) -> str | None:
        factory = self._factory(name)
        method = getattr(factory, "dbt_adapter_name", None)
        value = method() if method else None
        return str(value) if value else None

    def _factory(self, name: str) -> ConnectorFactory:
        if name in self.factories:
            return self.factories[name]
        matches = [
            point for point in entry_points(group="dbtv.source_connectors") if point.name == name
        ]
        if not matches:
            raise ConfigError(f"No source connector named {name!r} is installed.")
        factory_type = cast(type[ConnectorFactory], matches[0].load())
        return factory_type()
