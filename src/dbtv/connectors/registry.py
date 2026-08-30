from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any

from dbtv.core.errors import ConfigError, OfflineViolation


class ConnectorRegistry:
    def __init__(self, *, offline: bool = False) -> None:
        self.offline = offline

    def names(self) -> tuple[str, ...]:
        points = entry_points(group="dbtv.source_connectors")
        return tuple(sorted(point.name for point in points))

    def create(self, name: str, **kwargs: Any) -> object:
        if self.offline:
            raise OfflineViolation("Source connector creation is forbidden in offline mode.")
        matches = [
            point for point in entry_points(group="dbtv.source_connectors") if point.name == name
        ]
        if not matches:
            raise ConfigError(f"No source connector named {name!r} is installed.")
        factory = matches[0].load()
        return factory().create(**kwargs)

