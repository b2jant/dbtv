from __future__ import annotations

from typing import Any

from dbtv.core.errors import NotImplementedMilestone
from dbtv.core.models import SourceCapabilities


class SnowflakeConnectorFactory:
    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            arrow_streaming=True,
            projection_pushdown=True,
            predicate_pushdown=True,
            limit_pushdown=True,
            bernoulli_sampling=True,
            deterministic_sampling=True,
            snapshot_identity=False,
            consistent_snapshot_time=False,
            direct_duckdb_read=False,
            cancellable_queries=True,
        )

    def create(self, **_: Any) -> object:
        raise NotImplementedMilestone("Snowflake extraction")

