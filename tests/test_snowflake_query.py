from __future__ import annotations

import pytest

from dbtv.connectors.snowflake.query import quote_identifier, render_select
from dbtv.core.errors import PolicyError
from dbtv.core.models import (
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotRequest,
    SourceRef,
)


def _request(sampling: SamplingSpec) -> SnapshotRequest:
    return SnapshotRequest(
        SourceRef("source.analytics.app.orders", "analytics", "app", "orders"),
        Relation("RAW", "APP", 'odd"name'),
        sampling,
        FidelityMode.STRICT,
    )


def test_identifier_quoting_and_bounded_query() -> None:
    query = render_select(
        _request(SamplingSpec(SamplingStrategy.WHERE_LIMIT, 100, "created_at >= current_date"))
    )
    assert query == (
        'SELECT * FROM "RAW"."APP"."odd""name" WHERE created_at >= current_date LIMIT 100'
    )
    assert quote_identifier('a"b') == '"a""b"'


def test_predicate_cannot_inject_another_statement() -> None:
    with pytest.raises(PolicyError):
        render_select(_request(SamplingSpec(SamplingStrategy.WHERE, where="1=1; DROP TABLE x")))


def test_deterministic_hash_sampling_query() -> None:
    query = render_select(
        _request(SamplingSpec(SamplingStrategy.HASH, key="customer_id", rate=0.01, seed=42))
    )
    assert 'HASH("customer_id", 42)' in query
    assert "< 10000" in query
