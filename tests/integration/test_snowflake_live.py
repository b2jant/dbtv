from __future__ import annotations

import os
from pathlib import Path

import pytest

from dbtv.config.schema import ExtractionSettings, SourceSessionSettings
from dbtv.connectors.snowflake import SnowflakeConnector
from dbtv.core.cancellation import CancellationToken
from dbtv.core.models import (
    FidelityMode,
    Relation,
    SamplingSpec,
    SamplingStrategy,
    SnapshotRequest,
    SourceRef,
)
from dbtv.credentials.dbt_profile import DbtProfileCredentialResolver

_REQUIRED = (
    "DBTV_TEST_SNOWFLAKE_PROFILES_DIR",
    "DBTV_TEST_SNOWFLAKE_PROFILE",
    "DBTV_TEST_SNOWFLAKE_TARGET",
    "DBTV_TEST_SNOWFLAKE_DATABASE",
    "DBTV_TEST_SNOWFLAKE_SCHEMA",
    "DBTV_TEST_SNOWFLAKE_TABLE",
)

pytestmark = pytest.mark.skipif(
    any(not os.getenv(name) for name in _REQUIRED),
    reason="live read-only Snowflake fixture is not configured",
)


def test_live_read_only_arrow_extraction_is_bounded() -> None:
    credentials = DbtProfileCredentialResolver().resolve(
        profiles_dir=Path(os.environ["DBTV_TEST_SNOWFLAKE_PROFILES_DIR"]),
        profile_name=os.environ["DBTV_TEST_SNOWFLAKE_PROFILE"],
        target_name=os.environ["DBTV_TEST_SNOWFLAKE_TARGET"],
        env=os.environ,
        interactive=False,
    )
    connector = SnowflakeConnector(
        credentials,
        session=SourceSessionSettings(
            query_tag_prefix="dbtv/live-contract",
            statement_timeout_seconds=120,
        ),
        extraction=ExtractionSettings(
            parallel_sources=1,
            arrow_batch_rows=1_000,
            max_retries=1,
        ),
    )
    request = SnapshotRequest(
        SourceRef("source.dbtv_contract.fixture.table", "dbtv_contract", "fixture", "table"),
        Relation(
            os.environ["DBTV_TEST_SNOWFLAKE_DATABASE"],
            os.environ["DBTV_TEST_SNOWFLAKE_SCHEMA"],
            os.environ["DBTV_TEST_SNOWFLAKE_TABLE"],
        ),
        SamplingSpec(SamplingStrategy.LIMIT, limit=10),
        FidelityMode.STRICT,
        query_tag="dbtv/live-contract/limit-10",
    )
    try:
        batches = list(connector.extract(request, CancellationToken()))
    finally:
        connector.close()
    assert batches
    assert sum(batch.data.num_rows for batch in batches) <= 10
    assert all(batch.query_id for batch in batches)
