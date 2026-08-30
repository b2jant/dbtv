from __future__ import annotations

import pytest

from dbtv.config.schema import PolicySettings
from dbtv.core.errors import PolicyError
from dbtv.core.models import FidelityMode, SamplingSpec, SamplingStrategy, SourceRef
from dbtv.snapshot.policy import LocalPolicyEngine


def test_source_tag_policy_fails_before_extraction() -> None:
    source = SourceRef("source.analytics.app.users", "analytics", "app", "users")
    engine = LocalPolicyEngine(
        PolicySettings(
            deny_source_tags=["prohibited_local"],
            require_explicit_where_for_tags=["pii"],
        )
    )
    with pytest.raises(PolicyError, match="denied"):
        engine.evaluate_sampling(
            source,
            SamplingSpec(SamplingStrategy.LIMIT, limit=100),
            tags=("prohibited_local",),
            fidelity=FidelityMode.STRICT,
        )
    with pytest.raises(PolicyError, match="where predicate"):
        engine.evaluate_sampling(
            source,
            SamplingSpec(SamplingStrategy.LIMIT, limit=100),
            tags=("pii",),
            fidelity=FidelityMode.STRICT,
        )


def test_full_source_requires_explicit_consent() -> None:
    source = SourceRef("source.analytics.app.users", "analytics", "app", "users")
    with pytest.raises(PolicyError, match="Full extraction"):
        LocalPolicyEngine(PolicySettings()).evaluate_sampling(
            source,
            SamplingSpec(SamplingStrategy.FULL),
        )
    LocalPolicyEngine(PolicySettings(), allow_full_source=True).evaluate_sampling(
        source, SamplingSpec(SamplingStrategy.FULL)
    )
