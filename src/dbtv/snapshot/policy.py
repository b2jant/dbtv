from __future__ import annotations

import fnmatch
from dataclasses import replace

from dbtv.config.schema import DataProfile, PolicySettings, SamplingRule
from dbtv.core.errors import PolicyError
from dbtv.core.models import FidelityMode, SamplingSpec, SamplingStrategy, SourceRef


def resolve_sampling(profile: DataProfile, source: SourceRef) -> SamplingSpec:
    selected: SamplingRule = profile.default
    candidates = (
        f"source:{source.source_name}.{source.table_name}",
        f"source:{source.package_name}.{source.source_name}.{source.table_name}",
        source.unique_id,
    )
    for rule in profile.sources:
        if any(fnmatch.fnmatchcase(candidate, rule.select) for candidate in candidates):
            selected = rule
    return SamplingSpec(
        strategy=SamplingStrategy(selected.strategy),
        limit=selected.limit,
        where=selected.where,
        key=selected.key,
        rate=selected.rate,
        seed=selected.seed,
    )


def apply_max_rows(sampling: SamplingSpec, maximum: int | None) -> SamplingSpec:
    if maximum is None:
        return sampling
    if sampling.strategy is SamplingStrategy.FULL:
        return replace(sampling, strategy=SamplingStrategy.LIMIT, limit=maximum)
    if sampling.strategy is SamplingStrategy.WHERE:
        return replace(sampling, strategy=SamplingStrategy.WHERE_LIMIT, limit=maximum)
    if sampling.limit is not None:
        return replace(sampling, limit=min(sampling.limit, maximum))
    return sampling


class LocalPolicyEngine:
    def __init__(self, settings: PolicySettings, *, allow_full_source: bool = False) -> None:
        self.settings = settings
        self.allow_full_source = allow_full_source

    def evaluate_sampling(
        self,
        source: SourceRef,
        sampling: SamplingSpec,
        *,
        tags: tuple[str, ...] = (),
        fidelity: FidelityMode | None = None,
    ) -> None:
        denied = sorted(set(tags) & set(self.settings.deny_source_tags))
        if denied:
            raise PolicyError(
                f"Local storage is denied for {source.unique_id} by source tag(s): "
                f"{', '.join(denied)}."
            )
        restricted = sorted(set(tags) & set(self.settings.require_explicit_where_for_tags))
        if restricted and sampling.strategy not in {
            SamplingStrategy.WHERE,
            SamplingStrategy.WHERE_LIMIT,
        }:
            raise PolicyError(
                f"Source {source.unique_id} requires an explicit where predicate due to "
                f"tag(s): {', '.join(restricted)}."
            )
        if (
            fidelity is not None
            and self.settings.require_fidelity is not None
            and fidelity.value != self.settings.require_fidelity
        ):
            raise PolicyError(
                f"Policy requires {self.settings.require_fidelity!r} fidelity for local data."
            )
        if sampling.strategy is SamplingStrategy.FULL and not (
            self.settings.allow_full_source or self.allow_full_source
        ):
            raise PolicyError(
                f"Full extraction is not allowed for {source.unique_id}.",
                hint="Choose a bounded data profile or pass --allow-full-source with approval.",
            )
        if sampling.limit and sampling.limit > self.settings.max_rows_per_source:
            raise PolicyError(
                f"Requested limit {sampling.limit:,} exceeds the per-source policy cap "
                f"of {self.settings.max_rows_per_source:,} for {source.unique_id}."
            )
