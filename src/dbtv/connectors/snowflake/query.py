from __future__ import annotations

import re

from dbtv.core.errors import ConfigError, PolicyError
from dbtv.core.models import Relation, SamplingStrategy, SnapshotRequest

_IDENTIFIER = re.compile(r"^[^\x00-\x1f]+$")
_FORBIDDEN_PREDICATE = re.compile(
    r"(?:;|--|/\*|\*/|\b(?:select|insert|update|delete|merge|drop|alter|create|grant|revoke|call|copy)\b)",
    re.IGNORECASE,
)


def quote_identifier(value: str) -> str:
    if not value or not _IDENTIFIER.fullmatch(value):
        raise ConfigError("Relation contains an empty or invalid identifier.")
    return '"' + value.replace('"', '""') + '"'


def render_relation(relation: Relation) -> str:
    return ".".join(
        quote_identifier(part)
        for part in (relation.catalog, relation.schema, relation.identifier)
        if part
    )


def validate_predicate(predicate: str) -> str:
    candidate = predicate.strip()
    if not candidate or _FORBIDDEN_PREDICATE.search(candidate):
        raise PolicyError(
            "Sampling predicate must be one read-only Snowflake expression.",
            hint="Remove statements, comments, and query-level keywords from the predicate.",
        )
    return candidate


def render_select(request: SnapshotRequest) -> str:
    relation = render_relation(request.relation)
    projection = (
        ", ".join(quote_identifier(item) for item in request.projection)
        if request.projection
        else "*"
    )
    sampling = request.sampling
    table_sampling = ""
    clauses: list[str] = []
    if sampling.strategy in {SamplingStrategy.WHERE, SamplingStrategy.WHERE_LIMIT}:
        assert sampling.where is not None
        clauses.append(f"WHERE {validate_predicate(sampling.where)}")
    elif sampling.strategy is SamplingStrategy.HASH:
        if not sampling.key or sampling.rate is None:
            raise ConfigError("Hash sampling requires a key and rate.")
        seed = sampling.seed or 0
        threshold = int(sampling.rate * 1_000_000)
        clauses.append(
            f"WHERE MOD(ABS(HASH({quote_identifier(sampling.key)}, {seed})), 1000000) < {threshold}"
        )
    elif sampling.strategy is SamplingStrategy.BERNOULLI:
        if sampling.rate is None:
            raise ConfigError("Bernoulli sampling requires a rate.")
        table_sampling = f" SAMPLE BERNOULLI ({sampling.rate * 100:g})"
        if sampling.seed is not None:
            table_sampling += f" SEED ({sampling.seed})"
    if sampling.strategy in {SamplingStrategy.LIMIT, SamplingStrategy.WHERE_LIMIT}:
        if sampling.limit is None:
            raise ConfigError(f"Sampling strategy {sampling.strategy} requires a limit.")
        clauses.append(f"LIMIT {sampling.limit}")
    suffix = " " + " ".join(clauses) if clauses else ""
    return f"SELECT {projection} FROM {relation}{table_sampling}{suffix}"
