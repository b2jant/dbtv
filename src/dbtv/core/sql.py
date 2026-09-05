from __future__ import annotations

import re

from dbtv.core.errors import ConfigError, PolicyError
from dbtv.core.models import Relation

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
    # Data and quoted identifiers can legitimately contain SQL keywords. Only
    # inspect executable text; doubled quotes remain inside their quoted value.
    unquoted = re.sub(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"", "?", candidate)
    if not candidate or _FORBIDDEN_PREDICATE.search(unquoted) or "'" in unquoted or '"' in unquoted:
        raise PolicyError(
            "Sampling predicate must be one read-only SQL expression.",
            hint="Remove statements, comments, and query-level keywords from the predicate.",
        )
    return candidate
