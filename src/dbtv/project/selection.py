from __future__ import annotations

import json
from collections.abc import Iterable

from dbtv.core.errors import DbtInvocationError
from dbtv.core.models import NormalizedManifest, ResourceType


def parse_dbt_ls_json(output: str) -> tuple[str, ...]:
    selected: set[str] = set()
    malformed = 0
    for line in output.splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            malformed += 1
            continue
        unique_id = value.get("unique_id") if isinstance(value, dict) else None
        if isinstance(unique_id, str):
            selected.add(unique_id)
    if not selected and malformed:
        raise DbtInvocationError("dbt ls returned malformed JSON output.")
    return tuple(sorted(selected))


def upstream_sources(
    manifest: NormalizedManifest,
    selected_ids: Iterable[str],
) -> tuple[str, ...]:
    sources: set[str] = set()
    visited: set[str] = set()
    pending = list(selected_ids)
    while pending:
        unique_id = pending.pop()
        if unique_id in visited:
            continue
        visited.add(unique_id)
        node = manifest.get(unique_id)
        if node is None:
            continue
        if node.resource_type is ResourceType.SOURCE:
            sources.add(unique_id)
            continue
        pending.extend(reversed(node.depends_on_nodes))
    return tuple(sorted(sources))
