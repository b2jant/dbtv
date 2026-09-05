from __future__ import annotations

from dataclasses import asdict
from typing import Any

from dbtv.core.models import NormalizedManifest, ResourceType


def dependency_boundaries(
    manifest: NormalizedManifest,
    selected: tuple[str, ...],
    command: str,
) -> list[dict[str, Any]]:
    scheduled_types = {
        "run": {ResourceType.MODEL},
        "build": {ResourceType.MODEL, ResourceType.SEED, ResourceType.SNAPSHOT, ResourceType.TEST},
        "test": {ResourceType.TEST},
    }.get(command, set())
    scheduled = {
        node_id
        for node_id in selected
        if (node := manifest.get(node_id)) and node.resource_type in scheduled_types
    }
    required: dict[str, dict[str, Any]] = {}
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        visited.add(node_id)
        node = manifest.get(node_id)
        if node is None:
            return
        for parent_id in node.depends_on_nodes:
            parent = manifest.get(parent_id)
            if parent is None or parent.resource_type is ResourceType.SOURCE:
                continue
            if parent_id in scheduled or parent.config.get("materialized") == "ephemeral":
                visit(parent_id)
            elif parent.resource_type in {
                ResourceType.MODEL,
                ResourceType.SEED,
                ResourceType.SNAPSHOT,
            }:
                required[parent_id] = {
                    "unique_id": parent_id,
                    "resource_type": parent.resource_type.value,
                    "relation": asdict(parent.relation) if parent.relation else None,
                    "needed_by": node_id,
                }

    for node_id in scheduled:
        visit(node_id)
    return [required[key] for key in sorted(required)]


def lineage(manifest: NormalizedManifest, selected: tuple[str, ...]) -> dict[str, Any]:
    visited: set[str] = set()
    edges: list[dict[str, str]] = []

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        visited.add(node_id)
        node = manifest.get(node_id)
        if node:
            for parent in node.depends_on_nodes:
                edges.append({"from": parent, "to": node_id})
                visit(parent)

    for node_id in selected:
        visit(node_id)
    return {
        "nodes": [
            {
                "unique_id": item,
                "selected": item in selected,
                "resource_type": manifest.nodes[item].resource_type.value,
                "relation": (
                    asdict(relation) if (relation := manifest.nodes[item].relation) else None
                ),
            }
            for item in sorted(visited)
            if item in manifest.nodes
        ],
        "edges": edges,
    }
