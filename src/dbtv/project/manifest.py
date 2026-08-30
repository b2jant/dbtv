from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dbtv.core.errors import ManifestError
from dbtv.core.models import (
    NormalizedManifest,
    NormalizedNode,
    Relation,
    ResourceType,
)

SUPPORTED_MANIFEST_VERSIONS = {"v9", "v10", "v11", "v12"}


def load_manifest(path: Path) -> NormalizedManifest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"Unable to read dbt manifest at {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError(f"Manifest at {path} is not a JSON object.")
    return normalize_manifest(raw)


def normalize_manifest(raw: Mapping[str, Any]) -> NormalizedManifest:
    metadata = _mapping(raw.get("metadata"))
    schema_url = str(metadata.get("dbt_schema_version") or "")
    version = schema_url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".json")
    if version not in SUPPORTED_MANIFEST_VERSIONS:
        raise ManifestError(
            f"Unsupported dbt manifest schema {schema_url or '<missing>'}.",
            hint=f"Supported schemas: {', '.join(sorted(SUPPORTED_MANIFEST_VERSIONS))}.",
        )

    combined: dict[str, NormalizedNode] = {}
    for collection_name in ("nodes", "sources"):
        collection = _mapping(raw.get(collection_name))
        for unique_id, node_raw in collection.items():
            if not isinstance(node_raw, Mapping):
                continue
            combined[str(unique_id)] = _normalize_node(str(unique_id), node_raw)

    return NormalizedManifest(
        schema_url=schema_url,
        dbt_version=str(metadata.get("dbt_version") or "unknown"),
        generated_at=_optional_string(metadata.get("generated_at")),
        nodes=combined,
    )


def _normalize_node(unique_id: str, raw: Mapping[str, Any]) -> NormalizedNode:
    resource_text = str(raw.get("resource_type") or "unknown")
    try:
        resource_type = ResourceType(resource_text)
    except ValueError:
        resource_type = ResourceType.UNKNOWN

    depends_on = _mapping(raw.get("depends_on"))
    dependencies = depends_on.get("nodes") or []
    relation = _relation(raw, resource_type)
    return NormalizedNode(
        unique_id=unique_id,
        name=str(raw.get("name") or unique_id.rsplit(".", 1)[-1]),
        resource_type=resource_type,
        package_name=str(raw.get("package_name") or ""),
        path=_optional_string(raw.get("original_file_path") or raw.get("path")),
        depends_on_nodes=tuple(str(item) for item in dependencies),
        relation=relation,
        config=dict(_mapping(raw.get("config"))),
        source_name=_optional_string(raw.get("source_name")),
    )


def _relation(raw: Mapping[str, Any], resource_type: ResourceType) -> Relation | None:
    if resource_type not in {ResourceType.SOURCE, ResourceType.MODEL, ResourceType.SEED}:
        return None
    schema = raw.get("schema")
    if schema is None:
        return None
    identifier = raw.get("identifier") or raw.get("alias") or raw.get("name")
    if identifier is None:
        return None
    quoting = _mapping(raw.get("quoting"))
    return Relation(
        catalog=_optional_string(raw.get("database")),
        schema=str(schema),
        identifier=str(identifier),
        quoting={str(k): bool(v) for k, v in quoting.items() if isinstance(v, bool)},
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_string(value: Any) -> str | None:
    return str(value) if value is not None else None
