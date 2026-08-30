from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


class ResourceType(StrEnum):
    MODEL = "model"
    SOURCE = "source"
    TEST = "test"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    ANALYSIS = "analysis"
    EXPOSURE = "exposure"
    METRIC = "metric"
    SEMANTIC_MODEL = "semantic_model"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SourceRef:
    unique_id: str
    package_name: str
    source_name: str
    table_name: str


@dataclass(frozen=True)
class Relation:
    catalog: str | None
    schema: str
    identifier: str
    quoting: Mapping[str, bool] = field(default_factory=dict)

    @property
    def display_name(self) -> str:
        parts = [self.catalog, self.schema, self.identifier]
        return ".".join(part for part in parts if part)


@dataclass(frozen=True)
class NormalizedNode:
    unique_id: str
    name: str
    resource_type: ResourceType
    package_name: str
    path: str | None
    depends_on_nodes: tuple[str, ...]
    relation: Relation | None
    config: Mapping[str, Any]
    source_name: str | None = None

    @property
    def source_ref(self) -> SourceRef | None:
        if self.resource_type is not ResourceType.SOURCE or self.source_name is None:
            return None
        return SourceRef(
            unique_id=self.unique_id,
            package_name=self.package_name,
            source_name=self.source_name,
            table_name=self.name,
        )


@dataclass(frozen=True)
class NormalizedManifest:
    schema_url: str
    dbt_version: str
    generated_at: str | None
    nodes: Mapping[str, NormalizedNode]

    def get(self, unique_id: str) -> NormalizedNode | None:
        return self.nodes.get(unique_id)


@dataclass(frozen=True)
class SourceMapping:
    source: SourceRef
    production_relation: Relation
    local_relation: Relation


@dataclass(frozen=True)
class CompatibilityFinding:
    rule_id: str
    severity: str
    message: str
    node_id: str | None = None
    path: str | None = None


@dataclass(frozen=True)
class ExecutionPlan:
    invocation_id: str
    project_dir: Path
    project_name: str
    production_target: str
    local_target: str
    selected_ids: tuple[str, ...]
    local_selected_ids: tuple[str, ...]
    source_mappings: tuple[SourceMapping, ...]
    findings: tuple[CompatibilityFinding, ...]
    production_manifest_schema: str
    local_manifest_schema: str
    plan_hash: str

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["project_dir"] = str(self.project_dir)
        return value


@dataclass(frozen=True)
class SourceCapabilities:
    arrow_streaming: bool
    projection_pushdown: bool
    predicate_pushdown: bool
    limit_pushdown: bool
    bernoulli_sampling: bool
    deterministic_sampling: bool
    snapshot_identity: bool
    consistent_snapshot_time: bool
    direct_duckdb_read: bool
    cancellable_queries: bool


@runtime_checkable
class CredentialResolver(Protocol):
    def supports(self, profile_type: str) -> bool: ...


@runtime_checkable
class SourceConnector(Protocol):
    def capabilities(self) -> SourceCapabilities: ...

    def open(self) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class SnapshotStore(Protocol):
    def lookup(self, key: str) -> object | None: ...


@runtime_checkable
class PolicyEngine(Protocol):
    def evaluate(self, plan: ExecutionPlan) -> object: ...


@runtime_checkable
class LocalExecutionBackend(Protocol):
    def prepare(self, plan: ExecutionPlan) -> object: ...
