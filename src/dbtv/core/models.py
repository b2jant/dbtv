from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from dbtv.core.cancellation import CancellationToken


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
    raw_code: str | None = None
    tags: tuple[str, ...] = ()
    meta: Mapping[str, Any] = field(default_factory=dict)

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
    tags: tuple[str, ...] = ()
    meta: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CompatibilityFinding:
    rule_id: str
    severity: str
    message: str
    node_id: str | None = None
    path: str | None = None
    remediation: str | None = None
    may_continue: bool = True


class SamplingStrategy(StrEnum):
    FULL = "full"
    LIMIT = "limit"
    WHERE = "where"
    WHERE_LIMIT = "where_limit"
    HASH = "hash"
    BERNOULLI = "bernoulli"


class FidelityMode(StrEnum):
    STRICT = "strict"
    WARN = "warn"
    LOSSY = "lossy"


class SourceMode(StrEnum):
    AUTO = "auto"
    REFRESH = "refresh"
    CACHED = "cached"
    OFFLINE = "offline"


@dataclass(frozen=True)
class SamplingSpec:
    strategy: SamplingStrategy
    limit: int | None = None
    where: str | None = None
    key: str | None = None
    rate: float | None = None
    seed: int | None = None


@dataclass(frozen=True)
class CanonicalField:
    name: str
    arrow_type: str
    nullable: bool
    provider_type: str | None = None


@dataclass(frozen=True)
class CanonicalSchema:
    fields: tuple[CanonicalField, ...]
    fingerprint: str


@dataclass(frozen=True)
class SourceVersion:
    value: str
    observed_at: str


@dataclass(frozen=True)
class ExtractionEstimate:
    row_count: int | None = None
    byte_count: int | None = None


@dataclass(frozen=True)
class SnapshotRequest:
    source: SourceRef
    relation: Relation
    sampling: SamplingSpec
    fidelity: FidelityMode
    projection: tuple[str, ...] | None = None
    query_tag: str | None = None
    connection_scope: str = ""

    def identity_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExtractionBatch:
    data: Any
    query_id: str | None = None


@dataclass(frozen=True)
class SnapshotFile:
    path: str
    size: int
    checksum: str | None = None


@dataclass(frozen=True)
class DatasetSnapshot:
    snapshot_key: str
    request_fingerprint: str
    content_fingerprint: str
    source: SourceRef
    provider: str
    remote_relation: Relation
    sampling: SamplingSpec
    fidelity: FidelityMode
    projection: tuple[str, ...] | None
    schema: CanonicalSchema
    source_version: SourceVersion | None
    root: Path
    files: tuple[SnapshotFile, ...]
    created_at: str
    completed_at: str
    expires_at: str | None
    row_count: int
    byte_count: int
    query_id: str | None = None
    query_tag: str | None = None
    provider_version: str | None = None
    pinned: bool = False
    connection_scope: str = ""
    verified_at: str | None = None

    @property
    def parquet_paths(self) -> tuple[Path, ...]:
        return tuple(self.root / item.path for item in self.files)


class SnapshotAction(StrEnum):
    REUSE = "reuse"
    REFRESH = "refresh"
    MISSING = "missing"


@dataclass(frozen=True)
class SnapshotDecision:
    request: SnapshotRequest
    action: SnapshotAction
    reason: str
    snapshot: DatasetSnapshot | None = None


@dataclass(frozen=True)
class SourceBinding:
    mapping: SourceMapping
    snapshot: DatasetSnapshot


@dataclass(frozen=True)
class BindingResult:
    source_unique_id: str
    relation: str
    catalog_path: str
    verified: bool
    row_count: int | None = None


@dataclass(frozen=True)
class BindingReport:
    results: tuple[BindingResult, ...]
    attachments: Mapping[str, Path]


@dataclass(frozen=True)
class DbtExecutionRequest:
    command: str
    project_dir: Path
    profiles_dir: Path
    target_path: Path
    target: str
    select: tuple[str, ...]
    exclude: tuple[str, ...]
    variables: str | None = None
    full_refresh: bool = False


@dataclass(frozen=True)
class DbtExecutionResult:
    command: tuple[str, ...]
    return_code: int
    stdout: str
    stderr: str
    run_results_path: Path | None


@dataclass(frozen=True)
class DbtNodeResult:
    unique_id: str
    status: str
    message: str | None
    execution_time: float
    failures: int | None = None


@dataclass(frozen=True)
class RunSummary:
    invocation_id: str
    command: str
    state: str
    exit_code: int
    remote_connection_attempted: bool
    remote_query_count: int
    snapshots_reused: int
    snapshots_refreshed: int
    local_database: Path
    dbt_target: str
    run_artifact_dir: Path
    started_at: str
    completed_at: str
    timings: Mapping[str, float]
    warnings: tuple[str, ...] = ()
    dbt_results: tuple[DbtNodeResult, ...] = ()
    state_transitions: tuple[Mapping[str, str], ...] = ()
    dataset_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["local_database"] = str(self.local_database)
        value["run_artifact_dir"] = str(self.run_artifact_dir)
        return value


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
    project_fingerprint: str
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
    remote_access: bool = True


@runtime_checkable
class CredentialResolver(Protocol):
    def supports(self, profile_type: str) -> bool: ...

    def resolve(
        self,
        *,
        profiles_dir: Path,
        profile_name: str,
        target_name: str,
        env: Mapping[str, str],
        interactive: bool,
    ) -> object: ...


@runtime_checkable
class SourceConnector(Protocol):
    def capabilities(self) -> SourceCapabilities: ...

    def version(self) -> str: ...

    def open(self) -> None: ...

    def close(self) -> None: ...

    def inspect_schema(self, relation: Relation) -> CanonicalSchema: ...

    def source_version(self, relation: Relation) -> SourceVersion | None: ...

    def estimate(self, request: SnapshotRequest) -> ExtractionEstimate | None: ...

    def extract(
        self,
        request: SnapshotRequest,
        cancellation: CancellationToken,
    ) -> Iterable[ExtractionBatch]: ...

    def cancel(self, query_id: str) -> None: ...


@runtime_checkable
class SnapshotStore(Protocol):
    def lookup(self, key: str) -> DatasetSnapshot | None: ...

    def decide(
        self,
        request: SnapshotRequest,
        *,
        provider: str,
        mode: str,
        snapshot_id: str | None = None,
    ) -> SnapshotDecision: ...

    def validate(self, snapshot: DatasetSnapshot) -> None: ...

    def activate(self, snapshot: DatasetSnapshot) -> None: ...


@runtime_checkable
class PolicyEngine(Protocol):
    def evaluate_sampling(
        self,
        source: SourceRef,
        sampling: SamplingSpec,
        *,
        tags: tuple[str, ...] = (),
        fidelity: FidelityMode | None = None,
    ) -> None: ...


@runtime_checkable
class LocalExecutionBackend(Protocol):
    def bind(self, bindings: list[SourceBinding], invocation_id: str) -> BindingReport: ...

    def verify(
        self,
        report: BindingReport,
        bindings: list[SourceBinding],
    ) -> BindingReport: ...


def utc_now() -> str:
    return datetime.now(UTC).isoformat()
