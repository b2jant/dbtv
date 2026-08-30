from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dbtv.core.units import parse_duration, parse_size


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProjectSettings(StrictModel):
    dbt_executable: str = "dbt"
    profile: str | None = None
    production_target: str | None = None
    partial_parse: bool = True


class SourceSessionSettings(StrictModel):
    query_tag_prefix: str = "dbtv"
    statement_timeout_seconds: int = Field(default=900, gt=0)
    login_timeout_seconds: int = Field(default=60, gt=0)
    network_timeout_seconds: int = Field(default=300, gt=0)
    timezone: str = "UTC"


class ExtractionSettings(StrictModel):
    parallel_sources: int = Field(default=2, ge=1, le=16)
    arrow_batch_rows: int = Field(default=100_000, ge=1_000)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_base_seconds: float = Field(default=1.0, gt=0, le=60)


class SourceSettings(StrictModel):
    connector: str = "snowflake"
    credential_resolver: str = "dbt_profile"
    session: SourceSessionSettings = Field(default_factory=SourceSessionSettings)
    extraction: ExtractionSettings = Field(default_factory=ExtractionSettings)
    plugin: dict[str, object] = Field(default_factory=dict)


class LocalSettings(StrictModel):
    backend: Literal["duckdb"] = "duckdb"
    database: Path = Path(".dbtv/local.duckdb")
    schema_name: str = Field(default="dbtv_dev", alias="schema")
    threads: int = Field(default_factory=lambda: min(4, os.cpu_count() or 1), ge=1)
    memory_limit: str = "8GB"
    temp_directory: Path = Path(".dbtv/tmp")
    preserve_identifier_case: bool = True

    @field_validator("memory_limit")
    @classmethod
    def validate_memory_limit(cls, value: str) -> str:
        parse_size(value)
        return value


class CacheSettings(StrictModel):
    provider: Literal["parquet"] = "parquet"
    root: Path = Path(".dbtv/cache")
    default_ttl: str = "24h"
    compression: Literal["zstd", "snappy", "gzip", "brotli", "none"] = "zstd"
    row_group_target_bytes: int = Field(default=134_217_728, ge=1_048_576)
    maximum_size: str = "100GB"
    retain_previous_snapshots: int = Field(default=2, ge=0, le=100)
    integrity: Literal["metadata", "metadata_and_sizes", "checksums"] = "metadata_and_sizes"

    @field_validator("default_ttl")
    @classmethod
    def validate_default_ttl(cls, value: str) -> str:
        parse_duration(value)
        return value

    @field_validator("maximum_size")
    @classmethod
    def validate_maximum_size(cls, value: str) -> str:
        parse_size(value)
        return value


class SamplingRule(StrictModel):
    strategy: Literal["full", "limit", "where", "where_limit", "hash", "bernoulli"]
    limit: int | None = Field(default=None, gt=0)
    where: str | None = None
    key: str | None = None
    rate: float | None = Field(default=None, gt=0, le=1)
    seed: int | None = None
    deterministic: bool = False

    @model_validator(mode="after")
    def validate_strategy_fields(self) -> SamplingRule:
        if self.strategy in {"limit", "where_limit"} and self.limit is None:
            raise ValueError(f"strategy {self.strategy!r} requires limit")
        if self.strategy in {"where", "where_limit"} and not self.where:
            raise ValueError(f"strategy {self.strategy!r} requires where")
        if self.strategy == "hash" and (not self.key or self.rate is None):
            raise ValueError("strategy 'hash' requires key and rate")
        if self.strategy == "bernoulli" and self.rate is None:
            raise ValueError("strategy 'bernoulli' requires rate")
        if self.where and re.search(r";|--|/\*|\*/", self.where):
            raise ValueError("where must be one expression without comments or semicolons")
        return self


class SourceSamplingRule(SamplingRule):
    select: str


class DataProfile(StrictModel):
    default: SamplingRule
    sources: list[SourceSamplingRule] = Field(default_factory=list)


class CompatibilitySettings(StrictModel):
    mode: Literal["strict", "warn", "lossy"] = "strict"
    install_shims: bool = True
    allow_rules: list[str] = Field(default_factory=list)
    warn_rules: list[str] = Field(default_factory=list)
    deny_rules: list[str] = Field(default_factory=list)


class PolicySettings(StrictModel):
    allow_full_source: bool = False
    max_rows_per_source: int = Field(default=5_000_000, gt=0)
    max_estimated_bytes_per_run: str = "20GB"
    require_explicit_where_for_tags: list[str] = Field(default_factory=list)
    deny_source_tags: list[str] = Field(default_factory=list)
    max_cache_age_for_tags: dict[str, str] = Field(default_factory=dict)
    require_fidelity: Literal["strict", "warn", "lossy"] | None = None
    cache_file_mode: str = "0600"
    cache_directory_mode: str = "0700"

    @field_validator("max_estimated_bytes_per_run")
    @classmethod
    def validate_max_estimated_bytes(cls, value: str) -> str:
        parse_size(value)
        return value

    @field_validator("max_cache_age_for_tags")
    @classmethod
    def validate_cache_ages(cls, value: dict[str, str]) -> dict[str, str]:
        for duration in value.values():
            parse_duration(duration)
        return value

    @field_validator("cache_file_mode", "cache_directory_mode")
    @classmethod
    def validate_modes(cls, value: str) -> str:
        if not re.fullmatch(r"0[0-7]{3}", value):
            raise ValueError("file modes must be four-digit octal strings")
        return value


class ReportingSettings(StrictModel):
    show_query_text: bool = False
    show_relation_names: bool = True
    telemetry: Literal["disabled"] = "disabled"


class DbtvConfig(StrictModel):
    version: Literal[1] = 1
    project: ProjectSettings = Field(default_factory=ProjectSettings)
    source: SourceSettings = Field(default_factory=SourceSettings)
    local: LocalSettings = Field(default_factory=LocalSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    data_profiles: dict[str, DataProfile] = Field(
        default_factory=lambda: {
            "developer": DataProfile(default=SamplingRule(strategy="limit", limit=100_000))
        }
    )
    default_data_profile: str = "developer"
    compatibility: CompatibilitySettings = Field(default_factory=CompatibilitySettings)
    policy: PolicySettings = Field(default_factory=PolicySettings)
    reporting: ReportingSettings = Field(default_factory=ReportingSettings)

    @field_validator("default_data_profile")
    @classmethod
    def validate_default_profile(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("default_data_profile cannot be empty")
        return value

    def resolve_paths(self, project_dir: Path) -> DbtvConfig:
        update: dict[str, object] = {}
        local = self.local.model_copy(
            update={
                "database": _resolve(project_dir, self.local.database),
                "temp_directory": _resolve(project_dir, self.local.temp_directory),
            }
        )
        cache = self.cache.model_copy(update={"root": _resolve(project_dir, self.cache.root)})
        update["local"] = local
        update["cache"] = cache
        return self.model_copy(update=update)

    @model_validator(mode="after")
    def validate_profile_reference(self) -> DbtvConfig:
        if self.default_data_profile not in self.data_profiles:
            raise ValueError(f"default_data_profile {self.default_data_profile!r} is not defined")
        return self


def _resolve(project_dir: Path, path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (project_dir / path).resolve()
