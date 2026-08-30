from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProjectSettings(StrictModel):
    dbt_executable: str = "dbt"
    production_target: str | None = None
    partial_parse: bool = True


class SourceSessionSettings(StrictModel):
    query_tag_prefix: str = "dbtv"
    statement_timeout_seconds: int = Field(default=900, gt=0)
    login_timeout_seconds: int = Field(default=60, gt=0)
    network_timeout_seconds: int = Field(default=300, gt=0)


class ExtractionSettings(StrictModel):
    parallel_sources: int = Field(default=2, ge=1, le=16)
    arrow_batch_rows: int = Field(default=100_000, ge=1_000)
    max_retries: int = Field(default=3, ge=0, le=10)


class SourceSettings(StrictModel):
    connector: str = "snowflake"
    session: SourceSessionSettings = Field(default_factory=SourceSessionSettings)
    extraction: ExtractionSettings = Field(default_factory=ExtractionSettings)


class LocalSettings(StrictModel):
    backend: Literal["duckdb"] = "duckdb"
    database: Path = Path(".dbtv/local.duckdb")
    schema_name: str = Field(default="dbtv_dev", alias="schema")
    threads: int = Field(default_factory=lambda: min(4, os.cpu_count() or 1), ge=1)
    memory_limit: str = "8GB"
    temp_directory: Path = Path(".dbtv/tmp")


class CacheSettings(StrictModel):
    provider: Literal["parquet"] = "parquet"
    root: Path = Path(".dbtv/cache")
    default_ttl: str = "24h"
    compression: str = "zstd"
    maximum_size: str = "100GB"


class SamplingRule(StrictModel):
    strategy: Literal["full", "limit", "where", "where_limit", "hash", "bernoulli"]
    limit: int | None = Field(default=None, gt=0)
    where: str | None = None
    key: str | None = None
    rate: float | None = Field(default=None, gt=0, le=1)
    seed: int | None = None


class DataProfile(StrictModel):
    default: SamplingRule


class CompatibilitySettings(StrictModel):
    mode: Literal["strict", "warn", "lossy"] = "strict"
    install_shims: bool = True


class PolicySettings(StrictModel):
    allow_full_source: bool = False
    max_rows_per_source: int = Field(default=5_000_000, gt=0)
    max_estimated_bytes_per_run: str = "20GB"


class DbtvConfig(StrictModel):
    version: Literal[1] = 1
    project: ProjectSettings = Field(default_factory=ProjectSettings)
    source: SourceSettings = Field(default_factory=SourceSettings)
    local: LocalSettings = Field(default_factory=LocalSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    data_profiles: dict[str, DataProfile] = Field(
        default_factory=lambda: {
            "developer": DataProfile(
                default=SamplingRule(strategy="limit", limit=100_000)
            )
        }
    )
    default_data_profile: str = "developer"
    compatibility: CompatibilitySettings = Field(default_factory=CompatibilitySettings)
    policy: PolicySettings = Field(default_factory=PolicySettings)

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


def _resolve(project_dir: Path, path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (project_dir / path).resolve()
