from __future__ import annotations

import json
import shutil
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from dbtv.compatibility import CompatibilityAnalyzer
from dbtv.config.schema import DbtvConfig
from dbtv.core.errors import ManifestError
from dbtv.core.hashing import sha256_value
from dbtv.core.models import (
    CompatibilityFinding,
    ExecutionPlan,
    NormalizedManifest,
    SourceMapping,
)
from dbtv.project.dbt_invoker import DbtInvoker
from dbtv.project.discovery import DbtProject
from dbtv.project.fingerprint import project_fingerprint
from dbtv.project.manifest import load_manifest
from dbtv.project.profile import LOCAL_TARGET_NAME, write_local_profile
from dbtv.project.selection import parse_dbt_ls_json, upstream_sources
from dbtv.workspace import RunWorkspace


class ProjectPlanner:
    def __init__(self, invoker: DbtInvoker) -> None:
        self.invoker = invoker

    def build(
        self,
        *,
        project: DbtProject,
        config: DbtvConfig,
        run: RunWorkspace,
        select: Sequence[str],
        exclude: Sequence[str],
        variables: str | None,
    ) -> ExecutionPlan:
        production_target = config.project.production_target
        if not production_target:
            raise ManifestError(
                "No production target is configured.",
                hint="Set project.production_target in dbtv.yml or DBTV_PRODUCTION_TARGET.",
            )

        production_manifest = self._parse(
            project=project,
            profiles_dir=project.profiles_dir,
            target=production_target,
            target_path=run.production_target_path,
            variables=variables,
            log_path=run.root / "logs",
            partial_parse=config.project.partial_parse,
        )
        production_selected = self._list(
            project=project,
            profiles_dir=project.profiles_dir,
            target=production_target,
            select=select,
            exclude=exclude,
            variables=variables,
            log_path=run.root / "logs",
        )

        write_local_profile(run.generated_profiles_dir, project, config)
        local_manifest = self._parse(
            project=project,
            profiles_dir=run.generated_profiles_dir,
            target=LOCAL_TARGET_NAME,
            target_path=run.local_target_path,
            variables=variables,
            log_path=run.root / "logs",
            partial_parse=config.project.partial_parse,
        )
        local_selected = self._list(
            project=project,
            profiles_dir=run.generated_profiles_dir,
            target=LOCAL_TARGET_NAME,
            select=select,
            exclude=exclude,
            variables=variables,
            log_path=run.root / "logs",
        )

        production_sources = set(upstream_sources(production_manifest, production_selected))
        local_sources = set(upstream_sources(local_manifest, local_selected))
        required_sources = sorted(production_sources | local_sources)
        mappings: list[SourceMapping] = []
        findings: list[CompatibilityFinding] = []
        findings.extend(
            CompatibilityAnalyzer(config.compatibility).analyze_manifest(
                local_manifest, local_selected
            )
        )

        if production_sources != local_sources:
            findings.append(
                CompatibilityFinding(
                    rule_id="DBTV-CFG-001",
                    severity="warning",
                    message=(
                        "Production and local targets resolve different upstream source sets: "
                        f"production-only={sorted(production_sources - local_sources)}, "
                        f"local-only={sorted(local_sources - production_sources)}"
                    ),
                )
            )

        for source_id in required_sources:
            production_node = production_manifest.get(source_id)
            local_node = local_manifest.get(source_id)
            if production_node is None or local_node is None:
                findings.append(
                    CompatibilityFinding(
                        rule_id="DBTV-CFG-002",
                        severity="error",
                        node_id=source_id,
                        message="Source is not present under both production and local targets.",
                    )
                )
                continue
            source_ref = production_node.source_ref
            if (
                source_ref is None
                or production_node.relation is None
                or local_node.relation is None
            ):
                findings.append(
                    CompatibilityFinding(
                        rule_id="DBTV-MANIFEST-002",
                        severity="error",
                        node_id=source_id,
                        message="Source relation metadata is incomplete.",
                    )
                )
                continue
            mappings.append(
                SourceMapping(
                    source=source_ref,
                    production_relation=production_node.relation,
                    local_relation=local_node.relation,
                    tags=production_node.tags,
                    meta=production_node.meta,
                )
            )

        fingerprint = project_fingerprint(project.root)
        self._cache_manifests(project.root, fingerprint, run)
        hash_payload = {
            "project": str(project.root),
            "project_fingerprint": fingerprint,
            "production_target": production_target,
            "local_target": LOCAL_TARGET_NAME,
            "selected": production_selected,
            "local_selected": local_selected,
            "sources": [asdict(mapping) for mapping in mappings],
            "findings": [asdict(finding) for finding in findings],
            "production_schema": production_manifest.schema_url,
            "local_schema": local_manifest.schema_url,
        }
        return ExecutionPlan(
            invocation_id=run.invocation_id,
            project_dir=project.root,
            project_name=project.name,
            production_target=production_target,
            local_target=LOCAL_TARGET_NAME,
            selected_ids=production_selected,
            local_selected_ids=local_selected,
            source_mappings=tuple(mappings),
            findings=tuple(findings),
            production_manifest_schema=production_manifest.schema_url,
            local_manifest_schema=local_manifest.schema_url,
            project_fingerprint=fingerprint,
            plan_hash=sha256_value(hash_payload),
        )

    @staticmethod
    def _cache_manifests(
        project_dir: Path,
        fingerprint: str,
        run: RunWorkspace,
    ) -> None:
        cache_dir = project_dir / ".dbtv" / "manifests" / fingerprint.split(":", 1)[1]
        cache_dir.mkdir(parents=True, exist_ok=True)
        production = run.production_target_path / "manifest.json"
        local = run.local_target_path / "manifest.json"
        shutil.copy2(production, cache_dir / "production-manifest.json")
        shutil.copy2(local, cache_dir / "local-manifest.json")
        (cache_dir / "metadata.json").write_text(
            json.dumps(
                {
                    "project_fingerprint": fingerprint,
                    "production_manifest": "production-manifest.json",
                    "local_manifest": "local-manifest.json",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _parse(
        self,
        *,
        project: DbtProject,
        profiles_dir: Path,
        target: str,
        target_path: Path,
        variables: str | None,
        log_path: Path,
        partial_parse: bool,
    ) -> NormalizedManifest:
        args = [
            "parse",
            "--project-dir",
            str(project.root),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            target,
            "--target-path",
            str(target_path),
        ]
        if variables:
            args.extend(["--vars", variables])
        if not partial_parse:
            args.append("--no-partial-parse")
        self.invoker.run(args, cwd=project.root, env=_dbt_environment(log_path))
        return load_manifest(target_path / "manifest.json")

    def _list(
        self,
        *,
        project: DbtProject,
        profiles_dir: Path,
        target: str,
        select: Sequence[str],
        exclude: Sequence[str],
        variables: str | None,
        log_path: Path,
    ) -> tuple[str, ...]:
        args = [
            "ls",
            "--project-dir",
            str(project.root),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            target,
            "--output",
            "json",
            "--output-keys",
            "unique_id",
        ]
        if select:
            args.extend(["--select", *select])
        if exclude:
            args.extend(["--exclude", *exclude])
        if variables:
            args.extend(["--vars", variables])
        result = self.invoker.run(
            args,
            cwd=project.root,
            env=_dbt_environment(log_path),
        )
        return parse_dbt_ls_json(result.stdout)


def _dbt_environment(log_path: Path) -> dict[str, str]:
    log_path.mkdir(parents=True, exist_ok=True)
    return {
        "DBT_LOG_PATH": str(log_path),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
