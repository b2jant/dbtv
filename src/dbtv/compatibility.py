from __future__ import annotations

import re
from dataclasses import dataclass

from dbtv.config.schema import CompatibilitySettings
from dbtv.core.models import (
    CanonicalSchema,
    CompatibilityFinding,
    NormalizedManifest,
    ResourceType,
)


@dataclass(frozen=True)
class SqlRule:
    rule_id: str
    pattern: re.Pattern[str]
    message: str
    remediation: str
    blocking: bool = True


_SQL_RULES = (
    SqlRule(
        "DBTV-SQL-001",
        re.compile(r"\btable\s*\(\s*generator\s*\(", re.I),
        "Snowflake GENERATOR table functions are not portable to DuckDB.",
        "Replace with generate_series/range behind adapter.dispatch.",
    ),
    SqlRule(
        "DBTV-SQL-002",
        re.compile(r"\b(?:lateral\s+)?flatten\s*\(", re.I),
        "Snowflake FLATTEN semantics require an explicit DuckDB translation.",
        "Add an adapter-dispatched unnest implementation and semantic test.",
    ),
    SqlRule(
        "DBTV-SQL-003",
        re.compile(r"\bresult_scan\s*\(", re.I),
        "Snowflake RESULT_SCAN cannot execute in a local DuckDB session.",
        "Refactor the model to read a durable relation.",
    ),
    SqlRule(
        "DBTV-SQL-004",
        re.compile(r"::\s*(?:variant|object)\b", re.I),
        "Snowflake semi-structured casts may differ in DuckDB.",
        "Use adapter-dispatched JSON conversion with a semantic test.",
    ),
    SqlRule(
        "DBTV-SQL-005",
        re.compile(r"(?<!\.)\bdateadd\s*\(", re.I),
        "Raw Snowflake DATEADD syntax does not preserve semantics in DuckDB.",
        "Use dbt.dateadd or an adapter-dispatched macro.",
    ),
    SqlRule(
        "DBTV-TARGET-001",
        re.compile(r"\btarget\.(?:name|type)\b", re.I),
        "Model behavior branches on the active target.",
        "Verify that the dbtv_local branch preserves production semantics.",
        blocking=False,
    ),
)


class CompatibilityAnalyzer:
    def __init__(self, settings: CompatibilitySettings) -> None:
        self.settings = settings

    def analyze_manifest(
        self,
        manifest: NormalizedManifest,
        selected_ids: tuple[str, ...],
    ) -> tuple[CompatibilityFinding, ...]:
        findings: list[CompatibilityFinding] = []
        for unique_id in selected_ids:
            node = manifest.get(unique_id)
            if not node or node.resource_type is not ResourceType.MODEL:
                continue
            materialized = str(node.config.get("materialized", "view"))
            if materialized not in {"view", "table", "ephemeral", "incremental"}:
                findings.append(
                    self._finding(
                        "DBTV-MAT-001",
                        f"Materialization {materialized!r} is not in the supported local set.",
                        node.unique_id,
                        node.path,
                        "Use table/view/ephemeral or add a tested DuckDB materialization.",
                        blocking=True,
                    )
                )
            if materialized == "incremental":
                findings.append(
                    self._finding(
                        "DBTV-INCR-001",
                        "Incremental output can depend on prior local state.",
                        node.unique_id,
                        node.path,
                        "Use --full-refresh for a reproducible first local validation.",
                        blocking=False,
                    )
                )
            code = node.raw_code or ""
            for rule in _SQL_RULES:
                if rule.pattern.search(code):
                    findings.append(
                        self._finding(
                            rule.rule_id,
                            rule.message,
                            node.unique_id,
                            node.path,
                            rule.remediation,
                            blocking=rule.blocking,
                        )
                    )
        return tuple(findings)

    def analyze_schema(
        self,
        schema: CanonicalSchema,
        source_unique_id: str,
    ) -> tuple[CompatibilityFinding, ...]:
        findings: list[CompatibilityFinding] = []
        for field in schema.fields:
            if re.search(
                r"decimal(?:128|256)?\((?:3[9]|[4-9]\d|\d{3,}),",
                field.arrow_type,
                re.I,
            ):
                findings.append(
                    self._finding(
                        "DBTV-TYPE-001",
                        f"Column {field.name!r} exceeds DuckDB DECIMAL precision 38.",
                        source_unique_id,
                        None,
                        "Choose warn/lossy fidelity or normalize the column in the source query.",
                        blocking=True,
                    )
                )
        return tuple(findings)

    def _finding(
        self,
        rule_id: str,
        message: str,
        node_id: str,
        path: str | None,
        remediation: str,
        *,
        blocking: bool,
    ) -> CompatibilityFinding:
        if rule_id in self.settings.allow_rules:
            severity = "info"
        elif rule_id in self.settings.deny_rules:
            severity = "error"
        elif rule_id in self.settings.warn_rules:
            severity = "warning"
        elif blocking and self.settings.mode == "strict":
            severity = "error"
        else:
            severity = "warning"
        return CompatibilityFinding(
            rule_id=rule_id,
            severity=severity,
            message=message,
            node_id=node_id,
            path=path,
            remediation=remediation,
            may_continue=severity != "error",
        )
