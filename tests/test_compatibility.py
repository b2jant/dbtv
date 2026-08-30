from __future__ import annotations

from dbtv.compatibility import CompatibilityAnalyzer
from dbtv.config.schema import CompatibilitySettings
from dbtv.core.models import CanonicalField, CanonicalSchema, NormalizedManifest
from dbtv.project.manifest import normalize_manifest


def _manifest(sql: str) -> NormalizedManifest:
    return normalize_manifest(
        {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                "dbt_version": "1.11.0",
            },
            "nodes": {
                "model.analytics.example": {
                    "unique_id": "model.analytics.example",
                    "name": "example",
                    "resource_type": "model",
                    "package_name": "analytics",
                    "schema": "analytics",
                    "raw_code": sql,
                    "config": {"materialized": "table"},
                    "depends_on": {"nodes": []},
                }
            },
        }
    )


def test_strict_mode_blocks_known_nonportable_sql() -> None:
    findings = CompatibilityAnalyzer(CompatibilitySettings()).analyze_manifest(
        _manifest("select * from table(generator(rowcount => 10))"),
        ("model.analytics.example",),
    )
    assert findings[0].rule_id == "DBTV-SQL-001"
    assert findings[0].severity == "error"
    assert not findings[0].may_continue


def test_rule_override_is_explicit() -> None:
    settings = CompatibilitySettings(allow_rules=["DBTV-SQL-001"])
    findings = CompatibilityAnalyzer(settings).analyze_manifest(
        _manifest("select * from table(generator(rowcount => 10))"),
        ("model.analytics.example",),
    )
    assert findings[0].severity == "info"


def test_decimal_precision_over_38_is_blocked_in_strict_mode() -> None:
    findings = CompatibilityAnalyzer(CompatibilitySettings()).analyze_schema(
        CanonicalSchema(
            (CanonicalField("amount", "decimal256(40, 2)", True),),
            "sha256:example",
        ),
        "source.analytics.app.orders",
    )
    assert findings[0].rule_id == "DBTV-TYPE-001"
    assert findings[0].severity == "error"
