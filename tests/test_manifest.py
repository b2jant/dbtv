from dbtv.core.models import ResourceType
from dbtv.project.manifest import normalize_manifest
from dbtv.project.selection import upstream_sources


def manifest_fixture(*, local: bool = False):
    database = "local" if local else "RAW"
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.11.0",
        },
        "nodes": {
            "model.analytics.stg_orders": {
                "unique_id": "model.analytics.stg_orders",
                "name": "stg_orders",
                "resource_type": "model",
                "package_name": "analytics",
                "schema": "dbtv_dev" if local else "ANALYTICS",
                "database": database,
                "depends_on": {"nodes": ["source.analytics.app.orders"]},
                "config": {"materialized": "view"},
            }
        },
        "sources": {
            "source.analytics.app.orders": {
                "unique_id": "source.analytics.app.orders",
                "name": "orders",
                "source_name": "app",
                "resource_type": "source",
                "package_name": "analytics",
                "database": database,
                "schema": "APP",
                "identifier": "ORDERS",
                "depends_on": {"nodes": []},
                "config": {},
            }
        },
    }


def test_normalizes_source_relation() -> None:
    manifest = normalize_manifest(manifest_fixture())
    source = manifest.get("source.analytics.app.orders")
    assert source is not None
    assert source.resource_type is ResourceType.SOURCE
    assert source.relation is not None
    assert source.relation.display_name == "RAW.APP.ORDERS"
    assert source.source_ref is not None
    assert source.source_ref.source_name == "app"


def test_discovers_upstream_sources() -> None:
    manifest = normalize_manifest(manifest_fixture())
    sources = upstream_sources(manifest, ["model.analytics.stg_orders"])
    assert sources == ("source.analytics.app.orders",)

