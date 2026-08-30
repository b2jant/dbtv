from __future__ import annotations

import json
import stat
import sys
import textwrap
from pathlib import Path

from click.testing import CliRunner

from dbtv.cli.app import main


def test_plan_maps_production_and_local_sources_by_unique_id(tmp_path: Path) -> None:
    fake_dbt = tmp_path / "fake_dbt.py"
    fake_dbt.write_text(
        textwrap.dedent(
            f"""\
            #!{sys.executable}
            import json
            import sys
            from pathlib import Path

            args = sys.argv[1:]
            command = args[0]
            if command == "--version":
                print("Core: 1.11.0\\nPlugins: snowflake: 1.11.0, duckdb: 1.9.0")
                raise SystemExit(0)

            target = args[args.index("--target") + 1]
            local = target == "dbtv_local"
            source_database = "LOCAL_RAW" if local else "RAW"
            model_database = "local" if local else "ANALYTICS"
            manifest = {{
                "metadata": {{
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                    "dbt_version": "1.11.0"
                }},
                "nodes": {{
                    "model.analytics.stg_orders": {{
                        "unique_id": "model.analytics.stg_orders",
                        "name": "stg_orders",
                        "resource_type": "model",
                        "package_name": "analytics",
                        "database": model_database,
                        "schema": "DBTV_DEV" if local else "ANALYTICS",
                        "depends_on": {{"nodes": ["source.analytics.app.orders"]}},
                        "config": {{"materialized": "view"}}
                    }}
                }},
                "sources": {{
                    "source.analytics.app.orders": {{
                        "unique_id": "source.analytics.app.orders",
                        "name": "orders",
                        "source_name": "app",
                        "resource_type": "source",
                        "package_name": "analytics",
                        "database": source_database,
                        "schema": "APP",
                        "identifier": "ORDERS",
                        "depends_on": {{"nodes": []}},
                        "config": {{}}
                    }}
                }}
            }}

            if command == "parse":
                target_path = Path(args[args.index("--target-path") + 1])
                target_path.mkdir(parents=True, exist_ok=True)
                (target_path / "manifest.json").write_text(json.dumps(manifest))
            elif command == "ls":
                print(json.dumps({{"unique_id": "model.analytics.stg_orders"}}))
            else:
                raise SystemExit(2)
            """
        ),
        encoding="utf-8",
    )
    fake_dbt.chmod(fake_dbt.stat().st_mode | stat.S_IXUSR)

    project = tmp_path / "project"
    project.mkdir()
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "analytics_profile:\n  target: dev\n  outputs:\n    dev:\n      type: snowflake\n",
        encoding="utf-8",
    )
    (project / "dbt_project.yml").write_text(
        "name: analytics\nprofile: analytics_profile\n",
        encoding="utf-8",
    )
    (project / "dbtv.yml").write_text(
        json.dumps(
            {
                "version": 1,
                "project": {
                    "dbt_executable": str(fake_dbt),
                    "production_target": "dev",
                },
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        main,
        [
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(profiles),
            "plan",
            "--select",
            "stg_orders",
        ],
    )

    assert result.exit_code == 0, result.output
    plan_files = list((project / ".dbtv" / "runs").glob("*/plan.json"))
    assert len(plan_files) == 1
    plan = json.loads(plan_files[0].read_text(encoding="utf-8"))
    assert plan["selected_ids"] == ["model.analytics.stg_orders"]
    assert plan["source_mappings"] == [
        {
            "source": {
                "unique_id": "source.analytics.app.orders",
                "package_name": "analytics",
                "source_name": "app",
                "table_name": "orders",
            },
            "production_relation": {
                "catalog": "RAW",
                "schema": "APP",
                "identifier": "ORDERS",
                "quoting": {},
            },
            "local_relation": {
                "catalog": "LOCAL_RAW",
                "schema": "APP",
                "identifier": "ORDERS",
                "quoting": {},
            },
        }
    ]
