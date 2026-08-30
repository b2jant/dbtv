# dbtv

`dbtv` is a CLI for accelerating dbt development by snapshotting the smallest useful
working set from a remote source and executing selected models locally in DuckDB.

The first supported path is:

```text
Snowflake -> Arrow batches -> immutable Parquet snapshots -> DuckDB -> dbt
```

## Current milestone

The repository currently implements the foundation and offline planning slice:

- Python package and `dbtv` command;
- strict, versioned configuration;
- project discovery and isolated `.dbtv` workspace;
- structured errors, events, redaction, and deterministic hashing;
- subprocess boundary for the project-installed dbt executable;
- production and local manifest normalization;
- dbt-native selector resolution;
- upstream source discovery and production/local source mapping by dbt `unique_id`;
- `init`, `doctor`, `version`, `plan`, and `status` commands;
- composable credential, source connector, snapshot store, policy, and local backend protocols.

Snowflake extraction, Parquet commits, DuckDB source binding, and local dbt execution are
the next vertical-slice milestones.

## Development

Python 3.12 is recommended for the initial support matrix.

```bash
uv sync --extra dev
uv run dbtv version
uv run pytest
uv run ruff check .
```

To run planning against an existing dbt project, install compatible dbt adapters in the
same environment and run:

```bash
dbtv plan --project-dir /path/to/dbt/project --select model_name+
```

See [`docs/implementation-plan.md`](docs/implementation-plan.md) for the complete design.

