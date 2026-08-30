# dbtv

`dbtv` accelerates dbt development by extracting only the source data required for a
selection, committing it as reusable local snapshots, and running the project's dbt
models against DuckDB.

```text
Snowflake -> Arrow batches -> immutable Parquet snapshots -> DuckDB -> dbt
```

The source boundary is plugin-based. Snowflake is the first implementation, while the
snapshot, policy, DuckDB, and dbt execution layers contain no provider-specific logic.
A future Databricks, Iceberg, or other Arrow-capable connector can reuse the complete
local path.

## What works

- dbt-native `--select`, `--exclude`, and `--vars` resolution;
- production and local manifests joined by dbt `unique_id`;
- restricted, non-serializable credential resolution from the existing dbt profile;
- bounded Snowflake Arrow streaming with safe quoting, query tags, timeout, retry, and
  cancellation;
- full, limit, where, where-limit, deterministic hash, and Bernoulli working sets;
- immutable Parquet snapshots, atomic commits, integrity checks, TTLs, quotas, pinning,
  activation, state rebuild, and garbage collection;
- exact three-part DuckDB source binding, including quoted catalogs and schemas;
- isolated local `dbt compile`, `run`, `test`, and `build` invocations;
- warm `cached` and strict zero-connector `offline` modes;
- source-tag policy controls, compatibility findings, redacted events, and diagnostics;
- read-only inspection and guarded cleanup.

## Install with UV

Python 3.12 is the recommended runtime.

```bash
uv sync --extra snowflake --extra duckdb
uv run dbtv version
```

For repository development:

```bash
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run mypy src/dbtv
```

No global Python installation or manually managed virtual environment is required.

## First run

Run from an existing dbt project that already has a working Snowflake target in
`profiles.yml`:

```bash
uv run dbtv --project-dir /path/to/project init --update-gitignore
uv run dbtv --project-dir /path/to/project doctor
uv run dbtv --project-dir /path/to/project plan --select stg_orders+
uv run dbtv --project-dir /path/to/project run --select stg_orders+
```

Planning is local-only by default. Use `plan --remote-estimates` only when you explicitly
want tagged source count queries before execution.

Repeat without any source connector or credential resolution:

```bash
uv run dbtv --project-dir /path/to/project run --select stg_orders+ --offline
```

`dbtv` does not run Snowflake locally. Cold or refreshed runs connect to an actual
Snowflake account using the selected dbt profile. Offline runs use only committed local
snapshots.

## Important commands

```bash
dbtv plan --select fct_orders+
dbtv sync --select fct_orders+ --source-mode refresh
dbtv run --select fct_orders+
dbtv test --select fct_orders --offline
dbtv build --select fct_orders+ --offline
dbtv status
dbtv inspect --query "select count(*) from dbtv_dev.fct_orders"
dbtv clean --snapshots --older-than 7d --preview
dbtv diagnostics
```

Every execution summary states whether remote access was attempted, how many source
queries completed, which snapshots were reused or refreshed, the DuckDB path, dbt
status, timings, and artifact directory.

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Command reference](docs/commands.md)
- [Compatibility and fidelity](docs/compatibility.md)
- [Security model](docs/security.md)
- [Troubleshooting and recovery](docs/troubleshooting.md)
- [Development and testing](docs/development.md)
- [Initial support matrix](docs/support-matrix.md)
- [Implementation status and external release gates](docs/implementation-status.md)
- [Full implementation plan](docs/implementation-plan.md)

Local execution is a development accelerator, not a substitute for the company's
production dbt validation and deployment workflow.
