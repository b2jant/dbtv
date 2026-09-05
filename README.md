# dbtv

`dbtv` accelerates dbt development by extracting only the source data required for a
selection, committing it as reusable local snapshots, and running the project's dbt
models against DuckDB.

```text
Snowflake / local Parquet -> Arrow -> immutable datasets -> DuckDB -> local dbt
```

The source boundary is plugin-based, with Snowflake and local Parquet implementations.
Named connections route sources independently; storage and execution share one local path.
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
- local `run`, `test`, and `build` with dbt-owned compilation and reusable parsing;
- atomic source-set activation, connection-scoped caches, and frozen-input replay;
- key cohorts, actual extraction budgets, spill limits, and dependency diagnostics;
- isolated incremental validation and exact captured-output comparison;
- warm `cached` and strict zero-connector `offline` modes;
- source-tag policy controls, compatibility findings, redacted events, and diagnostics;
- read-only inspection and guarded cleanup.

## Install the CLI

Python 3.12 is the recommended runtime.

```bash
uv tool install \
  'dbtv[snowflake,duckdb] @ git+https://github.com/b2jant/dbtv.git'
dbtv version
```

After installation, run `dbtv` directly from any dbt project. Upgrade later with
`uv tool upgrade dbtv`.

For repository development:

```bash
git clone https://github.com/b2jant/dbtv.git
cd dbtv
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run mypy src/dbtv
```

UV manages the CLI's isolated environment. No global Python installation or manually
managed virtual environment is required.

## First run

Run from an existing dbt project that already has a working Snowflake target in
`profiles.yml`:

```bash
dbtv --project-dir /path/to/project init --update-gitignore
dbtv --project-dir /path/to/project doctor
dbtv --project-dir /path/to/project plan --select stg_orders+
dbtv --project-dir /path/to/project run --select stg_orders+
```

Planning is local-only by default. Use `plan --remote-estimates` only when you explicitly
want tagged source count queries before execution.

Repeat without any source connector or credential resolution:

```bash
dbtv --project-dir /path/to/project run --select stg_orders+ --offline
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

## Frozen inputs and validation

```bash
dbtv build --select +fct_orders --capture-results
dbtv datasets list
dbtv replay <run-uuid>
dbtv compare <original-run-uuid> <replay-run-uuid>
dbtv validate-incremental --base-dataset sha256:... --next-dataset sha256:...
```

DBTV stays a Python CLI using the native DuckDB engine. It invokes installed local dbt;
**dbt Cloud is not required**. Source refreshes can query Snowflake directly using
`authenticator: externalbrowser` in a local dbt profile.

See [local runtime workflows](docs/local-runtime.md) for named connections, cohorts,
replay limitations, disk budgets, and benchmark results.

## Documentation

- [Local runtime workflows](docs/local-runtime.md)
- [Ranked improvements and verification](docs/local-runtime-roadmap.md)
- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Command reference](docs/commands.md)
- [Compatibility and fidelity](docs/compatibility.md)
- [Security model](docs/security.md)
- [Troubleshooting and recovery](docs/troubleshooting.md)
- [Development and testing](docs/development.md)
- [Snowflake correctness validation](docs/snowflake-validation.md)
- [Initial support matrix](docs/support-matrix.md)
- [Implementation status and external release gates](docs/implementation-status.md)
- [Full implementation plan](docs/implementation-plan.md)

Local execution is a development accelerator, not a substitute for the company's
production dbt validation and deployment workflow.
