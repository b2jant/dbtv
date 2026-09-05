# Development and testing

The project is managed exclusively with UV.

```bash
uv sync --all-extras --python 3.12
uv run pytest
uv run pytest --cov=dbtv --cov-report=term-missing
uv run ruff check .
uv run mypy src/dbtv
uv build
```

The default suite includes unit tests, connector/query contracts, atomic cache and fault
tests, quoted DuckDB binding, redaction canaries, process cancellation, and a full fake
connector -> Arrow -> Parquet -> DuckDB -> real dbt run/offline rerun.

The live Snowflake test is opt-in. Set the variables documented in
`tests/integration/test_snowflake_live.py` for a read-only test table and run:

```bash
uv run pytest tests/integration -v
```

Supported initial matrix: Python 3.11–3.13 (3.12 recommended), dbt-core 1.11.x,
dbt-snowflake 1.11.x, dbt-duckdb 1.11.x, DuckDB 1.x, Snowflake Connector 3.12–4.x,
and manifest schemas v9–v12. CI should test supported operating systems and pin the UV
lockfile.

Performance pilots should record remote dbt baseline, cold extraction, warm cached,
refresh, and offline timings for representative selections. The final run artifacts
already contain per-phase timings suitable for aggregation. Pilot ownership, security
approval, hardware baselines, and production incident paths are organization-specific
release gates rather than code defaults.


The local runtime suite also exercises Parquet connector routing, source identity
separation, dataset rollback/recovery, cohort coverage and budget failure, frozen-input
replay, exact duplicate-aware comparison, and a failing incremental-update scenario.
Use `scripts/benchmark_local.py` for the repeatable local benchmark; its JSON report
records per-phase timings and evidence of partial parse reuse/invalidation.

See [Snowflake correctness validation](snowflake-validation.md) for the verified test
evidence, the current live-test and CI gaps, and the required comparison of a complex
model graph against normal dbt execution in Snowflake.
