# Snowflake correctness validation

As of September 5, 2026, dbtv has a passing local regression suite, but complex
Snowflake-versus-DuckDB result parity has not been established. A successful local
run and a high coverage percentage do not establish that parity.

## Evidence available today

`uv run pytest --cov=dbtv --cov-report=term-missing` reports **77 passed, 1 skipped,
86% statement coverage** on macOS with Python 3.12.8. Ruff, strict mypy over 48
source files, and the wheel/source build pass.

| Layer | Existing evidence | Limit |
|---|---|---|
| Planning and source access | Native dbt selection and manifest mapping; query quoting, bounded Arrow batches, query tags, credential handling, and offline guards | Most Snowflake connector checks use a fake driver |
| Storage and binding | Arrow/Parquet round trips, exact DuckDB source names, corruption detection, schema drift, cancellation, quota failures, atomic dataset activation and recovery | Does not cover every Snowflake type or a consistent warehouse snapshot across tables |
| Local dbt execution | Fake-source extraction through snapshots and DuckDB into real dbt `run`, `test`, and `build`; local Parquet fixtures exercise replay and cohorts | Small synthetic projects, not a representative complex Snowflake graph |
| Result comparison | Frozen-input replay, schema comparison, exact unordered row comparison with duplicate counts, intentional model-change detection | `dbtv compare` currently compares two captured local runs; it does not ingest a Snowflake dbt baseline |
| Incremental validation | Isolated base build, incremental next-state build, and separate full rebuild; a regression fixture detects a missed update to an existing key | One two-state local scenario, not a Snowflake adapter strategy matrix |
| Live Snowflake | One opt-in read-only extraction test requests at most 10 rows and checks query IDs | Not configured in this verification; no remote model execution or output comparison |

The repository has no checked-in CI workflow. These checks were run locally;
pushing a commit does not by itself make them a required merge or release gate.
The broader golden tests described in the implementation plan remain engineering
work, in addition to requiring an approved Snowflake test environment.

## Required parity campaign

The following is the validation design to implement and run, not a claim that a
remote comparison harness already exists.

1. **Freeze identical inputs.** Use an immutable fixture or isolated frozen copy of
   every required source table. Both engines must read the same rows, including
   related records. Prefer complete bounded fixtures or explicitly captured key
   cohorts. Independent `LIMIT` queries or engine-specific hash samples can select
   different rows. Atomic local dataset activation does not freeze several live
   Snowflake tables at one warehouse point in time.
2. **Run both paths.** Pin the project commit, packages, variables, selection,
   session settings, and engine versions. Run normal dbt against Snowflake into an
   isolated validation schema and dbtv against extracted copies of those inputs.
   Exercise the full ancestor graph, with intermediate models as well as final
   marts. Use `dbt build`/`dbtv build`, or `run` followed by `test`, so data tests are
   included. Plain `run` alone is not evidence that data tests passed.
3. **Compare every materialized output.** Match outputs by dbt `unique_id`. Fail on
   missing models, unexpected skips, schema differences outside an explicit type
   mapping, row-count differences, duplicate-count differences, and differing
   values. Ignore row order unless ordering is part of the contract. Check exact
   integers, decimals, strings, booleans, and nulls; declare float tolerances and
   timestamp/JSON normalization per contract before comparing. Keep null distinct
   from empty strings and missing JSON values. Hashes and aggregate totals are
   useful diagnostics, but cannot replace a full comparison for bounded fixtures.
4. **Exercise difficult semantics.** Include multi-source joins, unmatched keys,
   fan-out, deduplication, windows with ties and deterministic tie-breakers,
   aggregates, null keys, Unicode, empty tables, high-precision decimals, time zones
   and DST, adapter-dispatched macros, target-dependent Jinja, and supported
   materializations. Unsupported Snowflake constructs should produce their expected
   compatibility failure. Supply a fixed clock where models depend on current time.
5. **Validate state changes.** Cover inserts, updates, late arrivals, duplicates,
   nulls, schema changes, and deletes where the model contract supports them. Compare
   incremental execution with a full rebuild on the next input state, and compare
   Snowflake and DuckDB for each supported strategy. `dbtv validate-incremental`
   provides the local two-state check today; it does not perform the remote branch.
6. **Check repeatability and failure behavior.** Compare cold, cached, refreshed,
   and offline runs on identical input contents. Assert zero dbtv source access in
   offline mode. Inject interrupted extraction, corrupt files, partial writes,
   timeouts, and resource exhaustion, and verify that the previous committed
   dataset remains usable. Expand the live connector suite to cover the warehouse
   type, authentication, cancellation, and sampling contracts.
7. **Gate releases with retained evidence.** Add local checks to CI and a separately
   configured live parity job. A required parity job must fail when credentials or
   fixtures are missing, rather than report a skipped test as success. Keep project
   and input fingerprints, versions, selections, query IDs, dbt artifacts, and
   per-model difference reports. Keep warehouse rows and credentials out of source
   control. Measure remote, cold, cached, refresh, and offline performance only
   after correctness passes on the same workload.

Passing this campaign establishes confidence for the tested project, supported SQL
subset, input scenarios, and dependency versions. New macros, engine upgrades, and
new incremental strategies need the same comparison before extending that claim.

## Commands available now

Local regression checks:

```bash
uv run pytest --cov=dbtv --cov-report=term-missing
uv run ruff check .
uv run mypy src/dbtv
uv build
```

For the existing live extraction smoke test, configure these environment variables
for a read-only fixture and a supported noninteractive dbt profile:

- `DBTV_TEST_SNOWFLAKE_PROFILES_DIR`
- `DBTV_TEST_SNOWFLAKE_PROFILE`
- `DBTV_TEST_SNOWFLAKE_TARGET`
- `DBTV_TEST_SNOWFLAKE_DATABASE`
- `DBTV_TEST_SNOWFLAKE_SCHEMA`
- `DBTV_TEST_SNOWFLAKE_TABLE`

```bash
uv run pytest tests/integration/test_snowflake_live.py -v -rs
```

Check that the test passed rather than skipped. It is still only an extraction
smoke test. See [local runtime workflows](local-runtime.md) for captured local
results, replay, and incremental validation.
