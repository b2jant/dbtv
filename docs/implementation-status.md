# Implementation status

The repository implements the complete local MVP code path from the architecture plan:

```text
dbt selection -> dual manifests -> source policy -> connector/resolver plugins
-> Arrow batches -> immutable Parquet snapshots -> exact DuckDB bindings
-> local dbt run/test/build (native compilation)
```

## Delivered

| Plan area | Repository status |
|---|---|
| Foundation and CLI | Implemented with UV, structured errors/events, cancellation, strict config, redaction, and the documented command surface. |
| dbt planner | Implemented with dbt-native selection, dual manifests, `unique_id` mapping, compatibility checks, and cached artifacts. |
| Source boundary | Connector and credential-resolver entry-point registries implemented; Snowflake and local Parquet connectors support named routing. |
| Extraction | Quoted read-only SQL, working-set pushdown, query tags, timeouts, retries, cancellation, and bounded Arrow batches implemented. |
| Snapshot store | Content-addressed immutable snapshots, atomic activation, TTLs, integrity, pinning, quota, GC, and index recovery implemented. |
| DuckDB | Exact catalog/schema binding, attachments, verification, compatibility shims, and allowlisted read-only inspection implemented. |
| dbt execution | `sync`, `run`, `test`, and `build`, including native compilation, reusable parsing, and normalized dbt results, implemented. |
| Hardening | Fault, policy, offline, redaction, diagnostics, compatibility, contract, and full fake-connector E2E tests implemented. |
| Packaging | Lockfile, sdist, and wheel build successfully with UV. |

Current automated evidence, rechecked September 5, 2026: **77 tests pass, 1 opt-in live
Snowflake test is skipped, 86% statement coverage**.
Strict mypy checks all 48 source files, Ruff passes, and wheel/source distributions build.
The built wheel starts in an isolated DuckDB-only environment. The local benchmark
exercises cold, warm, edited, and offline runs; its report is linked below. These checks
were run on the current macOS/Python 3.12 host, not on live Snowflake.

The September local-runtime additions include connection-scoped cache identity,
transactional dataset activation and recovery, named connections, key cohorts, actual
extraction budgets and disk monitoring, isolated replay, exact output comparison, and
two-state incremental validation. See the [ranked checklist](local-runtime-roadmap.md),
[workflow guide](local-runtime.md), and [benchmark report](benchmarks/local-100k.json).

## Remaining engineering and release gates

The Snowflake-versus-DuckDB golden comparison harness and a checked-in CI workflow
are not implemented. `dbtv compare` compares captured local runs, and the single live
Snowflake test only checks bounded extraction. Complex warehouse result parity is
therefore unverified. See [Snowflake correctness validation](snowflake-validation.md)
for the current evidence and required validation campaign.

These parts of the plan require company systems or decisions and cannot be completed by
repository code alone:

- configure and run the opt-in extraction test, expand the live connector suite, and
  implement and run golden comparisons against an approved Snowflake test account and
  representative dbt project;
- record remote, cold, warm, refresh, and offline performance on pilot hardware;
- approve local-data tags, retention, encryption/device posture, and incident ownership;
- sign and publish the internal package;
- conduct the controlled 5–10 developer pilot and retain normal remote production
  validation.

Until those gates are complete, `0.1.0a1` accurately labels this as a pilot-ready alpha,
not a production-validated company release.
