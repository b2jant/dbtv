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

Current automated evidence, rechecked September 5, 2026: **239 local tests pass,
1 opt-in live Snowflake test is skipped by default, 99.44% statement coverage**.
The live extraction test also passes when invoked separately with local credentials.
Strict mypy checks all 48 source files, Ruff passes, and wheel/source distributions build.
The previously built wheel passed an isolated DuckDB-only startup check. The local
benchmark exercises cold, warm, edited, and offline runs; its report is linked below.
The host is macOS/Python 3.12. Live key-pair authentication now works for both the
source and golden profiles, and the sandbox fixture tables and grants are present.

The September local-runtime additions include connection-scoped cache identity,
transactional dataset activation and recovery, named connections, key cohorts, actual
extraction budgets and disk monitoring, isolated replay, exact output comparison, and
two-state incremental validation. See the [ranked checklist](local-runtime-roadmap.md),
[workflow guide](local-runtime.md), and [benchmark report](benchmarks/local-100k.json).

## Remaining engineering and release gates

A maintained Snowflake-versus-DuckDB comparison harness and checked-in CI workflow
are still missing. An earlier temporary sandbox comparison matched 11 of 12 models
and exposed timestamp divergence; that harness is no longer present on disk.
`dbtv compare` compares captured local runs, and the checked-in live Snowflake test
only checks bounded extraction. Complete parity on the current code is unverified.
See [Snowflake correctness validation](snowflake-validation.md) for the historical
findings, current access evidence, and required validation campaign.

These parts of the plan require company systems or decisions and cannot be completed by
repository code alone:

- expand the live connector suite beyond the passing extraction test, restore a
  repeatable golden comparison suite using the available sandbox, and validate a
  representative company dbt project;
- record remote, cold, warm, refresh, and offline performance on pilot hardware;
- approve local-data tags, retention, encryption/device posture, and incident ownership;
- sign and publish the internal package;
- conduct the controlled 5–10 developer pilot and retain normal remote production
  validation.

Until those gates are complete, `0.1.0a1` accurately labels this as a pilot-ready alpha,
not a production-validated company release.
