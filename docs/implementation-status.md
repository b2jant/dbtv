# Implementation status

The repository implements the complete local MVP code path from the architecture plan:

```text
dbt selection -> dual manifests -> source policy -> connector/resolver plugins
-> Arrow batches -> immutable Parquet snapshots -> exact DuckDB bindings
-> local dbt compile/run/test/build
```

## Delivered

| Plan area | Repository status |
|---|---|
| Foundation and CLI | Implemented with UV, structured errors/events, cancellation, strict config, redaction, and the documented command surface. |
| dbt planner | Implemented with dbt-native selection, dual manifests, `unique_id` mapping, compatibility checks, and cached artifacts. |
| Source boundary | Connector and credential-resolver entry-point registries implemented; Snowflake is the initial connector. |
| Extraction | Quoted read-only SQL, working-set pushdown, query tags, timeouts, retries, cancellation, and bounded Arrow batches implemented. |
| Snapshot store | Content-addressed immutable snapshots, atomic activation, TTLs, integrity, pinning, quota, GC, and index recovery implemented. |
| DuckDB | Exact catalog/schema binding, attachments, verification, compatibility shims, and allowlisted read-only inspection implemented. |
| dbt execution | `sync`, `run`, `test`, and `build`, including local compile and normalized dbt results, implemented. |
| Hardening | Fault, policy, offline, redaction, diagnostics, compatibility, contract, and full fake-connector E2E tests implemented. |
| Packaging | Lockfile, sdist, and wheel build successfully with UV. |

Current automated evidence: 61 tests pass, the live Snowflake test is opt-in, strict
mypy and Ruff pass, and line coverage is 84% on the verified macOS/Python 3.12 setup.

## Organization-owned release gates

These parts of the plan require company systems or decisions and cannot be completed by
repository code alone:

- run the opt-in integration and golden-comparison suites against an approved Snowflake
  test account and representative dbt project;
- record remote, cold, warm, refresh, and offline performance on pilot hardware;
- approve local-data tags, retention, encryption/device posture, and incident ownership;
- sign and publish the internal package;
- conduct the controlled 5–10 developer pilot and retain normal remote production
  validation.

Until those gates are complete, `0.1.0a1` accurately labels this as a pilot-ready alpha,
not a production-validated company release.
