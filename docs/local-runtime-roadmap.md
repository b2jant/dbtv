# Local runtime improvements

This is the implementation checklist for the September 2026 local-runtime work.
Impact is scored against fast, correct, reproducible dbt execution in local DuckDB.
Scores describe product impact, not a promise of measured speedup.

| Impact | Work | Acceptance |
|---|---|---|
| 10 | Connection-scoped cache identity | Changing source account/access context cannot reuse another connection's data; no secrets enter artifacts. |
| 10 | Versioned working datasets and atomic activation | A complete source set is published together, survives recovery, and can be replayed without connectors. |
| 9 | Shared source planning | `plan` and execution use the same routing, sampling, policy, and cache decisions. |
| 9 | Safe persistent parsing | Unchanged inputs reuse artifacts; code, macros, packages, vars, profiles, and engine changes invalidate them. |
| 9 | Resource budgets | Actual row/byte limits apply to all sampling strategies, parallel extraction is accounted for, and DuckDB spill/workspace use is bounded and reported. |
| 9 | Named connections and second source connector | Snowflake and local Parquet can be routed independently in one project; contract tests cover both paths. |
| 9 | Reproducible replay | Frozen inputs and recorded invocation settings can be reused with explicit code/context validation and isolated output state. |
| 9 | Relationship-preserving datasets | Configured key cohorts retain matching inputs, with explicit bounds and coverage information. |
| 8 | Comparison and incremental validation | Two isolated runs can compare results on fixed inputs; incremental scenarios can compare with a full-refresh reference. |
| 8 | Compatibility and execution diagnostics | Users can inspect source/cache decisions, graph boundaries, validation findings, and per-phase performance as structured artifacts. |
| 8 | Dependency boundary validation | Missing local model/seed/snapshot prerequisites are explained without silently changing dbt selection semantics. |
| 8 | Benchmark harness and documentation | Repeatable cold/warm/edited/offline measurements, documented examples, and regression checks. |
| 7 | Watch daemon, automatic model-output reuse | Deferred: persistent process lifecycle and side-effect-aware skipping need measured justification. |
| 7 | Additional warehouse connectors, automatic fixtures | Deferred until the shared contract and named routing are proven with the second connector. |
| 6 | Automatic SQL projection/transpilation | Deferred: SQL semantic analysis is a separate compatibility surface; explicit projections are supported first. |
| 5 | Rust/Go rewrite or GUI | Deferred: does not remove repeated dbt work or improve dataset correctness by itself. |

## Constraints

- dbt owns selection, compilation, hooks, scheduling, and materializations.
- DuckDB remains the local engine; no dbt Cloud service is required.
- Offline source decisions precede credential resolution or connector creation.
- Immutable Parquet inputs remain distinct from writable model outputs.
- A dataset records capture consistency; it does not claim a simultaneous cross-provider snapshot.
- Existing configuration remains usable. Unscoped legacy cache entries require a refresh.
- Live Snowflake/SSO validation requires an available account; local contract tests must not imply it happened.

## Progress

All items ranked 8–10 are implemented. The 5–7 items remain deferred as ranked.

Verification on this host:

- 239 local tests passed with 99.44% statement coverage; one opt-in live Snowflake
  test is skipped by default and passes separately with the saved local credentials.
- Both Snowflake profiles authenticate. The six source fixtures and golden schema
  grants are present; broader parity retesting remains pending.
- Strict mypy passed for 48 source files; Ruff and `git diff --check` passed.
- Wheel and source distribution built; the wheel passed an isolated DuckDB-only CLI smoke test
  and the complete cold/warm/edited/offline benchmark.
- Real local dbt tests cover frozen-file replay, model-change comparison, missing ancestor
  diagnostics, relationship-preserving extraction, a child budget failure after parent
  capture, dataset recovery, duplicate-aware comparison, and missed incremental updates.
- A refresh of identical content keeps its immutable ID while a separate observation
  receipt updates cache freshness. Redaction preserves legitimate dbt IDs containing
  words such as `tokens`, while continuing to redact credential values.

The [workflow guide](local-runtime.md) documents the source/plugin contract, local dbt
profile with browser SSO, new commands, examples, current constraints, and the
[repeatable benchmark](benchmarks/local-100k.json).

Operational limits: the disk budget uses cooperative monitoring rather than an OS
quota; DuckDB's memory setting does not cap total process memory. Cohorts cover one
scalar key and independent source captures. Replay rebuilds fresh output state;
incremental validation tests an explicit two-state scenario. No local sample or
scenario proves every production input or warehouse behavior correct. Live SSO,
warehouse extraction performance, and other OS/dependency versions still need their
own validation.


Final packaged benchmark (100,000 input rows, one aggregation model): 8.328 s first
cold invocation, 2.749 s median warm, 3.393 s after an edit, and 2.761 s median offline.
Warm/offline each contain three runs. The isolated package environment had no
`dbt-snowflake` installation; all runs reported zero remote queries. The earlier warm
development environment's cold-source measurement was 3.489 s, so startup conditions
matter. These are local synthetic timings, not a production performance guarantee.
