# Architecture

`dbtv` separates logical planning, remote extraction, persistent snapshot storage, and
local execution. The orchestration layer depends on protocols and capability values;
it does not branch on provider names.

```text
dbt project + production profile
             |
       dual-manifest planner
             |
      source unique_id mappings
             |
   policy + snapshot decisions
        /             \
 committed cache    source connector
        \             /
       immutable Parquet
             |
     exact DuckDB bindings
             |
 local dbt run/test/build (native compilation)
```

## Invariants

1. A dbt source's logical identity is its `unique_id`, never a physical table name.
2. The production manifest determines what to extract; the local manifest determines
   where to bind it.
3. Arrow record batches are the connector boundary. Parquet is the persistent boundary.
4. A pending snapshot is never visible. `_SUCCESS` plus metadata and integrity checks
   define a committed snapshot.
5. Every input dataset activates in one SQLite transaction after extraction and schema validation.
6. Offline mode reaches a cache decision before credential or connector code.
7. dbt owns compilation, selection, materializations, and node execution.
8. The generated DuckDB profile contains no production credentials.

## Adding another source

Implement a factory registered in the `dbtv.source_connectors` entry-point group. The
factory declares `SourceCapabilities` and creates an object implementing
`SourceConnector`: lifecycle, schema inspection, version, estimate, Arrow extraction,
and cancellation. Register any provider-specific credential resolver independently in
`dbtv.credential_resolvers`, then select both names under `source` in `dbtv.yml`.
Provider code owns quoting, credential translation, and query construction. It does not
select cache paths, local relation names, or dbt nodes.

Contract tests should run the new connector against the shared snapshot-store and fake
dbt end-to-end suites. No orchestration or DuckDB changes should be necessary.

## Workspace

The project-contained `.dbtv` directory holds the SQLite index, persistent DuckDB file,
immutable cache, attached catalog files, cached manifests, run artifacts, locks, and
temporary files. Snapshot metadata describes immutable files. SQLite is authoritative for complete
dataset activation; post-commit dataset sidecars support recovery. Rebuilding never
automatically activates a scoped source snapshot from an unpublished refresh.


## Runtime boundaries

`project/planner.py` asks native dbt `ls` for each target and consumes its manifest.
It restores dbt-owned partial parsing state under a key containing project/package
content, profiles, vars, configuration, and engine/executable identity. Native dbt runs
on every plan, including its own environment invalidation and selector handling.
No rendered SQL, model outputs, or selector result is silently reused as a run result.

`sources.py` owns the shared plan/run preparation of routes, source identity, policy,
and cache decisions. Cohort rules add an explicit dependency between source captures.
A reused parent resolves a child's exact predicate during local planning; a refreshed
parent defers that predicate until its snapshot exists. Both outcomes use the same code.

`orchestration.py` resolves credentials only for refreshes, reuses independent connector
sessions within a connection, streams Arrow under a shared run budget, publishes a
complete dataset, checks local dependency boundaries, and delegates to dbt.
`validation.py` handles optional output capture, replay, and exact comparison. These
features do not impose result-copying costs on ordinary run/build commands.

Source, snapshot, and output identities are separate. Adding another SQL-generating
frontend should produce normalized nodes and source mappings through a new planner,
then reuse source preparation, datasets, and DuckDB binding. Keep dbt-specific semantics
inside the current planner/invoker; do not infer dbt materialization behavior in the
source connector. A future GUI can consume `lineage.json`, events, and run artifacts
without changing the execution engine.
