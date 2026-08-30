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
 local dbt compile/run/test/build
```

## Invariants

1. A dbt source's logical identity is its `unique_id`, never a physical table name.
2. The production manifest determines what to extract; the local manifest determines
   where to bind it.
3. Arrow record batches are the connector boundary. Parquet is the persistent boundary.
4. A pending snapshot is never visible. `_SUCCESS` plus metadata and integrity checks
   define a committed snapshot.
5. All sources in a refresh wave activate only after every extraction succeeds.
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
temporary files. Snapshot metadata is authoritative; `dbtv status --rebuild-index` can
reconstruct the SQLite snapshot index.
