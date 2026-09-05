# Configuration

Configuration precedence is CLI, `DBTV_*` environment values, project `dbtv.yml`, user
configuration, then built-in defaults. Unknown keys fail validation. Paths are resolved
relative to the dbt project.

The generated example is [`dbtv.example.yml`](../dbtv.example.yml). Important groups:

- `project`: dbt executable, production profile/target, partial parsing and persistent parse reuse;
- `source`: default connector, session timeouts, Arrow batch size, retries, concurrency;
- `connections` / `routes`: named connector/resolver settings, source patterns, and projections;
- `local`: DuckDB path, schema, threads, memory, spill limit, temp directory, and inspection row cap;
- `cache`: root, TTL, compression, integrity, quota, and retention;
- `data_profiles`: default and source-specific working-set rules and explicit key cohorts;
- `compatibility`: strict/warn/lossy behavior and explicit rule overrides;
- `policy`: extraction caps, source-tag restrictions, file modes, and retention limits.

## Working sets

```yaml
data_profiles:
  developer:
    default:
      strategy: limit
      limit: 100000
    sources:
      - select: source:reference.*
        strategy: full
      - select: source:app.orders
        strategy: where_limit
        where: "created_at >= dateadd(day, -30, current_timestamp())"
        limit: 1000000
      - select: source:app.customers
        strategy: hash
        key: customer_id
        rate: 0.01
        seed: 42
```

`full` requires policy allowance or the explicit `--allow-full-source` flag. Predicates
must be one expression without statements or comments. Hash sampling requires a key and
a connector declaring deterministic-sampling support.

## Credentials

Production secrets are forbidden in `dbtv.yml` and CLI flags. The built-in resolver
reads the selected dbt profile and supports literal scalars and complete
`{{ env_var('NAME') }}` expressions. Other Jinja fails closed. Resolved credentials stay
in a non-serializable, redacted in-memory handle. Alternative providers register a
resolver through `dbtv.credential_resolvers`; `source.credential_resolver` chooses it
without changing orchestration.

## Environment overrides

Supported core overrides include `DBTV_DBT_EXECUTABLE`, `DBTV_PRODUCTION_TARGET`,
`DBTV_LOCAL_DATABASE`, `DBTV_CACHE_ROOT`, `DBTV_DEFAULT_DATA_PROFILE`,
`DBTV_SOURCE_CONNECTOR`, and `DBTV_LOCAL_MEMORY_LIMIT`.


See [local runtime workflows](local-runtime.md) for complete named-connection, Parquet,
cohort, replay, and resource-policy examples. `credential_resolver: none` is the explicit
choice for local Parquet inputs. `identity` provides public account/access context for
custom resolvers; it is hashed into the snapshot request identity.

`policy.max_extracted_bytes_per_run` caps uncompressed Arrow bytes across workers.
`cache.maximum_size` caps stored snapshots. `policy.max_workspace_bytes` and
`policy.minimum_free_disk` apply to the monitored local workspace, cache, output
file, and spill roots. These cooperative checks complement `local.memory_limit`
and `local.max_temp_directory_size`; they are not OS-level process or disk quotas.
