# Configuration

Configuration precedence is CLI, `DBTV_*` environment values, project `dbtv.yml`, user
configuration, then built-in defaults. Unknown keys fail validation. Paths are resolved
relative to the dbt project.

The generated example is [`dbtv.example.yml`](../dbtv.example.yml). Important groups:

- `project`: dbt executable, production profile/target, partial parsing;
- `source`: connector, session timeouts, Arrow batch size, retries, concurrency;
- `local`: DuckDB path, schema, threads, memory, and temp directory;
- `cache`: root, TTL, compression, integrity, quota, and retention;
- `data_profiles`: default and source-specific working-set rules;
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
