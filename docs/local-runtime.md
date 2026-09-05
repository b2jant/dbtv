# Local runtime workflows

DBTV is a Python CLI orchestrating local dbt and the native DuckDB engine. It requires
no dbt Cloud account or service. Cold Snowflake refreshes still execute source reads in
Snowflake and use its compute; cached and offline runs reuse local immutable Parquet.
A successful sampled local run is evidence about that working set, not a guarantee
about every production row, warehouse dialect feature, or production workload size.

## Source authentication and routing

A local `~/.dbt/profiles.yml` can use browser SSO:

```yaml
analytics_profile:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      authenticator: externalbrowser
      role: DBTV_READER
      warehouse: DEV_WH
      database: ANALYTICS
      schema: DEV
      threads: 4
```

`dbtv.yml` selects that production target. Source reads use the profile directly;
the generated local dbt profile contains only DuckDB settings and local attachments.
Browser authentication requires an interactive refresh. Subsequent offline runs do
not invoke the credential resolver or create connectors. Public profile identity
fields may still need their environment values to identify the cached connection.

```yaml
project:
  production_target: dev
source:
  connector: snowflake
  credential_resolver: dbt_profile
connections:
  finance:
    connector: snowflake
    credential_resolver: dbt_profile
    profile: finance_profile
    target: dev
  fixtures:
    connector: parquet
    credential_resolver: none
    plugin:
      tables:
        RAW.REFERENCE.CURRENCIES: fixtures/currencies.parquet
routes:
  - select: source:finance.*
    connection: finance
  - select: source:reference.currencies
    connection: fixtures
    projection: [CURRENCY_CODE, RATE]
```

Copy physical relation names from `dbtv plan` when configuring Parquet mappings.
File paths are project-relative; directories recursively include `.parquet` files.
The first Parquet implementation supports projections, predicates, limits, and hash
sampling. It validates schema consistency and checks file stat identity before and
after extraction. This detects ordinary concurrent file changes, not adversarial
replacement preserving metadata. Cached/offline reuse never reopens original files.

Unmatched sources use `source`. Multiple matching routes fail instead of depending
on order. Sessions are reused within each named connection, with a global extraction
worker cap from `source.extraction.parallel_sources`. Connector credentials stay in
memory. Third-party resolvers should supply public account/access identifiers through
the connection's `identity` mapping so cache scopes describe their access context.

## Datasets and replay

Every successful source preparation commits a content-addressed dataset containing
all source snapshot IDs, access scopes, sampling, capture times, verification times, and source versions
when available. Source snapshots are published together after extraction and schema
checks. A failing child source cannot activate a newly captured parent on its own.
Refreshing identical content preserves its data identity; a separate observation receipt
advances cache freshness without rewriting the original capture metadata.

```bash
dbtv build --select +fct_orders --capture-results
dbtv datasets list
dbtv datasets show sha256:...
dbtv plan --select +fct_orders --dataset sha256:...
dbtv build --select +fct_orders --dataset sha256:...
dbtv replay <run-uuid>
dbtv compare <original-run-uuid> <replay-run-uuid>
```

The dataset is frozen input state. `--dataset` requires compatible routing, projection,
sampling, access scope, and current policy. It does not require fresh authentication.
Captured inputs remain retained until `datasets drop ID --yes` releases their dataset
reference, or `clean --all` removes the history. Disk budgets fail clearly if retained
datasets fill the cache; DBTV does not silently delete replay inputs to make room.

Replay uses recorded selection, vars, and sampling in an isolated output database.
It checks project/package content, relevant environment values, engine versions, and
execution configuration. Environment values enter a digest, not a saved environment
file. Dynamic `env_var` names conservatively hash the environment. Use
`--allow-code-change` to evaluate an intentional code or context change on fixed inputs.
Redacted invocation values cannot be replayed. Keep the installed local dbt environment
pinned; arbitrary external executables can introduce dependencies beyond version checks.

Replay does not preserve a historical incremental output database. An incremental
original requires `replay --full-refresh`, or the explicit scenario below. SQL using
the current time, nondeterministic functions, or side effects can still produce different
outputs from frozen inputs. dbt continues to own project hooks and materializations.

Comparison uses exact DuckDB multiset equality: schema and values must match, row order
does not matter, and duplicate counts matter. It returns counts rather than loading
all differing rows into Python. No approximate float tolerance or warehouse-equivalence
claim is implied. Optional `--capture-results` writes extra output Parquet and therefore
adds time and disk use to the run.

## Key cohorts and incremental scenarios

Independent limits can select orders without their customers. A cohort derives a
child predicate from the selected parent's actual distinct, non-null keys:

```yaml
data_profiles:
  developer:
    default: {strategy: limit, limit: 100}
    cohorts:
      - select: source:app.orders
        parent: source:app.customers
        parent_key: CUSTOMER_ID
        key: CUSTOMER_ID
        max_keys: 100
```

The parent must identify exactly one source in the selected upstream graph. Child
cohorts replace independent sampling/limits with a key filter, retaining any configured
`where` predicate. All matching children are captured or the run fails its row/byte
budget. It never truncates children and labels the truncated result complete. Cycles,
ambiguous parents, missing key columns, and excessive distinct keys fail explicitly.
Initial support is one scalar key per relationship, up to 10,000 configured keys;
composite keys and automatic relationship inference remain future extensions.

`cohorts.json` reports parent and child coverage. Completeness is relative to the chosen
keys and predicates. Captures across sources are independent, even for two Snowflake
tables; atomic local publication is not a distributed or warehouse snapshot transaction.

```bash
dbtv sync --select +fct_orders --source-mode refresh
# Record the first dataset ID; change the input fixture/window and capture the next.
dbtv sync --select +fct_orders --source-mode refresh
dbtv validate-incremental --base-dataset sha256:... --next-dataset sha256:... \
  --select +fct_orders
```

Validation builds the initial state, applies the next dataset incrementally, and compares
against a separate fresh build on the next dataset. Both branches use current project
code. A scenario with updated existing keys can expose an append-only filter that a
simple repeated run would miss. Choose fixtures covering updates, late arrivals,
deletions, nulls, and duplicates as relevant to the model; one scenario cannot cover
all incremental behavior.

## Performance, budgets, and diagnostics

Normal execution uses two native dbt `ls` invocations, which also emit the manifests,
then the requested dbt command. An extra parse is used only for executables whose
`ls` does not emit a manifest. There is no redundant compile subprocess. Persistent
partial parse files are checksum-checked; dbt remains responsible for parsing and
environment invalidation on every invocation. Disable reuse with
`project.cache_parsing: false`, or all partial parsing with `project.partial_parse: false`.

```yaml
local:
  memory_limit: 8GB
  max_temp_directory_size: 20GB
  inspect_max_rows: 1000
cache:
  maximum_size: 100GB
policy:
  max_rows_per_source: 5000000
  max_extracted_bytes_per_run: 20GB
  max_workspace_bytes: 150GB
  minimum_free_disk: 1GB
```

Actual Arrow rows and uncompressed bytes are counted across parallel extraction.
`--max-bytes` applies to that byte count, while cache quota applies to stored files.
DuckDB receives memory and spill settings. A cooperative disk monitor includes cache,
pending writes, outputs, and spill; it checks every 250 ms and cancels work on breach.
This is not an OS quota: one batch, a running operation, or cancellation latency can
overshoot the threshold. DuckDB's memory setting is not a hard limit on total Python,
driver, and Arrow process memory. Large or skewed operations can still run out of memory
or disk. Limits provide a controlled failure path, not a guarantee of success.

`lineage.json`, `dependencies.json`, `parsing.json`, `resources.json`, source decisions,
and dbt results describe what ran and why. Missing unselected model/seed/snapshot
prerequisites produce a concrete error with the unchanged native selection. Use
`--select +model` and `build` when the ancestors should be built. Inspection is read-only
and bounded by `local.inspect_max_rows`; registered local source snapshots are readable.

Run the synthetic local benchmark with:

```bash
uv run python scripts/benchmark_local.py --rows 100000 --repeats 3 \
  --output docs/benchmarks/local-100k.json
```

The [recorded report](benchmarks/local-100k.json) includes hardware/runtime context,
cold/warm/edited/offline measurements, and parsing-cache evidence. It measures a local
synthetic aggregation. Measure representative projects and source extraction separately
before making a production or cloud-cost performance claim.


The checked-in report was produced through the built wheel in an isolated environment
with only the DuckDB extra: first cold invocation 8.328 s, warm median 2.749 s, edited
3.393 s, offline median 2.761 s. The first invocation includes fresh-environment startup;
its cost should be kept separate from steady repeated development runs.
