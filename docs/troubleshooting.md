# Troubleshooting and recovery

Start with `dbtv doctor`, then inspect the run directory printed in the final summary.
`run.json`, `events.jsonl`, `plan.json`, `compatibility.json`, `bindings.json`, and
normalized dbt results contain redacted diagnostic state.

- Cache miss in offline/cached mode: run `dbtv sync --select ...` while connected.
- Corrupt cache: it is marked `CORRUPT` and never selected; refresh or clean it.
- Stale index: run `dbtv status --rebuild-index` to scan authoritative sidecars.
- Lock conflict: let the named writer finish; dbtv never breaks a live lock automatically.
- Binding failure: inspect `bindings.json`; snapshots remain committed.
- Compile/compatibility failure: review the stable rule ID and local dbt artifacts.
- Disk quota: preview cleanup, reduce the data profile, or move the configured cache.
- Authentication: validate the selected dbt target and required environment variables.
- Cancellation: incomplete snapshots are removed and prior active snapshots remain.

Use `dbtv diagnostics` to create a small redacted support archive. It contains no
Parquet data, credentials, manifests, or compiled SQL.
