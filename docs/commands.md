# Command reference

Global options include project/profile/target overrides, console or JSON-lines output,
non-interactive mode, log level/path, quiet output, color control, and an optional UUID
invocation ID.

## Planning and execution

- `plan` parses both targets, resolves dbt selection, maps required sources, applies
  working-set policy, and reports local cache decisions without connecting remotely by
  default. `--remote-estimates` makes explicitly tagged source count queries and is
  rejected in offline mode.
- `sync` prepares committed snapshots without running dbt models.
- `run`, `test`, and `build` prepare snapshots, bind and verify DuckDB sources, perform
  prerequisite checks, then delegate compilation and execution to the matching dbt command.

Shared controls include `--select`, `--exclude`, `--vars`, `--source-mode`,
`--data-profile`, `--cache-ttl`, `--max-rows`, `--max-bytes`,
`--allow-full-source`, `--snapshot-id`, `--keep-snapshot`, `--fidelity`, and
`--full-refresh` where relevant. Execution also accepts `--dataset sha256:...` for frozen
inputs and `--capture-results` on run/build for immutable comparison outputs.

`--max-bytes` limits total extracted **uncompressed Arrow bytes**, separately from
`cache.maximum_size` on stored files. `--max-rows` caps requested limits and actual
rows per source, including hash, predicate, and cohort extraction.

Source modes:

- `auto`: reuse valid committed snapshots and refresh missing or expired data;
- `refresh`: refresh every required source;
- `cached`: require a cache hit and never refresh;
- `offline`: require cache hits and structurally prohibit connector creation.

## Operations

- `status [--rebuild-index]` reports snapshots, sizes, runs, database, and lock files.
- `inspect --query ...` permits one read-only DuckDB statement.
- `clean ... --preview` lists exact paths. Removal requires confirmation or `--yes`.
- `diagnostics` creates a redacted archive without Parquet files or data values.
- `doctor`, `version`, and `init` validate or initialize the environment.

Exit codes are stable: 1 dbt failure, 2 configuration/usage, 3 dbt project/artifact,
4 source authentication, 5 extraction, 6 policy, 7 cache/lock/offline, 8 binding,
9 compatibility, 10 internal, and 130 cancellation.


## Replay and comparison

- `datasets list` / `datasets show sha256:...`: discover and inspect retained input sets.
- `datasets drop sha256:... --yes`: release replay retention; files remain until eligible
  for snapshot cleanup. Active cache entries and explicitly pinned snapshots remain protected.
- `replay RUN_UUID`: rebuild a recorded run/build against its frozen sources in a new
  output database. Validates project, referenced environment, configuration, and engine
  context. `--allow-code-change` permits intentional context changes; source identity
  and current policy still apply. Incremental originals need `--full-refresh` because
  replay has no original incremental output state.
- `compare LEFT_UUID RIGHT_UUID`: exact multiset comparison of outputs recorded with
  `--capture-results`. Requires identical input datasets unless `--allow-different-inputs`
  is explicit. Returns exit 1 for differing results or missing output nodes.
- `validate-incremental --base-dataset ID --next-dataset ID [--select +model]`: builds a
  fresh initial state from the base inputs, applies the next inputs incrementally, and
  compares to a separate fresh build on the next inputs. Exit 1 signals a difference.

Replay and validation retain their output databases and captured Parquet inside their
run directories. `clean --runs` removes those artifacts and their ability to be compared.
`clean --snapshots` protects dataset references; `clean --all` removes the entire local
history, including datasets and parsing caches.
