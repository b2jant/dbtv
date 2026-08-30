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
- `run`, `test`, and `build` prepare snapshots, bind and verify DuckDB sources, run a
  local compile pass, then delegate to the matching dbt command.

Shared controls include `--select`, `--exclude`, `--vars`, `--source-mode`,
`--data-profile`, `--cache-ttl`, `--max-rows`, `--max-bytes`,
`--allow-full-source`, `--snapshot-id`, `--keep-snapshot`, `--fidelity`, and
`--full-refresh` where relevant.

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
