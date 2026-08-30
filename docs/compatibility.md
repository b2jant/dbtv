# Compatibility and fidelity

Snowflake and DuckDB do not have identical SQL, type, catalog, or incremental semantics.
`dbtv` therefore fails closed for known blocking constructs in strict mode and records
stable findings in each run.

Initial blocking rules cover Snowflake `GENERATOR`, `FLATTEN`, `RESULT_SCAN`, selected
semi-structured casts, and unsupported materializations. Target-dependent SQL and local
incremental state produce explicit warnings. Rule IDs may be allowed, warned, or denied
in configuration only after a project validates the semantic difference.

Arrow and Parquet preserve the source transfer schema. Type analysis blocks decimal
precision beyond DuckDB's supported precision in strict mode. `warn` and `lossy` are
explicit fidelity choices; they are never selected silently.

Local results should be compared with representative Snowflake results before a project
adopts dbtv. Company production validation remains authoritative. Adapter-dispatched
macros are preferred for dialect differences because dbt remains responsible for SQL
compilation.
