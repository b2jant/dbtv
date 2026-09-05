# Security model

The primary risks are credential leakage, excessive production extraction, unmanaged
local retention, path or predicate injection, permissive file modes, unsafe cleanup,
and accidental network access in offline mode.

Controls include:

- no secret-valued CLI flags or production credentials in generated profiles;
- restricted dbt-profile expression resolution and non-serializable handles;
- recursive key, connection-string, and inline secret redaction;
- quoted provider/backend identifiers and single-expression predicate validation;
- bounded defaults, full-source consent, source-tag denial, and per-source caps;
- immutable atomic snapshots with `0700` directories and `0600` files by default;
- canonical workspace checks and cleanup that cannot target broad roots;
- no unsigned DuckDB extensions and read-only inspection;
- an offline registry guard tested to prevent connector instantiation;
- diagnostics that whitelist metadata artifacts and exclude cached rows.

MVP storage relies on company-approved full-disk encryption. If application-layer
encryption is mandatory, it belongs at the snapshot-store boundary. Governance owners
must approve allowed source tags, retention, device posture, and incident handling
before a company pilot.


Connection scopes hash public account, user, role, profile/target, configured identity,
and plugin settings. They prevent unintended reuse across access contexts, but cached
data is still local data that can outlive remote permission changes. Ordinary cleanup
protects retained datasets. Explicit dataset release or full cleanup controls retention.

Offline prevents DBTV from creating source connectors or resolving source credentials.
The dbt project is trusted executable code; offline is not a network sandbox for arbitrary
project macros, hooks, or third-party dbt packages. Diagnostic archives exclude source and
captured-output Parquet, and omit predicates, vars, and invocation settings. Names, schemas,
and error messages can still contain project-specific literals; review an archive before sharing it.
