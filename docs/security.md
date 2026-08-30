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
