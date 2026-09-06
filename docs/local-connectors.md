# Connector credentials on this computer

Keep connector credentials and machine-specific configuration in `.local/` at the
repository root. Git ignores the entire directory, including nested connectors,
profiles, private keys, downloaded fixtures, and reports. It persists across runs
and is separate from temporary files and DBTV's `.dbtv/` cache.

Suggested layout:

```text
.local/
  env.sh
  profiles/profiles.yml
  connectors/
    snowflake/
      private_key.p8
      public_key.pem
      register-public-key.sql
    <another-connector>/
```

Use `0700` permissions for these directories and `0600` for credential/configuration
files. Keep a protected backup if the credentials must survive loss of this checkout.
Git will not back them up. Do not force-add `.local/` or private keys to Git.

## Load settings for a command

Create `.local/env.sh` with the environment variables needed by your connectors.
`scripts/with-local-env.sh` exports `DBTV_LOCAL_DIR` as this checkout's absolute
`.local` path, sources that trusted shell file, and runs the supplied command. For
Snowflake, the environment file can contain:

```bash
DBTV_TEST_SNOWFLAKE_PROFILES_DIR="$DBTV_LOCAL_DIR/profiles"
DBTV_TEST_SNOWFLAKE_PROFILE="dbtv_sandbox"
DBTV_TEST_SNOWFLAKE_TARGET="dev"
DBTV_TEST_SNOWFLAKE_DATABASE="YOUR_TEST_DATABASE"
DBTV_TEST_SNOWFLAKE_SCHEMA="YOUR_TEST_SCHEMA"
DBTV_TEST_SNOWFLAKE_TABLE="YOUR_TEST_TABLE"
DBTV_SNOWFLAKE_PRIVATE_KEY_PATH="$DBTV_LOCAL_DIR/connectors/snowflake/private_key.p8"
```

In the Snowflake target within `.local/profiles/profiles.yml`, reference the key with
`private_key_path: "{{ env_var('DBTV_SNOWFLAKE_PRIVATE_KEY_PATH') }}"`. Keep the account,
user, role, warehouse, database, and schema settings in this ignored profile.

Run the existing read-only extraction smoke test explicitly:

```bash
bash scripts/with-local-env.sh uv run pytest tests/integration/test_snowflake_live.py -v -rs
```

The remote test requires a registered key and an accessible fixture table. Confirm
that it passes rather than skips. It extracts at most ten rows; it is not the complete
Snowflake-versus-DuckDB comparison campaign. Ordinary `uv run pytest` does not load
`.local/env.sh`, so adding credentials here does not automatically enable remote tests.

The same wrapper can supply other connectors' own environment variables. Store each
connector's credentials under its own directory; a Snowflake key does not authenticate
to other services. DBTV currently implements Snowflake and local Parquet. Additional
providers still require their connector plugin and applicable integration tests.

## If a Snowflake private key is lost

A public key or service-user name cannot recover the private key. Generate a new
local pair, then register its public key on the existing service user using a role
that can manage that user's authentication. Snowflake supports adding a named key
pair without overwriting existing keys:

```sql
ALTER USER <service_user> ADD KEY PAIR <unique_key_name>
  PUBLIC_KEY = '<public key without PEM delimiters>';
```

Only the public key belongs in the registration SQL. Existing account resources and
grants can be reused. After registration, run the live test above to verify access.
See [Snowflake key-pair registration](https://docs.snowflake.com/en/sql-reference/sql/alter-user-add-key-pair).
