# Initial support matrix

| Component | Supported initial range |
|---|---|
| Python | 3.11–3.13; 3.12 recommended |
| dbt-core | 1.11.x; lockfile pins 1.11.14 |
| dbt-snowflake | 1.11.x; lockfile pins 1.11.6 |
| dbt-duckdb | 1.11.x; lockfile pins 1.11.0 |
| DuckDB | 1.x |
| Snowflake Connector for Python | 3.12–4.x |
| PyArrow | 18–23 |
| dbt manifest schema | v9–v12 |

The UV lockfile is the tested dependency set for this revision. A company pilot should
narrow this matrix to versions used by its representative projects and authentication
modes. macOS is the currently verified development host; Linux and Windows require CI
and pilot validation before being advertised internally as supported.
