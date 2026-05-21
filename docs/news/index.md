# Changelog

## db2pq 0.0.3.9000

- Added first-class WRDS credential helpers:
  [`wrds_get_username()`](https://iandgow.github.io/db2pqr/reference/wrds_get_username.md),
  [`wrds_conninfo()`](https://iandgow.github.io/db2pqr/reference/wrds_conninfo.md),
  [`wrds_check_credentials()`](https://iandgow.github.io/db2pqr/reference/wrds_check_credentials.md),
  [`pgpass_path()`](https://iandgow.github.io/db2pqr/reference/pgpass_path.md),
  [`pgpass_find()`](https://iandgow.github.io/db2pqr/reference/pgpass_find.md),
  and
  [`pgpass_has_entry()`](https://iandgow.github.io/db2pqr/reference/pgpass_has_entry.md).
- Added Parquet-first public helpers for schema/table listing and WRDS
  SQL exports:
  [`db_schema_tables()`](https://iandgow.github.io/db2pqr/reference/db_schema_tables.md),
  [`wrds_get_tables()`](https://iandgow.github.io/db2pqr/reference/wrds_get_tables.md),
  [`db_schema_to_pq()`](https://iandgow.github.io/db2pqr/reference/db_schema_to_pq.md),
  and
  [`wrds_sql_to_pq()`](https://iandgow.github.io/db2pqr/reference/wrds_sql_to_pq.md).
- Added `rename` support to
  [`db_to_pq()`](https://iandgow.github.io/db2pqr/reference/db_to_pq.md)
  and
  [`wrds_update_pq()`](https://iandgow.github.io/db2pqr/reference/wrds_update_pq.md).
- Added `pkgdown` site configuration and workflow-oriented vignettes.
- Added ADBC diagnostics and clearer SSL-related error messages for the
  optional ADBC PostgreSQL path.
