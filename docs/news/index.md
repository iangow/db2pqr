# Changelog

## db2pq 0.0.4

- Added first-class WRDS credential helpers:
  [`wrds_get_username()`](https://iangow.github.io/db2pqr/reference/wrds_get_username.md),
  [`wrds_conninfo()`](https://iangow.github.io/db2pqr/reference/wrds_conninfo.md),
  and
  [`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md).
- Added Parquet-first public helpers for schema/table listing and WRDS
  SQL exports:
  [`db_schema_tables()`](https://iangow.github.io/db2pqr/reference/db_schema_tables.md),
  [`wrds_get_tables()`](https://iangow.github.io/db2pqr/reference/wrds_get_tables.md),
  [`db_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/db_schema_to_pq.md),
  and
  [`wrds_sql_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_sql_to_pq.md).
- Added
  [`pq_data_dir()`](https://iangow.github.io/db2pqr/reference/pq_data_dir.md)
  to inspect or resolve the default Parquet repository directory.
- Added `rename` support to
  [`db_to_pq()`](https://iangow.github.io/db2pqr/reference/db_to_pq.md)
  and
  [`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).
- Added `pkgdown` site configuration and workflow-oriented vignettes.
- Added ADBC diagnostics and clearer SSL-related error messages for the
  optional ADBC PostgreSQL path.
