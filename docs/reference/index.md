# Package index

## PostgreSQL and WRDS to Parquet

- [`db_to_pq()`](https://iangow.github.io/db2pqr/reference/db_to_pq.md)
  : Export a PostgreSQL table to Parquet

- [`lazy_tbl_to_pq()`](https://iangow.github.io/db2pqr/reference/lazy_tbl_to_pq.md)
  :

  Export a lazy `dbplyr` table to Parquet

- [`tbl_to_pq()`](https://iangow.github.io/db2pqr/reference/tbl_to_pq.md)
  :

  Export a lazy `dbplyr` table to Parquet via ADBC

- [`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)
  : Export a WRDS table to Parquet, skipping if already up to date

- [`wrds_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_schema_to_pq.md)
  : Export all tables in a WRDS schema to Parquet

- [`wrds_sql_to_pq()`](https://iangow.github.io/db2pqr/reference/wrds_sql_to_pq.md)
  : Export a WRDS SQL query to Parquet

- [`db_schema_to_pq()`](https://iangow.github.io/db2pqr/reference/db_schema_to_pq.md)
  : Export all tables in a PostgreSQL schema to Parquet

- [`db_schema_tables()`](https://iangow.github.io/db2pqr/reference/db_schema_tables.md)
  : List PostgreSQL relations in a schema

- [`wrds_get_tables()`](https://iangow.github.io/db2pqr/reference/wrds_get_tables.md)
  : List WRDS relations in a schema

## Authentication and configuration

- [`wrds_get_username()`](https://iangow.github.io/db2pqr/reference/wrds_get_username.md)
  : Resolve a WRDS username

- [`wrds_conninfo()`](https://iangow.github.io/db2pqr/reference/wrds_conninfo.md)
  : Build WRDS PostgreSQL connection information

- [`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
  : Check WRDS PostgreSQL credentials

- [`pgpass_path()`](https://iangow.github.io/db2pqr/reference/pgpass_path.md)
  : Default PostgreSQL password file path

- [`pgpass_find()`](https://iangow.github.io/db2pqr/reference/pgpass_find.md)
  :

  Find a matching `.pgpass` entry

- [`pgpass_has_entry()`](https://iangow.github.io/db2pqr/reference/pgpass_has_entry.md)
  :

  Check for a matching `.pgpass` entry

- [`adbc_diagnostics()`](https://iangow.github.io/db2pqr/reference/adbc_diagnostics.md)
  : Report optional ADBC dependency status

## Parquet file utilities

- [`pq_last_modified()`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md)
  : Get last-modified metadata for Parquet data files
- [`pq_archive()`](https://iangow.github.io/db2pqr/reference/pq_archive.md)
  : Archive a Parquet file into the archive subdirectory
- [`pq_restore()`](https://iangow.github.io/db2pqr/reference/pq_restore.md)
  : Restore an archived Parquet file to the active schema directory
- [`pq_remove()`](https://iangow.github.io/db2pqr/reference/pq_remove.md)
  : Remove a Parquet file from active or archive storage
- [`arrow_type()`](https://iangow.github.io/db2pqr/reference/arrow_type.md)
  : Resolve an Arrow data type from a string or pass through an existing
  type

## ADBC development helpers

- [`con_to_adbi()`](https://iangow.github.io/db2pqr/reference/con_to_adbi.md)
  :

  Create an ADBC PostgreSQL connection from an `RPostgres` connection

- [`tbl_to_pq_debug()`](https://iangow.github.io/db2pqr/reference/tbl_to_pq_debug.md)
  :

  Debug the ADBC chunk fetch path for a lazy `dbplyr` table
