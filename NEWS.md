# db2pq 0.0.3.9000

* Added first-class WRDS credential helpers: `wrds_get_username()`,
  `wrds_conninfo()`, `wrds_check_credentials()`, `pgpass_path()`,
  `pgpass_find()`, and `pgpass_has_entry()`.
* Added Parquet-first public helpers for schema/table listing and WRDS SQL
  exports: `db_schema_tables()`, `wrds_get_tables()`, `db_schema_to_pq()`,
  and `wrds_sql_to_pq()`.
* Added `rename` support to `db_to_pq()` and `wrds_update_pq()`.
* Added `pkgdown` site configuration and workflow-oriented vignettes.
* Added ADBC diagnostics and clearer SSL-related error messages for the
  optional ADBC PostgreSQL path.
