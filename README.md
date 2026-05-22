# db2pq: export PostgreSQL and WRDS data to Parquet

`db2pq` is an R package for moving data from PostgreSQL into Apache Parquet files.
It is designed for both general PostgreSQL sources and the WRDS PostgreSQL service.

## What it does

- Export a single PostgreSQL table to Parquet.
- Export all tables in a PostgreSQL schema to Parquet.
- Update WRDS Parquet files only when the source table is newer.
- Read and manage `last_modified` metadata embedded in Parquet files.
- Archive and restore historical versions of Parquet files.

## Installation

```r
# install.packages("pak")
pak::pak("iangow/db2pqr")
```

## Quickstart

### Update a WRDS table

You can pass a WRDS username directly for a first call:

```r
library(db2pq)

wrds_update_pq("dsi", "crsp", wrds_id = "your_wrds_id")
```

For repeated use, configure the WRDS username and PostgreSQL password outside
the call. The [authentication
article](https://iangow.github.io/db2pqr/articles/authentication.html)
documents the `WRDS_ID`, `.pgpass`, and `wrds::wrds_set_credentials()` paths.
The remaining WRDS examples assume that setup is in place.

### Force a re-download

```r
wrds_update_pq("dsi", "crsp", force = TRUE)
```

### Use SAS metadata to check for updates

```r
wrds_update_pq("dsi", "crsp", use_sas = TRUE)
```

SSH setup is only needed for this SAS metadata path. See the
[WRDS SSH setup article](https://iangow.github.io/db2pqr/articles/wrds-ssh.html)
for the key-based setup used by that option.

### Update all tables in a schema

```r
wrds_schema_to_pq("crsp")
```

### Export a custom WRDS SQL query

```r
wrds_sql_to_pq(
  "SELECT permno, date, ret FROM crsp.dsf WHERE date >= '2024-01-01'",
  table_name = "dsf_recent",
  schema = "crsp"
)
```

### Export a local PostgreSQL table

```r
db_to_pq(
  table_name = "company",
  schema = "comp",
  keep = c("gvkey", "conm"),
  rename = c(conm = "company_name")
)
```

### Check when local Parquet files were last updated

```r
pq_last_modified(schema = "crsp")
```

## ADBC backend

The stable default transfer path uses DBI/RPostgres. The optional ADBC path can
be selected with `transfer_method = "adbc"` when `adbi` and a PostgreSQL ADBC
driver are installed:

```r
adbc_diagnostics()
wrds_update_pq("dsi", "crsp", transfer_method = "adbc")
```

If ADBC reports an SSL/libpq error, use `transfer_method = "dbi"` or install a
current SSL-capable `adbcpostgresql` build.

## Parquet layout

Files are organized as:

```
<DATA_DIR>/<schema>/<table>.parquet
```

For example:

```
~/pq_data/crsp/dsi.parquet
```

The `DATA_DIR` environment variable sets the root directory. It can also be
passed directly as `data_dir` to any function.

When `archive = TRUE`, replaced files are moved to:

```
<DATA_DIR>/<schema>/archive/<table>_<timestamp>.parquet
```

## License

MIT License. See `LICENSE.md`.
