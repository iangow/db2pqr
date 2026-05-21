# Export a WRDS SQL query to Parquet

Runs SQL against WRDS PostgreSQL and writes the result to a Parquet file
using the same DBI or ADBC transfer paths as
[`wrds_update_pq`](https://iandgow.github.io/db2pqr/reference/wrds_update_pq.md).

## Usage

``` r
wrds_sql_to_pq(
  sql,
  table_name,
  schema,
  wrds_id = NULL,
  data_dir = Sys.getenv("DATA_DIR", "."),
  out_file = NULL,
  modified = NULL,
  alt_table_name = NULL,
  chunk_size = NULL,
  transfer_method = c("dbi", "adbc"),
  archive = FALSE,
  archive_dir = "archive",
  col_types = NULL
)
```

## Arguments

- sql:

  SQL query to execute against WRDS PostgreSQL.

- table_name:

  Logical table name used for the output basename.

- schema:

  Logical schema name used for the output directory.

- wrds_id:

  Optional WRDS username override.

- data_dir:

  Root directory of the local Parquet data repository.

- out_file:

  Optional full output path.

- modified:

  Optional last-modified metadata string to embed.

- alt_table_name:

  Optional output basename when `out_file` is omitted.

- chunk_size:

  Number of rows fetched and written per chunk.

- transfer_method:

  Transfer backend: `"dbi"` or `"adbc"`.

- archive:

  Whether an existing output file should be archived before replacement.

- archive_dir:

  Archive directory name.

- col_types:

  Optional named list of Arrow output type overrides.

## Value

Invisibly returns the output file path.
