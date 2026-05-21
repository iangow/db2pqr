# Export all tables in a PostgreSQL schema to Parquet

Export all tables in a PostgreSQL schema to Parquet

## Usage

``` r
db_schema_to_pq(
  schema,
  data_dir = Sys.getenv("DATA_DIR", "."),
  force = FALSE,
  tables = NULL,
  chunk_size = 100000L,
  transfer_method = c("dbi", "adbc"),
  numeric_mode = c("float64", "raw"),
  archive = FALSE,
  archive_dir = "archive",
  con = NULL,
  ...
)
```

## Arguments

- schema:

  PostgreSQL schema name.

- data_dir:

  Root directory of the local Parquet data repository.

- force:

  If `FALSE`, existing output files are skipped.

- tables:

  Optional subset of table names to export.

- chunk_size:

  Number of rows fetched and written per chunk.

- transfer_method:

  Transfer backend passed to
  [`db_to_pq`](https://iandgow.github.io/db2pqr/reference/db_to_pq.md).

- numeric_mode:

  Numeric handling mode passed to
  [`db_to_pq`](https://iandgow.github.io/db2pqr/reference/db_to_pq.md).

- archive:

  Whether existing output files should be archived before replacement.

- archive_dir:

  Archive directory name.

- con:

  Optional existing DBI connection.

- ...:

  Additional arguments passed to
  [`db_to_pq`](https://iandgow.github.io/db2pqr/reference/db_to_pq.md).

## Value

Invisibly returns a named list of output paths.
