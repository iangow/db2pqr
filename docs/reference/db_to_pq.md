# Export a PostgreSQL table to Parquet

Exports a table from PostgreSQL to a Parquet file, with optional row
filtering, column filtering, column renaming, Arrow type overrides, and
timestamp normalization.

## Usage

``` r
db_to_pq(
  table_name,
  schema,
  host = Sys.getenv("PGHOST", "localhost"),
  database = Sys.getenv("PGDATABASE", unset = Sys.getenv("PGUSER")),
  user = Sys.getenv("PGUSER", Sys.info()[["user"]]),
  password = Sys.getenv("PGPASSWORD", ""),
  port = as.integer(Sys.getenv("PGPORT", 5432)),
  data_dir = Sys.getenv("DATA_DIR", "."),
  out_file = NULL,
  where = NULL,
  obs = NULL,
  keep = NULL,
  drop = NULL,
  rename = NULL,
  alt_table_name = NULL,
  chunk_size = 100000L,
  transfer_method = c("dbi", "adbc"),
  numeric_mode = c("float64", "raw"),
  con = NULL,
  metadata = NULL,
  col_types = NULL,
  tz = NULL
)
```

## Arguments

- table_name:

  Name of the source PostgreSQL table.

- schema:

  Name of the source PostgreSQL schema.

- host, database, user, password, port:

  PostgreSQL connection parameters used when `con` is not supplied.

- data_dir:

  Root directory of the local Parquet data repository. Defaults to
  `DATA_DIR`, or `"."` if unset.

- out_file:

  Optional full output path. Overrides `data_dir`, `schema`, and
  `table_name`.

- where:

  Optional SQL `WHERE` clause without the `WHERE` keyword.

- obs:

  Optional integer row limit.

- keep, drop:

  Optional character vectors of regex patterns used to select source
  columns. `drop` is applied before `keep`.

- rename:

  Optional named character vector or list mapping source column names to
  output column names, e.g. `c(conm = "company_name")`.

- alt_table_name:

  Optional output basename when `out_file` is omitted.

- chunk_size:

  Number of rows fetched and written per chunk.

- transfer_method:

  Transfer backend: `"dbi"` for the stable DBI path or `"adbc"` for the
  optional Arrow/ADBC path.

- numeric_mode:

  Numeric handling mode for the ADBC path. `"float64"` casts PostgreSQL
  `numeric` columns to `DOUBLE PRECISION`; `"raw"` keeps the driver
  default.

- con:

  Optional existing DBI connection.

- metadata:

  Optional named list of Parquet schema metadata.

- col_types:

  Optional named list of Arrow output type overrides. Names refer to
  output column names after `rename`.

- tz:

  Time zone used to interpret `TIMESTAMP WITHOUT TIME ZONE` columns.

## Value

Invisibly returns the output file path.
