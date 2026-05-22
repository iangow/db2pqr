# Export a WRDS table to Parquet, skipping if already up to date

Exports a table from the WRDS PostgreSQL database to a Parquet file.
Before downloading, the WRDS table comment is compared against the
`last_modified` metadata embedded in any existing local Parquet file.
The download is skipped if the local file is already up to date, making
this function safe to call repeatedly as part of a scheduled data
refresh.

## Usage

``` r
wrds_update_pq(
  table_name,
  schema,
  data_dir = Sys.getenv("DATA_DIR", "."),
  out_file = NULL,
  force = FALSE,
  where = NULL,
  obs = NULL,
  keep = NULL,
  drop = NULL,
  rename = NULL,
  alt_table_name = NULL,
  chunk_size = NULL,
  col_types = NULL,
  tz = "UTC",
  archive = FALSE,
  archive_dir = "archive",
  use_sas = FALSE,
  sas_schema = NULL,
  encoding = "utf-8",
  wrds_id = NULL,
  transfer_method = c("dbi", "adbc"),
  numeric_mode = c("decimal", "float64", "text", "raw")
)
```

## Arguments

- table_name:

  Name of the table in the WRDS PostgreSQL database.

- schema:

  Name of the database schema (e.g. `"crsp"`, `"comp"`).

- data_dir:

  Root directory of the local Parquet data repository. Defaults to the
  `DATA_DIR` environment variable, or `"."` if unset. The output file is
  written to `<data_dir>/<schema>/<table_name>.parquet`.

- out_file:

  Optional. Full path for the output Parquet file. Overrides the path
  derived from `data_dir`, `schema`, and `table_name`.

- force:

  If `TRUE`, download proceeds regardless of the date comparison result.

- where:

  Optional SQL `WHERE` clause (without the `WHERE` keyword) to filter
  rows before export. For example, `where = "date > '2020-01-01'"`.

- obs:

  Optional integer. Limits the number of rows imported using SQL
  `LIMIT`. Useful for testing with large tables (e.g. `obs = 1000`).

- keep:

  Optional character vector of regex patterns. Only columns whose names
  match at least one pattern are retained. Applied after `drop`.

- drop:

  Optional character vector of regex patterns. Columns whose names match
  at least one pattern are removed. Applied before `keep`.

- rename:

  Optional named character vector or list mapping source column names to
  output column names. `col_types` names should refer to output names
  after renaming.

- alt_table_name:

  Optional. Alternative basename for the output Parquet file, used when
  the file should have a different name from `table_name`.

- chunk_size:

  Number of rows fetched and written per chunk. If omitted, defaults to
  `100000` for `transfer_method = "dbi"` and `250000` for
  `transfer_method = "adbc"`.

- col_types:

  Optional named list specifying column type overrides. Values may be
  string type names (e.g. `"int32"`, `"float32"`, `"date"`) or Arrow
  `DataType` objects. Only columns that need to differ from their
  inferred types need to be supplied. See
  [`arrow_type`](https://iangow.github.io/db2pqr/reference/arrow_type.md)
  for supported names. For example,
  `col_types = list(permno = "int32", ret = "float32")`.

- tz:

  Time zone used to interpret `TIMESTAMP WITHOUT TIME ZONE` columns.
  Such columns are cast to `TIMESTAMPTZ` in the SQL query using
  `AT TIME ZONE`, so they are written as UTC-normalised timestamps in
  the Parquet file. Defaults to `"UTC"`. Set to `NULL` to leave naive
  timestamps as-is.

- archive:

  If `TRUE`, the existing local Parquet file (if any) is moved to the
  archive subdirectory before being replaced. The archived filename is
  `<table>_<YYYYMMDDTHHMMSSz>.parquet`, where the timestamp suffix is
  derived from the `last_modified` metadata embedded in the existing
  file (i.e. the date it was last downloaded, not the incoming WRDS
  table comment).

- archive_dir:

  Name of the archive subdirectory relative to the schema directory.
  Defaults to `"archive"`.

- use_sas:

  If `TRUE`, the last-modified date is obtained by running
  `PROC CONTENTS` on the SAS dataset via SSH, rather than reading the
  PostgreSQL table comment. Requires the ssh package and SSH key access
  to `wrds-cloud-sshkey.wharton.upenn.edu`.

- sas_schema:

  SAS library name to use when `use_sas = TRUE`. Defaults to `schema`.

- encoding:

  Character encoding passed to `PROC CONTENTS` when `use_sas = TRUE`.
  Default is `"utf-8"`.

- wrds_id:

  WRDS user ID used for the SSH connection when `use_sas = TRUE`.
  Defaults to the `WRDS_ID` environment variable.

- transfer_method:

  Transfer backend used for the SQL-to-Parquet step. Use `"dbi"` for the
  existing DBI/data-frame path or `"adbc"` for the experimental
  ADBC/Arrow path.

- numeric_mode:

  Numeric handling mode for PostgreSQL `numeric` columns. The default
  `"decimal"` transfers values as text and writes bounded
  `numeric(p, s)` columns as Arrow decimals. Use `"float64"` to cast
  values to `DOUBLE PRECISION` before fetching, `"text"` to retain text
  values, or `"raw"` to keep the transfer backend's default
  representation.

## Value

Invisibly returns the path to the Parquet file if written, or `NULL` if
the update was skipped.

## See also

[`pq_last_modified`](https://iangow.github.io/db2pqr/reference/pq_last_modified.md),
[`pq_archive`](https://iangow.github.io/db2pqr/reference/pq_archive.md),
[`pq_restore`](https://iangow.github.io/db2pqr/reference/pq_restore.md)

## Examples

``` r
if (FALSE) { # \dontrun{
wrds_update_pq("dsi", "crsp")
wrds_update_pq("feed21_bankruptcy_notification", "audit")

# Force re-download even if local file is current
wrds_update_pq("dsi", "crsp", force = TRUE)

# Limit columns and rows (useful for testing)
wrds_update_pq("dsf", "crsp", obs = 1000, keep = c("permno", "date", "ret"))
} # }
```
