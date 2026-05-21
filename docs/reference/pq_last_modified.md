# Get last-modified metadata for Parquet data files

Retrieves the `last_modified` metadata embedded in Parquet files by
[`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).

## Usage

``` r
pq_last_modified(
  table_name = NULL,
  schema = NULL,
  data_dir = Sys.getenv("DATA_DIR", "."),
  archive = FALSE,
  archive_dir = "archive"
)
```

## Arguments

- table_name:

  Optional. Name of a specific table.

- schema:

  Optional. Name of the schema (subdirectory under `data_dir`).

- data_dir:

  Root directory of the Parquet data repository. Defaults to the
  `DATA_DIR` environment variable, or `"."` if unset.

- archive:

  If `TRUE`, look in the archive subdirectory instead of the main schema
  directory.

- archive_dir:

  Name of the archive subdirectory. Defaults to `"archive"`.

## Value

When `table_name` is provided and `archive = FALSE`, a single string.
Otherwise a [tibble](https://tibble.tidyverse.org/reference/tibble.html)
with columns `file_name`, `table`, `schema`, `last_mod` (a `POSIXct` in
UTC), and `last_mod_str`.

## Details

Behaviour depends on the arguments supplied:

- **`table_name` provided, `archive = FALSE`** (default): Returns the
  raw `last_modified` string embedded in
  `<data_dir>/<schema>/<table_name>.parquet`, or `""` if the file has no
  such metadata.

- **`table_name` provided, `archive = TRUE`**: Returns a data frame of
  all archived files matching `table_name` in
  `<data_dir>/<schema>/<archive_dir>/`.

- **Only `schema` provided (no `table_name`)**: Returns a data frame
  summarising all Parquet files in the schema directory (or its archive
  subdirectory if `archive = TRUE`).

- **Neither `table_name` nor `schema` provided**: Returns a data frame
  summarising all Parquet files across every schema subdirectory of
  `data_dir`.

## See also

[`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Raw metadata string for a single table
pq_last_modified("dsi", "crsp")

# Summary of archived versions of a table
pq_last_modified("company", "comp", archive = TRUE)

# Summary of all tables in a schema
pq_last_modified(schema = "crsp")

# Summary of all tables across all schemas
pq_last_modified(data_dir = "data")
} # }
```
