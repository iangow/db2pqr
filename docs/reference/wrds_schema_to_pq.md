# Export all tables in a WRDS schema to Parquet

Iterates over tables in a WRDS PostgreSQL schema and calls
[`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)
for each table.

## Usage

``` r
wrds_schema_to_pq(
  schema,
  data_dir = NULL,
  force = FALSE,
  tables = NULL,
  views = FALSE,
  chunk_size = 100000L,
  wrds_id = NULL,
  transfer_method = c("dbi", "adbc"),
  numeric_mode = c("decimal", "float64", "text", "raw"),
  ...
)
```

## Arguments

- schema:

  WRDS schema name.

- data_dir:

  Root directory of the local Parquet data repository.

- force:

  If `TRUE`, re-download tables even if local files appear current.

- tables:

  Optional subset of table names to process.

- views:

  If `TRUE`, include views as well as base tables.

- chunk_size:

  Number of rows fetched and written per chunk.

- wrds_id:

  Optional WRDS username override.

- transfer_method:

  Transfer backend passed to
  [`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).

- numeric_mode:

  Numeric handling mode passed to
  [`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).

- ...:

  Additional arguments passed to
  [`wrds_update_pq`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md).

## Value

Invisibly returns a named list of output paths or `NULL` values for
skipped/failed tables.

## Examples

``` r
if (wrds_credentials_available(prompt = FALSE)) {
  wrds_schema_to_pq("crsp", data_dir = "~/pq_data")

  # Process a specific subset of tables
  wrds_schema_to_pq("crsp", data_dir = "~/pq_data",
                    tables = c("dsi", "dsf"))
}
#> No tables to process in schema 'crsp'.
#> Warning: The following tables were not found in schema 'crsp' and will be skipped: dsi, dsf
#> No tables to process in schema 'crsp'.
```
