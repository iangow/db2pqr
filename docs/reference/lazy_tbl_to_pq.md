# Export a lazy `dbplyr` table to Parquet

Renders a lazy `dbplyr` query to SQL and streams the result to a Parquet
file using the package's internal SQL-to-Parquet writer. This avoids
collecting the full result into memory before writing.

## Usage

``` r
lazy_tbl_to_pq(
  tbl,
  out_file,
  chunk_size = 100000L,
  metadata = NULL,
  col_types = NULL
)
```

## Arguments

- tbl:

  A lazy table backed by `dbplyr`, such as the result of
  [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html) or a
  pipeline of `dplyr` verbs on a remote table.

- out_file:

  Full path to the output Parquet file.

- chunk_size:

  Number of rows fetched and written per chunk. Default is `100000`.

- metadata:

  Optional named list of schema metadata to embed in the Parquet file.

- col_types:

  Optional named list specifying Arrow type overrides. Values may be
  string type names (for example `"int32"` or `"date"`) or Arrow
  `DataType` objects.

## Value

Invisibly returns `out_file`.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(RPostgres::Postgres())
qry <- dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsi")) |>
  dplyr::filter(date >= as.Date("2020-01-01"))

lazy_tbl_to_pq(qry, "~/pq_data/crsp/dsi_recent.parquet")
} # }
```
