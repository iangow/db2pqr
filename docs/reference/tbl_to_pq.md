# Export a lazy `dbplyr` table to Parquet via ADBC

Renders a lazy `dbplyr` query to SQL, derives an ADBC PostgreSQL
connection from the backing `RPostgres` connection, and streams Arrow
batches directly to a Parquet file.

## Usage

``` r
tbl_to_pq(
  tbl,
  out_file,
  chunk_size = 100000L,
  metadata = NULL,
  col_types = NULL
)
```

## Arguments

- tbl:

  A lazy table backed by `dbplyr`.

- out_file:

  Full path to the output Parquet file.

- chunk_size:

  Number of rows written per Parquet row group. Default is `100000`.

- metadata:

  Optional named list of schema metadata to embed in the Parquet file.

- col_types:

  Optional named list specifying Arrow type overrides. Values may be
  string type names (for example `"int32"` or `"date"`) or Arrow
  `DataType` objects.

## Value

Invisibly returns `out_file`.

## Details

This is an experimental development path kept separate from
[`lazy_tbl_to_pq`](https://iangow.github.io/db2pqr/reference/lazy_tbl_to_pq.md)
while we evaluate the ADBC transfer route. The ADBC path opens a second
PostgreSQL connection for the transfer, so the database must allow an
additional concurrent connection beyond the one already backing `tbl`.

## Examples

``` r
if (FALSE) { # \dontrun{
# Requires a PostgreSQL connection with the ADBC driver installed
con <- DBI::dbConnect(RPostgres::Postgres())
qry <- dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsi"))
tbl_to_pq(qry, "~/pq_data/crsp/dsi.parquet")
DBI::dbDisconnect(con)
} # }
```
