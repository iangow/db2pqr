# Debug the ADBC chunk fetch path for a lazy `dbplyr` table

Fetches Arrow chunks one at a time using the experimental ADBC path and
records where failures occur. This is intended for diagnosing driver or
result-set issues without writing a Parquet file.

## Usage

``` r
tbl_to_pq_debug(tbl, max_chunks = Inf)
```

## Arguments

- tbl:

  A lazy table backed by `dbplyr`.

- max_chunks:

  Maximum number of chunks to inspect. Default is `Inf`.

## Value

A data frame with one row per attempted chunk.

## Examples

``` r
if (FALSE) { # \dontrun{
# Requires a PostgreSQL connection and the ADBC driver installed
con <- DBI::dbConnect(RPostgres::Postgres())
qry <- dplyr::tbl(con, DBI::Id(schema = "crsp", table = "dsi"))
tbl_to_pq_debug(qry, max_chunks = 3L)
DBI::dbDisconnect(con)
} # }
```
