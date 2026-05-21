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
