# Connect to WRDS PostgreSQL with DBI

Opens a `DBI` connection to the WRDS PostgreSQL database using
`RPostgres` and the `db2pq` WRDS authentication flow.

## Usage

``` r
wrds_connect_dbi(
  wrds_id = NULL,
  prompt = interactive(),
  save = TRUE,
  passfile = NULL
)
```

## Arguments

- wrds_id:

  Optional WRDS username override.

- prompt:

  If `TRUE`, prompt interactively for a WRDS PostgreSQL password when no
  password was supplied and no matching `.pgpass` entry exists.

- save:

  If `TRUE`, save a supplied or prompted password to `.pgpass` after a
  successful WRDS connection test.

- passfile:

  PostgreSQL password-file path. Defaults to `PGPASSFILE` when set,
  otherwise the platform default password-file path.

## Value

A `DBIConnection` object.

## Details

The username is resolved from `wrds_id`, `WRDS_ID`, `WRDS_USER`, or the
keyring entry written by
[`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html).
Password handling prefers a matching PostgreSQL `.pgpass` entry, then
`WRDS_PASSWORD`, then an interactive password prompt when
`prompt = TRUE`. When a supplied, prompted, or `WRDS_PASSWORD` password
is used successfully and `save = TRUE`, it is saved to `.pgpass` for
future connections.
