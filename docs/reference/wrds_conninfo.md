# Build WRDS PostgreSQL connection information

Build WRDS PostgreSQL connection information

## Usage

``` r
wrds_conninfo(wrds_id = NULL, format = c("uri", "dbi"), sslmode = "require")
```

## Arguments

- wrds_id:

  Optional WRDS username override.

- format:

  Return format. `"uri"` returns a PostgreSQL URI. `"dbi"` returns a
  list suitable for `DBI::dbConnect(RPostgres::Postgres(), !!!x)`.

- sslmode:

  PostgreSQL SSL mode. WRDS requires SSL; the default is `"require"`.

## Value

A character URI or a named list of DBI connection arguments.
