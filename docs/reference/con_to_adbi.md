# Create an ADBC PostgreSQL connection from an `RPostgres` connection

This helper is intentionally narrow: it only supports `RPostgres`
`PqConnection` objects and assumes enough connection information is
available to build a PostgreSQL URI. If any required piece is missing,
the function fails instead of attempting alternative fallbacks. The
returned ADBC connection is separate from the original `RPostgres`
connection, so using this helper requires capacity for an additional
concurrent database connection.

## Usage

``` r
con_to_adbi(con)
```

## Arguments

- con:

  An `RPostgres` connection object.

## Value

An ADBC connection created by `adbi`.

## Examples

``` r
if (FALSE) { # \dontrun{
# Requires a PostgreSQL connection and the adbi and adbcpostgresql packages
pg_con <- DBI::dbConnect(RPostgres::Postgres())
adbi_con <- con_to_adbi(pg_con)
DBI::dbDisconnect(adbi_con)
DBI::dbDisconnect(pg_con)
} # }
```
