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

## Examples

``` r
wrds_conninfo(wrds_id = "example_user")
#> [1] "postgresql://wrds-pgdata.wharton.upenn.edu:9737/wrds?user=example_user&sslmode=require"
wrds_conninfo(wrds_id = "example_user", format = "dbi")
#> $host
#> [1] "wrds-pgdata.wharton.upenn.edu"
#> 
#> $port
#> [1] 9737
#> 
#> $dbname
#> [1] "wrds"
#> 
#> $user
#> [1] "example_user"
#> 
#> $sslmode
#> [1] "require"
#> 
```
