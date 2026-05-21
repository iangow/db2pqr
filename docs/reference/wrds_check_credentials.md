# Check WRDS PostgreSQL credentials

Attempts a small WRDS PostgreSQL connection using `RPostgres`. The
return value is a data frame so it can be printed or inspected in setup
scripts.

## Usage

``` r
wrds_check_credentials(wrds_id = NULL, password = NULL)
```

## Arguments

- wrds_id:

  Optional WRDS username override.

- password:

  Optional password. If omitted, PostgreSQL may use `.pgpass`,
  `PGPASSWORD`, or another libpq-supported mechanism.

## Value

A one-row tibble with `ok`, `method`, and `message` columns.
