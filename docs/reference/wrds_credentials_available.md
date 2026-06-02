# Check whether WRDS credentials are available

Check whether WRDS credentials are available

## Usage

``` r
wrds_credentials_available(
  wrds_id = NULL,
  password = NULL,
  prompt = interactive(),
  passfile = NULL
)
```

## Arguments

- wrds_id:

  Optional WRDS username override.

- password:

  Optional password. If omitted, a matching `.pgpass` entry is
  preferred. When `.pgpass` has no matching entry, an existing
  `WRDS_PASSWORD` environment variable is used before an interactive
  prompt. PostgreSQL may also use `PGPASSWORD` or another
  libpq-supported mechanism when no password is passed to the
  connection.

- prompt:

  If `TRUE`, prompt interactively for a WRDS PostgreSQL password when no
  password was supplied and no matching `.pgpass` entry exists.

- passfile:

  PostgreSQL password-file path. Defaults to `PGPASSFILE` when set,
  otherwise the platform default password-file path.

## Value

`TRUE` if
[`wrds_check_credentials()`](https://iangow.github.io/db2pqr/reference/wrds_check_credentials.md)
can open a WRDS PostgreSQL connection, otherwise `FALSE`.

## Examples

``` r
wrds_credentials_available(prompt = FALSE)
#> [1] TRUE
```
