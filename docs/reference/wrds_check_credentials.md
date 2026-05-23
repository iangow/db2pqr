# Check WRDS PostgreSQL credentials

Attempts a small WRDS PostgreSQL connection using `RPostgres`. The
return value is a data frame so it can be printed or inspected in setup
scripts.

## Usage

``` r
wrds_check_credentials(
  wrds_id = NULL,
  password = NULL,
  prompt = interactive(),
  save = TRUE,
  passfile = pgpass_path()
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

- save:

  If `TRUE`, save a supplied or prompted password to `.pgpass` after a
  successful WRDS connection test.

- passfile:

  PostgreSQL password-file path. Defaults to `PGPASSFILE` when set,
  otherwise the platform default from
  [`pgpass_path`](https://iangow.github.io/db2pqr/reference/pgpass_path.md).

## Value

A one-row tibble with `ok`, `method`, and `message` columns.
