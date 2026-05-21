# Find a matching `.pgpass` entry

Find a matching `.pgpass` entry

## Usage

``` r
pgpass_find(host, port = 5432L, database, user, passfile = pgpass_path())
```

## Arguments

- host, port, database, user:

  PostgreSQL connection target.

- passfile:

  Path to the password file.

## Value

A tibble with matching entries. The `password` column is included
because this is a low-level credential helper; avoid printing it in
logs.
